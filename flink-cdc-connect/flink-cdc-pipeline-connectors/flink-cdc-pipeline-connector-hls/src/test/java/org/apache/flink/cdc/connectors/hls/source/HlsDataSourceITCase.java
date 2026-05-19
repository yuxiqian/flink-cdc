/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.hls.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HlsDataSourceITCase {

    private static final int MAX_PARALLELISM = 1;

    private static final org.apache.flink.configuration.Configuration MINI_CLUSTER_CONFIG =
            new org.apache.flink.configuration.Configuration();

    static {
        MINI_CLUSTER_CONFIG.set(
                org.apache.flink.configuration.CoreOptions
                        .ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
    }

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(MAX_PARALLELISM)
                            .setConfiguration(MINI_CLUSTER_CONFIG)
                            .build());

    private HlsTestHttpServer http;

    @BeforeEach
    void setUp() throws Exception {
        http = new HlsTestHttpServer();
        // Three synthetic .ts segments
        for (int i = 0; i < 3; i++) {
            http.put("/seg" + i + ".ts", payload(i));
        }
    }

    @AfterEach
    void tearDown() {
        http.close();
    }

    @Test
    void testVariantPlaylist() throws Exception {
        http.put("/v.m3u8", buildVariantPlaylist());
        List<Event> events = collectEvents(http.url("/v.m3u8"));
        verifyEvents(events);
    }

    @Test
    void testMasterPlaylist() throws Exception {
        http.put("/master.m3u8", "#EXTM3U\n" + "#EXT-X-STREAM-INF:BANDWIDTH=400000\n" + "v.m3u8\n");
        http.put("/v.m3u8", buildVariantPlaylist());
        List<Event> events = collectEvents(http.url("/master.m3u8"));
        verifyEvents(events);
    }

    private static String buildVariantPlaylist() {
        return "#EXTM3U\n"
                + "#EXT-X-VERSION:3\n"
                + "#EXT-X-TARGETDURATION:4\n"
                + "#EXT-X-MEDIA-SEQUENCE:0\n"
                + "#EXTINF:4.0,\n"
                + "seg0.ts\n"
                + "#EXTINF:4.0,\n"
                + "seg1.ts\n"
                + "#EXTINF:4.0,\n"
                + "seg2.ts\n"
                + "#EXT-X-ENDLIST\n";
    }

    private static byte[] payload(int seq) {
        byte[] data = new byte[16];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (seq * 31 + i);
        }
        return data;
    }

    private List<Event> collectEvents(String playlistUrl) throws Exception {
        HlsDataSource dataSource = new HlsDataSource(playlistUrl, "vod", 5000, 5000, 5000);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) dataSource.getEventSourceProvider();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(MAX_PARALLELISM);
        RestartStrategyUtils.configureNoRestartStrategy(env);

        try (CloseableIterator<Event> iterator =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "hls-source",
                                new EventTypeInfo())
                        .executeAndCollect()) {
            List<Event> events = new ArrayList<>();
            while (iterator.hasNext()) {
                events.add(iterator.next());
            }
            return events;
        }
    }

    private void verifyEvents(List<Event> events) {
        assertThat(events).hasSize(4); // 1 CreateTable + 3 segments

        Event first = events.get(0);
        assertThat(first).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTable = (CreateTableEvent) first;
        assertThat(createTable.tableId()).isEqualTo(HlsSourceReader.TABLE_ID);

        Schema schema = createTable.getSchema();
        List<Column> columns = schema.getColumns();
        assertThat(columns)
                .isEqualTo(
                        Arrays.asList(
                                Column.physicalColumn("url", DataTypes.STRING()),
                                Column.physicalColumn("sequence_number", DataTypes.BIGINT()),
                                Column.physicalColumn("duration", DataTypes.DOUBLE()),
                                Column.physicalColumn("data", DataTypes.BYTES()),
                                Column.physicalColumn("playlist_url", DataTypes.STRING())));
        assertThat(schema.primaryKeys()).isEmpty();

        for (int i = 1; i < events.size(); i++) {
            Event event = events.get(i);
            assertThat(event).isInstanceOf(DataChangeEvent.class);
            DataChangeEvent dce = (DataChangeEvent) event;
            assertThat(dce.tableId()).isEqualTo(HlsSourceReader.TABLE_ID);

            RecordData after = dce.after();
            assertThat(after).isNotNull();

            long seq = after.getLong(1);
            assertThat(seq).isBetween(0L, 2L);
            assertThat(after.getString(0).toString()).endsWith("/seg" + seq + ".ts");
            assertThat(after.getDouble(2)).isEqualTo(4.0);
            assertThat(after.getBinary(3)).isEqualTo(payload((int) seq));
            assertThat(after.getString(4).toString()).contains(".m3u8");
        }
    }
}
