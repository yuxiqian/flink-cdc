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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

@Disabled("For manual testing only")
class HlsExampleITCase {

    private static final int MAX_PARALLELISM = 1;

    private static final String VARIANT_PLAYLIST_URL = "http://localhost/hls/stream.m3u8";

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

    @Test
    void testSaveSegmentsToFiles() throws Exception {
        Path outputDir = Files.createTempDirectory("hls-segments-");
        Runtime.getRuntime().exec(new String[] {"open", outputDir.toString()});

        HlsDataSource dataSource =
                new HlsDataSource(VARIANT_PLAYLIST_URL, "auto", 5000, 30000, 60000);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) dataSource.getEventSourceProvider();

        org.apache.flink.configuration.Configuration flinkConfig =
                new org.apache.flink.configuration.Configuration();
        flinkConfig.setString("collect-sink.batch-size.max", String.valueOf(128 * 1024 * 1024));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.setParallelism(MAX_PARALLELISM);
        env.setRestartStrategy(RestartStrategies.noRestart());

        CloseableIterator<Event> iterator =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "hls-source",
                                new EventTypeInfo())
                        .executeAndCollect();

        int segmentCount = 0;
        try {
            while (iterator.hasNext()) {
                Event event = iterator.next();
                if (!(event instanceof DataChangeEvent)) {
                    continue;
                }
                DataChangeEvent dce = (DataChangeEvent) event;
                RecordData after = dce.after();

                String url = after.getString(0).toString();
                long seq = after.getLong(1);
                double duration = after.getDouble(2);
                byte[] data = after.getBinary(3);
                String playlistUrl = after.getString(4).toString();

                String baseName = String.format("segment_%03d", seq);
                Files.write(outputDir.resolve(baseName + ".ts"), data);

                String meta =
                        String.format(
                                "url: %s%nsequence_number: %d%nduration: %.2f%ndata_size: %d%nplaylist_url: %s%n",
                                url, seq, duration, data.length, playlistUrl);
                Files.write(
                        outputDir.resolve(baseName + ".txt"),
                        meta.getBytes(StandardCharsets.UTF_8));

                System.out.printf(
                        "[%d] %s  (%.2fs, %d bytes) -> %s%n",
                        seq,
                        url.substring(url.lastIndexOf('/') + 1),
                        duration,
                        data.length,
                        baseName + ".ts");
                segmentCount++;
            }
        } finally {
            iterator.close();
        }

        System.out.printf("%nSaved %d segments to: %s%n", segmentCount, outputDir);
    }
}
