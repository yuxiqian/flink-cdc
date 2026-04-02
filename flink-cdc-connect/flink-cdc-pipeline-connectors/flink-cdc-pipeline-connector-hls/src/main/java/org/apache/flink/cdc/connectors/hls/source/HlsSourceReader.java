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

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.core.io.InputStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/** HLS segment source reader. */
public class HlsSourceReader implements SourceReader<Event, HlsSegmentSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(HlsSourceReader.class);

    public static final TableId TABLE_ID = TableId.tableId("hls", "segments");

    public static final Schema SEGMENT_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("url", DataTypes.STRING())
                    .physicalColumn("sequence_number", DataTypes.BIGINT())
                    .physicalColumn("duration", DataTypes.DOUBLE())
                    .physicalColumn("data", DataTypes.BYTES())
                    .physicalColumn("playlist_url", DataTypes.STRING())
                    .build();

    private static final DataType[] COLUMN_TYPES =
            new DataType[] {
                DataTypes.STRING(),
                DataTypes.BIGINT(),
                DataTypes.DOUBLE(),
                DataTypes.BYTES(),
                DataTypes.STRING()
            };

    private final SourceReaderContext readerContext;
    private final M3u8Parser parser;
    private final Queue<HlsSegmentSplit> assignedSplits;
    private final BinaryRecordDataGenerator recordGenerator;
    private boolean tableCreated;
    private boolean noMoreSplits;
    private CompletableFuture<Void> availabilityFuture;

    public HlsSourceReader(SourceReaderContext readerContext, M3u8Parser parser) {
        this.readerContext = readerContext;
        this.parser = parser;
        this.assignedSplits = new ArrayDeque<>();
        this.recordGenerator = new BinaryRecordDataGenerator(COLUMN_TYPES);
        this.tableCreated = false;
        this.noMoreSplits = false;
        this.availabilityFuture = new CompletableFuture<>();
    }

    @Override
    public void start() {
        LOG.info("HLS source reader started on subtask {}", readerContext.getIndexOfSubtask());
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Event> output) throws Exception {
        if (!tableCreated) {
            output.collect(new CreateTableEvent(TABLE_ID, SEGMENT_SCHEMA));
            tableCreated = true;
        }

        HlsSegmentSplit split = assignedSplits.poll();
        if (split == null) {
            return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }

        try {
            LOG.debug(
                    "Downloading segment: {} (seq={})",
                    split.getSegmentUrl(),
                    split.getSequenceNumber());
            byte[] data = parser.downloadSegment(split.getSegmentUrl());

            Object[] fields =
                    new Object[] {
                        BinaryStringData.fromString(split.getSegmentUrl()),
                        split.getSequenceNumber(),
                        split.getDuration(),
                        data,
                        BinaryStringData.fromString(split.getPlaylistUrl())
                    };

            DataChangeEvent event =
                    DataChangeEvent.insertEvent(TABLE_ID, recordGenerator.generate(fields));
            output.collect(event);

            LOG.debug("Emitted segment seq={} ({} bytes)", split.getSequenceNumber(), data.length);
        } catch (Exception e) {
            LOG.error(
                    "Failed to download segment: {} (seq={})",
                    split.getSegmentUrl(),
                    split.getSequenceNumber(),
                    e);
            throw e;
        }

        if (assignedSplits.isEmpty()) {
            return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }
        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<HlsSegmentSplit> snapshotState(long checkpointId) {
        return Collections.unmodifiableList(new java.util.ArrayList<>(assignedSplits));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availabilityFuture;
    }

    @Override
    public void addSplits(List<HlsSegmentSplit> splits) {
        LOG.info("Received {} splits", splits.size());
        assignedSplits.addAll(splits);
        availabilityFuture.complete(null);
        availabilityFuture = new CompletableFuture<>();
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.info("No more splits will be assigned");
        noMoreSplits = true;
        availabilityFuture.complete(null);
    }

    @Override
    public void close() throws Exception {}
}
