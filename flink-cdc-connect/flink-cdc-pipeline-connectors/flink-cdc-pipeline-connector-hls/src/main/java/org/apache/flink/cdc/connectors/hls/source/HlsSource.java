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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.HashSet;

/** FLIP-27 HLS source. */
public class HlsSource implements Source<Event, HlsSegmentSplit, HlsEnumeratorState> {

    private static final long serialVersionUID = 1L;

    private final String playlistUrl;
    private final String mode;
    private final long pollIntervalMs;
    private final int connectionTimeoutMs;
    private final int readTimeoutMs;

    public HlsSource(
            String playlistUrl,
            String mode,
            long pollIntervalMs,
            int connectionTimeoutMs,
            int readTimeoutMs) {
        this.playlistUrl = playlistUrl;
        this.mode = mode;
        this.pollIntervalMs = pollIntervalMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    @Override
    public Boundedness getBoundedness() {
        if ("live".equalsIgnoreCase(mode)) {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        } else if ("vod".equalsIgnoreCase(mode)) {
            return Boundedness.BOUNDED;
        }
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<HlsSegmentSplit, HlsEnumeratorState> createEnumerator(
            SplitEnumeratorContext<HlsSegmentSplit> enumContext) {
        M3u8Parser parser = new M3u8Parser(connectionTimeoutMs, readTimeoutMs);
        boolean isLive = resolveIsLive(parser);
        return new HlsSourceEnumerator(
                enumContext, playlistUrl, parser, isLive, pollIntervalMs, new HashSet<>());
    }

    @Override
    public SplitEnumerator<HlsSegmentSplit, HlsEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<HlsSegmentSplit> enumContext, HlsEnumeratorState checkpoint) {
        M3u8Parser parser = new M3u8Parser(connectionTimeoutMs, readTimeoutMs);
        boolean isLive = resolveIsLive(parser);
        return new HlsSourceEnumerator(
                enumContext,
                playlistUrl,
                parser,
                isLive,
                pollIntervalMs,
                checkpoint.getDiscoveredSequences());
    }

    @Override
    public SimpleVersionedSerializer<HlsSegmentSplit> getSplitSerializer() {
        return new HlsSegmentSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<HlsEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new HlsEnumeratorStateSerializer();
    }

    @Override
    public SourceReader<Event, HlsSegmentSplit> createReader(SourceReaderContext readerContext) {
        M3u8Parser parser = new M3u8Parser(connectionTimeoutMs, readTimeoutMs);
        return new HlsSourceReader(readerContext, parser);
    }

    private boolean resolveIsLive(M3u8Parser parser) {
        if ("live".equalsIgnoreCase(mode)) {
            return true;
        } else if ("vod".equalsIgnoreCase(mode)) {
            return false;
        }
        try {
            M3u8Playlist playlist = parser.parse(playlistUrl);
            return playlist.isLive();
        } catch (Exception e) {
            return false;
        }
    }
}
