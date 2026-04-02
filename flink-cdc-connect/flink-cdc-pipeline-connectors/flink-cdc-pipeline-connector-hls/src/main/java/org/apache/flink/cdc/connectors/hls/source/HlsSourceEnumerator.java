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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/** HLS segment split enumerator. */
public class HlsSourceEnumerator implements SplitEnumerator<HlsSegmentSplit, HlsEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(HlsSourceEnumerator.class);

    private final SplitEnumeratorContext<HlsSegmentSplit> context;
    private final String playlistUrl;
    private final M3u8Parser parser;
    private final boolean isLiveMode;
    private final long pollIntervalMs;
    private final Set<Long> discoveredSequences;
    private final Queue<HlsSegmentSplit> pendingSplits;
    private boolean allSegmentsDiscovered;

    /**
     * For a new live job (no checkpointed sequences), the first playlist snapshot is only used to
     * seed {@link #discoveredSequences}; splits are created only for segments that appear after
     * startup (via polling), so we do not download URLs that may already have left the origin
     * window.
     */
    private boolean pendingLiveStartupSeed;

    public HlsSourceEnumerator(
            SplitEnumeratorContext<HlsSegmentSplit> context,
            String playlistUrl,
            M3u8Parser parser,
            boolean isLiveMode,
            long pollIntervalMs,
            Set<Long> discoveredSequences) {
        this.context = context;
        this.playlistUrl = playlistUrl;
        this.parser = parser;
        this.isLiveMode = isLiveMode;
        this.pollIntervalMs = pollIntervalMs;
        this.discoveredSequences = new HashSet<>(discoveredSequences);
        this.pendingSplits = new ArrayDeque<>();
        this.allSegmentsDiscovered = false;
        this.pendingLiveStartupSeed = isLiveMode && discoveredSequences.isEmpty();
    }

    @Override
    public void start() {
        LOG.info("Starting HLS source enumerator for playlist: {}", playlistUrl);
        discoverSegments();

        if (isLiveMode) {
            context.callAsync(
                    this::pollForNewSegments,
                    this::handleNewSplits,
                    pollIntervalMs,
                    pollIntervalMs);
        } else {
            allSegmentsDiscovered = true;
        }
    }

    private void discoverSegments() {
        try {
            boolean skipEnqueueForLiveStartup = pendingLiveStartupSeed;
            if (pendingLiveStartupSeed) {
                pendingLiveStartupSeed = false;
            }

            M3u8Playlist playlist = parser.parse(playlistUrl);
            List<HlsSegmentSplit> newSplits = new ArrayList<>();
            int skippedAtStartup = 0;
            for (M3u8Playlist.Segment segment : playlist.getSegments()) {
                if (!discoveredSequences.contains(segment.getSequenceNumber())) {
                    discoveredSequences.add(segment.getSequenceNumber());
                    if (skipEnqueueForLiveStartup) {
                        skippedAtStartup++;
                    } else {
                        newSplits.add(
                                new HlsSegmentSplit(
                                        segment.getUrl(),
                                        segment.getSequenceNumber(),
                                        segment.getDuration(),
                                        playlistUrl));
                    }
                }
            }
            if (skipEnqueueForLiveStartup && skippedAtStartup > 0) {
                LOG.info(
                        "Live HLS: skipped {} segment(s) already listed at startup; ingesting only segments that appear after startup",
                        skippedAtStartup);
            }
            pendingSplits.addAll(newSplits);
            LOG.info("Discovered {} new segments from playlist", newSplits.size());
        } catch (IOException e) {
            LOG.error("Failed to parse m3u8 playlist: {}", playlistUrl, e);
            throw new RuntimeException("Failed to parse m3u8 playlist", e);
        }
    }

    private List<HlsSegmentSplit> pollForNewSegments() {
        try {
            M3u8Playlist playlist = parser.parse(playlistUrl);
            List<HlsSegmentSplit> newSplits = new ArrayList<>();
            for (M3u8Playlist.Segment segment : playlist.getSegments()) {
                if (!discoveredSequences.contains(segment.getSequenceNumber())) {
                    discoveredSequences.add(segment.getSequenceNumber());
                    newSplits.add(
                            new HlsSegmentSplit(
                                    segment.getUrl(),
                                    segment.getSequenceNumber(),
                                    segment.getDuration(),
                                    playlistUrl));
                }
            }
            return newSplits;
        } catch (IOException e) {
            LOG.warn("Failed to poll m3u8 playlist for new segments", e);
            return new ArrayList<>();
        }
    }

    private void handleNewSplits(List<HlsSegmentSplit> newSplits, Throwable error) {
        if (error != null) {
            LOG.error("Error polling for new HLS segments", error);
            return;
        }
        if (!newSplits.isEmpty()) {
            LOG.info("Polled {} new segments", newSplits.size());
            pendingSplits.addAll(newSplits);
            assignPendingSplits();
        }
    }

    private void assignPendingSplits() {
        while (!pendingSplits.isEmpty()) {
            int numReaders = context.currentParallelism();
            if (numReaders == 0) {
                return;
            }
            HlsSegmentSplit split = pendingSplits.poll();
            if (split != null) {
                int subtask = (int) (split.getSequenceNumber() % numReaders);
                context.assignSplit(split, subtask);
                LOG.debug("Assigned split {} to subtask {}", split.splitId(), subtask);
            }
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void addSplitsBack(List<HlsSegmentSplit> splits, int subtaskId) {
        LOG.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);
        pendingSplits.addAll(splits);
        assignPendingSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Reader {} added", subtaskId);
        assignPendingSplits();
        if (allSegmentsDiscovered && pendingSplits.isEmpty()) {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public HlsEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new HlsEnumeratorState(discoveredSequences);
    }

    @Override
    public void close() throws IOException {}
}
