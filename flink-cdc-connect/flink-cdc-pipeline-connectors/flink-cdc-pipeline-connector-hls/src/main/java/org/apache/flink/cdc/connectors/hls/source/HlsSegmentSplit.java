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

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

/** HLS .ts segment split. */
public class HlsSegmentSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String segmentUrl;
    private final long sequenceNumber;
    private final double duration;
    private final String playlistUrl;

    public HlsSegmentSplit(
            String segmentUrl, long sequenceNumber, double duration, String playlistUrl) {
        this.segmentUrl = segmentUrl;
        this.sequenceNumber = sequenceNumber;
        this.duration = duration;
        this.playlistUrl = playlistUrl;
    }

    @Override
    public String splitId() {
        return "hls-segment-" + sequenceNumber;
    }

    public String getSegmentUrl() {
        return segmentUrl;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public double getDuration() {
        return duration;
    }

    public String getPlaylistUrl() {
        return playlistUrl;
    }

    @Override
    public String toString() {
        return "HlsSegmentSplit{"
                + "segmentUrl='"
                + segmentUrl
                + '\''
                + ", seq="
                + sequenceNumber
                + ", duration="
                + duration
                + '}';
    }
}
