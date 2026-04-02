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

import java.io.Serializable;
import java.util.List;

/** Parsed HLS media playlist. */
public class M3u8Playlist implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<Segment> segments;
    private final boolean isLive;
    private final long mediaSequence;

    public M3u8Playlist(List<Segment> segments, boolean isLive, long mediaSequence) {
        this.segments = segments;
        this.isLive = isLive;
        this.mediaSequence = mediaSequence;
    }

    public List<Segment> getSegments() {
        return segments;
    }

    public boolean isLive() {
        return isLive;
    }

    public long getMediaSequence() {
        return mediaSequence;
    }

    /** Single .ts segment. */
    public static class Segment implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String url;
        private final long sequenceNumber;
        private final double duration;

        public Segment(String url, long sequenceNumber, double duration) {
            this.url = url;
            this.sequenceNumber = sequenceNumber;
            this.duration = duration;
        }

        public String getUrl() {
            return url;
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }

        public double getDuration() {
            return duration;
        }

        @Override
        public String toString() {
            return "Segment{"
                    + "url='"
                    + url
                    + '\''
                    + ", seq="
                    + sequenceNumber
                    + ", duration="
                    + duration
                    + '}';
        }
    }
}
