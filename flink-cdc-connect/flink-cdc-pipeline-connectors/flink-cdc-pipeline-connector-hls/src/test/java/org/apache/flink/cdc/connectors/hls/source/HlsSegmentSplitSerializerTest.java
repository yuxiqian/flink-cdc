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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HlsSegmentSplitSerializerTest {

    @Test
    void roundTrip() throws Exception {
        HlsSegmentSplit original =
                new HlsSegmentSplit("http://h/p/seg42.ts", 42L, 6.5, "http://h/p/playlist.m3u8");
        HlsSegmentSplitSerializer serializer = new HlsSegmentSplitSerializer();

        byte[] bytes = serializer.serialize(original);
        HlsSegmentSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(restored.getSegmentUrl()).isEqualTo(original.getSegmentUrl());
        assertThat(restored.getSequenceNumber()).isEqualTo(original.getSequenceNumber());
        assertThat(restored.getDuration()).isEqualTo(original.getDuration());
        assertThat(restored.getPlaylistUrl()).isEqualTo(original.getPlaylistUrl());
        assertThat(restored.splitId()).isEqualTo(original.splitId());
    }

    @Test
    void versionIsStable() {
        assertThat(new HlsSegmentSplitSerializer().getVersion()).isEqualTo(1);
    }
}
