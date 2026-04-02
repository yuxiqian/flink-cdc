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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class M3u8ParserTest {

    private HlsTestHttpServer http;
    private M3u8Parser parser;

    @BeforeEach
    void setUp() throws Exception {
        http = new HlsTestHttpServer();
        parser = new M3u8Parser(5000, 5000);
    }

    @AfterEach
    void tearDown() {
        http.close();
    }

    @Test
    void parsesVodMediaPlaylistWithRelativeUrls() throws Exception {
        http.put(
                "/v.m3u8",
                "#EXTM3U\n"
                        + "#EXT-X-VERSION:3\n"
                        + "#EXT-X-TARGETDURATION:6\n"
                        + "#EXT-X-MEDIA-SEQUENCE:10\n"
                        + "#EXTINF:6.0,\n"
                        + "a.ts\n"
                        + "#EXTINF:5.5,\n"
                        + "b.ts\n"
                        + "#EXT-X-ENDLIST\n");

        M3u8Playlist playlist = parser.parse(http.url("/v.m3u8"));

        assertThat(playlist.isLive()).isFalse();
        assertThat(playlist.getMediaSequence()).isEqualTo(10L);
        List<M3u8Playlist.Segment> segments = playlist.getSegments();
        assertThat(segments).hasSize(2);
        assertThat(segments.get(0).getSequenceNumber()).isEqualTo(10L);
        assertThat(segments.get(0).getDuration()).isEqualTo(6.0);
        assertThat(segments.get(0).getUrl()).isEqualTo(http.url("/a.ts"));
        assertThat(segments.get(1).getSequenceNumber()).isEqualTo(11L);
        assertThat(segments.get(1).getDuration()).isEqualTo(5.5);
        assertThat(segments.get(1).getUrl()).isEqualTo(http.url("/b.ts"));
    }

    @Test
    void parsesLivePlaylistWhenEndlistMissing() throws Exception {
        http.put(
                "/live.m3u8",
                "#EXTM3U\n" + "#EXT-X-MEDIA-SEQUENCE:0\n" + "#EXTINF:2.0,\n" + "a.ts\n");

        M3u8Playlist playlist = parser.parse(http.url("/live.m3u8"));

        assertThat(playlist.isLive()).isTrue();
        assertThat(playlist.getSegments()).hasSize(1);
    }

    @Test
    void resolvesFirstVariantFromMasterPlaylist() throws Exception {
        http.put(
                "/master.m3u8",
                "#EXTM3U\n"
                        + "#EXT-X-STREAM-INF:BANDWIDTH=800000\n"
                        + "low/index.m3u8\n"
                        + "#EXT-X-STREAM-INF:BANDWIDTH=2000000\n"
                        + "high/index.m3u8\n");
        http.put(
                "/low/index.m3u8",
                "#EXTM3U\n"
                        + "#EXT-X-MEDIA-SEQUENCE:0\n"
                        + "#EXTINF:4.0,\n"
                        + "seg0.ts\n"
                        + "#EXT-X-ENDLIST\n");

        M3u8Playlist playlist = parser.parse(http.url("/master.m3u8"));

        assertThat(playlist.isLive()).isFalse();
        assertThat(playlist.getSegments()).hasSize(1);
        assertThat(playlist.getSegments().get(0).getUrl()).isEqualTo(http.url("/low/seg0.ts"));
    }

    @Test
    void resolveUrlKeepsAbsoluteUrlsUnchanged() {
        assertThat(M3u8Parser.resolveUrl("http://h/p/x.m3u8", "https://other/seg.ts"))
                .isEqualTo("https://other/seg.ts");
        assertThat(M3u8Parser.resolveUrl("http://h/p/x.m3u8", "http://other/seg.ts"))
                .isEqualTo("http://other/seg.ts");
    }

    @Test
    void resolveUrlJoinsRelativeAgainstBase() {
        assertThat(M3u8Parser.resolveUrl("http://h/p/x.m3u8", "seg.ts"))
                .isEqualTo("http://h/p/seg.ts");
        assertThat(M3u8Parser.resolveUrl("http://h/p/x.m3u8", "sub/seg.ts"))
                .isEqualTo("http://h/p/sub/seg.ts");
        assertThat(M3u8Parser.resolveUrl("http://h/p/x.m3u8", "/abs/seg.ts"))
                .isEqualTo("http://h/abs/seg.ts");
    }

    @Test
    void downloadSegmentReturnsRawBytes() throws Exception {
        byte[] body = new byte[] {1, 2, 3, 4, 5};
        http.put("/raw.ts", body);

        assertThat(parser.downloadSegment(http.url("/raw.ts"))).isEqualTo(body);
    }

    @Test
    void masterPlaylistWithoutVariantThrows() {
        http.put("/empty-master.m3u8", "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\n");

        assertThatThrownBy(() -> parser.parse(http.url("/empty-master.m3u8")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
