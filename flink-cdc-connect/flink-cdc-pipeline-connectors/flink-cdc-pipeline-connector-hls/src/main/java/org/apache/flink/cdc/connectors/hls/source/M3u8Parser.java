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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/** M3U8 playlist parser. */
public class M3u8Parser implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(M3u8Parser.class);

    private final int connectionTimeoutMs;
    private final int readTimeoutMs;

    public M3u8Parser(int connectionTimeoutMs, int readTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    public M3u8Playlist parse(String playlistUrl) throws IOException {
        List<String> lines = fetchLines(playlistUrl);

        if (isMasterPlaylist(lines)) {
            String variantUrl = resolveFirstVariant(playlistUrl, lines);
            LOG.info("Resolved master playlist to variant: {}", variantUrl);
            lines = fetchLines(variantUrl);
            return parseMediaPlaylist(variantUrl, lines);
        }

        return parseMediaPlaylist(playlistUrl, lines);
    }

    private boolean isMasterPlaylist(List<String> lines) {
        for (String line : lines) {
            if (line.startsWith("#EXT-X-STREAM-INF:")) {
                return true;
            }
        }
        return false;
    }

    private String resolveFirstVariant(String masterUrl, List<String> lines) {
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).startsWith("#EXT-X-STREAM-INF:")) {
                for (int j = i + 1; j < lines.size(); j++) {
                    String candidate = lines.get(j).trim();
                    if (!candidate.isEmpty() && !candidate.startsWith("#")) {
                        return resolveUrl(masterUrl, candidate);
                    }
                }
            }
        }
        throw new IllegalArgumentException(
                "Master playlist does not contain any variant stream: " + masterUrl);
    }

    private M3u8Playlist parseMediaPlaylist(String playlistUrl, List<String> lines) {
        List<M3u8Playlist.Segment> segments = new ArrayList<>();
        boolean isLive = true;
        long mediaSequence = 0;
        double currentDuration = 0.0;

        for (String s : lines) {
            String line = s.trim();

            if (line.startsWith("#EXT-X-MEDIA-SEQUENCE:")) {
                mediaSequence =
                        Long.parseLong(line.substring("#EXT-X-MEDIA-SEQUENCE:".length()).trim());
            } else if (line.equals("#EXT-X-ENDLIST")) {
                isLive = false;
            } else if (line.startsWith("#EXTINF:")) {
                String durationStr = line.substring("#EXTINF:".length());
                int commaIdx = durationStr.indexOf(',');
                if (commaIdx >= 0) {
                    durationStr = durationStr.substring(0, commaIdx);
                }
                currentDuration = Double.parseDouble(durationStr.trim());
            } else if (!line.isEmpty() && !line.startsWith("#")) {
                String segmentUrl = resolveUrl(playlistUrl, line);
                long seqNum = mediaSequence + segments.size();
                segments.add(new M3u8Playlist.Segment(segmentUrl, seqNum, currentDuration));
                currentDuration = 0.0;
            }
        }

        return new M3u8Playlist(segments, isLive, mediaSequence);
    }

    static String resolveUrl(String baseUrl, String relativeUrl) {
        if (relativeUrl.startsWith("http://") || relativeUrl.startsWith("https://")) {
            return relativeUrl;
        }
        try {
            URI baseUri = new URI(baseUrl);
            URI resolved = baseUri.resolve(relativeUrl);
            return resolved.toString();
        } catch (Exception e) {
            int lastSlash = baseUrl.lastIndexOf('/');
            if (lastSlash >= 0) {
                return baseUrl.substring(0, lastSlash + 1) + relativeUrl;
            }
            return relativeUrl;
        }
    }

    private List<String> fetchLines(String urlStr) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(connectionTimeoutMs);
        conn.setReadTimeout(readTimeoutMs);
        conn.setRequestMethod("GET");

        List<String> lines = new ArrayList<>();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } finally {
            conn.disconnect();
        }
        return lines;
    }

    public byte[] downloadSegment(String segmentUrl) throws IOException {
        URL url = new URL(segmentUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(connectionTimeoutMs);
        conn.setReadTimeout(readTimeoutMs);
        conn.setRequestMethod("GET");

        try (java.io.InputStream is = conn.getInputStream();
                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = is.read(buf)) >= 0) {
                baos.write(buf, 0, n);
            }
            return baos.toByteArray();
        } finally {
            conn.disconnect();
        }
    }
}
