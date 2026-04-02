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

import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;

/** HLS data source. */
public class HlsDataSource implements DataSource {

    private final String playlistUrl;
    private final String mode;
    private final long pollIntervalMs;
    private final int connectionTimeoutMs;
    private final int readTimeoutMs;

    public HlsDataSource(
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
    public EventSourceProvider getEventSourceProvider() {
        return FlinkSourceProvider.of(
                new HlsSource(
                        playlistUrl, mode, pollIntervalMs, connectionTimeoutMs, readTimeoutMs));
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        throw new UnsupportedOperationException(
                "HLS data source does not support getMetadataAccessor.");
    }
}
