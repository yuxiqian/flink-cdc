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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import java.time.Duration;

/** HLS data source options. */
public class HlsDataSourceOptions {

    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The URL of the HLS m3u8 playlist.");

    public static final ConfigOption<String> MODE =
            ConfigOptions.key("mode")
                    .stringType()
                    .defaultValue("auto")
                    .withDescription(
                            "The stream mode: 'auto' (detect from playlist), 'live', or 'vod'.");

    public static final ConfigOption<Duration> POLL_INTERVAL =
            ConfigOptions.key("poll.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription(
                            "Polling interval for live playlists to discover new segments.");

    public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
            ConfigOptions.key("connection.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("HTTP connection timeout.");

    public static final ConfigOption<Duration> READ_TIMEOUT =
            ConfigOptions.key("read.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("HTTP read timeout.");
}
