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

package org.apache.flink.cdc.connectors.hls.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.hls.source.HlsDataSource;
import org.apache.flink.table.api.ValidationException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class HlsDataSourceFactoryTest {

    @Test
    void testCreateDataSource() {
        DataSourceFactory factory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("hls", DataSourceFactory.class);
        Assertions.assertThat(factory).isInstanceOf(HlsDataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(buildOptions("https://example.com/playlist.m3u8"));

        DataSource dataSource =
                factory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));

        Assertions.assertThat(dataSource).isInstanceOf(HlsDataSource.class);
    }

    @Test
    void testCreateDataSourceWithAllOptions() {
        DataSourceFactory factory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("hls", DataSourceFactory.class);

        Map<String, String> options = buildOptions("https://example.com/playlist.m3u8");
        options.put("mode", "live");
        options.put("poll.interval", "10s");
        options.put("connection.timeout", "60s");
        options.put("read.timeout", "120s");

        Configuration conf = Configuration.fromMap(options);

        DataSource dataSource =
                factory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));

        Assertions.assertThat(dataSource).isInstanceOf(HlsDataSource.class);
    }

    @Test
    void testMissingRequiredOption() {
        DataSourceFactory factory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("hls", DataSourceFactory.class);

        Configuration conf = Configuration.fromMap(new HashMap<>());

        Assertions.assertThatThrownBy(
                        () ->
                                factory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "One or more required options are missing.\n\n"
                                + "Missing required options are:\n\n"
                                + "uri");
    }

    @Test
    void testUnsupportedOption() {
        DataSourceFactory factory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("hls", DataSourceFactory.class);

        Map<String, String> options = buildOptions("https://example.com/playlist.m3u8");
        options.put("unsupported_key", "unsupported_value");

        Configuration conf = Configuration.fromMap(options);

        Assertions.assertThatThrownBy(
                        () ->
                                factory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'hls'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    private static Map<String, String> buildOptions(String uri) {
        Map<String, String> options = new HashMap<>();
        options.put("uri", uri);
        return options;
    }
}
