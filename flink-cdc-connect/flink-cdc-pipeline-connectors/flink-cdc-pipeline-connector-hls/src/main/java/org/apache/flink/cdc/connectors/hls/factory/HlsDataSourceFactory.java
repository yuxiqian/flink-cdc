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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.connectors.hls.source.HlsDataSource;
import org.apache.flink.cdc.connectors.hls.source.HlsDataSourceOptions;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/** HLS data source factory. */
public class HlsDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "hls";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validate();

        String uri = context.getFactoryConfiguration().get(HlsDataSourceOptions.URI);
        String mode = context.getFactoryConfiguration().get(HlsDataSourceOptions.MODE);
        Duration pollInterval =
                context.getFactoryConfiguration().get(HlsDataSourceOptions.POLL_INTERVAL);
        Duration connectionTimeout =
                context.getFactoryConfiguration().get(HlsDataSourceOptions.CONNECTION_TIMEOUT);
        Duration readTimeout =
                context.getFactoryConfiguration().get(HlsDataSourceOptions.READ_TIMEOUT);

        return new HlsDataSource(
                uri,
                mode,
                pollInterval.toMillis(),
                (int) connectionTimeout.toMillis(),
                (int) readTimeout.toMillis());
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HlsDataSourceOptions.URI);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HlsDataSourceOptions.MODE);
        options.add(HlsDataSourceOptions.POLL_INTERVAL);
        options.add(HlsDataSourceOptions.CONNECTION_TIMEOUT);
        options.add(HlsDataSourceOptions.READ_TIMEOUT);
        return options;
    }
}
