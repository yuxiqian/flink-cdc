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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlPipelineRecordEmitter;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

/** A {@link DataSource} for mysql cdc connector. */
@Internal
public class MySqlDataSource implements DataSource {

    private final MySqlSourceConfigFactory configFactory;
    private final MySqlSourceConfig sourceConfig;

    public MySqlDataSource(MySqlSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.sourceConfig = configFactory.createConfig(0);
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        MySqlEventDeserializer deserializer =
                new MySqlEventDeserializer(
                        DebeziumChangelogMode.ALL, sourceConfig.isIncludeSchemaChanges());

        MySqlSource<Event> source =
                new MySqlSource<>(
                        configFactory,
                        deserializer,
                        (sourceReaderMetrics, sourceConfig) ->
                                new MySqlPipelineRecordEmitter(
                                        deserializer, sourceReaderMetrics, sourceConfig));

        return FlinkSourceProvider.of(source);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new MySqlMetadataAccessor(sourceConfig);
    }

    @VisibleForTesting
    public MySqlSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    @Override
    public boolean canContainDistributedTables() {
        // During incremental stage, MySQL never emits schema change events on different partitions
        // (since it has one Binlog stream only.)
        return false;
    }
}
