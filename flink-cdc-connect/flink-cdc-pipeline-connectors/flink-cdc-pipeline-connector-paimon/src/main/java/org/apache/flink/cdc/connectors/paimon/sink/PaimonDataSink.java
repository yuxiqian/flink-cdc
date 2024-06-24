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

package org.apache.flink.cdc.connectors.paimon.sink;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonRecordSerializer;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonSink;

import org.apache.paimon.options.Options;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A {@link DataSink} for Paimon connector that supports schema evolution. */
public class PaimonDataSink implements DataSink, Serializable {

    // options for creating Paimon catalog.
    private final Options options;

    // options for creating Paimon table.
    private final Map<String, String> tableOptions;

    private final String commitUser;

    private final Map<TableId, List<String>> partitionMaps;

    private final PaimonRecordSerializer<Event> serializer;

    public PaimonDataSink(
            Options options,
            Map<String, String> tableOptions,
            String commitUser,
            Map<TableId, List<String>> partitionMaps,
            PaimonRecordSerializer<Event> serializer) {
        this.options = options;
        this.tableOptions = tableOptions;
        this.commitUser = commitUser;
        this.partitionMaps = partitionMaps;
        this.serializer = serializer;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(new PaimonSink<>(options, commitUser, serializer));
    }

    @Override
    public MetadataApplier getMetadataApplier(Set<SchemaChangeEventType> enabledEventTypes) {
        return new PaimonMetadataApplier(options, tableOptions, partitionMaps, enabledEventTypes);
    }
}
