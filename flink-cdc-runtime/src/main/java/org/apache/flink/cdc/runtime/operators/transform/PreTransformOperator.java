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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A data process function that filters out columns which aren't (directly & indirectly) referenced.
 */
public class PreTransformOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {

    private final List<TransformRule> transformRules;
    private transient List<PreTransformers> transforms;
    private final Map<TableId, PreTransformChangeInfo> preTransformChangeInfoMap;
    private final List<Tuple2<Selectors, SchemaMetadataTransform>> schemaMetadataTransformers;
    private transient ListState<byte[]> state;
    private Map<TableId, PreTransformProcessor> preTransformProcessorMap;

    public static PreTransformOperator.Builder newBuilder() {
        return new PreTransformOperator.Builder();
    }

    /** Builder of {@link PreTransformOperator}. */
    public static class Builder {
        private final List<TransformRule> transformRules = new ArrayList<>();

        public PreTransformOperator.Builder addTransform(
                String tableInclusions, @Nullable String projection, @Nullable String filter) {
            transformRules.add(new TransformRule(tableInclusions, projection, filter, "", "", ""));
            return this;
        }

        public PreTransformOperator.Builder addTransform(
                String tableInclusions,
                @Nullable String projection,
                @Nullable String filter,
                String primaryKey,
                String partitionKey,
                String tableOption) {
            transformRules.add(
                    new TransformRule(
                            tableInclusions,
                            projection,
                            filter,
                            primaryKey,
                            partitionKey,
                            tableOption));
            return this;
        }

        public PreTransformOperator build() {
            return new PreTransformOperator(transformRules);
        }
    }

    private PreTransformOperator(List<TransformRule> transformRules) {
        this.transformRules = transformRules;
        this.preTransformChangeInfoMap = new ConcurrentHashMap<>();
        this.preTransformProcessorMap = new ConcurrentHashMap<>();
        this.schemaMetadataTransformers = new ArrayList<>();
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        transforms = new ArrayList<>();
        for (TransformRule transformRule : transformRules) {
            String tableInclusions = transformRule.getTableInclusions();
            String projection = transformRule.getProjection();
            String filter = transformRule.getFilter();
            String primaryKeys = transformRule.getPrimaryKey();
            String partitionKeys = transformRule.getPartitionKey();
            String tableOptions = transformRule.getTableOption();
            Selectors selectors =
                    new Selectors.SelectorsBuilder().includeTables(tableInclusions).build();
            transforms.add(
                    new PreTransformers(
                            selectors,
                            TransformProjection.of(projection).orElse(null),
                            TransformFilter.of(filter).orElse(null)));
            schemaMetadataTransformers.add(
                    new Tuple2<>(
                            selectors,
                            new SchemaMetadataTransform(primaryKeys, partitionKeys, tableOptions)));
        }
        this.preTransformProcessorMap = new ConcurrentHashMap<>();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        OperatorStateStore stateStore = context.getOperatorStateStore();
        ListStateDescriptor<byte[]> descriptor =
                new ListStateDescriptor<>("originalSchemaState", byte[].class);
        state = stateStore.getUnionListState(descriptor);
        if (context.isRestored()) {
            for (byte[] serializedTableInfo : state.get()) {
                PreTransformChangeInfo stateTableChangeInfo =
                        PreTransformChangeInfo.SERIALIZER.deserialize(
                                PreTransformChangeInfo.SERIALIZER.getVersion(),
                                serializedTableInfo);
                preTransformChangeInfoMap.put(
                        stateTableChangeInfo.getTableId(), stateTableChangeInfo);
                // Since PostTransformOperator doesn't preserve state, pre-transformed schema
                // information needs to be passed by PreTransformOperator.
                output.collect(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        stateTableChangeInfo.getTableId(),
                                        stateTableChangeInfo.getPreTransformedSchema())));
            }
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        state.update(
                new ArrayList<>(
                        preTransformChangeInfoMap.values().stream()
                                .map(
                                        tableChangeInfo -> {
                                            try {
                                                return PreTransformChangeInfo.SERIALIZER.serialize(
                                                        tableChangeInfo);
                                            } catch (IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        })
                                .collect(Collectors.toList())));
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        clearOperator();
    }

    @Override
    public void close() throws Exception {
        super.close();
        clearOperator();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) event;
            preTransformProcessorMap.remove(createTableEvent.tableId());
            event = cacheCreateTable(createTableEvent);
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            preTransformProcessorMap.remove(schemaChangeEvent.tableId());
            event = cacheChangeSchema(schemaChangeEvent);
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = processDataChangeEvent(((DataChangeEvent) event));
            output.collect(new StreamRecord<>(dataChangeEvent));
        }
    }

    private SchemaChangeEvent cacheCreateTable(CreateTableEvent event) {
        TableId tableId = event.tableId();
        Schema originalSchema = event.getSchema();
        event = transformCreateTableEvent(event);
        Schema newSchema = (event).getSchema();
        preTransformChangeInfoMap.put(
                tableId, PreTransformChangeInfo.of(tableId, originalSchema, newSchema));
        return event;
    }

    private SchemaChangeEvent cacheChangeSchema(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        PreTransformChangeInfo tableChangeInfo = preTransformChangeInfoMap.get(tableId);
        Schema originalSchema =
                SchemaUtils.applySchemaChangeEvent(tableChangeInfo.getSourceSchema(), event);
        Schema newSchema =
                SchemaUtils.applySchemaChangeEvent(
                        tableChangeInfo.getPreTransformedSchema(), event);
        preTransformChangeInfoMap.put(
                tableId, PreTransformChangeInfo.of(tableId, originalSchema, newSchema));
        return event;
    }

    private CreateTableEvent transformCreateTableEvent(CreateTableEvent createTableEvent) {
        TableId tableId = createTableEvent.tableId();
        PreTransformChangeInfo tableChangeInfo = preTransformChangeInfoMap.get(tableId);

        for (Tuple2<Selectors, SchemaMetadataTransform> transform : schemaMetadataTransformers) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId)) {
                createTableEvent =
                        new CreateTableEvent(
                                tableId,
                                transformSchemaMetaData(
                                        createTableEvent.getSchema(), transform.f1));
            }
        }

        for (PreTransformers transform : transforms) {
            Selectors selectors = transform.getSelectors();
            if (selectors.isMatch(tableId) && transform.getProjection().isPresent()) {
                TransformProjection transformProjection = transform.getProjection().get();
                TransformFilter transformFilter = transform.getFilter().orElse(null);
                if (transformProjection.isValid()) {
                    if (!preTransformProcessorMap.containsKey(tableId)) {
                        preTransformProcessorMap.put(
                                tableId,
                                new PreTransformProcessor(
                                        tableChangeInfo, transformProjection, transformFilter));
                    }
                    PreTransformProcessor preTransformProcessor =
                            preTransformProcessorMap.get(tableId);
                    // filter out unreferenced columns in pre-transform process
                    return preTransformProcessor.preTransformCreateTableEvent(createTableEvent);
                }
            }
        }
        return createTableEvent;
    }

    private Schema transformSchemaMetaData(
            Schema schema, SchemaMetadataTransform schemaMetadataTransform) {
        Schema.Builder schemaBuilder = Schema.newBuilder().setColumns(schema.getColumns());
        if (!schemaMetadataTransform.getPrimaryKeys().isEmpty()) {
            schemaBuilder.primaryKey(schemaMetadataTransform.getPrimaryKeys());
        } else {
            schemaBuilder.primaryKey(schema.primaryKeys());
        }
        if (!schemaMetadataTransform.getPartitionKeys().isEmpty()) {
            schemaBuilder.partitionKey(schemaMetadataTransform.getPartitionKeys());
        } else {
            schemaBuilder.partitionKey(schema.partitionKeys());
        }
        if (!schemaMetadataTransform.getOptions().isEmpty()) {
            schemaBuilder.options(schemaMetadataTransform.getOptions());
        } else {
            schemaBuilder.options(schema.options());
        }
        return schemaBuilder.build();
    }

    private DataChangeEvent processDataChangeEvent(DataChangeEvent dataChangeEvent)
            throws Exception {
        TableId tableId = dataChangeEvent.tableId();
        for (PreTransformers transform : transforms) {
            Selectors selectors = transform.getSelectors();

            if (selectors.isMatch(tableId) && transform.getProjection().isPresent()) {
                TransformProjection transformProjection = transform.getProjection().get();
                TransformFilter transformFilter = transform.getFilter().orElse(null);
                if (transformProjection.isValid()) {
                    return processProjection(transformProjection, transformFilter, dataChangeEvent);
                }
            }
        }
        return dataChangeEvent;
    }

    private DataChangeEvent processProjection(
            TransformProjection transformProjection,
            @Nullable TransformFilter transformFilter,
            DataChangeEvent dataChangeEvent) {
        TableId tableId = dataChangeEvent.tableId();
        PreTransformChangeInfo tableChangeInfo = preTransformChangeInfoMap.get(tableId);
        if (!preTransformProcessorMap.containsKey(tableId)
                || !preTransformProcessorMap.get(tableId).hasTableChangeInfo()) {
            preTransformProcessorMap.put(
                    tableId,
                    new PreTransformProcessor(
                            tableChangeInfo, transformProjection, transformFilter));
        }
        PreTransformProcessor preTransformProcessor = preTransformProcessorMap.get(tableId);
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if (before != null) {
            BinaryRecordData projectedBefore = preTransformProcessor.processFillDataField(before);
            dataChangeEvent = DataChangeEvent.projectBefore(dataChangeEvent, projectedBefore);
        }
        if (after != null) {
            BinaryRecordData projectedAfter = preTransformProcessor.processFillDataField(after);
            dataChangeEvent = DataChangeEvent.projectAfter(dataChangeEvent, projectedAfter);
        }
        return dataChangeEvent;
    }

    private void clearOperator() {
        this.transforms = null;
        this.preTransformProcessorMap = null;
        this.state = null;
    }
}
