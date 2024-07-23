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

package org.apache.flink.cdc.runtime.operators.schema;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeFamily;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.utils.ChangeEventUtils;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.event.RefreshPendingListsRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.ReleaseUpstreamRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeProcessingResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResultRequest;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;

/**
 * The operator will evolve schemas in {@link SchemaRegistry} for incoming {@link
 * SchemaChangeEvent}s and block the stream for tables before their schema changes finish.
 */
@Internal
public class SchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SchemaOperator.class);
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);

    private final List<RouteRule> routingRules;

    /**
     * Storing route source table selector, sink table name (before symbol replacement), and replace
     * symbol in a tuple.
     */
    private transient List<Tuple3<Selectors, String, String>> routes;

    private transient TaskOperatorEventGateway toCoordinator;
    private transient SchemaEvolutionClient schemaEvolutionClient;
    private transient LoadingCache<TableId, Schema> cachedSchemas;

    /**
     * Storing mapping relations between upstream tableId (source table) mapping to downstream
     * tableIds (sink tables).
     */
    private transient LoadingCache<TableId, List<TableId>> tableIdMappingCache;

    private final long rpcTimeOutInMillis;

    public SchemaOperator(List<RouteRule> routingRules) {
        this.routingRules = routingRules;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT.toMillis();
    }

    public SchemaOperator(List<RouteRule> routingRules, Duration rpcTimeOut) {
        this.routingRules = routingRules;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        this.toCoordinator = containingTask.getEnvironment().getOperatorCoordinatorEventGateway();
        routes =
                routingRules.stream()
                        .map(
                                rule -> {
                                    String tableInclusions = rule.sourceTable;
                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple3<>(
                                            selectors, rule.sinkTable, rule.replaceSymbol);
                                })
                        .collect(Collectors.toList());
        schemaEvolutionClient = new SchemaEvolutionClient(toCoordinator, getOperatorID());
        cachedSchemas =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, Schema>() {
                                    @Override
                                    public Schema load(TableId tableId) {
                                        return getLatestSchema(tableId);
                                    }
                                });
        tableIdMappingCache =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, List<TableId>>() {
                                    @Override
                                    public List<TableId> load(TableId tableId) {
                                        return getRoutedTables(tableId);
                                    }
                                });
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        if (context.isRestored()) {
            // Multiple operators may appear during a restart process,
            // only clear the pendingSchemaChanges when the first operator starts.
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                sendRequestToCoordinator(new RefreshPendingListsRequest());
            }
        }
    }

    /**
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     */
    @Override
    public void processElement(StreamRecord<Event> streamRecord)
            throws InterruptedException, TimeoutException, ExecutionException {
        Event event = streamRecord.getValue();
        // Schema changes
        if (event instanceof SchemaChangeEvent) {
            TableId tableId = ((SchemaChangeEvent) event).tableId();
            LOG.info(
                    "Table {} received SchemaChangeEvent and start to be blocked.",
                    tableId.toString());
            handleSchemaChangeEvent(tableId, (SchemaChangeEvent) event);
            // Update caches
            cachedSchemas.put(tableId, getLatestSchema(tableId));
            tableIdMappingCache
                    .get(tableId)
                    .forEach(routed -> cachedSchemas.put(routed, getLatestSchema(routed)));
            return;
        }

        // Data changes
        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
        List<TableId> optionalRoutedTable = tableIdMappingCache.get(dataChangeEvent.tableId());
        if (optionalRoutedTable.isEmpty()) {
            output.collect(streamRecord);
        } else {
            optionalRoutedTable.forEach(
                    route ->
                            output.collect(
                                    new StreamRecord<>(
                                            maybeFillInNullForEmptyColumns(
                                                    dataChangeEvent, route))));
        }
    }

    // ----------------------------------------------------------------------------------

    private DataChangeEvent maybeFillInNullForEmptyColumns(
            DataChangeEvent originalEvent, TableId routedTableId) {
        try {
            Schema originalSchema = cachedSchemas.get(originalEvent.tableId());
            Schema routedTableSchema = cachedSchemas.get(routedTableId);
            if (originalSchema.equals(routedTableSchema)) {
                return ChangeEventUtils.recreateDataChangeEvent(originalEvent, routedTableId);
            }
            switch (originalEvent.op()) {
                case INSERT:
                    return DataChangeEvent.insertEvent(
                            routedTableId,
                            regenerateRecordData(
                                    originalEvent.after(), originalSchema, routedTableSchema),
                            originalEvent.meta());
                case UPDATE:
                    return DataChangeEvent.updateEvent(
                            routedTableId,
                            regenerateRecordData(
                                    originalEvent.before(), originalSchema, routedTableSchema),
                            regenerateRecordData(
                                    originalEvent.after(), originalSchema, routedTableSchema),
                            originalEvent.meta());
                case DELETE:
                    return DataChangeEvent.deleteEvent(
                            routedTableId,
                            regenerateRecordData(
                                    originalEvent.before(), originalSchema, routedTableSchema),
                            originalEvent.meta());
                case REPLACE:
                    return DataChangeEvent.replaceEvent(
                            routedTableId,
                            regenerateRecordData(
                                    originalEvent.after(), originalSchema, routedTableSchema),
                            originalEvent.meta());
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unrecognized operation type \"%s\"", originalEvent.op()));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to fill null for empty columns", e);
        }
    }

    private RecordData regenerateRecordData(
            RecordData recordData, Schema originalSchema, Schema routedTableSchema) {
        // Regenerate record data
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>();
        for (Column column : routedTableSchema.getColumns()) {
            String columnName = column.getName();
            int columnIndex = originalSchema.getColumnNames().indexOf(columnName);
            if (columnIndex == -1) {
                fieldGetters.add(new NullFieldGetter());
            } else {
                RecordData.FieldGetter fieldGetter =
                        RecordData.createFieldGetter(
                                originalSchema.getColumn(columnName).get().getType(), columnIndex);
                // Check type compatibility
                if (originalSchema.getColumn(columnName).get().getType().equals(column.getType())) {
                    fieldGetters.add(fieldGetter);
                } else {
                    fieldGetters.add(new TypeCoercionFieldGetter(column.getType(), fieldGetter));
                }
            }
        }
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(
                        routedTableSchema.getColumnDataTypes().toArray(new DataType[0]));
        return recordDataGenerator.generate(
                fieldGetters.stream()
                        .map(fieldGetter -> fieldGetter.getFieldOrNull(recordData))
                        .toArray());
    }

    private List<TableId> getRoutedTables(TableId originalTableId) {
        return routes.stream()
                .filter(route -> route.f0.isMatch(originalTableId))
                .map(route -> resolveReplacement(originalTableId, route))
                .collect(Collectors.toList());
    }

    private TableId resolveReplacement(
            TableId originalTable, Tuple3<Selectors, String, String> route) {
        if (route.f2 != null) {
            return TableId.parse(route.f1.replace(route.f2, originalTable.getTableName()));
        }
        return TableId.parse(route.f1);
    }

    private void handleSchemaChangeEvent(TableId tableId, SchemaChangeEvent schemaChangeEvent)
            throws InterruptedException, TimeoutException {
        // The request will need to send a FlushEvent or block until flushing finished
        SchemaChangeResponse response = requestSchemaChange(tableId, schemaChangeEvent);
        if (!response.getSchemaChangeEvents().isEmpty()) {
            LOG.info(
                    "Sending the FlushEvent for table {} in subtask {}.",
                    tableId,
                    getRuntimeContext().getIndexOfThisSubtask());
            output.collect(new StreamRecord<>(new FlushEvent(tableId)));
            response.getSchemaChangeEvents().forEach(e -> output.collect(new StreamRecord<>(e)));
            // The request will block until flushing finished in each sink writer
            requestReleaseUpstream();
        }
    }

    private SchemaChangeResponse requestSchemaChange(
            TableId tableId, SchemaChangeEvent schemaChangeEvent) {
        return sendRequestToCoordinator(new SchemaChangeRequest(tableId, schemaChangeEvent));
    }

    private void requestReleaseUpstream() throws InterruptedException, TimeoutException {
        CoordinationResponse coordinationResponse =
                sendRequestToCoordinator(new ReleaseUpstreamRequest());
        long nextRpcTimeOutMillis = System.currentTimeMillis() + rpcTimeOutInMillis;
        while (coordinationResponse instanceof SchemaChangeProcessingResponse) {
            if (System.currentTimeMillis() < nextRpcTimeOutMillis) {
                Thread.sleep(1000);
                coordinationResponse = sendRequestToCoordinator(new SchemaChangeResultRequest());
            } else {
                throw new TimeoutException("TimeOut when requesting release upstream");
            }
        }
    }

    private <REQUEST extends CoordinationRequest, RESPONSE extends CoordinationResponse>
            RESPONSE sendRequestToCoordinator(REQUEST request) {
        try {
            CompletableFuture<CoordinationResponse> responseFuture =
                    toCoordinator.sendRequestToCoordinator(
                            getOperatorID(), new SerializedValue<>(request));
            return CoordinationResponseUtils.unwrap(responseFuture.get());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to send request to coordinator: " + request.toString(), e);
        }
    }

    private Schema getLatestSchema(TableId tableId) {
        try {
            Optional<Schema> optionalSchema = schemaEvolutionClient.getLatestSchema(tableId);
            if (!optionalSchema.isPresent()) {
                throw new IllegalStateException(
                        String.format("Schema doesn't exist for table \"%s\"", tableId));
            }
            return optionalSchema.get();
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format("Unable to get latest schema for table \"%s\"", tableId));
        }
    }

    private static class NullFieldGetter implements RecordData.FieldGetter {
        @Nullable
        @Override
        public Object getFieldOrNull(RecordData recordData) {
            return null;
        }
    }

    private static class TypeCoercionFieldGetter implements RecordData.FieldGetter {
        private final DataType destinationType;
        private final RecordData.FieldGetter originalFieldGetter;

        public TypeCoercionFieldGetter(
                DataType destinationType, RecordData.FieldGetter originalFieldGetter) {
            this.destinationType = destinationType;
            this.originalFieldGetter = originalFieldGetter;
        }

        @Nullable
        @Override
        public Object getFieldOrNull(RecordData recordData) {
            Object originalField = originalFieldGetter.getFieldOrNull(recordData);
            if (originalField == null) {
                return null;
            }
            if (destinationType.is(DataTypeRoot.BIGINT)) {
                if (originalField instanceof Byte) {
                    // TINYINT
                    return ((Byte) originalField).longValue();
                } else if (originalField instanceof Short) {
                    // SMALLINT
                    return ((Short) originalField).longValue();
                } else if (originalField instanceof Integer) {
                    // INT
                    return ((Integer) originalField).longValue();
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Cannot fit type \"%s\" into a BIGINT column. "
                                            + "Currently only TINYINT / SMALLINT / INT can be accepted by a BIGINT column",
                                    originalField.getClass()));
                }
            } else if (destinationType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
                if (originalField instanceof Float) {
                    // FLOAT
                    return ((Float) originalField).doubleValue();
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Cannot fit type \"%s\" into a DOUBLE column. "
                                            + "Currently only FLOAT can be accepted by a DOUBLE column",
                                    originalField.getClass()));
                }
            } else if (destinationType.is(DataTypeRoot.VARCHAR)) {
                if (originalField instanceof StringData) {
                    return originalField;
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Cannot fit type \"%s\" into a STRING column. "
                                            + "Currently only CHAR / VARCHAR can be accepted by a STRING column",
                                    originalField.getClass()));
                }
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Column type \"%s\" doesn't support type coercion",
                                destinationType));
            }
        }
    }
}
