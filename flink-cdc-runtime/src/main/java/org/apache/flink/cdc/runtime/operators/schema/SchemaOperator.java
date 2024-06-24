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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeFamily;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.utils.ChangeEventUtils;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import org.apache.flink.cdc.runtime.operators.schema.event.ApplyEvolvedSchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.ApplyUpstreamSchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.event.RefreshPendingListsRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.ReleaseUpstreamRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.ReleaseUpstreamResponse;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;

/**
 * The operator will evolve schemas in {@link SchemaRegistry} for incoming {@link
 * SchemaChangeEvent}s and block the stream for tables before their schema changes finish.
 */
@Internal
public class SchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SchemaOperator.class);
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);

    private final List<Tuple2<String, TableId>> routingRules;

    private transient List<Tuple2<Selectors, TableId>> routes;
    private transient TaskOperatorEventGateway toCoordinator;
    private transient SchemaEvolutionClient schemaEvolutionClient;
    private transient LoadingCache<TableId, Schema> upstreamSchema;
    private transient LoadingCache<TableId, Schema> evolvedSchema;
    private transient LoadingCache<TableId, Boolean> schemaDivergesMap;

    private final long rpcTimeOutInMillis;
    private final SchemaChangeBehavior schemaChangeBehavior;

    @VisibleForTesting
    public SchemaOperator(List<Tuple2<String, TableId>> routingRules) {
        this.routingRules = routingRules;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT.toMillis();
        this.schemaChangeBehavior = SchemaChangeBehavior.EVOLVE;
    }

    @VisibleForTesting
    public SchemaOperator(List<Tuple2<String, TableId>> routingRules, Duration rpcTimeOut) {
        this.routingRules = routingRules;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
        this.schemaChangeBehavior = SchemaChangeBehavior.EVOLVE;
    }

    public SchemaOperator(
            List<Tuple2<String, TableId>> routingRules,
            Duration rpcTimeOut,
            SchemaChangeBehavior schemaChangeBehavior) {
        this.routingRules = routingRules;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
        this.schemaChangeBehavior = schemaChangeBehavior;
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
                                tuple2 -> {
                                    String tableInclusions = tuple2.f0;
                                    TableId replaceBy = tuple2.f1;
                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple2<>(selectors, replaceBy);
                                })
                        .collect(Collectors.toList());
        schemaEvolutionClient = new SchemaEvolutionClient(toCoordinator, getOperatorID());
        evolvedSchema =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, Schema>() {
                                    @Override
                                    public Schema load(TableId tableId) {
                                        return getLatestEvolvedSchema(tableId);
                                    }
                                });
        upstreamSchema =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, Schema>() {
                                    @Override
                                    public Schema load(TableId tableId) throws Exception {
                                        return getLatestUpstreamSchema(tableId);
                                    }
                                });
        schemaDivergesMap =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, Boolean>() {
                                    @Override
                                    public Boolean load(TableId tableId) throws Exception {
                                        return checkSchemaDiverges(tableId);
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
            throws InterruptedException, TimeoutException {
        Event event = streamRecord.getValue();
        if (event instanceof SchemaChangeEvent) {
            processSchemaChangeEvents((SchemaChangeEvent) event);
        } else if (event instanceof DataChangeEvent) {
            processDataChangeEvents(streamRecord, (DataChangeEvent) event);
        } else {
            throw new RuntimeException("Unknown event type in Stream record: " + event);
        }
    }

    private void processSchemaChangeEvents(SchemaChangeEvent event)
            throws InterruptedException, TimeoutException {
        TableId tableId = event.tableId();
        LOG.info("Table {} received SchemaChangeEvent and start to be blocked.", tableId);
        handleSchemaChangeEvent(tableId, event);
        // Update caches
        upstreamSchema.put(tableId, getLatestUpstreamSchema(tableId));
        schemaDivergesMap.put(tableId, checkSchemaDiverges(tableId));

        List<TableId> optionalRoutedTable = getRoutedTables(tableId);
        if (!optionalRoutedTable.isEmpty()) {
            getRoutedTables(tableId)
                    .forEach(routed -> evolvedSchema.put(routed, getLatestEvolvedSchema(routed)));
        } else {
            evolvedSchema.put(tableId, getLatestEvolvedSchema(tableId));
        }
    }

    private void processDataChangeEvents(StreamRecord<Event> streamRecord, DataChangeEvent event) {
        TableId tableId = event.tableId();
        List<TableId> optionalRoutedTable = getRoutedTables(tableId);
        if (!optionalRoutedTable.isEmpty()) {
            optionalRoutedTable.forEach(evolvedTableId -> {
                output.collect(
                        new StreamRecord<>(
                                normalizeSchemaChangeEvents(event, evolvedTableId, false)));
            });
        } else if (Boolean.FALSE.equals(schemaDivergesMap.getIfPresent(tableId))) {
            output.collect(new StreamRecord<>(normalizeSchemaChangeEvents(event, true)));
        } else {
            output.collect(streamRecord);
        }
    }

    private DataChangeEvent normalizeSchemaChangeEvents(
            DataChangeEvent event, boolean tolerantMode) {
        return normalizeSchemaChangeEvents(event, event.tableId(), tolerantMode);
    }

    private DataChangeEvent normalizeSchemaChangeEvents(
            DataChangeEvent event, TableId renamedTableId, boolean tolerantMode) {
        try {
            Schema originalSchema = upstreamSchema.get(event.tableId());
            Schema evolvedTableSchema = evolvedSchema.get(renamedTableId);
            if (originalSchema.equals(evolvedTableSchema)) {
                return ChangeEventUtils.recreateDataChangeEvent(event, renamedTableId);
            }
            switch (event.op()) {
                case INSERT:
                    return DataChangeEvent.insertEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.after(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode),
                            event.meta());
                case UPDATE:
                    return DataChangeEvent.updateEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.before(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode),
                            regenerateRecordData(
                                    event.after(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode),
                            event.meta());
                case DELETE:
                    return DataChangeEvent.deleteEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.before(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode),
                            event.meta());
                case REPLACE:
                    return DataChangeEvent.replaceEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.after(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode),
                            event.meta());
                default:
                    throw new IllegalArgumentException(
                            String.format("Unrecognized operation type \"%s\"", event.op()));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to fill null for empty columns", e);
        }
    }

    private RecordData regenerateRecordData(
            RecordData recordData,
            Schema originalSchema,
            Schema routedTableSchema,
            boolean tolerantMode) {
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
                    fieldGetters.add(
                            new TypeCoercionFieldGetter(
                                    column.getType(), fieldGetter, tolerantMode));
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
                .map(route -> route.f1)
                .collect(Collectors.toList());
    }

    private void handleSchemaChangeEvent(TableId tableId, SchemaChangeEvent schemaChangeEvent)
            throws InterruptedException, TimeoutException {

        if (schemaChangeBehavior == SchemaChangeBehavior.EXCEPTION
                && schemaChangeEvent.getType() != SchemaChangeEventType.CREATE_TABLE) {
            // CreateTableEvent should be applied even in EXCEPTION mode
            throw new RuntimeException(
                    String.format(
                            "Refused to apply schema change event %s in EXCEPTION mode.",
                            schemaChangeEvent));
        }

        // The request will need to send a FlushEvent or block until flushing finished
        SchemaChangeResponse response = requestSchemaChange(tableId, schemaChangeEvent);

        if (!response.getSchemaChangeEvents().isEmpty()) {
            LOG.info(
                    "Sending the FlushEvent for table {} in subtask {}.",
                    tableId,
                    getRuntimeContext().getIndexOfThisSubtask());

            output.collect(new StreamRecord<>(new FlushEvent(tableId)));
            List<SchemaChangeEvent> expectedSchemaChangeEvents = response.getSchemaChangeEvents();
            // The request will block until flushing finished in each sink writer
            ReleaseUpstreamResponse schemaEvolveResponse = requestReleaseUpstream();
            List<SchemaChangeEvent> finishedSchemaChangeEvents =
                    schemaEvolveResponse.getFinishedSchemaChangeEvents();
            List<Tuple2<SchemaChangeEvent, Throwable>> failedSchemaChangeEvents =
                    schemaEvolveResponse.getFailedSchemaChangeEvents();
            List<SchemaChangeEvent> ignoredSchemaChangeEvents =
                    schemaEvolveResponse.getIgnoredSchemaChangeEvents();

            if (schemaChangeBehavior == SchemaChangeBehavior.EVOLVE
                    || schemaChangeBehavior == SchemaChangeBehavior.EXCEPTION) {
                if (schemaEvolveResponse.hasException()) {
                    throw new RuntimeException(
                            String.format(
                                    "Failed to apply schema change event %s.\nExceptions: %s",
                                    schemaChangeEvent,
                                    schemaEvolveResponse.getPrintableFailedSchemaChangeEvents()));
                }
            } else if (schemaChangeBehavior == SchemaChangeBehavior.TRY_EVOLVE
                    || schemaChangeBehavior == SchemaChangeBehavior.IGNORE) {
                if (schemaEvolveResponse.hasException()) {
                    schemaEvolveResponse
                            .getFailedSchemaChangeEvents()
                            .forEach(
                                    e ->
                                            LOG.warn(
                                                    "Failed to apply event {}, but keeps running in TRY_EVOLVE mode. Caused by: {}",
                                                    e.f0,
                                                    e.f1));
                }
            } else {
                throw new IllegalStateException(
                        "Unexpected schema change behavior: " + schemaChangeBehavior);
            }

            // Update evolved schema changes based on apply results
            requestApplyEvolvedSchemaChanges(tableId, finishedSchemaChangeEvents);
            finishedSchemaChangeEvents.forEach(e -> output.collect(new StreamRecord<>(e)));

            LOG.info(
                    "Applied schema change event {} to downstream. Among {} total evolved events, {} succeeded, {} failed, and {} ignored.",
                    schemaChangeEvent,
                    expectedSchemaChangeEvents.size(),
                    finishedSchemaChangeEvents.size(),
                    failedSchemaChangeEvents.size(),
                    ignoredSchemaChangeEvents.size());
        }
    }

    private SchemaChangeResponse requestSchemaChange(
            TableId tableId, SchemaChangeEvent schemaChangeEvent) {
        return sendRequestToCoordinator(new SchemaChangeRequest(tableId, schemaChangeEvent));
    }

    private void requestApplyUpstreamSchemaChanges(
            TableId tableId, SchemaChangeEvent schemaChangeEvent) {
        sendRequestToCoordinator(new ApplyUpstreamSchemaChangeRequest(tableId, schemaChangeEvent));
    }

    private void requestApplyEvolvedSchemaChanges(
            TableId tableId, List<SchemaChangeEvent> schemaChangeEvents) {
        sendRequestToCoordinator(new ApplyEvolvedSchemaChangeRequest(tableId, schemaChangeEvents));
    }

    private ReleaseUpstreamResponse requestReleaseUpstream()
            throws InterruptedException, TimeoutException {
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
        return ((ReleaseUpstreamResponse) coordinationResponse);
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

    private Schema getLatestEvolvedSchema(TableId tableId) {
        try {
            Optional<Schema> optionalSchema = schemaEvolutionClient.getLatestEvolvedSchema(tableId);
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

    private Schema getLatestUpstreamSchema(TableId tableId) {
        try {
            Optional<Schema> optionalSchema =
                    schemaEvolutionClient.getLatestUpstreamSchema(tableId);
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

    private Boolean checkSchemaDiverges(TableId tableId) {
        try {
            return getLatestEvolvedSchema(tableId).equals(getLatestUpstreamSchema(tableId));
        } catch (IllegalStateException e) {
            // schema fetch failed, regard it as diverged
            return true;
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
        private final boolean tolerantMode;

        public TypeCoercionFieldGetter(
                DataType destinationType,
                RecordData.FieldGetter originalFieldGetter,
                boolean tolerantMode) {
            this.destinationType = destinationType;
            this.originalFieldGetter = originalFieldGetter;
            this.tolerantMode = tolerantMode;
        }

        private Object fail(IllegalArgumentException e) throws IllegalArgumentException {
            if (tolerantMode) {
                return null;
            }
            throw e;
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
                    return fail(
                            new IllegalArgumentException(
                                    String.format(
                                            "Cannot fit type \"%s\" into a BIGINT column. "
                                                    + "Currently only TINYINT / SMALLINT / INT can be accepted by a BIGINT column",
                                            originalField.getClass())));
                }
            } else if (destinationType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
                if (originalField instanceof Float) {
                    // FLOAT
                    return ((Float) originalField).doubleValue();
                } else {
                    return fail(
                            new IllegalArgumentException(
                                    String.format(
                                            "Cannot fit type \"%s\" into a DOUBLE column. "
                                                    + "Currently only FLOAT can be accepted by a DOUBLE column",
                                            originalField.getClass())));
                }
            } else if (destinationType.is(DataTypeRoot.VARCHAR)) {
                if (originalField instanceof StringData) {
                    return originalField;
                } else {
                    return fail(
                            new IllegalArgumentException(
                                    String.format(
                                            "Cannot fit type \"%s\" into a STRING column. "
                                                    + "Currently only CHAR / VARCHAR can be accepted by a STRING column",
                                            originalField.getClass())));
                }
            } else {
                return fail(
                        new IllegalArgumentException(
                                String.format(
                                        "Column type \"%s\" doesn't support type coercion",
                                        destinationType)));
            }
        }
    }
}
