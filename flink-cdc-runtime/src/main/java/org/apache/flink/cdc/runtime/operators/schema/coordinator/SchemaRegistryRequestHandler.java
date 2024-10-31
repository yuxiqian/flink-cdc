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

package org.apache.flink.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventWithPreSchema;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.runtime.operators.schema.event.AbandonSchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.AbandonSchemaChangeResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.AskForSchemaChangePermissionRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.AskForSchemaChangePermissionResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeProcessingResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResultResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.WaitingForFlushResponse;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils.wrap;

/** A handler to deal with all requests and events for {@link SchemaRegistry}. */
@Internal
public class SchemaRegistryRequestHandler implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryRequestHandler.class);

    /** The {@link MetadataApplier} for every table. */
    private final MetadataApplier metadataApplier;
    /** All active sink writers. */
    private final Set<Integer> activeSinkWriters;
    /** Schema manager holding schema for all tables. */
    private final SchemaManager schemaManager;

    private final SchemaDerivation schemaDerivation;

    /**
     * Atomic flag indicating if current RequestHandler could accept more schema changes for now.
     */
    private volatile RequestStatus schemaChangeStatus;

    private final Object schemaChangeRequestLock;

    private volatile Throwable currentChangeException;
    private volatile List<SchemaChangeEvent> currentDerivedSchemaChangeEvents;
    private volatile List<SchemaChangeEvent> currentFinishedSchemaChanges;
    private volatile List<SchemaChangeEvent> currentIgnoredSchemaChanges;

    /** Sink writers which have sent flush success events for the request. */
    private final Set<Integer> flushedSinkWriters;

    /** Executor service to execute schema change. */
    private final ExecutorService schemaChangeThreadPool;

    private final SchemaChangeBehavior schemaChangeBehavior;

    private final OperatorCoordinator.Context context;

    private final AtomicLong currentSchemaChangeVersion;

    public SchemaRegistryRequestHandler(
            MetadataApplier metadataApplier,
            SchemaManager schemaManager,
            SchemaDerivation schemaDerivation,
            SchemaChangeBehavior schemaChangeBehavior,
            OperatorCoordinator.Context context) {
        this.metadataApplier = metadataApplier;
        this.schemaManager = schemaManager;
        this.schemaDerivation = schemaDerivation;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.context = context;

        this.activeSinkWriters = ConcurrentHashMap.newKeySet();
        this.flushedSinkWriters = ConcurrentHashMap.newKeySet();
        this.schemaChangeThreadPool = Executors.newSingleThreadExecutor();

        this.currentDerivedSchemaChangeEvents = new ArrayList<>();
        this.currentFinishedSchemaChanges = new ArrayList<>();
        this.currentIgnoredSchemaChanges = new ArrayList<>();

        this.schemaChangeStatus = RequestStatus.IDLE;
        this.schemaChangeRequestLock = new Object();

        this.currentSchemaChangeVersion = new AtomicLong(0);
    }

    /**
     * Handles the permission for starting a schema change request. Will not alter SchemaRegistry
     * internal state until received the following {@link SchemaChangeRequest}.
     *
     * @param request the received SchemaChangeRequest
     */
    public void handleAskForPermissionRequest(
            AskForSchemaChangePermissionRequest request,
            CompletableFuture<CoordinationResponse> response) {
        // We use requester subTask ID as the pending ticket, because there will be at most 1 schema
        // change requests simultaneously from each subTask
        int requestSubTaskId = request.getSubTaskId();

        synchronized (schemaChangeRequestLock) {
            // Make sure we handle the first request in the pending list to avoid out-of-order
            // waiting and blocks checkpointing mechanism.
            if (schemaChangeStatus == RequestStatus.IDLE) {
                LOG.info(
                        "SchemaChangeStatus switched from IDLE to WAITING_FOR_FLUSH, other requests will be blocked.");
                // This request has been accepted.
                schemaChangeStatus = RequestStatus.WAITING_FOR_FLUSH;
                response.complete(
                        wrap(
                                AskForSchemaChangePermissionResponse.accepted(
                                        currentSchemaChangeVersion.incrementAndGet())));
            } else {
                LOG.info(
                        "Schema Registry is busy processing a schema change request, could not handle request {} from subTask {} for now.",
                        request,
                        requestSubTaskId);
                response.complete(wrap(AskForSchemaChangePermissionResponse.rejected()));
            }
        }
    }

    /**
     * Handle the {@link SchemaChangeRequest} and wait for all sink subtasks flushing.
     *
     * @param request the received SchemaChangeRequest
     */
    public void handleSchemaChangeRequest(
            SchemaChangeRequest request, CompletableFuture<CoordinationResponse> response) {
        if (schemaChangeStatus.equals(RequestStatus.WAITING_FOR_FLUSH)) {
            LOG.info("Not all sink writers have finished flushing.");
            response.complete(wrap(new WaitingForFlushResponse()));
            return;
        } else if (schemaChangeStatus.equals(RequestStatus.APPLYING)) {
            LOG.info(
                    "Received enough FlushSuccessEvent and going to start evolving schema changes.");
        } else {
            throw new IllegalStateException(
                    "Illegal schemaChangeStatus: state should be WAITING_FOR_FLUSH or APPLYING when handlingSchemaChangeRequest, but "
                            + schemaChangeStatus);
        }

        SchemaChangeEvent event = request.getSchemaChangeEvent();

        // If this schema change event has been requested by another subTask, ignore it.
        if (schemaManager.isOriginalSchemaChangeEventRedundant(event)) {
            LOG.info("Event {} has been addressed before, ignoring it.", event);
            clearCurrentSchemaChangeRequest();
            LOG.info(
                    "SchemaChangeStatus switched from {} to IDLE for request {} due to duplicated request.",
                    schemaChangeStatus,
                    request);
            schemaChangeStatus = RequestStatus.IDLE;
            response.complete(wrap(SchemaChangeResponse.duplicate()));
            return;
        }

        schemaManager.applyOriginalSchemaChange(event);
        List<SchemaChangeEvent> derivedSchemaChangeEvents =
                calculateDerivedSchemaChangeEvents(request.getSchemaChangeEvent());

        // If this schema change event is filtered out by LENIENT mode or merging table
        // route strategies, ignore it.
        if (derivedSchemaChangeEvents.isEmpty()) {
            LOG.info("Event {} is omitted from sending to downstream, ignoring it.", event);
            clearCurrentSchemaChangeRequest();
            LOG.info(
                    "SchemaChangeStatus switched from WAITING_FOR_FLUSH to IDLE for request {} due to ignored request.",
                    request);
            schemaChangeStatus = RequestStatus.IDLE;
            response.complete(wrap(SchemaChangeResponse.ignored()));
            return;
        }

        // Backfill pre-schema info for sink applying
        derivedSchemaChangeEvents.forEach(
                e -> {
                    if (e instanceof SchemaChangeEventWithPreSchema) {
                        SchemaChangeEventWithPreSchema pe = (SchemaChangeEventWithPreSchema) e;
                        if (!pe.hasPreSchema()) {
                            schemaManager
                                    .getLatestEvolvedSchema(pe.tableId())
                                    .ifPresent(pe::fillPreSchema);
                        }
                    }
                });

        currentDerivedSchemaChangeEvents = new ArrayList<>(derivedSchemaChangeEvents);
        schemaChangeThreadPool.submit(() -> applySchemaChange(currentDerivedSchemaChangeEvents));
        response.complete(wrap(SchemaChangeResponse.evolved(derivedSchemaChangeEvents)));
    }

    /**
     * Abandon an ongoing {@link SchemaChangeRequest} and be ready for the next request.
     *
     * @param request the received SchemaChangeRequest
     */
    public void handleAbandonSchemaChangeRequest(
            AbandonSchemaChangeRequest request, CompletableFuture<CoordinationResponse> response) {
        // We use requester subTask ID as the pending ticket, because there will be at most 1 schema
        // change requests simultaneously from each subTask
        synchronized (schemaChangeRequestLock) {
            LOG.info("SchemaChangeStatus switched to IDLE, previous request has been abandoned.");
            clearCurrentSchemaChangeRequest();
            schemaChangeStatus = RequestStatus.IDLE;
            response.complete(wrap(new AbandonSchemaChangeResponse()));
        }
    }

    /**
     * Apply the schema change to the external system.
     *
     * @param derivedSchemaChangeEvents list of the schema changes
     */
    private void applySchemaChange(List<SchemaChangeEvent> derivedSchemaChangeEvents) {
        for (SchemaChangeEvent changeEvent : derivedSchemaChangeEvents) {
            if (changeEvent.getType() != SchemaChangeEventType.CREATE_TABLE) {
                if (schemaChangeBehavior == SchemaChangeBehavior.IGNORE) {
                    currentIgnoredSchemaChanges.add(changeEvent);
                    continue;
                }
            }
            if (!metadataApplier.acceptsSchemaEvolutionType(changeEvent.getType())) {
                LOG.info("Ignored schema change {}.", changeEvent);
                currentIgnoredSchemaChanges.add(changeEvent);
            } else {
                try {
                    metadataApplier.applySchemaChange(changeEvent);
                    LOG.info("Applied schema change {}.", changeEvent);
                    schemaManager.applyEvolvedSchemaChange(changeEvent);
                    currentFinishedSchemaChanges.add(changeEvent);
                } catch (Throwable t) {
                    LOG.error("Failed to apply schema change {}. Caused by: {}", changeEvent, t);
                    if (!shouldIgnoreException(t)) {
                        currentChangeException = t;
                        break;
                    } else {
                        LOG.warn(
                                "Failed to apply event {}, but keeps running in tolerant mode. Caused by: {}",
                                changeEvent,
                                t);
                    }
                }
            }
        }
        Preconditions.checkState(
                schemaChangeStatus == RequestStatus.APPLYING,
                "Illegal schemaChangeStatus state: should be APPLYING before applySchemaChange finishes, not "
                        + schemaChangeStatus);
        schemaChangeStatus = RequestStatus.FINISHED;
        LOG.info(
                "SchemaChangeStatus switched from APPLYING to FINISHED for request {}.",
                currentDerivedSchemaChangeEvents);
    }

    /**
     * Register a sink subtask.
     *
     * @param sinkSubtask the sink subtask to register
     */
    public void registerSinkWriter(int sinkSubtask) {
        LOG.info("Register sink subtask {}.", sinkSubtask);
        activeSinkWriters.add(sinkSubtask);
    }

    /**
     * Record flushed sink subtasks after receiving FlushSuccessEvent.
     *
     * @param tableId the subtask in SchemaOperator and table that the FlushEvent is about
     * @param sinkSubtask the sink subtask succeed flushing
     */
    public void flushSuccess(TableId tableId, int sinkSubtask, long versionCode, int parallelism) {
        long currentVersion = currentSchemaChangeVersion.get();
        if (versionCode < currentVersion) {
            LOG.warn(
                    "Schema registry received a stale flush success event (version {}), since current version is already {}. Ignoring it.",
                    versionCode,
                    currentVersion);
        } else if (versionCode > currentVersion) {
            throw new IllegalStateException(
                    String.format(
                            "Schema registry received a flush success event with newer version code %d. Current version: %d. This should never happen.",
                            versionCode, currentVersion));
        }
        flushedSinkWriters.add(sinkSubtask);
        if (activeSinkWriters.size() < parallelism) {
            LOG.info(
                    "Not all active sink writers have been registered. Current {}, expected {}.",
                    activeSinkWriters.size(),
                    parallelism);
            return;
        }
        if (flushedSinkWriters.equals(activeSinkWriters)) {

            // If current state isn't WAITING_FOR_FLUSH, it is likely that the schema change request
            // was abandoned before.
            if (schemaChangeStatus == RequestStatus.WAITING_FOR_FLUSH) {
                schemaChangeStatus = RequestStatus.APPLYING;
                LOG.info(
                        "All sink subtasks have flushed for table {}. State switched from WAITING_FOR_FLUSH to APPLYING.",
                        tableId.toString());
            }
        }
    }

    public void getSchemaChangeResult(CompletableFuture<CoordinationResponse> response) {
        Preconditions.checkState(
                schemaChangeStatus != RequestStatus.IDLE,
                "Illegal schemaChangeStatus: should not be IDLE before getting schema change request results.");
        if (schemaChangeStatus == RequestStatus.FINISHED) {
            schemaChangeStatus = RequestStatus.IDLE;
            LOG.info(
                    "SchemaChangeStatus switched from FINISHED to IDLE for request {}",
                    currentDerivedSchemaChangeEvents);

            // This request has been finished, return it and prepare for the next request
            List<SchemaChangeEvent> finishedEvents = clearCurrentSchemaChangeRequest();
            SchemaChangeResultResponse resultResponse =
                    new SchemaChangeResultResponse(finishedEvents);
            response.complete(wrap(resultResponse));
        } else {
            // Still working on schema change request, waiting it
            response.complete(wrap(new SchemaChangeProcessingResponse()));
        }
    }

    @Override
    public void close() throws IOException {
        if (schemaChangeThreadPool != null) {
            schemaChangeThreadPool.shutdown();
        }
    }

    private List<SchemaChangeEvent> calculateDerivedSchemaChangeEvents(SchemaChangeEvent event) {
        if (SchemaChangeBehavior.LENIENT.equals(schemaChangeBehavior)) {
            return schemaDerivation.applySchemaChange(event).stream()
                    .flatMap(evt -> lenientizeSchemaChangeEvent(evt).stream())
                    .collect(Collectors.toList());
        } else {
            return schemaDerivation.applySchemaChange(event);
        }
    }

    private List<SchemaChangeEvent> lenientizeSchemaChangeEvent(SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            return Collections.singletonList(event);
        }
        TableId tableId = event.tableId();
        Schema evolvedSchema =
                schemaManager
                        .getLatestEvolvedSchema(tableId)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Evolved schema does not exist, not ready for schema change event "
                                                        + event));
        switch (event.getType()) {
            case ADD_COLUMN:
                {
                    AddColumnEvent addColumnEvent = (AddColumnEvent) event;
                    return Collections.singletonList(
                            new AddColumnEvent(
                                    tableId,
                                    addColumnEvent.getAddedColumns().stream()
                                            .map(
                                                    col ->
                                                            new AddColumnEvent.ColumnWithPosition(
                                                                    Column.physicalColumn(
                                                                            col.getAddColumn()
                                                                                    .getName(),
                                                                            col.getAddColumn()
                                                                                    .getType()
                                                                                    .nullable(),
                                                                            col.getAddColumn()
                                                                                    .getComment())))
                                            .collect(Collectors.toList())));
                }
            case DROP_COLUMN:
                {
                    DropColumnEvent dropColumnEvent = (DropColumnEvent) event;
                    Map<String, DataType> convertNullableColumns =
                            dropColumnEvent.getDroppedColumnNames().stream()
                                    .map(evolvedSchema::getColumn)
                                    .flatMap(e -> e.map(Stream::of).orElse(Stream.empty()))
                                    .filter(col -> !col.getType().isNullable())
                                    .collect(
                                            Collectors.toMap(
                                                    Column::getName,
                                                    column -> column.getType().nullable()));

                    if (convertNullableColumns.isEmpty()) {
                        return Collections.emptyList();
                    } else {
                        return Collections.singletonList(
                                new AlterColumnTypeEvent(tableId, convertNullableColumns));
                    }
                }
            case RENAME_COLUMN:
                {
                    RenameColumnEvent renameColumnEvent = (RenameColumnEvent) event;
                    List<AddColumnEvent.ColumnWithPosition> appendColumns = new ArrayList<>();
                    Map<String, DataType> convertNullableColumns = new HashMap<>();
                    renameColumnEvent
                            .getNameMapping()
                            .forEach(
                                    (key, value) -> {
                                        Column column =
                                                evolvedSchema
                                                        .getColumn(key)
                                                        .orElseThrow(
                                                                () ->
                                                                        new IllegalArgumentException(
                                                                                "Non-existed column "
                                                                                        + key
                                                                                        + " in evolved schema."));
                                        if (!column.getType().isNullable()) {
                                            // It's a not-nullable column, we need to cast it to
                                            // nullable first
                                            convertNullableColumns.put(
                                                    key, column.getType().nullable());
                                        }
                                        appendColumns.add(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                value,
                                                                column.getType().nullable(),
                                                                column.getComment())));
                                    });

                    List<SchemaChangeEvent> events = new ArrayList<>();
                    events.add(new AddColumnEvent(tableId, appendColumns));
                    if (!convertNullableColumns.isEmpty()) {
                        events.add(new AlterColumnTypeEvent(tableId, convertNullableColumns));
                    }
                    return events;
                }
            default:
                return Collections.singletonList(event);
        }
    }

    private boolean shouldIgnoreException(Throwable throwable) {
        // In IGNORE mode, will never try to apply schema change events
        // In EVOLVE and LENIENT mode, such failure will not be tolerated
        // In EXCEPTION mode, an exception will be thrown once captured
        return (throwable instanceof UnsupportedSchemaChangeEventException)
                && (schemaChangeBehavior == SchemaChangeBehavior.TRY_EVOLVE);
    }

    private List<SchemaChangeEvent> clearCurrentSchemaChangeRequest() {
        if (currentChangeException != null) {
            context.failJob(
                    new RuntimeException("Failed to apply schema change.", currentChangeException));
        }
        List<SchemaChangeEvent> finishedSchemaChanges =
                new ArrayList<>(currentFinishedSchemaChanges);
        flushedSinkWriters.clear();
        currentDerivedSchemaChangeEvents.clear();
        currentFinishedSchemaChanges.clear();
        currentIgnoredSchemaChanges.clear();
        currentChangeException = null;
        return finishedSchemaChanges;
    }

    // Schema change event state could transfer in the following way:
    //
    //      -------- B --------
    //      |                 |
    //      v                 |
    //  --------           ---------------------
    //  | IDLE | --- A --> | WAITING_FOR_FLUSH |
    //  --------           ---------------------
    //     ^                        |
    //      E                       C
    //       \                      v
    //  ------------          ------------
    //  | FINISHED | <-- D -- | APPLYING |
    //  ------------          ------------
    //
    //  A: When a request came to an idling request handler.
    //  B: When current request is duplicate or ignored by LENIENT / routed table merging
    // strategies, or FlushEvent got blocked and the request is abandoned.
    //  C: When schema registry collected enough flush success events, and actually started to apply
    // schema changes.
    //  D: When schema change application finishes (successfully or with exceptions)
    //  E: When current schema change request result has been retrieved by SchemaOperator, and ready
    // for the next request.
    private enum RequestStatus {
        IDLE,
        WAITING_FOR_FLUSH,
        APPLYING,
        FINISHED
    }
}
