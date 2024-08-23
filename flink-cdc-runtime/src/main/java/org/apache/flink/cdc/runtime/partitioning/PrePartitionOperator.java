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

package org.apache.flink.cdc.runtime.partitioning;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

/** Operator for processing events from {@link SchemaOperator} before {@link EventPartitioner}. */
@Internal
public class PrePartitionOperator extends AbstractStreamOperator<PartitioningEvent>
        implements OneInputStreamOperator<Event, PartitioningEvent>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);

    private final OperatorID schemaOperatorId;
    private final int downstreamParallelism;
    private final HashFunctionProvider<DataChangeEvent> hashFunctionProvider;

    private transient SchemaEvolutionClient schemaEvolutionClient;
    private transient LoadingCache<TableId, HashFunction<DataChangeEvent>> cachedHashFunctions;

    public PrePartitionOperator(
            OperatorID schemaOperatorId,
            int downstreamParallelism,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.schemaOperatorId = schemaOperatorId;
        this.downstreamParallelism = downstreamParallelism;
        this.hashFunctionProvider = hashFunctionProvider;
    }

    @Override
    public void open() throws Exception {
        super.open();
        TaskOperatorEventGateway toCoordinator =
                getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway();
        schemaEvolutionClient = new SchemaEvolutionClient(toCoordinator, schemaOperatorId);
        cachedHashFunctions = createCache();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            // Update hash function
            TableId tableId = ((SchemaChangeEvent) event).tableId();
            cachedHashFunctions.put(tableId, recreateHashFunction(tableId));
            // Broadcast SchemaChangeEvent
            broadcastEvent(event);
        } else if (event instanceof FlushEvent) {
            // Broadcast FlushEvent
            LOG.info(
                    ">>> PrePartition[{}] received a FlushEvent for {}. Going to broadcast it...",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    ((FlushEvent) event).getTableId());
            broadcastEvent(event);
        } else if (event instanceof DataChangeEvent) {
            // Partition DataChangeEvent by table ID and primary keys
            partitionBy(((DataChangeEvent) event));
        }
    }

    private void partitionBy(DataChangeEvent dataChangeEvent) throws Exception {
        output.collect(
                new StreamRecord<>(
                        new PartitioningEvent(
                                dataChangeEvent,
                                cachedHashFunctions
                                                .get(dataChangeEvent.tableId())
                                                .hashcode(dataChangeEvent)
                                        % downstreamParallelism)));
    }

    private void broadcastEvent(Event toBroadcast) {
        LOG.info(
                ">>> PrePartition[{}] current downstreamParallelism: {}",
                getRuntimeContext().getIndexOfThisSubtask(),
                downstreamParallelism);
        for (int i = 0; i < downstreamParallelism; i++) {
            LOG.info(
                    ">>> PrePartition[{}] is broadcasting event: {}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    new PartitioningEvent(toBroadcast, i));
            output.collect(new StreamRecord<>(new PartitioningEvent(toBroadcast, i)));
        }
    }

    private Schema loadLatestSchemaFromRegistry(TableId tableId) {
        Optional<Schema> schema;
        try {
            schema = schemaEvolutionClient.getLatestEvolvedSchema(tableId);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to request latest schema for table \"%s\"", tableId), e);
        }
        if (!schema.isPresent()) {
            throw new IllegalStateException(
                    String.format(
                            "Schema is never registered or outdated for table \"%s\"", tableId));
        }
        return schema.get();
    }

    private HashFunction<DataChangeEvent> recreateHashFunction(TableId tableId) {
        return hashFunctionProvider.getHashFunction(tableId, loadLatestSchemaFromRegistry(tableId));
    }

    private LoadingCache<TableId, HashFunction<DataChangeEvent>> createCache() {
        return CacheBuilder.newBuilder()
                .expireAfterAccess(CACHE_EXPIRE_DURATION)
                .build(
                        new CacheLoader<TableId, HashFunction<DataChangeEvent>>() {
                            @Override
                            public HashFunction<DataChangeEvent> load(TableId key) {
                                return recreateHashFunction(key);
                            }
                        });
    }
}
