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

package org.apache.flink.cdc.runtime.serializer.event;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Unit test for {@link PartitioningEventSerializer}. */
class PartitioningEventSerializerTest extends SerializerTestBase<PartitioningEvent> {

    @Override
    protected TypeSerializer<PartitioningEvent> createSerializer() {
        return PartitioningEventSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<PartitioningEvent> getTypeClass() {
        return PartitioningEvent.class;
    }

    @Override
    protected PartitioningEvent[] getTestData() {
        Event[] flushEvents =
                new Event[] {
                    new FlushEvent(TableId.tableId("table"), 1L),
                    new FlushEvent(TableId.tableId("schema", "table"), 2L),
                    new FlushEvent(TableId.tableId("namespace", "schema", "table"), 3L)
                };
        Event[] dataChangeEvents = new DataChangeEventSerializerTest().getTestData();
        Event[] schemaChangeEvents = new SchemaChangeEventSerializerTest().getTestData();

        List<PartitioningEvent> partitioningEvents = new ArrayList<>();

        partitioningEvents.addAll(
                Arrays.stream(flushEvents)
                        .map(event -> new PartitioningEvent(event, 1))
                        .collect(Collectors.toList()));
        partitioningEvents.addAll(
                Arrays.stream(dataChangeEvents)
                        .map(event -> new PartitioningEvent(event, 2))
                        .collect(Collectors.toList()));
        partitioningEvents.addAll(
                Arrays.stream(schemaChangeEvents)
                        .map(event -> new PartitioningEvent(event, 3))
                        .collect(Collectors.toList()));

        return partitioningEvents.toArray(new PartitioningEvent[0]);
    }
}
