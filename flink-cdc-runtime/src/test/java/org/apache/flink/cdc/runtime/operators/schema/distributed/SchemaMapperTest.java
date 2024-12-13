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

package org.apache.flink.cdc.runtime.operators.schema.distributed;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaTestBase;
import org.apache.flink.cdc.runtime.testutils.operators.DistributedEventOperatorTestHarness;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.BiConsumerWithException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.Supplier;

/** Unit test cases for {@link SchemaOperator}. */
public class SchemaMapperTest extends SchemaTestBase {
    private static final TableId TABLE_ID = TableId.parse("foo.bar.baz");
    private static final Schema INITIAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("name", DataTypes.VARCHAR(128))
                    .physicalColumn("age", DataTypes.FLOAT())
                    .physicalColumn("notes", DataTypes.STRING().notNull())
                    .build();

    @Test
    void testSchemaEvolution() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "id")));
        AddColumnEvent addColumnEventAtLast =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.LAST,
                                        null)));

        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("notes", "footnotes"));
        AddColumnEvent appendRenamedColumnAtLast =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("footnotes", DataTypes.STRING()),
                                        AddColumnEvent.ColumnPosition.LAST,
                                        null)));
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(
                        TABLE_ID, Collections.singletonMap("age", DataTypes.DOUBLE()));
        AlterColumnTypeEvent alterColumnTypeEventWithBackfill =
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("age", DataTypes.DOUBLE()),
                        Collections.singletonMap("age", DataTypes.FLOAT()));

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(TABLE_ID, Collections.singletonList("footnotes"));
        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(TABLE_ID);
        DropTableEvent dropTableEvent = new DropTableEvent(TABLE_ID);

        Assertions.assertThat(
                        runInHarness(
                                () ->
                                        new SchemaOperator(
                                                ROUTING_RULES, Duration.ofMinutes(3), "UTC"),
                                (op) ->
                                        new DistributedEventOperatorTestHarness<>(
                                                op,
                                                20,
                                                Duration.ofSeconds(3),
                                                Duration.ofMinutes(3)),
                                (operator, harness) -> {

                                    // Create a Table
                                    operator.processElement(wrap(createTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "ISFS", 1, "Alice", 17.1828f,
                                                            "Hello")));

                                    // Add a Column
                                    operator.processElement(wrap(addColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSFS",
                                                            2,
                                                            false,
                                                            "Bob",
                                                            31.415926f,
                                                            "Bye-bye")));

                                    // Rename a Column
                                    operator.processElement(wrap(renameColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSFS", 3, true, "Cicada",
                                                            123.456f, "Ok")));

                                    // Alter a Column's Type
                                    operator.processElement(wrap(alterColumnTypeEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSDS",
                                                            4,
                                                            false,
                                                            "Derrida",
                                                            7.81876754837,
                                                            "Nah")));

                                    // Drop a column
                                    operator.processElement(wrap(dropColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 5, true, "Eve",
                                                            1.414)));

                                    // Truncate a table
                                    operator.processElement(wrap(truncateTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 6, false, "Ferris",
                                                            0.001)));

                                    // Drop a table
                                    operator.processElement(wrap(dropTableEvent));
                                }))
                .map(StreamRecord::getValue)
                .containsExactly(
                        FlushEvent.ofAll(),
                        createTableEvent,
                        genInsert(TABLE_ID, "ISFS", 1, "Alice", 17.1828f, "Hello"),
                        FlushEvent.ofAll(),
                        addColumnEventAtLast,
                        genInsert(TABLE_ID, "ISFSB", 2, "Bob", 31.415926f, "Bye-bye", false),
                        FlushEvent.ofAll(),
                        appendRenamedColumnAtLast,
                        genInsert(TABLE_ID, "ISFSBS", 3, "Cicada", 123.456f, null, true, "Ok"),
                        FlushEvent.ofAll(),
                        alterColumnTypeEventWithBackfill,
                        genInsert(
                                TABLE_ID,
                                "ISDSBS",
                                4,
                                "Derrida",
                                7.81876754837,
                                null,
                                false,
                                "Nah"),
                        FlushEvent.ofAll(),
                        genInsert(TABLE_ID, "ISDSBS", 5, "Eve", 1.414, null, true, null),
                        FlushEvent.ofAll(),
                        genInsert(TABLE_ID, "ISDSBS", 6, "Ferris", 0.001, null, false, null),
                        FlushEvent.ofAll());
    }

    protected static <OP extends AbstractStreamOperator<E>, E extends Event, T extends Throwable>
            LinkedList<StreamRecord<E>> runInHarness(
                    Supplier<OP> opCreator,
                    Function<OP, DistributedEventOperatorTestHarness<OP, E>> harnessCreator,
                    BiConsumerWithException<OP, DistributedEventOperatorTestHarness<OP, E>, T>
                            closure)
                    throws T, Exception {
        OP operator = opCreator.get();
        try (DistributedEventOperatorTestHarness<OP, E> harness = harnessCreator.apply(operator)) {
            harness.open();
            closure.accept(operator, harness);
            return harness.getOutputRecords();
        }
    }
}
