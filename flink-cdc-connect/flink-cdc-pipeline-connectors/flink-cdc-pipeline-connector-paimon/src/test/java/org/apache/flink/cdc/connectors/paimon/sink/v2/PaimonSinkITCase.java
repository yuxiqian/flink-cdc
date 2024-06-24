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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonMetadataApplier;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommitRequestImpl;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.stream.Collectors;

/** An ITCase for {@link PaimonWriter} and {@link PaimonCommitter}. */
public class PaimonSinkITCase {

    @TempDir public static java.nio.file.Path temporaryFolder;

    private Options catalogOptions;

    private TableEnvironment tEnv;

    private String warehouse;

    private TableId table1;

    private BinaryRecordDataGenerator generator;

    public static final String TEST_DATABASE = "test";
    private static final String HADOOP_CONF_DIR =
            Objects.requireNonNull(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResource("hadoop-conf-dir"))
                    .getPath();

    private static final String HIVE_CONF_DIR =
            Objects.requireNonNull(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResource("hive-conf-dir"))
                    .getPath();

    private void initialize(String metastore)
            throws Catalog.DatabaseNotEmptyException, Catalog.DatabaseNotExistException {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        warehouse = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions = new Options();
        catalogOptions.setString("metastore", metastore);
        catalogOptions.setString("warehouse", warehouse);
        table1 = TableId.tableId("test", "table1");
        if ("hive".equals(metastore)) {
            catalogOptions.setString("hadoop-conf-dir", HADOOP_CONF_DIR);
            catalogOptions.setString("hive-conf-dir", HIVE_CONF_DIR);
            tEnv.executeSql(
                    String.format(
                            "CREATE CATALOG paimon_catalog WITH ("
                                    + "'type'='paimon', "
                                    + "'warehouse'='%s', "
                                    + "'metastore'='hive', "
                                    + "'hadoop-conf-dir'='%s', "
                                    + "'hive-conf-dir'='%s' "
                                    + ")",
                            warehouse, HADOOP_CONF_DIR, HIVE_CONF_DIR));
        } else {
            tEnv.executeSql(
                    String.format(
                            "CREATE CATALOG paimon_catalog WITH ('type'='paimon', 'warehouse'='%s')",
                            warehouse));
        }
        FlinkCatalogFactory.createPaimonCatalog(catalogOptions)
                .dropDatabase(TEST_DATABASE, true, true);
    }

    private List<Event> createTestEvents() throws SchemaEvolveException {
        List<Event> testEvents = new ArrayList<>();
        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        testEvents.add(createTableEvent);
        PaimonMetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        metadataApplier.applySchemaChange(createTableEvent);

        generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        // insert
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        testEvents.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        testEvents.add(insertEvent2);
        return testEvents;
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    public void testSinkWithDataChange(String metastore)
            throws IOException, InterruptedException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException, SchemaEvolveException {
        initialize(metastore);
        PaimonSink<Event> paimonSink =
                new PaimonSink<>(
                        catalogOptions, new PaimonRecordEventSerializer(ZoneId.systemDefault()));
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();

        // insert
        for (Event event : createTestEvents()) {
            writer.write(event, null);
        }
        writer.flush(false);
        Collection<Committer.CommitRequest<MultiTableCommittable>> commitRequests =
                writer.prepareCommit().stream()
                        .map(MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        committer.commit(commitRequests);
        List<Row> result = new ArrayList<>();
        tEnv.sqlQuery("select * from paimon_catalog.test.table1")
                .execute()
                .collect()
                .forEachRemaining(result::add);
        Assertions.assertEquals(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "1", "1"), Row.ofKind(RowKind.INSERT, "2", "2")),
                result);

        // delete
        Event event =
                DataChangeEvent.deleteEvent(
                        TableId.tableId("test", "table1"),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        writer.write(event, null);
        writer.flush(false);
        commitRequests =
                writer.prepareCommit().stream()
                        .map(MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        committer.commit(commitRequests);
        result = new ArrayList<>();
        tEnv.sqlQuery("select * from paimon_catalog.test.table1")
                .execute()
                .collect()
                .forEachRemaining(result::add);
        Assertions.assertEquals(
                Collections.singletonList(Row.ofKind(RowKind.INSERT, "2", "2")), result);

        // update
        event =
                DataChangeEvent.updateEvent(
                        TableId.tableId("test", "table1"),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("x")
                                }));
        writer.write(event, null);
        writer.flush(false);
        commitRequests =
                writer.prepareCommit().stream()
                        .map(MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        committer.commit(commitRequests);
        result = new ArrayList<>();
        tEnv.sqlQuery("select * from paimon_catalog.test.table1")
                .execute()
                .collect()
                .forEachRemaining(result::add);
        Assertions.assertEquals(
                Collections.singletonList(Row.ofKind(RowKind.INSERT, "2", "x")), result);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    public void testSinkWithSchemaChange(String metastore)
            throws IOException, InterruptedException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException, SchemaEvolveException {
        initialize(metastore);
        PaimonSink<Event> paimonSink =
                new PaimonSink(
                        catalogOptions, new PaimonRecordEventSerializer(ZoneId.systemDefault()));
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();

        // 1. receive only DataChangeEvents during one checkpoint
        for (Event event : createTestEvents()) {
            writer.write(event, null);
        }
        writer.flush(false);
        Collection<Committer.CommitRequest<MultiTableCommittable>> commitRequests =
                writer.prepareCommit().stream()
                        .map(MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        committer.commit(commitRequests);
        List<Row> result = new ArrayList<>();
        tEnv.sqlQuery("select * from paimon_catalog.test.table1")
                .execute()
                .collect()
                .forEachRemaining(result::add);
        Assertions.assertEquals(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "1", "1"), Row.ofKind(RowKind.INSERT, "2", "2")),
                result);

        // 2. receive DataChangeEvents and SchemaChangeEvents during one checkpoint
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("3")
                                }));
        writer.write(insertEvent3, null);
        writer.flush(false);

        // add column
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", DataTypes.STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(table1, Collections.singletonList(columnWithPosition));
        PaimonMetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        writer.write(addColumnEvent, null);
        metadataApplier.applySchemaChange(addColumnEvent);
        generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()));
        DataChangeEvent insertEvent4 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("4"),
                                    BinaryStringData.fromString("4"),
                                    BinaryStringData.fromString("4")
                                }));
        writer.write(insertEvent4, null);
        writer.flush(false);
        commitRequests =
                writer.prepareCommit().stream()
                        .map(MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        committer.commit(commitRequests);
        result = new ArrayList<>();
        tEnv.sqlQuery("select * from paimon_catalog.test.table1")
                .execute()
                .collect()
                .forEachRemaining(result::add);
        Assertions.assertEquals(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "1", "1", null),
                        Row.ofKind(RowKind.INSERT, "2", "2", null),
                        Row.ofKind(RowKind.INSERT, "3", "3", null),
                        Row.ofKind(RowKind.INSERT, "4", "4", "4")),
                result);

        // 2. receive DataChangeEvents and SchemaChangeEvents during one checkpoint
        DataChangeEvent insertEvent5 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("5"),
                                    BinaryStringData.fromString("5"),
                                    BinaryStringData.fromString("5")
                                }));
        writer.write(insertEvent5, null);
        writer.flush(false);
        // drop column
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(table1, Collections.singletonList("col2"));
        metadataApplier.applySchemaChange(dropColumnEvent);
        writer.write(dropColumnEvent, null);
        generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        DataChangeEvent insertEvent6 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("6"),
                                    BinaryStringData.fromString("6")
                                }));
        writer.write(insertEvent6, null);
        writer.flush(false);
        commitRequests =
                writer.prepareCommit().stream()
                        .map(MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        committer.commit(commitRequests);
        result = new ArrayList<>();
        tEnv.sqlQuery("select * from paimon_catalog.test.table1")
                .execute()
                .collect()
                .forEachRemaining(result::add);
        Assertions.assertEquals(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "1", null),
                        Row.ofKind(RowKind.INSERT, "2", null),
                        Row.ofKind(RowKind.INSERT, "3", null),
                        Row.ofKind(RowKind.INSERT, "4", "4"),
                        Row.ofKind(RowKind.INSERT, "5", "5"),
                        Row.ofKind(RowKind.INSERT, "6", "6")),
                result);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    public void testSinkWithMultiTables(String metastore)
            throws IOException, InterruptedException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException, SchemaEvolveException {
        initialize(metastore);
        PaimonSink<Event> paimonSink =
                new PaimonSink<>(
                        catalogOptions, new PaimonRecordEventSerializer(ZoneId.systemDefault()));
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();
        List<Event> testEvents = createTestEvents();
        // create table
        TableId table2 = TableId.tableId("test", "table2");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table2, schema);
        testEvents.add(createTableEvent);
        PaimonMetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        metadataApplier.applySchemaChange(createTableEvent);
        // insert
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table2,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        testEvents.add(insertEvent1);

        // insert
        for (Event event : testEvents) {
            writer.write(event, null);
        }
        writer.flush(false);
        Collection<Committer.CommitRequest<MultiTableCommittable>> commitRequests =
                writer.prepareCommit().stream()
                        .map(MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        committer.commit(commitRequests);
        List<Row> result = new ArrayList<>();
        tEnv.sqlQuery("select * from paimon_catalog.test.table1")
                .execute()
                .collect()
                .forEachRemaining(result::add);
        Assertions.assertEquals(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "1", "1"), Row.ofKind(RowKind.INSERT, "2", "2")),
                result);
        result = new ArrayList<>();
        tEnv.sqlQuery("select * from paimon_catalog.test.table2")
                .execute()
                .collect()
                .forEachRemaining(result::add);
        Assertions.assertEquals(
                Collections.singletonList(Row.ofKind(RowKind.INSERT, "1", "1")), result);
    }

    private static class MockCommitRequestImpl<CommT> extends CommitRequestImpl<CommT> {

        protected MockCommitRequestImpl(CommT committable) {
            super(committable);
        }
    }

    private static class MockInitContext
            implements Sink.InitContext, SerializationSchema.InitializationContext {

        private MockInitContext() {}

        public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
        }

        public MailboxExecutor getMailboxExecutor() {
            return null;
        }

        public ProcessingTimeService getProcessingTimeService() {
            return null;
        }

        public int getSubtaskId() {
            return 0;
        }

        public int getNumberOfParallelSubtasks() {
            return 0;
        }

        public int getAttemptNumber() {
            return 0;
        }

        public SinkWriterMetricGroup metricGroup() {
            return null;
        }

        public MetricGroup getMetricGroup() {
            return null;
        }

        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return this;
        }

        public boolean isObjectReuseEnabled() {
            return false;
        }

        public <IN> TypeSerializer<IN> createInputSerializer() {
            return null;
        }

        public JobID getJobId() {
            return null;
        }
    }
}
