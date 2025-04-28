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

package org.apache.flink.cdc.composer.flink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.testsource.factory.DistributedDataSourceFactory;
import org.apache.flink.cdc.composer.testsource.source.DistributedSourceOptions;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSink;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;

/** Integration test for {@link FlinkPipelineComposer} in parallelized schema evolution cases. */
@Timeout(value = 600, unit = java.util.concurrent.TimeUnit.SECONDS)
class FlinkParallelizedPipelineITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(FlinkParallelizedPipelineITCase.class);

    private static final int MAX_PARALLELISM = 4;
    private static final int UPSTREAM_TABLE_COUNT = 4;
    private static final List<RouteDef> ROUTING_RULES;

    static {
        ROUTING_RULES =
                IntStream.range(0, UPSTREAM_TABLE_COUNT)
                        .mapToObj(
                                i ->
                                        new RouteDef(
                                                "default_namespace_subtask_\\d.default_database.table_"
                                                        + i,
                                                "default_namespace.default_database.table_" + i,
                                                null,
                                                null))
                        .collect(Collectors.toList());
    }

    private static final List<String> SINGLE_PARALLELISM_REGULAR = loadReferenceFile("regular.txt");

    private static final List<String> SINGLE_PARALLELISM_DISTRIBUTED =
            loadReferenceFile("distributed.txt");

    private static final List<String> SINGLE_PARALLELISM_DISTRIBUTED_IGNORE =
            loadReferenceFile("distributed-ignore.txt");

    private static List<String> loadReferenceFile(String name) {
        final String refFile = String.format("ref-output/%s", name);
        final URL refFileUrl =
                FlinkParallelizedPipelineITCase.class.getClassLoader().getResource(refFile);
        Assertions.assertThat(refFileUrl).isNotNull();
        try {
            return Files.readAllLines(Paths.get(refFileUrl.toURI())).stream()
                    .map(String::trim)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Always use parent-first classloader for CDC classes.
    // The reason is that ValuesDatabase uses static field for holding data, we need to make sure
    // the class is loaded by AppClassloader so that we can verify data in the test case.
    private static final org.apache.flink.configuration.Configuration MINI_CLUSTER_CONFIG =
            new org.apache.flink.configuration.Configuration();

    static {
        MINI_CLUSTER_CONFIG.set(
                ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
    }

    /**
     * Use {@link MiniClusterExtension} to reduce the overhead of restarting the MiniCluster for
     * every test case.
     */
    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(MAX_PARALLELISM)
                            .setConfiguration(MINI_CLUSTER_CONFIG)
                            .build());

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();

    @BeforeEach
    void init() {
        // Take over STDOUT as we need to check the output of values sink
        System.setOut(new PrintStream(outCaptor));
        // Initialize in-memory database
        ValuesDatabase.clear();
    }

    @AfterEach
    void cleanup() {
        System.setOut(standardOut);
        LOG.debug(
                "NOTICE: This is a semi-fuzzy test. Please also check if value sink prints expected events:");
        LOG.debug("================================");
        LOG.debug(outCaptor.toString());
        LOG.debug("================================");
        outCaptor.reset();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17})
    void testRegularTablesSourceInMultipleParallelism(int repeatId) {
        LOG.info("#{} ================================", repeatId);
        try {
            runPipelineJob(
                    ValuesDataSink.SinkApi.SINK_V2,
                    MAX_PARALLELISM,
                    SourceTraits.REGULAR,
                    SchemaChangeBehavior.LENIENT);
        } catch (Exception e) {
            Assertions.fail("No need to continue.", e);
        }

        // Validate generated downstream schema

        for (int taskIdx = 0; taskIdx < MAX_PARALLELISM; taskIdx++) {
            for (int tableIdx = 0; tableIdx < UPSTREAM_TABLE_COUNT; tableIdx++) {
                Schema schema =
                        ValuesDatabase.getTableSchema(
                                TableId.tableId(
                                        "default_namespace_subtask_" + taskIdx,
                                        "default_database",
                                        "table_" + tableIdx));

                // The order of result schema is uncertain.
                Assertions.assertThat(schema.getColumns())
                        .containsExactlyInAnyOrder(
                                Column.physicalColumn("id", DataTypes.STRING()),
                                Column.physicalColumn("col_booleantype", DataTypes.BOOLEAN()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_booleantype",
                                        DataTypes.BOOLEAN()),
                                Column.physicalColumn("col_tinyinttype", DataTypes.TINYINT()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_tinyinttype",
                                        DataTypes.TINYINT()),
                                Column.physicalColumn("col_smallinttype", DataTypes.SMALLINT()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_smallinttype",
                                        DataTypes.SMALLINT()),
                                Column.physicalColumn("col_inttype", DataTypes.INT()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_inttype", DataTypes.INT()),
                                Column.physicalColumn("col_biginttype", DataTypes.BIGINT()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_biginttype",
                                        DataTypes.BIGINT()),
                                Column.physicalColumn("col_decimaltype", DataTypes.DECIMAL(17, 11)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_decimaltype",
                                        DataTypes.DECIMAL(17, 11)),
                                Column.physicalColumn("col_floattype", DataTypes.FLOAT()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_floattype", DataTypes.FLOAT()),
                                Column.physicalColumn("col_doubletype", DataTypes.DOUBLE()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_doubletype",
                                        DataTypes.DOUBLE()),
                                Column.physicalColumn("col_chartype", DataTypes.CHAR(17)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_chartype", DataTypes.CHAR(17)),
                                Column.physicalColumn("col_varchartype", DataTypes.VARCHAR(17)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_varchartype",
                                        DataTypes.VARCHAR(17)),
                                Column.physicalColumn("col_binarytype", DataTypes.BINARY(17)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_binarytype",
                                        DataTypes.BINARY(17)),
                                Column.physicalColumn("col_varbinarytype", DataTypes.VARBINARY(17)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_varbinarytype",
                                        DataTypes.VARBINARY(17)),
                                Column.physicalColumn("col_timetype", DataTypes.TIME(9)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_timetype", DataTypes.TIME(9)),
                                Column.physicalColumn("col_timestamptype", DataTypes.TIMESTAMP(9)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_timestamptype",
                                        DataTypes.TIMESTAMP(9)),
                                Column.physicalColumn(
                                        "col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_zonedtimestamptype",
                                        DataTypes.TIMESTAMP_TZ(9)),
                                Column.physicalColumn(
                                        "col_localzonedtimestamptype", DataTypes.TIMESTAMP_LTZ(9)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_localzonedtimestamptype",
                                        DataTypes.TIMESTAMP_LTZ(9)));
            }
        }

        String outputStr = outCaptor.toString();
        IntStream.range(0, MAX_PARALLELISM)
                .forEach(
                        subTaskId ->
                                IntStream.range(0, 168)
                                        .forEach(
                                                seqNum ->
                                                        Assertions.assertThat(outputStr)
                                                                .contains(
                                                                        String.format(
                                                                                "__$%d$%d$__",
                                                                                subTaskId,
                                                                                seqNum))));

        String[] dataLines = outputStr.split(System.lineSeparator());
        String[] expectedTokens = {
            "true",
            "17",
            "34",
            "68",
            "136",
            "272.0",
            "544.0",
            "1088.00000000000",
            "Alice",
            "Bob",
            "Q2ljYWRh",
            "RGVycmlkYQ==",
            "64800000",
            "2019-12-31T18:00",
            "2020-07-17T18:00",
            "1970-01-05T05:20:00.000123456+08:00"
        };

        Stream.of(expectedTokens)
                .forEach(
                        token ->
                                Assertions.assertThat(
                                                Stream.of(dataLines)
                                                        .filter(line -> line.contains(token))
                                                        .count())
                                        .as("Checking presence of %s", token)
                                        .isGreaterThanOrEqualTo(
                                                UPSTREAM_TABLE_COUNT * MAX_PARALLELISM));
    }

    private void runPipelineJob(
            ValuesDataSink.SinkApi sinkApi,
            int parallelism,
            SourceTraits traits,
            SchemaChangeBehavior exception)
            throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                DistributedSourceOptions.DISTRIBUTED_TABLES, traits == SourceTraits.DISTRIBUTED);
        sourceConfig.set(DistributedSourceOptions.TABLE_COUNT, UPSTREAM_TABLE_COUNT);
        SourceDef sourceDef =
                new SourceDef(
                        DistributedDataSourceFactory.IDENTIFIER,
                        "Distributed Source",
                        sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT, Duration.ofMinutes(1));
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, parallelism);
        pipelineConfig.set(PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, exception);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        traits == SourceTraits.MERGING ? ROUTING_RULES : Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);

        execution.execute();
    }

    /**
     *
     *
     * <ul>
     *   <li>DISTRIBUTED means data from one table might evolve independently in multiple
     *       partitions.
     *   <li>REGULAR means data from each table will not be presented in multiple partitions.
     *   <li>MERGING means data from multiple tables are forcefully merged into one partition.
     * </ul>
     */
    private enum SourceTraits {
        DISTRIBUTED,
        REGULAR,
        MERGING
    }
}
