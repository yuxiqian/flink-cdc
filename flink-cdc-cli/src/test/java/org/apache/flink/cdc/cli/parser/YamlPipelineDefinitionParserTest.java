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

package org.apache.flink.cdc.cli.parser;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.definition.UdfDef;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.io.Resources;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.PIPELINE_LOCAL_TIME_ZONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Unit test for {@link org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser}. */
class YamlPipelineDefinitionParserTest {

    @Test
    void testParsingFullDefinition() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-full.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(fullDef);
    }

    @Test
    void testParsingNecessaryOnlyDefinition() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-with-optional.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(defWithOptional);
    }

    @Test
    void testMinimizedDefinition() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(minimizedDef);
    }

    @Test
    void testOverridingGlobalConfig() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-full.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                parser.parse(
                        Paths.get(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put("parallelism", "1")
                                        .build()));
        assertThat(pipelineDef).isEqualTo(fullDefWithGlobalConf);
    }

    @Test
    void testEvaluateDefaultLocalTimeZone() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE))
                .isNotEqualTo(PIPELINE_LOCAL_TIME_ZONE.defaultValue());
    }

    @Test
    void testEvaluateDefaultRpcTimeOut() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                parser.parse(
                        Paths.get(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put(
                                                PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT
                                                        .key(),
                                                "1h")
                                        .build()));
        assertThat(
                        pipelineDef
                                .getConfig()
                                .get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT))
                .isEqualTo(Duration.ofSeconds(60 * 60));
    }

    @Test
    void testValidTimeZone() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                parser.parse(
                        Paths.get(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put(PIPELINE_LOCAL_TIME_ZONE.key(), "Asia/Shanghai")
                                        .build()));
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE))
                .isEqualTo("Asia/Shanghai");

        pipelineDef =
                parser.parse(
                        Paths.get(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put(PIPELINE_LOCAL_TIME_ZONE.key(), "GMT+08:00")
                                        .build()));
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE)).isEqualTo("GMT+08:00");

        pipelineDef =
                parser.parse(
                        Paths.get(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put(PIPELINE_LOCAL_TIME_ZONE.key(), "UTC")
                                        .build()));
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE)).isEqualTo("UTC");
    }

    @Test
    void testInvalidTimeZone() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        assertThatThrownBy(
                        () ->
                                parser.parse(
                                        Paths.get(resource.toURI()),
                                        Configuration.fromMap(
                                                ImmutableMap.<String, String>builder()
                                                        .put(
                                                                PIPELINE_LOCAL_TIME_ZONE.key(),
                                                                "invalid time zone")
                                                        .build())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Invalid time zone. The valid value should be a Time Zone Database ID"
                                + " such as 'America/Los_Angeles' to include daylight saving time. "
                                + "Fixed offsets are supported using 'GMT-08:00' or 'GMT+08:00'. "
                                + "Or use 'UTC' without time zone and daylight saving time.");
    }

    @Test
    void testRouteWithReplacementSymbol() throws Exception {
        URL resource =
                Resources.getResource("definitions/pipeline-definition-full-with-repsym.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(fullDefWithRouteRepSym);
    }

    private final PipelineDef fullDef =
            new PipelineDef(
                    new SourceDef(
                            "mysql",
                            "source-database",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("host", "localhost")
                                            .put("port", "3306")
                                            .put("username", "admin")
                                            .put("password", "pass")
                                            .put(
                                                    "tables",
                                                    "adb.*, bdb.user_table_[0-9]+, [app|web]_order_.*")
                                            .put(
                                                    "chunk-column",
                                                    "app_order_.*:id,web_order:product_id")
                                            .put("capture-new-tables", "true")
                                            .build())),
                    new SinkDef(
                            "kafka",
                            "sink-queue",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("bootstrap-servers", "localhost:9092")
                                            .put("auto-create-table", "true")
                                            .build())),
                    Arrays.asList(
                            new RouteDef(
                                    "mydb.default.app_order_.*",
                                    "odsdb.default.app_order",
                                    null,
                                    "sync all sharding tables to one"),
                            new RouteDef(
                                    "mydb.default.web_order",
                                    "odsdb.default.ods_web_order",
                                    null,
                                    "sync table to with given prefix ods_")),
                    Arrays.asList(
                            new TransformDef(
                                    "mydb.app_order_.*",
                                    "id, order_id, TO_UPPER(product_name)",
                                    "id > 10 AND order_id > 100",
                                    "id",
                                    "product_name",
                                    "comment=app order",
                                    "project fields from source table"),
                            new TransformDef(
                                    "mydb.web_order_.*",
                                    "CONCAT(id, order_id) as uniq_id, *",
                                    "uniq_id > 10",
                                    null,
                                    null,
                                    null,
                                    "add new uniq_id for each row")),
                    Arrays.asList(
                            new UdfDef(
                                    "substring",
                                    "com.example.functions.scalar.SubStringFunctionClass"),
                            new UdfDef(
                                    "encrypt",
                                    "com.example.functions.scalar.EncryptFunctionClass")),
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("name", "source-database-sync-pipe")
                                    .put("parallelism", "4")
                                    .put("schema.change.behavior", "evolve")
                                    .put("schema-operator.rpc-timeout", "1 h")
                                    .build()));

    private final PipelineDef fullDefWithGlobalConf =
            new PipelineDef(
                    new SourceDef(
                            "mysql",
                            "source-database",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("host", "localhost")
                                            .put("port", "3306")
                                            .put("username", "admin")
                                            .put("password", "pass")
                                            .put(
                                                    "tables",
                                                    "adb.*, bdb.user_table_[0-9]+, [app|web]_order_.*")
                                            .put(
                                                    "chunk-column",
                                                    "app_order_.*:id,web_order:product_id")
                                            .put("capture-new-tables", "true")
                                            .build())),
                    new SinkDef(
                            "kafka",
                            "sink-queue",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("bootstrap-servers", "localhost:9092")
                                            .put("auto-create-table", "true")
                                            .build())),
                    Arrays.asList(
                            new RouteDef(
                                    "mydb.default.app_order_.*",
                                    "odsdb.default.app_order",
                                    null,
                                    "sync all sharding tables to one"),
                            new RouteDef(
                                    "mydb.default.web_order",
                                    "odsdb.default.ods_web_order",
                                    null,
                                    "sync table to with given prefix ods_")),
                    Arrays.asList(
                            new TransformDef(
                                    "mydb.app_order_.*",
                                    "id, order_id, TO_UPPER(product_name)",
                                    "id > 10 AND order_id > 100",
                                    "id",
                                    "product_name",
                                    "comment=app order",
                                    "project fields from source table"),
                            new TransformDef(
                                    "mydb.web_order_.*",
                                    "CONCAT(id, order_id) as uniq_id, *",
                                    "uniq_id > 10",
                                    null,
                                    null,
                                    null,
                                    "add new uniq_id for each row")),
                    Arrays.asList(
                            new UdfDef(
                                    "substring",
                                    "com.example.functions.scalar.SubStringFunctionClass"),
                            new UdfDef(
                                    "encrypt",
                                    "com.example.functions.scalar.EncryptFunctionClass")),
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("name", "source-database-sync-pipe")
                                    .put("parallelism", "4")
                                    .put("schema.change.behavior", "evolve")
                                    .put("schema-operator.rpc-timeout", "1 h")
                                    .build()));

    private final PipelineDef defWithOptional =
            new PipelineDef(
                    new SourceDef(
                            "mysql",
                            null,
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("host", "localhost")
                                            .put("port", "3306")
                                            .put("username", "admin")
                                            .put("password", "pass")
                                            .put(
                                                    "tables",
                                                    "adb.*, bdb.user_table_[0-9]+, [app|web]_order_.*")
                                            .build())),
                    new SinkDef(
                            "kafka",
                            null,
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("bootstrap-servers", "localhost:9092")
                                            .build())),
                    Collections.singletonList(
                            new RouteDef(
                                    "mydb.default.app_order_.*",
                                    "odsdb.default.app_order",
                                    null,
                                    null)),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("parallelism", "4")
                                    .build()));

    private final PipelineDef minimizedDef =
            new PipelineDef(
                    new SourceDef("mysql", null, new Configuration()),
                    new SinkDef("kafka", null, new Configuration()),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Configuration.fromMap(Collections.singletonMap("parallelism", "1")));

    private final PipelineDef fullDefWithRouteRepSym =
            new PipelineDef(
                    new SourceDef(
                            "mysql",
                            "source-database",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("host", "localhost")
                                            .put("port", "3306")
                                            .put("username", "admin")
                                            .put("password", "pass")
                                            .put(
                                                    "tables",
                                                    "adb.*, bdb.user_table_[0-9]+, [app|web]_order_.*")
                                            .put(
                                                    "chunk-column",
                                                    "app_order_.*:id,web_order:product_id")
                                            .put("capture-new-tables", "true")
                                            .build())),
                    new SinkDef(
                            "kafka",
                            "sink-queue",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("bootstrap-servers", "localhost:9092")
                                            .put("auto-create-table", "true")
                                            .build())),
                    Arrays.asList(
                            new RouteDef(
                                    "mydb.default.app_order_.*",
                                    "odsdb.default.app_order_<>",
                                    "<>",
                                    "sync all sharding tables to one"),
                            new RouteDef(
                                    "mydb.default.web_order",
                                    "odsdb.default.ods_web_order_>_<",
                                    ">_<",
                                    "sync table to with given prefix ods_")),
                    Arrays.asList(
                            new TransformDef(
                                    "mydb.app_order_.*",
                                    "id, order_id, TO_UPPER(product_name)",
                                    "id > 10 AND order_id > 100",
                                    "id",
                                    "product_name",
                                    "comment=app order",
                                    "project fields from source table"),
                            new TransformDef(
                                    "mydb.web_order_.*",
                                    "CONCAT(id, order_id) as uniq_id, *",
                                    "uniq_id > 10",
                                    null,
                                    null,
                                    null,
                                    "add new uniq_id for each row")),
                    Collections.emptyList(),
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("name", "source-database-sync-pipe")
                                    .put("parallelism", "4")
                                    .put("schema.change.behavior", "evolve")
                                    .put("schema-operator.rpc-timeout", "1 h")
                                    .build()));
}
