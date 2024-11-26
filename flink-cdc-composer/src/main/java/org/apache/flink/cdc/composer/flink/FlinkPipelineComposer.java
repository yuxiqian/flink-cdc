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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
import org.apache.flink.cdc.composer.flink.translator.DataSourceTranslator;
import org.apache.flink.cdc.composer.flink.translator.PartitioningTranslator;
import org.apache.flink.cdc.composer.flink.translator.SchemaMapReducerTranslator;
import org.apache.flink.cdc.composer.flink.translator.SchemaOperatorTranslator;
import org.apache.flink.cdc.composer.flink.translator.TransformTranslator;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Composer for translating data pipeline to a Flink DataStream job. */
@Internal
public class FlinkPipelineComposer implements PipelineComposer {

    private final StreamExecutionEnvironment env;
    private final boolean isBlocking;

    public static FlinkPipelineComposer ofRemoteCluster(
            org.apache.flink.configuration.Configuration flinkConfig, List<Path> additionalJars) {
        org.apache.flink.configuration.Configuration effectiveConfiguration =
                new org.apache.flink.configuration.Configuration();
        // Use "remote" as the default target
        effectiveConfiguration.set(DeploymentOptions.TARGET, "remote");
        effectiveConfiguration.addAll(flinkConfig);
        StreamExecutionEnvironment env = new StreamExecutionEnvironment(effectiveConfiguration);
        additionalJars.forEach(
                jarPath -> {
                    try {
                        FlinkEnvironmentUtils.addJar(env, jarPath.toUri().toURL());
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format(
                                        "Unable to convert JAR path \"%s\" to URL when adding JAR to Flink environment",
                                        jarPath),
                                e);
                    }
                });
        return new FlinkPipelineComposer(env, false);
    }

    public static FlinkPipelineComposer ofMiniCluster() {
        return new FlinkPipelineComposer(
                StreamExecutionEnvironment.getExecutionEnvironment(), true);
    }

    private FlinkPipelineComposer(StreamExecutionEnvironment env, boolean isBlocking) {
        this.env = env;
        this.isBlocking = isBlocking;
    }

    @Override
    public PipelineExecution compose(PipelineDef pipelineDef) {
        Configuration pipelineDefConfig = pipelineDef.getConfig();

        int parallelism = pipelineDefConfig.get(PipelineOptions.PIPELINE_PARALLELISM);
        env.getConfig().setParallelism(parallelism);

        SchemaChangeBehavior schemaChangeBehavior =
                pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR);

        // Source Translator
        DataSourceTranslator sourceTranslator = new DataSourceTranslator();

        // Pre- and Post- Transform Translators
        TransformTranslator transformTranslator = new TransformTranslator();

        // Schema Operator Translator
        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(
                        schemaChangeBehavior,
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID),
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT),
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));

        // Schema Map-Reducer Translator
        SchemaMapReducerTranslator schemaMapReducerTranslator =
                new SchemaMapReducerTranslator(
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID),
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT),
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));

        // Partitioning Translator
        PartitioningTranslator partitioningTranslator = new PartitioningTranslator();

        // Sink Translator
        DataSinkTranslator sinkTranslator = new DataSinkTranslator();

        DataSink dataSink =
                sinkTranslator.createDataSink(pipelineDef.getSink(), pipelineDefConfig, env);

        OperatorIDGenerator schemaOperatorIDGenerator =
                new OperatorIDGenerator(
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID));

        // Start translating job graph

        // -> Source
        DataStream<Event> stream =
                sourceTranslator.translate(
                        pipelineDef.getSource(), env, pipelineDefConfig, parallelism);
        boolean needsSchemaInferencing = sourceTranslator.needsSchemaInferencing();

        // Source -> PreTransform
        stream =
                transformTranslator.translatePreTransform(
                        stream, pipelineDef.getTransforms(), pipelineDef.getUdfs(), pipelineDef.getModels());

        // PreTransform -> PostTransform
        stream =
                transformTranslator.translatePostTransform(
                        stream,
                        pipelineDef.getTransforms(),
                        pipelineDefConfig.get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                        pipelineDef.getUdfs(),
                        pipelineDef.getModels());

        if (needsSchemaInferencing) {
            // PostTransform -> Partitioning
            stream =
                    partitioningTranslator.translate(
                            stream,
                            parallelism,
                            parallelism,
                            schemaOperatorIDGenerator.generate(),
                            dataSink.getDataChangeEventHashFunctionProvider(parallelism),
                            true);

            // Partitioning -> Route -> MapReduce
            stream =
                    schemaMapReducerTranslator.translate(
                            stream,
                            parallelism,
                            dataSink.getMetadataApplier()
                                    .setAcceptedSchemaEvolutionTypes(
                                            pipelineDef
                                                    .getSink()
                                                    .getIncludedSchemaEvolutionTypes()),
                            pipelineDef.getRoute());

            // MapReduce -> Sink
            sinkTranslator.translate(
                    pipelineDef.getSink(), stream, dataSink, schemaOperatorIDGenerator.generate());
        } else {
            // PostTransform -> Schema
            stream =
                    schemaOperatorTranslator.translate(
                            stream,
                            parallelism,
                            dataSink.getMetadataApplier()
                                    .setAcceptedSchemaEvolutionTypes(
                                            pipelineDef
                                                    .getSink()
                                                    .getIncludedSchemaEvolutionTypes()),
                            pipelineDef.getRoute());

            // Schema -> Partitioning
            stream =
                    partitioningTranslator.translate(
                            stream,
                            parallelism,
                            parallelism,
                            schemaOperatorIDGenerator.generate(),
                            dataSink.getDataChangeEventHashFunctionProvider(parallelism),
                            false);

            // Partitioning -> Sink
            sinkTranslator.translate(
                    pipelineDef.getSink(), stream, dataSink, schemaOperatorIDGenerator.generate());
        }

        // Add framework JARs
        addFrameworkJars();

        return new FlinkPipelineExecution(
                env, pipelineDefConfig.get(PipelineOptions.PIPELINE_NAME), isBlocking);
    }

    private void addFrameworkJars() {
        try {
            Set<URI> frameworkJars = new HashSet<>();
            // Common JAR
            // We use the core interface (Event) to search the JAR
            Optional<URL> commonJar = getContainingJar(Event.class);
            if (commonJar.isPresent()) {
                frameworkJars.add(commonJar.get().toURI());
            }
            // Runtime JAR
            // We use the serializer of the core interface (EventSerializer) to search the JAR
            Optional<URL> runtimeJar = getContainingJar(EventSerializer.class);
            if (runtimeJar.isPresent()) {
                frameworkJars.add(runtimeJar.get().toURI());
            }
            for (URI jar : frameworkJars) {
                FlinkEnvironmentUtils.addJar(env, jar.toURL());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to search and add Flink CDC framework JARs", e);
        }
    }

    private Optional<URL> getContainingJar(Class<?> clazz) throws Exception {
        URL container = clazz.getProtectionDomain().getCodeSource().getLocation();
        if (Files.isDirectory(Paths.get(container.toURI()))) {
            return Optional.empty();
        }
        return Optional.of(container);
    }
}
