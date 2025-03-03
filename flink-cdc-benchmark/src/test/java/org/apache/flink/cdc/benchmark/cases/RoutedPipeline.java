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

package org.apache.flink.cdc.benchmark.cases;

import org.apache.flink.cdc.benchmark.common.PipelineBenchmarkBase;
import org.apache.flink.cdc.benchmark.common.PipelineBenchmarkOptions;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.Collections;

/** Benchmarking test cases with routed pipelines. */
public class RoutedPipeline extends PipelineBenchmarkBase {

    @Benchmark
    public void runRoutedPipelineBenchmarkTest(Blackhole blackhole) throws Exception {
        PipelineBenchmarkOptions options =
                new PipelineBenchmarkOptions.Builder().setParallelism(1).build();
        Arrays.stream(
                        runInPipeline(
                                Collections.singletonList(
                                        new CreateTableEvent(
                                                TableId.tableId("foo", "bar", "baz"),
                                                Schema.newBuilder()
                                                        .physicalColumn(
                                                                "id", DataTypes.BIGINT().notNull())
                                                        .physicalColumn(
                                                                "everything", DataTypes.STRING())
                                                        .primaryKey("id")
                                                        .build())),
                                options))
                .forEach(blackhole::consume);
    }
}
