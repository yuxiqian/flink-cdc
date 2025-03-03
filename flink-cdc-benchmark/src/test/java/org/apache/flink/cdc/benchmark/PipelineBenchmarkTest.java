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

package org.apache.flink.cdc.benchmark;

import org.apache.flink.cdc.benchmark.cases.RoutedPipeline;
import org.apache.flink.cdc.benchmark.cases.SchemaEvolvingPipeline;
import org.apache.flink.cdc.benchmark.cases.TransformedPipeline;
import org.apache.flink.cdc.benchmark.cases.VanillaPipeline;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/** Test cases for executing pipeline benchmark tests. */
class PipelineBenchmarkTest {

    @Test
    void runPipelineBenchmarkTests() throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .include(VanillaPipeline.class.getSimpleName())
                        .include(TransformedPipeline.class.getSimpleName())
                        .include(RoutedPipeline.class.getSimpleName())
                        .include(SchemaEvolvingPipeline.class.getSimpleName())
                        .mode(Mode.AverageTime)
                        .warmupIterations(1)
                        .measurementIterations(5)
                        .forks(1)
                        .resultFormat(ResultFormatType.JSON)
                        .build();
        new Runner(opt).run();
    }
}
