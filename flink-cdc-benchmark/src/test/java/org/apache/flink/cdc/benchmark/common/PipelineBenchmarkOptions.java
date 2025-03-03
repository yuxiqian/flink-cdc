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

package org.apache.flink.cdc.benchmark.common;

import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.TransformDef;

import java.util.Collections;
import java.util.List;

/** Configurable options for running pipeline benchmark cases. */
public class PipelineBenchmarkOptions {

    List<TransformDef> transformDefs;
    List<RouteDef> routeDefs;
    SchemaChangeBehavior schemaChangeBehavior;
    int parallelism;

    public List<TransformDef> getTransformDefs() {
        return transformDefs;
    }

    public List<RouteDef> getRouteDefs() {
        return routeDefs;
    }

    public SchemaChangeBehavior getSchemaChangeBehavior() {
        return schemaChangeBehavior;
    }

    public int getParallelism() {
        return parallelism;
    }

    /** Builder for {@link PipelineBenchmarkOptions}. */
    public static class Builder {
        private List<TransformDef> transformDefs = Collections.emptyList();
        private List<RouteDef> routeDefs = Collections.emptyList();
        private SchemaChangeBehavior schemaChangeBehavior = SchemaChangeBehavior.LENIENT;
        private int parallelism = 1;

        public Builder setTransformDefs(List<TransformDef> transformDefs) {
            this.transformDefs = transformDefs;
            return this;
        }

        public Builder setRouteDefs(List<RouteDef> routeDefs) {
            this.routeDefs = routeDefs;
            return this;
        }

        public Builder setSchemaChangeBehavior(SchemaChangeBehavior schemaChangeBehavior) {
            this.schemaChangeBehavior = schemaChangeBehavior;
            return this;
        }

        public Builder setParallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public PipelineBenchmarkOptions build() {
            PipelineBenchmarkOptions options = new PipelineBenchmarkOptions();
            options.transformDefs = transformDefs;
            options.routeDefs = routeDefs;
            options.schemaChangeBehavior = schemaChangeBehavior;
            options.parallelism = parallelism;
            return options;
        }
    }
}
