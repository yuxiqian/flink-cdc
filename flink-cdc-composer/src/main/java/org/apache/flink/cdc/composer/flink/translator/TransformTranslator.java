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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.runtime.operators.transform.PostTransformOperator;
import org.apache.flink.cdc.runtime.operators.transform.PreTransformOperator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

/**
 * Translator used to build {@link PreTransformOperator} and {@link PostTransformOperator} for event
 * transform.
 */
public class TransformTranslator {

    public DataStream<Event> translatePreTransform(
            DataStream<Event> input, List<TransformDef> transforms) {
        if (transforms.isEmpty()) {
            return input;
        }

        PreTransformOperator.Builder preTransformFunctionBuilder =
                PreTransformOperator.newBuilder();
        for (TransformDef transform : transforms) {
            if (transform.isValidProjection()) {
                preTransformFunctionBuilder.addTransform(
                        transform.getSourceTable(),
                        transform.getProjection().orElse(null),
                        transform.getFilter().orElse(null),
                        transform.getPrimaryKeys(),
                        transform.getPartitionKeys(),
                        transform.getTableOptions());
            }
        }
        return input.transform(
                "Transform:Schema", new EventTypeInfo(), preTransformFunctionBuilder.build());
    }

    public DataStream<Event> translatePostTransform(
            DataStream<Event> input,
            List<TransformDef> transforms,
            OperatorID schemaOperatorID,
            String timezone) {
        if (transforms.isEmpty()) {
            return input;
        }

        PostTransformOperator.Builder postTransformFunctionBuilder =
                PostTransformOperator.newBuilder();
        for (TransformDef transform : transforms) {
            if (transform.isValidProjection() || transform.isValidFilter()) {
                postTransformFunctionBuilder.addTransform(
                        transform.getSourceTable(),
                        transform.isValidProjection() ? transform.getProjection().get() : null,
                        transform.isValidFilter() ? transform.getFilter().get() : null,
                        transform.getPrimaryKeys(),
                        transform.getPartitionKeys(),
                        transform.getTableOptions());
            }
        }
        postTransformFunctionBuilder.addSchemaOperatorID(schemaOperatorID);
        postTransformFunctionBuilder.addTimezone(timezone);
        return input.transform(
                "Transform:Data", new EventTypeInfo(), postTransformFunctionBuilder.build());
    }
}
