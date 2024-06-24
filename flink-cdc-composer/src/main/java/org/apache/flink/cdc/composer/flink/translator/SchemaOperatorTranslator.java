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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperatorFactory;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Translator used to build {@link SchemaOperator} for schema event process. */
@Internal
public class SchemaOperatorTranslator {
    private final SchemaChangeBehavior schemaChangeBehavior;
    private final String schemaOperatorUid;

    private final Duration rpcTimeOut;

    public SchemaOperatorTranslator(
            SchemaChangeBehavior schemaChangeBehavior,
            String schemaOperatorUid,
            Duration rpcTimeOut) {
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.schemaOperatorUid = schemaOperatorUid;
        this.rpcTimeOut = rpcTimeOut;
    }

    public DataStream<Event> translate(
            DataStream<Event> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes) {
        return addSchemaOperator(input, parallelism, metadataApplier, routes, schemaChangeBehavior);
    }

    public String getSchemaOperatorUid() {
        return schemaOperatorUid;
    }

    private DataStream<Event> addSchemaOperator(
            DataStream<Event> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes,
            SchemaChangeBehavior schemaChangeBehavior) {
        List<Tuple2<String, TableId>> routingRules = new ArrayList<>();
        for (RouteDef route : routes) {
            routingRules.add(
                    Tuple2.of(route.getSourceTable(), TableId.parse(route.getSinkTable())));
        }
        SingleOutputStreamOperator<Event> stream =
                input.transform(
                        "SchemaOperator",
                        new EventTypeInfo(),
                        new SchemaOperatorFactory(
                                metadataApplier, routingRules, rpcTimeOut, schemaChangeBehavior));
        stream.uid(schemaOperatorUid).setParallelism(parallelism);
        return stream;
    }

    private DataStream<Event> dropSchemaChangeEvent(DataStream<Event> input, int parallelism) {
        return input.filter(event -> !(event instanceof SchemaChangeEvent))
                .setParallelism(parallelism);
    }

    private DataStream<Event> exceptionOnSchemaChange(DataStream<Event> input, int parallelism) {
        return input.map(
                        event -> {
                            if (event instanceof SchemaChangeEvent) {
                                throw new RuntimeException(
                                        String.format(
                                                "Aborting execution as the pipeline encountered a schema change event: %s",
                                                event));
                            }
                            return event;
                        })
                .setParallelism(parallelism);
    }
}
