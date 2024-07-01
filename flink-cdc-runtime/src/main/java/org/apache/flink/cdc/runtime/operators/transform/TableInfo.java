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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import java.util.List;

/** The TableInfo applies to cache schema and fieldGetters. */
public class TableInfo {
    private TableId tableId;
    private Schema schema;
    private RecordData.FieldGetter[] fieldGetters;
    private Schema originalSchema;
    private RecordData.FieldGetter[] originalFieldGetters;
    private BinaryRecordDataGenerator recordDataGenerator;

    public TableInfo(
            TableId tableId,
            Schema schema,
            RecordData.FieldGetter[] fieldGetters,
            Schema originalSchema,
            RecordData.FieldGetter[] originalFieldGetters,
            BinaryRecordDataGenerator recordDataGenerator) {
        this.tableId = tableId;
        this.schema = schema;
        this.fieldGetters = fieldGetters;
        this.originalSchema = originalSchema;
        this.originalFieldGetters = originalFieldGetters;
        this.recordDataGenerator = recordDataGenerator;
    }

    public String getName() {
        return tableId.identifier();
    }

    public String getTableName() {
        return tableId.getTableName();
    }

    public String getSchemaName() {
        return tableId.getSchemaName();
    }

    public String getNamespace() {
        return tableId.getNamespace();
    }

    public TableId getTableId() {
        return tableId;
    }

    public Schema getSchema() {
        return schema;
    }

    public Schema getOriginalSchema() {
        return originalSchema;
    }

    public RecordData.FieldGetter[] getFieldGetters() {
        return fieldGetters;
    }

    public RecordData.FieldGetter[] getOriginalFieldGetters() {
        return originalFieldGetters;
    }

    public BinaryRecordDataGenerator getRecordDataGenerator() {
        return recordDataGenerator;
    }

    public static TableInfo of(TableId tableId, Schema projectedSchema, Schema originalSchema) {

        List<RecordData.FieldGetter> projectedFieldGetters =
                SchemaUtils.createFieldGetters(projectedSchema.getColumns());

        List<RecordData.FieldGetter> originalFieldGetters =
                SchemaUtils.createFieldGetters(originalSchema.getColumns());

        BinaryRecordDataGenerator projectedRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        DataTypeConverter.toRowType(projectedSchema.getColumns()));

        return new TableInfo(
                tableId,
                projectedSchema,
                projectedFieldGetters.toArray(new RecordData.FieldGetter[0]),
                originalSchema,
                originalFieldGetters.toArray(new RecordData.FieldGetter[0]),
                projectedRecordDataGenerator);
    }
}
