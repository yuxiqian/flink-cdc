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
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * The processor of pre-transform projection in {@link PreTransformOperator}.
 *
 * <p>A pre-transform projection processor handles:
 *
 * <ul>
 *   <li>CreateTableEvent: removes unused (unreferenced) columns from given schema.
 *   <li>SchemaChangeEvent: update the columns of TransformProjection.
 *   <li>DataChangeEvent: omits unused columns in data row.
 * </ul>
 */
public class PreTransformProcessor {
    private TableChangeInfo tableChangeInfo;
    private TransformProjection transformProjection;
    private @Nullable TransformFilter transformFilter;

    public PreTransformProcessor(
            TableChangeInfo tableChangeInfo,
            TransformProjection transformProjection,
            @Nullable TransformFilter transformFilter) {
        this.tableChangeInfo = tableChangeInfo;
        this.transformProjection = transformProjection;
        this.transformFilter = transformFilter;
    }

    public boolean hasTableChangeInfo() {
        return this.tableChangeInfo != null;
    }

    public CreateTableEvent preTransformCreateTableEvent(CreateTableEvent createTableEvent) {
        List<Column> preTransformColumns =
                TransformParser.generateReferencedColumns(
                        transformProjection.getProjection(),
                        transformFilter != null ? transformFilter.getExpression() : null,
                        createTableEvent.getSchema().getColumns());
        Schema schema = createTableEvent.getSchema().copy(preTransformColumns);
        return new CreateTableEvent(createTableEvent.tableId(), schema);
    }

    public BinaryRecordData processFillDataField(BinaryRecordData data) {
        List<Object> valueList = new ArrayList<>();
        for (Column column : tableChangeInfo.getTransformedSchema().getColumns()) {
            boolean isProjectionColumn = false;
            for (ProjectionColumn projectionColumn : transformProjection.getProjectionColumns()) {
                if (column.getName().equals(projectionColumn.getColumnName())
                        && projectionColumn.isValidTransformedProjectionColumn()) {
                    valueList.add(null);
                    isProjectionColumn = true;
                    break;
                }
            }
            if (!isProjectionColumn) {
                valueList.add(
                        getValueFromBinaryRecordData(
                                column.getName(),
                                data,
                                tableChangeInfo.getOriginalSchema().getColumns(),
                                tableChangeInfo.getFieldGetters()));
            }
        }
        return tableChangeInfo
                .getRecordDataGenerator()
                .generate(valueList.toArray(new Object[valueList.size()]));
    }

    private Object getValueFromBinaryRecordData(
            String columnName,
            BinaryRecordData binaryRecordData,
            List<Column> columns,
            RecordData.FieldGetter[] fieldGetters) {
        for (int i = 0; i < columns.size(); i++) {
            if (columnName.equals(columns.get(i).getName())) {
                return DataTypeConverter.convert(
                        fieldGetters[i].getFieldOrNull(binaryRecordData), columns.get(i).getType());
            }
        }
        return null;
    }
}
