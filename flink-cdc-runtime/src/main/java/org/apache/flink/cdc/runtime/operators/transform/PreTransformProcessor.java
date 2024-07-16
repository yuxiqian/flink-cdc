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
    private PreTransformChangeInfo tableChangeInfo;
    private TransformProjection transformProjection;
    private @Nullable TransformFilter transformFilter;
    private List<Boolean> cachedProjectionColumnsState;

    public PreTransformProcessor(
            PreTransformChangeInfo tableChangeInfo,
            TransformProjection transformProjection,
            @Nullable TransformFilter transformFilter) {
        this.tableChangeInfo = tableChangeInfo;
        this.transformProjection = transformProjection;
        this.transformFilter = transformFilter;
        this.cachedProjectionColumnsState =
                cacheIsProjectionColumnMap(tableChangeInfo, transformProjection);
    }

    public boolean hasTableChangeInfo() {
        return this.tableChangeInfo != null;
    }

    /**
     * This method analyses (directly and indirectly) referenced columns, and peels unused columns
     * from schema. For example, given original schema with columns (A, B, C, D, E) with projection
     * rule (A, B + 1 as newB) and filtering rule (C > 0), a peeled schema containing (A, B, C) only
     * will be sent to downstream, and (D, E) column along with corresponding data will be trimmed.
     */
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
        List<Column> columns = tableChangeInfo.getPreTransformedSchema().getColumns();

        for (int i = 0; i < columns.size(); i++) {
            if (cachedProjectionColumnsState.get(i)) {
                valueList.add(null);
            } else {
                valueList.add(
                        getValueFromBinaryRecordData(
                                columns.get(i).getName(),
                                data,
                                tableChangeInfo.getSourceSchema().getColumns(),
                                tableChangeInfo.getSourceFieldGetters()));
            }
        }

        return tableChangeInfo
                .getPreTransformedRecordDataGenerator()
                .generate(valueList.toArray(new Object[0]));
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

    private List<Boolean> cacheIsProjectionColumnMap(
            PreTransformChangeInfo tableChangeInfo, TransformProjection transformProjection) {
        List<Boolean> cachedMap = new ArrayList<>();
        if (!hasTableChangeInfo()) {
            return cachedMap;
        }

        for (Column column : tableChangeInfo.getPreTransformedSchema().getColumns()) {
            boolean isProjectionColumn = false;
            for (ProjectionColumn projectionColumn : transformProjection.getProjectionColumns()) {
                if (column.getName().equals(projectionColumn.getColumnName())
                        && projectionColumn.isValidTransformedProjectionColumn()) {
                    isProjectionColumn = true;
                    break;
                }
            }
            cachedMap.add(isProjectionColumn);
        }

        return cachedMap;
    }
}
