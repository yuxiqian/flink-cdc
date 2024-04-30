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
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The processor of transform projection applies to process a row of filtering tables.
 *
 * <p>A transform projection processor contains:
 *
 * <ul>
 *   <li>CreateTableEvent: add the user-defined computed columns into Schema.
 *   <li>SchemaChangeEvent: update the columns of TransformProjection.
 *   <li>DataChangeEvent: Fill data field to row in PreTransformOperator. Process the data column
 *       and the user-defined expression computed columns.
 * </ul>
 */
public class PostTransformProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PostTransformProcessor.class);
    private TableInfo tableInfo;
    private TableChangeInfo tableChangeInfo;
    private TransformProjection transformProjection;
    private @Nullable TransformFilter transformFilter;
    private String timezone;
    private Map<String, ProjectionColumnProcessor> projectionColumnProcessorMap;

    public PostTransformProcessor(
            TableInfo tableInfo,
            TableChangeInfo tableChangeInfo,
            TransformProjection transformProjection,
            @Nullable TransformFilter transformFilter,
            String timezone) {
        this.tableInfo = tableInfo;
        this.tableChangeInfo = tableChangeInfo;
        this.transformProjection = transformProjection;
        this.transformFilter = transformFilter;
        this.timezone = timezone;
        this.projectionColumnProcessorMap = new ConcurrentHashMap<>();
    }

    public boolean hasTableChangeInfo() {
        return this.tableChangeInfo != null;
    }

    public boolean hasTableInfo() {
        return this.tableInfo != null;
    }

    public static PostTransformProcessor of(
            TableInfo tableInfo,
            TransformProjection transformProjection,
            TransformFilter transformFilter,
            String timezone) {
        return new PostTransformProcessor(
                tableInfo, null, transformProjection, transformFilter, timezone);
    }

    public static PostTransformProcessor of(
            TableChangeInfo tableChangeInfo,
            TransformProjection transformProjection,
            TransformFilter transformFilter) {
        return new PostTransformProcessor(
                null, tableChangeInfo, transformProjection, transformFilter, null);
    }

    public static PostTransformProcessor of(
            TransformProjection transformProjection, TransformFilter transformFilter) {
        return new PostTransformProcessor(null, null, transformProjection, transformFilter, null);
    }

    public Schema processSchemaChangeEvent(Schema schema) {
        List<ProjectionColumn> projectionColumns =
                TransformParser.generateProjectionColumns(
                        transformProjection.getProjection(), schema.getColumns());
        transformProjection.setProjectionColumns(projectionColumns);
        return schema.copy(
                projectionColumns.stream()
                        .map(ProjectionColumn::getColumn)
                        .collect(Collectors.toList()));
    }

    public BinaryRecordData processData(BinaryRecordData after, long epochTime) {
        List<Object> valueList = new ArrayList<>();
        for (Column column : tableInfo.getSchema().getColumns()) {
            boolean isProjectionColumn = false;
            for (ProjectionColumn projectionColumn : transformProjection.getProjectionColumns()) {
                if (column.getName().equals(projectionColumn.getColumnName())
                        && projectionColumn.isValidTransformedProjectionColumn()) {
                    if (!projectionColumnProcessorMap.containsKey(
                            projectionColumn.getColumnName())) {
                        projectionColumnProcessorMap.put(
                                projectionColumn.getColumnName(),
                                ProjectionColumnProcessor.of(
                                        tableInfo, projectionColumn, timezone));
                    }
                    ProjectionColumnProcessor projectionColumnProcessor =
                            projectionColumnProcessorMap.get(projectionColumn.getColumnName());
                    valueList.add(
                            DataTypeConverter.convert(
                                    projectionColumnProcessor.evaluate(after, epochTime),
                                    projectionColumn.getDataType()));
                    isProjectionColumn = true;
                    break;
                }
            }
            if (!isProjectionColumn) {
                valueList.add(
                        getValueFromBinaryRecordData(
                                column.getName(),
                                column.getType(),
                                after,
                                tableInfo.getOriginalSchema().getColumns(),
                                tableInfo.getOriginalFieldGetters()));
            }
        }
        return tableInfo
                .getRecordDataGenerator()
                .generate(valueList.toArray(new Object[valueList.size()]));
    }

    public BinaryRecordData preProcessData(BinaryRecordData after, long epochTime) {
        List<Object> valueList = new ArrayList<>();
        for (Column column : tableInfo.getSchema().getColumns()) {
            boolean isProjectionColumn = false;
            for (ProjectionColumn projectionColumn : transformProjection.getProjectionColumns()) {
                if (column.getName().equals(projectionColumn.getColumnName())
                        && projectionColumn.isValidTransformedProjectionColumn()) {
                    if (!projectionColumnProcessorMap.containsKey(
                            projectionColumn.getColumnName())) {
                        projectionColumnProcessorMap.put(
                                projectionColumn.getColumnName(),
                                ProjectionColumnProcessor.of(
                                        tableInfo, projectionColumn, timezone));
                    }
                    ProjectionColumnProcessor projectionColumnProcessor =
                            projectionColumnProcessorMap.get(projectionColumn.getColumnName());
                    valueList.add(
                            DataTypeConverter.convert(
                                    projectionColumnProcessor.evaluate(after, epochTime),
                                    projectionColumn.getDataType()));
                    isProjectionColumn = true;
                    break;
                }
            }
            if (!isProjectionColumn) {
                valueList.add(
                        getValueFromBinaryRecordData(
                                column.getName(),
                                column.getType(),
                                after,
                                tableInfo.getSchema().getColumns(),
                                tableInfo.getFieldGetters()));
            }
        }
        return tableInfo
                .getRecordDataGenerator()
                .generate(valueList.toArray(new Object[valueList.size()]));
    }

    private Object getValueFromBinaryRecordData(
            String columnName,
            DataType expectedType,
            BinaryRecordData binaryRecordData,
            List<Column> columns,
            RecordData.FieldGetter[] fieldGetters) {
        for (int i = 0; i < columns.size(); i++) {
            if (columnName.equals(columns.get(i).getName())) {
                return DataTypeConverter.convert(
                        fieldGetters[i].getFieldOrNull(binaryRecordData), expectedType);
            }
        }
        return null;
    }
}
