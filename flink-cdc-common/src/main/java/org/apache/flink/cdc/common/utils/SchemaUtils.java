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

package org.apache.flink.cdc.common.utils;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeFamily;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utils for {@link Schema} to perform the ability of evolution. */
@PublicEvolving
public class SchemaUtils {

    /**
     * create a list of {@link RecordData.FieldGetter} from given {@link Schema} to get Object from
     * RecordData.
     */
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema) {
        return createFieldGetters(schema.getColumns());
    }

    /**
     * create a list of {@link RecordData.FieldGetter} from given {@link Column} to get Object from
     * RecordData.
     */
    public static List<RecordData.FieldGetter> createFieldGetters(List<Column> columns) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            fieldGetters.add(RecordData.createFieldGetter(columns.get(i).getType(), i));
        }
        return fieldGetters;
    }

    /** Restore original data fields from RecordData structure. */
    public static List<Object> restoreOriginalData(
            @Nullable RecordData recordData, List<RecordData.FieldGetter> fieldGetters) {
        if (recordData == null) {
            return Collections.emptyList();
        }
        List<Object> actualFields = new ArrayList<>();
        for (RecordData.FieldGetter fieldGetter : fieldGetters) {
            actualFields.add(fieldGetter.getFieldOrNull(recordData));
        }
        return actualFields;
    }

    /** Merge compatible upstream schemas. */
    public static Schema mergeCompatibleSchemas(List<Schema> schemas) {
        if (schemas.isEmpty()) {
            return null;
        } else if (schemas.size() == 1) {
            return schemas.get(0);
        } else {
            Schema outputSchema = null;
            for (Schema schema : schemas) {
                outputSchema = mergeSchema(outputSchema, schema);
            }
            return outputSchema;
        }
    }

    /** Try to combine two schemas with potential incompatible type. */
    @VisibleForTesting
    public static Schema mergeSchema(@Nullable Schema lhs, Schema rhs) {
        if (lhs == null) {
            return rhs;
        }
        if (lhs.getColumnCount() != rhs.getColumnCount()) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge schema %s and %s with different column counts.",
                            lhs, rhs));
        }
        if (!lhs.primaryKeys().equals(rhs.primaryKeys())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge schema %s and %s with different primary keys.",
                            lhs, rhs));
        }
        if (!lhs.partitionKeys().equals(rhs.partitionKeys())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge schema %s and %s with different partition keys.",
                            lhs, rhs));
        }
        if (!lhs.options().equals(rhs.options())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge schema %s and %s with different options.", lhs, rhs));
        }
        if (!Objects.equals(lhs.comment(), rhs.comment())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge schema %s and %s with different comments.", lhs, rhs));
        }

        List<Column> leftColumns = lhs.getColumns();
        List<Column> rightColumns = rhs.getColumns();

        List<Column> mergedColumns =
                IntStream.range(0, lhs.getColumnCount())
                        .mapToObj(i -> mergeColumn(leftColumns.get(i), rightColumns.get(i)))
                        .collect(Collectors.toList());

        return lhs.copy(mergedColumns);
    }

    /** Try to combine two columns with potential incompatible type. */
    @VisibleForTesting
    public static Column mergeColumn(Column lhs, Column rhs) {
        if (!Objects.equals(lhs.getName(), rhs.getName())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge column %s and %s with different name.", lhs, rhs));
        }
        if (!Objects.equals(lhs.getComment(), rhs.getComment())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge column %s and %s with different comments.", lhs, rhs));
        }
        return lhs.copy(mergeDataType(lhs.getType(), rhs.getType()));
    }

    /** Try to combine given data types to a compatible wider data type. */
    @VisibleForTesting
    public static DataType mergeDataType(DataType lhs, DataType rhs) {
        // Ignore nullability during data type merge
        boolean nullable = lhs.isNullable() || rhs.isNullable();
        lhs = lhs.notNull();
        rhs = rhs.notNull();

        DataType mergedType;
        if (lhs.equals(rhs)) {
            // identical type
            mergedType = rhs;
        } else if (lhs.is(DataTypeFamily.INTEGER_NUMERIC)
                && rhs.is(DataTypeFamily.INTEGER_NUMERIC)) {
            mergedType = DataTypes.BIGINT();
        } else if (lhs.is(DataTypeFamily.CHARACTER_STRING)
                && rhs.is(DataTypeFamily.CHARACTER_STRING)) {
            mergedType = DataTypes.STRING();
        } else if (lhs.is(DataTypeFamily.APPROXIMATE_NUMERIC)
                && rhs.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
            mergedType = DataTypes.DOUBLE();
        } else if (lhs.is(DataTypeRoot.DECIMAL) && rhs.is(DataTypeRoot.DECIMAL)) {
            // Merge two decimal types
            DecimalType lhsDecimal = (DecimalType) lhs;
            DecimalType rhsDecimal = (DecimalType) rhs;
            int resultIntDigits =
                    Math.max(
                            lhsDecimal.getPrecision() - lhsDecimal.getScale(),
                            rhsDecimal.getPrecision() - rhsDecimal.getScale());
            int resultScale = Math.max(lhsDecimal.getScale(), rhsDecimal.getScale());
            mergedType = DataTypes.DECIMAL(resultIntDigits + resultScale, resultScale);
        } else if (lhs.is(DataTypeRoot.DECIMAL) && rhs.is(DataTypeFamily.EXACT_NUMERIC)) {
            // Merge decimal and int
            DecimalType lhsDecimal = (DecimalType) lhs;
            mergedType =
                    DataTypes.DECIMAL(
                            Math.max(
                                    lhsDecimal.getPrecision(),
                                    lhsDecimal.getScale() + getNumericPrecision(rhs)),
                            lhsDecimal.getScale());
        } else if (rhs.is(DataTypeRoot.DECIMAL) && lhs.is(DataTypeFamily.EXACT_NUMERIC)) {
            // Merge decimal and int
            DecimalType rhsDecimal = (DecimalType) rhs;
            mergedType =
                    DataTypes.DECIMAL(
                            Math.max(
                                    rhsDecimal.getPrecision(),
                                    rhsDecimal.getScale() + getNumericPrecision(lhs)),
                            rhsDecimal.getScale());
        } else {
            throw new IllegalStateException(
                    String.format("Incompatible types: \"%s\" and \"%s\"", lhs, rhs));
        }

        if (nullable) {
            return mergedType.nullable();
        } else {
            return mergedType.notNull();
        }
    }

    @VisibleForTesting
    public static int getNumericPrecision(DataType dataType) {
        if (dataType.is(DataTypeFamily.EXACT_NUMERIC)) {
            if (dataType.is(DataTypeRoot.TINYINT)) {
                return 3;
            } else if (dataType.is(DataTypeRoot.SMALLINT)) {
                return 5;
            } else if (dataType.is(DataTypeRoot.INTEGER)) {
                return 10;
            } else if (dataType.is(DataTypeRoot.BIGINT)) {
                return 19;
            } else if (dataType.is(DataTypeRoot.DECIMAL)) {
                return ((DecimalType) dataType).getPrecision();
            }
        }

        throw new IllegalArgumentException(
                "Failed to get precision of non-exact decimal type " + dataType);
    }

    /** apply SchemaChangeEvent to the old schema and return the schema after changing. */
    public static Schema applySchemaChangeEvent(Schema schema, SchemaChangeEvent event) {
        if (event instanceof AddColumnEvent) {
            return applyAddColumnEvent((AddColumnEvent) event, schema);
        } else if (event instanceof DropColumnEvent) {
            return applyDropColumnEvent((DropColumnEvent) event, schema);
        } else if (event instanceof RenameColumnEvent) {
            return applyRenameColumnEvent((RenameColumnEvent) event, schema);
        } else if (event instanceof AlterColumnTypeEvent) {
            return applyAlterColumnTypeEvent((AlterColumnTypeEvent) event, schema);
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported schema change event type \"%s\"",
                            event.getClass().getCanonicalName()));
        }
    }

    private static Schema applyAddColumnEvent(AddColumnEvent event, Schema oldSchema) {
        LinkedList<Column> columns = new LinkedList<>(oldSchema.getColumns());
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            switch (columnWithPosition.getPosition()) {
                case FIRST:
                    {
                        columns.addFirst(columnWithPosition.getAddColumn());
                        break;
                    }
                case LAST:
                    {
                        columns.addLast(columnWithPosition.getAddColumn());
                        break;
                    }
                case BEFORE:
                    {
                        Preconditions.checkNotNull(
                                columnWithPosition.getExistedColumnName(),
                                "existedColumnName could not be null in BEFORE type AddColumnEvent");
                        List<String> columnNames =
                                columns.stream().map(Column::getName).collect(Collectors.toList());
                        int index = columnNames.indexOf(columnWithPosition.getExistedColumnName());
                        if (index < 0) {
                            throw new IllegalArgumentException(
                                    columnWithPosition.getExistedColumnName()
                                            + " of AddColumnEvent is not existed");
                        }
                        columns.add(index, columnWithPosition.getAddColumn());
                        break;
                    }
                case AFTER:
                    {
                        Preconditions.checkNotNull(
                                columnWithPosition.getExistedColumnName(),
                                "existedColumnName could not be null in AFTER type AddColumnEvent");
                        List<String> columnNames =
                                columns.stream().map(Column::getName).collect(Collectors.toList());
                        int index = columnNames.indexOf(columnWithPosition.getExistedColumnName());
                        if (index < 0) {
                            throw new IllegalArgumentException(
                                    columnWithPosition.getExistedColumnName()
                                            + " of AddColumnEvent is not existed");
                        }
                        columns.add(index + 1, columnWithPosition.getAddColumn());
                        break;
                    }
            }
        }
        return oldSchema.copy(columns);
    }

    private static Schema applyDropColumnEvent(DropColumnEvent event, Schema oldSchema) {
        List<Column> columns =
                oldSchema.getColumns().stream()
                        .filter(
                                (column ->
                                        !event.getDroppedColumnNames().contains(column.getName())))
                        .collect(Collectors.toList());
        return oldSchema.copy(columns);
    }

    private static Schema applyRenameColumnEvent(RenameColumnEvent event, Schema oldSchema) {
        List<Column> columns = new ArrayList<>();
        oldSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (event.getNameMapping().containsKey(column.getName())) {
                                columns.add(
                                        column.copy(event.getNameMapping().get(column.getName())));
                            } else {
                                columns.add(column);
                            }
                        });
        return oldSchema.copy(columns);
    }

    private static Schema applyAlterColumnTypeEvent(AlterColumnTypeEvent event, Schema oldSchema) {
        List<Column> columns = new ArrayList<>();
        oldSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (event.getTypeMapping().containsKey(column.getName())) {
                                columns.add(
                                        column.copy(event.getTypeMapping().get(column.getName())));
                            } else {
                                columns.add(column);
                            }
                        });
        return oldSchema.copy(columns);
    }
}
