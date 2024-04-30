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

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A test for the {@link org.apache.flink.cdc.common.utils.SchemaUtils}. */
public class SchemaUtilsTest {

    @Test
    public void testApplySchemaChangeEvent() {
        TableId tableId = TableId.parse("default.default.table1");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .build();

        // add column in last position
        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", DataTypes.STRING())));
        AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build());

        // add new column before existed column
        addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col4", DataTypes.STRING()),
                        AddColumnEvent.ColumnPosition.BEFORE,
                        "col3"));
        addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col4", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build());

        // add new column after existed column
        addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col5", DataTypes.STRING()),
                        AddColumnEvent.ColumnPosition.AFTER,
                        "col4"));
        addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col4", DataTypes.STRING())
                        .physicalColumn("col5", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build());

        // add column in first position
        addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col0", DataTypes.STRING()),
                        AddColumnEvent.ColumnPosition.FIRST,
                        null));
        addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col0", DataTypes.STRING())
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col4", DataTypes.STRING())
                        .physicalColumn("col5", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build());

        // drop columns
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(tableId, Arrays.asList("col3", "col5"));
        schema = SchemaUtils.applySchemaChangeEvent(schema, dropColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col0", DataTypes.STRING())
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col4", DataTypes.STRING())
                        .build());

        // rename columns
        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col4", "newCol4");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(tableId, nameMapping);
        schema = SchemaUtils.applySchemaChangeEvent(schema, renameColumnEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col0", DataTypes.STRING())
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("newCol2", DataTypes.STRING())
                        .physicalColumn("newCol4", DataTypes.STRING())
                        .build());

        // alter column types
        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("newCol2", DataTypes.VARCHAR(10));
        typeMapping.put("newCol4", DataTypes.VARCHAR(10));
        AlterColumnTypeEvent alterColumnTypeEvent = new AlterColumnTypeEvent(tableId, typeMapping);
        schema = SchemaUtils.applySchemaChangeEvent(schema, alterColumnTypeEvent);
        Assert.assertEquals(
                schema,
                Schema.newBuilder()
                        .physicalColumn("col0", DataTypes.STRING())
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("newCol2", DataTypes.VARCHAR(10))
                        .physicalColumn("newCol4", DataTypes.VARCHAR(10))
                        .build());
    }

    @Test
    public void testGetNumericPrecision() {
        Assert.assertEquals(SchemaUtils.getNumericPrecision(DataTypes.TINYINT()), 3);
        Assert.assertEquals(SchemaUtils.getNumericPrecision(DataTypes.SMALLINT()), 5);
        Assert.assertEquals(SchemaUtils.getNumericPrecision(DataTypes.INT()), 10);
        Assert.assertEquals(SchemaUtils.getNumericPrecision(DataTypes.BIGINT()), 19);
        Assert.assertEquals(SchemaUtils.getNumericPrecision(DataTypes.DECIMAL(10, 2)), 10);
        Assert.assertEquals(SchemaUtils.getNumericPrecision(DataTypes.DECIMAL(17, 0)), 17);
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> SchemaUtils.getNumericPrecision(DataTypes.STRING()));
    }

    @Test
    public void testMergeDataType() {
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.BINARY(17), DataTypes.BINARY(17)),
                DataTypes.BINARY(17));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.VARBINARY(17), DataTypes.VARBINARY(17)),
                DataTypes.VARBINARY(17));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.BYTES(), DataTypes.BYTES()), DataTypes.BYTES());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.BOOLEAN(), DataTypes.BOOLEAN()),
                DataTypes.BOOLEAN());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.INT(), DataTypes.INT()), DataTypes.INT());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.TINYINT(), DataTypes.TINYINT()),
                DataTypes.TINYINT());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.SMALLINT(), DataTypes.SMALLINT()),
                DataTypes.SMALLINT());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.BIGINT(), DataTypes.BIGINT()),
                DataTypes.BIGINT());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.FLOAT(), DataTypes.FLOAT()), DataTypes.FLOAT());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.DOUBLE(), DataTypes.DOUBLE()),
                DataTypes.DOUBLE());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.CHAR(17), DataTypes.CHAR(17)),
                DataTypes.CHAR(17));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.VARCHAR(17), DataTypes.VARCHAR(17)),
                DataTypes.VARCHAR(17));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.STRING(), DataTypes.STRING()),
                DataTypes.STRING());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.DECIMAL(17, 7), DataTypes.DECIMAL(17, 7)),
                DataTypes.DECIMAL(17, 7));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.DATE(), DataTypes.DATE()), DataTypes.DATE());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.TIME(), DataTypes.TIME()), DataTypes.TIME());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.TIME(6), DataTypes.TIME(6)), DataTypes.TIME(6));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.TIMESTAMP(), DataTypes.TIMESTAMP()),
                DataTypes.TIMESTAMP());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(3)),
                DataTypes.TIMESTAMP(3));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.TIMESTAMP_TZ(), DataTypes.TIMESTAMP_TZ()),
                DataTypes.TIMESTAMP_TZ());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.TIMESTAMP_TZ(3), DataTypes.TIMESTAMP_TZ(3)),
                DataTypes.TIMESTAMP_TZ(3));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.TIMESTAMP_LTZ(), DataTypes.TIMESTAMP_LTZ()),
                DataTypes.TIMESTAMP_LTZ());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.TIMESTAMP_LTZ(3), DataTypes.TIMESTAMP_LTZ(3)),
                DataTypes.TIMESTAMP_LTZ(3));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(
                        DataTypes.ARRAY(DataTypes.INT()), DataTypes.ARRAY(DataTypes.INT())),
                DataTypes.ARRAY(DataTypes.INT()));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(
                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())),
                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()));

        // Test compatible widening cast
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.INT(), DataTypes.BIGINT()), DataTypes.BIGINT());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.VARCHAR(17), DataTypes.STRING()),
                DataTypes.STRING());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.FLOAT(), DataTypes.DOUBLE()),
                DataTypes.DOUBLE());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.INT(), DataTypes.DECIMAL(4, 0)),
                DataTypes.DECIMAL(10, 0));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.INT(), DataTypes.DECIMAL(10, 5)),
                DataTypes.DECIMAL(15, 5));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.BIGINT(), DataTypes.DECIMAL(10, 5)),
                DataTypes.DECIMAL(24, 5));
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.DECIMAL(5, 4), DataTypes.DECIMAL(10, 2)),
                DataTypes.DECIMAL(12, 4));

        // Test merging with nullability
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.INT().notNull(), DataTypes.INT().notNull()),
                DataTypes.INT().notNull());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.INT().nullable(), DataTypes.INT().notNull()),
                DataTypes.INT().nullable());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.INT().notNull(), DataTypes.INT().nullable()),
                DataTypes.INT().nullable());
        Assert.assertEquals(
                SchemaUtils.mergeDataType(DataTypes.INT().nullable(), DataTypes.INT().nullable()),
                DataTypes.INT().nullable());

        // incompatible type merges test
        Assert.assertThrows(
                IllegalStateException.class,
                () -> SchemaUtils.mergeDataType(DataTypes.INT(), DataTypes.DOUBLE()));
        Assert.assertThrows(
                IllegalStateException.class,
                () -> SchemaUtils.mergeDataType(DataTypes.DECIMAL(17, 0), DataTypes.DOUBLE()));
        Assert.assertThrows(
                IllegalStateException.class,
                () -> SchemaUtils.mergeDataType(DataTypes.INT(), DataTypes.STRING()));
    }

    @Test
    public void testMergeColumn() {
        // Test normal merges
        Assert.assertEquals(
                SchemaUtils.mergeColumn(
                        Column.physicalColumn("Column1", DataTypes.INT()),
                        Column.physicalColumn("Column1", DataTypes.BIGINT())),
                Column.physicalColumn("Column1", DataTypes.BIGINT()));

        Assert.assertEquals(
                SchemaUtils.mergeColumn(
                        Column.physicalColumn("Column2", DataTypes.FLOAT()),
                        Column.physicalColumn("Column2", DataTypes.DOUBLE())),
                Column.physicalColumn("Column2", DataTypes.DOUBLE()));

        // Test merging columns with incompatible types
        Assert.assertThrows(
                IllegalStateException.class,
                () ->
                        SchemaUtils.mergeColumn(
                                Column.physicalColumn("Column3", DataTypes.INT()),
                                Column.physicalColumn("Column3", DataTypes.STRING())));

        // Test merging with incompatible names
        Assert.assertThrows(
                IllegalStateException.class,
                () ->
                        SchemaUtils.mergeColumn(
                                Column.physicalColumn("Column4", DataTypes.INT()),
                                Column.physicalColumn("AnotherColumn4", DataTypes.INT())));
    }

    @Test
    public void testMergeSchema() {
        // Test normal merges
        Assert.assertEquals(
                SchemaUtils.mergeSchema(
                        Schema.newBuilder()
                                .physicalColumn("Column1", DataTypes.INT())
                                .physicalColumn("Column2", DataTypes.DOUBLE())
                                .primaryKey("Column1")
                                .partitionKey("Column2")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("Column1", DataTypes.BIGINT())
                                .physicalColumn("Column2", DataTypes.FLOAT())
                                .primaryKey("Column1")
                                .partitionKey("Column2")
                                .build()),
                Schema.newBuilder()
                        .physicalColumn("Column1", DataTypes.BIGINT())
                        .physicalColumn("Column2", DataTypes.DOUBLE())
                        .primaryKey("Column1")
                        .partitionKey("Column2")
                        .build());

        // Test merging with incompatible types
        Assert.assertThrows(
                IllegalStateException.class,
                () ->
                        SchemaUtils.mergeSchema(
                                Schema.newBuilder()
                                        .physicalColumn("Column1", DataTypes.INT())
                                        .physicalColumn("Column2", DataTypes.DOUBLE())
                                        .primaryKey("Column1")
                                        .partitionKey("Column2")
                                        .build(),
                                Schema.newBuilder()
                                        .physicalColumn("Column1", DataTypes.STRING())
                                        .physicalColumn("Column2", DataTypes.STRING())
                                        .primaryKey("Column1")
                                        .partitionKey("Column2")
                                        .build()));

        // Test merging with incompatible column names
        Assert.assertThrows(
                IllegalStateException.class,
                () ->
                        SchemaUtils.mergeSchema(
                                Schema.newBuilder()
                                        .physicalColumn("Column1", DataTypes.INT())
                                        .physicalColumn("Column2", DataTypes.DOUBLE())
                                        .primaryKey("Column1")
                                        .partitionKey("Column2")
                                        .build(),
                                Schema.newBuilder()
                                        .physicalColumn("NotColumn1", DataTypes.INT())
                                        .physicalColumn("NotColumn2", DataTypes.DOUBLE())
                                        .primaryKey("NotColumn1")
                                        .partitionKey("NotColumn2")
                                        .build()));

        // Test merging with different column counts
        Assert.assertThrows(
                IllegalStateException.class,
                () ->
                        SchemaUtils.mergeSchema(
                                Schema.newBuilder()
                                        .physicalColumn("Column1", DataTypes.INT())
                                        .physicalColumn("Column2", DataTypes.DOUBLE())
                                        .physicalColumn("Column3", DataTypes.STRING())
                                        .primaryKey("Column1")
                                        .partitionKey("Column2")
                                        .build(),
                                Schema.newBuilder()
                                        .physicalColumn("NotColumn1", DataTypes.INT())
                                        .physicalColumn("NotColumn2", DataTypes.DOUBLE())
                                        .primaryKey("NotColumn1")
                                        .partitionKey("NotColumn2")
                                        .build()));

        // Test merging with incompatible schema metadata
        Assert.assertThrows(
                IllegalStateException.class,
                () ->
                        SchemaUtils.mergeSchema(
                                Schema.newBuilder()
                                        .physicalColumn("Column1", DataTypes.INT())
                                        .physicalColumn("Column2", DataTypes.DOUBLE())
                                        .primaryKey("Column1")
                                        .partitionKey("Column2")
                                        .option("Key1", "Value1")
                                        .build(),
                                Schema.newBuilder()
                                        .physicalColumn("Column1", DataTypes.INT())
                                        .physicalColumn("Column2", DataTypes.DOUBLE())
                                        .primaryKey("Column2")
                                        .partitionKey("Column1")
                                        .option("Key2", "Value2")
                                        .build()));
    }
}
