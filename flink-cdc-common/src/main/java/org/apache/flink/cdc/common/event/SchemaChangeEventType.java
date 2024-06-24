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

package org.apache.flink.cdc.common.event;

/** An enumeration of schema change event types for {@link SchemaChangeEvent}. */
public enum SchemaChangeEventType {
    ADD_COLUMN,
    ALTER_COLUMN_TYPE,
    CREATE_TABLE,
    DROP_COLUMN,
    RENAME_COLUMN;

    public static SchemaChangeEventType ofEvent(SchemaChangeEvent event) {
        if (event instanceof AddColumnEvent) {
            return ADD_COLUMN;
        } else if (event instanceof AlterColumnTypeEvent) {
            return ALTER_COLUMN_TYPE;
        } else if (event instanceof CreateTableEvent) {
            return CREATE_TABLE;
        } else if (event instanceof DropColumnEvent) {
            return DROP_COLUMN;
        } else if (event instanceof RenameColumnEvent) {
            return RENAME_COLUMN;
        } else {
            throw new RuntimeException("Unknown schema change event type: " + event.getClass());
        }
    }

    public static SchemaChangeEventType ofTag(String tag) {
        switch (tag) {
            case "add.column":
                return ADD_COLUMN;
            case "alter.column.type":
                return ALTER_COLUMN_TYPE;
            case "create.table":
                return CREATE_TABLE;
            case "drop.column":
                return DROP_COLUMN;
            case "rename.column":
                return RENAME_COLUMN;
            default:
                return null;
        }
    }
}
