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

import java.util.Objects;

/**
 * An {@link Event} from {@code SchemaOperator} to notify {@code DataSinkWriterOperator} that it
 * start flushing.
 */
public class FlushEvent implements Event {

    /** The schema changes from which table. */
    private final TableId tableId;

    /**
     * The schema evolution version code that increases for each schema change request. Used for
     * distinguishing FlushEvents triggered by different schema change events.
     */
    private final long versionCode;

    public FlushEvent(TableId tableId, long versionCode) {
        this.tableId = tableId;
        this.versionCode = versionCode;
    }

    public TableId getTableId() {
        return tableId;
    }

    public long getVersionCode() {
        return versionCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FlushEvent)) {
            return false;
        }
        FlushEvent that = (FlushEvent) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(versionCode, that.versionCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, versionCode);
    }
}
