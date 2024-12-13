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

package org.apache.flink.cdc.runtime.operators.schema.event;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

/**
 * A {@link OperatorEvent} from sink writer to notify {@link SchemaRegistry} that it finished
 * flushing.
 */
public class FlushSuccessEvent implements OperatorEvent {
    private static final long serialVersionUID = 1L;

    /** The sink subtask finished flushing. */
    private final int subtask;

    /** The schema changes from which table is executing it. */
    private final TableId tableId;

    /**
     * Nonce code to distinguish flush events corresponding to each schema change event from
     * different subTasks.
     */
    private final long nonce;

    public FlushSuccessEvent(int subtask, TableId tableId, long nonce) {
        this.subtask = subtask;
        this.tableId = tableId;
        this.nonce = nonce;
    }

    public int getSubtask() {
        return subtask;
    }

    public TableId getTableId() {
        return tableId;
    }

    public long getNonce() {
        return nonce;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FlushSuccessEvent)) {
            return false;
        }
        FlushSuccessEvent that = (FlushSuccessEvent) o;
        return subtask == that.subtask
                && Objects.equals(tableId, that.tableId)
                && nonce == that.nonce;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtask, tableId, nonce);
    }
}
