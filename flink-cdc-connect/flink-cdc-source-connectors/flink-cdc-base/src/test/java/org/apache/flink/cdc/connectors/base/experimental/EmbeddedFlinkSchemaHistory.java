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

package org.apache.flink.cdc.connectors.base.experimental;

import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link SchemaHistory} implementation which store the latest table schema in Flink state.
 *
 * <p>It stores/recovers history using data offered by {@link SourceSplitState}.
 */
public class EmbeddedFlinkSchemaHistory implements SchemaHistory {

    public static final String DATABASE_HISTORY_INSTANCE_NAME = "database.history.instance.name";

    public static final ConcurrentMap<String, Collection<TableChange>> TABLE_SCHEMAS =
            new ConcurrentHashMap<>();

    private Map<TableId, TableChange> tableSchemas;
    private SchemaHistoryListener listener;
    private boolean storeOnlyMonitoredTablesDdl;
    private boolean skipUnparseableDDL;

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            SchemaHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        this.listener = listener;
        this.storeOnlyMonitoredTablesDdl = config.getBoolean(STORE_ONLY_CAPTURED_TABLES_DDL);
        this.skipUnparseableDDL = config.getBoolean(SKIP_UNPARSEABLE_DDL_STATEMENTS);

        // recover
        String instanceName = config.getString(DATABASE_HISTORY_INSTANCE_NAME);
        this.tableSchemas = new HashMap<>();
        for (TableChange tableChange : removeHistory(instanceName)) {
            tableSchemas.put(tableChange.getId(), tableChange);
        }
    }

    @Override
    public void start() {
        listener.started();
    }

    @Override
    public void record(
            Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl)
            throws SchemaHistoryException {
        throw new UnsupportedOperationException("should not call here, error");
    }

    @Override
    public void record(
            Map<String, ?> source,
            Map<String, ?> position,
            String databaseName,
            String schemaName,
            String ddl,
            TableChanges changes,
            Instant timestamp)
            throws SchemaHistoryException {
        final HistoryRecord record =
                new HistoryRecord(
                        source, position, databaseName, schemaName, ddl, changes, timestamp);
        listener.onChangeApplied(record);
    }

    @Override
    public void recover(
            Map<String, ?> source, Map<String, ?> position, Tables schema, DdlParser ddlParser) {
        listener.recoveryStarted();
        for (TableChange tableChange : tableSchemas.values()) {
            schema.overwriteTable(tableChange.getTable());
        }
        listener.recoveryStopped();
    }

    @Override
    public void recover(
            Map<Map<String, ?>, Map<String, ?>> offsets, Tables schema, DdlParser ddlParser) {
        offsets.forEach((source, position) -> recover(source, position, schema, ddlParser));
    }

    @Override
    public void stop() {
        listener.stopped();
    }

    @Override
    public boolean exists() {
        return tableSchemas != null && !tableSchemas.isEmpty();
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public void initializeStorage() {
        // do nothing
    }

    public boolean storeOnlyCapturedTables() {
        return storeOnlyMonitoredTablesDdl;
    }

    public boolean skipUnparseableDdlStatements() {
        return skipUnparseableDDL;
    }

    public static void registerHistory(String engineName, Collection<TableChange> engineHistory) {
        TABLE_SCHEMAS.put(engineName, engineHistory);
    }

    public static Collection<TableChange> removeHistory(String engineName) {
        if (engineName == null) {
            return Collections.emptyList();
        }
        Collection<TableChange> tableChanges = TABLE_SCHEMAS.remove(engineName);
        return tableChanges != null ? tableChanges : Collections.emptyList();
    }
}
