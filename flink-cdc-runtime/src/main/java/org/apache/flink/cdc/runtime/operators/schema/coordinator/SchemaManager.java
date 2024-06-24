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

package org.apache.flink.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/**
 * Schema manager handles schema changes for tables, and manages historical schema versions of
 * tables.
 */
@Internal
public class SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);
    private static final int INITIAL_SCHEMA_VERSION = 0;
    private static final int VERSIONS_TO_KEEP = 3;
    private final SchemaChangeBehavior behavior;

    // Serializer for checkpointing
    public static final Serializer SERIALIZER = new Serializer();

    // Schema management
    private final Map<TableId, SortedMap<Integer, Schema>> upstreamSchemas;

    // Schema management
    private final Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas;

    public SchemaManager() {
        evolvedSchemas = new HashMap<>();
        upstreamSchemas = new HashMap<>();
        behavior = SchemaChangeBehavior.EVOLVE;
    }

    public SchemaManager(SchemaChangeBehavior behavior) {
        evolvedSchemas = new HashMap<>();
        upstreamSchemas = new HashMap<>();
        this.behavior = behavior;
    }

    public SchemaManager(
            Map<TableId, SortedMap<Integer, Schema>> upstreamSchemas,
            Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas,
            SchemaChangeBehavior behavior) {
        this.evolvedSchemas = evolvedSchemas;
        this.upstreamSchemas = upstreamSchemas;
        this.behavior = behavior;
    }

    public SchemaChangeBehavior getBehavior() {
        return behavior;
    }

    public final boolean schemaExists(
            Map<TableId, SortedMap<Integer, Schema>> schemaMap, TableId tableId) {
        return schemaMap.containsKey(tableId) && !schemaMap.get(tableId).isEmpty();
    }

    public final boolean upstreamSchemaExists(TableId tableId) {
        return schemaExists(upstreamSchemas, tableId);
    }

    public final boolean evolvedSchemaExists(TableId tableId) {
        return schemaExists(evolvedSchemas, tableId);
    }

    /** Get the latest evolved schema of the specified table. */
    public Optional<Schema> getLatestEvolvedSchema(TableId tableId) {
        return getLatestSchemaVersion(evolvedSchemas, tableId)
                .map(version -> evolvedSchemas.get(tableId).get(version));
    }

    /** Get the latest upstream schema of the specified table. */
    public Optional<Schema> getLatestUpstreamSchema(TableId tableId) {
        return getLatestSchemaVersion(upstreamSchemas, tableId)
                .map(version -> upstreamSchemas.get(tableId).get(version));
    }

    /** Get schema at the specified version of a table. */
    public Schema getEvolvedSchema(TableId tableId, int version) {
        checkArgument(
                evolvedSchemas.containsKey(tableId),
                "Unable to find evolved schema for table \"%s\"",
                tableId);
        SortedMap<Integer, Schema> versionedSchemas = evolvedSchemas.get(tableId);
        checkArgument(
                versionedSchemas.containsKey(version),
                "Schema version %s does not exist for table \"%s\"",
                version,
                tableId);
        return versionedSchemas.get(version);
    }

    /** Get schema at the specified version of a table. */
    public Schema getUpstreamSchema(TableId tableId, int version) {
        checkArgument(
                upstreamSchemas.containsKey(tableId),
                "Unable to find upstream schema for table \"%s\"",
                tableId);
        SortedMap<Integer, Schema> versionedSchemas = upstreamSchemas.get(tableId);
        checkArgument(
                versionedSchemas.containsKey(version),
                "Schema version %s does not exist for table \"%s\"",
                version,
                tableId);
        return versionedSchemas.get(version);
    }

    /** Apply schema change to a table. */
    public void applyUpstreamSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        if (schemaChangeEvent instanceof CreateTableEvent) {
            handleCreateTableEvent(upstreamSchemas, ((CreateTableEvent) schemaChangeEvent));
        } else {
            Optional<Schema> optionalSchema = getLatestUpstreamSchema(schemaChangeEvent.tableId());
            checkArgument(
                    optionalSchema.isPresent(),
                    "Unable to apply SchemaChangeEvent for table \"%s\" without existing schema",
                    schemaChangeEvent.tableId());

            LOG.info("Handling upstream schema change event: {}", schemaChangeEvent);
            registerNewSchema(
                    upstreamSchemas,
                    schemaChangeEvent.tableId(),
                    SchemaUtils.applySchemaChangeEvent(optionalSchema.get(), schemaChangeEvent));
        }
    }

    /** Apply schema change to a table. */
    public void applyEvolvedSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        if (schemaChangeEvent instanceof CreateTableEvent) {
            handleCreateTableEvent(evolvedSchemas, ((CreateTableEvent) schemaChangeEvent));
        } else {
            Optional<Schema> optionalSchema = getLatestEvolvedSchema(schemaChangeEvent.tableId());
            checkArgument(
                    optionalSchema.isPresent(),
                    "Unable to apply SchemaChangeEvent for table \"%s\" without existing schema",
                    schemaChangeEvent.tableId());

            LOG.info("Handling evolved schema change event: {}", schemaChangeEvent);
            registerNewSchema(
                    evolvedSchemas,
                    schemaChangeEvent.tableId(),
                    SchemaUtils.applySchemaChangeEvent(optionalSchema.get(), schemaChangeEvent));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaManager that = (SchemaManager) o;
        return Objects.equals(upstreamSchemas, that.upstreamSchemas)
                && Objects.equals(evolvedSchemas, that.evolvedSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(upstreamSchemas, evolvedSchemas);
    }

    // -------------------------------- Helper functions -------------------------------------

    private Optional<Integer> getLatestSchemaVersion(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap, TableId tableId) {
        if (!schemaMap.containsKey(tableId)) {
            return Optional.empty();
        }
        try {
            return Optional.of(schemaMap.get(tableId).lastKey());
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }

    private void handleCreateTableEvent(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap, CreateTableEvent event) {
        checkArgument(
                !schemaExists(schemaMap, event.tableId()),
                "Unable to apply CreateTableEvent to an existing schema for table \"%s\"",
                event.tableId());
        LOG.info("Handling schema change event: {}", event);
        registerNewSchema(schemaMap, event.tableId(), event.getSchema());
    }

    private void registerNewSchema(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap,
            TableId tableId,
            Schema newSchema) {
        if (schemaExists(schemaMap, tableId)) {
            SortedMap<Integer, Schema> versionedSchemas = schemaMap.get(tableId);
            Integer latestVersion = versionedSchemas.lastKey();
            versionedSchemas.put(latestVersion + 1, newSchema);
            if (versionedSchemas.size() > VERSIONS_TO_KEEP) {
                versionedSchemas.remove(versionedSchemas.firstKey());
            }
        } else {
            TreeMap<Integer, Schema> versionedSchemas = new TreeMap<>();
            versionedSchemas.put(INITIAL_SCHEMA_VERSION, newSchema);
            schemaMap.putIfAbsent(tableId, versionedSchemas);
        }
    }

    /** Serializer for {@link SchemaManager}. */
    public static class Serializer implements SimpleVersionedSerializer<SchemaManager> {

        public static final int CURRENT_VERSION = 2;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(SchemaManager schemaManager) throws IOException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(baos)) {
                serializeSchemaMap(schemaManager.evolvedSchemas, out);
                serializeSchemaMap(schemaManager.upstreamSchemas, out);
                out.writeUTF(schemaManager.getBehavior().name());
                return baos.toByteArray();
            }
        }

        @Override
        public SchemaManager deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {
                switch (version) {
                    case 0:
                    case 1:
                        {
                            Map<TableId, SortedMap<Integer, Schema>> schemas =
                                    deserializeSchemaMap(version, in);
                            // In legacy mode, upstream schema and evolved schema never differs
                            return new SchemaManager(schemas, schemas, SchemaChangeBehavior.EVOLVE);
                        }
                    case 2:
                        {
                            Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas =
                                    deserializeSchemaMap(version, in);
                            Map<TableId, SortedMap<Integer, Schema>> upstreamSchemas =
                                    deserializeSchemaMap(version, in);
                            SchemaChangeBehavior behavior =
                                    SchemaChangeBehavior.valueOf(in.readUTF());
                            return new SchemaManager(upstreamSchemas, evolvedSchemas, behavior);
                        }
                    default:
                        throw new RuntimeException("Unknown serialize version: " + version);
                }
            }
        }
    }

    private static void serializeSchemaMap(
            Map<TableId, SortedMap<Integer, Schema>> schemaMap, DataOutputStream out)
            throws IOException {
        TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
        SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
        // Number of tables
        out.writeInt(schemaMap.size());
        for (Map.Entry<TableId, SortedMap<Integer, Schema>> tableSchema : schemaMap.entrySet()) {
            // Table ID
            TableId tableId = tableSchema.getKey();
            tableIdSerializer.serialize(tableId, new DataOutputViewStreamWrapper(out));

            // Schema with versions
            SortedMap<Integer, Schema> versionedSchemas = tableSchema.getValue();
            out.writeInt(versionedSchemas.size());
            for (Map.Entry<Integer, Schema> versionedSchema : versionedSchemas.entrySet()) {
                // Version
                Integer version = versionedSchema.getKey();
                out.writeInt(version);
                // Schema
                Schema schema = versionedSchema.getValue();
                schemaSerializer.serialize(schema, new DataOutputViewStreamWrapper(out));
            }
        }
    }

    private static Map<TableId, SortedMap<Integer, Schema>> deserializeSchemaMap(
            int version, DataInputStream in) throws IOException {
        TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
        SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
        // Total schema length
        int numTables = in.readInt();
        Map<TableId, SortedMap<Integer, Schema>> tableSchemas = new HashMap<>(numTables);
        for (int i = 0; i < numTables; i++) {
            // Table ID
            TableId tableId = tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
            // Schema with versions
            int numVersions = in.readInt();
            SortedMap<Integer, Schema> versionedSchemas = new TreeMap<>(Integer::compareTo);
            for (int j = 0; j < numVersions; j++) {
                // Version
                int schemaVersion = in.readInt();
                Schema schema =
                        schemaSerializer.deserialize(version, new DataInputViewStreamWrapper(in));
                versionedSchemas.put(schemaVersion, schema);
            }
            tableSchemas.put(tableId, versionedSchemas);
        }
        return tableSchemas;
    }
}
