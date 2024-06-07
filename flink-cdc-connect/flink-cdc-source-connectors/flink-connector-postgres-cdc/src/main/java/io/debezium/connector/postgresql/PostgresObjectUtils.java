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

package io.debezium.connector.postgresql;

import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;

/**
 * A factory for creating various Debezium objects
 *
 * <p>It is a hack to access package-private constructor in debezium.
 */
public class PostgresObjectUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresObjectUtils.class);

    /** Create a new PostgresSchema and initialize the content of the schema. */
    public static PostgresSchema newSchema(
            PostgresConnection connection,
            PostgresConnectorConfig config,
            TopicNamingStrategy<TableId> topicNamingStrategy,
            PostgresValueConverter valueConverter)
            throws SQLException {
        PostgresSchema schema =
                new PostgresSchema(
                        config,
                        connection.getDefaultValueConverter(),
                        topicNamingStrategy,
                        valueConverter);
        schema.refresh(connection, false);
        return schema;
    }

    public static PostgresTaskContext newTaskContext(
            PostgresConnectorConfig connectorConfig,
            PostgresSchema schema,
            TopicNamingStrategy<TableId> topicNamingStrategy) {
        return new PostgresTaskContext(connectorConfig, schema, topicNamingStrategy);
    }

    public static PostgresEventMetadataProvider newEventMetadataProvider() {
        return new PostgresEventMetadataProvider();
    }

    /**
     * Create a new PostgresVauleConverterBuilder instance and offer type registry for JDBC
     * connection.
     *
     * <p>It is created in this package because some methods (e.g., includeUnknownDatatypes) of
     * PostgresConnectorConfig is protected.
     */
    public static PostgresConnection.PostgresValueConverterBuilder newPostgresValueConverterBuilder(
            PostgresConnectorConfig config) {
        return typeRegistry ->
                PostgresValueConverter.of(config, StandardCharsets.UTF_8, typeRegistry);
    }

    // modified from
    // io.debezium.connector.postgresql.PostgresConnectorTask.createReplicationConnection.
    // pass connectorConfig instead of maxRetries and retryDelay as parameters.
    // - old: ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext,
    // int maxRetries, Duration retryDelay)
    // - new: ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext,
    // PostgresConnection postgresConnection, PostgresConnectorConfig
    // connectorConfig)
    public static ReplicationConnection createReplicationConnection(
            PostgresTaskContext taskContext,
            PostgresConnection postgresConnection,
            PostgresConnectorConfig connectorConfig) {
        int maxRetries = connectorConfig.maxRetries();
        Duration retryDelay = connectorConfig.retryDelay();

        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        while (retryCount <= maxRetries) {
            try {
                LOGGER.info("Creating a new replication connection for {}", taskContext);
                return taskContext.createReplicationConnection(postgresConnection);
            } catch (SQLException ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOGGER.error(
                            "Too many errors connecting to server. All {} retries failed.",
                            maxRetries);
                    throw new FlinkRuntimeException(ex);
                }

                LOGGER.warn(
                        "Error connecting to server; will attempt retry {} of {} after {} "
                                + "seconds. Exception message: {}",
                        retryCount,
                        maxRetries,
                        retryDelay.getSeconds(),
                        ex.getMessage());
                try {
                    metronome.pause();
                } catch (InterruptedException e) {
                    LOGGER.warn("Connection retry sleep interrupted by exception: " + e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        LOGGER.error("Failed to create replication connection after {} retries", maxRetries);
        throw new FlinkRuntimeException(
                "Failed to create replication connection for " + taskContext);
    }

    public static void waitForReplicationSlotReady(
            int retryTimes, PostgresConnection jdbcConnection, String slotName, String pluginName)
            throws SQLException {
        int count = 0;
        SlotState slotState = jdbcConnection.getReplicationSlotState(slotName, pluginName);

        while (slotState == null && count < retryTimes) {
            LOGGER.info("Waiting until the replication slot is ready ...");
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                // do nothing
            }
            count++;
            slotState = jdbcConnection.getReplicationSlotState(slotName, pluginName);
        }
        if (slotState == null) {
            throw new IllegalStateException(
                    String.format(
                            "The replication slot is not ready after %d seconds.", 2 * retryTimes));
        }
    }
}
