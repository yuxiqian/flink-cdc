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

package io.debezium.connector.mysql;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogSnapshotChangeEventSource;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.mysql.MySqlOffsetContext.Loader;
import io.debezium.connector.mysql.jdbc.MySqlConnection;
import io.debezium.function.BlockingConsumer;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Copied from Debezium project(2.7.4.Final) to fix MySQL 8.x compatibility.
 *
 * <p>In Debezium 2.7 the MySQL connector was re-architected onto the shared {@code
 * debezium-connector-binlog} base module. This class is now a thin subclass of {@link
 * BinlogSnapshotChangeEventSource} (most logic moved to that base) instead of a full copy of the
 * snapshot change event source.
 *
 * <p>The flink-cdc patch re-derived onto this new structure: {@link
 * #setOffsetContextBinlogPositionAndGtidDetailsForSnapshot} uses the probing method {@link
 * MySqlConnection#getShowBinaryLogStatement()} to determine the statement (either {@code SHOW
 * MASTER STATUS} or, on MySQL 8.4+, {@code SHOW BINARY LOG STATUS}) instead of the hard-coded
 * {@code SHOW MASTER STATUS} used by the upstream base, for MySQL 8.x compatibility.
 */
public class MySqlSnapshotChangeEventSource
        extends BinlogSnapshotChangeEventSource<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(MySqlSnapshotChangeEventSource.class);

    private final MySqlConnectorConfig connectorConfig;

    public MySqlSnapshotChangeEventSource(
            MySqlConnectorConfig connectorConfig,
            MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
            MySqlDatabaseSchema schema,
            EventDispatcher<MySqlPartition, TableId> dispatcher,
            Clock clock,
            MySqlSnapshotChangeEventSourceMetrics metrics,
            BlockingConsumer<Function<SourceRecord, SourceRecord>> lastEventProcessor,
            Runnable preSnapshotAction,
            NotificationService<MySqlPartition, MySqlOffsetContext> notificationService,
            SnapshotterService snapshotterService) {
        super(
                connectorConfig,
                connectionFactory,
                schema,
                dispatcher,
                clock,
                metrics,
                lastEventProcessor,
                preSnapshotAction,
                notificationService,
                snapshotterService);
        this.connectorConfig = connectorConfig;
    }

    @Override
    protected MySqlOffsetContext getInitialOffsetContext(BinlogConnectorConfig connectorConfig) {
        return MySqlOffsetContext.initial((MySqlConnectorConfig) connectorConfig);
    }

    @Override
    protected void setOffsetContextBinlogPositionAndGtidDetailsForSnapshot(
            MySqlOffsetContext offsetContext,
            BinlogConnectorConnection connection,
            SnapshotterService snapshotterService)
            throws Exception {
        LOGGER.info("Read binlog position of MySQL primary server");
        // flink-cdc: use the probed statement for MySQL 8.x (8.4 replaced SHOW MASTER STATUS with
        // SHOW BINARY LOG STATUS) instead of the hard-coded SHOW MASTER STATUS.
        final String showMasterStmt = ((MySqlConnection) connection).getShowBinaryLogStatement();
        connection.query(
                showMasterStmt,
                rs -> {
                    if (rs.next()) {
                        final String binlogFilename = rs.getString(1);
                        final long binlogPosition = rs.getLong(2);
                        offsetContext.setBinlogStartPoint(binlogFilename, binlogPosition);
                        if (rs.getMetaData().getColumnCount() > 4) {
                            // This column exists only in MySQL 5.6.5 or later ...
                            final String gtidSet =
                                    rs.getString(5); // GTID set, may be null, blank, or contain set
                            offsetContext.setCompletedGtidSet(gtidSet);
                            LOGGER.info(
                                    "\t using binlog '{}' at position '{}' and gtid '{}'",
                                    binlogFilename,
                                    binlogPosition,
                                    gtidSet);
                        } else {
                            LOGGER.info(
                                    "\t using binlog '{}' at position '{}'",
                                    binlogFilename,
                                    binlogPosition);
                        }
                    } else if (!snapshotterService.getSnapshotter().shouldStream()) {
                        LOGGER.warn(
                                "Failed retrieving binlog position, continuing as streaming CDC wasn't requested");
                    } else {
                        throw new DebeziumException(
                                "Cannot read the binlog filename and position via '"
                                        + showMasterStmt
                                        + "'. Make sure your server is correctly configured");
                    }
                });
    }

    @Override
    protected MySqlOffsetContext copyOffset(
            RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }
}
