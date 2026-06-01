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

import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.network.ServerException;
import io.debezium.DebeziumException;
import io.debezium.connector.binlog.BinlogStreamingChangeEventSource;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.mysql.util.ErrorMessageUtils;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Predicate;

/**
 * Copied from mysql-cdc to be compatible with OceanBase Binlog Service.
 *
 * <p>Copied from Debezium project(2.7.4.Final).
 *
 * <p>In Debezium 2.7 the MySQL connector was re-architected onto the shared {@code
 * debezium-connector-binlog} base module. This class is now a thin subclass of {@link
 * BinlogStreamingChangeEventSource} (most logic moved to that base) instead of a full copy of the
 * streaming change event source. It mirrors {@code flink-connector-mysql-cdc}'s re-derived {@code
 * MySqlStreamingChangeEventSource} and adds the single OceanBase-specific behavior described below.
 *
 * <p>The flink-cdc patches were re-derived onto this new structure as follows:
 *
 * <ul>
 *   <li><b>OceanBase Binlog Service heartbeat handling</b>: HEARTBEAT events must NOT be counted
 *       toward {@code BinlogOffsetContext#restartEventsToSkip}. In 1.9.8 this was expressed inline
 *       in {@code handleEvent} by guarding the {@code offsetContext.completeEvent()} call with
 *       {@code if (eventType != EventType.HEARTBEAT)}. In 2.7.4 {@code handleEvent} lives in the
 *       binlog base and relies on {@code private} state, so the guard cannot be inlined. Instead
 *       {@link #handleEvent(MySqlPartition, MySqlOffsetContext, ChangeEventSourceContext, Event)}
 *       is overridden to invoke {@code super.handleEvent(...)} and then, for HEARTBEAT events, undo
 *       the single {@code ++restartEventsToSkip} performed by {@code completeEvent()}. Since {@code
 *       completeEvent()} is the only difference between the 1.9.8 OceanBase path and the base path
 *       for a heartbeat, decrementing the post-call value by exactly one reproduces the original
 *       behavior in every case (including when {@code setEventPosition} resets the counter to 0).
 *   <li><b>FLINK-39149</b> (GTID merging for recovery when a starting offset was previously
 *       specified): the GTID filtering logic moved out of the streaming source into {@code
 *       io.debezium.connector.mysql.jdbc.MySqlConnection#filterGtidSet}, which the binlog base
 *       invokes. The EARLIEST/LATEST merge (via {@code GtidUtils}) now lives there.
 *   <li><b>Error message enrichment</b>: {@link #wrap(Throwable)} is overridden to enrich the
 *       exception message via {@link ErrorMessageUtils#optimizeErrorMessage(String)}.
 *   <li><b>FLINK-38846</b> (avoid O(n^2) processing of LinkedList row batches in {@code
 *       handleChange}): NOT re-applied. In 2.7.4 the row-batch loop lives in {@code
 *       BinlogStreamingChangeEventSource#handleChange}, which is {@code private} and relies on
 *       {@code private} state. It cannot be overridden without copying the entire base class, so
 *       the optimization is intentionally dropped here, matching the mysql-cdc module. Correctness
 *       is unaffected (the base uses an index-based loop); only the performance benefit for large
 *       LinkedList-backed row events is lost.
 * </ul>
 *
 * @author Jiri Pechanec
 */
public class MySqlStreamingChangeEventSource
        extends BinlogStreamingChangeEventSource<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(MySqlStreamingChangeEventSource.class);

    private final MySqlConnectorConfig connectorConfig;
    private GtidSet gtidSet;

    public MySqlStreamingChangeEventSource(
            MySqlConnectorConfig connectorConfig,
            BinlogConnectorConnection connection,
            EventDispatcher<MySqlPartition, TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            MySqlTaskContext taskContext,
            MySqlStreamingChangeEventSourceMetrics metrics,
            SnapshotterService snapshotterService) {
        super(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                taskContext,
                taskContext.getSchema(),
                metrics,
                snapshotterService);
        this.connectorConfig = connectorConfig;
    }

    /**
     * Skips counting HEARTBEAT events toward the restart-skip counter for the OceanBase Binlog
     * Service. See the class-level javadoc for the rationale and why this is expressed as an undo
     * of {@code completeEvent()} rather than an inline guard.
     */
    @Override
    protected void handleEvent(
            MySqlPartition partition,
            MySqlOffsetContext offsetContext,
            ChangeEventSourceContext context,
            Event event) {
        final boolean isHeartbeat =
                event != null
                        && event.getHeader() != null
                        && event.getHeader().getEventType() == EventType.HEARTBEAT;
        super.handleEvent(partition, offsetContext, context, event);
        if (isHeartbeat) {
            // Undo the single ++restartEventsToSkip performed by completeEvent() in the base, so
            // OceanBase Binlog Service heartbeats are not counted toward the restart skip offset.
            offsetContext.setInitialSkips(
                    offsetContext.eventsToSkipUponRestart() - 1,
                    offsetContext.rowsToSkipUponRestart());
        }
    }

    @Override
    protected void setEventTimestamp(Event event, long eventTs) {
        if (eventTimestamp == null || !isGtidModeEnabled()) {
            // Fallback to second resolution event timestamps
            eventTimestamp = Instant.ofEpochMilli(eventTs);
        } else if (event.getHeader().getEventType() == EventType.GTID) {
            // Prefer higher resolution replication timestamps from MySQL 8 GTID events, if possible
            GtidEventData gtidEvent = unwrapData(event);
            final long gtidEventTs = gtidEvent.getOriginalCommitTimestamp();
            if (gtidEventTs != 0) {
                // >= MySQL 8.0.1, prefer the higher resolution replication timestamp
                eventTimestamp = Instant.EPOCH.plus(gtidEventTs, ChronoUnit.MICROS);
            } else {
                // Fallback to second resolution event timestamps
                eventTimestamp = Instant.ofEpochMilli(eventTs);
            }
        }
    }

    @Override
    protected void handleGtidEvent(
            MySqlPartition partition,
            MySqlOffsetContext offsetContext,
            Event event,
            Predicate<String> gtidSourceFilter) {
        LOGGER.debug("GTID transaction: {}", event);
        GtidEventData gtidEvent = unwrapData(event);
        String gtid = gtidEvent.getGtid();
        gtidSet.add(gtid);
        offsetContext.startGtid(gtid, gtidSet.toString()); // rather than use the client's GTID set
        setIgnoreDmlEventByGtidSource(false);
        if (gtidSourceFilter != null && gtid != null) {
            String uuid = gtid.trim().substring(0, gtid.indexOf(":"));
            if (!gtidSourceFilter.test(uuid)) {
                setIgnoreDmlEventByGtidSource(true);
            }
        }
        setGtidChanged(gtid);
    }

    @Override
    protected void handleRecordingQuery(MySqlOffsetContext offsetContext, Event event) {
        final EventData eventData = unwrapData(event);
        if (eventData instanceof RowsQueryEventData) {
            final String query = ((RowsQueryEventData) eventData).getQuery();
            offsetContext.setQuery(query);
        }
    }

    @Override
    public void init(MySqlOffsetContext offsetContext) {
        setEffectiveOffsetContext(
                offsetContext != null
                        ? offsetContext
                        : MySqlOffsetContext.initial(connectorConfig));
    }

    @Override
    protected Class<? extends SourceConnector> getConnectorClass() {
        return MySqlConnector.class;
    }

    @Override
    protected EventType getIncludeQueryEventType() {
        return EventType.ROWS_QUERY;
    }

    @Override
    protected EventType getGtidEventType() {
        return EventType.GTID;
    }

    @Override
    protected void initializeGtidSet(String value) {
        this.gtidSet = new GtidSet(value);
    }

    /**
     * Wraps the specified exception in a {@link DebeziumException}, ensuring that all useful state
     * is captured inside the new exception's message, and enriches the message with flink-cdc
     * specific diagnostics via {@link ErrorMessageUtils#optimizeErrorMessage(String)}.
     *
     * @param error the exception; may not be null
     * @return the wrapped exception
     */
    @Override
    protected DebeziumException wrap(Throwable error) {
        assert error != null;
        String msg = error.getMessage();
        if (error instanceof ServerException) {
            ServerException e = (ServerException) error;
            msg = msg + " Error code: " + e.getErrorCode() + "; SQLSTATE: " + e.getSqlState() + ".";
        } else if (error instanceof SQLException) {
            SQLException e = (SQLException) error;
            msg =
                    e.getMessage()
                            + " Error code: "
                            + e.getErrorCode()
                            + "; SQLSTATE: "
                            + e.getSQLState()
                            + ".";
        }
        msg = ErrorMessageUtils.optimizeErrorMessage(msg);
        return new DebeziumException(msg, error);
    }
}
