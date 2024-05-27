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

package org.apache.flink.cdc.connectors.mysql.debezium.task;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl;
import org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.StoppableChangeEventSourceContext;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;

import com.github.shyiko.mysql.binlog.event.Event;
import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSource;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Predicate;

import static org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffsetUtils.isNonStoppingOffset;

/**
 * Task to read all binlog for table and also supports read bounded (from lowWatermark to
 * highWatermark) binlog.
 */
public class MySqlBinlogSplitReadTask extends MySqlStreamingChangeEventSource {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplitReadTask.class);
    private final MySqlBinlogSplit binlogSplit;
    private final EventDispatcherImpl<TableId> eventDispatcher;
    private final SignalEventDispatcher signalEventDispatcher;
    private final ErrorHandler errorHandler;
    private final Predicate<Event> eventFilter;
    private ChangeEventSourceContext context;

    public MySqlBinlogSplitReadTask(
            MySqlConnectorConfig connectorConfig,
            MySqlConnection connection,
            EventDispatcherImpl<TableId> dispatcher,
            SignalEventDispatcher signalEventDispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            MySqlTaskContext taskContext,
            MySqlStreamingChangeEventSourceMetrics metrics,
            MySqlBinlogSplit binlogSplit,
            Predicate<Event> eventFilter) {
        super(connectorConfig, connection, dispatcher, errorHandler, clock, taskContext, metrics);
        this.binlogSplit = binlogSplit;
        this.eventDispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.signalEventDispatcher = signalEventDispatcher;
        this.eventFilter = eventFilter;
    }

    @Override
    public void execute(
            ChangeEventSourceContext context,
            MySqlPartition partition,
            MySqlOffsetContext offsetContext)
            throws InterruptedException {
        this.context = context;
        super.execute(context, partition, offsetContext);
    }

    @Override
    protected void handleEvent(
            MySqlPartition partition, MySqlOffsetContext offsetContext, Event event) {
        if (!eventFilter.test(event)) {
            return;
        }
        super.handleEvent(partition, offsetContext, event);
        // check do we need to stop for read binlog for snapshot split.
        if (isBoundedRead()) {
            final BinlogOffset currentBinlogOffset =
                    RecordUtils.getBinlogPosition(offsetContext.getOffset());
            // reach the high watermark, the binlog reader should finished
            if (currentBinlogOffset.isAtOrAfter(binlogSplit.getEndingOffset())) {
                // send binlog end event
                try {
                    signalEventDispatcher.dispatchWatermarkEvent(
                            binlogSplit,
                            currentBinlogOffset,
                            SignalEventDispatcher.WatermarkKind.BINLOG_END);
                } catch (InterruptedException e) {
                    LOG.error("Send signal event error.", e);
                    errorHandler.setProducerThrowable(
                            new DebeziumException("Error processing binlog signal event", e));
                }
                // tell reader the binlog task finished
                ((StoppableChangeEventSourceContext) context).stopChangeEventSource();
            }
        }
    }

    private boolean isBoundedRead() {
        return !isNonStoppingOffset(binlogSplit.getEndingOffset());
    }

    @VisibleForTesting
    public Predicate<Event> getEventFilter() {
        return eventFilter;
    }
}
