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

package org.apache.flink.cdc.connectors.base.experimental.fetch;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.experimental.EmbeddedFlinkSchemaHistory;
import org.apache.flink.cdc.connectors.base.experimental.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.base.experimental.handler.MySqlSchemaChangeEventHandler;
import org.apache.flink.cdc.connectors.base.experimental.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.base.experimental.utils.MySqlUtils;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.table.types.logical.RowType;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlChangeEventSourceMetricsFactory;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlErrorHandler;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Collect;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** The context for fetch task that fetching data of snapshot split from MySQL data source. */
public class MySqlSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceFetchTaskContext.class);
    private final MySqlConnection connection;
    private final BinaryLogClient binaryLogClient;
    private final MySqlEventMetadataProvider metadataProvider;

    private MySqlDatabaseSchema databaseSchema;
    private MySqlTaskContextImpl taskContext;
    private MySqlOffsetContext offsetContext;
    private SnapshotChangeEventSourceMetrics<MySqlPartition> snapshotChangeEventSourceMetrics;
    private MySqlStreamingChangeEventSourceMetrics streamingChangeEventSourceMetrics;
    private TopicNamingStrategy<TableId> topicNamingStrategy;
    private JdbcSourceEventDispatcher<MySqlPartition> dispatcher;
    private MySqlPartition mySqlPartition;
    private ChangeEventQueue<DataChangeEvent> queue;
    private MySqlErrorHandler errorHandler;

    public MySqlSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dataSourceDialect,
            MySqlConnection connection,
            BinaryLogClient binaryLogClient) {
        super(sourceConfig, dataSourceDialect);
        this.connection = connection;
        this.binaryLogClient = binaryLogClient;
        this.metadataProvider = new MySqlEventMetadataProvider();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        // initial stateful objects
        final MySqlConnectorConfig connectorConfig = getDbzConnectorConfig();
        final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();
        this.topicNamingStrategy =
                connectorConfig.getTopicNamingStrategy(MySqlConnectorConfig.TOPIC_NAMING_STRATEGY);
        EmbeddedFlinkSchemaHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedFlinkSchemaHistory.DATABASE_HISTORY_INSTANCE_NAME),
                sourceSplitBase.getTableSchemas().values());
        this.databaseSchema =
                MySqlUtils.createMySqlDatabaseSchema(connectorConfig, tableIdCaseInsensitive);
        this.offsetContext =
                loadStartingOffsetState(
                        new MySqlOffsetContext.Loader(connectorConfig), sourceSplitBase);
        this.mySqlPartition = new MySqlPartition(connectorConfig.getLogicalName(), null);

        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext =
                new MySqlTaskContextImpl(connectorConfig, databaseSchema, binaryLogClient);
        final int queueSize =
                sourceSplitBase.isSnapshotSplit()
                        ? Integer.MAX_VALUE
                        : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();
        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "mysql-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
        this.dispatcher =
                new JdbcSourceEventDispatcher<>(
                        connectorConfig,
                        topicNamingStrategy,
                        databaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster,
                        new MySqlSchemaChangeEventHandler());

        final MySqlChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
                new MySqlChangeEventSourceMetricsFactory(
                        new MySqlStreamingChangeEventSourceMetrics(
                                taskContext, queue, metadataProvider));
        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);
        this.streamingChangeEventSourceMetrics =
                (MySqlStreamingChangeEventSourceMetrics)
                        changeEventSourceMetricsFactory.getStreamingMetrics(
                                taskContext, queue, metadataProvider);
        this.errorHandler = new MySqlErrorHandler(connectorConfig, queue);
    }

    @Override
    public MySqlSourceConfig getSourceConfig() {
        return (MySqlSourceConfig) sourceConfig;
    }

    public MySqlConnection getConnection() {
        return connection;
    }

    public BinaryLogClient getBinaryLogClient() {
        return binaryLogClient;
    }

    @Override
    public MySqlPartition getPartition() {
        return mySqlPartition;
    }

    public MySqlTaskContextImpl getTaskContext() {
        return taskContext;
    }

    @Override
    public MySqlConnectorConfig getDbzConnectorConfig() {
        return (MySqlConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public MySqlOffsetContext getOffsetContext() {
        return offsetContext;
    }

    public SnapshotChangeEventSourceMetrics<MySqlPartition> getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    public MySqlStreamingChangeEventSourceMetrics getStreamingChangeEventSourceMetrics() {
        return streamingChangeEventSourceMetrics;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public MySqlDatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public RowType getSplitType(Table table) {
        return MySqlUtils.getSplitType(table);
    }

    @Override
    public JdbcSourceEventDispatcher<MySqlPartition> getDispatcher() {
        return dispatcher;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return MySqlUtils.getBinlogPosition(sourceRecord);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private MySqlOffsetContext loadStartingOffsetState(
            OffsetContext.Loader<MySqlOffsetContext> loader, SourceSplitBase mySqlSplit) {
        Offset offset =
                mySqlSplit.isSnapshotSplit()
                        ? BinlogOffset.INITIAL_OFFSET
                        : mySqlSplit.asStreamSplit().getStartingOffset();

        MySqlOffsetContext mySqlOffsetContext = loader.load(offset.getOffset());

        if (!isBinlogAvailable(mySqlOffsetContext)) {
            throw new IllegalStateException(
                    "The connector is trying to read binlog starting at "
                            + mySqlOffsetContext.getSourceInfo()
                            + ", but this is no longer "
                            + "available on the server. Reconfigure the connector to use a snapshot when needed.");
        }
        return mySqlOffsetContext;
    }

    private boolean isBinlogAvailable(MySqlOffsetContext offset) {
        String binlogFilename =
                offset.getSourceInfo().getString(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY);
        if (binlogFilename == null) {
            return true; // start at current position
        }
        if (binlogFilename.equals("")) {
            return true; // start at beginning
        }

        // Accumulate the available binlog filenames ...
        List<String> logNames = connection.availableBinlogFiles();

        // And compare with the one we're supposed to use ...
        boolean found = logNames.stream().anyMatch(binlogFilename::equals);
        if (!found) {
            LOG.info(
                    "Connector requires binlog file '{}', but MySQL only has {}",
                    binlogFilename,
                    String.join(", ", logNames));
        } else {
            LOG.info("MySQL has the binlog file '{}' required by the connector", binlogFilename);
        }
        return found;
    }

    private void validateAndLoadDatabaseHistory(
            MySqlOffsetContext offset, MySqlDatabaseSchema schema) {
        schema.initializeStorage();
        schema.recover(Offsets.of(mySqlPartition, offset));
    }

    /** A subclass implementation of {@link MySqlTaskContext} which reuses one BinaryLogClient. */
    public static class MySqlTaskContextImpl extends MySqlTaskContext {

        private final BinaryLogClient reusedBinaryLogClient;

        public MySqlTaskContextImpl(
                MySqlConnectorConfig config,
                MySqlDatabaseSchema schema,
                BinaryLogClient reusedBinaryLogClient) {
            super(config, schema);
            this.reusedBinaryLogClient = reusedBinaryLogClient;
        }

        @Override
        public BinaryLogClient getBinaryLogClient() {
            return reusedBinaryLogClient;
        }
    }

    /** Copied from debezium for accessing here. */
    public static class MySqlEventMetadataProvider implements EventMetadataProvider {
        public static final String SERVER_ID_KEY = "server_id";

        public static final String GTID_KEY = "gtid";
        public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
        public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
        public static final String BINLOG_ROW_IN_EVENT_OFFSET_KEY = "row";
        public static final String THREAD_KEY = "thread";
        public static final String QUERY_KEY = "query";

        @Override
        public Instant getEventTimestamp(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            final Long timestamp = sourceInfo.getInt64(AbstractSourceInfo.TIMESTAMP_KEY);
            return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
        }

        @Override
        public Map<String, String> getEventSourcePosition(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            return Collect.hashMapOf(
                    BINLOG_FILENAME_OFFSET_KEY,
                    sourceInfo.getString(BINLOG_FILENAME_OFFSET_KEY),
                    BINLOG_POSITION_OFFSET_KEY,
                    Long.toString(sourceInfo.getInt64(BINLOG_POSITION_OFFSET_KEY)),
                    BINLOG_ROW_IN_EVENT_OFFSET_KEY,
                    Integer.toString(sourceInfo.getInt32(BINLOG_ROW_IN_EVENT_OFFSET_KEY)));
        }

        @Override
        public String getTransactionId(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return ((MySqlOffsetContext) offset).getTransactionId();
        }
    }
}
