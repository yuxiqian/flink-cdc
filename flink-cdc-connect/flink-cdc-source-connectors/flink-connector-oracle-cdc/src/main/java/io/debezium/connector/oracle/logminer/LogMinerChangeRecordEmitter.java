/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.oracle.logminer;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.util.TimestampUtils;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import oracle.jdbc.OracleTypes;
import oracle.sql.ROWID;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * Copied from Debezium 2.7.4.Final. Emits change records based on an event read from Oracle
 * LogMiner.
 *
 * <p>This class adds RowId support: the constructors accept a rowId, and the emitted change records
 * carry the rowId in a Connect header (header name {@code ROWID.class.getSimpleName()}). In
 * Debezium 1.9.8.Final this was achieved by overriding {@code getEmitConnectHeaders()}, but that
 * hook was removed upstream in 2.x. The header is now attached by wrapping the {@code Receiver} and
 * injecting the rowId header for the standard create/read/update/delete paths (i.e. when upstream
 * passes {@code null} headers), preserving the original behavior of not touching the
 * primary-key-change records that already carry their own headers.
 */
public class LogMinerChangeRecordEmitter extends BaseChangeRecordEmitter<Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerChangeRecordEmitter.class);

    private final Operation operation;
    private final String rowId;

    public LogMinerChangeRecordEmitter(
            OracleConnectorConfig connectorConfig,
            Partition partition,
            OffsetContext offset,
            Operation operation,
            Object[] oldValues,
            Object[] newValues,
            Table table,
            OracleDatabaseSchema schema,
            Clock clock,
            String rowId) {
        super(connectorConfig, partition, offset, schema, table, clock, oldValues, newValues);
        this.operation = operation;
        this.rowId = rowId;
    }

    public LogMinerChangeRecordEmitter(
            OracleConnectorConfig connectorConfig,
            Partition partition,
            OffsetContext offset,
            EventType eventType,
            Object[] oldValues,
            Object[] newValues,
            Table table,
            OracleDatabaseSchema schema,
            Clock clock,
            String rowId) {
        this(
                connectorConfig,
                partition,
                offset,
                getOperation(eventType),
                oldValues,
                newValues,
                table,
                schema,
                clock,
                rowId);
    }

    private static Operation getOperation(EventType eventType) {
        switch (eventType) {
            case INSERT:
                return Operation.CREATE;
            case UPDATE:
            case SELECT_LOB_LOCATOR:
            case XML_BEGIN:
                return Operation.UPDATE;
            case DELETE:
                return Operation.DELETE;
            default:
                throw new DebeziumException("Unsupported operation type: " + eventType);
        }
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    protected Object convertReselectPrimaryKeyColumn(
            Connection connection, Column column, Object value) {
        if (value instanceof String) {
            // LogMiner raw values are always string; otherwise generally null
            switch (column.jdbcType()) {
                case OracleTypes.TIMESTAMP:
                case OracleTypes.DATE:
                    final String formattedTimestamp =
                            TimestampUtils.toSqlCompliantFunctionCall((String) value);
                    if (!Strings.isNullOrBlank(formattedTimestamp)) {
                        value = convertValueViaQuery(connection, formattedTimestamp);
                    }
                    break;
                case OracleTypes.INTERVALYM:
                case OracleTypes.INTERVALDS:
                    // LogMiner provides these values in SQL-compliant query fragments
                    value = convertValueViaQuery(connection, (String) value);
                    break;
                default:
                    // no -op
                    break;
            }
        }
        return value;
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver receiver)
            throws InterruptedException {
        super.emitChangeRecords(schema, new RowIdHeaderReceiver(receiver));
    }

    /**
     * Wraps a {@link Receiver} to attach the Oracle rowId as a Connect header on the standard
     * change records. Mirrors the previous {@code getEmitConnectHeaders()} override: the rowId
     * header is only added when upstream passes no headers (the create/read/update/delete paths),
     * leaving primary-key-change records (which carry their own headers) untouched.
     */
    private class RowIdHeaderReceiver implements Receiver {

        private final Receiver delegate;

        private RowIdHeaderReceiver(Receiver delegate) {
            this.delegate = delegate;
        }

        @Override
        public void changeRecord(
                Partition partition,
                DataCollectionSchema schema,
                Operation operation,
                Object key,
                Struct value,
                OffsetContext offset,
                ConnectHeaders headers)
                throws InterruptedException {
            if (headers == null) {
                headers = new ConnectHeaders();
                headers.add(ROWID.class.getSimpleName(), new SchemaAndValue(null, rowId));
            }
            delegate.changeRecord(partition, schema, operation, key, value, offset, headers);
        }
    }
}
