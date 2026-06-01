/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.jdbc;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.BinlogFieldReader;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.gtid.GtidUtils;
import io.debezium.connector.mysql.gtid.MySqlGtidSet;
import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Predicate;

/**
 * Copied from Debezium project(2.7.4.Final) to add custom jdbc properties in the jdbc url and to
 * add MySQL 8.4+ compatibility.
 *
 * <p>In Debezium 2.7 the MySQL connector was re-architected onto the shared {@code
 * debezium-connector-binlog} base module, and this class moved from {@code
 * io.debezium.connector.mysql.MySqlConnection} to {@code
 * io.debezium.connector.mysql.jdbc.MySqlConnection}. It now extends {@link
 * BinlogConnectorConnection} instead of {@link JdbcConnection} directly, and most logic moved to
 * the base class.
 *
 * <p>The flink-cdc additions re-applied on top of the 2.7.4 base are:
 *
 * <ul>
 *   <li>The new constructor parameter {@code jdbcProperties} on the inner {@link
 *       MySqlConnectionConfiguration} is used to generate the jdbc url pattern and may overwrite
 *       the default values. {@link MySqlConnectionConfiguration} overrides {@link
 *       MySqlConnectionConfiguration#getUrlPattern()} and {@link
 *       MySqlConnectionConfiguration#factory()} so both the displayed connection string and the
 *       actual connection factory honor the custom properties.
 *   <li>A convenience single-argument constructor {@link MySqlConnection#MySqlConnection(
 *       MySqlConnectionConfiguration)} preserved from the original flink-cdc patch.
 *   <li>MySQL 8.4+ compatible probing fields {@link
 *       MySqlConnection#MYSQL_CLASSIC_SHOW_BINARY_LOG_STATEMENT}, {@link
 *       MySqlConnection#MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT} and field {@link
 *       MySqlConnection#showBinaryLogStatement}, exposed through {@link
 *       MySqlConnection#getShowBinaryLogStatement} and {@link
 *       MySqlConnection#probeShowBinaryLogStatement}. {@link MySqlConnection#knownGtidSet()} uses
 *       the probed statement so it keeps working on MySQL 8.4 where {@code SHOW MASTER STATUS} was
 *       removed in favor of {@code SHOW BINARY LOG STATUS}.
 * </ul>
 */
public class MySqlConnection extends BinlogConnectorConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnection.class);

    private static final String MYSQL_CLASSIC_SHOW_BINARY_LOG_STATEMENT = "SHOW MASTER STATUS";
    private static final String MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT = "SHOW BINARY LOG STATUS";
    private final String showBinaryLogStatement;

    /**
     * The {@code gtid.new.channel.position} option (removed as a typed Debezium Field in 2.7) is
     * read from the raw configuration to preserve the FLINK-39149 EARLIEST/LATEST behavior.
     */
    private static final String GTID_NEW_CHANNEL_POSITION_PROPERTY = "gtid.new.channel.position";

    private static final String GTID_NEW_CHANNEL_POSITION_EARLIEST = "earliest";

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param connectionConfig {@link MySqlConnectionConfiguration} instance, may not be null.
     * @param fieldReader binary or text protocol based readers
     */
    public MySqlConnection(
            MySqlConnectionConfiguration connectionConfig, BinlogFieldReader fieldReader) {
        super(connectionConfig, fieldReader);
        this.showBinaryLogStatement = probeShowBinaryLogStatement();
    }

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param connectionConfig {@link MySqlConnectionConfiguration} instance, may not be null.
     */
    public MySqlConnection(MySqlConnectionConfiguration connectionConfig) {
        this(connectionConfig, new MySqlTextProtocolFieldReader(null));
    }

    @Override
    public MySqlConnectionConfiguration connectionConfig() {
        return (MySqlConnectionConfiguration) super.connectionConfig();
    }

    @Override
    public boolean isGtidModeEnabled() {
        try {
            return queryAndMap(
                    "SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'",
                    rs -> {
                        if (rs.next()) {
                            return "ON".equalsIgnoreCase(rs.getString(2));
                        }
                        return false;
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID mode: ", e);
        }
    }

    @Override
    public GtidSet knownGtidSet() {
        try {
            return queryAndMap(
                    showBinaryLogStatement,
                    rs -> {
                        if (rs.next() && rs.getMetaData().getColumnCount() > 4) {
                            // GTID set, may be null, blank, or contain a GTID set
                            return new MySqlGtidSet(rs.getString(5));
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID mode: ", e);
        }
    }

    @Override
    public GtidSet subtractGtidSet(GtidSet set1, GtidSet set2) {
        try {
            return prepareQueryAndMap(
                    "SELECT GTID_SUBTRACT(?, ?)",
                    ps -> {
                        ps.setString(1, set1.toString());
                        ps.setString(2, set2.toString());
                    },
                    rs -> {
                        if (rs.next()) {
                            return new MySqlGtidSet(rs.getString(1));
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while executing GTID_SUBTRACT: ", e);
        }
    }

    @Override
    public GtidSet purgedGtidSet() {
        try {
            return queryAndMap(
                    "SELECT @@global.gtid_purged",
                    rs -> {
                        if (rs.next() && rs.getMetaData().getColumnCount() > 0) {
                            // GTID set, may be null, blank, or contain a GTID set
                            return new MySqlGtidSet(rs.getString(1));
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException(
                    "Unexpected error while looking at gtid_purged variable: ", e);
        }
    }

    /**
     * Re-derived FLINK-39149 GTID merging logic. In Debezium 2.7 the GTID filtering moved from the
     * streaming change event source into this connection method. The flink-cdc patch adjusts GTID
     * merging so a job can recover when it previously specified a starting offset (earliest /
     * timestamp / binlog-file), avoiding replay of pre-checkpoint transactions.
     *
     * <p>The {@code gtid.new.channel.position} (EARLIEST / LATEST) configuration option was removed
     * as a typed Debezium {@code Field} in 2.7, so it is read here directly from the raw
     * configuration (defaulting to {@code earliest}, the historical Debezium default). EARLIEST
     * delegates to {@link GtidUtils#fixOldChannelsGtidSet}; LATEST delegates to {@link
     * GtidUtils#computeLatestModeGtidSet}.
     */
    @Override
    public GtidSet filterGtidSet(
            Predicate<String> gtidSourceFilter,
            String offsetGtids,
            GtidSet availableServerGtidSet,
            GtidSet purgedServerGtidSet) {
        String gtidStr = offsetGtids;
        if (gtidStr == null) {
            return null;
        }
        LOGGER.info("Attempting to generate a filtered GTID set");
        LOGGER.info("GTID set from previous recorded offset: {}", gtidStr);
        MySqlGtidSet filteredGtidSet = new MySqlGtidSet(gtidStr);
        if (gtidSourceFilter != null) {
            filteredGtidSet = filteredGtidSet.retainAll(gtidSourceFilter);
            LOGGER.info(
                    "GTID set after applying GTID source includes/excludes to previous recorded offset: {}",
                    filteredGtidSet);
        }
        LOGGER.info("GTID set available on server: {}", availableServerGtidSet);

        final MySqlGtidSet availableServerSet = (MySqlGtidSet) availableServerGtidSet;
        final MySqlGtidSet purgedServerSet = (MySqlGtidSet) purgedServerGtidSet;

        final String channelPosition =
                connectionConfig()
                        .originalConfig()
                        .getString(
                                GTID_NEW_CHANNEL_POSITION_PROPERTY,
                                GTID_NEW_CHANNEL_POSITION_EARLIEST);

        MySqlGtidSet mergedGtidSet;
        if (GTID_NEW_CHANNEL_POSITION_EARLIEST.equalsIgnoreCase(channelPosition)) {
            LOGGER.info("Using first available positions for new GTID channels");
            final MySqlGtidSet relevantAvailableServerGtidSet =
                    (gtidSourceFilter != null)
                            ? availableServerSet.retainAll(gtidSourceFilter)
                            : availableServerSet;
            LOGGER.info(
                    "Relevant GTID set available on server: {}", relevantAvailableServerGtidSet);
            // Since the GTID recorded in the checkpoint represents the CDC-executed records, in
            // certain scenarios (such as when the startup mode is earliest/timestamp/binlogfile),
            // the recorded GTID may not start from the beginning. For example, A:300-500. However,
            // during job recovery, we usually only need to focus on the last consumed point instead
            // of consuming A:1-299. Therefore, some adjustments need to be made to the recorded
            // offset in the checkpoint, and the available GTID for other MySQL instances should be
            // completed.
            mergedGtidSet =
                    GtidUtils.fixOldChannelsGtidSet(
                            relevantAvailableServerGtidSet, purgedServerSet, filteredGtidSet);
        } else {
            LOGGER.info("Using latest positions for new GTID channels");
            mergedGtidSet =
                    GtidUtils.computeLatestModeGtidSet(
                            availableServerSet, purgedServerSet, filteredGtidSet, gtidSourceFilter);
        }

        LOGGER.info("Final merged GTID set to use when connecting to MySQL: {}", mergedGtidSet);
        return mergedGtidSet;
    }

    public String getShowBinaryLogStatement() {
        return showBinaryLogStatement;
    }

    private String probeShowBinaryLogStatement() {
        LOGGER.info("Probing binary log statement.");
        try {
            // Attempt to query
            query(MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT, rs -> {});
            LOGGER.info(
                    "Successfully found show binary log statement with `{}`.",
                    MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT);
            return MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT;
        } catch (SQLException e) {
            LOGGER.info(
                    "Probing with {} failed, fallback to classic {}. Caused by: {}",
                    MYSQL_NEW_SHOW_BINARY_LOG_STATEMENT,
                    MYSQL_CLASSIC_SHOW_BINARY_LOG_STATEMENT,
                    e.getMessage());
            return MYSQL_CLASSIC_SHOW_BINARY_LOG_STATEMENT;
        }
    }

    /**
     * Connection configuration to create a {@link MySqlConnection}.
     *
     * <p>Extends the upstream {@link io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration}
     * so that all of the 2.7.4 behavior (ssl handling, security hardening defaults, connection time
     * zone resolution, jdbc protocol handling) is preserved, while overriding {@link
     * #getUrlPattern()} and {@link #factory()} to append the user supplied {@code jdbcProperties}
     * to the connection url.
     */
    public static class MySqlConnectionConfiguration
            extends io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration {

        private final String urlPattern;
        private final JdbcConnection.ConnectionFactory factory;

        public MySqlConnectionConfiguration(Configuration config) {
            this(config, new Properties());
        }

        public MySqlConnectionConfiguration(Configuration config, Properties jdbcProperties) {
            super(config);
            this.urlPattern = formatJdbcUrl(jdbcProperties);
            final String driverClassName = config().getString(MySqlConnectorConfig.JDBC_DRIVER);
            this.factory =
                    JdbcConnection.patternBasedFactory(
                            urlPattern,
                            driverClassName,
                            getClass().getClassLoader(),
                            MySqlConnectorConfig.JDBC_PROTOCOL);
        }

        @Override
        public String getUrlPattern() {
            return urlPattern;
        }

        @Override
        public JdbcConnection.ConnectionFactory factory() {
            return factory;
        }

        private String formatJdbcUrl(Properties jdbcProperties) {
            // The upstream URL_PATTERN already contains the default jdbc properties
            // (useInformationSchema, nullCatalogMeansCurrent, useUnicode, characterEncoding,
            // characterSetResults, zeroDateTimeBehavior, connectTimeout). User supplied properties
            // are appended afterwards so they override the defaults and add custom entries.
            StringBuilder jdbcUrlStringBuilder =
                    new StringBuilder(
                            io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration
                                    .URL_PATTERN);
            if (jdbcProperties != null) {
                jdbcProperties.forEach(
                        (key, value) ->
                                jdbcUrlStringBuilder
                                        .append("&")
                                        .append(key)
                                        .append("=")
                                        .append(value));
            }
            return jdbcUrlStringBuilder.toString();
        }
    }
}
