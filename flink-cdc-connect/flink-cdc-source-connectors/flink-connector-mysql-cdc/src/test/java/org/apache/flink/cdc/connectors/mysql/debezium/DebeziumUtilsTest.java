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

package org.apache.flink.cdc.connectors.mysql.debezium;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;

import io.debezium.connector.mysql.jdbc.MySqlConnection;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Properties;

/** Tests for {@link org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils}. */
class DebeziumUtilsTest {
    @Test
    void testCreateMySqlConnection() {
        // test without set useSSL.
        // Since Debezium 2.7.x the jdbc url is rendered from
        // io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration#URL_PATTERN, which lists
        // the
        // default jdbc properties in a different order than the pre-2.7 flink-cdc patch. When the
        // user does not override useSSL, SSL is still disabled by default, but that is now conveyed
        // through the "sslMode=disabled" connection property (passed to the driver, not rendered in
        // the connection string) instead of the legacy "useSSL=false" url parameter, so the no-SSL
        // case no longer contains a "useSSL" entry in the url.
        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("onlyTest", "test");
        MySqlSourceConfig configWithoutUseSSL = getConfig(jdbcProps);
        MySqlConnection connection0 = DebeziumUtils.createMySqlConnection(configWithoutUseSSL);
        assertJdbcUrl(
                "jdbc:mysql://localhost:3306/?useInformationSchema=true&nullCatalogMeansCurrent=false"
                        + "&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8"
                        + "&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=20000&onlyTest=test",
                connection0.connectionString());

        // test with set useSSL=false
        jdbcProps.setProperty("useSSL", "false");
        MySqlSourceConfig configNotUseSSL = getConfig(jdbcProps);
        MySqlConnection connection1 = DebeziumUtils.createMySqlConnection(configNotUseSSL);
        assertJdbcUrl(
                "jdbc:mysql://localhost:3306/?useInformationSchema=true&nullCatalogMeansCurrent=false"
                        + "&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8"
                        + "&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=20000&useSSL=false&onlyTest=test",
                connection1.connectionString());

        // test with set useSSL=true
        jdbcProps.setProperty("useSSL", "true");
        MySqlSourceConfig configUseSSL = getConfig(jdbcProps);
        MySqlConnection connection2 = DebeziumUtils.createMySqlConnection(configUseSSL);
        assertJdbcUrl(
                "jdbc:mysql://localhost:3306/?useInformationSchema=true&nullCatalogMeansCurrent=false"
                        + "&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8"
                        + "&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=20000&useSSL=true&onlyTest=test",
                connection2.connectionString());
    }

    private MySqlSourceConfig getConfig(Properties jdbcProperties) {
        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.initial())
                .databaseList("fakeDb")
                .tableList("fakeDb.fakeTable")
                .includeSchemaChanges(false)
                .hostname("localhost")
                .port(3306)
                .splitSize(10)
                .fetchSize(2)
                .connectTimeout(Duration.ofSeconds(20))
                .username("fakeUser")
                .password("fakePw")
                .serverTimeZone(ZoneId.of("UTC").toString())
                .jdbcProperties(jdbcProperties)
                .createConfig(0);
    }

    private void assertJdbcUrl(String expected, String actual) {
        // Compare after splitting to avoid the orderless jdbc parameters in jdbc url at Java 11
        String[] expectedParam = expected.split("&");
        String[] actualParam = actual.split("&");
        Assertions.assertThat(actualParam).containsExactlyInAnyOrder(expectedParam);
    }
}
