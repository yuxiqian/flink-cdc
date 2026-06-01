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

package io.debezium.connector.mysql.gtid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.debezium.connector.mysql.gtid.GtidUtils.computeLatestModeGtidSet;
import static io.debezium.connector.mysql.gtid.GtidUtils.fixRestoredGtidSet;
import static io.debezium.connector.mysql.gtid.GtidUtils.mergeGtidSetInto;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link GtidUtils}.
 *
 * <p>Migrated to the {@code io.debezium.connector.mysql.gtid} package and the {@link MySqlGtidSet}
 * type as part of the Debezium 2.7.4 upgrade ({@code io.debezium.connector.mysql.GtidSet} became
 * {@code io.debezium.connector.mysql.gtid.MySqlGtidSet}).
 */
class GtidUtilsTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("gtidSetsProvider")
    void testFixingRestoredGtidSet(
            String description, String serverStr, String restoredStr, String expectedStr) {
        MySqlGtidSet serverGtidSet = new MySqlGtidSet(serverStr);
        MySqlGtidSet restoredGtidSet = new MySqlGtidSet(restoredStr);

        MySqlGtidSet result = fixRestoredGtidSet(serverGtidSet, restoredGtidSet);

        assertThat(result).hasToString(expectedStr);
    }

    private static Stream<Arguments> gtidSetsProvider() {
        return Stream.of(
                Arguments.of(
                        "Basic example with a straightforward subset",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-50:63-100",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-50:63-100"),
                Arguments.of(
                        "Multiple intervals with gaps in restored",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:45-80:83-90:92-98",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-80:83-90:92-98"),
                Arguments.of(
                        "Server has disjoint intervals, restored partially overlaps",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-50:60-90:95-200",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:45-50:65-70:96-100",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-50:65-70:96-100"),
                Arguments.of(
                        "Restored partially covers server range",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100:102-200",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:106-150:152-200",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100:102-150:152-200"),
                Arguments.of(
                        "Restored end exceeds server range",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-200:205-300",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-110,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-201:210-230:245-305",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-200:210-230:245-300"),
                Arguments.of(
                        "Multiple UUIDs with different overlaps",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-50",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:45-80,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:30-60,cccccccc-cccc-cccc-cccc-cccccccccccc:1-20",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-80,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-50,cccccccc-cccc-cccc-cccc-cccccccccccc:1-20"),
                Arguments.of(
                        "Restored starts after server ends",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:80-150",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100"),
                Arguments.of(
                        "Complex overlapping intervals",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-20:30-50:60-80",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:15-35:45-65:75-85",
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-20:30-35:45-50:60-65:75-80"));
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} for FLINK-39149. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("latestModeGtidSetsProvider")
    void testLatestModeGtidMerge(
            String description,
            String serverGtidStr,
            String checkpointGtidStr,
            String expectedMergedStr) {
        MySqlGtidSet serverGtidSet = new MySqlGtidSet(serverGtidStr);
        MySqlGtidSet checkpointGtidSet = new MySqlGtidSet(checkpointGtidStr);

        MySqlGtidSet mergedGtidSet =
                computeLatestModeGtidSet(
                        serverGtidSet, new MySqlGtidSet(""), checkpointGtidSet, null);

        assertThat(mergedGtidSet).hasToString(expectedMergedStr);

        // Verify MySQL would not replay pre-checkpoint transactions
        MySqlGtidSet transactionsToSend = serverGtidSet.subtract(mergedGtidSet);
        for (MySqlGtidSet.UUIDSet uuidSet : checkpointGtidSet.getUUIDSets()) {
            String uuid = uuidSet.getUUID();
            long earliestCheckpointTx =
                    uuidSet.getIntervals().stream()
                            .mapToLong(MySqlGtidSet.Interval::getStart)
                            .min()
                            .orElse(1);
            if (earliestCheckpointTx > 1) {
                MySqlGtidSet.UUIDSet toSendUuidSet = transactionsToSend.forServerWithId(uuid);
                if (toSendUuidSet != null) {
                    for (MySqlGtidSet.Interval interval : toSendUuidSet.getIntervals()) {
                        assertThat(interval.getStart())
                                .as(
                                        "Should not replay pre-checkpoint transactions for UUID %s",
                                        uuid)
                                .isGreaterThan(earliestCheckpointTx);
                    }
                }
            }
        }
    }

    private static Stream<Arguments> latestModeGtidSetsProvider() {
        return Stream.of(
                Arguments.of(
                        "Old channel with non-contiguous GTID, new channel present",
                        "11111111-1111-1111-1111-111111111111:1-10000,22222222-2222-2222-2222-222222222222:1-3000",
                        "11111111-1111-1111-1111-111111111111:5000-8000",
                        "11111111-1111-1111-1111-111111111111:1-8000,22222222-2222-2222-2222-222222222222:1-3000"),
                Arguments.of(
                        "Mixed old channels (contiguous and non-contiguous) with new channel",
                        "11111111-1111-1111-1111-111111111111:1-10000,22222222-2222-2222-2222-222222222222:1-3000,33333333-3333-3333-3333-333333333333:1-5000",
                        "11111111-1111-1111-1111-111111111111:5000-8000,22222222-2222-2222-2222-222222222222:1-2000",
                        "11111111-1111-1111-1111-111111111111:1-8000,22222222-2222-2222-2222-222222222222:1-2000,33333333-3333-3333-3333-333333333333:1-5000"),
                Arguments.of(
                        "All old channels, no new channels",
                        "11111111-1111-1111-1111-111111111111:1-10000,22222222-2222-2222-2222-222222222222:1-3000",
                        "11111111-1111-1111-1111-111111111111:1-8000,22222222-2222-2222-2222-222222222222:1-2000",
                        "11111111-1111-1111-1111-111111111111:1-8000,22222222-2222-2222-2222-222222222222:1-2000"),
                Arguments.of(
                        "Contiguous checkpoint GTID, no regression",
                        "11111111-1111-1111-1111-111111111111:1-10000,22222222-2222-2222-2222-222222222222:1-3000",
                        "11111111-1111-1111-1111-111111111111:1-8000",
                        "11111111-1111-1111-1111-111111111111:1-8000,22222222-2222-2222-2222-222222222222:1-3000"),
                Arguments.of(
                        "Only new channels, checkpoint has unknown UUID",
                        "11111111-1111-1111-1111-111111111111:1-10000,22222222-2222-2222-2222-222222222222:1-3000",
                        "99999999-9999-9999-9999-999999999999:1-500",
                        "11111111-1111-1111-1111-111111111111:1-10000,22222222-2222-2222-2222-222222222222:1-3000,99999999-9999-9999-9999-999999999999:1-500"));
    }

    @Test
    void testMergingGtidSets() {
        MySqlGtidSet base = new MySqlGtidSet("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100");
        MySqlGtidSet toMerge = new MySqlGtidSet("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10");
        assertThat(mergeGtidSetInto(base, toMerge))
                .hasToString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100");

        base = new MySqlGtidSet("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100");
        toMerge = new MySqlGtidSet("cccccccc-cccc-cccc-cccc-cccccccccccc:1-10");
        assertThat(mergeGtidSetInto(base, toMerge))
                .hasToString(
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,cccccccc-cccc-cccc-cccc-cccccccccccc:1-10");
        base =
                new MySqlGtidSet(
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-100");
        toMerge =
                new MySqlGtidSet(
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10,cccccccc-cccc-cccc-cccc-cccccccccccc:1-10");
        assertThat(mergeGtidSetInto(base, toMerge))
                .hasToString(
                        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-100,cccccccc-cccc-cccc-cccc-cccccccccccc:1-10");
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} with {@code gtidSourceFilter}. */
    @Test
    void testLatestModeGtidMergeWithSourceFilter() {
        MySqlGtidSet availableServerGtidSet =
                new MySqlGtidSet(
                        "11111111-1111-1111-1111-111111111111:1-10000,22222222-2222-2222-2222-222222222222:1-3000,33333333-3333-3333-3333-333333333333:1-5000");
        MySqlGtidSet checkpointGtidSet =
                new MySqlGtidSet(
                        "11111111-1111-1111-1111-111111111111:5000-8000,22222222-2222-2222-2222-222222222222:1-2000");
        Predicate<String> gtidSourceFilter =
                uuid -> !uuid.equals("33333333-3333-3333-3333-333333333333");

        MySqlGtidSet mergedGtidSet =
                computeLatestModeGtidSet(
                        availableServerGtidSet,
                        new MySqlGtidSet(""),
                        checkpointGtidSet,
                        gtidSourceFilter);

        assertThat(mergedGtidSet.toString())
                .contains("11111111-1111-1111-1111-111111111111:1-8000");
        assertThat(mergedGtidSet.toString())
                .contains("22222222-2222-2222-2222-222222222222:1-2000");
        assertThat(mergedGtidSet.toString()).doesNotContain("33333333-3333-3333-3333-333333333333");
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} with purged GTID. */
    @Test
    void testLatestModeGtidMergeWithPurgedGtid() {
        MySqlGtidSet availableServerGtidSet =
                new MySqlGtidSet(
                        "11111111-1111-1111-1111-111111111111:50-10000,22222222-2222-2222-2222-222222222222:1-3000");
        MySqlGtidSet purgedServerGtid =
                new MySqlGtidSet("11111111-1111-1111-1111-111111111111:1-49");
        MySqlGtidSet checkpointGtidSet =
                new MySqlGtidSet("11111111-1111-1111-1111-111111111111:5000-8000");

        MySqlGtidSet mergedGtidSet =
                computeLatestModeGtidSet(
                        availableServerGtidSet, purgedServerGtid, checkpointGtidSet, null);

        assertThat(mergedGtidSet.toString())
                .contains("11111111-1111-1111-1111-111111111111:50-8000");
        assertThat(mergedGtidSet.toString())
                .contains("22222222-2222-2222-2222-222222222222:1-3000");

        // Verify no pre-checkpoint replay
        MySqlGtidSet transactionsToSend = availableServerGtidSet.subtract(mergedGtidSet);
        MySqlGtidSet.UUIDSet aaaToSend =
                transactionsToSend.forServerWithId("11111111-1111-1111-1111-111111111111");
        if (aaaToSend != null) {
            for (MySqlGtidSet.Interval interval : aaaToSend.getIntervals()) {
                assertThat(interval.getStart())
                        .as("Should not request pre-checkpoint transactions")
                        .isGreaterThanOrEqualTo(8001);
            }
        }
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} with a completely purged UUID. */
    @Test
    void testLatestModeGtidMergeWithFullyPurgedChannel() {
        MySqlGtidSet availableServerGtidSet =
                new MySqlGtidSet("22222222-2222-2222-2222-222222222222:1-3000");
        MySqlGtidSet purgedServerGtid =
                new MySqlGtidSet("11111111-1111-1111-1111-111111111111:1-500");
        MySqlGtidSet checkpointGtidSet =
                new MySqlGtidSet("11111111-1111-1111-1111-111111111111:200-400");

        MySqlGtidSet mergedGtidSet =
                computeLatestModeGtidSet(
                        availableServerGtidSet, purgedServerGtid, checkpointGtidSet, null);

        assertThat(mergedGtidSet.toString()).contains("11111111-1111-1111-1111-111111111111:1-400");
        assertThat(mergedGtidSet.toString())
                .contains("22222222-2222-2222-2222-222222222222:1-3000");
    }
}
