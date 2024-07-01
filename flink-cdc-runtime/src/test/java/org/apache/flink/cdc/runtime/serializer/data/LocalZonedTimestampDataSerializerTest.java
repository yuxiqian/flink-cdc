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

package org.apache.flink.cdc.runtime.serializer.data;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import static org.apache.flink.cdc.common.data.LocalZonedTimestampData.isCompact;

/** A test for the {@link LocalZonedTimestampDataSerializer}. */
abstract class LocalZonedTimestampDataSerializerTest
        extends SerializerTestBase<LocalZonedTimestampData> {
    @Override
    protected TypeSerializer<LocalZonedTimestampData> createSerializer() {
        return new LocalZonedTimestampDataSerializer(getPrecision());
    }

    @Override
    protected int getLength() {
        return isCompact(getPrecision()) ? 8 : 12;
    }

    @Override
    protected Class<LocalZonedTimestampData> getTypeClass() {
        return LocalZonedTimestampData.class;
    }

    @Override
    protected LocalZonedTimestampData[] getTestData() {
        if (getPrecision() > 3) {
            return new LocalZonedTimestampData[] {
                LocalZonedTimestampData.fromEpochMillis(1, 1),
                LocalZonedTimestampData.fromEpochMillis(2, 2),
                LocalZonedTimestampData.fromEpochMillis(3, 3),
                LocalZonedTimestampData.fromEpochMillis(4, 4)
            };
        } else {
            return new LocalZonedTimestampData[] {
                LocalZonedTimestampData.fromEpochMillis(1),
                LocalZonedTimestampData.fromEpochMillis(2),
                LocalZonedTimestampData.fromEpochMillis(3),
                LocalZonedTimestampData.fromEpochMillis(4)
            };
        }
    }

    protected abstract int getPrecision();

    static final class LocalZonedTimestampSerializer0Test
            extends LocalZonedTimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 0;
        }
    }

    static final class LocalZonedTimestampSerializer3Test
            extends LocalZonedTimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 3;
        }
    }

    static final class LocalZonedTimestampSerializer6Test
            extends LocalZonedTimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 6;
        }
    }

    static final class LocalZonedTimestampSerializer8Test
            extends LocalZonedTimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 8;
        }
    }
}
