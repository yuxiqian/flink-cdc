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

package org.apache.flink.cdc.connectors.hls.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** Serializer for {@link HlsEnumeratorState}. */
public class HlsEnumeratorStateSerializer implements SimpleVersionedSerializer<HlsEnumeratorState> {

    public static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(HlsEnumeratorState state) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        Set<Long> sequences = state.getDiscoveredSequences();
        out.writeInt(sequences.size());
        for (Long seq : sequences) {
            out.writeLong(seq);
        }
        out.flush();
        return baos.toByteArray();
    }

    @Override
    public HlsEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
        int size = in.readInt();
        Set<Long> sequences = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            sequences.add(in.readLong());
        }
        return new HlsEnumeratorState(sequences);
    }
}
