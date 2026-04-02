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

/** Serializer for {@link HlsSegmentSplit}. */
public class HlsSegmentSplitSerializer implements SimpleVersionedSerializer<HlsSegmentSplit> {

    public static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(HlsSegmentSplit split) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeUTF(split.getSegmentUrl());
        out.writeLong(split.getSequenceNumber());
        out.writeDouble(split.getDuration());
        out.writeUTF(split.getPlaylistUrl());
        out.flush();
        return baos.toByteArray();
    }

    @Override
    public HlsSegmentSplit deserialize(int version, byte[] serialized) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
        String segmentUrl = in.readUTF();
        long sequenceNumber = in.readLong();
        double duration = in.readDouble();
        String playlistUrl = in.readUTF();
        return new HlsSegmentSplit(segmentUrl, sequenceNumber, duration, playlistUrl);
    }
}
