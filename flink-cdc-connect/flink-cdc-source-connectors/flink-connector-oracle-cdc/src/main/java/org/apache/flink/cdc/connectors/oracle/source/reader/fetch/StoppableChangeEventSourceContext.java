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

package org.apache.flink.cdc.connectors.oracle.source.reader.fetch;

import io.debezium.pipeline.source.spi.ChangeEventSource;

/**
 * A change event source context that can stop the running source by invoking {@link
 * #stopChangeEventSource()}.
 */
public class StoppableChangeEventSourceContext
        implements ChangeEventSource.ChangeEventSourceContext {

    private volatile boolean isRunning = true;

    public void stopChangeEventSource() {
        isRunning = false;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    // The blocking-snapshot pause/resume hooks added in Debezium 2.x are not used by Flink CDC,
    // which manages bounded reads via watermarks; they are implemented as no-ops here.
    @Override
    public boolean isPaused() {
        return false;
    }

    @Override
    public void resumeStreaming() throws InterruptedException {}

    @Override
    public void waitSnapshotCompletion() throws InterruptedException {}

    @Override
    public void streamingPaused() {}

    @Override
    public void waitStreamingPaused() throws InterruptedException {}
}
