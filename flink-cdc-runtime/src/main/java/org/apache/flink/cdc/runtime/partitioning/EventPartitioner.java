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

package org.apache.flink.cdc.runtime.partitioning;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.cdc.common.annotation.Internal;

/** Partitioner that send {@link PartitioningEvent} to its target partition. */
@Internal
public class EventPartitioner implements Partitioner<Integer> {
    @Override
    public int partition(Integer target, int numPartitions) {
        if (target >= numPartitions) {
            throw new IllegalStateException(
                    String.format(
                            "The target of the event %d is greater than number of downstream partitions %d",
                            target, numPartitions));
        }
        System.out.printf(">>> EventPartitioner Received partition request %d / %d\n", target, numPartitions);
        return target;
    }
}
