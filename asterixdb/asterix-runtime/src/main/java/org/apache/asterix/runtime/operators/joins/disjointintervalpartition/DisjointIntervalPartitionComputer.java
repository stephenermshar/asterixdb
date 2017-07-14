/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.operators.joins.disjointintervalpartition;

import java.util.PriorityQueue;

import org.apache.asterix.runtime.operators.joins.IntervalJoinUtil;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class DisjointIntervalPartitionComputer implements ITuplePartitionComputer {
    private static final long serialVersionUID = 1L;
    private final int intervalFieldId;
    private final PriorityQueue<PartitionMinItem> partitionMinHeap;
    private int partitionCount = 0;

    public DisjointIntervalPartitionComputer(int intervalFieldId, PriorityQueue<PartitionMinItem> partitionMinHeap) {
        this.intervalFieldId = intervalFieldId;
        this.partitionMinHeap = partitionMinHeap;
    }

    @Override
    public int partition(IFrameTupleAccessor accessor, int tIndex, int nPartitions) throws HyracksDataException {
        PartitionMinItem pmi = partitionMinHeap.peek();
        int partition = -1;
        if (pmi == null) {
            // Assume nPartitions will be greater than 0.
            partition = getNewPartitionId(accessor, tIndex);
        } else {
            long heapMin = pmi.getPoint();
            long start = IntervalJoinUtil.getIntervalStart(accessor, tIndex, intervalFieldId);
            if (heapMin <= start) {
                partition = pmi.getPartition();
                updateMin(accessor, tIndex);
            } else if (partitionCount < nPartitions) {
                // Only add partitions if there is space.
                partition = getNewPartitionId(accessor, tIndex);
            }
        }
        return partition;
    }

    private int getNewPartitionId(IFrameTupleAccessor accessor, int tIndex) {
        long end = IntervalJoinUtil.getIntervalEnd(accessor, tIndex, intervalFieldId);
        PartitionMinItem pmi = new PartitionMinItem(partitionCount, end);
        partitionMinHeap.add(pmi);
        return partitionCount++;
    }

    private void updateMin(IFrameTupleAccessor accessor, int tIndex) {
        PartitionMinItem pmi = partitionMinHeap.poll();
        long end = IntervalJoinUtil.getIntervalEnd(accessor, tIndex, intervalFieldId);
        pmi.setPoint(end);
        partitionMinHeap.add(pmi);
    }

    public void reset() {
        partitionMinHeap.clear();
        partitionCount = 0;
    }

}