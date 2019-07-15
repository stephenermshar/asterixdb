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
package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedMemoryConstrain;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleInFrameListAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

/**
 * (stephen) This class includes methods for obscuring the frames from the class that extends it.
 */
public abstract class AbstractTupleStreamJoiner extends AbstractFrameStreamJoiner {

    private int primaryTupleIndex;
    VPartitionTupleBufferManager secondaryTupleBufferManager;
    ITuplePairComparator comparator;

    private TuplePointer tempPtr = new TuplePointer(); // (stephen) for method signature, see OptimizedHybridHashJoin

    public AbstractTupleStreamJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF,
            int availableMemoryForJoinInFrames, ITuplePairComparator comparator) throws HyracksDataException {
        super(ctx, leftCF, rightCF, availableMemoryForJoinInFrames);

        this.comparator = comparator;

        final int availableMemoryForJoinInBytes = availableMemoryForJoinInFrames * ctx.getInitialFrameSize();
        int partitions = 1; // (stephen) can probably use partitions for grouping many branches for multi-joins
        BitSet spilledStatus = new BitSet(partitions);
        IPartitionedMemoryConstrain memoryConstraint =
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus);
        secondaryTupleBufferManager =
                new VPartitionTupleBufferManager(ctx, memoryConstraint, partitions, availableMemoryForJoinInBytes);

    }

    protected void getNextTuple(int branch) {
        if (branch == 0) { // (stephen) assume the primary branch is always 0

        }

    }

    protected void clearTupleBuffer() {

    }

    /**
     *
     * @param branch1
     * @param branch2
     * @param fromInput if false, compare to next tuple in the buffer. (the buffer needs to behave like a queue)
     * @return
     * @throws HyracksDataException
     */
    protected int compareTuples(int branch1, int branch2, boolean fromInput) throws HyracksDataException {

        IFrameTupleAccessor accessor1 = getBranchAccessor(branch1, fromInput);
        int index1 = getBranchIndex(branch1);
        IFrameTupleAccessor accessor2 = getBranchAccessor(branch2, fromInput);
        int index2 = getBranchIndex(branch2);
        return comparator.compare(accessor1, index1, accessor2, index2);

    }

    private ITupleAccessor getBranchAccessor(int branch, boolean fromInput) {
        ITupleAccessor accessor;
        if (fromInput || branch == 0) {
            accessor = inputAccessor[branch];
        } else {
            accessor = (ITupleAccessor) secondaryTupleBufferManager
                    .getTuplePointerAccessor(consumerFrames[branch].getRecordDescriptor());
        }
        return accessor;
    }

    protected void appendTuple() {

    }
}
