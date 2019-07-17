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
import org.apache.hyracks.dataflow.std.buffermanager.AbstractTuplePointerAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedMemoryConstrain;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
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

    VPartitionTupleBufferManager secondaryTupleBufferManager;
    ITupleAccessor secondaryTupleBufferAccessor;
    ITuplePairComparator comparator;

    int index[];

    private static TuplePointer tempPtr = new TuplePointer(); // (stephen) for method signature, see OptimizedHybridHashJoin

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

        secondaryTupleBufferAccessor = (ITupleAccessor) secondaryTupleBufferManager.getTuplePointerAccessor(consumerFrames[RIGHT_PARTITION].getRecordDescriptor());

        index = new int[JOIN_PARTITIONS];
        index[LEFT_PARTITION] = 0;
        index[RIGHT_PARTITION] = 0;
    }


    // need to implement branch lists, and use branch identifiers instead of accessors and indexes. also, indexes need
    // to be class members, not method parameters
    //

    protected void joinTuples(ITupleAccessor accessor0, int index0, ITupleAccessor accessor1, int index1) {

    }

    protected void saveToBuffer(int BRANCH) throws HyracksDataException {
        if (BRANCH != RIGHT_PARTITION) {
            throw new RuntimeException("(stephen) There is currently only one buffer branch. It's for the right side, so its ID is RIGHT_PARTITION");
        }
        secondaryTupleBufferManager.insertTuple(0, inputAccessor[BRANCH], index[BRANCH], tempPtr);
    }

    protected void clearSavedBuffer(int BRANCH) throws HyracksDataException {
        if (BRANCH != RIGHT_PARTITION) {
            throw new RuntimeException("(stephen) There is currently only one buffer branch. It's for the right side, so its ID is RIGHT_PARTITION");
        }
        secondaryTupleBufferManager.clearPartition(0);
    }

    protected void getNextTuple(int BRANCH) {
        return;
    }

    protected int compareTuples(ITupleAccessor accessor0, int index0, ITupleAccessor accessor1, int index1) {
        return 0;
    }

    protected int compareTuples(int BRANCH_0, int BRANCH_1) {
        return 0;
    }

    protected int compareTuplesStreamWithBuffer(int STREAM_BRANCH, int BUFFER_BRANCH) {
        return 0;
    }

    protected boolean moreTuples(int BRANCH) {
        return false;
    }

    /**
     * Attempts to join the tuple at the current index of the STREAM_BRANCH, with all the tuples in the BUFFER_BRANCH.
     * @param STREAM_BRANCH
     * @param BUFFER_BRANCH
     */
    protected void joinStreamWithBuffer(int STREAM_BRANCH, int BUFFER_BRANCH) throws HyracksDataException {
        if (BUFFER_BRANCH != 1) {
            throw new RuntimeException("(stephen) There is currently only one buffer branch. It's for the right side, so its ID is RIGHT_PARTITION");
        }
        getNextTuple(STREAM_BRANCH);
        while (secondaryTupleBufferManager.getNumTuples(0) != 0 && moreTuples(LEFT_PARTITION)) {
            int c = compareTuplesStreamWithBuffer(STREAM_BRANCH, BUFFER_BRANCH);
            if (c < 0) {
                getNextTuple(STREAM_BRANCH);
            } else if (c > 0) {
                clearSavedBuffer(BUFFER_BRANCH);
            } else {
                for (int i=0; i < secondaryTupleBufferManager.getNumTuples(0); i++) {
                    joinTuples(inputAccessor[STREAM_BRANCH], index[STREAM_BRANCH], secondaryTupleBufferAccessor, i);
                }
                getNextTuple(STREAM_BRANCH);
            }
        }
    }

    /**
     * Attempts to join the tuple at the current index of the LEFT_BRANCH with all matches in the RIGHT_BRANCH.
     * @param LEFT_BRANCH
     * @param RIGHT_BRANCH
     */
    protected void joinStreams(int LEFT_BRANCH, int RIGHT_BRANCH) throws HyracksDataException {
        int c = compareTuples(LEFT_BRANCH, RIGHT_BRANCH);
        if (c < 0) {
            joinStreamWithBuffer(RIGHT_BRANCH, RIGHT_BRANCH);
        } else if (c > 0) {
            getNextTuple(RIGHT_BRANCH);
        } else {
            joinTuples(inputAccessor[LEFT_BRANCH], index[LEFT_BRANCH], inputAccessor[RIGHT_BRANCH], index[RIGHT_BRANCH]);
            saveToBuffer(RIGHT_BRANCH);
            getNextTuple(RIGHT_BRANCH);
        }
    }

    // (stephen) consider a getNextMatchingTuple().
}
