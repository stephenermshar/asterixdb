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

import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedMemoryConstrain;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * (stephen) This class includes methods for obscuring the frames from the class that extends it.
 */
public abstract class AbstractTupleStreamJoiner extends AbstractFrameStreamJoiner {

    VPartitionTupleBufferManager secondaryTupleBufferManager;
    IFrameTupleAccessor secondaryTupleBufferAccessor;
    ITuplePairComparator comparator;

    IFrameWriter writer;

    protected static TuplePointer tempPtr = new TuplePointer(); // (stephen) for method signature, see OptimizedHybridHashJoin

    public AbstractTupleStreamJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF,
            int availableMemoryForJoinInFrames, ITuplePairComparator comparator, IFrameWriter writer)
            throws HyracksDataException {
        super(ctx, leftCF, rightCF, availableMemoryForJoinInFrames);

        this.comparator = comparator;
        this.writer = writer;

        final int availableMemoryForJoinInBytes = availableMemoryForJoinInFrames * ctx.getInitialFrameSize();
        int partitions = 1; // (stephen) can probably use partitions for grouping many branches for multi-joins
        BitSet spilledStatus = new BitSet(partitions);
        IPartitionedMemoryConstrain memoryConstraint =
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus);
        secondaryTupleBufferManager =
                new VPartitionTupleBufferManager(ctx, memoryConstraint, partitions, availableMemoryForJoinInBytes);

        secondaryTupleBufferAccessor = secondaryTupleBufferManager
                .getTuplePointerAccessor(consumerFrames[RIGHT_PARTITION].getRecordDescriptor());
    }
}
