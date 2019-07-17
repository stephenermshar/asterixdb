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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class MergeJoiner extends AbstractTupleStreamJoiner {
    IFrameWriter writer;

    public MergeJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF, IFrameWriter writer,
            int memoryForJoinInFrames, ITuplePairComparator comparator) throws HyracksDataException {
        super(ctx, leftCF, rightCF, memoryForJoinInFrames - JOIN_PARTITIONS, comparator);
        this.writer = writer;
    }

    @Override
    public void processJoin() throws HyracksDataException {
        // initialize the join by getting the first tuples
        getNextTuple(LEFT_PARTITION);
        getNextTuple(LEFT_PARTITION);

        while (moreTuples(LEFT_PARTITION)) {
            if (moreTuples(RIGHT_PARTITION)) {
                joinStreams(LEFT_PARTITION, RIGHT_PARTITION);
            } else {
                joinStreamWithBuffer(LEFT_PARTITION, RIGHT_PARTITION);
            }
        }
    }

}
