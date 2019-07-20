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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;

public class MergeJoiner extends AbstractTupleStreamJoiner {

    private boolean[] ready;

    public MergeJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF, IFrameWriter writer,
            int memoryForJoinInFrames, ITuplePairComparator comparator) throws HyracksDataException {
        super(ctx, leftCF, rightCF, memoryForJoinInFrames - JOIN_PARTITIONS, comparator, writer);
        ready = new boolean[2];
    }

    private void getNextTuple(int branch) throws HyracksDataException {
        if (inputAccessor[branch].exists() && inputAccessor[branch].hasNext()) {
            inputAccessor[branch].next();
            ready[branch] = true;
        } else {
            if (getNextFrame(branch)) {
                ready[branch] = true;
            } else {
                ready[branch] = false;
            }
        }
    }

    private void join() throws HyracksDataException {
        ByteBuffer secondaryTupleBuffer = secondaryTupleBufferAccessor.getBuffer();
        TupleAccessor secondaryTupleBufferDirectAccessor =
                new TupleAccessor(consumerFrames[RIGHT_PARTITION].getRecordDescriptor());
        secondaryTupleBufferDirectAccessor.reset(secondaryTupleBuffer);

        for (int i = 0; i < secondaryTupleBufferDirectAccessor.getTupleCount(); i++) {
            addToResult(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                    secondaryTupleBufferDirectAccessor, i, false, writer);
        }
    }

    private void clearSavedRight() throws HyracksDataException {
        secondaryTupleBufferManager.clearPartition(0);

    }

    private void saveRight(boolean clear) throws HyracksDataException {
        if (clear) {
            clearSavedRight();
        }
        secondaryTupleBufferManager.insertTuple(0, inputAccessor[RIGHT_PARTITION],
                inputAccessor[RIGHT_PARTITION].getTupleId(), tempPtr);

        ((ITuplePointerAccessor) secondaryTupleBufferAccessor).reset(tempPtr);
    }

    private int compareTuplesInStream() throws HyracksDataException {
        return comparator.compare(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                inputAccessor[RIGHT_PARTITION], inputAccessor[RIGHT_PARTITION].getTupleId());
    }

    private boolean compareTupleWithBuffer() throws HyracksDataException {
        if (secondaryTupleBufferAccessor.getBuffer() == null) {
            return false;
        }
        return 0 == comparator.compare(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                secondaryTupleBufferAccessor, tempPtr.getTupleIndex());
    }

    @Override
    public void processJoin() throws HyracksDataException {
        getNextTuple(LEFT_PARTITION);
        getNextTuple(RIGHT_PARTITION);

        while (ready[LEFT_PARTITION]) {
            if (ready[RIGHT_PARTITION]) {
                int c = compareTuplesInStream();
                if (c < 0) {
                    join();
                    getNextTuple(LEFT_PARTITION);
                } else if (c > 0) {
                    getNextTuple(RIGHT_PARTITION);
                    clearSavedRight();
                } else {
                    if (compareTupleWithBuffer()) {
                        saveRight(false);
                    } else {
                        saveRight(true);
                    }
                    getNextTuple(RIGHT_PARTITION);
                }
            } else {
                if (compareTupleWithBuffer()) {
                    join();
                }
                getNextTuple(LEFT_PARTITION);
            }
        }
        secondaryTupleBufferManager.close();
        closeJoin(writer);
    }

}
