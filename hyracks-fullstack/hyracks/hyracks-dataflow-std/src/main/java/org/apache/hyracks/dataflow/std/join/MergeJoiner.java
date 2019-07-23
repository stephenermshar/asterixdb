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
    private TupleAccessor secondaryTupleBufferDirectAccessor;

    public MergeJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF, IFrameWriter writer,
            int memoryForJoinInFrames, ITuplePairComparator comparator) throws HyracksDataException {
        super(ctx, leftCF, rightCF, memoryForJoinInFrames - JOIN_PARTITIONS, comparator, writer);
        ready = new boolean[2];
        secondaryTupleBufferDirectAccessor = new TupleAccessor(consumerFrames[RIGHT_PARTITION].getRecordDescriptor());
    }

    /**
     * Get the next available tuple from a branch and update the ready flag appropriately.
     * @param branch
     * @throws HyracksDataException
     */
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

    /**
     * Join the current left tuple with all the tuples in the right buffer.
     * @throws HyracksDataException
     */
    private void join() throws HyracksDataException {
        ByteBuffer secondaryTupleBuffer = secondaryTupleBufferAccessor.getBuffer();
        secondaryTupleBufferDirectAccessor.reset(secondaryTupleBuffer);

        for (int i = 0; i < secondaryTupleBufferDirectAccessor.getTupleCount(); i++) {
            addToResult(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                    secondaryTupleBufferDirectAccessor, i, false, writer);
        }
    }

    /**
     * Clear the right buffer.
     * @throws HyracksDataException
     */
    private void clearSavedRight() throws HyracksDataException {
        secondaryTupleBufferManager.clearPartition(0);

    }

    /**
     * Save the current tuple from the right stream into the buffer.
     * @param clear if true, clear the right buffer before inserting the tuple.
     * @throws HyracksDataException
     */
    private void saveRight(boolean clear) throws HyracksDataException {
        if (clear) {
            clearSavedRight();
        }
        secondaryTupleBufferManager.insertTuple(0, inputAccessor[RIGHT_PARTITION],
                inputAccessor[RIGHT_PARTITION].getTupleId(), tempPtr);

        ((ITuplePointerAccessor) secondaryTupleBufferAccessor).reset(tempPtr);
    }

    /**
     * Compares the current tuples in the left and right streams with each other.
     * @return c < 0 if Left is smaller, c > 0 if right is smaller, c == 0 if they are the same.
     * @throws HyracksDataException
     */
    private int compareTuplesInStream() throws HyracksDataException {
        return comparator.compare(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                inputAccessor[RIGHT_PARTITION], inputAccessor[RIGHT_PARTITION].getTupleId());
    }

    /**
     * Compares the current left tuple with a tuple in the buffer, since all tuples in the buffer have the same value
     * for the comparison.
     * @return true if they match, false if they don't match or the buffer is empty
     * @throws HyracksDataException
     */
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
                    // (stephen) if there are tuples in the buffer from the last while loop and right has gotten ahead,
                    //           then they must match the current left tuple so they may be joined.
                    //
                    //           if the right buffer is empty, then this won't join anything and it will just attempt to
                    //           catch the left side up with the right side.
                    join();
                    getNextTuple(LEFT_PARTITION);
                } else if (c > 0) {
                    // (stephen) if the right has gotten behind the left, then the tuples in the right buffer can no
                    //           longer match anything so they may be cleared.
                    //
                    //           Then this attempts to catch the right side up with the left side.
                    getNextTuple(RIGHT_PARTITION);
                    clearSavedRight();
                } else {
                    // (stephen) if the left and right sides match, then the right tuple should be saved to be handled
                    //           in the next iteration of the loop.
                    //
                    //           if the new tuple is different than those in the buffer, the buffer should be cleared.
                    saveRight(!compareTupleWithBuffer());
                    getNextTuple(RIGHT_PARTITION);
                }
            } else {
                // (stephen) the remaining left tuples could still match with the right buffer.
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
