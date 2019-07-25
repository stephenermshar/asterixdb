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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hyracks.api.client.HyracksClientInterfaceFunctions;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class MergeJoiner extends AbstractTupleStreamJoiner {

    private boolean[] ready;

    public MergeJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF, IFrameWriter writer,
            int memoryForJoinInFrames, ITuplePairComparator comparator) throws HyracksDataException {
        super(ctx, leftCF, rightCF, memoryForJoinInFrames - JOIN_PARTITIONS, comparator, writer);
        ready = new boolean[2];
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
            ready[branch] = getNextFrame(branch);
        }
    }

    /**
     * Join the current left tuple with all the tuples in the right buffer.
     * @throws HyracksDataException
     */
    private void join() throws HyracksDataException {

        if (secondaryTupleBufferManager.getNumTuples(0) <= 0) {
            return;
        }
        secondaryTupleBufferAccessor.reset();
        secondaryTupleBufferAccessor.getTuplePointer(tempPtr);

        while (secondaryTupleBufferAccessor.hasNext()) {
            secondaryTupleBufferAccessor.next();
            addToResult(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                    secondaryTupleBufferAccessor, secondaryTupleBufferAccessor.getTupleId(), false, writer);
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
     * @return true if the current tuple was added to the buffer.
     * @throws HyracksDataException
     */
    private boolean saveRight(boolean clear) throws HyracksDataException {
        if (clear) {
            clearSavedRight();
        }

        // (stephen) if insertTuple returns false, then do not get next RIGHT tuple, instead, begin reading all LEFT,
        //           including current left to run file. once all left of current key are in run file begin run file
        //           join once run file join is complete return to beginning of main while loop. do not get new left or
        //           right after completing run file join, since the current left and right haven't been joined with
        //           anything because they were fetched in order to determine when to stop saving to the run file or
        //           buffer.

        if (secondaryTupleBufferManager.insertTuple(0, inputAccessor[RIGHT_PARTITION],
                inputAccessor[RIGHT_PARTITION].getTupleId(), tempPtr)) {
            // (stephen) sets the accessor to point to the tempPtr. Using the temp pointer because it's guaranteed to be
            //           pointing to a valid tuple that was just inserted.
            secondaryTupleBufferAccessor.reset(tempPtr);
            return true;
        } else {
            // (stephen) begin run file join
            processRunFileJoin();
            return false;
        }
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
        if (secondaryTupleBufferManager.getNumTuples(0) <= 0) {
            return false;
        }

        if (secondaryTupleBufferAccessor.getBuffer() == null) {
            secondaryTupleBufferAccessor.reset(tempPtr);
        }
        return 0 == comparator.compare(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                secondaryTupleBufferAccessor, tempPtr.getTupleIndex());
    }

    /**
     * loads all matching left tuples of the current key into the run file. Since it runs until it finds a tuple that
     * doesn't match or there are no more tuples in the stream, then when it finishes either the left stream's current
     * tuple has not been processed, or ready[LEFT] is false.
     */
    private void loadAllLeftIntoRunFile() throws HyracksDataException {
        // make sure the runFile is empty (this may be guaranteed naturally)

        // add current left to runFile, we know it matches the right buffer because this function is indirectly called
        // from the equal else branch in process join
        int c = 0; // don't replace this with a comparison, it's actually supposed to start out as 0, not a dummy init.

        do {
            getNextTuple(LEFT_PARTITION);
            c = 0; // replace with comparison between current left and left in runFile
            if (c == 0) {
                // add it to the run file
            } else {
                return; // current left will need to be compared by someone else
            }
        } while(ready[LEFT_PARTITION] && c == 0); // c==0 will always be true here, else function returns first.
    }

    /**
     * clears the right buffer and then
     * loads as many tuples of the current key as possible into memory. since it runs until it finds a tuple that
     * doesn't match or there are no more tuples in the stream, then when it finishes either the right stream's current
     * tuple has not been processed, or ready[RIGHT] is false.
     * @return true if it stops because the buffer is full,
     *         false if it stops because there was a new key or there were no more right tuples in the stream.
     */
    private boolean loadAllRightIntoBuffer() throws HyracksDataException{
        // the current right tuple has not been added to the buffer or compared to anything at this point
        clearSavedRight();
        // compare the current right tuple to the run file key
        // if it matches then add it to the buffer, if not return false
        // assuming it has been added to the buffer get the next right tuple and continue comparing until there is one
        //      that doesn't match, at that point do not get the next right tuple, it will be compared by someone else,
        //      probably with the current left tuple rather than the run file.
        // insert the current right tuple into the buffer, since it hasn't been processed yet, but

        boolean bufferIsFull = false;

        do {
            int c = 0; // replace with comparison between current right and run file key
            if (c == 0) {
                // add it to the buffer, update bufferIsFull flag
                // get the next right tuple
            } else {
                return false;
            }
        } while (ready[RIGHT_PARTITION] && !bufferIsFull);
        return true;
    }

    private void clearRunFile() {

    }

    /**
     * for each item in the left run file, join it with all the items in the right buffer
     */
    private void runFileJoin() {
        
    }

    private void processRunFileJoin() throws HyracksDataException {
        // the current right tuple has not been added to the buffer because it was full
        loadAllLeftIntoRunFile();
        boolean bufferIsFull = true;
        while(bufferIsFull) {
            runFileJoin();
            bufferIsFull = loadAllRightIntoBuffer();
        }
        // (stephen) join remaining tuples from the partially filled buffer.
        runFileJoin();
        clearRunFile();
        clearSavedRight();
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
                    if (saveRight(!compareTupleWithBuffer())) {
                        getNextTuple(RIGHT_PARTITION);
                    }
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
