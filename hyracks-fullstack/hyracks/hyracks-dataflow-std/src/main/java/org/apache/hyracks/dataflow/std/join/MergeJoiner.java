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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;

public class MergeJoiner extends AbstractTupleStreamJoiner {

    private final int runFileAppenderBufferAccessorTupleId;
    private final ITupleAccessor runFileAppenderBufferAccessor;
    private final RunFileStream runFileStream;

    public MergeJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF, IFrameWriter writer,
            int memoryForJoinInFrames, ITuplePairComparator[] comparators) throws HyracksDataException {
        super(ctx, leftCF, rightCF, memoryForJoinInFrames - JOIN_PARTITIONS, comparators, writer);
        runFileStream = new RunFileStream(ctx, "left", branchStatus[LEFT_PARTITION]);

        // (stephen) ----------- POTENTIAL PROBLEM AREA -------------
        runFileAppenderBufferAccessor = new TupleAccessor(consumerFrames[LEFT_PARTITION].getRecordDescriptor());
        runFileAppenderBufferAccessorTupleId = 0;
        // ----------------------------------------------------------
    }

    private void getNextTuple(int branch) throws HyracksDataException {
        if (inputAccessor[branch].hasNext()) {
            inputAccessor[branch].next();
        } else if (!getNextFrame(branch)) {
            inputAccessor[branch].next();
        }
    }

    private void join() throws HyracksDataException {
        join(inputAccessor[LEFT_PARTITION]);
    }

    private void join(ITupleAccessor leftAccessor) throws HyracksDataException {
        if (secondaryTupleBufferManager.getNumTuples(0) <= 0) {
            return;
        }

        secondaryTupleBufferAccessor.reset();

        while (secondaryTupleBufferAccessor.hasNext()) {
            secondaryTupleBufferAccessor.next();
            addToResult(leftAccessor, leftAccessor.getTupleId(), secondaryTupleBufferAccessor,
                    secondaryTupleBufferAccessor.getTupleId(), false, writer);
        }
    }

    private void clearSavedRight() throws HyracksDataException {
        secondaryTupleBufferManager.clearPartition(0);
    }

    private boolean saveRight(boolean clear) throws HyracksDataException {
        return saveRight(clear, false);
    }

    private boolean saveRight(boolean clear, boolean forRunFileJoin) throws HyracksDataException {
        if (clear) {
            clearSavedRight();
        }

        if (secondaryTupleBufferManager.insertTuple(0, inputAccessor[RIGHT_PARTITION],
                inputAccessor[RIGHT_PARTITION].getTupleId(), tempPtr)) {

            secondaryTupleBufferAccessor.reset();
            secondaryTupleBufferAccessor.next();
            return true;
        } else {
            // begin run file join, unless this is being called from inside a run file join
            if (!forRunFileJoin) {
                processRunFileJoin();
            }
            return false;
        }
    }

    private int compare(IFrameTupleAccessor leftAccessor, int leftIndex, IFrameTupleAccessor rightAccessor,
            int rightIndex) throws HyracksDataException {
        for (ITuplePairComparator comparator : comparators) {
            int c;
            try {
                c = comparator.compare(leftAccessor, leftIndex, rightAccessor, rightIndex);
            } catch (Exception ex) {
                throw ex;
            }
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private int compareTuplesInStream() throws HyracksDataException {
        return compare(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                inputAccessor[RIGHT_PARTITION], inputAccessor[RIGHT_PARTITION].getTupleId());
    }

    private boolean compareTupleWithBuffer() throws HyracksDataException {
        if (secondaryTupleBufferManager.getNumTuples(0) <= 0) {
            // this if block may not be necessary, consider removing during cleanup
            return false;
        }

        if (!secondaryTupleBufferAccessor.exists()) {
            return false;
        }

        return 0 == compare(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                secondaryTupleBufferAccessor, secondaryTupleBufferAccessor.getTupleId());
    }

    private void loadAllLeftIntoRunFile() throws HyracksDataException {
        // make sure the runFile is empty (this may be guaranteed naturally)

        runFileStream.createRunFileWriting();
        runFileStream.startRunFileWriting();
        // add current left to runFile, we know it matches the right buffer because this function is indirectly called
        // from the equal else branch in process join
        runFileStream.addToRunFile(inputAccessor[LEFT_PARTITION]);

        // resetting after adding a tuple in hopes of avoiding a case where the buffer doesn't exist yet.
        runFileAppenderBufferAccessor.reset(runFileStream.getAppenderBuffer());

        int c = 0; // don't replace this with a comparison, it's actually supposed to start out as 0, not a dummy init.

        do {
            getNextTuple(LEFT_PARTITION);
            c = compare(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                    runFileAppenderBufferAccessor, runFileAppenderBufferAccessorTupleId);

            if (c == 0) {
                runFileStream.addToRunFile(inputAccessor[LEFT_PARTITION]);
            } else {
                runFileStream.flushRunFile();
                return; // current left will need to be compared by the caller (or their caller etc.)
            }
        } while (inputAccessor[LEFT_PARTITION].exists()/*ready[LEFT_PARTITION]*/); // c==0 will always be true here, else function returns first.

        runFileStream.flushRunFile();

        if (inputAccessor[LEFT_PARTITION].exists()/*ready[LEFT_PARTITION]*/) {
            // since the current left was added to the run file and the caller of this function expects the current
            // left to be unprocessed, or ready[LEFT] to be false, once this function ends, get the next left tuple
            // get the next tuple.
            getNextTuple(LEFT_PARTITION);
        }
    }

    private void clearRunFile() {
        runFileStream.removeRunFile();
    }

    private void runFileJoin() throws HyracksDataException {
        runFileStream.flushRunFile();
        // could probably use the accessor used to read from the appender
        ITupleAccessor runFileReaderAccessor = new TupleAccessor(consumerFrames[LEFT_PARTITION].getRecordDescriptor());
        runFileStream.startReadingRunFile(runFileReaderAccessor);
        join(runFileReaderAccessor);
    }

    private void processRunFileJoin() throws HyracksDataException {
        System.err.println("--- Skipping Unverified Code ---");
        return;
        //        System.err.println("--- Entering Unverified Code ---");
        //        // the current right tuple has not been added to the buffer because it was full
        //        loadAllLeftIntoRunFile();
        //        boolean bufferIsFull = true;
        //        while (bufferIsFull) {
        //            runFileJoin();
        //            bufferIsFull = loadAllRightIntoBuffer();
        //        }
        //        // (stephen) join remaining tuples from the partially filled buffer.
        //        runFileJoin();
        //        clearRunFile();
        //        clearSavedRight();
    }

    public boolean loadAllRightIntoBufferNew() throws HyracksDataException {
        // PRE:  the current right tuple represents the unique key for which all matching right tuples should be saved
        //       the current left and right accessor tupleIds point to valid tuples
        // POST: all right tuples with the same key as the current right tuple when the function was called have been
        //       added to the buffer OR the buffer has been filled to capacity with right tuples of that same key.
        //
        // RETURN: TRUE  if all right tuples of the desired key have been added.
        //         FALSE if the tupleId of the right accessor refers to the next tuple of the desired key that has not
        //         been added to the buffer.

        boolean saveSuccessful = saveRight(true);
        int c = 0;

        while (saveSuccessful) {
            getNextTuple(RIGHT_PARTITION);
            if (inputAccessor[RIGHT_PARTITION].exists()) {
                // we'd like to do this
                // c = compare(inputAccessor[RIGHT_PARTITION], inputAccessor[RIGHT_PARTITION].getTupleId(),
                //          secondaryTupleBufferAccessor, secondaryTupleBufferAccessor.getTupleId());
                //
                // but the comparator expects the left argument to come from the left stream, so inputAccessor[RIGHT]
                // and secondaryTupleBufferAccessor can't be compared since they both come from the right stream.
                // but, since when loading all Right into buffer we haven't incremented the left stream tupleId from
                // the time we compared it to enter this function, its key should be equivalent to inputAccessor[RIGHT]
                // and it can be used instead.
                c = compare(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                        inputAccessor[RIGHT_PARTITION], inputAccessor[RIGHT_PARTITION].getTupleId());
                if (c != 0) {
                    return true;
                } else {
                    saveSuccessful = saveRight(false);
                    if (!saveSuccessful) {
                        return false;
                    }
                }
            } else {
                return true;
            }
        }
        return false;
    }

    public void joinAllMatchingLeftWithMatchingRightBuffer() throws HyracksDataException {

        while (inputAccessor[LEFT_PARTITION].exists() && secondaryTupleBufferAccessor.exists()
                && compareTupleWithBuffer()) {

            join();
            getNextTuple(LEFT_PARTITION);
        }
        clearSavedRight();
    }

    public boolean makeStreamsEven() throws HyracksDataException {
        while (inputAccessor[LEFT_PARTITION].exists() && inputAccessor[RIGHT_PARTITION].exists()) {
            int c = compareTuplesInStream();

            if (c < 0) {
                getNextTuple(LEFT_PARTITION);
            } else if (c > 0) {
                getNextTuple(RIGHT_PARTITION);
            } else {
                return true;
            }
        }
        return false;
    }

    public void loadAllRight() throws HyracksDataException {
        loadAllRightIntoBufferNew();
    }

    public void joinAllLeft() throws HyracksDataException {
        joinAllMatchingLeftWithMatchingRightBuffer();
    }

    @Override
    public void processJoin() throws HyracksDataException {
        getNextTuple(LEFT_PARTITION);
        getNextTuple(RIGHT_PARTITION);

        while (makeStreamsEven()) {
            loadAllRight();
            joinAllLeft();
        }
        secondaryTupleBufferManager.close();
        closeJoin(writer);
    }
}
