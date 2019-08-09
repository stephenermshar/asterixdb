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
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedMemoryConstrain;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class MergeJoiner implements IStreamJoiner {

    // MJ

    protected static TuplePointer tempPtr = new TuplePointer(); // (stephen) for method signature, see OptimizedHybridHashJoin
    private final ITupleAccessor runFileAccessor;
    private final RunFileStream runFileStream;

    // ATSJ

    VPartitionTupleBufferManager secondaryTupleBufferManager;
    ITupleAccessor secondaryTupleBufferAccessor;
    ITuplePairComparator[] comparators;
    IFrameWriter writer;

    // AFSJ

    protected final int availableMemoryForJoinInFrames;

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int LEFT_PARTITION = 0;
    protected static final int RIGHT_PARTITION = 1;

    protected final MergeBranchStatus[] branchStatus;
    protected final IConsumerFrame[] consumerFrames;
    private final FrameTupleAppender resultAppender;

    protected final IFrame[] inputBuffer;
    protected final ITupleAccessor[] inputAccessor;

    // for logging only
    protected long[] frameCounts = { 0, 0 };
    protected long[] tupleCounts = { 0, 0 };
    private long[][] currentTuple;
    private long[][] toJoin;
    private String joinTypeStatus;

    public MergeJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF, IFrameWriter writer,
            int memoryForJoinInFrames, ITuplePairComparator[] comparators) throws HyracksDataException {
        // AFSJ

        inputAccessor = new TupleAccessor[JOIN_PARTITIONS];
        inputAccessor[LEFT_PARTITION] = new TupleAccessor(leftCF.getRecordDescriptor());
        inputAccessor[RIGHT_PARTITION] = new TupleAccessor(rightCF.getRecordDescriptor());

        inputBuffer = new IFrame[JOIN_PARTITIONS];
        inputBuffer[LEFT_PARTITION] = new VSizeFrame(ctx);
        inputBuffer[RIGHT_PARTITION] = new VSizeFrame(ctx);

        branchStatus = new MergeBranchStatus[JOIN_PARTITIONS];
        branchStatus[LEFT_PARTITION] = new MergeBranchStatus();
        branchStatus[RIGHT_PARTITION] = new MergeBranchStatus();

        consumerFrames = new IConsumerFrame[JOIN_PARTITIONS];
        consumerFrames[LEFT_PARTITION] = leftCF;
        consumerFrames[RIGHT_PARTITION] = rightCF;

        // Result
        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));

        // ATSJ

        this.comparators = comparators;
        this.writer = writer;

        availableMemoryForJoinInFrames = memoryForJoinInFrames - JOIN_PARTITIONS;
        final int availableMemoryForJoinInBytes = availableMemoryForJoinInFrames * ctx.getInitialFrameSize();
        int partitions = 1; // (stephen) can probably use partitions for grouping many branches for multi-joins
        BitSet spilledStatus = new BitSet(partitions);
        IPartitionedMemoryConstrain memoryConstraint =
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus);
        secondaryTupleBufferManager =
                new VPartitionTupleBufferManager(ctx, memoryConstraint, partitions, availableMemoryForJoinInBytes);
        secondaryTupleBufferManager.reset();
        secondaryTupleBufferAccessor = secondaryTupleBufferManager
                .createPartitionTupleAccessor(consumerFrames[RIGHT_PARTITION].getRecordDescriptor(), 0);

        // MJ

        runFileStream = new RunFileStream(ctx, "left", branchStatus[LEFT_PARTITION]);

        // (stephen) ----------- POTENTIAL PROBLEM AREA FOR SPILLING -------------
        runFileAccessor = new TupleAccessor(consumerFrames[LEFT_PARTITION].getRecordDescriptor());
        // ----------------------------------------------------------

        currentTuple = new long[][] { null, null };
        toJoin = new long[][] { null, null };
        joinTypeStatus = "";
    }

    @Override
    public boolean getNextFrame(int branch) throws HyracksDataException {
        if (consumerFrames[branch].hasMoreFrames()) {
            setFrame(branch, consumerFrames[branch].getFrame());
            return true;
        }
        return false;
    }

    private void setFrame(int branch, ByteBuffer buffer) throws HyracksDataException {
        inputBuffer[branch].getBuffer().clear();
        if (inputBuffer[branch].getFrameSize() < buffer.capacity()) {
            inputBuffer[branch].resize(buffer.capacity());
        }
        inputBuffer[branch].getBuffer().put(buffer.array(), 0, buffer.capacity());
        inputAccessor[branch].reset(inputBuffer[branch].getBuffer());
        inputAccessor[branch].next();
        frameCounts[branch]++;
        tupleCounts[branch] += inputAccessor[branch].getTupleCount();
    }

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2,
            boolean reversed, IFrameWriter writer) throws HyracksDataException {
        toJoin[LEFT_PARTITION] = TuplePrinterUtil.returnTupleFieldsAsBigInts((ITupleAccessor) accessor1);
        toJoin[RIGHT_PARTITION] = TuplePrinterUtil.returnTupleFieldsAsBigInts((ITupleAccessor) accessor2);

        if (reversed) {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor2, index2, accessor1, index1);
        } else {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
        }
    }

    private void closeJoin(IFrameWriter writer) throws HyracksDataException {
        // writes the results out, closing the writer is handled outside of this class
        resultAppender.write(writer, true);
    }

    private void getNextLeftTupleFromFile() throws HyracksDataException {
        if (runFileAccessor.hasNext()) {
            runFileAccessor.next();
        } else if (!runFileStream.loadNextBuffer(runFileAccessor)) {
            runFileAccessor.next();
        }
    }

    private void getNextTuple(int branch) throws HyracksDataException {
        if (inputAccessor[branch].hasNext()) {
            inputAccessor[branch].next();
        } else if (!getNextFrame(branch)) {
            inputAccessor[branch].next();
        }
        currentTuple[branch] = TuplePrinterUtil.returnTupleFieldsAsBigInts(inputAccessor[branch]);
        return;
    }

    private void join(ITupleAccessor leftAccessor) throws HyracksDataException {
        // joins the current leftAccessor tuple with all tuples in the right buffer

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
        if (clear) {
            clearSavedRight();
        }

        if (!inputAccessor[RIGHT_PARTITION].exists()) {
            return true;
        }

        if (secondaryTupleBufferManager.insertTuple(0, inputAccessor[RIGHT_PARTITION],
                inputAccessor[RIGHT_PARTITION].getTupleId(), tempPtr)) {

            secondaryTupleBufferAccessor.reset();
            secondaryTupleBufferAccessor.next();
            return true;
        } else {
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

        // * This should blindly grab a tuple from the left stream and add it to the the runfile.
        //   it should then attempt to get another tuple from the left stream. if the next tuple from the left stream
        //   does not exist then this method is done (may need to cleanup first)
        // * assuming it did not return after attempting to get another tuple and checking its existance, it should check
        //   whether the new tuple matches the tuple(s) that have been added to the run file. if it does not match it
        //   should return (after cleanup)
        // * otherwise it can continue.

        runFileStream.createRunFileWriting();
        runFileStream.startRunFileWriting();
        runFileStream.addToRunFile(inputAccessor[LEFT_PARTITION]);

        // resetting after adding a tuple in hopes of avoiding a case where the buffer doesn't exist yet.
        //        runFileAccessor.reset(runFileStream.getAppenderBuffer());

        getNextTuple(LEFT_PARTITION);

        boolean lastTupleAdded = true;
        while (lastTupleAdded) {
            if (inputAccessor[LEFT_PARTITION].exists() && secondaryTupleBufferAccessor.exists()
                    && 0 == compare(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                            secondaryTupleBufferAccessor, secondaryTupleBufferAccessor.getTupleId())) {
                runFileStream.addToRunFile(inputAccessor[LEFT_PARTITION]);
                getNextTuple(LEFT_PARTITION);
            } else {
                lastTupleAdded = false;
            }
        }

        runFileStream.flushRunFile();
        runFileStream.startReadingRunFile(runFileAccessor);
    }

    private void clearRunFile() {
        runFileStream.removeRunFile();
    }

    /**
     * Joins by reading from the left run file and the right secondary buffer.
     * @throws HyracksDataException
     */
    private void joinFromFile() throws HyracksDataException {
        // pre:  L == R (because of failed load), RB is full, L == RB
        // post: L is new, R is new

        boolean loadRightSuccessful = false; // because of "RB is full" precondition
        loadAllLeftIntoRunFile(); // satisfies "L is new" post-condition
        //        runFileAccessor.next();

        while (!loadRightSuccessful) {
            while (runFileAccessor.exists() && secondaryTupleBufferAccessor.exists()) {
                join(runFileAccessor); // make sure works w/ run file or make new func to use run file
                getNextLeftTupleFromFile();
            }
            runFileStream.startReadingRunFile(runFileAccessor);
            loadRightSuccessful = loadRight(runFileAccessor); // satisfies "R is new" post-condition on last run
        }
        while (runFileAccessor.exists() && secondaryTupleBufferAccessor.exists()) { // joins remaining not-full RB
            join(runFileAccessor);
            getNextLeftTupleFromFile();
        }
        clearRunFile(); // so that the joiner doesn't use the runFile in the main loop.
        // no need to clear saved right since it's always cleared before saving new keys.
    }

    /**
     * Joins by reading directly from the left input buffer and the right secondary buffer.
     * @throws HyracksDataException
     */
    private void joinFromStream() throws HyracksDataException {
        while (inputAccessor[LEFT_PARTITION].exists() && secondaryTupleBufferAccessor.exists()
                && compareTupleWithBuffer()) {

            join(inputAccessor[LEFT_PARTITION]);
            getNextTuple(LEFT_PARTITION);
        }
        clearSavedRight();
    }

    /**
     * Joins the current pair of matched tuples and all pairs with equivalent keys.
     * @throws HyracksDataException
     */
    private void joinMatched() throws HyracksDataException {
        boolean initialLoadSuccessful = loadRight(inputAccessor[LEFT_PARTITION]);
        if (initialLoadSuccessful) {
            joinTypeStatus = "stream";
            joinFromStream();
            joinTypeStatus = "";
        } else {
            joinTypeStatus = "file";
            joinFromFile();
            joinTypeStatus = "";
        }
    }

    /**
     * Loads as many right tuples matching the current right tuple key as possible into the buffer.
     * @return true if all matching right tuples were loaded, right tuple at current tupleId is either new or does not
     *         exist. false if there are more matching right tuples to be loaded. right tuple at current tupleId matches
     *         tuples in the buffer.
     * @throws HyracksDataException
     */
    private boolean loadRight(ITupleAccessor leftAccessor) throws HyracksDataException {
        // PRE:  the current right tuple represents the unique key for which all matching right tuples should be saved
        //       the current left and right accessor tupleIds point to valid tuples
        // POST: all right tuples with the same key as the current right tuple when the function was called have been
        //       added to the buffer OR the buffer has been filled to capacity with right tuples of that same key.
        //
        // RETURN: TRUE  if all right tuples of the desired key have been added.
        //         FALSE if the tupleId of the right accessor refers to the next tuple of the desired key that has not
        //         been added to the buffer.

        // DOES
        //
        // * clears the buffer and then saves the current right tuple. This should always succeed since PRE requires
        //   defined right tuple.
        // * If the first save failed return false because there are still more tuples of the key that is supposed to be
        //   fully loaded remaining in the stream at or after the current right tupleId.
        // * if the first save succeeded, get another tuple, and compare it to the first. Since both tuples are from the
        //   right this is not possible. the new tuple must be compared to a tuple originally from the left stream.
        //    * if this is called before a call to loadAllLeftIntoRunFile() then the right buffer is expected to match
        //      the left tupleId tuple. if it is called after loadAllLeftIntoRunFile() has been called, then it should
        //      not match the left tupleId tuple, but it should match a tuple in the run file.
        //      add a parameter to this function for the left accessor.
        // * if the new tuple matches the correct left tuple, then it should be added, another should be gotten from
        //   the right stream and this can continue.
        // * if the new tuple does not match the correct left tuple, then the function should return true and not
        //   get a new right tuple
        // * if at any time a tuple fails to be added but it matched, the function should return false.

        boolean saveSuccessful = saveRight(true);

        while (saveSuccessful) {
            getNextTuple(RIGHT_PARTITION);
            if (!inputAccessor[RIGHT_PARTITION].exists()) {
                return true;
            }

            int c = compare(leftAccessor, leftAccessor.getTupleId(), inputAccessor[RIGHT_PARTITION],
                    inputAccessor[RIGHT_PARTITION].getTupleId());
            if (c != 0) {
                return true;
            }

            saveSuccessful = saveRight(false);
        }
        return false;
    }

    /**
     * Increments both sides as needed until they match.
     * @return true if the left tuple and right tuple at tupleId match. false if they do not match, or one or both do
     *         not exist.
     * @throws HyracksDataException
     */
    private boolean matchStreams() throws HyracksDataException {
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

    @Override
    public void processJoin() throws HyracksDataException {
        getNextTuple(LEFT_PARTITION);
        getNextTuple(RIGHT_PARTITION);

        while (matchStreams()) {
            joinMatched();
        }

        secondaryTupleBufferManager.close();
        closeJoin(writer);
    }
}
