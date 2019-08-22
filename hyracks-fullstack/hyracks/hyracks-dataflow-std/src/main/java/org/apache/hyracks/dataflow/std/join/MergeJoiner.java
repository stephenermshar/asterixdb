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

    private static TuplePointer tempPtr = new TuplePointer();
    private final ITupleAccessor runFileAccessor;
    private final RunFileStream runFileStream;

    private VPartitionTupleBufferManager secondaryTupleBufferManager;
    private ITupleAccessor secondaryTupleBufferAccessor;
    private ITuplePairComparator[] comparators;
    private IFrameWriter writer;

    private static final int JOIN_PARTITIONS = 2;
    private static final int LEFT_PARTITION = 0;
    private static final int RIGHT_PARTITION = 1;

    private final IConsumerFrame[] consumerFrames;
    private final FrameTupleAppender resultAppender;

    private final IFrame[] inputBuffer;
    private final ITupleAccessor[] inputAccessor;

    private long[] inputTuple;
    private int findBranch;
    private int findField;
    private long findValue;
    private int countFinds;

    public MergeJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF, IFrameWriter writer,
            int memoryForJoinInFrames, ITuplePairComparator[] comparators) throws HyracksDataException {

        findBranch = 0;
        findField = 0;
        findValue = 0;

        inputAccessor = new TupleAccessor[JOIN_PARTITIONS];
        inputAccessor[LEFT_PARTITION] = new TupleAccessor(leftCF.getRecordDescriptor());
        inputAccessor[RIGHT_PARTITION] = new TupleAccessor(rightCF.getRecordDescriptor());

        inputBuffer = new IFrame[JOIN_PARTITIONS];
        inputBuffer[LEFT_PARTITION] = new VSizeFrame(ctx);
        inputBuffer[RIGHT_PARTITION] = new VSizeFrame(ctx);

        consumerFrames = new IConsumerFrame[JOIN_PARTITIONS];
        consumerFrames[LEFT_PARTITION] = leftCF;
        consumerFrames[RIGHT_PARTITION] = rightCF;

        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));

        this.comparators = comparators;
        this.writer = writer;

        int availableMemoryForJoinInFrames = memoryForJoinInFrames - JOIN_PARTITIONS - 2;
        final int availableMemoryForJoinInBytes = availableMemoryForJoinInFrames * ctx.getInitialFrameSize();
        int partitions = 1;
        BitSet spilledStatus = new BitSet(partitions);
        IPartitionedMemoryConstrain memoryConstraint =
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus);
        secondaryTupleBufferManager =
                new VPartitionTupleBufferManager(ctx, memoryConstraint, partitions, availableMemoryForJoinInBytes);
        secondaryTupleBufferManager.reset();
        secondaryTupleBufferAccessor = secondaryTupleBufferManager
                .createPartitionTupleAccessor(consumerFrames[RIGHT_PARTITION].getRecordDescriptor(), 0);

        MergeBranchStatus[] branchStatus = new MergeBranchStatus[JOIN_PARTITIONS];
        branchStatus[LEFT_PARTITION] = new MergeBranchStatus();
        branchStatus[RIGHT_PARTITION] = new MergeBranchStatus();
        runFileStream = new RunFileStream(ctx, "left", branchStatus[LEFT_PARTITION]);

        runFileAccessor = new TupleAccessor(consumerFrames[LEFT_PARTITION].getRecordDescriptor());
    }

    // Tuples and Frames

    private void setFrame(int branch, ByteBuffer buffer) throws HyracksDataException {
        inputBuffer[branch].getBuffer().clear();
        if (inputBuffer[branch].getFrameSize() < buffer.capacity()) {
            inputBuffer[branch].resize(buffer.capacity());
        }
        inputBuffer[branch].getBuffer().put(buffer.array(), 0, buffer.capacity());
        inputAccessor[branch].reset(inputBuffer[branch].getBuffer());
        inputAccessor[branch].next();
    }

    private boolean getNextFrame(int branch, boolean fromFile) throws HyracksDataException {
        if (fromFile) {
            return runFileStream.loadNextBuffer(runFileAccessor);
        } else {
            if (consumerFrames[branch].hasMoreFrames()) {
                setFrame(branch, consumerFrames[branch].getFrame());
                return true;
            }
            return false;
        }
    }

    private void getNextTuple(int branch, boolean fromFile) throws HyracksDataException {
        ITupleAccessor accessor = fromFile ? runFileAccessor : inputAccessor[branch];
        if (accessor.hasNext() || !getNextFrame(branch, fromFile)) {
            accessor.next();
        }

        // --- DEBUGGING ---

        if (branch == 1 && !fromFile) {
            inputTuple = TuplePrinterUtil.returnTupleFieldsAsBigInts((inputAccessor[findBranch]));
            if (inputTuple.length > 0 && inputTuple[findField] == findValue) {
                countFinds = countFinds + 1;
            }
        }

        //

    }

    // Buffer and File

    private void clearSavedTuples(int branch) throws HyracksDataException {
        if (branch == LEFT_PARTITION) {
            runFileStream.removeRunFile();
            runFileStream.createRunFileWriting();
            runFileStream.startRunFileWriting();
        } else if (branch == RIGHT_PARTITION) {
            secondaryTupleBufferManager.clearPartition(0);
        } else {
            throw new RuntimeException();
        }
    }

    private boolean saveTuple(int branch, boolean clear) throws HyracksDataException {
        if (clear) {
            clearSavedTuples(branch);
        }

        if (!inputAccessor[branch].exists()) {
            return true;
        }

        boolean saveSuccessful;

        if (branch == LEFT_PARTITION) {
            runFileStream.addToRunFile(inputAccessor[LEFT_PARTITION]);
            saveSuccessful = true;

        } else if (branch == RIGHT_PARTITION) {
            saveSuccessful = secondaryTupleBufferManager.insertTuple(0, inputAccessor[RIGHT_PARTITION],
                    inputAccessor[RIGHT_PARTITION].getTupleId(), tempPtr);
            secondaryTupleBufferAccessor.reset();
            secondaryTupleBufferAccessor.next();
        } else {
            throw new RuntimeException();
        }
        return saveSuccessful;
    }

    private void loadAllLeftIntoRunFile() throws HyracksDataException {
        saveTuple(LEFT_PARTITION, true);
        getNextTuple(LEFT_PARTITION, false);

        boolean lastTupleAdded = true;
        while (lastTupleAdded) {
            if (inputAccessor[LEFT_PARTITION].exists() && secondaryTupleBufferAccessor.exists()
                    && 0 == compare(inputAccessor[LEFT_PARTITION], secondaryTupleBufferAccessor)) {
                saveTuple(LEFT_PARTITION, false);
                getNextTuple(LEFT_PARTITION, false);
            } else {
                lastTupleAdded = false;
            }
        }

        runFileStream.flushRunFile();
        runFileStream.startReadingRunFile(runFileAccessor);
    }

    private boolean loadRight(ITupleAccessor leftAccessor) throws HyracksDataException {
        boolean saveSuccessful = saveTuple(RIGHT_PARTITION, true);

        while (saveSuccessful) {
            getNextTuple(RIGHT_PARTITION, false);
            if (!inputAccessor[RIGHT_PARTITION].exists()) {
                return true;
            }

            int c = compare(leftAccessor, inputAccessor[RIGHT_PARTITION]);
            if (c != 0) {
                return true;
            }
            saveSuccessful = saveTuple(RIGHT_PARTITION, false);
        }
        return false;
    }

    // Comparators

    private int compare(ITupleAccessor leftAccessor, ITupleAccessor rightAccessor) throws HyracksDataException {
        for (ITuplePairComparator comparator : comparators) {
            int c = comparator.compare(leftAccessor, leftAccessor.getTupleId(), rightAccessor,
                    rightAccessor.getTupleId());
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private int compareTuplesInStream() throws HyracksDataException {
        return compare(inputAccessor[LEFT_PARTITION], inputAccessor[RIGHT_PARTITION]);
    }

    private boolean compareTupleWithBuffer() throws HyracksDataException {
        if (secondaryTupleBufferManager.getNumTuples(0) <= 0) {
            return false;
        }

        if (!secondaryTupleBufferAccessor.exists()) {
            return false;
        }

        return 0 == compare(inputAccessor[LEFT_PARTITION], secondaryTupleBufferAccessor);
    }

    // Results

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2,
            boolean reversed, IFrameWriter writer) throws HyracksDataException {

        if (reversed) {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor2, index2, accessor1, index1);
        } else {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
        }
    }

    private void joinWithBuffer(ITupleAccessor leftAccessor) throws HyracksDataException {

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

    // Main Functions

    private void joinFromFile() throws HyracksDataException {

        boolean loadRightSuccessful = false;
        loadAllLeftIntoRunFile();

        while (!loadRightSuccessful) {
            while (runFileAccessor.exists() && secondaryTupleBufferAccessor.exists()) {
                joinWithBuffer(runFileAccessor);
                getNextTuple(LEFT_PARTITION, true);
            }
            runFileStream.startReadingRunFile(runFileAccessor);
            loadRightSuccessful = loadRight(runFileAccessor);
        }
        while (runFileAccessor.exists() && secondaryTupleBufferAccessor.exists()) {
            joinWithBuffer(runFileAccessor);
            getNextTuple(LEFT_PARTITION, true);
        }
    }

    private void joinFromStream() throws HyracksDataException {
        while (inputAccessor[LEFT_PARTITION].exists() && secondaryTupleBufferAccessor.exists()
                && compareTupleWithBuffer()) {

            joinWithBuffer(inputAccessor[LEFT_PARTITION]);
            getNextTuple(LEFT_PARTITION, false);
        }
        clearSavedTuples(RIGHT_PARTITION);
    }

    private void joinMatched() throws HyracksDataException {
        boolean initialLoadSuccessful = loadRight(inputAccessor[LEFT_PARTITION]);
        if (initialLoadSuccessful) {
            joinFromStream();
        } else {
            joinFromFile();
        }
    }

    private boolean matchStreams() throws HyracksDataException {
        while (inputAccessor[LEFT_PARTITION].exists() && inputAccessor[RIGHT_PARTITION].exists()) {
            int c = compareTuplesInStream();

            if (c < 0) {
                getNextTuple(LEFT_PARTITION, false);
            } else if (c > 0) {
                getNextTuple(RIGHT_PARTITION, false);
            } else {
                return true;
            }
        }
        return false;
    }

    private void closeJoin() throws HyracksDataException {
        resultAppender.write(writer, true);
        secondaryTupleBufferManager.close();
        runFileStream.removeRunFile();
        runFileStream.close();
        consumerFrames[LEFT_PARTITION].closeFrameState();
        consumerFrames[RIGHT_PARTITION].closeFrameState();
    }

    // Entry Function

    @Override
    public void processJoin() throws HyracksDataException {
        try {
            getNextTuple(LEFT_PARTITION, false);
            getNextTuple(RIGHT_PARTITION, false);

            while (matchStreams()) {
                joinMatched();
            }
        } finally {
            closeJoin();
        }
    }
}
