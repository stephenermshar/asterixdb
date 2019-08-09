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

    protected static TuplePointer tempPtr = new TuplePointer(); // (stephen) for method signature, see OptimizedHybridHashJoin
    private final ITupleAccessor runFileAccessor;
    private final RunFileStream runFileStream;

    VPartitionTupleBufferManager secondaryTupleBufferManager;
    ITupleAccessor secondaryTupleBufferAccessor;
    ITuplePairComparator[] comparators;
    IFrameWriter writer;

    protected final int availableMemoryForJoinInFrames;

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int LEFT_PARTITION = 0;
    protected static final int RIGHT_PARTITION = 1;

    protected final MergeBranchStatus[] branchStatus;
    protected final IConsumerFrame[] consumerFrames;
    private final FrameTupleAppender resultAppender;

    protected final IFrame[] inputBuffer;
    protected final ITupleAccessor[] inputAccessor;

    // for debugging
    protected long[] frameCounts = { 0, 0 };
    protected long[] tupleCounts = { 0, 0 };
    private long[][] currentTuple;
    private long[][] toJoin;
    private String joinTypeStatus;

    public MergeJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF, IFrameWriter writer,
            int memoryForJoinInFrames, ITuplePairComparator[] comparators) throws HyracksDataException {

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

        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));

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

        runFileStream = new RunFileStream(ctx, "left", branchStatus[LEFT_PARTITION]);

        // ----------- POTENTIAL PROBLEM AREA FOR SPILLING -------------
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
            return false;
        }

        if (!secondaryTupleBufferAccessor.exists()) {
            return false;
        }

        return 0 == compare(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                secondaryTupleBufferAccessor, secondaryTupleBufferAccessor.getTupleId());
    }

    private void loadAllLeftIntoRunFile() throws HyracksDataException {

        runFileStream.createRunFileWriting();
        runFileStream.startRunFileWriting();
        runFileStream.addToRunFile(inputAccessor[LEFT_PARTITION]);

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

    private void joinFromFile() throws HyracksDataException {

        boolean loadRightSuccessful = false;
        loadAllLeftIntoRunFile();

        while (!loadRightSuccessful) {
            while (runFileAccessor.exists() && secondaryTupleBufferAccessor.exists()) {
                join(runFileAccessor);
                getNextLeftTupleFromFile();
            }
            runFileStream.startReadingRunFile(runFileAccessor);
            loadRightSuccessful = loadRight(runFileAccessor);
        }
        while (runFileAccessor.exists() && secondaryTupleBufferAccessor.exists()) {
            join(runFileAccessor);
            getNextLeftTupleFromFile();
        }
        clearRunFile();
    }

    private void joinFromStream() throws HyracksDataException {
        while (inputAccessor[LEFT_PARTITION].exists() && secondaryTupleBufferAccessor.exists()
                && compareTupleWithBuffer()) {

            join(inputAccessor[LEFT_PARTITION]);
            getNextTuple(LEFT_PARTITION);
        }
        clearSavedRight();
    }

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

    private boolean loadRight(ITupleAccessor leftAccessor) throws HyracksDataException {

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
