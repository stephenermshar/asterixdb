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
package org.apache.asterix.runtime.operators.joins.intervalforwardsweep;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.IntervalJoinUtil;
import org.apache.asterix.runtime.operators.joins.intervalindex.TuplePrinterUtil;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.join.AbstractMergeJoiner;
import org.apache.hyracks.dataflow.std.join.MergeJoinLocks;
import org.apache.hyracks.dataflow.std.join.MergeStatus;
import org.apache.hyracks.dataflow.std.join.RunFileStream;
import org.apache.hyracks.dataflow.std.structures.RunFilePointer;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Interval Forward Sweep Joiner takes two sorted streams of input and joins.
 * The two sorted streams must be in a logical order and the comparator must
 * support keeping that order so the join will work.
 * The left stream will spill to disk when memory is full.
 * The both right and left use memory to maintain active intervals for the join.
 */
public class IntervalForwardSweepJoiner extends AbstractMergeJoiner {

    private static final Logger LOGGER = Logger.getLogger(IntervalForwardSweepJoiner.class.getName());

    private final IPartitionedDeletableTupleBufferManager bufferManager;

    private final ForwardSweepActiveManager[] activeManager;
    private final ITuplePointerAccessor[] memoryAccessor;
    private final int[] streamIndex;
    private final RunFileStream[] runFileStream;
    private final RunFilePointer[] runFilePointer;

    private final IIntervalMergeJoinChecker imjc;

    private final int leftKey;
    private final int rightKey;

    private long joinComparisonCount = 0;
    private long joinResultCount = 0;
    private long leftSpillCount = 0;
    private long rightSpillCount = 0;
    private long[] spillFileCount = { 0, 0 };
    private long[] spillReadCount = { 0, 0 };
    private long[] spillWriteCount = { 0, 0 };

    private final int partition;
    private final int memorySize;
    private final int processingPartition = -1;
    private final LinkedList<TuplePointer> processingGroup = new LinkedList<>();

    public IntervalForwardSweepJoiner(IHyracksTaskContext ctx, int memorySize, int partition, MergeStatus status,
            MergeJoinLocks locks, IIntervalMergeJoinCheckerFactory imjcf, int[] leftKeys, int[] rightKeys,
            RecordDescriptor leftRd, RecordDescriptor rightRd) throws HyracksDataException {
        super(ctx, partition, status, locks, leftRd, rightRd);
        this.partition = partition;
        this.memorySize = memorySize;

        this.imjc = imjcf.createMergeJoinChecker(leftKeys, rightKeys, ctx);

        this.leftKey = leftKeys[0];
        this.rightKey = rightKeys[0];

        RecordDescriptor[] recordDescriptors = new RecordDescriptor[JOIN_PARTITIONS];
        recordDescriptors[LEFT_PARTITION] = leftRd;
        recordDescriptors[RIGHT_PARTITION] = rightRd;

        streamIndex = new int[JOIN_PARTITIONS];
        streamIndex[LEFT_PARTITION] = TupleAccessor.UNSET;
        streamIndex[RIGHT_PARTITION] = TupleAccessor.UNSET;

        if (memorySize < 5) {
            throw new HyracksDataException(
                    "IntervalIndexJoiner does not have enough memory (needs > 4, got " + memorySize + ").");
        }
        //        bufferManager = new VPartitionDeletableTupleBufferManager(ctx,
        //                VPartitionDeletableTupleBufferManager.NO_CONSTRAIN, JOIN_PARTITIONS,
        //                (memorySize - 4) * ctx.getInitialFrameSize(), recordDescriptors);
        bufferManager =
                new VPartitionDeletableTupleBufferManager(ctx, VPartitionDeletableTupleBufferManager.NO_CONSTRAIN,
                        JOIN_PARTITIONS, memorySize * ctx.getInitialFrameSize(), recordDescriptors);
        memoryAccessor = new ITuplePointerAccessor[JOIN_PARTITIONS];
        memoryAccessor[LEFT_PARTITION] = bufferManager.getTuplePointerAccessor(leftRd);
        memoryAccessor[RIGHT_PARTITION] = bufferManager.getTuplePointerAccessor(rightRd);

        activeManager = new ForwardSweepActiveManager[JOIN_PARTITIONS];
        activeManager[LEFT_PARTITION] = new ForwardSweepActiveManager(bufferManager, LEFT_PARTITION);
        activeManager[RIGHT_PARTITION] = new ForwardSweepActiveManager(bufferManager, RIGHT_PARTITION);

        // Run files for both branches
        runFileStream = new RunFileStream[JOIN_PARTITIONS];
        runFileStream[LEFT_PARTITION] = new RunFileStream(ctx, "left", status.branch[LEFT_PARTITION]);
        runFileStream[RIGHT_PARTITION] = new RunFileStream(ctx, "right", status.branch[RIGHT_PARTITION]);
        runFilePointer = new RunFilePointer[JOIN_PARTITIONS];
        runFilePointer[LEFT_PARTITION] = new RunFilePointer();
        runFilePointer[RIGHT_PARTITION] = new RunFilePointer();

        LOGGER.setLevel(Level.FINE);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("IntervalIndexJoiner has started partition " + partition + " with " + memorySize
                    + " frames of memory.");
        }
    }

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2,
            boolean reversed, IFrameWriter writer) throws HyracksDataException {
        if (reversed) {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor2, index2, accessor1, index1);
        } else {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
        }
        joinResultCount++;
    }

    private void flushMemory(int partition) throws HyracksDataException {
        activeManager[partition].clear();
    }

    private TupleStatus loadSpilledTuple(int partition) throws HyracksDataException {
        if (!inputAccessor[partition].exists()) {
            // if statement must be separate.
            if (!runFileStream[partition].loadNextBuffer(inputAccessor[partition])) {
                return TupleStatus.EMPTY;
            }
        }
        return TupleStatus.LOADED;
    }

    private TupleStatus loadTuple(int partition) throws HyracksDataException {
        TupleStatus loaded;
        if (status.branch[partition].isRunFileReading()) {
            loaded = loadSpilledTuple(partition);
            if (loaded.isEmpty()) {
                continueStream(partition, inputAccessor[partition]);
                loaded = loadTuple(partition);
            }
        } else {
            loaded = loadMemoryTuple(partition);
        }
        return loaded;
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private TupleStatus loadRightTuple() throws HyracksDataException {
        TupleStatus loaded = loadTuple(RIGHT_PARTITION);
        if (loaded == TupleStatus.UNKNOWN) {
            loaded = pauseAndLoadRightTuple();
        }
        return loaded;
    }

    /**
     * Ensures a frame exists for the left branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private TupleStatus loadLeftTuple() throws HyracksDataException {
        return loadTuple(LEFT_PARTITION);
    }

    @Override
    public void processLeftFrame(IFrameWriter writer) throws HyracksDataException {
        TupleStatus leftTs = loadLeftTuple();
        TupleStatus rightTs = loadRightTuple();

        while (leftTs.isLoaded()) {
            // Frozen.
            if (runFileStream[LEFT_PARTITION].isWriting()) {
                processLeftTupleSpill(writer);
                inputAccessor[LEFT_PARTITION].next();
                leftTs = loadLeftTuple();
                continue;
            } else if (runFileStream[RIGHT_PARTITION].isWriting()) {
                processRightTupleSpill(writer);
                inputAccessor[RIGHT_PARTITION].next();
                rightTs = loadRightTuple();
                continue;
            }
            // Ensure a tuple is in memory.
            if (leftTs.isLoaded() && !activeManager[LEFT_PARTITION].hasRecords()) {
                TuplePointer tp = activeManager[LEFT_PARTITION].addTuple(inputAccessor[LEFT_PARTITION]);
                if (tp == null) {
                    // Should never happen if memory budget is correct.
                    throw new HyracksDataException("Left partition does not have access to a single page of memory.");
                }
                inputAccessor[LEFT_PARTITION].next();
                leftTs = loadLeftTuple();
                System.err.println("Active empty, load left: " + tp);
            }
            if (rightTs.isLoaded() && !activeManager[RIGHT_PARTITION].hasRecords()) {
                TuplePointer tp = activeManager[RIGHT_PARTITION].addTuple(inputAccessor[RIGHT_PARTITION]);
                if (tp == null) {
                    // Should never happen if memory budget is correct.
                    throw new HyracksDataException("Right partition does not have access to a single page of memory.");
                }
                inputAccessor[RIGHT_PARTITION].next();
                rightTs = loadRightTuple();
                System.err.println("Active empty, load right: " + tp);
            }
            // If both sides have value in memory, run join.
            if (activeManager[LEFT_PARTITION].hasRecords() && activeManager[RIGHT_PARTITION].hasRecords()) {
                if (checkToProcessRightTuple()) {
                    // Right side from stream
                    processRightTuple(writer);
                    leftTs = loadLeftTuple();
                } else {
                    // Left side from stream
                    processLeftTuple(writer);
                    rightTs = loadRightTuple();
                }
            }
        }
        //            if (runFileStream[RIGHT_PARTITION].isWriting()) {
        //                // Right side from disk
        //                rightTs = processRightTupleSpill(writer);
        //            } else if (runFileStream[LEFT_PARTITION].isWriting()) {
        //                // Left side from disk
        //                leftTs = processLeftTupleSpill(writer);
        //            } else {
        //            }

    }

    @Override
    public void processLeftClose(IFrameWriter writer) throws HyracksDataException {
        processLeftFrame(writer);
        resultAppender.write(writer, true);

        activeManager[LEFT_PARTITION].clear();
        activeManager[RIGHT_PARTITION].clear();
        runFileStream[LEFT_PARTITION].close();
        runFileStream[RIGHT_PARTITION].close();

        if (LOGGER.isLoggable(Level.WARNING)) {
            long ioCost = runFileStream[LEFT_PARTITION].getWriteCount() + runFileStream[LEFT_PARTITION].getReadCount()
                    + runFileStream[RIGHT_PARTITION].getWriteCount() + runFileStream[RIGHT_PARTITION].getReadCount();
            LOGGER.warning(",IntervalIndexJoiner Statistics Log," + partition + ",partition," + memorySize + ",memory,"
                    + joinResultCount + ",results," + joinComparisonCount + ",CPU," + ioCost + ",IO," + leftSpillCount
                    + ",left spills," + runFileStream[LEFT_PARTITION].getWriteCount() + ",left frames_written,"
                    + runFileStream[LEFT_PARTITION].getReadCount() + ",left frames_read," + rightSpillCount
                    + ",right spills," + runFileStream[RIGHT_PARTITION].getWriteCount() + ",right frames_written,"
                    + runFileStream[RIGHT_PARTITION].getReadCount() + ",right frames_read");
        }
    }

    private boolean checkHasMoreProcessing(TupleStatus ts, int partition, int joinPartition) {
        return ts.isLoaded() || status.branch[partition].isRunFileWriting()
                || (checkHasMoreTuples(joinPartition) && activeManager[partition].hasRecords());
    }

    private boolean checkHasMoreTuples(int partition) {
        return status.branch[partition].hasMore() || status.branch[partition].isRunFileReading();
    }

    //    private String printTuple(ITupleAccessor accessor,TuplePointer tp) {
    //       
    //        return  IntervalJoinUtil.getIntervalPointable(accessor, tp.getTupleIndex(), leftKey);
    //
    //    }

    private boolean checkToProcessRightTuple() {
        TuplePointer leftTp = activeManager[LEFT_PARTITION].getFirst();
        memoryAccessor[LEFT_PARTITION].reset(leftTp);
        long leftStart =
                IntervalJoinUtil.getIntervalStart(memoryAccessor[LEFT_PARTITION], leftTp.getTupleIndex(), leftKey);

        TuplePointer rightTp = activeManager[RIGHT_PARTITION].getFirst();
        memoryAccessor[RIGHT_PARTITION].reset(rightTp);
        long rightStart =
                IntervalJoinUtil.getIntervalStart(memoryAccessor[RIGHT_PARTITION], rightTp.getTupleIndex(), rightKey);
        if (leftStart < rightStart) {
            // Left stream has next tuple, check if right active must be updated first.
            return activeManager[RIGHT_PARTITION].hasRecords();
        } else {
            // Right stream has next tuple, check if left active must be update first.
            return !(activeManager[LEFT_PARTITION].hasRecords());
        }
    }

    private boolean checkToProcessAdd(long startMemory, long endMemory) {
        return startMemory < endMemory;
    }

    private TupleStatus processLeftTupleSpill(IFrameWriter writer) throws HyracksDataException {
        // Process left tuples one by one, check them with active memory from the right branch.
        int count = 0;
        TupleStatus ts = loadLeftTuple();
        while (ts.isLoaded() && activeManager[RIGHT_PARTITION].hasRecords() && inputAccessor[LEFT_PARTITION].exists()) {
            int tupleId = inputAccessor[LEFT_PARTITION].getTupleId();
            runFileStream[LEFT_PARTITION].addToRunFile(inputAccessor[LEFT_PARTITION]);
            processTupleJoin(inputAccessor[LEFT_PARTITION], tupleId, RIGHT_PARTITION, writer);
            inputAccessor[LEFT_PARTITION].next();
            ts = loadLeftTuple();
        }

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Spill for " + count + " left tuples");
        }

        // Memory is empty and we can start processing the run file.
        if (activeManager[RIGHT_PARTITION].isEmpty() || ts.isEmpty()) {
            unfreezeAndContinue(LEFT_PARTITION, inputAccessor[LEFT_PARTITION]);
            ts = loadLeftTuple();
        }
        return ts;
    }

    private TupleStatus processRightTupleSpill(IFrameWriter writer) throws HyracksDataException {
        // Process left tuples one by one, check them with active memory from the right branch.
        int count = 0;
        TupleStatus ts = loadRightTuple();
        while (ts.isLoaded() && activeManager[LEFT_PARTITION].hasRecords() && inputAccessor[RIGHT_PARTITION].exists()) {
            int tupleId = inputAccessor[RIGHT_PARTITION].getTupleId();
            runFileStream[RIGHT_PARTITION].addToRunFile(inputAccessor[RIGHT_PARTITION]);
            processTupleJoin(inputAccessor[RIGHT_PARTITION], tupleId, LEFT_PARTITION, writer);
            inputAccessor[RIGHT_PARTITION].next();
            ts = loadRightTuple();
        }

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Spill for " + count + " right tuples");
        }

        // Memory is empty and we can start processing the run file.
        if (!activeManager[LEFT_PARTITION].hasRecords() || ts.isEmpty()) {
            unfreezeAndContinue(RIGHT_PARTITION, inputAccessor[RIGHT_PARTITION]);
            ts = loadRightTuple();
        }
        return ts;
    }

    private void processLeftTuple(IFrameWriter writer) throws HyracksDataException {
        // Check tuple with all memory.
        // Purge as processing
        // Added new items from right to memory and check
        if (!activeManager[LEFT_PARTITION].hasRecords()) {
            return;
        }
        TuplePointer searchTp = activeManager[LEFT_PARTITION].getFirst();
        memoryAccessor[LEFT_PARTITION].reset(searchTp);

        // Compare with tuple in memory
        for (Iterator<TuplePointer> iterator = activeManager[RIGHT_PARTITION].getIterator(); iterator.hasNext();) {
            TuplePointer matchTp = iterator.next();
            memoryAccessor[RIGHT_PARTITION].reset(matchTp);
            // Add to result if matched.
            if (imjc.checkToSaveInResult(memoryAccessor[LEFT_PARTITION], searchTp.getTupleIndex(),
                    memoryAccessor[RIGHT_PARTITION], matchTp.getTupleIndex(), false)) {
                addToResult(memoryAccessor[LEFT_PARTITION], searchTp.getTupleIndex(), memoryAccessor[RIGHT_PARTITION],
                        matchTp.getTupleIndex(), false, writer);
                System.err.println("Memory Match: " + searchTp + " " + matchTp);
            }
            joinComparisonCount++;
            // Remove if the tuple no long matches.
            if (imjc.checkToRemoveInMemory(memoryAccessor[LEFT_PARTITION], searchTp.getTupleIndex(),
                    memoryAccessor[RIGHT_PARTITION], matchTp.getTupleIndex())) {
                System.err.println("Remove right tuple: " + matchTp);
                activeManager[RIGHT_PARTITION].remove(iterator, matchTp);
            }
        }

        // Add tuples from the stream.
        while (loadRightTuple().isLoaded()
                && imjc.checkToSaveInMemory(memoryAccessor[LEFT_PARTITION], searchTp.getTupleIndex(),
                        inputAccessor[RIGHT_PARTITION], inputAccessor[RIGHT_PARTITION].getTupleId())) {
            TuplePointer tp = activeManager[RIGHT_PARTITION].addTuple(inputAccessor[RIGHT_PARTITION]);
            if (tp != null) {
                memoryAccessor[RIGHT_PARTITION].reset(tp);
                System.err.println("Stream add: " + tp);
                // Add to result if matched.
                if (imjc.checkToSaveInResult(memoryAccessor[LEFT_PARTITION], searchTp.getTupleIndex(),
                        memoryAccessor[RIGHT_PARTITION], tp.getTupleIndex(), false)) {
                    addToResult(memoryAccessor[LEFT_PARTITION], searchTp.getTupleIndex(),
                            memoryAccessor[RIGHT_PARTITION], tp.getTupleIndex(), false, writer);
                    System.err.println("Stream Match: " + searchTp + " " + tp);
                }
                joinComparisonCount++;
            } else {
                // Spill case, remove search tuple before freeze.
                activeManager[LEFT_PARTITION].remove(searchTp);
                freezeAndSpill(writer);
                return;
            }
            inputAccessor[RIGHT_PARTITION].next();
        }

        // Remove search tuple
        activeManager[LEFT_PARTITION].remove(searchTp);
    }

    private void processRightTuple(IFrameWriter writer) throws HyracksDataException {
        // Check tuple with all memory.
        // Purge as processing
        // Added new items from right to memory and check
        if (!activeManager[RIGHT_PARTITION].hasRecords()) {
            return;
        }
        TuplePointer searchTp = activeManager[RIGHT_PARTITION].getFirst();
        memoryAccessor[RIGHT_PARTITION].reset(searchTp);

        // Compare with tuple in memory
        for (Iterator<TuplePointer> iterator = activeManager[LEFT_PARTITION].getIterator(); iterator.hasNext();) {
            TuplePointer matchTp = iterator.next();
            memoryAccessor[LEFT_PARTITION].reset(matchTp);
            // Add to result if matched.
            if (imjc.checkToSaveInResult(memoryAccessor[RIGHT_PARTITION], searchTp.getTupleIndex(),
                    memoryAccessor[LEFT_PARTITION], matchTp.getTupleIndex(), false)) {
                addToResult(memoryAccessor[RIGHT_PARTITION], searchTp.getTupleIndex(), memoryAccessor[LEFT_PARTITION],
                        matchTp.getTupleIndex(), false, writer);
                System.err.println("Memory Match: " + searchTp + " " + matchTp);
            }
            joinComparisonCount++;
            // Remove if the tuple no long matches.
            if (imjc.checkToRemoveInMemory(memoryAccessor[RIGHT_PARTITION], searchTp.getTupleIndex(),
                    memoryAccessor[LEFT_PARTITION], matchTp.getTupleIndex())) {
                activeManager[LEFT_PARTITION].remove(iterator, matchTp);
                System.err.println("Remove left tuple: " + matchTp);
            }
        }

        // Add tuples from the stream.
        while (loadLeftTuple().isLoaded() && imjc.checkToSaveInMemory(memoryAccessor[RIGHT_PARTITION],
                searchTp.getTupleIndex(), inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId())) {
            TuplePointer tp = activeManager[LEFT_PARTITION].addTuple(inputAccessor[LEFT_PARTITION]);
            if (tp != null) {
                memoryAccessor[LEFT_PARTITION].reset(tp);
                System.err.println("Stream add: " + tp);
                // Add to result if matched.
                if (imjc.checkToSaveInResult(memoryAccessor[RIGHT_PARTITION], searchTp.getTupleIndex(),
                        memoryAccessor[LEFT_PARTITION], tp.getTupleIndex(), false)) {
                    addToResult(memoryAccessor[RIGHT_PARTITION], searchTp.getTupleIndex(),
                            memoryAccessor[LEFT_PARTITION], tp.getTupleIndex(), false, writer);
                    System.err.println("Stream Match: " + searchTp + " " + tp);
                }
                joinComparisonCount++;
            } else {
                // Spill case, remove search tuple before freeze.
                activeManager[RIGHT_PARTITION].remove(searchTp);
                freezeAndSpill(writer);
                return;
            }
            inputAccessor[LEFT_PARTITION].next();
        }

        // Remove search tuple
        activeManager[RIGHT_PARTITION].remove(searchTp);
    }

    private void processInMemoryJoin(int outer, int inner, IFrameWriter writer) throws HyracksDataException {
        // Compare with tuple in memory
        for (Iterator<TuplePointer> outerIterator = activeManager[outer].getIterator(); outerIterator.hasNext();) {
            TuplePointer outerTp = outerIterator.next();
            memoryAccessor[outer].reset(outerTp);
            processTupleJoin(memoryAccessor[outer], outerTp.getTupleIndex(), inner, writer);
        }
    }

    private void processTupleJoin(IFrameTupleAccessor accessor, int tupleId, int inner, IFrameWriter writer)
            throws HyracksDataException {
        // Compare with tuple in memory
        for (Iterator<TuplePointer> innerIterator = activeManager[inner].getIterator(); innerIterator.hasNext();) {
            TuplePointer innerTp = innerIterator.next();
            memoryAccessor[inner].reset(innerTp);

//            TuplePrinterUtil.printTuple("Outer", accessor, tupleId);
//            TuplePrinterUtil.printTuple("Inner", memoryAccessor[inner], innerTp.getTupleIndex());
            // Add to result if matched.
            if (imjc.checkToSaveInResult(accessor, tupleId, memoryAccessor[inner], innerTp.getTupleIndex(), false)) {
                addToResult(accessor, tupleId, memoryAccessor[inner], innerTp.getTupleIndex(), false, writer);
                System.err.println("Memory Match: " + tupleId + " " + innerTp);
            }
            joinComparisonCount++;
            // Remove if the tuple no long matches.
            if (imjc.checkToRemoveInMemory(accessor, tupleId, memoryAccessor[inner], innerTp.getTupleIndex())) {
                System.err.println("Remove right tuple: " + innerTp);
                activeManager[inner].remove(innerIterator, innerTp);
            }
            // Exit if no more possible matches
            if (!imjc.checkIfMoreMatches(accessor, tupleId, memoryAccessor[inner], innerTp.getTupleIndex())) {
                break;
            }
        }
    }

    private void freezeAndSpill(IFrameWriter writer) throws HyracksDataException {
        //        if (LOGGER.isLoggable(Level.FINEST)) {
        //            LOGGER.finest("freeze snapshot: " + frameCounts[RIGHT_PARTITION] + " right, " + frameCounts[LEFT_PARTITION]
        //                    + " left, left[" + bufferManager.getNumTuples(LEFT_PARTITION) + " memory]. right["
        //                    + bufferManager.getNumTuples(RIGHT_PARTITION) + " memory].");
        //        }
        //        LOGGER.warning("disk IO: right, " + runFileStream[RIGHT_PARTITION].getReadCount() + " left, "
        //                + runFileStream[LEFT_PARTITION].getReadCount());
        int freezePartition;
        if (bufferManager.getNumTuples(LEFT_PARTITION) > bufferManager.getNumTuples(RIGHT_PARTITION)) {
            freezePartition = RIGHT_PARTITION;
            processInMemoryJoin(freezePartition, LEFT_PARTITION, writer);
            rightSpillCount++;
        } else {
            freezePartition = LEFT_PARTITION;
            processInMemoryJoin(freezePartition, RIGHT_PARTITION, writer);
            leftSpillCount++;
        }

        //        LOGGER.warning("freeze snapshot(" + freezePartition + "): " + frameCounts[RIGHT_PARTITION] + " right, "
        //                + frameCounts[LEFT_PARTITION] + " left, left[" + bufferManager.getNumTuples(LEFT_PARTITION)
        //                + " memory, " + leftSpillCount + " spills, "
        //                + (runFileStream[LEFT_PARTITION].getFileCount() - spillFileCount[LEFT_PARTITION]) + " files, "
        //                + (runFileStream[LEFT_PARTITION].getWriteCount() - spillWriteCount[LEFT_PARTITION]) + " written, "
        //                + (runFileStream[LEFT_PARTITION].getReadCount() - spillReadCount[LEFT_PARTITION]) + " read]. right["
        //                + bufferManager.getNumTuples(RIGHT_PARTITION) + " memory, " + +rightSpillCount + " spills, "
        //                + (runFileStream[RIGHT_PARTITION].getFileCount() - spillFileCount[RIGHT_PARTITION]) + " files, "
        //                + (runFileStream[RIGHT_PARTITION].getWriteCount() - spillWriteCount[RIGHT_PARTITION]) + " written, "
        //                + (runFileStream[RIGHT_PARTITION].getReadCount() - spillReadCount[RIGHT_PARTITION]) + " read].");
        spillFileCount[LEFT_PARTITION] = runFileStream[LEFT_PARTITION].getFileCount();
        spillReadCount[LEFT_PARTITION] = runFileStream[LEFT_PARTITION].getReadCount();
        spillWriteCount[LEFT_PARTITION] = runFileStream[LEFT_PARTITION].getWriteCount();
        spillFileCount[RIGHT_PARTITION] = runFileStream[RIGHT_PARTITION].getFileCount();
        spillReadCount[RIGHT_PARTITION] = runFileStream[RIGHT_PARTITION].getReadCount();
        spillWriteCount[RIGHT_PARTITION] = runFileStream[RIGHT_PARTITION].getWriteCount();

        // Mark where to start reading
        if (runFileStream[freezePartition].isReading()) {
            runFilePointer[freezePartition].reset(runFileStream[freezePartition].getReadPointer(),
                    inputAccessor[freezePartition].getTupleId());
        } else {
            runFilePointer[freezePartition].reset(0, 0);
            runFileStream[freezePartition].createRunFileWriting();
        }
        // Start writing
        runFileStream[freezePartition].startRunFileWriting();

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("Memory is full. Freezing the " + freezePartition + " branch. (Left memory tuples: "
                    + bufferManager.getNumTuples(LEFT_PARTITION) + ", Right memory tuples: "
                    + bufferManager.getNumTuples(RIGHT_PARTITION) + ")");
            bufferManager.printStats("memory details");
        }
    }

    private void continueStream(int diskPartition, ITupleAccessor accessor) throws HyracksDataException {
        // Stop reading.
        runFileStream[diskPartition].closeRunFileReading();
        if (runFilePointer[diskPartition].getFileOffset() < 0) {
            // Remove file if not needed.
            runFileStream[diskPartition].close();
            runFileStream[diskPartition].removeRunFile();
        }

        // Continue on stream
        accessor.reset(inputBuffer[diskPartition].getBuffer());
        accessor.setTupleId(streamIndex[diskPartition]);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Continue with stream (" + diskPartition + ").");
        }
    }

    private void unfreezeAndContinue(int frozenPartition, ITupleAccessor accessor) throws HyracksDataException {
        int flushPartition = frozenPartition == LEFT_PARTITION ? RIGHT_PARTITION : LEFT_PARTITION;
        //                if (LOGGER.isLoggable(Level.FINEST)) {
        //        LOGGER.warning("unfreeze snapshot(" + frozenPartition + "): " + frameCounts[RIGHT_PARTITION] + " right, "
        //                + frameCounts[LEFT_PARTITION] + " left, left[" + bufferManager.getNumTuples(LEFT_PARTITION)
        //                + " memory, " + leftSpillCount + " spills, "
        //                + (runFileStream[LEFT_PARTITION].getFileCount() - spillFileCount[LEFT_PARTITION]) + " files, "
        //                + (runFileStream[LEFT_PARTITION].getWriteCount() - spillWriteCount[LEFT_PARTITION]) + " written, "
        //                + (runFileStream[LEFT_PARTITION].getReadCount() - spillReadCount[LEFT_PARTITION]) + " read]. right["
        //                + bufferManager.getNumTuples(RIGHT_PARTITION) + " memory, " + rightSpillCount + " spills, "
        //                + (runFileStream[RIGHT_PARTITION].getFileCount() - spillFileCount[RIGHT_PARTITION]) + " files, "
        //                + (runFileStream[RIGHT_PARTITION].getWriteCount() - spillWriteCount[RIGHT_PARTITION]) + " written, "
        //                + (runFileStream[RIGHT_PARTITION].getReadCount() - spillReadCount[RIGHT_PARTITION]) + " read].");
        //        spillFileCount[LEFT_PARTITION] = runFileStream[LEFT_PARTITION].getFileCount();
        //        spillReadCount[LEFT_PARTITION] = runFileStream[LEFT_PARTITION].getReadCount();
        //        spillWriteCount[LEFT_PARTITION] = runFileStream[LEFT_PARTITION].getWriteCount();
        //        spillFileCount[RIGHT_PARTITION] = runFileStream[RIGHT_PARTITION].getFileCount();
        //        spillReadCount[RIGHT_PARTITION] = runFileStream[RIGHT_PARTITION].getReadCount();
        //        spillWriteCount[RIGHT_PARTITION] = runFileStream[RIGHT_PARTITION].getWriteCount();
        //                }

        // Finish writing
        runFileStream[frozenPartition].flushRunFile();

        // Clear memory
        flushMemory(flushPartition);
        if ((LEFT_PARTITION == frozenPartition && !runFileStream[LEFT_PARTITION].isReading())
                || (RIGHT_PARTITION == frozenPartition && !runFileStream[RIGHT_PARTITION].isReading())) {
            streamIndex[frozenPartition] = accessor.getTupleId();
        }

        // Start reading
        runFileStream[frozenPartition].startReadingRunFile(accessor, runFilePointer[frozenPartition].getFileOffset());
        accessor.setTupleId(runFilePointer[frozenPartition].getTupleIndex());
        runFilePointer[frozenPartition].reset(-1, -1);

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Unfreezing (" + frozenPartition + ").");
        }
    }

}
