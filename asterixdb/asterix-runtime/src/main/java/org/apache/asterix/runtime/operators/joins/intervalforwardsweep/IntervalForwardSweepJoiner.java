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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.IntervalJoinUtil;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.join.AbstractMergeJoiner;
import org.apache.hyracks.dataflow.std.join.MergeJoinLocks;
import org.apache.hyracks.dataflow.std.join.MergeStatus;
import org.apache.hyracks.dataflow.std.join.RunFileStream;
import org.apache.hyracks.dataflow.std.structures.RunFilePointer;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

class IntervalSideTuple {
    // Tuple access
    int fieldId;
    ITupleAccessor accessor;
    int tupleIndex;
    int frameIndex = -1;

    // Join details
    final IIntervalMergeJoinChecker imjc;

    // Interval details
    long start;
    long end;

    public IntervalSideTuple(IIntervalMergeJoinChecker imjc, ITupleAccessor accessor, int fieldId) {
        this.imjc = imjc;
        this.accessor = accessor;
        this.fieldId = fieldId;
    }

    public void setTuple(TuplePointer tp) {
        if (frameIndex != tp.getFrameIndex()) {
            accessor.reset(tp);
            frameIndex = tp.getFrameIndex();
        }
        tupleIndex = tp.getTupleIndex();
        int offset = IntervalJoinUtil.getIntervalOffset(accessor, tupleIndex, fieldId);
        start = AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), offset);
        end = AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), offset);
    }

    public void loadTuple() {
        tupleIndex = accessor.getTupleId();
        int offset = IntervalJoinUtil.getIntervalOffset(accessor, tupleIndex, fieldId);
        start = AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), offset);
        end = AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), offset);
    }

    public int getTupleIndex() {
        return tupleIndex;
    }

    public ITupleAccessor getAccessor() {
        return accessor;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public boolean compareJoin(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkToSaveInResult(accessor, tupleIndex, ist.accessor, ist.tupleIndex, true);
    }

    public boolean addToMemory(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkToSaveInMemory(accessor, tupleIndex, ist.accessor, ist.tupleIndex);
    }

    public boolean removeFromMemory(IntervalSideTuple ist) throws HyracksDataException {
        return imjc.checkToRemoveInMemory(accessor, tupleIndex, ist.accessor, ist.tupleIndex);
    }

    public boolean startsBefore(IntervalSideTuple ist) {
        return start <= ist.start;
    }

}

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
    private final ITupleAccessor[] memoryAccessor;
    private final int[] streamIndex;
    private final RunFileStream[] runFileStream;
    private final RunFilePointer[] runFilePointer;

    private IntervalSideTuple[] memoryTuple;
    private IntervalSideTuple[] inputTuple;

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
    private static final LinkedList<TuplePointer> empty = new LinkedList<>();

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

        //        if (memorySize < 5) {
        //            throw new HyracksDataException(
        //                    "IntervalForwardSweepJoiner does not have enough memory (needs > 4, got " + memorySize + ").");
        //        }
        //        bufferManager = new VPartitionDeletableTupleBufferManager(ctx,
        //                VPartitionDeletableTupleBufferManager.NO_CONSTRAIN, JOIN_PARTITIONS,
        //                (memorySize - 4) * ctx.getInitialFrameSize(), recordDescriptors);
        bufferManager =
                new VPartitionDeletableTupleBufferManager(ctx, VPartitionDeletableTupleBufferManager.NO_CONSTRAIN,
                        JOIN_PARTITIONS, memorySize * ctx.getInitialFrameSize(), recordDescriptors);
        memoryAccessor = new ITupleAccessor[JOIN_PARTITIONS];
        memoryAccessor[LEFT_PARTITION] = bufferManager.getTupleAccessor(leftRd);
        memoryAccessor[RIGHT_PARTITION] = bufferManager.getTupleAccessor(rightRd);

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

        memoryTuple = new IntervalSideTuple[JOIN_PARTITIONS];
        memoryTuple[LEFT_PARTITION] = new IntervalSideTuple(imjc, memoryAccessor[LEFT_PARTITION], leftKey);
        memoryTuple[RIGHT_PARTITION] = new IntervalSideTuple(imjc, memoryAccessor[RIGHT_PARTITION], rightKey);

        inputTuple = new IntervalSideTuple[JOIN_PARTITIONS];
        inputTuple[LEFT_PARTITION] = new IntervalSideTuple(imjc, inputAccessor[LEFT_PARTITION], leftKey);
        inputTuple[RIGHT_PARTITION] = new IntervalSideTuple(imjc, inputAccessor[RIGHT_PARTITION], rightKey);

        //        LOGGER.setLevel(Level.FINE);
        //        if (LOGGER.isLoggable(Level.FINE)) {
        System.out.println("IntervalForwardSweepJoiner has started partition " + partition + " with " + memorySize
                + " frames of memory.");
        //        }
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

    private TupleStatus loadSideTuple(int partition) throws HyracksDataException {
        if (partition == RIGHT_PARTITION) {
            return loadRightTuple();
        } else {
            return loadLeftTuple();
        }
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
        TupleStatus leftTs = loadSideTuple(LEFT_PARTITION);
        TupleStatus rightTs = loadSideTuple(RIGHT_PARTITION);

        while (leftTs.isLoaded() && (rightTs.isLoaded() || activeManager[RIGHT_PARTITION].hasRecords())) {
            // Frozen.
            if (runFileStream[LEFT_PARTITION].isWriting()) {
                processTupleSpill(LEFT_PARTITION, RIGHT_PARTITION, false, writer);
                //inputAccessor[LEFT_PARTITION].next();
                leftTs = loadSideTuple(LEFT_PARTITION);
                continue;
            } else if (runFileStream[RIGHT_PARTITION].isWriting()) {
                processTupleSpill(RIGHT_PARTITION, LEFT_PARTITION, true, writer);
                //inputAccessor[RIGHT_PARTITION].next();
                rightTs = loadSideTuple(RIGHT_PARTITION);
                continue;
            }
            // Ensure a tuple is in memory.
            if (leftTs.isLoaded() && !activeManager[LEFT_PARTITION].hasRecords()) {
                TuplePointer tp = activeManager[LEFT_PARTITION].addTuple(inputAccessor[LEFT_PARTITION]);
                if (tp == null) {
                    // Should never happen if memory budget is correct.
                    throw new HyracksDataException("Left partition does not have access to a single page of memory.");
                }
                //                System.err.println("Active empty, load left: " + tp);
                //                TuplePrinterUtil.printTuple("    left: ", inputAccessor[LEFT_PARTITION]);
                inputAccessor[LEFT_PARTITION].next();
                leftTs = loadSideTuple(LEFT_PARTITION);
            }
            if (rightTs.isLoaded() && !activeManager[RIGHT_PARTITION].hasRecords()) {
                TuplePointer tp = activeManager[RIGHT_PARTITION].addTuple(inputAccessor[RIGHT_PARTITION]);
                if (tp == null) {
                    // Should never happen if memory budget is correct.
                    throw new HyracksDataException("Right partition does not have access to a single page of memory.");
                }
                //                System.err.println("Active empty, load right: " + tp);
                //                TuplePrinterUtil.printTuple("    right: ", inputAccessor[RIGHT_PARTITION]);
                inputAccessor[RIGHT_PARTITION].next();
                rightTs = loadSideTuple(RIGHT_PARTITION);
            }
            // If both sides have value in memory, run join.
            if (activeManager[LEFT_PARTITION].hasRecords() && activeManager[RIGHT_PARTITION].hasRecords()) {
                if (checkToProcessRightTuple()) {
                    // Right side from stream
                    processTuple(RIGHT_PARTITION, LEFT_PARTITION, writer);
                } else {
                    // Left side from stream
                    processTuple(LEFT_PARTITION, RIGHT_PARTITION, writer);
                }
                rightTs = loadSideTuple(RIGHT_PARTITION);
                leftTs = loadSideTuple(LEFT_PARTITION);
            }
        }
    }

    @Override
    public void processLeftClose(IFrameWriter writer) throws HyracksDataException {
        if (runFileStream[LEFT_PARTITION].isWriting()) {
            unfreezeAndContinue(LEFT_PARTITION, inputAccessor[LEFT_PARTITION]);
        }
        processLeftFrame(writer);
        // Continue loading Right side while memory is available
        TupleStatus rightTs = loadRightTuple();
        while (activeManager[LEFT_PARTITION].hasRecords() && rightTs.isLoaded()) {
            if (rightTs.isLoaded() && !activeManager[RIGHT_PARTITION].hasRecords()) {
                TuplePointer tp = activeManager[RIGHT_PARTITION].addTuple(inputAccessor[RIGHT_PARTITION]);
                if (tp == null) {
                    // Should never happen if memory budget is correct.
                    throw new HyracksDataException("Right partition does not have access to a single page of memory.");
                }
                //                System.err.println("Active empty, load right: " + tp);
                //                TuplePrinterUtil.printTuple("    right: ", inputAccessor[RIGHT_PARTITION]);
                inputAccessor[RIGHT_PARTITION].next();
                rightTs = loadRightTuple();
            }
            // If both sides have value in memory, run join.
            if (activeManager[LEFT_PARTITION].hasRecords() && activeManager[RIGHT_PARTITION].hasRecords()) {
                // Left side from stream
                processTuple(LEFT_PARTITION, RIGHT_PARTITION, writer);
                rightTs = loadRightTuple();
            }
        }
        // Process in memory tuples
        processInMemoryJoin(LEFT_PARTITION, RIGHT_PARTITION, false, writer, empty);

        resultAppender.write(writer, true);

        activeManager[LEFT_PARTITION].clear();
        activeManager[RIGHT_PARTITION].clear();
        runFileStream[LEFT_PARTITION].close();
        runFileStream[RIGHT_PARTITION].close();

        if (LOGGER.isLoggable(Level.WARNING)) {
            long ioCost = runFileStream[LEFT_PARTITION].getWriteCount() + runFileStream[LEFT_PARTITION].getReadCount()
                    + runFileStream[RIGHT_PARTITION].getWriteCount() + runFileStream[RIGHT_PARTITION].getReadCount();
            LOGGER.warning(",IntervalForwardSweepJoiner Statistics Log," + partition + ",partition," + memorySize
                    + ",memory," + joinResultCount + ",results," + joinComparisonCount + ",CPU," + ioCost + ",IO,"
                    + leftSpillCount + ",left spills," + runFileStream[LEFT_PARTITION].getWriteCount()
                    + ",left frames_written," + runFileStream[LEFT_PARTITION].getReadCount() + ",left frames_read,"
                    + rightSpillCount + ",right spills," + runFileStream[RIGHT_PARTITION].getWriteCount()
                    + ",right frames_written," + runFileStream[RIGHT_PARTITION].getReadCount() + ",right frames_read");
        }
//        long ioCost = runFileStream[LEFT_PARTITION].getWriteCount() + runFileStream[LEFT_PARTITION].getReadCount()
//                + runFileStream[RIGHT_PARTITION].getWriteCount() + runFileStream[RIGHT_PARTITION].getReadCount();
//        System.out.println(",IntervalForwardSweepJoiner Statistics Log," + partition + ",partition," + memorySize
//                + ",memory," + joinResultCount + ",results," + joinComparisonCount + ",CPU," + ioCost + ",IO,"
//                + leftSpillCount + ",left spills," + runFileStream[LEFT_PARTITION].getWriteCount()
//                + ",left frames_written," + runFileStream[LEFT_PARTITION].getReadCount() + ",left frames_read,"
//                + rightSpillCount + ",right spills," + runFileStream[RIGHT_PARTITION].getWriteCount()
//                + ",right frames_written," + runFileStream[RIGHT_PARTITION].getReadCount() + ",right frames_read,"
//                + runFileStream[LEFT_PARTITION].getTupleCount() + ",left tuple_count,"
//                + runFileStream[RIGHT_PARTITION].getTupleCount() + ",right tuple_count");
//        System.out.println("left=" + frameCounts[0] + ", right=" + frameCounts[1]);
    }

    private boolean checkToProcessRightTuple() {
        TuplePointer leftTp = activeManager[LEFT_PARTITION].getFirst();
        memoryAccessor[LEFT_PARTITION].reset(leftTp);
        long leftStart =
                IntervalJoinUtil.getIntervalStart(memoryAccessor[LEFT_PARTITION], leftTp.getTupleIndex(), leftKey);

        TuplePointer rightTp = activeManager[RIGHT_PARTITION].getFirst();
        memoryAccessor[RIGHT_PARTITION].reset(rightTp);
        long rightStart =
                IntervalJoinUtil.getIntervalStart(memoryAccessor[RIGHT_PARTITION], rightTp.getTupleIndex(), rightKey);
        return leftStart < rightStart;
    }

    private TupleStatus processTupleSpill(int main, int other, boolean reversed, IFrameWriter writer)
            throws HyracksDataException {
        // Process left tuples one by one, check them with active memory from the right branch.
        int count = 0;
        TupleStatus ts = loadSideTuple(main);
        while (ts.isLoaded() && activeManager[other].hasRecords() && inputAccessor[main].exists()) {
            //            System.err.println("Spilling stream left: ");
            //            TuplePrinterUtil.printTuple("    left: ", inputAccessor[LEFT_PARTITION]);

            int tupleId = inputAccessor[main].getTupleId();
            if (!runFileStream[main].isReading()) {
                runFileStream[main].addToRunFile(inputAccessor[main]);
            }
            processTupleJoin(inputAccessor[main], tupleId, other, reversed, writer, empty);
            inputAccessor[main].next();
            ts = loadSideTuple(main);
        }

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Spill for " + count + " left tuples");
        }

        // Memory is empty and we can start processing the run file.
        if (activeManager[other].isEmpty() || ts.isEmpty()) {
            unfreezeAndContinue(main, inputAccessor[main]);
            ts = loadSideTuple(main);
        }
        return ts;
    }

    private boolean addToTupleProcessingGroup(int main, int other) {
        inputTuple[main].loadTuple();
        return inputTuple[main].startsBefore(memoryTuple[other]);
    }

    private void processTuple(int main, int other, IFrameWriter writer) throws HyracksDataException {
        // Check tuple with all memory.
        // Purge as processing
        // Added new items from right to memory and check
        if (!activeManager[main].hasRecords()) {
            return;
        }
        TuplePointer searchTp = activeManager[main].getFirst();
        TuplePointer searchEndTp = searchTp;
        
//        System.err.println("count: " + inputAccessor[main].getTupleCount());
//        System.err.println("size: " + inputAccessor[main].getTupleLength());

        searchEndTp = buildGroupFor(main, other, searchTp, searchEndTp);

        // Compare with tuple in memory
        if (processingGroup.size() == 1) {
            processSingleWithMemory(other, main, searchTp, writer);
        } else {
            processGroupWithMemory(other, main, searchTp, writer);
        }

        // Add tuples from the stream.
        if (!processGroupWithStream(other, main, searchEndTp, writer)) {
            return;
        }

        // Remove search tuple
        //        System.err.println("Remove after all matches found -- right tuple: " + searchTp);
        //        TuplePrinterUtil.printTuple("    left: ", memoryAccessor[RIGHT_PARTITION], searchTp.getTupleIndex());
        for (Iterator<TuplePointer> groupIterator = processingGroup.iterator(); groupIterator.hasNext();) {
            TuplePointer groupTp = groupIterator.next();
            activeManager[main].remove(groupTp);
        }
    }

    private TuplePointer buildGroupFor(int groupPartition, int otherPartition, TuplePointer searchTp,
            TuplePointer searchEndTp) throws HyracksDataException {
        TuplePointer tpRight = activeManager[otherPartition].getFirst();
        memoryTuple[otherPartition].setTuple(tpRight);

        memoryTuple[groupPartition].setTuple(searchTp);
        long searchGroupEnd = memoryTuple[groupPartition].getEnd();

        //        System.err.println("Stream left: ");
        //        TuplePrinterUtil.printTuple("    left: ", memoryAccessor[LEFT_PARTITION], searchTp.getTupleIndex());

        TupleStatus leftTs = loadSideTuple(groupPartition);
        processingGroup.clear();
        processingGroup.add(searchTp);
        while (leftTs.isLoaded() && addToTupleProcessingGroup(groupPartition, otherPartition)) {
            TuplePointer tp = activeManager[groupPartition].addTuple(inputAccessor[groupPartition]);
            if (tp == null) {
                break;
            }
            //            System.err.println("Added to left search group: " + tp);
            //            TuplePrinterUtil.printTuple("    search: ", memoryAccessor[LEFT_PARTITION], tp.getTupleIndex());
            processingGroup.add(tp);
            memoryTuple[groupPartition].setTuple(tp);
            if (searchGroupEnd > memoryTuple[groupPartition].getEnd()) {
                searchGroupEnd = memoryTuple[groupPartition].getEnd();
                searchEndTp = tp;
            }

            inputAccessor[groupPartition].next();
            leftTs = loadSideTuple(groupPartition);
        }
        return searchEndTp;
    }

    private boolean processGroupWithStream(int outer, int inner, TuplePointer searchEndTp, IFrameWriter writer)
            throws HyracksDataException {
        memoryTuple[inner].setTuple(searchEndTp);
        // Add tuples from the stream.
        while (loadSideTuple(outer).isLoaded() && imjc.checkToSaveInMemory(memoryTuple[inner].getAccessor(),
                memoryTuple[inner].getTupleIndex(), inputAccessor[outer], inputAccessor[outer].getTupleId())) {
            TuplePointer tp = activeManager[outer].addTuple(inputAccessor[outer]);
            if (tp != null) {
                memoryTuple[outer].setTuple(tp);
                //                System.err.println("Stream add: " + tp);
                //                TuplePrinterUtil.printTuple("    right: ", memoryAccessor[RIGHT_PARTITION], tp.getTupleIndex());

                // Search group.
                for (Iterator<TuplePointer> groupIterator = processingGroup.iterator(); groupIterator.hasNext();) {
                    TuplePointer groupTp = groupIterator.next();
                    memoryTuple[inner].setTuple(groupTp);

                    // Add to result if matched.
                    if (memoryTuple[LEFT_PARTITION].compareJoin(memoryTuple[RIGHT_PARTITION])) {
                        //                      memoryAccessor[LEFT_PARTITION].reset(groupTp);
                        addToResult(memoryTuple[LEFT_PARTITION].getAccessor(),
                                memoryTuple[LEFT_PARTITION].getTupleIndex(), memoryTuple[RIGHT_PARTITION].getAccessor(),
                                memoryTuple[RIGHT_PARTITION].getTupleIndex(), false, writer);
                        //                    System.err.println("Stream Match: " + searchTp + " " + tp);
                        //                    TuplePrinterUtil.printTuple("    left: ", memoryAccessor[LEFT_PARTITION], searchTp.getTupleIndex());
                        //                    TuplePrinterUtil.printTuple("    right: ", memoryAccessor[RIGHT_PARTITION], tp.getTupleIndex());
                    }
                    joinComparisonCount++;
                }
            } else {
                // Spill case, remove search tuple before freeze.
                freezeAndStartSpill(writer, processingGroup);
                //                System.err.println("Remove search before spill -- left tuple: " + searchTp);
                //                TuplePrinterUtil.printTuple("    left: ", memoryAccessor[LEFT_PARTITION], searchTp.getTupleIndex());
                //                activeManager[LEFT_PARTITION].remove(searchTp);
                //                freezeAndClearMemory(writer);
                return false;
            }
            inputAccessor[outer].next();
            memoryTuple[inner].setTuple(searchEndTp);
        }
        return true;
    }

    private void processGroupWithMemory(int outer, int inner, TuplePointer searchTp, IFrameWriter writer)
            throws HyracksDataException {
        // Compare with tuple in memory
        for (Iterator<TuplePointer> iterator = activeManager[outer].getIterator(); iterator.hasNext();) {
            TuplePointer matchTp = iterator.next();
            memoryTuple[outer].setTuple(matchTp);

            // Search group.
            for (Iterator<TuplePointer> groupIterator = processingGroup.iterator(); groupIterator.hasNext();) {
                TuplePointer groupTp = groupIterator.next();
                memoryTuple[inner].setTuple(groupTp);

                // Add to result if matched.
                //if (imjc.checkToSaveInResult(startGroup, endGroup, startOuter, endOuter, false)) {
                if (memoryTuple[LEFT_PARTITION].compareJoin(memoryTuple[RIGHT_PARTITION])) {
                    addToResult(memoryTuple[LEFT_PARTITION].getAccessor(), memoryTuple[LEFT_PARTITION].getTupleIndex(),
                            memoryTuple[RIGHT_PARTITION].getAccessor(), memoryTuple[RIGHT_PARTITION].getTupleIndex(),
                            false, writer);
                    //                System.err.println("Memory Match: " + searchTp + " " + matchTp);
                    //                TuplePrinterUtil.printTuple("    left: ", memoryAccessor[LEFT_PARTITION], searchTp.getTupleIndex());
                    //                TuplePrinterUtil.printTuple("    right: ", memoryAccessor[RIGHT_PARTITION], matchTp.getTupleIndex());
                }
                joinComparisonCount++;
            }

            // Remove if the tuple no long matches.
            memoryTuple[inner].setTuple(searchTp);
            if (memoryTuple[inner].removeFromMemory(memoryTuple[outer])) {
                //                System.err.println("Remove right tuple: " + matchTp);
                //                TuplePrinterUtil.printTuple("    right: ", memoryAccessor[RIGHT_PARTITION], matchTp.getTupleIndex());
                activeManager[outer].remove(iterator, matchTp);
            }
        }
    }

    private void processSingleWithMemory(int outer, int inner, TuplePointer searchTp, IFrameWriter writer)
            throws HyracksDataException {
        memoryTuple[inner].setTuple(searchTp);

        // Compare with tuple in memory
        for (Iterator<TuplePointer> iterator = activeManager[outer].getIterator(); iterator.hasNext();) {
            TuplePointer matchTp = iterator.next();
            memoryTuple[outer].setTuple(matchTp);

            // Add to result if matched.
            //if (imjc.checkToSaveInResult(startGroup, endGroup, startOuter, endOuter, false)) {
            if (memoryTuple[LEFT_PARTITION].compareJoin(memoryTuple[RIGHT_PARTITION])) {
                addToResult(memoryTuple[LEFT_PARTITION].getAccessor(), memoryTuple[LEFT_PARTITION].getTupleIndex(),
                        memoryTuple[RIGHT_PARTITION].getAccessor(), memoryTuple[RIGHT_PARTITION].getTupleIndex(), false,
                        writer);
                //                System.err.println("Memory Match: " + searchTp + " " + matchTp);
                //                TuplePrinterUtil.printTuple("    left: ", memoryAccessor[LEFT_PARTITION], searchTp.getTupleIndex());
                //                TuplePrinterUtil.printTuple("    right: ", memoryAccessor[RIGHT_PARTITION], matchTp.getTupleIndex());
            }
            joinComparisonCount++;

            // Remove if the tuple no long matches.
            if (memoryTuple[inner].removeFromMemory(memoryTuple[outer])) {
                //                System.err.println("Remove right tuple: " + matchTp);
                //                TuplePrinterUtil.printTuple("    right: ", memoryAccessor[RIGHT_PARTITION], matchTp.getTupleIndex());
                activeManager[outer].remove(iterator, matchTp);
            }
        }
    }

    private void processInMemoryJoin(int outer, int inner, boolean reversed, IFrameWriter writer,
            LinkedList<TuplePointer> searchGroup) throws HyracksDataException {
        // Compare with tuple in memory
        for (Iterator<TuplePointer> outerIterator = activeManager[outer].getIterator(); outerIterator.hasNext();) {
            TuplePointer outerTp = outerIterator.next();
            if (searchGroup.contains(outerTp)) {
                continue;
            }
            memoryAccessor[outer].reset(outerTp);
            processTupleJoin(memoryAccessor[outer], outerTp.getTupleIndex(), inner, reversed, writer, searchGroup);
        }
    }

    private void processTupleJoin(ITupleAccessor accessor, int tupleId, int inner, boolean reversed,
            IFrameWriter writer, LinkedList<TuplePointer> searchGroup) throws HyracksDataException {
        // Compare with tuple in memory
        for (Iterator<TuplePointer> innerIterator = activeManager[inner].getIterator(); innerIterator.hasNext();) {
            TuplePointer innerTp = innerIterator.next();
            if (searchGroup.contains(innerTp)) {
                continue;
            }
            memoryAccessor[inner].reset(innerTp);

            //            TuplePrinterUtil.printTuple("Outer", accessor, tupleId);
            //            TuplePrinterUtil.printTuple("Inner", memoryAccessor[inner], innerTp.getTupleIndex());
            // Add to result if matched.
            if (imjc.checkToSaveInResult(accessor, tupleId, memoryAccessor[inner], innerTp.getTupleIndex(), reversed)) {
                addToResult(accessor, tupleId, memoryAccessor[inner], innerTp.getTupleIndex(), reversed, writer);
                //                System.err.println("Memory Match: " + tupleId + " " + innerTp);
                //                TuplePrinterUtil.printTuple("    ...: ", accessor, tupleId);
                //                TuplePrinterUtil.printTuple("    ...: ", memoryAccessor[inner], innerTp.getTupleIndex());
            }
            joinComparisonCount++;
            // Remove if the tuple no long matches.
            if (imjc.checkToRemoveInMemory(accessor, tupleId, memoryAccessor[inner], innerTp.getTupleIndex())) {
                //              System.err.println("Remove right tuple: " + innerTp);
                //              TuplePrinterUtil.printTuple("    ...: ", memoryAccessor[inner], innerTp.getTupleIndex());
                activeManager[inner].remove(innerIterator, innerTp);
            }
            // Exit if no more possible matches
            if (!imjc.checkIfMoreMatches(accessor, tupleId, memoryAccessor[inner], innerTp.getTupleIndex())) {
                break;
            }
        }
    }

    private void freezeAndClearMemory(IFrameWriter writer, LinkedList<TuplePointer> searchGroup)
            throws HyracksDataException {
        //        if (LOGGER.isLoggable(Level.FINEST)) {
        //            LOGGER.finest("freeze snapshot: " + frameCounts[RIGHT_PARTITION] + " right, " + frameCounts[LEFT_PARTITION]
        //                    + " left, left[" + bufferManager.getNumTuples(LEFT_PARTITION) + " memory]. right["
        //                    + bufferManager.getNumTuples(RIGHT_PARTITION) + " memory].");
        //        }
        //        LOGGER.warning("disk IO: right, " + runFileStream[RIGHT_PARTITION].getReadCount() + " left, "
        //                + runFileStream[LEFT_PARTITION].getReadCount());
        //        System.out.println("freeze snapshot: " + frameCounts[RIGHT_PARTITION] + " right, " + frameCounts[LEFT_PARTITION]
        //                + " left, left[" + bufferManager.getNumTuples(LEFT_PARTITION) + " memory]. right["
        //                + bufferManager.getNumTuples(RIGHT_PARTITION) + " memory].");
        //        System.out.println("disk IO: right, " + runFileStream[RIGHT_PARTITION].getReadCount() + " left, "
        //                + runFileStream[LEFT_PARTITION].getReadCount());
        if (bufferManager.getNumTuples(LEFT_PARTITION) > bufferManager.getNumTuples(RIGHT_PARTITION)) {
            processInMemoryJoin(RIGHT_PARTITION, LEFT_PARTITION, true, writer, searchGroup);
            rightSpillCount++;
        } else {
            processInMemoryJoin(LEFT_PARTITION, RIGHT_PARTITION, false, writer, searchGroup);
            leftSpillCount++;
        }
    }

    private void freezeAndStartSpill(IFrameWriter writer, LinkedList<TuplePointer> searchGroup)
            throws HyracksDataException {
        int freezePartition;
        if (bufferManager.getNumTuples(LEFT_PARTITION) > bufferManager.getNumTuples(RIGHT_PARTITION)) {
            freezePartition = RIGHT_PARTITION;
        } else {
            freezePartition = LEFT_PARTITION;
        }
//        System.err.println("freeze snapshot(" + freezePartition + "): " + frameCounts[RIGHT_PARTITION] + " right, "
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

        freezeAndClearMemory(writer, searchGroup);
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
        //        System.out.println("Unfreeze -- " + (LEFT_PARTITION == frozenPartition ? "Left" : "Right"));

        // Finish writing
        runFileStream[frozenPartition].flushRunFile();

        // Clear memory
        //        System.out.println("after freeze memory left[" + bufferManager.getNumTuples(LEFT_PARTITION) + " memory]. right["
        //                + bufferManager.getNumTuples(RIGHT_PARTITION) + " memory].");
        flushMemory(flushPartition);
        //        System.out.println("after clear memory left[" + bufferManager.getNumTuples(LEFT_PARTITION) + " memory]. right["
        //                + bufferManager.getNumTuples(RIGHT_PARTITION) + " memory].");
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
