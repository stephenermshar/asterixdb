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
package org.apache.asterix.runtime.operators.joins.disjointintervalpartition;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * This class partitions interval according to Disjoint Interval Partitioning method.
 * All partitions are flushed to disk.
 */
public class DisjointIntervalPartitionAndSpill {

    private IHyracksTaskContext ctx;

    private final String runFilePrefix;

    private final DisjointIntervalPartitionComputer dipc;

    private RunFileWriter[] runFileWriters; //writing spilled build partitions

    private final BitSet spilledStatus; //0=resident, 1=spilled
    private final int numOfPartitions;
    private final int memoryForPartitioning;

    private IPartitionedTupleBufferManager bufferManager;
    private PreferToSpillFullyOccupiedFramePolicy spillPolicy;

    private final FrameTupleAccessor accessor;

    // stats information
    private final long[] partitionSizeInTups;
    private long spillSizeInTups = 0;
    private long spillWriteCount = 0;

    // this is a reusable object to store the pointer, which is not used anywhere.
    // we mainly use it to match the corresponding function signature.
    private TuplePointer tempPtr = new TuplePointer();

    private final FrameTupleAppender spillAppender;
    private RunFileWriter spillWriter;

    public DisjointIntervalPartitionAndSpill(IHyracksTaskContext ctx, int memory, int numOfPartitions,
            String runFilePrefix, RecordDescriptor rd, DisjointIntervalPartitionComputer dipc)
            throws HyracksDataException {
        this.ctx = ctx;
        this.memoryForPartitioning = memory - 1;
        this.dipc = dipc;
        this.runFilePrefix = runFilePrefix;
        this.numOfPartitions = numOfPartitions;

        accessor = new FrameTupleAccessor(rd);
        runFileWriters = new RunFileWriter[numOfPartitions];
        spilledStatus = new BitSet(numOfPartitions);

        spillAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        partitionSizeInTups = new long[numOfPartitions];
    }

    public void init() throws HyracksDataException {
        bufferManager = new VPartitionTupleBufferManager(ctx,
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus),
                numOfPartitions, memoryForPartitioning * ctx.getInitialFrameSize());
        spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(bufferManager, spilledStatus,
                ctx.getInitialFrameSize());
        spilledStatus.clear();

    }

    public void reset(RunFileWriter writer) throws HyracksDataException {
        bufferManager.reset();
        spilledStatus.clear();
        Arrays.fill(partitionSizeInTups, 0);
        Arrays.fill(runFileWriters, null);
        spillWriter = writer;
        dipc.reset();
        spillSizeInTups = 0;
        spillWriteCount = 0;
    }

    public void processFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();

        for (int i = 0; i < tupleCount; ++i) {
            int pid = dipc.partition(accessor, i, numOfPartitions);
            processTuple(accessor, i, pid);
        }
    }

    public void processTupleAccessor(ITupleAccessor accessor) throws HyracksDataException {
        int pid = dipc.partition(accessor, accessor.getTupleId(), numOfPartitions);
        processTuple(accessor, accessor.getTupleId(), pid);
    }

    private void processTuple(IFrameTupleAccessor fta, int tid, int pid) throws HyracksDataException {
        //        TuplePrinterUtil.printTuple(runFilePrefix + " Partition: " + pid, fta, tid);
        if (pid < 0) {
            addToSpillPartition(fta, tid);
        } else {
            while (!bufferManager.insertTuple(pid, fta, tid, tempPtr)) {
                selectAndSpillVictim(pid);
            }
            partitionSizeInTups[pid]++;
        }
    }

    private void addToSpillPartition(IFrameTupleAccessor fta, int tupleId) throws HyracksDataException {
        if (!spillAppender.append(fta, tupleId)) {
            spillAppender.flush(spillWriter);
            if (!spillAppender.append(fta, tupleId)) {
                throw new HyracksDataException("Can not append tupe to spill file.");
            }
            spillWriteCount++;
        }
        spillSizeInTups++;
    }

    private void selectAndSpillVictim(int pid) throws HyracksDataException {
        int victimPartition = spillPolicy.selectVictimPartition(pid);
        if (victimPartition < 0) {
            throw new HyracksDataException(
                    "No more space left in the memory buffer, please give join more memory budgets.");
        }
        spillPartition(victimPartition);
    }

    private void spillPartition(int pid) throws HyracksDataException {
        RunFileWriter writer = getSpillWriterOrCreateNewOneIfNotExist(pid);
        spillWriteCount += bufferManager.getNumFrames(pid);
        bufferManager.flushPartition(pid, writer);
        bufferManager.clearPartition(pid);
        spilledStatus.set(pid);
    }

    private RunFileWriter getSpillWriterOrCreateNewOneIfNotExist(int pid) throws HyracksDataException {
        RunFileWriter writer = runFileWriters[pid];
        if (writer == null) {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(runFilePrefix);
            writer = new RunFileWriter(file, ctx.getIOManager());
            writer.open();
            runFileWriters[pid] = writer;
        }
        return writer;
    }

    /**
     * In case of failure happens, we need to clear up the generated temporary files.
     */
    public void clearTempFiles() {
        for (int i = 0; i < runFileWriters.length; i++) {
            if (runFileWriters[i] != null) {
                runFileWriters[i].getFileReference().delete();
            }
        }
    }

    public void spillAllPartitions() throws HyracksDataException {
        for (int pid = 0; pid < numOfPartitions; pid++) {
            if (bufferManager.getNumTuples(pid) > 0) {
                spillPartition(pid);
            } else {
                break;
            }
        }
        // Flush the spill partition.
        if (spillAppender.getTupleCount() > 0) {
            spillAppender.flush(spillWriter);
            spillWriteCount++;
        }
    }

    public RunFileReader getRFReader(int pid) throws HyracksDataException {
        return (runFileWriters[pid] == null) ? null : (runFileWriters[pid]).createReader();
    }

    public long getPartitionSizeInTup(int pid) {
        return partitionSizeInTups[pid];
    }

    public RunFileReader getSpillRFReader() throws HyracksDataException {
        return (spillWriter == null) ? null : spillWriter.createDeleteOnCloseReader();
    }

    public long getSpillSizeInTup() {
        return spillSizeInTups;
    }

    public long getSpillWriteCount() {
        return spillWriteCount;
    }

}
