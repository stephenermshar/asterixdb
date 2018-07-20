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
package org.apache.hyracks.dataflow.std.connectors;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.storage.IGrowableIntArray;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.RangeId;
import org.apache.hyracks.dataflow.std.misc.RangeForwardOperatorDescriptor.RangeForwardTaskState;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;

public class PartitionRangeDataWriter implements IFrameWriter {
    private final int consumerPartitionCount;
    private final IFrameWriter[] pWriters;
    private final FrameTupleAppender[] appenders;
    private final FrameTupleAccessor tupleAccessor;
    private final IHyracksTaskContext ctx;
    private boolean allocatedFrame = false;
    private final boolean[] isOpen;
    private final ITupleRangePartitionComputerFactory trpcf;
    private final RangeId rangeId;
    private final IGrowableIntArray map;
    private ITupleRangePartitionComputer tpc;

    public PartitionRangeDataWriter(IHyracksTaskContext ctx, int consumerPartitionCount,
            IPartitionWriterFactory pwFactory, RecordDescriptor recordDescriptor,
            ITupleRangePartitionComputerFactory trpcf, RangeId rangeId) throws HyracksDataException {
        this.consumerPartitionCount = consumerPartitionCount;
        pWriters = new IFrameWriter[consumerPartitionCount];
        isOpen = new boolean[consumerPartitionCount];
        appenders = new FrameTupleAppender[consumerPartitionCount];
        for (int i = 0; i < consumerPartitionCount; ++i) {
            try {
                pWriters[i] = pwFactory.createFrameWriter(i);
                appenders[i] = new FrameTupleAppender();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        this.ctx = ctx;
        this.trpcf = trpcf;
        this.rangeId = rangeId;
        this.map = new IntArrayList(8, 8);
    }

    protected FrameTupleAppender createTupleAppender(IHyracksTaskContext ctx) {
        return new FrameTupleAppender();
    }

    @Override
    public void close() throws HyracksDataException {
        HyracksDataException closeException = null;
        for (int i = 0; i < pWriters.length; ++i) {
            if (isOpen[i]) {
                if (allocatedFrame) {
                    try {
                        appenders[i].write(pWriters[i], true);
                    } catch (Throwable th) {
                        if (closeException == null) {
                            closeException = new HyracksDataException(th);
                        } else {
                            closeException.addSuppressed(th);
                        }
                    }
                }
                try {
                    pWriters[i].close();
                } catch (Throwable th) {
                    if (closeException == null) {
                        closeException = new HyracksDataException(th);
                    } else {
                        closeException.addSuppressed(th);
                    }
                }
            }
        }
        if (closeException != null) {
            throw closeException;
        }
    }

    @Override
    public void open() throws HyracksDataException {
        for (int i = 0; i < pWriters.length; ++i) {
            isOpen[i] = true;
            pWriters[i].open();
        }
        if (!allocatedFrame) {
            allocateFrames();
        }
        RangeForwardTaskState rangeState = RangeForwardTaskState.getRangeState(rangeId.getId(), ctx);
        tpc = trpcf.createPartitioner(rangeState.getRangeMap());
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tupleAccessor.reset(buffer);
        int tupleCount = tupleAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            tpc.partition(tupleAccessor, i, consumerPartitionCount, map);
            for (int h = 0; h < map.size(); ++h) {
                FrameUtils.appendToWriter(pWriters[map.get(h)], appenders[map.get(h)], tupleAccessor, i);
            }
            map.clear();
        }
    }

    protected void allocateFrames() throws HyracksDataException {
        for (int i = 0; i < appenders.length; ++i) {
            appenders[i].reset(new VSizeFrame(ctx), true);
        }
        allocatedFrame = true;
    }

    @Override
    public void fail() throws HyracksDataException {
        HyracksDataException failException = null;
        for (int i = 0; i < appenders.length; ++i) {
            if (isOpen[i]) {
                try {
                    pWriters[i].fail();
                } catch (Throwable th) {
                    if (failException == null) {
                        failException = new HyracksDataException(th);
                    } else {
                        failException.addSuppressed(th);
                    }
                }
            }
        }
        if (failException != null) {
            throw failException;
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        for (int i = 0; i < consumerPartitionCount; i++) {
            appenders[i].flush(pWriters[i]);
        }
    }

}
