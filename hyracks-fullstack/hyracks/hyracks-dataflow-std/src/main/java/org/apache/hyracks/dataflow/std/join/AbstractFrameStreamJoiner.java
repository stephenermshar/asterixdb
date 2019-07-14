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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;

public abstract class AbstractFrameStreamJoiner implements IStreamJoiner {

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int LEFT_PARTITION = 0;
    protected static final int RIGHT_PARTITION = 1;

    private final MergeBranchStatus[] branchStatus;
    private final IConsumerFrame[] consumerFrames;
    private final FrameTupleAppender resultAppender;

    protected final IFrame[] inputBuffer;
    protected final ITupleAccessor[] inputAccessor;

    protected long[] frameCounts = { 0, 0 };
    protected long[] tupleCounts = { 0, 0 };

    public AbstractFrameStreamJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF)
            throws HyracksDataException {

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

    protected void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2,
            boolean reversed, IFrameWriter writer) throws HyracksDataException {
        if (reversed) {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor2, index2, accessor1, index1);
        } else {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
        }
    }

    protected void closeJoin(IFrameWriter writer) throws HyracksDataException {
        resultAppender.write(writer, true);
    }

}
