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
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.union.UnionAllOperatorDescriptor;

public class IntervalIndexJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int LEFT_ACTIVITY_ID = 0;
    private static final int RIGHT_ACTIVITY_ID = 1;
    private static final int JOIN_ACTIVITY_ID = 2;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final int memoryForJoin;
    private final IMergeJoinCheckerFactory mjcf;

    private static final Logger LOGGER = Logger.getLogger(IntervalIndexJoinOperatorDescriptor.class.getName());

    public IntervalIndexJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memoryForJoin, int[] leftKeys,
            int[] rightKeys, RecordDescriptor recordDescriptor, IMergeJoinCheckerFactory mjcf) {
        super(spec, 2, 1);
        outRecDescs[0] = recordDescriptor;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.memoryForJoin = memoryForJoin;
        this.mjcf = mjcf;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {

        int joinInputs = 2;

        JoinerActivityNode joinerNode = new JoinerActivityNode(new ActivityId(getOperatorId(), 0), joinInputs);
        builder.addActivity(this, joinerNode);
        for (int i = 0; i < joinInputs; i++) {
            builder.addSourceEdge(i, joinerNode, i);
        }
        builder.addTargetEdge(0, joinerNode, 0);
    }

    private class JoinerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private final int numJoinInputs;

        public JoinerActivityNode(ActivityId id, int numJoinInputs) {
            super(id);
            this.numJoinInputs = numJoinInputs;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(id, 0);
            return new JoinerOperator(ctx, partition, numJoinInputs, inRecordDesc);
        }

        private class JoinerOperator extends AbstractUnaryOutputOperatorNodePushable {

            private final IHyracksTaskContext ctx;
            private final int partition;
            private final int numJoinInputs;
            private final RecordDescriptor recordDescriptor;
            private ProducerConsumerFrameState state;

            public JoinerOperator(IHyracksTaskContext ctx, int partition, int numJoinInputs,
                    RecordDescriptor inRecordDesc) {

                this.ctx = ctx;
                this.partition = partition;
                this.numJoinInputs = numJoinInputs;
                this.recordDescriptor = inRecordDesc;
            }

            @Override
            public int getInputArity() { return numJoinInputs; }

            @Override
            public void initialize() throws HyracksDataException {
                ProducerConsumerFrameState leftState = getPCFrameState(0);
                ProducerConsumerFrameState rightState = getPCFrameState(1);

                try {
                    writer.open();
                    // (stephen) is IStreamJoiner in interval_join_symmetric, but I'm making it IMergeJoiner for now so
                    //           I can refactor everything together.
                    IStreamJoiner joiner = new IntervalIndexJoiner(ctx, memoryForJoin, partition, mjcf,
                            leftKeys, rightKeys, leftState, rightState);
                    joiner.processJoin(writer);
                } catch (Exception ex) {
                    writer.fail();
                    throw new HyracksDataException(ex.getMessage());
                } finally {
                    writer.close();
                }
            }

            private ProducerConsumerFrameState getPCFrameState(int dataId) {
                int sleep = 0;
                ProducerConsumerFrameState state;
                do {
                    try {
                        Thread.sleep((int) Math.pow(sleep++, 2));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    state = (ProducerConsumerFrameState) ctx.getStateObject(new TaskId(0, partition));
                } while (state == null);
                return state;
            }

            @Override
            public IFrameWriter getInputFrameWriter(int index) {
                    return new IFrameWriter() {
                        @Override
                        public void open() throws HyracksDataException {
                            state = new ProducerConsumerFrameState(ctx.getJobletContext().getJobId(),
                                    new TaskId(getActivityId(), partition), recordDescriptor);
                            ctx.setStateObject(state);
                        }

                        @Override
                        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                            state.putFrame(buffer);
                        }

                        @Override
                        public void fail() throws HyracksDataException {
                            state.noMoreFrames();
                        }

                        @Override
                        public void close() throws HyracksDataException {
                            state.noMoreFrames();
                        }
                    };
                }

        }
    }
}
