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

package org.apache.asterix.runtime.operators.joins.overlappingintervalpartition;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.intervalindex.IConsumerFrame;
import org.apache.asterix.runtime.operators.joins.intervalindex.IStreamJoiner;
import org.apache.asterix.runtime.operators.joins.intervalindex.ProducerConsumerFrameState;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.RangeId;
import org.apache.hyracks.dataflow.std.misc.RangeForwardOperatorDescriptor.RangeForwardTaskState;

public class OverlappingIntervalPartitionJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int LEFT_ACTIVITY_ID = 0;
    private static final int RIGHT_ACTIVITY_ID = 1;
    private static final int JOIN_ACTIVITY_ID = 2;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final int memoryForJoin;
    private final IIntervalMergeJoinCheckerFactory imjcf;
    private final RangeId rangeId;
    private final int k;
    private final int probeKey;
    private final int buildKey;

    private static final Logger LOGGER = Logger
            .getLogger(OverlappingIntervalPartitionJoinOperatorDescriptor.class.getName());

    public OverlappingIntervalPartitionJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memoryForJoin,
            int k, int[] leftKeys, int[] rightKeys, RecordDescriptor recordDescriptor,
            IIntervalMergeJoinCheckerFactory imjcf, RangeId rangeId) {
        super(spec, 2, 1);
        recordDescriptors[0] = recordDescriptor;
        this.probeKey = leftKeys[0];
        this.buildKey = rightKeys[0];
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.memoryForJoin = memoryForJoin;
        this.imjcf = imjcf;
        this.k = k;
        this.rangeId = rangeId;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId leftAid = new ActivityId(odId, LEFT_ACTIVITY_ID);
        ActivityId rightAid = new ActivityId(odId, RIGHT_ACTIVITY_ID);
        ActivityId joinAid = new ActivityId(odId, JOIN_ACTIVITY_ID);
        ActivityId[] dataAids = { leftAid, rightAid };

        IActivity leftAN = new InputDataActivityNode(leftAid);
        IActivity rightAN = new InputDataActivityNode(rightAid);
        IActivity joinAN = new JoinerActivityNode(joinAid, dataAids);

        builder.addActivity(this, rightAN);
        builder.addSourceEdge(1, rightAN, 0);

        builder.addActivity(this, leftAN);
        builder.addSourceEdge(0, leftAN, 0);

        builder.addActivity(this, joinAN);
        builder.addTargetEdge(0, joinAN, 0);
    }

    private class JoinerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId[] dataIds;

        public JoinerActivityNode(ActivityId id, ActivityId[] dataIds) {
            super(id);
            this.dataIds = dataIds;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            return new JoinerOperator(ctx, partition, nPartitions, dataIds);
        }

        private class JoinerOperator extends AbstractUnaryOutputSourceOperatorNodePushable {

            private final IHyracksTaskContext ctx;
            private final int partition;
            private final int totalPartitions;
            private final ActivityId[] dataIds;

            public JoinerOperator(IHyracksTaskContext ctx, int partition, int totalPartitions, ActivityId[] dataIds)
                    throws HyracksDataException {
                this.ctx = ctx;
                this.dataIds = dataIds;
                this.partition = partition;
                this.totalPartitions = totalPartitions;
            }

            @Override
            public void initialize() throws HyracksDataException {
                int sleep = 0;
                ProducerConsumerFrameState leftState;
                do {
                    try {
                        Thread.sleep((int) Math.pow(sleep++, 2));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    leftState = (ProducerConsumerFrameState) ctx.getStateObject(new TaskId(dataIds[0], partition));
                } while (leftState == null);
                sleep = 0;
                ProducerConsumerFrameState rightState;
                do {
                    try {
                        Thread.sleep((int) Math.pow(sleep++, 2));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    rightState = (ProducerConsumerFrameState) ctx.getStateObject(new TaskId(dataIds[1], partition));
                } while (rightState == null);

                try {
                    writer.open();
                    RangeForwardTaskState rangeState = RangeForwardTaskState.getRangeState(rangeId.getId(), ctx);
                    long partitionStart = OverlappingIntervalPartitionUtil
                            .getPartitionStartValue(rangeState.getRangeMap(), partition, totalPartitions);
                    long partitionEnd = OverlappingIntervalPartitionUtil.getPartitionEndValue(rangeState.getRangeMap(),
                            partition, totalPartitions);
                    ITuplePartitionComputer buildHpc = new OverlappingIntervalPartitionComputerFactory(buildKey, k,
                            partitionStart, partitionEnd).createPartitioner();
                    ITuplePartitionComputer probeHpc = new OverlappingIntervalPartitionComputerFactory(probeKey, k,
                            partitionStart, partitionEnd).createPartitioner();
                    IIntervalMergeJoinChecker imjc = imjcf.createMergeJoinChecker(leftKeys, rightKeys, ctx);

                    IStreamJoiner joiner = new OverlappingIntervalPartitionJoiner(ctx, memoryForJoin, partition, k,
                            imjc, (IConsumerFrame) leftState, (IConsumerFrame) rightState, buildHpc, probeHpc);
                    joiner.processJoin(writer);
                } catch (Exception ex) {
                    writer.fail();
                    throw new HyracksDataException(ex);
                } finally {
                    writer.close();
                }
            }
        }
    }

    private class InputDataActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private int partition;

        public InputDataActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            this.partition = partition;
            RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(id, 0);
            return new InputDataOperator(ctx, inRecordDesc);
        }

        private class InputDataOperator extends AbstractUnaryInputSinkOperatorNodePushable {

            private IHyracksTaskContext ctx;
            private final RecordDescriptor recordDescriptor;
            private ProducerConsumerFrameState state;

            public InputDataOperator(IHyracksTaskContext ctx, RecordDescriptor inRecordDesc) {
                this.ctx = ctx;
                this.recordDescriptor = inRecordDesc;
            }

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
        }
    }
}