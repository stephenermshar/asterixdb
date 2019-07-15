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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.TuplePairEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.join.IMergeJoinCheckerFactory;
import org.apache.hyracks.dataflow.std.join.MergeJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.NaturalMergeJoinCheckerFactory;

public class MergeJoinPOperator extends AbstractJoinPOperator {

    private final int memSizeInFrames;
    protected final List<LogicalVariable> keysLeftBranch;
    protected final List<LogicalVariable> keysRightBranch;

    private static final Logger LOGGER = Logger.getLogger(MergeJoinPOperator.class.getName());

    public MergeJoinPOperator(JoinKind kind, List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight,
            int memSizeInFrames) {
        // (stephen) Merge Join will never be broadcast (?)
        super(kind, JoinPartitioningType.PAIRWISE);
        this.memSizeInFrames = memSizeInFrames;
        this.keysLeftBranch = sideLeft;
        this.keysRightBranch = sideRight;

        LOGGER.fine("MergeJoinPOperator constructed with: JoinKind=" + kind + ", JoinPartitioningType="
                + partitioningType + ", List<LogicalVariable>=" + keysLeftBranch + ", List<LogicalVariable>="
                + keysRightBranch + ", int memSizeInFrames=" + memSizeInFrames + ".");
    }

    public List<LogicalVariable> getKeysLeftBranch() {
        return keysLeftBranch;
    }

    public List<LogicalVariable> getKeysRightBranch() {
        return keysRightBranch;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.MERGE_JOIN;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator iop, IOptimizationContext context) {
        ArrayList<OrderColumn> order = new ArrayList<>();
        for (LogicalVariable v : keysLeftBranch) {
            order.add(new OrderColumn(v, OrderKind.ASC));
        }

        // (stephen) this is a guess, the AbstractHashJoinPOperator appeared to use one side of the join in deciding
        //           how to pass on the partitioning property. they used an existing property vector though. this might
        //           be better if it reuses ppLeft from getRequiredPropertiesForChildren() somehow.
        //
        IPartitioningProperty pp =
                new UnorderedPartitionedProperty(new ListSet<>(keysLeftBranch), context.getComputationNodeDomain());
        List<ILocalStructuralProperty> propsLocal = new ArrayList<>();
        propsLocal.add(new LocalOrderProperty(order));
        deliveredProperties = new StructuralPropertiesVector(pp, propsLocal);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator iop,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        // (stephen) MergeJoin needs locally ordered partitions that don't need to be ordered globally.

        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[2];
        AbstractLogicalOperator op = (AbstractLogicalOperator) iop;

        IPartitioningProperty ppLeft = null;
        List<ILocalStructuralProperty> ispLeft = new ArrayList<>();
        IPartitioningProperty ppRight = null;
        List<ILocalStructuralProperty> ispRight = new ArrayList<>();

        setRequiredLocalOrderProperty(ispLeft, keysLeftBranch);
        setRequiredLocalOrderProperty(ispRight, keysRightBranch);

        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            // (stephen) make unordered partitioned property
            // (stephen) Based on AbstractHashJoinPOperator getRequiredPropertiesForChildren()
            ppLeft = new UnorderedPartitionedProperty(new ListSet<>(keysLeftBranch),
                    context.getComputationNodeDomain());
            ppRight = new UnorderedPartitionedProperty(new ListSet<>(keysRightBranch),
                    context.getComputationNodeDomain());
        }

        pv[0] = new StructuralPropertiesVector(ppLeft, ispLeft);
        pv[1] = new StructuralPropertiesVector(ppRight, ispRight);
        IPartitioningRequirementsCoordinator prc =
                IPartitioningRequirementsCoordinator.EQCLASS_PARTITIONING_COORDINATOR;
        return new PhysicalRequirements(pv, prc);
    }

    private void setRequiredLocalOrderProperty(List<ILocalStructuralProperty> localStructuralProperties,
            List<LogicalVariable> keysBranch) {

        ArrayList<OrderColumn> order = new ArrayList<>();
        for (LogicalVariable v : keysBranch) {
            order.add(new OrderColumn(v, OrderKind.ASC));
        }
        // (stephen) LocalOrderProperty adds local sorting property
        localStructuralProperties.add(new LocalOrderProperty(order));
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {

        int[] keysLeft = JobGenHelper.variablesToFieldIndexes(keysLeftBranch, inputSchemas[0]);
        int[] keysRight = JobGenHelper.variablesToFieldIndexes(keysRightBranch, inputSchemas[1]);

        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        RecordDescriptor recordDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);

        // (stephen) from HybridHashJoinPOperator
        IOperatorSchema[] conditionInputSchemas = new IOperatorSchema[1];
        conditionInputSchemas[0] = opSchema;
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        IScalarEvaluatorFactory cond = expressionRuntimeProvider.createEvaluatorFactory(
                joinOp.getCondition().getValue(), context.getTypeEnvironment(op), conditionInputSchemas, context);

        ITuplePairComparatorFactory comparatorFactory =
                new TuplePairEvaluatorFactory(cond, false, context.getBinaryBooleanInspectorFactory());

        MergeJoinOperatorDescriptor opDesc =
                new MergeJoinOperatorDescriptor(spec, memSizeInFrames, keysLeft, keysRight, recordDescriptor, comparatorFactory);

        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src1 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src1, 0, op, 0);
        ILogicalOperator src2 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src2, 0, op, 1);
    }

}
