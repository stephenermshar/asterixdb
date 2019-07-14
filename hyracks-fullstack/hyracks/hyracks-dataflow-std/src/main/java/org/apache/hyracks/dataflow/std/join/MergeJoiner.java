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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class MergeJoiner extends AbstractTupleStreamJoiner {
    IFrameWriter writer;

    public MergeJoiner(IHyracksTaskContext ctx, IConsumerFrame leftCF, IConsumerFrame rightCF, IFrameWriter writer)
            throws HyracksDataException {
        super(ctx, leftCF, rightCF);
        this.writer = writer;
    }

    @Override
    public void processJoin() throws HyracksDataException {
        // (stephen) blindly joins tuples until one side runs out.

        if (!getNextFrame(LEFT_PARTITION)) {
            return;
        }
        if (!getNextFrame(RIGHT_PARTITION)) {
            return;
        }
        if (inputAccessor[RIGHT_PARTITION].getTupleCount() == 0) {
            return;
        }

        for (int i = 0; i < inputAccessor[LEFT_PARTITION].getTupleCount(); i++) {
            addToResult(inputAccessor[LEFT_PARTITION], i, inputAccessor[RIGHT_PARTITION], 0, false, writer);
        }
        closeJoin(writer);
    }

}
