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
package org.apache.hyracks.dataflow.std.join.intervalindex;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;

public class IntervalJoinUtil {

    private IntervalJoinUtil() {
    }

    public static void getIntervalPointable(ITupleAccessor accessor, int fieldId, TaggedValuePointable tvp,
            AIntervalPointable ip) {
        int start = accessor.getTupleStartOffset() + accessor.getFieldSlotsLength()
                + accessor.getFieldStartOffset(fieldId);
        int length = accessor.getFieldLength(fieldId);
        tvp.set(accessor.getBuffer().array(), start, length);
        tvp.getValue(ip);
    }

    public static void getIntervalPointable(IFrameTupleAccessor accessor, int tupleId, int fieldId,
            TaggedValuePointable tvp, AIntervalPointable ip) {
        int start = getIntervalOffset(accessor, tupleId, fieldId);
        int length = accessor.getFieldLength(tupleId, fieldId) - 1;
        ip.set(accessor.getBuffer().array(), start, length);
    }

    public static int getIntervalOffset(IFrameTupleAccessor accessor, int tupleId, int fieldId) {
        return getIntervalOffsetWithTag(accessor, tupleId, fieldId) + 1;
    }

    public static int getIntervalOffsetWithTag(IFrameTupleAccessor accessor, int tupleId, int fieldId) {
        int start = accessor.getTupleStartOffset(tupleId) + accessor.getFieldSlotsLength()
                + accessor.getFieldStartOffset(tupleId, fieldId);
        return start;
    }

    public static long getIntervalStart(IFrameTupleAccessor accessor, int tupleId, int fieldId) {
        int start = getIntervalOffset(accessor, tupleId, fieldId);
        long intervalStart = AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), start);
        return intervalStart;
    }

    public static long getIntervalEnd(IFrameTupleAccessor accessor, int tupleId, int fieldId) {
        int start = getIntervalOffset(accessor, tupleId, fieldId);
        long intervalEnd = AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), start);
        return intervalEnd;
    }

    public static long getIntervalStart(ITupleAccessor accessor, int fieldId) {
        int start = accessor.getTupleStartOffset() + accessor.getFieldSlotsLength()
                + accessor.getFieldStartOffset(fieldId) + 1;
        long intervalStart = AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), start);
        return intervalStart;
    }

    public static long getIntervalEnd(ITupleAccessor accessor, int fieldId) {
        int start = accessor.getTupleStartOffset() + accessor.getFieldSlotsLength()
                + accessor.getFieldStartOffset(fieldId) + 1;
        long intervalEnd = AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), start);
        return intervalEnd;
    }

}