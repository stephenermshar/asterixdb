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
package org.apache.hyracks.dataflow.common.data.partition.range;

import static org.junit.Assert.assertTrue;

import java.util.LinkedList;

import org.apache.hyracks.api.dataflow.value.IRangeMap;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.LongPointable;

public abstract class AbstractRangeMapTest {

    protected void testMapNumeric(LinkedList<Byte> tags, LinkedList<Long> values, IRangeMap rangeMap) throws HyracksDataException {
        LongPointable lp = (LongPointable) LongPointable.FACTORY.createPointable();
        int columnIndex = 0;

        // Metadata
        assertTrue("Test keys and values size matches", tags.size() == values.size());
        assertTrue("Split points match", tags.size() - 2 == rangeMap.getSplitCount());

        int i = 0;
        int splitIndex = 0;
        for (Byte key : tags) {
            if (i == 0) {
                // Min
                assertTrue("Min tag matches", (byte) key == rangeMap.getMinTag(columnIndex));
                lp.set(rangeMap.getMinByteArray(columnIndex), rangeMap.getMinStartOffset(columnIndex) + 1,
                        rangeMap.getMinLength(columnIndex) - 1);
                assertTrue("Min value matches", values.get(i) == lp.getLong());
            } else if (i == tags.size()) {
                // Max
                assertTrue("Max tag matches", (byte) key == rangeMap.getMaxTag(columnIndex));
                lp.set(rangeMap.getMaxByteArray(columnIndex), rangeMap.getMaxStartOffset(columnIndex) + 1,
                        rangeMap.getMaxLength(columnIndex) - 1);
                assertTrue("Max value matches", values.get(i) == lp.getLong());
            } else {
                // Splits
                assertTrue("Tag matches for split: " + splitIndex,
                        (byte) key == rangeMap.getTag(columnIndex, splitIndex));
                lp.set(rangeMap.getByteArray(columnIndex, splitIndex),
                        rangeMap.getStartOffset(columnIndex, splitIndex) + 1,
                        rangeMap.getLength(columnIndex, splitIndex) - 1);
                assertTrue("Value matches for split: " + splitIndex, values.get(i) == lp.getLong());
                splitIndex++;
            }
            ++i;
        }
    }

}
