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

import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.junit.Test;

public class RangeMapTest extends AbstractRangeMapTest {

    /*
     * Test a single field range map of five integers.
     */
    @Test
    public void testRangeMapNumeric() throws Exception {
        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();

        // Write five integers with tags.
        LinkedList<Byte> tags = new LinkedList<>();
        LinkedList<Long> values = new LinkedList<>();
        tags.add(Byte.valueOf((byte) 20));
        values.add(Long.valueOf(100));
        tags.add(Byte.valueOf((byte) 30));
        values.add(Long.valueOf(1000));
        tags.add(Byte.valueOf((byte) 40));
        values.add(Long.valueOf(10000));
        tags.add(Byte.valueOf((byte) 50));
        values.add(Long.valueOf(100000));
        tags.add(Byte.valueOf((byte) 70));
        values.add(Long.valueOf(10000000));

        int[] offsets = new int[tags.size()];

        createMapNumeric(tags, values, abvs, offsets);

        RangeMap rm = new RangeMap(1, abvs.getByteArray(), offsets);

        testMapNumeric(tags, values, rm);
    }

    private void createMapNumeric(LinkedList<Byte> tags, LinkedList<Long> values, ArrayBackedValueStorage abvs, int[] offsets)
            throws IOException {
        DataOutput dout = abvs.getDataOutput();
        int i = 0;
        for (Byte key : tags) {
            dout.writeByte(key);
            dout.writeLong(values.get(i));
            offsets[i++] = abvs.getLength();
        }
    }

}
