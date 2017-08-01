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
package org.apache.asterix.lang.aql.util;

import java.util.LinkedList;

import org.apache.hyracks.api.dataflow.value.IRangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.AbstractRangeMapTest;
import org.junit.Test;

public class RangeMapBuilderTest extends AbstractRangeMapTest {

    /*
     * Test a single field range map of five integers.
     */
    @Test
    public void testRangeMapNumeric() throws Exception {
        String hint = "[100,1000,10000,100000,10000000]";

        // Write five integers with tags.
        LinkedList<Byte> tags = new LinkedList<>();
        LinkedList<Long> values = new LinkedList<>();
        tags.add(Byte.valueOf((byte) 4));
        values.add(Long.valueOf(100));
        tags.add(Byte.valueOf((byte) 4));
        values.add(Long.valueOf(1000));
        tags.add(Byte.valueOf((byte) 4));
        values.add(Long.valueOf(10000));
        tags.add(Byte.valueOf((byte) 4));
        values.add(Long.valueOf(100000));
        tags.add(Byte.valueOf((byte) 4));
        values.add(Long.valueOf(10000000));

        IRangeMap rm = RangeMapBuilder.parseHint(hint);

        testMapNumeric(tags, values, rm);
    }

}
