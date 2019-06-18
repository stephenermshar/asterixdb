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
package org.apache.asterix.runtime.operators.joins.intervalindex;

import static junit.framework.Assert.assertEquals;

import java.util.Comparator;

import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.junit.Test;

public class EndPointIndexItemTest {

    @Test
    public void testCreationTest() {
        TuplePointer tp = new TuplePointer();
        byte start = -1;
        long point = Long.MIN_VALUE;
        EndPointIndexItem epii = new EndPointIndexItem();
        assertEquals(tp, epii.getTuplePointer());
        assertEquals(start, epii.getStart());
        assertEquals(point, epii.getPoint());

        tp.reset(2, 12);
        start = 20;
        point = 200;
        epii.reset(tp, start, point);
        assertEquals(tp, epii.getTuplePointer());
        assertEquals(start, epii.getStart());
        assertEquals(point, epii.getPoint());

        tp.reset(3, 13);
        start = 30;
        point = 300;
        EndPointIndexItem epii3 = new EndPointIndexItem(tp, start, point);
        assertEquals(tp, epii3.getTuplePointer());
        assertEquals(start, epii3.getStart());
        assertEquals(point, epii3.getPoint());

        epii.reset(epii3);
        assertEquals(tp, epii.getTuplePointer());
        assertEquals(start, epii.getStart());
        assertEquals(point, epii.getPoint());
    }

    @Test
    public void testAscComparitor() {
        Comparator<EndPointIndexItem> asc = EndPointIndexItem.EndPointAscComparator;
        TuplePointer tp = new TuplePointer();
        byte start = 1;
        long point = 100;
        EndPointIndexItem epii1 = new EndPointIndexItem(tp, start, point);

        start = 20;
        point = 200;
        EndPointIndexItem epii2 = new EndPointIndexItem(tp, start, point);

        start = 30;
        point = 200;
        EndPointIndexItem epii2b = new EndPointIndexItem(tp, start, point);

        assertEquals(asc.compare(epii1, epii2), -100);
        assertEquals(asc.compare(epii2, epii1), 100);
        assertEquals(asc.compare(epii1, epii1), 0);
        assertEquals(asc.compare(epii2, epii2b), -10);
        assertEquals(asc.compare(epii2b, epii2), 10);
    }

    @Test
    public void testDescComparitor() {
        Comparator<EndPointIndexItem> desc = EndPointIndexItem.EndPointDescComparator;
        TuplePointer tp = new TuplePointer();
        byte start = 1;
        long point = 100;
        EndPointIndexItem epii1 = new EndPointIndexItem(tp, start, point);

        start = 20;
        point = 200;
        EndPointIndexItem epii2 = new EndPointIndexItem(tp, start, point);

        start = 30;
        point = 200;
        EndPointIndexItem epii2b = new EndPointIndexItem(tp, start, point);

        assertEquals(desc.compare(epii1, epii2), 100);
        assertEquals(desc.compare(epii2, epii1), -100);
        assertEquals(desc.compare(epii1, epii1), 0);
        assertEquals(desc.compare(epii2, epii2b), 10);
        assertEquals(desc.compare(epii2b, epii2), -10);
    }

}