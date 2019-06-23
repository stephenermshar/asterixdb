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

import org.apache.hyracks.dataflow.std.structures.TuplePointer;

import java.io.Serializable;
import java.util.Comparator;

public class EndPointIndexItem implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final byte START_POINT = 0;
    public static final byte END_POINT = 1;

    public static final Comparator<EndPointIndexItem> EndPointAscComparator = new Comparator<EndPointIndexItem>() {
        @Override
        public int compare(EndPointIndexItem epi1, EndPointIndexItem epi2) {
            int c = Long.compare(epi1.getPoint(), epi2.getPoint());
            if (c == 0) {
                c = epi1.getStart() - epi2.getStart();
            }
            return c;
        }

    };

    public static final Comparator<EndPointIndexItem> EndPointDescComparator = new Comparator<EndPointIndexItem>() {
        @Override
        public int compare(EndPointIndexItem epi1, EndPointIndexItem epi2) {
            int c = Long.compare(epi2.getPoint(), epi1.getPoint());
            if (c == 0) {
                c = epi2.getStart() - epi1.getStart();
            }
            return c;
        }

    };

    private TuplePointer tp;
    private byte start;
    private long point;

    public EndPointIndexItem() {
        reset(new TuplePointer(), (byte) -1, Long.MIN_VALUE);
    }

    public EndPointIndexItem(TuplePointer tp, byte start, long point) {
        reset(tp, start, point);
    }

    public void reset(EndPointIndexItem item) {
        reset(item.getTuplePointer(), item.getStart(), item.getPoint());
    }

    public void reset(TuplePointer tp, byte start, long point) {
        this.tp = tp;
        this.start = start;
        this.point = point;
    }

    public TuplePointer getTuplePointer() {
        return tp;
    }

    public byte getStart() {
        return start;
    }

    public long getPoint() {
        return point;
    }

    @Override
    public String toString() {
        return "EndPointIndexItem " + tp + " " + (start == START_POINT ? "start" : "end") + ": " + point;
    }

}