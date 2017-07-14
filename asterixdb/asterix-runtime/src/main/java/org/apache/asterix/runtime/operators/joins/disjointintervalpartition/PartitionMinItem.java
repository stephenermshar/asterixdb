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
package org.apache.asterix.runtime.operators.joins.disjointintervalpartition;

import java.io.Serializable;
import java.util.Comparator;

public class PartitionMinItem implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final Comparator<PartitionMinItem> PartitionMinComparator = (epi1, epi2) -> {
        int c = (int) (epi1.getPoint() - epi2.getPoint());
        if (c == 0) {
            c = epi1.getPartition() - epi2.getPartition();
        }
        return c;
    };

    private int partition;
    private long point;

    public PartitionMinItem() {
        reset(-1, Long.MIN_VALUE);
    }

    public PartitionMinItem(int partition, long point) {
        reset(partition, point);
    }

    public void reset(PartitionMinItem item) {
        reset(item.getPartition(), item.getPoint());
    }

    public void reset(int partition, long point) {
        this.partition = partition;
        this.point = point;
    }

    public int getPartition() {
        return partition;
    }

    public long getPoint() {
        return point;
    }

    public void setPoint(long point) {
        this.point = point;
    }

    @Override
    public String toString() {
        return "PartitionMinItem " + partition + ": " + point;
    }

}