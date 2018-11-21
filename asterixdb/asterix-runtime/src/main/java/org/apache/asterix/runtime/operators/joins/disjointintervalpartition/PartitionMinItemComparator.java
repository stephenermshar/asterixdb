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

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

public class PartitionMinItemComparator implements Comparator<PartitionMinItem> {

    private final AtomicInteger counter;

    public PartitionMinItemComparator() {
        counter = new AtomicInteger();
    }

    @Override
    public int compare(final PartitionMinItem epi1, final PartitionMinItem epi2) {
        counter.incrementAndGet();
        int c = (int) (epi1.getPoint() - epi2.getPoint());
        if (c == 0) {
            c = epi1.getPartition() - epi2.getPartition();
        }
        return c;
    }

    public int getTotalCalled() {
        return counter.get();
    }

}