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
package org.apache.hyracks.storage.common.buffercache;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author yingyib
 */
public class CachedPage implements ICachedPageInternal {
    final int cpid;
    ByteBuffer buffer;
    public final AtomicInteger pinCount;
    final AtomicBoolean dirty;
    final ReentrantReadWriteLock latch;
    private final Object replacementStrategyObject;
    private final IPageReplacementStrategy pageReplacementStrategy;
    volatile long dpid; // disk page id (composed of file id and page id)
    CachedPage next;
    volatile boolean valid;
    final AtomicBoolean confiscated;
    private IQueueInfo queueInfo;
    ILargePageHelper largePageHelper;

    //Constructor for making dummy entry for FIFO queue
    public CachedPage(){
        this.cpid = -1;
        this.buffer = null;
        this.pageReplacementStrategy = null;
        this.dirty = new AtomicBoolean(false);
        this.confiscated = new AtomicBoolean(true);
        pinCount = null;
        queueInfo = null;
        replacementStrategyObject = null;
        latch =null;
    }

    public CachedPage(int cpid, ByteBuffer buffer, IPageReplacementStrategy pageReplacementStrategy) {
        this.cpid = cpid;
        this.buffer = buffer;
        this.pageReplacementStrategy = pageReplacementStrategy;
        pinCount = new AtomicInteger();
        dirty = new AtomicBoolean();
        latch = new ReentrantReadWriteLock(true);
        replacementStrategyObject = pageReplacementStrategy.createPerPageStrategyObject(cpid);
        dpid = -1;
        valid = false;
        confiscated = new AtomicBoolean(false);
        queueInfo = null;
    }

    public void reset(long dpid) {
        this.dpid = dpid;
        dirty.set(false);
        valid = false;
        confiscated.set(false);
        pageReplacementStrategy.notifyCachePageReset(this);
        queueInfo = null;
        largePageHelper = null;
    }

    public void invalidate() {
        reset(-1);
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public Object getReplacementStrategyObject() {
        return replacementStrategyObject;
    }

    @Override
    public boolean isGoodVictim() {
        if (confiscated.get())
            return false; // i am not a good victim because i cant flush!
        else {
            return pinCount.get() == 0;
        }
    }

    @Override
    public int getCachedPageId() {
        return cpid;
    }

    @Override
    public void acquireReadLatch() {
        latch.readLock().lock();
    }

    @Override
    public void acquireWriteLatch() {
        latch.writeLock().lock();
    }

    @Override
    public void releaseReadLatch() {
        latch.readLock().unlock();
    }

    @Override
    public void releaseWriteLatch(boolean markDirty) {
        try {
            if (markDirty) {
                if (dirty.compareAndSet(false, true)) {
                    pinCount.incrementAndGet();
                }
            }
        } finally {
            latch.writeLock().unlock();
        }
    }

    @Override
    public boolean confiscated() {
        return confiscated.get();
    }

    @Override
    public IQueueInfo getQueueInfo() {
        return queueInfo;
    }

    @Override
    public void setQueueInfo(IQueueInfo queueInfo) {
        this.queueInfo = queueInfo;
    }

    @Override
    public long getDiskPageId() {
        return dpid;
    }

    CachedPage getNext() {
        return next;
    }

    void setNext(CachedPage next) {
        this.next = next;
    }

    public ILargePageHelper getLargePageHelper() {
        return largePageHelper;
    }

    public void setLargePageHelper(ILargePageHelper largePageHelper) {
        this.largePageHelper = largePageHelper;
    }

}
