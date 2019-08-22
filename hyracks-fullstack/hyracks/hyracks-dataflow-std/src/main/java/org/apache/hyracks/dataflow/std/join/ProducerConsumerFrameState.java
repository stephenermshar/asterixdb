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

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class ProducerConsumerFrameState implements IConsumerFrame {

    private final RecordDescriptor recordDescriptor;
    private ByteBuffer buffer;
    private boolean noMoreData = false;
    private Lock lock = new ReentrantLock();
    private Condition frameAvailable = this.lock.newCondition();
    private Condition frameProcessed = this.lock.newCondition();
    private boolean active = true;

    public ProducerConsumerFrameState(RecordDescriptor recordDescriptor) {
        this.recordDescriptor = recordDescriptor;
    }

    public void putFrame(ByteBuffer buffer) {
        if (!active) { // no more data is needed
            return;
        }
        lock.lock();
        try {
            while (this.buffer != null) {
                try {
                    frameProcessed.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            cloneByteBuffer(buffer);
            frameAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Function copied from StackOverflow.
     * https://stackoverflow.com/questions/3366925/deep-copy-duplicate-of-javas-bytebuffer
     * @param original
     */
    private void cloneByteBuffer(final ByteBuffer original) {
        // Create clone with same capacity as original.
        if (this.buffer == null || this.buffer.capacity() < original.capacity()) {
            this.buffer = (original.isDirect()) ? ByteBuffer.allocateDirect(original.capacity())
                    : ByteBuffer.allocate(original.capacity());
        }

        //        // Create a read-only copy of the original.
        //        // This allows reading from the original without modifying it.
        //        final ByteBuffer readOnlyCopy = original.asReadOnlyBuffer();
        //
        //        // Flip and read from the original.
        //        readOnlyCopy.flip();
        //        this.buffer.put(original);
        original.rewind();//copy from the beginning
        this.buffer.put(original);
        original.rewind();
        this.buffer.flip();

        this.buffer.position(original.position());
        this.buffer.limit(original.limit());
        this.buffer.order(original.order());
    }

    public void noMoreFrames() {
        noMoreData = true;
    }

    public boolean hasMoreFrames() {
        return !(noMoreData && this.buffer == null);
    }

    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptor;
    }

    public ByteBuffer getFrame() {
        lock.lock();
        try {
            while (this.buffer == null) {
                try {
                    frameAvailable.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            ByteBuffer returnValue = this.buffer;
            this.buffer = null;
            frameProcessed.signal();
            return returnValue;
        } finally {
            this.lock.unlock();
        }
    }

    public void closeFrameState() {
        if (hasMoreFrames()) {
            this.active = false;
            getFrame();
            //            this.buffer = null;
            //            lock.lock();
            //            frameProcessed.signal();
            //            this.lock.unlock();
        }
    }

}
