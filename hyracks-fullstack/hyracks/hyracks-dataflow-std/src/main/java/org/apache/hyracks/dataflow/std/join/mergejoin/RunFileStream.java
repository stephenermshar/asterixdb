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
package org.apache.hyracks.dataflow.std.join.mergejoin;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;

import java.util.logging.Level;
import java.util.logging.Logger;

public class RunFileStream {

    private static final Logger LOGGER = Logger.getLogger(RunFileStream.class.getName());

    private final String key;
    private final IFrame runFileBuffer;
    private final IFrameTupleAppender runFileAppender;
    private RunFileWriter runFileWriter;
    private RunFileReader runFileReader;
    private final IRunFileStreamStatus status;
    private FileReference runfile;

    private final IHyracksTaskContext ctx;

    private long runFileCounter = 0;
    private long readCount = 0;
    private long writeCount = 0;
    private long frameTupleCount = 0;
    private long totalTupleCount = 0;

    /**
     * The RunFileSream uses two frames to buffer read and write operations.
     *
     * @param ctx
     * @param key
     * @param status
     * @throws HyracksDataException
     */
    public RunFileStream(IHyracksTaskContext ctx, String key, IRunFileStreamStatus status) throws HyracksDataException {
        this.ctx = ctx;
        this.key = key;
        this.status = status;

        // TODO make the stream only use one buffer.
        runFileBuffer = new VSizeFrame(ctx);
        runFileAppender = new FrameTupleAppender(new VSizeFrame(ctx));

    }

    public long getFileCount() {
        return runFileCounter;
    }

    public long getTupleCount() {
        return totalTupleCount;
    }

    public long getReadCount() {
        return readCount;
    }

    public long getWriteCount() {
        return writeCount;
    }

    public void createRunFileWriting() throws HyracksDataException {
        runFileCounter++;
        String prefix = key + '-' + runFileCounter + '-' + this.toString();
        runfile = ctx.getJobletContext().createManagedWorkspaceFile(prefix);
        if (runFileWriter != null) {
            runFileWriter.close();
        }

        runFileWriter = new RunFileWriter(runfile, ctx.getIOManager());
        runFileWriter.open();
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("A new run file has been started (key: " + key + ", number: " + runFileCounter + ", file: "
                    + runfile + ").");
        }

        totalTupleCount = 0;
    }

    public void startRunFileWriting() throws HyracksDataException {
        status.setRunFileWriting(true);
        runFileBuffer.reset();
    }

    public void addToRunFile(ITupleAccessor accessor) throws HyracksDataException {
        int idx = accessor.getTupleId();
        addToRunFile(accessor, idx);
    }

    public void addToRunFile(IFrameTupleAccessor accessor, int idx) throws HyracksDataException {
        if (!runFileAppender.append(accessor, idx)) {
            runFileAppender.write(runFileWriter, true);
            writeCount++;
            runFileAppender.append(accessor, idx);
        }
        totalTupleCount++;
    }

    public void startReadingRunFile(ITupleAccessor accessor) throws HyracksDataException {
        startReadingRunFile(accessor, 0);
    }

    public void startReadingRunFile(ITupleAccessor accessor, long startOffset) throws HyracksDataException {
        if (runFileReader != null) {
            runFileReader.close();
        }
        status.setRunFileReading(true);

        // Create reader
        runFileReader = runFileWriter.createReader();
        runFileReader.open();
        runFileReader.reset(startOffset);

        // Load first frame
        loadNextBuffer(accessor);
    }

    public boolean loadNextBuffer(ITupleAccessor accessor) throws HyracksDataException {
        if (runFileReader.nextFrame(runFileBuffer)) {
            accessor.reset(runFileBuffer.getBuffer());
            accessor.next();
            readCount++;
            return true;
        }
        return false;
    }

    public void flushRunFile() throws HyracksDataException {
        status.setRunFileWriting(false);

        // Flush buffer.
        if (runFileAppender.getTupleCount() > 0) {
            runFileAppender.write(runFileWriter, true);
            writeCount++;
        }
        runFileBuffer.reset();
    }

    public void closeRunFileReading() throws HyracksDataException {
        status.setRunFileReading(false);
        runFileReader.close();
    }

    public void close() throws HyracksDataException {
        if (runFileReader != null) {
            runFileReader.close();
        }
        if (runFileWriter != null) {
            runFileWriter.close();
        }
    }

    public void removeRunFile() {
        if (runfile != null) {
            FileUtils.deleteQuietly(runfile.getFile());
        }
    }

    public long getReadPointer() {
        if (runFileReader != null) {
            return runFileReader.getReadPointer();
        }
        return -1;
    }

    public boolean isReading() {
        return status.isRunFileReading();
    }

    public boolean isWriting() {
        return status.isRunFileWriting();
    }

}
