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

import org.apache.asterix.dataflow.data.nontagged.printers.adm.AObjectPrinterFactory;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;

public class TuplePrinterUtil {

    private static final IPrinter printer = AObjectPrinterFactory.INSTANCE.createPrinter();

    private TuplePrinterUtil() {
    }

    public static void printTuple(String message, ITupleAccessor accessor) throws HyracksDataException {
        if (accessor.exists()) {
            printTuple(message, accessor, accessor.getTupleId());
        } else {
            System.out.flush();
            System.err.flush();
            System.err.print(String.format("%1$-" + 15 + "s", message) + " --");
            System.err.print("no tuple");
            System.err.println();
            System.err.flush();
        }
    }

    public static void printTuple(String message, IFrameTupleAccessor accessor, int tupleId)
            throws HyracksDataException {
        System.out.flush();
        System.err.flush();
        System.err.print(String.format("%1$-" + 15 + "s", message) + " -- " + accessor.getTupleLength(tupleId) + " --");
        int fields = accessor.getFieldCount();
        for (int i = 0; i < fields; ++i) {
            System.err.print(" " + i + ": ");
            int fieldStartOffset = accessor.getFieldStartOffset(tupleId, i);
            int fieldSlotsLength = accessor.getFieldSlotsLength();
            int tupleStartOffset = accessor.getTupleStartOffset(tupleId);
            printer.print(accessor.getBuffer().array(), fieldStartOffset + fieldSlotsLength + tupleStartOffset,
                    accessor.getFieldLength(tupleId, i), System.err);
        }
        System.err.println();
        System.err.flush();
    }

}
