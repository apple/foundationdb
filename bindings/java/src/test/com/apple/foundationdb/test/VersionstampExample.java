/*
 * VersionstampExample.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.Function;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.async.Future;

import java.lang.System;

public class VersionstampExample {
    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static void main(String[] args) {
        FDB fdb = FDB.selectAPIVersion(500);
        Database db = fdb.open();
        Transaction tr = db.createTransaction();
        tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, "foo".getBytes(), "blahblahbl".getBytes());
        Future<byte[]> fvs = tr.getVersionstamp();
        tr.commit().get();
        byte[] vs = fvs.get();

        // Get the value of 'hello' from the database
        byte[] dbVs = db.run(new Function<Transaction, byte[]>() {
            public byte[] apply(Transaction tr) {
                return tr.get("foo".getBytes()).get();
            }
        });
        System.out.println("vs" + bytesToHex(vs));
        System.out.println("dbVs" + bytesToHex(dbVs));
    }
}
