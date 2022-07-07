/*
 * RepeatableReadMultiThreadClientTest
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
package com.apple.foundationdb;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.apple.foundationdb.tuple.Tuple;

import org.junit.jupiter.api.Assertions;

/**
 * This test verify transcations have repeatable read.
 * 1 First set initialValue to key.
 * 2 Have transactions to read the key and verify the initialValue in a loop, if it does not 
 *   see the initialValue as the value, it set the flag to false.
 * 
 * 3 Then have new transactions set the value and then read to verify the new value is set,
 *   if it does not read the new value, set the flag to false.
 * 
 * 4 Verify that old transactions have not finished when new transactions have finished, 
 *   then verify old transactions does not have false flag -- it means that old transactions
 *   are still seeting the initialValue even after new transactions set them to a new value. 
 */
public class RepeatableReadMultiThreadClientTest {
    public static final MultiClientHelper clientHelper = new MultiClientHelper();

    private static final int oldValueReadCount = 30;
    private static final int threadPerDB = 5;

    private static final String key = "foo";
    private static final String initialValue = "bar";
    private static final String newValue = "cool";
    private static final Map<Thread, OldValueReader> threadToOldValueReaders = new HashMap<>();

    public static void main(String[] args) throws Exception {
        FDB fdb = FDB.selectAPIVersion(720);
        setupThreads(fdb);
        Collection<Database> dbs = clientHelper.openDatabases(fdb); // the clientHelper will close the databases for us
        System.out.println("Starting tests");
        setup(dbs);
        System.out.println("Start processing and validating");
        readOldValue(dbs);
        setNewValueAndRead(dbs);
        System.out.println("Test finished");
    }

    private static synchronized void setupThreads(FDB fdb) {
        int clientThreadsPerVersion = clientHelper.readClusterFromEnv().length;
        fdb.options().setClientThreadsPerVersion(clientThreadsPerVersion);
        System.out.printf("thread per version is %d\n", clientThreadsPerVersion);
        fdb.options().setExternalClientDirectory("/var/dynamic-conf/lib");
        fdb.options().setTraceEnable("/tmp");
        fdb.options().setKnob("min_trace_severity=5");
    }

    private static void setup(Collection<Database> dbs) {
        // 0 -> 1 -> 2 -> 3 -> 0
        for (Database db : dbs) {
            db.run(tr -> {
                tr.set(Tuple.from(key).pack(), Tuple.from(initialValue).pack());
                return null;
            });
        }
    }

    private static void readOldValue(Collection<Database> dbs) throws InterruptedException {
        for (Database db : dbs) {
            for (int i = 0; i < threadPerDB; i++) {
                final OldValueReader oldValueReader = new OldValueReader(db);
                final Thread thread = new Thread(OldValueReader.create(db));
                thread.start();
                threadToOldValueReaders.put(thread, oldValueReader);
            }
        }
    }

    private static void setNewValueAndRead(Collection<Database> dbs) throws InterruptedException {
        // threads running NewValueReader need to wait for threads to start first who run OldValueReader
        Thread.sleep(1000);
        final Map<Thread, NewValueReader> threads = new HashMap<>();
        for (Database db : dbs) {
            for (int i = 0; i < threadPerDB; i++) {
                final NewValueReader newValueReader = new NewValueReader(db);
                final Thread thread = new Thread(NewValueReader.create(db));
                thread.start();
                threads.put(thread, newValueReader);
            }
        }

        for (Map.Entry<Thread, NewValueReader> entry : threads.entrySet()) {
            entry.getKey().join();
            Assertions.assertTrue(entry.getValue().succeed, "new value reader failed to read the correct value");
        }

        for (Map.Entry<Thread, OldValueReader> entry : threadToOldValueReaders.entrySet()) {
            Assertions.assertTrue(entry.getKey().isAlive(), "Old value reader finished too soon, cannot verify repeatable read, succeed is " + entry.getValue().succeed);
        }

        for (Map.Entry<Thread, OldValueReader> entry : threadToOldValueReaders.entrySet()) {
            entry.getKey().join();
            Assertions.assertTrue(entry.getValue().succeed, "old value reader failed to read the correct value");
        }
    }

    public static class OldValueReader implements Runnable {

        private final Database db;
        private boolean succeed;

        private OldValueReader(Database db) {
            this.db = db;
            this.succeed = true;
        }

        public static OldValueReader create(Database db) {
            return new OldValueReader(db);
        }

        @Override
        public void run() {
            db.run(tr -> {
                try {
                    for (int i = 0; i < oldValueReadCount; i++) {
                        byte[] result = tr.get(Tuple.from(key).pack()).join();
                        String value = Tuple.fromBytes(result).getString(0);
                        if (!initialValue.equals(value)) {
                            succeed = false;
                            break;
                        }
                        Thread.sleep(100);
                    }
                }
                catch (Exception e) {
                    succeed = false;
                }
                return null;
            });
        }
    }

    public static class NewValueReader implements Runnable {
        private final Database db;
        private boolean succeed;

        public NewValueReader(Database db) {
            this.db = db;
            this.succeed = true;
        }

        public static NewValueReader create(Database db) {
            return new NewValueReader(db);
        }

        @Override
        public void run() {
            db.run(tr -> {
                tr.set(Tuple.from(key).pack(), Tuple.from(newValue).pack());
                return null;
            });
            String value = db.run(tr -> {
                byte[] result = tr.get(Tuple.from(key).pack()).join();
                return Tuple.fromBytes(result).getString(0);
            });
            if (!newValue.equals(value)) {
                succeed = false;
            }
        }
    }
}
