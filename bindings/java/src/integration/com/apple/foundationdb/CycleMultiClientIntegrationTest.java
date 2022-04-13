/*
 * CycleMultiClientIntegrationTest
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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import com.apple.foundationdb.tuple.Tuple;

import org.junit.jupiter.api.Assertions;

/**
 * Setup: Generating a cycle 0 -> 1 -> 2 -> 3 -> 0, its length is 4
 * Process: randomly choose an element, reverse 2nd and 4rd element, considering the chosen one as the 1st element.
 * Check: verify no element is lost or added, and they are still a cycle.
 * 
 * This test is to verify the atomicity of transactions. 
 */
public class CycleMultiClientIntegrationTest {
    public static final MultiClientHelper clientHelper = new MultiClientHelper();

    // more write txn than validate txn, as parent thread waits only for validate txn.
    private static final int writeTxnCnt = 2000;
    private static final int validateTxnCnt = 1000;
    private static final int threadPerDB = 5;

    private static final int cycleLength = 4;
    private static List<String> expected = new ArrayList<>(Arrays.asList("0", "1", "2", "3"));

    public static void main(String[] args) throws Exception {
        FDB fdb = FDB.selectAPIVersion(720);
        setupThreads(fdb);
        Collection<Database> dbs = clientHelper.openDatabases(fdb); // the clientHelper will close the databases for us
        System.out.println("Starting tests");
        setup(dbs);
        System.out.println("Start processing and validating");
        process(dbs);
        check(dbs);
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
                for (int k = 0; k < cycleLength; k++) {
                    String key = Integer.toString(k);
                    String value = Integer.toString((k + 1) % cycleLength);
                    tr.set(Tuple.from(key).pack(), Tuple.from(value).pack());
                }
                return null;
            });
        }
    }

    private static void process(Collection<Database> dbs) {
        for (Database db : dbs) {
            for (int i = 0; i < threadPerDB; i++) {
                final Thread thread = new Thread(CycleWorkload.create(db));
                thread.start();
            }
        }
    }

    private static void check(Collection<Database> dbs) throws InterruptedException {
        final Map<Thread, CycleChecker> threadsToCheckers = new HashMap<>();
        for (Database db : dbs) {
            for (int i = 0; i < threadPerDB; i++) {
                final CycleChecker checker = new CycleChecker(db);
                final Thread thread = new Thread(checker);
                thread.start();
                threadsToCheckers.put(thread, checker);
            }
        }

        for (Map.Entry<Thread, CycleChecker> entry : threadsToCheckers.entrySet()) {
            entry.getKey().join();
            final boolean succeed = entry.getValue().succeed();
            Assertions.assertTrue(succeed, "Cycle test failed");
        }
    }

    public static class CycleWorkload implements Runnable {

        private final Database db;

        private CycleWorkload(Database db) {
            this.db = db;
        }

        public static CycleWorkload create(Database db) {
            return new CycleWorkload(db);
        }

        @Override
        public void run() {
            for (int i = 0; i < writeTxnCnt; i++) {
                db.run(tr -> {
                    final int k = ThreadLocalRandom.current().nextInt(cycleLength);
                    final String key = Integer.toString(k);
                    byte[] result1 = tr.get(Tuple.from(key).pack()).join();
                    String value1 = Tuple.fromBytes(result1).getString(0);

                    byte[] result2 = tr.get(Tuple.from(value1).pack()).join();
                    String value2 = Tuple.fromBytes(result2).getString(0);

                    byte[] result3 = tr.get(Tuple.from(value2).pack()).join();
                    String value3 = Tuple.fromBytes(result3).getString(0);

                    byte[] result4 = tr.get(Tuple.from(value3).pack()).join();

                    tr.set(Tuple.from(key).pack(), Tuple.from(value2).pack());
                    tr.set(Tuple.from(value2).pack(), Tuple.from(value1).pack());
                    tr.set(Tuple.from(value1).pack(), Tuple.from(value3).pack());
                    return null;
                });
            }
        }
    }

    public static class CycleChecker implements Runnable {
        private final Database db;
        private boolean succeed;

        public CycleChecker(Database db) {
            this.db = db;
            this.succeed = true;
        }

        public static CycleChecker create(Database db) {
            return new CycleChecker(db);
        }

        @Override
        public void run() {
            for (int i = 0; i < validateTxnCnt; i++) {
                db.run(tr -> {
                    final int k = ThreadLocalRandom.current().nextInt(cycleLength);
                    final String key = Integer.toString(k);
                    byte[] result1 = tr.get(Tuple.from(key).pack()).join();
                    String value1 = Tuple.fromBytes(result1).getString(0);

                    byte[] result2 = tr.get(Tuple.from(value1).pack()).join();
                    String value2 = Tuple.fromBytes(result2).getString(0);

                    byte[] result3 = tr.get(Tuple.from(value2).pack()).join();
                    String value3 = Tuple.fromBytes(result3).getString(0);

                    byte[] result4 = tr.get(Tuple.from(value3).pack()).join();
                    String value4 = Tuple.fromBytes(result4).getString(0);

                    if (!key.equals(value4)) {
                        succeed = false;
                    }
                    List<String> actual = new ArrayList<>(Arrays.asList(value1, value2, value3, value4));
                    Collections.sort(actual);
                    if (!expected.equals(actual)) {
                        succeed = false;
                    }
                    return null;
                });
            }
        }

        public boolean succeed() {
            return succeed;
        }
    }
}
