/*
 * RangeQueryTest.java
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
package com.apple.foundationdb;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.tuple.FastByteComparisons;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests around the Range Query logic.
 * 
 * These tests do _not_ require a running FDB server to function. Instead, we
 * are operating on a good-faith "The underlying native library is correct"
 * functionality. For end-to-end tests which require a running server, see the
 * src/tests source folder.
 */
@RunWith(Parameterized.class)
public class RangeQueryTest {
    private static Executor EXECUTOR = new Executor() {

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    };
    private StreamingMode mode;

    @Parameterized.Parameters
    public static Collection<Object[]> params(){
        return  Arrays.<Object[]>asList(
            new Object[]{StreamingMode.WANT_ALL},
            new Object[]{StreamingMode.ITERATOR},
            new Object[]{StreamingMode.EXACT},
            new Object[]{StreamingMode.SMALL},
            new Object[]{StreamingMode.MEDIUM},
            new Object[]{StreamingMode.LARGE},
            new Object[]{StreamingMode.SERIAL}
        );
    }

    public RangeQueryTest(StreamingMode mode) {
        this.mode = mode;
    }

    private static FDBDatabase makeFakeDatabase(List<Map.Entry<byte[],byte[]>> data){
        return new FDBDatabase(1, EXECUTOR) {
            private long txnCounter = 3;

            @Override
            public Transaction createTransaction() {
                long tId = txnCounter;
                txnCounter++;
                return new FakeFDBTransaction(data, tId, this, EXECUTOR);
            }

            @Override
            protected void finalize() throws Throwable {
                // no-op
            }

            @Override
            public void close() {
                // no-op
            }
        };
    }

    @Test
    public void testRangeScansWorkWithoutRowLimit() throws Exception {
        /*
         * Test that the Range scan will return all the rows without the row limit.
         */ 
        List<Map.Entry<byte[], byte[]>> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(new AbstractMap.SimpleEntry<>(("apple" + i).getBytes(), ("crunchy" + i).getBytes()));
        }

        try (Database db = makeFakeDatabase(data)) {
            try (Transaction tr = db.createTransaction()) {
                byte[] val = tr.get("apple4".getBytes()).join();
                Assert.assertNotNull("Missing entry for 'apple4'!", val);
                Assert.assertArrayEquals("incorrect entry for 'apple4'~", val, "crunchy4".getBytes());

                // now do a range scan on the whole data set
                AsyncIterable<KeyValue> iter = tr.getRange("a".getBytes(), "b".getBytes(),0,false,mode);
                List<KeyValue> kvs = iter.asList().join();
                for (Map.Entry<byte[], byte[]> entry : data) {
                    boolean found = false;
                    for (KeyValue actualKv : kvs) {
                        if (FastByteComparisons.compareTo(entry.getKey(), 0, entry.getKey().length, actualKv.getKey(),
                                0, actualKv.getKey().length) == 0) {
                            String erroMsg = String.format("Incorrect value for key '%s'; Expected: <%s>, Actual: <%s>",
                                    new String(entry.getKey()), new String(entry.getValue()),
                                    new String(actualKv.getValue()));
                            Assert.assertEquals(erroMsg, 0, FastByteComparisons.compareTo(entry.getValue(), 0,
                                    entry.getValue().length, actualKv.getValue(), 0, actualKv.getValue().length));
                            found = true;
                            break;
                        }
                    }
                    Assert.assertTrue("Did not find key '" + new String(entry.getKey()) + "'", found);
                }
            }
        }
    }


    @Test
    public void testRangeScansWorkWithRowLimit() throws Exception {
        /*
         * Basic test to make sure that we don't ask for too many records or return too much data
         * when exercising the row limit
         */ 
        List<Map.Entry<byte[], byte[]>> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(new AbstractMap.SimpleEntry<>(("apple" + i).getBytes(), ("crunchy" + i).getBytes()));
        }

        try (Database db = makeFakeDatabase(data)) {
            try (Transaction tr = db.createTransaction()) {
                byte[] val = tr.get("apple4".getBytes()).join();
                Assert.assertNotNull("Missing entry for 'apple4'!", val);
                Assert.assertArrayEquals("incorrect entry for 'apple4'~", val, "crunchy4".getBytes());

                // now do a range scan on the whole data set
                int limit = 3;
                AsyncIterable<KeyValue> iter = tr.getRange("a".getBytes(), "b".getBytes(),limit,false,mode);
                List<KeyValue> kvs = iter.asList().join();
                Assert.assertEquals("incorrect number of kvs returned!",limit,kvs.size());
                int cnt = 0;
                for (Map.Entry<byte[], byte[]> entry : data) {
                    boolean found = false;
                    for (KeyValue actualKv : kvs) {
                        if (FastByteComparisons.compareTo(entry.getKey(), 0, entry.getKey().length, actualKv.getKey(),
                                0, actualKv.getKey().length) == 0) {
                            String erroMsg = String.format("Incorrect value for key '%s'; Expected: <%s>, Actual: <%s>",
                                    new String(entry.getKey()), new String(entry.getValue()),
                                    new String(actualKv.getValue()));
                            Assert.assertEquals(erroMsg, 0, FastByteComparisons.compareTo(entry.getValue(), 0,
                                    entry.getValue().length, actualKv.getValue(), 0, actualKv.getValue().length));
                            found = true;
                            break;
                        }
                    }
                    Assert.assertTrue("Did not find key '" + new String(entry.getKey()) + "'", found);
                    cnt++;
                    if(cnt == limit){
                        break;
                    }
                }

                Assert.assertEquals("Did not do the correct number of range requests",1,((FakeFDBTransaction)tr).getNumRangeCalls());
            }
        }
    }
}
