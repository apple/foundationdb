/*
 * RangeQueryIntegrationTest.java
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.FastByteComparisons;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Integration tests around Range Queries. This requires a running FDB instance
 * to work properly; all tests will be skipped if it can't connect to a running
 * instance relatively quickly.
 */
public class RangeQueryIntegrationTest {
    @ClassRule
    public static final TestRule dbRule = RequiresDatabase.require();
    private static final FDB fdb = FDB.selectAPIVersion(700);

    private void writeReadTest(NavigableMap<byte[],byte[]> dataToLoad, Consumer<Database> test){
        try(Database db = fdb.open()){
            db.run(tr->{
                for(Map.Entry<byte[],byte[]> entry : dataToLoad.entrySet()){
                    tr.set(entry.getKey(),entry.getValue());
                }
                return null;
            });
            try{
                test.accept(db);
            }finally{
                db.run(tr->{
                    tr.clear(new Range(dataToLoad.firstKey(),ByteArrayUtil.strinc(dataToLoad.lastKey())));
                    return null;
                });
            }
        }
    }

    @Test
    public void canGetRowWithKeySelector() throws Exception{
        Random rand = new Random();
        byte[] key= new byte[128];
        byte[] value = new byte[128];
        rand.nextBytes(key);
        key[0] = (byte)0xEE;
        rand.nextBytes(value);

        NavigableMap<byte[],byte[]> data = new TreeMap<>(FastByteComparisons.comparator());
        data.put(key,value);
        writeReadTest(data, (db)->{
            db.run(tr ->{
                byte[] actualValue = tr.get(key).join();
                Assert.assertNotNull("Missing key!",actualValue);
                Assert.assertArrayEquals("incorrect value!",value, actualValue);


                KeySelector start =  KeySelector.firstGreaterOrEqual(new byte[]{key[0]});
                KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(start.getKey()));
                AsyncIterable<KeyValue> kvIterable = tr.getRange(start,end);
                AsyncIterator<KeyValue> kvs = kvIterable.iterator();

                Assert.assertTrue("Did not return a record!", kvs.hasNext());
                KeyValue n = kvs.next();
                Assert.assertArrayEquals("Did not return a key correctly!", key, n.getKey());
                Assert.assertArrayEquals("Did not return the corect value!", value, n.getValue());

                return null;
            });
        });
    }


    @Test
    public void rangeQueryReturnsResults() throws Exception {
        /*
         * A quick test that if you insert a record, then do a range query which
         * includes the record, it'll be returned
         */
        try(Database db = fdb.open()){
            db.run(tr ->{
                tr.set("vcount".getBytes(),"zz".getBytes());
                return null;
            });

            try{
            db.run(tr->{
                AsyncIterable<KeyValue> kvs = tr.getRange("v".getBytes(), "y".getBytes());
                int cnt = 0;
                for(KeyValue kv : kvs){
                    Assert.assertArrayEquals("Incorrect key returned!","vcount".getBytes(),kv.getKey());
                    Assert.assertArrayEquals("Incorrect value returned!","zz".getBytes(),kv.getValue());
                    cnt++;
                }
                Assert.assertEquals("Incorrect number of KeyValues returned",1,cnt);

                return null;
            });
        }finally{
            db.run(tr ->{
                //remove what we wrote
                tr.clear("vcount".getBytes());
                return null;
            });
        }
        }
    }

    @Test
    public void rangeQueryReturnsEmptyOutsideRange() throws Exception {
        /*
         * A quick test that if you insert a record, then do a range query which does
         * not include the record, it won't be returned
         */
        try (Database db = fdb.open()) {
            db.run(tr -> {
                tr.set("rangeEmpty".getBytes(), "zz".getBytes());
                return null;
            });

            try {
                db.run(tr -> {
                    AsyncIterable<KeyValue> kvs = tr.getRange("b".getBytes(), "c".getBytes());
                    int cnt = 0;
                    for (KeyValue kv : kvs) {
                        Assert.fail("Found kvs when it really shouldn't!");
                        cnt++;
                    }
                    Assert.assertEquals("Incorrect number of KeyValues returned", 0, cnt);

                    return null;
                });
            } finally {
                db.run(tr -> {
                    // remove what we wrote
                    tr.clear("rangeEmpty".getBytes());
                    return null;
                });
            }
        }
    }

    @Test
    public void rangeQueryOverMultipleRows() throws Exception{
        /*
         * Make sure that you can return multiple rows if you ask for it. 
         * Hopefully this is large enough to force multiple batches
         */
        int numRows = 100;
        try (Database db = fdb.open()) {
            db.run(tr -> {
                for(int i=0;i<numRows;i++){
                    tr.set(("multiRow"+i).getBytes(),("multiValue"+i).getBytes());
                }
                return null;
            });

            try {
                db.run(tr -> {
                    AsyncIterable<KeyValue> kvs = tr.getRange("multi".getBytes(), "multj".getBytes());
                    int cnt = 0;
                    for (KeyValue kv : kvs) {
                        Assert.assertArrayEquals("Incorrect row returned",("multiRow"+cnt).getBytes(),kv.getKey());
                        Assert.assertArrayEquals("Incorrect value returned",("multiValue"+cnt).getBytes(),kv.getValue());
                        cnt++;
                    }
                    Assert.assertEquals("Incorrect number of KeyValues returned", numRows, cnt);

                    return null;
                });
            } finally {
                db.run(tr -> {
                    // remove what we wrote
                    tr.clear("multiRow".getBytes(),"multiRox".getBytes());
                    return null;
                });
            }
        }
    }
}
