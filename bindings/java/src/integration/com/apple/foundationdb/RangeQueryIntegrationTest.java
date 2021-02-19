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

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
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

    @Test
    public void getWithExactKeySelectorWillIgnoreRow() throws Exception{
        // TODO: Move this to its own integration test
        byte[] key = new byte[]{(byte)0x02,0x01};
        byte[] value = "testValue".getBytes();
        NavigableMap<byte[],byte[]> data = new TreeMap<>(FastByteComparisons.comparator());
        data.put(key,value);
        writeReadTest(data, (db)->{
            db.run(tr ->{
                byte[] selectKey = new byte[]{(byte)0x01};
                byte[] shouldBeEmpty = tr.getKey(KeySelector.exact(selectKey)).join();
                Assert.assertNotNull("Returned a null key!",shouldBeEmpty);
                Assert.assertEquals("Did not return an empty key!",0,shouldBeEmpty.length);

                return null;
            });
        });

    }

    @Test
    public void canGetRowWithExactKeySelector() throws Exception{
        /*
         * Tests that a row is retrieved if you get a getRange(exact(),exact()).
         */

        byte[] key = new byte[]{(byte)0xEE,0x01};
        byte[] value = "testValue".getBytes();
        NavigableMap<byte[],byte[]> data = new TreeMap<>(FastByteComparisons.comparator());
        data.put(key,value);
        writeReadTest(data, (db)->{
            db.run(tr ->{
                byte[] actualValue = tr.get(key).join();
                Assert.assertNotNull("Missing key!",actualValue);
                Assert.assertArrayEquals("incorrect value!",value, actualValue);

                KeySelector start =  KeySelector.exact(new byte[]{key[0]});
                KeySelector end = KeySelector.exact(ByteArrayUtil.strinc(start.getKey()));

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
    public void lessThanOrEqualExcludesEndKey() throws Exception{
        /*
         * This does a scan on the range [0xAA, lastLessOrEqual(0xAB)), with a single
         * key in the value {0xAA,0x01}. The key should be ignored because lastLessOrEqual will
         * identify the end of the range as {0xAA,0x01}, and getRange is exclusive on the end value.
         */
        byte[] key = new byte[]{(byte)0xAA,0x01};
        byte[] value = "testValue".getBytes();
        NavigableMap<byte[],byte[]> data = new TreeMap<>(FastByteComparisons.comparator());
        data.put(key,value);
        writeReadTest(data, (db)->{
            db.run(tr ->{
                byte[] actualValue = tr.get(key).join();
                Assert.assertNotNull("Missing key!",actualValue);
                Assert.assertArrayEquals("incorrect value!",value, actualValue);

                KeySelector start =  KeySelector.exact(new byte[]{key[0]});
                KeySelector end = KeySelector.lastLessOrEqual(ByteArrayUtil.strinc(start.getKey()));

                AsyncIterable<KeyValue> kvIterable = tr.getRange(start,end);
                AsyncIterator<KeyValue> kvs = kvIterable.iterator();

                Assert.assertFalse("Did not return a record!", kvs.hasNext());

                return null;
            });
        });
    }

    @Test
    public void greaterThanIncludeStartKey() throws Exception{
        /*
         * This does a scan on the range [firstGreaterThan(0xBB), 0xBC), with a single
         * key in the value {0xBB,0x01}. The key should NOT be ignored because firstGreaterThan will
         * identify the start of the range as {0xBB,0x01}, and getRange is inclusive on the start value
         */
        byte[] key = new byte[]{(byte)0xBB,0x01};
        byte[] value = "testValue".getBytes();
        NavigableMap<byte[],byte[]> data = new TreeMap<>(FastByteComparisons.comparator());
        data.put(key,value);
        writeReadTest(data, (db)->{
            db.run(tr ->{
                byte[] actualValue = tr.get(key).join();
                Assert.assertNotNull("Missing key!",actualValue);
                Assert.assertArrayEquals("incorrect value!",value, actualValue);

                KeySelector start =  KeySelector.firstGreaterThan(new byte[]{key[0]});
                KeySelector end = KeySelector.exact(ByteArrayUtil.strinc(new byte[]{key[0]}));

                AsyncIterable<KeyValue> kvIterable = tr.getRange(start,end);
                AsyncIterator<KeyValue> kvs = kvIterable.iterator();

                Assert.assertTrue("Did not return a record!",kvs.hasNext());
                KeyValue kv = kvs.next();
                Assert.assertArrayEquals("Incorrect key",key, kv.getKey());
                Assert.assertArrayEquals("Incorrect value",value, kv.getValue());

                Assert.assertFalse("Returned too many records!", kvs.hasNext());

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
        NavigableMap<byte[],byte[]> map = new TreeMap<>(FastByteComparisons.comparator());
        map.put("vcount".getBytes(),"zz".getBytes());
        writeReadTest(map, (db)->{
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
        });
    }

    @Test
    public void rangeQueryReturnsEmptyOutsideRange() throws Exception {
        /*
         * A quick test that if you insert a record, then do a range query which does
         * not include the record, it won't be returned
         */
        NavigableMap<byte[],byte[]> map = new TreeMap<>(FastByteComparisons.comparator());
        map.put("rangeEmpty".getBytes(),"zz".getBytes());
        writeReadTest(map,(db)->{
            db.run(tr->{
                    AsyncIterator<KeyValue> kvs = tr.getRange("b".getBytes(), "c".getBytes()).iterator();
                    Assert.assertFalse("Incorrectly returning data!",kvs.hasNext());
                    return null;
            });
        });
    }

    @Test
    public void rangeQueryOverMultipleRows() throws Exception{
        /*
         * Make sure that you can return multiple rows if you ask for it. 
         * Hopefully this is large enough to force multiple batches
         */
        int numRows =25;
        TreeMap<byte[],byte[]> data = new TreeMap<>(FastByteComparisons.comparator());
        for(int i=0;i<numRows;i++){
            data.put(("multiRow"+i).getBytes(),("multiValue"+i).getBytes());
        }
        writeReadTest(data, db->{
            db.run(tr->{
                    AsyncIterator<KeyValue> kvs = tr.getRange("multi".getBytes(), "multj".getBytes()).iterator();
                    Iterator<Map.Entry<byte[],byte[]>> expectedKvs = data.entrySet().iterator();
                    int cnt = 0;
                    while(expectedKvs.hasNext()){
                        Assert.assertTrue("returned range is too small!",kvs.hasNext());
                        Map.Entry<byte[],byte[]> expectedKv = expectedKvs.next();
                        KeyValue actualKv = kvs.next();
                        Assert.assertArrayEquals("["+cnt+"]Incorrect row returned",expectedKv.getKey(),actualKv.getKey());
                        Assert.assertArrayEquals("["+cnt+"]Incorrect value returned",expectedKv.getValue(),actualKv.getValue());
                        
                        cnt++;
                    }
                    Assert.assertFalse("Iterator range is too great!",kvs.hasNext());
                    Assert.assertEquals("Incorrect number of KeyValues returned", numRows, cnt);
                return null;
            });
        });
    }

    /* ***********************************************************************************************************/
    /*private helper methods*/
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
}
