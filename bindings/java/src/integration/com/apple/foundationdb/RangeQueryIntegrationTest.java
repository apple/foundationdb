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

import com.apple.foundationdb.async.AsyncIterable;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Integration tests around Range Queries. This requires a running FDB instance to work properly; 
 * all tests will be skipped if it can't connect to a running instance relatively quickly.
 */
public class RangeQueryIntegrationTest {
    @ClassRule
    public static final TestRule dbRule = RequiresDatabase.require();
    private static final FDB fdb = FDB.selectAPIVersion(700);
    
    @Test
    public void rangeQueryReturnsResults() throws Exception {
        /*
         * A quick test that if you insert a record, then do a range query which includes
         * the record, it'll be returned
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
