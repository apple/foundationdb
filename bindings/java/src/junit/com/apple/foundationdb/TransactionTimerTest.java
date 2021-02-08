/*
 * TransactionTimerTest.java
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import com.apple.foundationdb.TransactionTimer.Events;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.ByteArrayUtil;

import org.junit.Assert;
import org.junit.Test;

/**
 * Basic test code for testing basic Transaction Timer logic.
 * 
 * These tests don't check for a whole lot, they just verify that instrumentation works as
 * expected for specific patterns.
 */
public class TransactionTimerTest {

    @Test
    public void testSetVersion() throws Exception {

        TransactionTimer timer = new TestTimer();

        try (FDBTransaction txn = new FDBTransaction(1, null, null, timer)) {
            try {
                txn.setReadVersion(1L);
                Assert.fail("Test should call a bad native method");
            } catch (UnsatisfiedLinkError ignored) {
            }
            long jniCalls = timer.getCount(Events.JNI_CALL);

            Assert.assertEquals("Unexpected number of JNI calls:", 1L, jniCalls);
        } catch (UnsatisfiedLinkError ignored) {
        }
    }

    @Test
    public void testGetReadVersion() throws Exception {
        TransactionTimer timer = new TestTimer();

        try (FDBTransaction txn = new FDBTransaction(1, null, null, timer)) {
            try {
                txn.getReadVersion();
                Assert.fail("Test should call a bad native method");
            } catch (UnsatisfiedLinkError ignored) {
            }
            long jniCalls = timer.getCount(Events.JNI_CALL);

            Assert.assertEquals("Unexpected number of JNI calls:", 1L, jniCalls);
        } catch (UnsatisfiedLinkError ignored) {
        }
    }

    @Test
    public void testGetRangeRecordsFetches() throws Exception {
        TransactionTimer timer = new TestTimer();
        List<KeyValue> testKvs = Arrays.asList(new KeyValue("hello".getBytes(), "goodbye".getBytes()));

        FDBTransaction txn = new FDBTransaction(1,null,null,timer){
            @Override
            protected FutureResults getRange_internal(KeySelector begin,KeySelector end, int rowLimit,int targetBytes, int streamingMode, int iteration, boolean isSnapshot, boolean reverse){
                return new FutureResults(1,false,null,timer){
                    @Override
                    protected RangeResultInfo getIfDone_internal(long cPtr) throws FDBException{
                        return new RangeResultInfo(this);
                    } 
                    @Override
                    public RangeResult getResults(){
                        return new RangeResult(testKvs,false);
                    }
                    @Override protected void registerMarshalCallback(Executor executor){
                        marshalWhenDone();
                    }
                };
            }
        };


        RangeQuery query = new RangeQuery(txn, true, null, null, -1, false, StreamingMode.ITERATOR, timer);
        AsyncIterator<KeyValue> iter = query.iterator();

        List<KeyValue> iteratedItems = new ArrayList<>();
        while(iter.hasNext()){
            iteratedItems.add(iter.next());
        }

        //basic verification that we got back what we expected to get back. 
        Assert.assertEquals("Incorrect iterated list, size incorrect.",testKvs.size(),iteratedItems.size());

        int expectedByteSize = 0;
        for(KeyValue expected : testKvs){
            byte[] eKey = expected.getKey();
            byte[] eVal = expected.getValue();
            expectedByteSize+=eKey.length+4;
            expectedByteSize+=eVal.length+4;
            boolean found = false;
            for(KeyValue actual : iteratedItems){
                byte[] aKey = actual.getKey();
                byte[] aVal = actual.getValue();
                if(ByteArrayUtil.compareTo(eKey,0,eKey.length,aKey,0,aKey.length)==0){
                    int cmp = ByteArrayUtil.compareTo(eVal,0,eVal.length,aVal,0,aVal.length);
                    Assert.assertEquals("Incorrect value returned",0,cmp);
                    found = true;
                    break;
                }
            }

            Assert.assertTrue("missing key!",found);
        }

        //now check the timer and see if it recorded any events
        Assert.assertEquals("Unexpected number of chunk fetches",1,timer.getCount(Events.RANGE_QUERY_FETCHES));
        Assert.assertEquals("Unexpected number of tuples fetched",testKvs.size(),timer.getCount(Events.RANGE_QUERY_TUPLES_FETCHED));
        Assert.assertEquals("Incorrect number of bytes fetched",expectedByteSize,timer.getCount(Events.BYTES_FETCHED));
    }
    

    /*private helper methods and classes*/
    private static class TestTimer implements TransactionTimer {
        private Map<Event,Long> counterMap = new HashMap<>();

        @Override
        public void count(Event event, long amt) {
            Long currCnt = counterMap.get(event);
            if(currCnt ==null){
                counterMap.put(event,amt);
            }else{
                counterMap.put(event,currCnt+amt);
            }
        }

        @Override
        public void timeNanos(Event event, long nanos) {
            count(event,nanos);
        }

        @Override
        public long getCount(Event event) {
            return counterMap.getOrDefault(event, 0L);
        }

        @Override
        public long getTimeNanos(Event event) {
            return counterMap.getOrDefault(event, 0L);
        }
    }
}
