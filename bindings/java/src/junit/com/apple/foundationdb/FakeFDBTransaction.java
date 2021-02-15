/*
 * FakeFDBTransaction.java
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A convenience class that makes it easier to construct a mock FDBTransaction. This class does no native library calls: 
 * instead, it seeks to mimic (in a simplistic way) what the FDB native API promises. The intent is to make it easier to
 * unit test specific java code without requiring a running server or needing to test the entire C library as well.
 */
public class FakeFDBTransaction extends FDBTransaction {
    private final NavigableMap<byte[], byte[]> backingData;
    private final Executor executor;
    
    private int numRangeCalls = 0;

    protected FakeFDBTransaction(long cPtr, Database database, Executor executor) {
        super(cPtr, database, executor);
        this.backingData = new TreeMap<>((l, r) -> byteArrayCompare(l, r));
        this.executor = executor;
    }

    public FakeFDBTransaction(Map<byte[], byte[]> backingData, long cPtr, Database db, Executor executor) {
        this(cPtr, db, executor);
        this.backingData.putAll(backingData);
    }

    public FakeFDBTransaction(Collection<Map.Entry<byte[], byte[]>> backingData, long cPtr, Database db,
            Executor executor) {
        this(cPtr, db, executor);

        for (Map.Entry<byte[], byte[]> entry : backingData) {
            this.backingData.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public CompletableFuture<byte[]> get(byte[] key) {
        return CompletableFuture.completedFuture(this.backingData.get(key));
    }

    /**
     * @return the number of times getRange_internal() was called. Useful for checking underlying behavior.
     */
    public int getNumRangeCalls(){
        return numRangeCalls;
    }

    @Override
    protected FutureResults getRange_internal(KeySelector begin, KeySelector end, int rowLimit, int targetBytes,
            int streamingMode, int iteration, boolean isSnapshot, boolean reverse) {
                numRangeCalls++;
        NavigableMap<byte[], byte[]> range;
        byte[] startKey = begin.getKey();
        byte[] endKey = end.getKey();
        if (startKey.length == 0) {
            if (endKey.length == 0) {
                range = backingData;
            } else {
                range = backingData.headMap(endKey, end.orEqual());
            }
        } else if (endKey.length == 0) {
            range = backingData.tailMap(startKey, begin.orEqual());
        } else {
            range = backingData.subMap(begin.getKey(), begin.orEqual(), end.getKey(), end.orEqual());
        }
        if (reverse) {
            // reverse the order of the scan
            NavigableMap<byte[], byte[]> reversed = new TreeMap<>((l, r) -> -1 * byteArrayCompare(l, r)); // reverse
                                                                                                          // order
            reversed.putAll(range);
            range = reversed;
        }

        // holder variable so that we can pass the range to the results function safely
        final NavigableMap<byte[], byte[]> retMap = range;
        FutureResults fr = new FutureResults(-1L, false, executor) {

            @Override
            protected void registerMarshalCallback(Executor executor) {
                // no-op
            }

            @Override
            protected RangeResultInfo getIfDone_internal(long cPtr) throws FDBException {
                return new RangeResultInfo(this);
            }

            @Override
            public RangeResult getResults() {
                SortedMap<byte[], byte[]> reduced = shrinkToFit(retMap, rowLimit, targetBytes, reverse);
                boolean more = reduced.size() < retMap.size();
                List<KeyValue> kvs = new ArrayList<>(reduced.size());
                for (Map.Entry<byte[], byte[]> kvEntry : reduced.entrySet()) {
                    kvs.add(new KeyValue(kvEntry.getKey(), kvEntry.getValue()));
                }
                return new RangeResult(kvs, more);
            }

            @Override
            public void close() {
                //no-op
            }
            
        };

        fr.complete(new RangeResultInfo(fr));
        return fr;
    }

    @Override
    protected void closeInternal(long cPtr) {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    protected void finalize() throws Throwable {
        //no-op
    }

    /* **************************************************************************************************************************************************************************/
    /* private helper functions */
    private static SortedMap<byte[], byte[]> shrinkToFit(NavigableMap<byte[], byte[]> map, int rowLimit,
            int targetBytes, boolean reversed) {
        if (rowLimit <= 0) {
            rowLimit = Integer.MAX_VALUE;
        }
        if (targetBytes <= 0) {
            targetBytes = Integer.MAX_VALUE;
        }

        SortedMap<byte[], byte[]> retMap;
        if (reversed) {
            retMap = new TreeMap<>((l, r) -> -1 * byteArrayCompare(l, r));
        } else {
            retMap = new TreeMap<>((l, r) -> byteArrayCompare(l, r));
        }

        int totalBytes = 0;
        int rowCount = 0;
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            // stop if we have exceeded our byte limit
            totalBytes += entry.getKey().length + entry.getValue().length;
            if (totalBytes > targetBytes) {
                break;
            }
            retMap.put(entry.getKey(), entry.getValue());
            rowCount++;
            // stop if we exceed our row count
            if (rowCount == rowLimit) {
                break;
            }
        }
        return retMap;
    }

    public static int byteArrayCompare(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }



}
