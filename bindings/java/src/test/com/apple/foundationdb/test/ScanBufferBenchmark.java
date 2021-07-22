/*
 * ScanBufferBenchmark.java
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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionTimer;
import com.apple.foundationdb.tuple.ByteArrayUtil;

public class ScanBufferBenchmark {

    public static void main(String[] args) throws Exception {
        int numRows = 1000;
        int keySizeBytes = 20;
        int valueSizeBytes = 20;
        int iterations = 10;
        int warmUpReads = 2;

        FDB api = FDB.selectAPIVersion(700);
        api.resizeDirectBufferPool(2, 110016);
        api.enableDirectBufferQuery(true);
        MapTimer timer = new MapTimer();
        try (Database database = api.open(null, timer)) {
            Map.Entry<byte[], byte[]> bounds = loadData(database, numRows, keySizeBytes, valueSizeBytes);
            timer.printEvents("Data Load");
            timer.clear();
            try {
                readData(database, timer, bounds, numRows, iterations,warmUpReads);
            } finally {
				database.run(tr -> {
					tr.clear(new Range(bounds.getKey(),ByteArrayUtil.strinc(bounds.getValue())));
					timer.printEvents("Clear");
                    timer.clear();
                    return null;
				});
			}
        }
    }

	private static void readData(Database database, MapTimer timer, Map.Entry<byte[], byte[]> bounds, int numRows,
	                             int iterations, int numWarmups) {
		System.out.println("Warm up Reads:");
		System.out.println("================");
        AtomicLong totalFetchTimeNanos = new AtomicLong(0L);
		for (int i = 0; i < numWarmups; i++) {
			final String label = "WarmupRead " + i;
            database.run(tr ->{
                doRead(tr,timer,bounds,numRows,label);
                totalFetchTimeNanos.addAndGet(timer.getTimeNanos(TransactionTimer.Events.RANGE_QUERY_FETCH_TIME_NANOS));
                timer.clear();
                return null;
            });
		}
        System.out.printf("Total runtime: %.3f micros %n",totalFetchTimeNanos.get()/1000f);
        System.out.printf("Avg runtime: %.3f micros %n",(totalFetchTimeNanos.get()/numWarmups)/1000f);
		System.out.println("================");

		System.out.println("Full Reads:");
		System.out.println("================");
        totalFetchTimeNanos.set(0L);
		for (int i = 0; i < iterations; i++) {
			final String label = "Read " + i;
            database.run(tr ->{
                doRead(tr,timer,bounds,numRows,label);
                totalFetchTimeNanos.addAndGet(timer.getTimeNanos(TransactionTimer.Events.RANGE_QUERY_FETCH_TIME_NANOS));
                timer.clear();
                return null;
            });
		}
        System.out.printf("Total runtime: %.3f micros %n",totalFetchTimeNanos.get()/1000f);
        System.out.printf("Avg runtime: %.3f micros %n",(totalFetchTimeNanos.get()/iterations)/1000f);
		System.out.println("================");
	}

	private static void doRead(Transaction tr, MapTimer timer, Map.Entry<byte[], byte[]> bounds, int numRows, String label) {
		Iterator<KeyValue> kvs = tr.getRange(bounds.getKey(), ByteArrayUtil.strinc(bounds.getValue())).iterator();
		int cnt = 0;
		while (kvs.hasNext()) {
			KeyValue kv = kvs.next();
			// forcibly materialize the keys and values
			byte[] key = kv.getKey();
			byte[] value = kv.getValue();
			cnt++;
		}

		timer.printEvents(label + "(numRows=" + cnt + ")");
	}

	private static Map.Entry<byte[], byte[]> loadData(Database database, int numRows, int keySizeBytes, int valueSizeBytes) throws Exception {
        /*
         * Returns the smallest and the largest byte[] loaded
         */
        return database.run(tr -> {
            byte[] minKey = null;
            byte[] maxKey = null;

            Random randomGen = new Random();
            byte[] key = new byte[keySizeBytes];
            byte[] value = new byte[valueSizeBytes];
            for (int i = 0; i < numRows; i++) {
                randomGen.nextBytes(key);
                randomGen.nextBytes(value);
                if(key[0] == (byte)0xFF){
                    key[0] = (byte)0xEF;
                }
                if (minKey == null) {
                    minKey = Arrays.copyOf(key, key.length);
                } else if (ByteArrayUtil.compareUnsigned(minKey, key) > 0) {
                    System.arraycopy(key, 0, minKey, 0, key.length);
                }
                if (maxKey == null) {
                    maxKey = Arrays.copyOf(key, key.length);
                } else if (ByteArrayUtil.compareUnsigned(key, maxKey) > 0) {
                    System.arraycopy(key, 0, maxKey, 0, key.length);
                }

                tr.set(key, value);
            }
            return new AbstractMap.SimpleEntry<>(minKey, maxKey);
        });
    }
}
