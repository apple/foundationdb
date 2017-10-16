/*
 * IterableTest.java
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

package com.apple.cie.foundationdb.test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.apple.cie.foundationdb.Cluster;
import com.apple.cie.foundationdb.Database;
import com.apple.cie.foundationdb.FDB;
import com.apple.cie.foundationdb.KeyValue;
import com.apple.cie.foundationdb.Transaction;
import com.apple.cie.foundationdb.TransactionContext;

public class IterableTest {
	private static final String CLUSTER_FILE = "C:\\Users\\Ben\\workspace\\fdb\\fdb.cluster";

	public static void main(String[] args) throws InterruptedException {
		final int reps = 1000;
		try {
			Cluster cluster = FDB.selectAPIVersion(500).createCluster(CLUSTER_FILE);
			Database db = cluster.openDatabase();
			runTests(reps, db);
		} catch(Throwable t) {
			t.printStackTrace();
		}
	}

	private static void runTests(final int reps, TransactionContext db) {
		System.out.println("Running tests...");
		long start = System.currentTimeMillis();
		final AtomicInteger lastcount = new AtomicInteger(0);
		try {
			db.run(new Function<Transaction, Void>() {
				@Override
				public Void apply(Transaction tr) {
					for(KeyValue e : tr.getRange("vcount".getBytes(), "zz".getBytes())) {
						System.out.println("K: " + new String(e.getKey()) + ", V: " + new String(e.getValue()));
					}
					return null;
				}
			});
		} catch (Throwable e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();

		double seconds = (end - start) / 1000.0;
		System.out.println(" Transactions:    " + reps);
		System.out.println(" Total Time:      " + seconds);
		System.out.println(" Gets+Sets / sec: " + reps / seconds);
		System.out.println(" Count:           " + lastcount.get());

		System.exit(0);
	}
}
