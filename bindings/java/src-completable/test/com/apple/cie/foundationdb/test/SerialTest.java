/*
 * SerialTest.java
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
import com.apple.cie.foundationdb.Transaction;
import com.apple.cie.foundationdb.TransactionContext;

public class SerialTest {

	private static final String CLUSTER_FILE = "T:\\Ben\\cluster";

	public static void main(String[] args) throws InterruptedException {
		final int reps = 1000;
		try {
			Cluster c = FDB.selectAPIVersion(510).createCluster(CLUSTER_FILE);
			Database db = c.openDatabase();
			runTests(reps, db);

			/*Cluster fCluster = Cluster.create("C:\\Users\\Ben\\workspace\\fdb\\fdb.cluster").get();
			System.out.println("I now have the cluster");
			Database db = cluster.openDatabase().get();

			runTests(reps, db);*/
		} catch(Throwable t) {
			t.printStackTrace();
		}
	}

	private static void runTests(int reps, TransactionContext db) {
		System.out.println("Running tests...");
		long start = System.currentTimeMillis();
		final AtomicInteger lastcount = new AtomicInteger(0);
		for(int i = 0; i < reps; i++) {
			try {
				db.run(new Function<Transaction, Void>() {
					@Override
					public Void apply(Transaction tr) {
						byte[] val = tr.get("count".getBytes()).join();
						//System.out.println("Got value");
						int count = Integer.parseInt(new String(val));
						tr.set("count".getBytes(), Integer.toString(count + 1).getBytes());
						lastcount.set(count);

						return null;
					}
				});
			} catch (Throwable e) {
				e.printStackTrace();
			}
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
