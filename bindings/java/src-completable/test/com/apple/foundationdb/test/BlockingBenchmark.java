/*
 * BlockingBenchmark.java
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

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;

public class BlockingBenchmark {
	private static final int REPS = 100000;
	private static final int PARALLEL = 100;

	public static void main(String[] args) throws InterruptedException {
		FDB fdb = FDB.selectAPIVersion(500);

		// The cluster file DOES NOT need to be valid, although it must exist.
		//  This is because the database is never really contacted in this test.
		Database database = fdb.open("T:\\circus\\tags\\RebarCluster-bbc\\cluster_id.txt");

		byte[] key = {0x1, 0x1, 0x1, 0x1, 0x1};
		byte[] val = {0x2, 0x2, 0x2, 0x2, 0x2};

		Transaction tr = database.createTransaction();
		tr.setReadVersion(100000);
		final Function<Long,Long> identity = new Function<Long, Long>() {
			@Override
			public Long apply(Long o) {
				return o;
			}
		};


		System.out.println("readVersion().join():");
		runTests(tr, new Function<CompletableFuture<Long>, Void>() {
			@Override
			public Void apply(CompletableFuture<Long> o) {
				try {
					o.join();
				} catch(Exception e) { }

				return null;
			}
		});

		System.out.println("readVersion().get():");
		runTests(tr, new Function<CompletableFuture<Long>, Void>() {
			@Override
			public Void apply(CompletableFuture<Long> o) {
				try {
					o.get();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch(Exception e) { }

				return null;
			}
		});

		System.out.println("readVersion().thenApplyAsync(identity).get():");
		runTests(tr, new Function<CompletableFuture<Long>, Void>() {
			@Override
			public Void apply(CompletableFuture<Long> o) {
				try {
					o.thenApplyAsync(identity).get();
				} catch(Exception e) { }

				return null;
			}
		});

		System.out.println("readVersion().thenApplyAsync^10(identity).get():");
		runTests(tr, new Function<CompletableFuture<Long>, Void>() {
			@Override
			public Void apply(CompletableFuture<Long> o) {
				for(int i=0; i<10; i++)
					o = o.thenApplyAsync(identity);
				try {
					o.get();
				} catch(Exception e) { }

				return null;
			}
		});

		System.out.println("readVersion().get^100():");
		runTests(tr, new Function<CompletableFuture<Long>, Void>() {
			@Override
			public Void apply(CompletableFuture<Long> o) {
				for(int i=0; i<100; i++) {
					try {
						o.get();
					} catch(Exception e) { }
				}
				return null;
			}
		});

	}

	private static void runTests(Transaction tr, Function<CompletableFuture<Long>, Void> blockMethod) {
		for(int r=0; r<4; r++) {
			long start = System.currentTimeMillis();
			for(int i = 0; i < REPS; i++) {
				blockMethod.apply( tr.getReadVersion() );
			}

			long taken = System.currentTimeMillis() - start;
			System.out.println("  " + REPS + " done in " + taken + "ms -> " + ((taken * 1000.0) / REPS) + " us latency");

			ArrayList<CompletableFuture<Long>> futures = new ArrayList<CompletableFuture<Long>>(PARALLEL);
			for(int j=0; j<PARALLEL; j++)
				futures.add(null);

			start = System.currentTimeMillis();
			for(int i = 0; i < REPS; i += PARALLEL) {
				for(int j=0; j<PARALLEL; j++)
					futures.set(j, tr.getReadVersion());
				for(int j=0; j<PARALLEL; j++)
					blockMethod.apply(futures.get(j));
			}
			taken = System.currentTimeMillis() - start;
			System.out.println("  " + REPS + " done in " + taken + "ms -> " + (REPS / (taken)) + " KHz");
		}
	}
}
