/*
 * ParallelRandomScan.java
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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.ByteArrayUtil;

public class ParallelRandomScan {
	private static final int ROWS = 1000000;
	private static final int DURATION_MS = 2000;
	private static final int PARALLELISM_MIN = 10;
	private static final int PARALLELISM_MAX = 100;
	private static final int PARALLELISM_STEP = 5;

	public static void main(String[] args) throws InterruptedException {
		FDB api = FDB.selectAPIVersion(720);
		try(Database database = api.open(args[0])) {
			for(int i = PARALLELISM_MIN; i <= PARALLELISM_MAX; i += PARALLELISM_STEP) {
				runTest(database, i, ROWS, DURATION_MS);
				Thread.sleep(1000);
			}
		}
	}

	private static void runTest(Database database,
			int parallelism, int rows, int duration) throws InterruptedException {
		final Random r = new Random();
		final AtomicInteger readsCompleted = new AtomicInteger(0);
		final AtomicInteger errors = new AtomicInteger(0);
		final Semaphore coordinator = new Semaphore(parallelism);
		final ContinuousSample<Long> latencies = new ContinuousSample<>(1000);

		try(Transaction tr = database.createTransaction()) {
			tr.options().setReadYourWritesDisable();

			// Clearing the whole database before starting means all reads are local
			/*ByteBuffer buf = ByteBuffer.allocate(4);
			buf.putInt(0, Integer.MAX_VALUE);
			tr.clear(new byte[0], buf.array());*/

			// We use this for the key generation
			ByteBuffer buf = ByteBuffer.allocate(4);

			// Eat the cost of the read version up-front
			tr.getReadVersion().join();

			final long start = System.currentTimeMillis();
			while(true) {
				coordinator.acquire();
				if(System.currentTimeMillis() - start > duration) {
					coordinator.release();
					break;
				}

				int row = r.nextInt(rows - 1);
				buf.putInt(0, row);
				AsyncIterable<KeyValue> range = tr.getRange(
						buf.array(), ByteArrayUtil.strinc(buf.array()), 1, false, StreamingMode.SMALL);

				final long launch = System.nanoTime();

				final AsyncIterator<KeyValue> it = range.iterator();
				final CompletableFuture<KeyValue> f = it.onHasNext().thenApplyAsync(hasFirst -> {
					if(!hasFirst) {
						return null;
					}
					return it.next();
				}, FDB.DEFAULT_EXECUTOR);
				f.whenCompleteAsync((kv, t) -> {
					if(kv != null) {
						readsCompleted.incrementAndGet();
						long timeTaken = System.nanoTime() - launch;
						synchronized(latencies) {
							latencies.addSample(timeTaken);
						}
					}
					else if(t != null) {
						errors.incrementAndGet();
					}

					coordinator.release();
				}, FDB.DEFAULT_EXECUTOR);
			}

			// Block for ALL tasks to end!
			coordinator.acquire(parallelism);
			long end = System.currentTimeMillis();

			double rowsPerSecond = readsCompleted.get() / ((end - start) / 1000.0);
			System.out.println(parallelism + " ->\t" + rowsPerSecond);
			System.out.println(String.format("  Reads: %d, errors: %d, time: %dms",
					readsCompleted.get(), errors.get(), (int) (end - start)));
			System.out.println(String.format("  Mean: %.2f, Median: %d, 98%%: %d",
					latencies.mean(), latencies.median(), latencies.percentile(0.98)));
		}
	}

	private ParallelRandomScan() {}
}
