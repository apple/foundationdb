/*
 * SerialIteration.java
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;

public class SerialIteration {
	private static final int ROWS = 1000000;
	private static final int RUNS = 25;
	private static final int THREAD_COUNT = 1;

	public static void main(String[] args) throws InterruptedException {
		FDB api = FDB.selectAPIVersion(720);
		try(Database database = api.open(args[0])) {
			for(int i = 1; i <= THREAD_COUNT; i++) {
				runThreadedTest(database, i);
				Thread.sleep(1000);
			}
		}
	}

	private static double runThreadedTest(Database database, int threadCount) {
		List<IterationThread> threads = new ArrayList<>(threadCount);
		for(int i = 0; i < threadCount; i++) {
			IterationThread thread = new IterationThread(database);
			thread.start();
			threads.add(thread);
		}

		double rowsPerSecond = 0;
		for(IterationThread t : threads) {
			try {
				t.join();
				rowsPerSecond += t.getAverageRowsPerSecond();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.out.println("\n====================");
		System.out.println("Global rows/sec (over " + threadCount + " threads): "  + rowsPerSecond);
		return rowsPerSecond;
	}

	private static class IterationThread extends Thread {
		private static final Random r = new Random();
		private final Database db;
		private double averageRowsPerSecond;

		IterationThread(Database database) {
			this.db = database;
		}

		double getAverageRowsPerSecond() {
			return this.averageRowsPerSecond;
		}

		@Override
		public void run() {
			try {
				Thread.sleep(r.nextInt(1000));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long totalTime = 0;
			int totalRows = 0;
			for(int i = 0; i < RUNS; i++) {
				long start = System.currentTimeMillis();
				int rowsRead = scanDatabase(db, ROWS);
				long taken = System.currentTimeMillis() - start;
				//System.out.println(i + ": time taken for " + ROWS + " rows: " + taken + "ms");
				System.out.print(".");
				System.out.flush();
				if(i != 0) {
					totalRows += rowsRead;
					totalTime += taken;
				}
			}
			averageRowsPerSecond = (totalRows / (double)totalTime) * 1000;
			//System.out.println("Average rows/s: " + averageRowsPerSecond);
			System.out.print("+");
			System.out.flush();
		}
	}

	private static int scanDatabase(Database database, int rows) {
		try(Transaction tr = database.createTransaction()) {
			tr.options().setReadYourWritesDisable();

			ByteBuffer buf = ByteBuffer.allocate(4);
			buf.putInt(0, Integer.MAX_VALUE);
			AsyncIterable<KeyValue> range = tr.getRange(new byte[0], buf.array(),
					ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL);

			int counter = 0;
			try {
				for(@SuppressWarnings("unused") KeyValue keys : range) {
					counter++;
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
			return counter;
		}
	}

	private SerialIteration() {}
}
