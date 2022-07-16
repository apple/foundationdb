/*
 * SerialInsertion.java
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;

public class SerialInsertion {
	private static final int THREAD_COUNT = 10;
	private static final int BATCH_SIZE = 1000;
	private static final int NODES = 1000000;

	public static void main(String[] args) {
		FDB api = FDB.selectAPIVersion(720);
		try(Database database = api.open()) {
			long start = System.currentTimeMillis();

			List<InsertionThread> threads = new ArrayList<InsertionThread>(THREAD_COUNT);
			int nodesPerThread = NODES / THREAD_COUNT;
			for(int i = 0; i < THREAD_COUNT; i++) {
				// deal with non even division by adding remainder onto last thread's work
				if(i == THREAD_COUNT - 1) {
					nodesPerThread += (NODES % THREAD_COUNT);
				}
				InsertionThread t = new InsertionThread(database, nodesPerThread * i, nodesPerThread);
				t.start();
				threads.add(t);
			}
			for(InsertionThread t : threads) {
				try {
					t.join();
				}
				catch(InterruptedException e) {
					e.printStackTrace();
				}
			}

			System.out.println("Time taken: " + (System.currentTimeMillis() - start) + "ms");
		}
	}

	static class InsertionThread extends Thread {
		private final Database db;
		private final int insertionStart;
		private final int insertionCount;

		InsertionThread(Database db, int insertionStart, int insertionCount) {
			this.db = db;
			this.insertionStart = insertionStart;
			this.insertionCount = insertionCount;
		}

		@Override
		public void run() {
			byte[] value = new byte[] { '.', '.', '.', '.' };
			int done = 0;
			ByteBuffer buf = ByteBuffer.allocate(4);
			Transaction tr = db.createTransaction();
			try {
				while(done < insertionCount) {
					try {
						int i = 0;
						for(; i < BATCH_SIZE && done + i < insertionCount; i++) {
							buf.putInt(0, insertionStart + done + i);
							tr.set(buf.array(), value);
						}
						tr.commit().join();
						tr.close();
						tr = db.createTransaction();
						done += i;
					}
					catch(RuntimeException e) {
						tr = tr.onError(e).join();
					}
				}
			}
			finally {
				tr.close();
			}
		}
	}

	private SerialInsertion() {}
}
