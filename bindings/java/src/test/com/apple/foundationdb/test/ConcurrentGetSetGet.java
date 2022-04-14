/*
 * ConcurrentGetSetGet.java
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

import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

public class ConcurrentGetSetGet {
	public static final Charset UTF8 = Charset.forName("UTF-8");

	final Semaphore semaphore = new Semaphore(CONCURRENCY);
	final AtomicInteger errorCount = new AtomicInteger();
	final AtomicInteger attemptCount = new AtomicInteger();
	final AtomicInteger getCompleteCount = new AtomicInteger();

	// Total trials
	public static final int COUNT = 1000;

	// How many to have outstanding at once
	public static final int CONCURRENCY = 100;

	private static byte[] $(String s) {
		return s.getBytes(UTF8);
	}

	public static void main(String[] args) {
		try(Database database = FDB.selectAPIVersion(720).open()) {
			new ConcurrentGetSetGet().apply(database);
		}
	}

	public void apply(Database db) {
		new Thread(() -> {
			int loops = 0;
			try {
				Thread.sleep(5000);
				System.out.println("Loop " + loops++ + ":");
				System.out.println(" attempts: " + attemptCount.get());
				System.out.println(" gets complete: " + getCompleteCount.get());
				System.out.println(" errors: " + errorCount.get());
				System.out.println(" sem: " + semaphore);
				System.out.println();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}).start();
		final Random random = new SecureRandom();
		try {
			long start = System.currentTimeMillis();
			long current = start;
			for (int i = 0; i < COUNT; i++) {
				semaphore.acquire();
				long wait = System.currentTimeMillis() - current;
				if (wait > 100) {
					System.out.println("Waited " + wait + "ms");
				}
				current = System.currentTimeMillis();
				db.runAsync(tr -> {
					attemptCount.addAndGet(1);
					final String key = "test:" + random.nextInt();
					return tr.get($(key)).thenComposeAsync(ignore -> {
						tr.set($(key), $("value"));
						return tr.get($(key)).thenRunAsync(() -> {
							getCompleteCount.addAndGet(1);
							semaphore.release();
						}, FDB.DEFAULT_EXECUTOR);
					}, FDB.DEFAULT_EXECUTOR).exceptionally(t -> {
						errorCount.addAndGet(1);
						System.err.println("Fail (" + t.getMessage() + ")");
						semaphore.release();
						return null;
					});
				});
			}
			semaphore.acquire(CONCURRENCY);
			long diff = System.currentTimeMillis() - start;
			System.out.println("time taken (ms): " + diff);
			System.out.println("tr/sec:" + COUNT * 1000L / diff);
			System.out.println("attempts: " + attemptCount.get());
			System.out.println("gets complete: " + getCompleteCount.get());
			System.out.println("errors: " + errorCount.get());
			System.out.println();
			// Can be enabled in Database.java
			//System.out.println("db success: " + db.commitSuccessCount.get());
			//System.out.println("db errors: " + db.commitErrorCount.get());
			System.exit(0);
		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(1);
		}
	}
}
