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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.Function;
import com.apple.foundationdb.async.Future;
import com.apple.foundationdb.async.ReadyFuture;

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
		Database database = FDB.selectAPIVersion(500).open();
		new ConcurrentGetSetGet().apply(database);
	}

	public void apply(Database d) {
		new Thread(new Runnable() {
			@Override
			public void run() {
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
				d.runAsync(new Function<Transaction, Future<Void>>() {
					@Override
					public Future<Void> apply(final Transaction r) {
						attemptCount.addAndGet(1);
						final String key = "test:" + random.nextInt();
						return r.get($(key)).flatMap(new Function<byte[], Future<Void>>() {
							@Override
							public Future<Void> apply(byte[] o) {
								r.set($(key), $("value"));
								return r.get($(key)).map(new Function<byte[], Void>() {
									@Override
									public Void apply(byte[] o) {
										getCompleteCount.addAndGet(1);
										semaphore.release();
										return null;
									}
								});
							}
						}).rescue(new Function<Exception, Future<Void>>() {
							@Override
							public Future<Void> apply(Exception o) {
								errorCount.addAndGet(1);
								System.err.println("Fail (" + o.getMessage() + ")");
								semaphore.release();
								return ReadyFuture.DONE;
							}
						});
					}
/*							@Override
							public void apply(byte[] value) {
								getCompleteCount.addAndGet(1);
								r.set($(key), $("value"));
								r.get($(key)).onSuccess(new Block<byte[]>() {
									@Override
									public void apply(byte[] value) {
										getCompleteCount.addAndGet(1);
										semaphore.release();
									}
								}).onFailure(new Block<Throwable>() {
									@Override
									public void apply(Throwable value) {
										errorCount.addAndGet(1);
										System.err.println("Inner fail (" + value.getMessage() + ")");
										semaphore.release();
									}
								});
							}
						}).onFailure(new Block<Throwable>() {
							@Override
							public void apply(Throwable value) {
								errorCount.addAndGet(1);
								System.err.println("Outer fail (" + value.getMessage() + ")");
								semaphore.release();
							}
						});
						//return ReadyFuture.DONE;
					}
*/				});
			}
			semaphore.acquire(CONCURRENCY);
			long diff = System.currentTimeMillis() - start;
			System.out.println("time taken (ms): " + diff);
			System.out.println("tr/sec:" + COUNT * 1000l / diff);
			System.out.println("attempts: " + attemptCount.get());
			System.out.println("gets complete: " + getCompleteCount.get());
			System.out.println("errors: " + errorCount.get());
			System.out.println();
			// Can be enabled in Database.java
			//System.out.println("db success: " + d.commitSuccessCount.get());
			//System.out.println("db errors: " + d.commitErrorCount.get());
			System.exit(0);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
			System.exit(1);
		}
	}
}
