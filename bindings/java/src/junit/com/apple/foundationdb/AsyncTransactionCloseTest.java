/*
 * AsyncTransactionCloseTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

/**
 * Tests that transactions opened by `Database#runAsync` are always closed.
 *
 * These tests do _not_ require a running FDB server to function. Instead, we
 * are operating on a good-faith "The underlying native library is correct"
 * functionality. For end-to-end tests which require a running server, see the
 * src/tests source folder.
 */
class AsyncTransactionCloseTest {
	private static Executor EXECUTOR = new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		};

	private static Database makeFakeDatabase(final AtomicBoolean transactionClosed) {
		final AtomicReference<Database> db = new AtomicReference<>();

		db.set(new FDBDatabase(0, EXECUTOR) {
			@Override
			public Transaction createTransaction(final Executor e) {
				return new FakeFDBTransaction(List.<KeyValue>of(), 0, db.get(), e) {
					@Override
					public void close() {
						transactionClosed.set(true);
					}
				};
			}

			@Override
			public void setOption(int code, byte[] value) {
			}
		});

		return db.get();
	}

	@Test
	public void asyncTransactionClose() {
		final AtomicBoolean transactionClosed = new AtomicBoolean(false);

		final Database db = makeFakeDatabase(transactionClosed);
		final CompletableFuture<Void> someLongOperation = new CompletableFuture<>();
		final CompletableFuture<Void> runFuture = db.runAsync(t -> someLongOperation);
		runFuture.cancel(true);
		someLongOperation.complete(null);

		assertTrue(transactionClosed.get());
	}
}
