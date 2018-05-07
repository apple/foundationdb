/*
 * FDBDatabase.java
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

package com.apple.foundationdb;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.apple.foundationdb.async.AsyncUtil;

class FDBDatabase extends NativeObjectWrapper implements Database, OptionConsumer {
	private DatabaseOptions options;
	private final Executor executor;

	protected FDBDatabase(long cPtr, Executor executor) {
		super(cPtr);
		this.executor = executor;
		this.options = new DatabaseOptions(this);
	}

	@Override
	public DatabaseOptions options() {
		return options;
	}

	@Override
	public <T> T run(Function<? super Transaction, T> retryable, Executor e) {
		Transaction t = this.createTransaction();
		try {
			while (true) {
				try {
					T returnVal = retryable.apply(t);
					t.commit().join();
					return returnVal;
				} catch (RuntimeException err) {
					t = t.onError(err).join();
				}
			}
		} finally {
			t.close();
		}
	}

	@Override
	public <T> T read(Function<? super ReadTransaction, T> retryable, Executor e) {
		return this.run(retryable);
	}

	@Override
	public <T> CompletableFuture<T> runAsync(final Function<? super Transaction, ? extends CompletableFuture<T>> retryable, Executor e) {
		final AtomicReference<Transaction> trRef = new AtomicReference<>(createTransaction(e));
		final AtomicReference<T> returnValue = new AtomicReference<>();
		return AsyncUtil.whileTrue(() -> {
			CompletableFuture<T> process = AsyncUtil.applySafely(retryable, trRef.get());

			return AsyncUtil.composeHandleAsync(process.thenComposeAsync(returnVal ->
				trRef.get().commit().thenApply(o -> {
					returnValue.set(returnVal);
					return false;
				}), e),
				(value, t) -> {
					if(t == null)
						return CompletableFuture.completedFuture(value);
					if(!(t instanceof RuntimeException))
						throw new CompletionException(t);
					return trRef.get().onError(t).thenApply(newTr -> {
						trRef.set(newTr);
						return true;
					});
				}, e);
		}, e)
		.thenApply(o -> returnValue.get())
		.whenComplete((v, t) -> trRef.get().close());
	}

	@Override
	public <T> CompletableFuture<T> readAsync(
			Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable, Executor e) {
		return this.runAsync(retryable, e);
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			checkUnclosed("Database");
			close();
		}
		finally {
			super.finalize();
		}
	}

	@Override
	public Transaction createTransaction(Executor e) {
		pointerReadLock.lock();
		Transaction tr = null;
		try {
			tr = new FDBTransaction(Database_createTransaction(getPtr()), this, e);
			tr.options().setUsedDuringCommitProtectionDisable();
			return tr;
		} catch(RuntimeException err) {
			if(tr != null) {
				tr.close();
			}

			throw err;
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public void setOption(int code, byte[] value) {
		pointerReadLock.lock();
		try {
			Database_setOption(getPtr(), code, value);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public Executor getExecutor() {
		return executor;
	}

	@Override
	protected void closeInternal(long cPtr) {
		Database_dispose(cPtr);
	}

	private native long Database_createTransaction(long cPtr);
	private native void Database_dispose(long cPtr);
	private native void Database_setOption(long cPtr, int code, byte[] value) throws FDBException;
}