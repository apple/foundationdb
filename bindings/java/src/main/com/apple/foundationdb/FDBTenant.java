/*
 * FDBTenant.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.tuple.ByteArrayUtil;

class FDBTenant extends NativeObjectWrapper implements Tenant {
	private final Database database;
	private final byte[] name;
	private final Executor executor;
	private final EventKeeper eventKeeper;

	protected FDBTenant(long cPtr, Database database, byte[] name, Executor executor) {
		this(cPtr, database, name, executor, null);
	}

	protected FDBTenant(long cPtr, Database database, byte[] name, Executor executor, EventKeeper eventKeeper) {
		super(cPtr);
		this.database = database;
		this.name = name;
		this.executor = executor;
		this.eventKeeper = eventKeeper;
	}

	@Override
	public <T> T run(Function<? super Transaction, T> retryable, Executor e) {
		Transaction t = this.createTransaction(e);
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
		return this.run(retryable, e);
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
			checkUnclosed("Tenant");
			close();
		}
		finally {
			super.finalize();
		}
	}

	@Override
	public Transaction createTransaction(Executor e) {
		return createTransaction(e, eventKeeper);
	}

	@Override
	public Transaction createTransaction(Executor e, EventKeeper eventKeeper) {
		pointerReadLock.lock();
		Transaction tr = null;
		try {
			tr = new FDBTransaction(Tenant_createTransaction(getPtr()), database, e, eventKeeper);
			tr.options().setUsedDuringCommitProtectionDisable();
			return tr;
		} catch (RuntimeException err) {
			if (tr != null) {
				tr.close();
			}

			throw err;
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public byte[] getName() {
		return name;
	}

	@Override
	public Executor getExecutor() {
		return executor;
	}

	@Override
	protected void closeInternal(long cPtr) {
		Tenant_dispose(cPtr);
	}

	private native long Tenant_createTransaction(long cPtr);
	private native void Tenant_dispose(long cPtr);
}