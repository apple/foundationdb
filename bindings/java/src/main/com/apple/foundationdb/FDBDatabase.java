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
import com.apple.foundationdb.tuple.Tuple;

class FDBDatabase extends NativeObjectWrapper implements Database, OptionConsumer {
	private DatabaseOptions options;
	private final Executor executor;
	private final EventKeeper eventKeeper;

	protected FDBDatabase(long cPtr, Executor executor) {
		this(cPtr, executor, null);
	}

	protected FDBDatabase(long cPtr, Executor executor, EventKeeper eventKeeper) {
		super(cPtr);
		this.executor = executor;
		this.options = new DatabaseOptions(this);
		// Automatically set the UsedDuringCommitProtectionDisable option
		// This is because the Java bindings disallow use of Transaction objects after
		// Transaction#onError is called.
		if (FDB.instance().getAPIVersion() >= 730) {
			this.options.setTransactionUsedDuringCommitProtectionDisable();
		}
		this.eventKeeper = eventKeeper;
	}

	@Override
	public DatabaseOptions options() {
		return options;
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
				} catch (Exception err) {
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
			checkUnclosed("Database");
			close();
		}
		finally {
			super.finalize();
		}
	}

	@Override
	public Tenant openTenant(byte[] tenantName, Executor e) {
		return openTenant(tenantName, e, eventKeeper);
	}

	@Override
	public Tenant openTenant(Tuple tenantName) {
		return openTenant(tenantName.pack());
	}

	@Override
	public Tenant openTenant(Tuple tenantName, Executor e) {
		return openTenant(tenantName.pack(), e);
	}

	@Override
	public Tenant openTenant(byte[] tenantName, Executor e, EventKeeper eventKeeper) {
		pointerReadLock.lock();
		Tenant tenant = null;
		try {
			tenant = new FDBTenant(Database_openTenant(getPtr(), tenantName), this, tenantName, e, eventKeeper);
			return tenant;
		} catch (RuntimeException err) {
			if (tenant != null) {
				tenant.close();
			}

			throw err;
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public Tenant openTenant(Tuple tenantName, Executor e, EventKeeper eventKeeper) {
		return openTenant(tenantName.pack(), e, eventKeeper);
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
			tr = new FDBTransaction(Database_createTransaction(getPtr()), this, e, eventKeeper);
			// In newer versions, this option is set as a default option on the database
			if (FDB.instance().getAPIVersion() < 730) {
				tr.options().setUsedDuringCommitProtectionDisable();
			}
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
	public void setOption(int code, byte[] value) {
		pointerReadLock.lock();
		try {
			Database_setOption(getPtr(), code, value);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public double getMainThreadBusyness() {
		pointerReadLock.lock();
		try {
			return Database_getMainThreadBusyness(getPtr());
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public CompletableFuture<byte[]> purgeBlobGranules(byte[] beginKey, byte[] endKey, long purgeVersion, boolean force, Executor e) {
		pointerReadLock.lock();
		try {
			return new FutureKey(Database_purgeBlobGranules(getPtr(), beginKey, endKey, purgeVersion, force), e, eventKeeper);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public CompletableFuture<Void> waitPurgeGranulesComplete(byte[] purgeKey, Executor e) {
		pointerReadLock.lock();
		try {
			return new FutureVoid(Database_waitPurgeGranulesComplete(getPtr(), purgeKey), e);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public CompletableFuture<Boolean> blobbifyRange(byte[] beginKey, byte[] endKey, Executor e) {
		pointerReadLock.lock();
		try {
			return new FutureBool(Database_blobbifyRange(getPtr(), beginKey, endKey), e);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public CompletableFuture<Boolean> blobbifyRangeBlocking(byte[] beginKey, byte[] endKey, Executor e) {
		pointerReadLock.lock();
		try {
			return new FutureBool(Database_blobbifyRangeBlocking(getPtr(), beginKey, endKey), e);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public CompletableFuture<Boolean> unblobbifyRange(byte[] beginKey, byte[] endKey, Executor e) {
		pointerReadLock.lock();
		try {
			return new FutureBool(Database_unblobbifyRange(getPtr(), beginKey, endKey), e);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public CompletableFuture<KeyRangeArrayResult> listBlobbifiedRanges(byte[] beginKey, byte[] endKey, int rangeLimit, Executor e) {
		pointerReadLock.lock();
		try {
			return new FutureKeyRangeArray(Database_listBlobbifiedRanges(getPtr(), beginKey, endKey, rangeLimit), e);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public CompletableFuture<Long> verifyBlobRange(byte[] beginKey, byte[] endKey, long version, Executor e) {
		pointerReadLock.lock();
		try {
			return new FutureInt64(Database_verifyBlobRange(getPtr(), beginKey, endKey, version), e);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public CompletableFuture<Boolean> flushBlobRange(byte[] beginKey, byte[] endKey, boolean compact, long version, Executor e) {
		pointerReadLock.lock();
		try {
			return new FutureBool(Database_flushBlobRange(getPtr(), beginKey, endKey, compact, version), e);
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

	@Override
	public CompletableFuture<byte[]> getClientStatus(Executor e) {
		pointerReadLock.lock();
		try {
			return new FutureKey(Database_getClientStatus(getPtr()), e, eventKeeper);
		} finally {
			pointerReadLock.unlock();
		}
	}

	private native long Database_openTenant(long cPtr, byte[] tenantName);
	private native long Database_createTransaction(long cPtr);
	private native void Database_dispose(long cPtr);
	private native void Database_setOption(long cPtr, int code, byte[] value) throws FDBException;
	private native double Database_getMainThreadBusyness(long cPtr);
	private native long Database_purgeBlobGranules(long cPtr, byte[] beginKey, byte[] endKey, long purgeVersion, boolean force);
	private native long Database_waitPurgeGranulesComplete(long cPtr, byte[] purgeKey);
	private native long Database_blobbifyRange(long cPtr, byte[] beginKey, byte[] endKey);
	private native long Database_blobbifyRangeBlocking(long cPtr, byte[] beginKey, byte[] endKey);
	private native long Database_unblobbifyRange(long cPtr, byte[] beginKey, byte[] endKey);
	private native long Database_listBlobbifiedRanges(long cPtr, byte[] beginKey, byte[] endKey, int rangeLimit);
	private native long Database_verifyBlobRange(long cPtr, byte[] beginKey, byte[] endKey, long version);
	private native long Database_flushBlobRange(long cPtr, byte[] beginKey, byte[] endKey, boolean compact, long version);
	private native long Database_getClientStatus(long cPtr);
}