/*
 * FDBTransaction.java
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

import java.util.concurrent.Executor;

import com.apple.foundationdb.async.*;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.Future;
import com.apple.foundationdb.async.PartialFunction;
import com.apple.foundationdb.async.PartialFuture;
import com.apple.foundationdb.async.ReadyFuture;

class FDBTransaction extends DefaultDisposableImpl implements Disposable, Transaction, OptionConsumer {
	private final Executor executor;
	private final Database database;
	private final TransactionOptions options;

	private boolean transactionOwner;

	public final ReadTransaction snapshot;

	class ReadSnapshot implements ReadTransaction {
		@Override
		public Future<Long> getReadVersion() {
			return FDBTransaction.this.getReadVersion();
		}

		@Override
		public Future<byte[]> get(byte[] key) {
			return get_internal(key, true);
		}

		@Override
		public Future<byte[]> getKey(KeySelector selector) {
			return getKey_internal(selector, true);
		}

		///////////////////
		//  getRange -> KeySelectors
		///////////////////
		@Override
		public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end,
				int limit, boolean reverse, StreamingMode mode) {
			return RangeQuery.start(FDBTransaction.this, true, begin, end, limit, reverse, mode);
		}
		@Override
		public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end,
				int limit, boolean reverse) {
			return getRange(begin, end, limit, reverse, StreamingMode.ITERATOR);
		}
		@Override
		public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end,
				int limit) {
			return getRange(begin, end, limit, false);
		}
		@Override
		public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end) {
			return getRange(begin, end, ReadTransaction.ROW_LIMIT_UNLIMITED);
		}

		///////////////////
		//  getRange -> byte[]s
		///////////////////
		@Override
		public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end,
				int limit, boolean reverse, StreamingMode mode) {
			return getRange(KeySelector.firstGreaterOrEqual(begin),
					KeySelector.firstGreaterOrEqual(end),
					limit, reverse, mode);
		}
		@Override
		public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end,
				int limit, boolean reverse) {
			return getRange(begin, end, limit, reverse, StreamingMode.ITERATOR);
		}
		@Override
		public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end,
				int limit) {
			return getRange(begin, end, limit, false);
		}
		@Override
		public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end) {
			return getRange(begin, end, ReadTransaction.ROW_LIMIT_UNLIMITED);
		}

		///////////////////
		//  getRange (Range)
		///////////////////
		@Override
		public AsyncIterable<KeyValue> getRange(Range range,
				int limit, boolean reverse, StreamingMode mode) {
			return getRange(range.begin, range.end, limit, reverse, mode);
		}
		@Override
		public AsyncIterable<KeyValue> getRange(Range range,
				int limit, boolean reverse) {
			return getRange(range, limit, reverse, StreamingMode.ITERATOR);
		}
		@Override
		public AsyncIterable<KeyValue> getRange(Range range,
				int limit) {
			return getRange(range, limit, false);
		}
		@Override
		public AsyncIterable<KeyValue> getRange(Range range) {
			return getRange(range, ReadTransaction.ROW_LIMIT_UNLIMITED);
		}

		@Override
		public TransactionOptions options() {
			return FDBTransaction.this.options();
		}

		@Override
		public <T> T read(Function<? super ReadTransaction, T> retryable) {
			return retryable.apply(this);
		}

		@Override
		public <T> T read(PartialFunction<? super ReadTransaction, T> retryable)
				throws Exception {
			return retryable.apply(this);
		}

		@Override
		public <T> Future<T> readAsync(
				Function<? super ReadTransaction, Future<T>> retryable) {
			return AsyncUtil.applySafely(retryable, this);
		}

		@Override
		public <T> PartialFuture<T> readAsync(
				PartialFunction<? super ReadTransaction, ? extends PartialFuture<T>> retryable) {
			return AsyncUtil.applySafely(retryable, this);
		}

		@Override
		public Executor getExecutor() {
			return FDBTransaction.this.getExecutor();
		}
	}

	protected FDBTransaction(long cPtr, Database database, Executor executor) {
		super(cPtr);
		this.database = database;
		this.executor = executor;
		snapshot = new ReadSnapshot();
		options = new TransactionOptions(this);
		transactionOwner = true;
	}

	@Override
	public ReadTransaction snapshot() {
		return snapshot;
	}

	@Override
	public TransactionOptions options() {
		return options;
	}

	@Override
	public void setReadVersion(long version) {
		pointerReadLock.lock();
		try {
			Transaction_setVersion(getPtr(), version);
		} finally {
			pointerReadLock.unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Future<Long> getReadVersion() {
		pointerReadLock.lock();
		try {
			return new FutureVersion( Transaction_getReadVersion(getPtr()), this.executor );
		} finally {
			pointerReadLock.unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Future<byte[]> get(byte[] key) {
		return get_internal(key, false);
	}

	private Future<byte[]> get_internal(byte[] key, boolean isSnapshot) {
		pointerReadLock.lock();
		try {
			return new FutureResult( Transaction_get(getPtr(), key, isSnapshot), this.executor );
		} finally {
			pointerReadLock.unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Future<byte[]> getKey(KeySelector selector) {
		return getKey_internal(selector, false);
	}

	private Future<byte[]> getKey_internal(KeySelector selector, boolean isSnapshot) {
		pointerReadLock.lock();
		try {
			return new FutureKey( Transaction_getKey(getPtr(),
					selector.getKey(), selector.orEqual(), selector.getOffset(), isSnapshot), this.executor );
		} finally {
			pointerReadLock.unlock();
		}
	}

	///////////////////
	//  getRange -> KeySelectors
	///////////////////
	@Override
	public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end,
			int limit, boolean reverse, StreamingMode mode) {
		return RangeQuery.start(this, false, begin, end, limit, reverse, mode);
	}
	@Override
	public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end,
			int limit, boolean reverse) {
		return getRange(begin, end, limit, reverse, StreamingMode.ITERATOR);
	}
	@Override
	public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end,
			int limit) {
		return getRange(begin, end, limit, false);
	}
	@Override
	public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end) {
		return getRange(begin, end, ReadTransaction.ROW_LIMIT_UNLIMITED);
	}

	///////////////////
	//  getRange -> byte[]s
	///////////////////
	@Override
	public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end,
			int limit, boolean reverse, StreamingMode mode) {
		return getRange(KeySelector.firstGreaterOrEqual(begin),
				KeySelector.firstGreaterOrEqual(end),
				limit, reverse, mode);
	}
	@Override
	public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end,
			int limit, boolean reverse) {
		return getRange(begin, end, limit, reverse, StreamingMode.ITERATOR);
	}
	@Override
	public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end,
			int limit) {
		return getRange(begin, end, limit, false);
	}
	@Override
	public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end) {
		return getRange(begin, end, ReadTransaction.ROW_LIMIT_UNLIMITED);
	}

	///////////////////
	//  getRange (Range)
	///////////////////
	@Override
	public AsyncIterable<KeyValue> getRange(Range range,
			int limit, boolean reverse, StreamingMode mode) {
		return getRange(range.begin, range.end, limit, reverse, mode);
	}
	@Override
	public AsyncIterable<KeyValue> getRange(Range range,
			int limit, boolean reverse) {
		return getRange(range, limit, reverse, StreamingMode.ITERATOR);
	}
	@Override
	public AsyncIterable<KeyValue> getRange(Range range,
			int limit) {
		return getRange(range, limit, false);
	}
	@Override
	public AsyncIterable<KeyValue> getRange(Range range) {
		return getRange(range, ReadTransaction.ROW_LIMIT_UNLIMITED);
	}

	@Override
	public Database getDatabase() {
		return database;
	}

	protected FutureResults getRange_internal(
			KeySelector begin, KeySelector end,
			int rowLimit, int targetBytes, int streamingMode,
			int iteration, boolean isSnapshot, boolean reverse) {
		pointerReadLock.lock();
		try {
			/*System.out.println(String.format(
					" -- range get: (%s, %s) limit: %d, bytes: %d, mode: %d, iteration: %d, snap: %s, reverse %s",
				begin.toString(), end.toString(), rowLimit, targetBytes, streamingMode,
				iteration, Boolean.toString(isSnapshot), Boolean.toString(reverse)));*/
			return new FutureResults(Transaction_getRange(
					getPtr(), begin.getKey(), begin.orEqual(), begin.getOffset(),
					end.getKey(), end.orEqual(), end.getOffset(), rowLimit, targetBytes,
					streamingMode, iteration, isSnapshot, reverse), this.executor);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public void addReadConflictRange(byte[] keyBegin, byte[] keyEnd) {
		addConflictRange(keyBegin, keyEnd, ConflictRangeType.READ);
	}

	@Override
	public void addReadConflictKey(byte[] key) {
		addConflictRange(key, ByteArrayUtil.join(key, new byte[]{(byte) 0}), ConflictRangeType.READ);
	}

	@Override
	public void addWriteConflictRange(byte[] keyBegin, byte[] keyEnd) {
		addConflictRange(keyBegin, keyEnd, ConflictRangeType.WRITE);
	}

	@Override
	public void addWriteConflictKey(byte[] key) {
		addConflictRange(key, ByteArrayUtil.join(key, new byte[] { (byte)0 }), ConflictRangeType.WRITE);
	}

	private void addConflictRange(byte[] keyBegin, byte[] keyEnd,
			ConflictRangeType type) {
		pointerReadLock.lock();
		try {
			Transaction_addConflictRange(getPtr(), keyBegin, keyEnd, type.code());
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public <T> T run(Function<? super Transaction, T> retryable) {
		return retryable.apply(this);
	}

	@Override
	public <T> T run(PartialFunction<? super Transaction, T> retryable) throws Exception {
		return retryable.apply(this);
	}

	@Override
	public <T> Future<T> runAsync(
			Function<? super Transaction, Future<T>> retryable) {
		return AsyncUtil.applySafely(retryable, this);
	}

	@Override
	public <T> PartialFuture<T> runAsync(
			PartialFunction<? super Transaction, ? extends PartialFuture<T>> retryable) {
		return AsyncUtil.applySafely(retryable, this);
	}

	@Override
	public <T> T read(Function<? super ReadTransaction, T> retryable) {
		return retryable.apply(this);
	}

	@Override
	public <T> T read(PartialFunction<? super ReadTransaction, T> retryable)
			throws Exception {
		return retryable.apply(this);
	}

	@Override
	public <T> Future<T> readAsync(
			Function<? super ReadTransaction, Future<T>> retryable) {
		return AsyncUtil.applySafely(retryable, this);
	}

	@Override
	public <T> PartialFuture<T> readAsync(
			PartialFunction<? super ReadTransaction, ? extends PartialFuture<T>> retryable) {
		return AsyncUtil.applySafely(retryable, this);
	}

	@Override
	public void set(byte[] key, byte[] value) {
		if(key == null || value == null)
			throw new IllegalArgumentException("Keys/Values must be non-null");
		pointerReadLock.lock();
		try {
			Transaction_set(getPtr(), key, value);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public void clear(byte[] key) {
		if(key == null)
			throw new IllegalArgumentException("Key cannot be null");
		pointerReadLock.lock();
		try {
			Transaction_clear(getPtr(), key);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public void clear(byte[] beginKey, byte[] endKey) {
		if(beginKey == null || endKey == null)
			throw new IllegalArgumentException("Keys cannot be null");
		pointerReadLock.lock();
		try {
			Transaction_clear(getPtr(), beginKey, endKey);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	@Deprecated
	public void clearRangeStartsWith(byte[] prefix) {
		clear(Range.startsWith(prefix));
	}

	@Override
	public void clear(Range range) {
		clear(range.begin, range.end);
	}

	@Override
	public void mutate(MutationType optype, byte[] key, byte[] value) {
		pointerReadLock.lock();
		try {
			Transaction_mutate(getPtr(), optype.code(), key, value);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public void setOption(int code, byte[] param) {
		pointerReadLock.lock();
		try {
			Transaction_setOption(getPtr(), code, param);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public Future<Void> commit() {
		pointerReadLock.lock();
		try {
			return new FutureVoid(Transaction_commit(getPtr()), this.executor);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public Long getCommittedVersion() {
		pointerReadLock.lock();
		try {
			return Transaction_getCommittedVersion(getPtr());
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public Future<byte[]> getVersionstamp() {
		pointerReadLock.lock();
		try {
			return new FutureKey( Transaction_getVersionstamp(getPtr()), this.executor );
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public Future<Void> watch(byte[] key) throws FDBException {
		pointerReadLock.lock();
		try {
			return new FutureVoid(Transaction_watch(getPtr(), key), this.executor);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public Future<Transaction> onError(RuntimeException e) {
		if(!(e instanceof FDBException)) {
			return new ReadyFuture<Transaction>(e, getExecutor());
		}
		pointerReadLock.lock();
		try {
			FutureVoid f = new FutureVoid(Transaction_onError(getPtr(), ((FDBException)e).getCode()), this.executor);
			final Transaction tr = transfer();
			return f.map(new Function<Void, Transaction>() {
				@Override
				public Transaction apply(Void o) {
					return tr;
				}
			})
			.rescueRuntime(new Function<RuntimeException, Future<Transaction>>() {
				@Override
				public Future<Transaction> apply(RuntimeException e) {
					tr.dispose();
					throw e;
				}
			});
		} finally {
			pointerReadLock.unlock();
			if(!transactionOwner) {
				dispose();
			}
		}
	}

	@Override
	public PartialFuture<Transaction> onError(Exception e) {
		if (!(e instanceof RuntimeException))
			return new ReadyPartialFuture<Transaction>(e, getExecutor());

		return onError((RuntimeException)e);
	}

	@Override
	public void cancel() {
		pointerReadLock.lock();
		try {
			Transaction_cancel(getPtr());
		} finally {
			pointerReadLock.unlock();
		}
	}

	public Future<String[]> getAddressesForKey(byte[] key) {
		pointerReadLock.lock();
		try {
			return new FutureStrings(Transaction_getKeyLocations(getPtr(), key), this.executor);
		} finally {
			pointerReadLock.unlock();
		}
	}

	@Override
	public Executor getExecutor() {
		return executor;
	}

	// Must hold pointerReadLock when calling
	private FDBTransaction transfer() {
		FDBTransaction tr = null;
		try {
			tr = new FDBTransaction(getPtr(), database, executor);
			tr.options().setUsedDuringCommitProtectionDisable();
			transactionOwner = false;
			return tr;
		}
		catch(RuntimeException err) {
			if(tr != null) {
				tr.dispose();
			}

			throw err;
		}
	}

	@Override
	protected long getPtr() {
		if(!transactionOwner) {
			throw new IllegalStateException("Transaction has been invalidated by reset");
		}
		else {
			return super.getPtr();
		}
	}

	@Override
	protected void finalize() throws Throwable {
		dispose();
	}

	@Override
	protected void disposeInternal(long cPtr) {
		if(transactionOwner) {
			Transaction_dispose(cPtr);
		}
	}

	private native long Transaction_getReadVersion(long cPtr);
	private native  void Transaction_setVersion(long cPtr, long version);
	private native long Transaction_get(long cPtr, byte[] key, boolean isSnapshot);
	private native  long Transaction_getKey(long cPtr, byte[] key, boolean orEqual,
			int offset, boolean isSnapshot);
	private native long Transaction_getRange(long cPtr,
			byte[] keyBegin, boolean orEqualBegin, int offsetBegin,
			byte[] keyEnd, boolean orEqualEnd, int offsetEnd,
			int rowLimit, int targetBytes, int streamingMode, int iteration,
			boolean isSnapshot, boolean reverse);
	private native void Transaction_addConflictRange(long cPtr,
			byte[] keyBegin, byte[] keyEnd, int conflictRangeType);
	private native void Transaction_set(long cPtr, byte[] key, byte[] value);
	private native void Transaction_clear(long cPtr, byte[] key);
	private native void Transaction_clear(long cPtr, byte[] beginKey, byte[] endKey);
	private native void Transaction_mutate(long ptr, int code, byte[] key, byte[] value);
	private native void Transaction_setOption(long cPtr, int code, byte[] value) throws FDBException;
	private native long Transaction_commit(long cPtr);
	private native long Transaction_getCommittedVersion(long cPtr);
	private native long Transaction_getVersionstamp(long cPtr);
	private native long Transaction_onError(long cPtr, int errorCode);
	private native void Transaction_dispose(long cPtr);
	private native void Transaction_reset(long cPtr);
	private native long Transaction_watch(long ptr, byte[] key) throws FDBException;
	private native void Transaction_cancel(long cPtr);
	private native long Transaction_getKeyLocations(long cPtr, byte[] key);
}
