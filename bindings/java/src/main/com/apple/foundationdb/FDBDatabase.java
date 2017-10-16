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

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.Function;
import com.apple.foundationdb.async.Future;
import com.apple.foundationdb.async.PartialFunction;
import com.apple.foundationdb.async.PartialFuture;

class FDBDatabase extends DefaultDisposableImpl implements Database, Disposable, OptionConsumer {
	private Executor executor;
	private DatabaseOptions options;

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
	public <T> T run(Function<? super Transaction, T> retryable) {
		return this.run(retryable, executor);
	}

	@Override
	public <T> T run(Function<? super Transaction, T> retryable, Executor e) {
		Transaction t = this.createTransaction(e);
		try {
			while (true) {
				try {
					T returnVal = retryable.apply(t);
					t.commit().get();
					return returnVal;
				} catch (RuntimeException err) {
					t = t.onError(err).get();
				}
			}
		} finally {
			t.dispose();
		}
	}

	@Override
	public <T> T read(Function<? super ReadTransaction, T> retryable) {
		return this.read(retryable, executor);
	}

	@Override
	public <T> T read(Function<? super ReadTransaction, T> retryable, Executor e) {
		return this.run(retryable, e);
	}

	@Override
	public <T> T run(PartialFunction<? super Transaction, T> retryable) throws Exception {
		return this.run(retryable, executor);
	}

	@Override
	public <T> T run(PartialFunction<? super Transaction, T> retryable, Executor e) throws Exception {
		Transaction t = this.createTransaction(e);
		try {
			while (true) {
				try {
					T returnVal = retryable.apply(t);
					t.commit().get();
					return returnVal;
				} catch (RuntimeException err) {
					t = t.onError(err).get();
				}
			}
		} finally {
			t.dispose();
		}
	}

	@Override
	public <T> T read(PartialFunction<? super ReadTransaction, T> retryable)
			throws Exception {
		return this.read(retryable, executor);
	}

	@Override
	public <T> T read(PartialFunction<? super ReadTransaction, T> retryable, Executor e)
			throws Exception {
		return this.run(retryable, e);
	}

	@Override
	public <T> Future<T> runAsync(final Function<? super Transaction, Future<T>> retryable) {
		return this.runAsync(retryable, executor);
	}

	@Override
	public <T> Future<T> runAsync(final Function<? super Transaction, Future<T>> retryable, Executor e) {
		final AtomicReference<Transaction> trRef = new AtomicReference<Transaction>(createTransaction(e));
		final AtomicReference<T> returnValue = new AtomicReference<T>();
		return AsyncUtil.whileTrue(new Function<Void, Future<Boolean>>() {
			@Override
			public Future<Boolean> apply(Void v) {
				Future<T> process = AsyncUtil.applySafely(retryable, trRef.get());

				return process.flatMap(new Function<T, Future<Boolean>>() {
					@Override
					public Future<Boolean> apply(final T returnVal) {
						return trRef.get().commit().map(new Function<Void, Boolean>() {
							@Override
							public Boolean apply(Void o)  {
								returnValue.set(returnVal);
								return false;
							}
						});
					}
				}).rescueRuntime(new Function<RuntimeException, Future<Boolean>>() {
					@Override
					public Future<Boolean> apply(RuntimeException err) {
						return trRef.get().onError(err).map(new Function<Transaction, Boolean>() {
						    @Override
							public Boolean apply(final Transaction tr) {
								trRef.set(tr);
								return true;
							}
						});
					}
				});
			}
		}).map(new Function<Void, T>(){
			@Override
			public T apply(Void o) {
				trRef.get().dispose();
				return returnValue.get();
			}
		});
	}

	@Override
	public <T> Future<T> readAsync(
			Function<? super ReadTransaction, Future<T>> retryable) {
		return this.readAsync(retryable, executor);
	}

	@Override
	public <T> Future<T> readAsync(
			Function<? super ReadTransaction, Future<T>> retryable, Executor e) {
		return this.runAsync(retryable, e);
	}

	@Override
	public <T> PartialFuture<T> runAsync(final PartialFunction<? super Transaction, ? extends PartialFuture<T>> retryable) {
		return this.runAsync(retryable, executor);
	}

	@Override
	public <T> PartialFuture<T> runAsync(final PartialFunction<? super Transaction, ? extends PartialFuture<T>> retryable, Executor e) {
		final AtomicReference<Transaction> trRef = new AtomicReference<Transaction>(createTransaction());
		final AtomicReference<T> returnValue = new AtomicReference<T>();
		return AsyncUtil.whileTrue(new Function<Void, PartialFuture<Boolean>>() {
			@Override
			public PartialFuture<Boolean> apply(Void v) {
				PartialFuture<T> process = AsyncUtil.applySafely(retryable, trRef.get());

				return process.flatMap(new Function<T, Future<Boolean>>() {
					@Override
					public Future<Boolean> apply(final T returnVal) {
						return trRef.get().commit().map(new Function<Void, Boolean>() {
							@Override
							public Boolean apply(Void o)  {
								returnValue.set(returnVal);
								return false;
							}
						});
					}
				}).rescue(new Function<Exception, PartialFuture<Boolean>>() {
					@Override
					public PartialFuture<Boolean> apply(Exception err) {
						return trRef.get().onError(err).map(new Function<Transaction, Boolean>() {
							@Override
							public Boolean apply(final Transaction tr) {
								trRef.set(tr);
								return true;
							}
						});
					}
				});
			}
		}).map(new Function<Void, T>(){
			@Override
			public T apply(Void o) {
				trRef.get().dispose();
				return returnValue.get();
			}
		});
	}

	@Override
	public <T> PartialFuture<T> readAsync(
			PartialFunction<? super ReadTransaction, ? extends PartialFuture<T>> retryable) {
		return this.readAsync(retryable, executor);
	}

	@Override
	public <T> PartialFuture<T> readAsync(
			PartialFunction<? super ReadTransaction, ? extends PartialFuture<T>> retryable, Executor e) {
		return this.runAsync(retryable, e);
	}

	@Override
	protected void finalize() throws Throwable {
		dispose();
		super.finalize();
	}

	@Override
	public Transaction createTransaction() {
		return createTransaction(executor);
	}

	@Override
	public Transaction createTransaction(Executor e) {
		pointerReadLock.lock();
		try {
			Transaction tr = new FDBTransaction(Database_createTransaction(getPtr()), this, e);
			tr.options().setUsedDuringCommitProtectionDisable();
			return tr;
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
	protected void disposeInternal(long cPtr) {
		Database_dispose(cPtr);
	}

	private native long Database_createTransaction(long cPtr);
	private native void Database_dispose(long cPtr);
	private native void Database_setOption(long cPtr, int code, byte[] value) throws FDBException;
}