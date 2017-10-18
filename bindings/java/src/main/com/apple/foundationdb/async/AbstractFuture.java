/*
 * AbstractFuture.java
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

package com.apple.foundationdb.async;

import java.util.concurrent.Executor;

/**
 * An abstract implementation of part of the {@link Future} interface. This class should
 *  generally only be useful to the advanced user of the FoundationDB Async library for
 *  creating new types of native-backed futures.
 *
 * @param <T> the type of the eventual value of the {@code Future}
 */
public abstract class AbstractFuture<T> extends AbstractPartialFuture<T> implements Future<T> {
	public AbstractFuture(Executor executor) {
		super(executor);
	}

	@Deprecated
	public AbstractFuture(long cPtr, Executor executor) {
		super(cPtr, executor);
	}

	@Override
	protected abstract T getIfDone();

	@Override
	public T get() {
		blockUntilReady();
		return getIfDone();
	}

	@Override
	public T getInterruptibly() throws InterruptedException {
		blockInterruptibly();
		return getIfDone();
	}

	@Override
	public <V> Future<V> flatMap(final Function<? super T, Future<V>> m) {
		final SettableFuture<V> r = new SettableFuture<V>(getExecutor());
		onReady(new Runnable() {
			@Override
			public void run() {
				final Future<V> rP;
				try {
					rP = m.apply(get());
				} catch(RuntimeException e){
					r.setError(e);
					return;
				} catch(Error e) {
					r.setError(e);
					throw e;
				}
				rP.onReady(new Runnable() {
					@Override
					public void run() {
						V v;
						try {
							v = rP.get();
						} catch (RuntimeException e) {
							r.setError(e);
							return;
						} catch (Error e) {
							r.setError(e);
							throw e;
						}
						r.set(v);
					}
				});
				r.onCancelledCancel(rP);
			}
		});
		r.onCancelledCancel(this);
		return r;
	}

	@Override
	public <V> Future<V> map(final Function<? super T, V> m) {
		final SettableFuture<V> r = new SettableFuture<V>(getExecutor());
		onReady(new Runnable() {
			@Override
			public void run() {
				V mapped;
				try {
					mapped = m.apply(get());
				} catch(RuntimeException e) {
					r.setError(e);
					return;
				} catch(Error e) {
					r.setError(e);
					throw e;
				}
				r.set(mapped);
			}
		});
		r.onCancelledCancel(this);
		return r;
	}

	@Override
	public Future<T> rescueRuntime(final Function<? super RuntimeException, Future<T>> m) {
		final SettableFuture<T> r = new SettableFuture<T>(getExecutor());
		onReady(new Runnable() {
			@Override
			public void run() {
				T t;
				try {
					t = get();
				} catch (Error error) {
					r.setError(error);
					throw error;
				} catch(RuntimeException error) {
					final Future<T> f;
					try {
						f = m.apply(error);
					} catch(RuntimeException e) {
						r.setError(e);
						return;
					} catch(Error e) {
						r.setError(e);
						throw e;
					}
					f.onReady(new Runnable() {
						@Override
						public void run() {
							T t;
							try {
								t = f.get();
							} catch(RuntimeException e) {
								r.setError(e);
								return;
							} catch(Error e) {
								r.setError(e);
								throw e;
							}
							r.set(t);
						}
					});
					r.onCancelledCancel(f);
					return;
				}
				r.set(t);
			}
		});
		r.onCancelledCancel(this);
		return r;
	}
}
