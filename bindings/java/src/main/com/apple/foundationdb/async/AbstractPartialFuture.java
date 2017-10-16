/*
 * AbstractPartialFuture.java
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

import java.util.LinkedList;
import java.util.concurrent.Executor;

import com.apple.foundationdb.Disposable;
import com.apple.foundationdb.FDBException;

/**
 * An abstract implementation of part of the {@link PartialFuture} interface. This class should
 *  generally only be useful to the advanced user of the FoundationDB Async library for
 *  creating new types of native-backed futures.
 *
 * @param <T> the type of the eventual value of the {@code PartialFuture}
 */
public abstract class AbstractPartialFuture<T> implements Disposable, PartialFuture<T> {
	private boolean isCallbackRegistered = false;

	private boolean hasFired = false;

	private LinkedList<Runnable> runnables;

	private final Executor executor;

	public AbstractPartialFuture(Executor executor) {
		this.executor = executor;
	}

	@Deprecated
	public AbstractPartialFuture(long cPtr, Executor executor) {
		if(cPtr != 0)
			throw new IllegalArgumentException("Cannot construct with pointer");
		this.executor = executor;
	}

	/**
	 * To be called only once. Called synchronously if set.
	 */
	protected abstract void registerSingleCallback(Runnable callback);

	/**
	 * Gets the value from this {@code Future}.  It must be in the ready and non-error
	 *  state.  This is a non-blocking call.  If this {@code Future} is set to
	 *  an error {@code get()} throws this error.
	 *
	 * @return the value to which this {@code Future} has been set.
	 * @throws FDBException if the value is either unset, or has been set to an
	 *  error.
	 */
	protected abstract T getIfDone() throws Exception;

	/**
	 * If the returns false, guarantee that a callback added in the same synchronized block
	 *  will be called when the future is set.
	 */
	private boolean setupCallback() {
		// assert that we were only called when already synchronized on "this"
		if(!Thread.holdsLock(this))
			throw new IllegalStateException("called without external synchronization");

		if(isCallbackRegistered)
			return hasFired;

		this.runnables = new LinkedList<Runnable>();

		registerSingleCallback(new Runnable() {
			@Override
			public void run() {
				synchronized (AbstractPartialFuture.this) {
					if (!isCallbackRegistered) {
						hasFired = true;
						return;
					}
				}
				AbstractPartialFuture.this.executeCallbacks();
			}
		});
		isCallbackRegistered = true;
		return hasFired; 	// this is most likely be false, but if the executor was
							//  synchronous and the future already set, we could have already
							//  done the callback.
	}

	private void executeCallbacks() {
		LinkedList<Runnable> runnables;

		synchronized (this) {
			runnables = this.runnables;
			this.runnables = null;
			hasFired = true;
		}

		for(Runnable cbp : runnables) {
			 this.executor.execute(cbp);
		}
	}

	@Override
	public void onReady(final Runnable r) {
		boolean shouldRun = false;
		synchronized (this) {
			if(setupCallback())
				shouldRun = true;
			else
				runnables.add(r);
		}
		if(shouldRun) {
			this.executor.execute(new Runnable() {
				@Override
				public void run() {
					r.run();
				}
			});
		}
	}

	@Override
	public boolean onReadyAlready(final Runnable r) {
		synchronized (this) {
			if(setupCallback())
				return true;
			runnables.add(r);
			return false;
		}
	}

	@Override
	public <V> PartialFuture<V> flatMap(final PartialFunction<? super T, ? extends PartialFuture<V>> m) {
		final SettablePartialFuture<V> r = new SettablePartialFuture<V>(getExecutor());
		onReady(new Runnable() {
			@Override
			public void run() {
				final PartialFuture<V> rP;
				try {
					rP = m.apply(get());
				} catch(Exception e){
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
						} catch (Exception e) {
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
	public <V> PartialFuture<V> map(final PartialFunction<? super T, V> m) {
		final SettablePartialFuture<V> r = new SettablePartialFuture<V>(getExecutor());
		onReady(new Runnable() {
			@Override
			public void run() {
				V mapped;
				try {
					mapped = m.apply(get());
				} catch(Exception e) {
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
	public Future<T> rescue(final Function<? super Exception, Future<T>> m) {
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
				} catch(Exception error) {
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

	@Override
	public PartialFuture<T> rescue(
			final PartialFunction<? super Exception, ? extends PartialFuture<T>> m) {
		final SettablePartialFuture<T> r = new SettablePartialFuture<T>(getExecutor());
		onReady(new Runnable() {
			@Override
			public void run() {
				T t;
				try {
					t = get();
				} catch (Error error) {
					r.setError(error);
					throw error;
				} catch(Exception error) {
					final PartialFuture<T> f;
					try {
						f = m.apply(error);
					} catch(Exception e) {
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
							} catch(Exception e) {
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

	@Override
	public T get() throws Exception {
		blockUntilReady();
		return getIfDone();
	}

	@Override
	public T getInterruptibly() throws Exception {
		blockInterruptibly();
		return getIfDone();
	}

	@Override
	public void dispose() {
		cancel();
	}

	protected Executor getExecutor() {
		return this.executor;
	}

	/**
	 * A runnable that, when run, calls {@code notify()} on itself.
	 *
	 */
	protected static final class SelfNotifier implements Runnable {
		public SelfNotifier() {}

		@Override
		public void run() {
			synchronized (this) {
				this.notify();
			}
		}
	}
}
