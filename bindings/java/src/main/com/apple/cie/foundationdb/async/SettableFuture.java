/*
 * SettableFuture.java
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

package com.apple.cie.foundationdb.async;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

import com.apple.cie.foundationdb.FDBException;

/**
 * Represents an {@link Future} that can be fulfilled via the {@link Settable} interface.
 *
 *  {@code SettableFuture} has a default {@code Executor} that is used when a
 *  {@code SettableFuture} is created without passing this parameter, which runs
 *  callback functions immediately on the stack when the future is fulfilled.
 *
 * @see ReadyFuture
 *
 * @param <T> the output type of a successful process
 */
public class SettableFuture<T> extends AbstractFuture<T> implements Settable<T> {
	private static final Executor DEFAULT_ES = new Executor() {
		@Override
		public void execute(Runnable command) {
			command.run();
		}
	};

	/**
	 * Construct a {@code SettableFuture} with the default {@code Executor}
	 */
	public SettableFuture() {
		this(DEFAULT_ES);
	}

	/**
	 * Construct a {@code SettableFuture} with a specified {@code Executor}
	 *
	 * @param executor the {@code Executor} with which to execute callbacks
	 */
	public SettableFuture(Executor executor) {
		super(executor);
	}

	private List<Runnable> cancelCallbacks = null;
	private List<Cancellable> chainedCancels = null;

	private volatile boolean isSet = false;
	private volatile boolean isError = false;
	private volatile boolean isCancelled = false;
	private T value = null;
	private Throwable error = null;

	private Runnable callback = null;

	@Override
	public void set(T value) {
		Runnable localCb = null;
		synchronized(this) {
			if(isCancelled) {
				return;
			}
			if(isSet()) {
				throw new IllegalStateException("Settable already set");
			}
			this.isSet = true;
			this.isError = false;
			this.value = value;
			localCb = this.callback;
			this.callback = null;
			this.cancelCallbacks = null;
			this.chainedCancels = null;
		}
		if(localCb != null)
			localCb.run();
	}

	@Override
	public void setError(Error error) {
		setErrorInternal(error);
	}

	@Override
	public void setError(RuntimeException error) {
		setErrorInternal(error);
	}

	private void setErrorInternal(Throwable error) {
		Runnable localCb = null;
		synchronized(this) {
			if(isCancelled) {
				return;
			}
			if(isSet()) {
				throw new IllegalStateException("Settable already set");
			}
			this.isSet = true;
			this.isError = true;
			this.error = error;
			localCb = this.callback;
			this.callback = null;
			this.cancelCallbacks = null;
			this.chainedCancels = null;
		}
		if(localCb != null)
			localCb.run();
	}

	@Override
	public void cancel() {
		Runnable localCb = null;
		List<Runnable> cancelCbs = null;
		List<Cancellable> cancels = null;
		synchronized(this) {
			if(isSet()) {
				return;
			}
			this.isSet = true;
			this.isError = true;
			this.isCancelled = true;
			this.error = (new FDBException("operation_cancelled", 1101));
			localCb = this.callback;
			cancelCbs = this.cancelCallbacks;
			cancels = this.chainedCancels;
			this.callback = null;
			this.cancelCallbacks = null;
			this.chainedCancels = null;
		}
		if(cancelCbs != null) {
			for(Runnable r : cancelCbs) {
				getExecutor().execute(r);
			}
		}
		if(cancels != null) {
			for(Cancellable c : cancels) {
				c.cancel();
			}
		}
		if(localCb != null)
			localCb.run();
	}


	@Override
	public synchronized boolean isSet() {
		return isSet;
	}


	@Override
	public synchronized boolean onCancelled(Runnable r) {
		if(isCancelled) {
			r.run();
			return true;
		}
		if(isSet()) {
			return false;
		}
		if(this.cancelCallbacks == null) {
			this.cancelCallbacks = new LinkedList<Runnable>();
		}
		this.cancelCallbacks.add(r);
		return true;
	}

	@Override
	public synchronized void onCancelledCancel(Cancellable c) {
		if(isCancelled) {
			c.cancel();
			return;
		}
		if(isSet()) {
			return;
		}
		if(this.chainedCancels == null) {
			this.chainedCancels = new LinkedList<Cancellable>();
		}
		this.chainedCancels.add(c);
	}

	@Override
	public T getIfDone() {
		if(!isDone())
			throw new IllegalStateException("Future has not been set");

		if(isError()) {
			if (error instanceof Error) throw (Error)error;
			if(error instanceof CloneableException) {
				throw (RuntimeException)(((CloneableException)error).retargetClone());
			}
			throw (RuntimeException)error;
		}

		return value;
	}

	@Override
	public void blockInterruptibly() throws InterruptedException {
		if(isDone())
			return;

		Runnable trigger = new SelfNotifier();
		onReady(trigger);

		synchronized (trigger) {
			while(!isDone()) {
				trigger.wait();
			}
		}
	}

	@Override
	public void blockUntilReady() {
		if(isDone())
			return;

		Runnable trigger = new SelfNotifier();
		onReady(trigger);

		boolean interrupted = false;
		synchronized (trigger) {
			while(!isDone()) {
				try {
					trigger.wait();
				} catch (InterruptedException e) {
					interrupted = true;
				}
			}
		}
		if(interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public boolean isDone() {
		return isSet();
	}

	@Override
	public synchronized boolean isError() {
		if(!isDone()) {
			throw new IllegalStateException("Not yet set");
		}
		return isError;
	}

	@Override
	public synchronized Throwable getError() {
		if(!isError())
			throw new IllegalStateException("Not in an error state");
		return this.error;
	}

	@Override
	protected synchronized void registerSingleCallback(Runnable callback) {
		if(isDone()) {
			callback.run();
			return;
		}
		if(this.callback != null)
			throw new IllegalStateException("Callback already registered");
		this.callback = callback;
	}
}
