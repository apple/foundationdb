/*
 * ReadyPartialFuture.java
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

import java.util.concurrent.Executor;

/**
 * A {@code PartialFuture} that is always in the "set" state. No call on a {@code ReadyPartialFuture}
 *  will block, since this is either a value or an error from its time of creation. A
 *  {@code ReadyPartialFuture} is useful for returning the result of an asynchronous process.<br>
 *  <br>
 *  If code needs to return a {@code Future<Void>} to indicate completion, {@link ReadyFuture#DONE}
 *  should be used.
 *
 * @param <T> the type of value that can be set
 */
public class ReadyPartialFuture<T> implements PartialFuture<T> {
	private static final Executor DEFAULT_ES = new Executor() {
		@Override
		public void execute(Runnable command) {
			command.run();
		}
	};

	protected final Executor executor;
	private final boolean isError;
	private final T value;
	private final Throwable error;

	/**
	 * Create a new {@code ReadyFuture} set to a value.
	 *
	 * @param value the final value of this {@code Future}
	 */
	public ReadyPartialFuture(T value) {
		this(value, DEFAULT_ES);
	}

	/**
	 * Create a new {@code ReadyFuture} set to an error. If {@code error} is {@code null} the
	 *  {@code Future} will be set to a {@code null} value and <i>NOT</i> an error.
	 *
	 * @param error the final error of this {@code Future}
	 */
	public ReadyPartialFuture(Error error) {
		this(error, DEFAULT_ES);
	}

	/**
	 * Create a new {@code ReadyPartialFuture} set to an error. If {@code error} is
	 *  {@code null} the {@code Future} will be set to a {@code null} value and
	 *  <i>NOT</i> an error.
	 *
	 * @param error the final error of this {@code Future}
	 */
	public ReadyPartialFuture(Exception error) {
		this(error, DEFAULT_ES);
	}

	/**
	 * Create a new {@code ReadyPartialFuture} set to a value.
	 *
	 * @param value the final value of this {@code Future}
	 * @param executor the executor with which to execute callbacks
	 */
	public ReadyPartialFuture(T value, Executor executor) {
		this.isError = false;
		this.value = value;
		this.error = null;
		this.executor = executor;
	}

	/**
	 * Create a new {@code ReadyFuture} set to an error. If {@code error} is {@code null} the
	 *  {@code Future} will be set to a {@code null} value and <i>NOT</i> an error.
	 *
	 * @param error the final error of this {@code Future}
	 * @param executor the executor with which to execute callbacks
	 */
	public ReadyPartialFuture(Error error, Executor executor) {
		this((Throwable)error, executor);
	}

	/**
	 * Create a new {@code ReadyFuture} set to an error. If {@code error} is {@code null} the
	 *  {@code Future} will be set to a {@code null} value and <i>NOT</i> an error.
	 *
	 * @param error the final error of this {@code Future}
	 * @param executor the executor with which to execute callbacks
	 */
	public ReadyPartialFuture(Exception error, Executor executor) {
		this((Throwable)error, executor);
	}

	private ReadyPartialFuture(Throwable error, Executor executor) {
		// If the error is set to null, this is NOT an error
		isError = error != null;

		this.error = error;
		this.value = null;
		this.executor = executor;
	}

	/**
	 * Does nothing, as a {@code ReadyPartialFuture} cannot be cancelled.
	 */
	@Override
	public void cancel() {}

	@Override
	public void onReady(Runnable r) {
		this.executor.execute(r);
	}

	@Override
	public boolean onReadyAlready(Runnable r) {
		return true;
	}

	@Override
	public <V> PartialFuture<V> flatMap(
			PartialFunction<? super T, ? extends PartialFuture<V>> m) {
		try {
			return m.apply(get());
		} catch(Exception t) {
			return new ReadyPartialFuture<V>(t, executor);
		}
	}

	@Override
	public <V> PartialFuture<V> map(PartialFunction<? super T, V> m) {
		try {
			V mapped = m.apply(get());
			return new ReadyPartialFuture<V>(mapped, executor);
		} catch(Exception t) {
			return new ReadyPartialFuture<V>(t, executor);
		}
	}

	@Override
	public Future<T> rescue(Function<? super Exception, Future<T>> m) {
		if(!isError()) {
			return new ReadyFuture<T>(value, executor);
		}

		try {
			if (error instanceof Error)
				throw (Error)error;
			return m.apply( (Exception)error );
		} catch(RuntimeException t) {
			return new ReadyFuture<T>(t, executor);
		}
	}

	@Override
	public PartialFuture<T> rescue(
			PartialFunction<? super Exception, ? extends PartialFuture<T>> m) {
		if(!isError()) {
			return this;
		}

		try {
			if (error instanceof Error)
				throw (Error)error;
			return m.apply((Exception)error);
		} catch(Exception t) {
			return new ReadyPartialFuture<T>(t, executor);
		}
	}

	/**
	 * Always returns immediately.
	 */
	@Override
	public void blockUntilReady() {
		return;
	}

	@Override
	public void blockInterruptibly() {
		return;
	}

	@Override
	public T get() throws Exception {
		if(isError()) {
			if (this.error instanceof Error)
				throw (Error)this.error;
			throw (Exception)this.error;
		}
		return value;
	}

	@Override
	public T getInterruptibly() throws Exception {
		return this.get();
	}

	/**
	 * A {@code ReadyFuture} is always done.
	 *
	 * @return always returns {@code true}.
	 */
	@Override
	public boolean isDone() {
		return true;
	}

	@Override
	public boolean isError() {
		return isError;
	}

	@Override
	public Throwable getError() {
		if(!isError())
			throw new IllegalStateException("Future is set to value, not error");
		return error;
	}

	@Override
	public void dispose() {
		cancel();
	}
}
