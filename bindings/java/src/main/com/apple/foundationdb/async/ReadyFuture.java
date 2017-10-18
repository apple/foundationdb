/*
 * ReadyFuture.java
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
 * A {@code Future} that is always in the "set" state. No call on a {@code ReadyFuture}
 *  will block, since this is either a value or an error from its time of creation. A
 *  {@code ReadyFuture} is useful for returning the result of an asynchronous process.<br>
 *  <br>
 *  If code needs to return a {@code Future<Void>} to indicate completion,
 *  {@link #DONE} should be used.
 *
 * @param <T> the type of value that can be set
 */
public class ReadyFuture<T> extends ReadyPartialFuture<T> implements Future<T> {
	/**
	 * A set {@code Future} that indicates a process has completed successfully. If the
	 *  the contract of a function is to signal completion with a {@code Future<Void>}
	 *  this value can be returned at successful completion.
	 */
	public static final Future<Void> DONE = new ReadyFuture<Void>((Void)null);

	/**
	 * Create a new {@code ReadyFuture} set to a value.
	 *
	 * @param value the final value of this {@code Future}
	 */
	public ReadyFuture(T value) {
		super(value);
	}

	/**
	 * Create a new {@code ReadyFuture} set to an error. If {@code error} is {@code null} the
	 *  {@code Future} will be set to a {@code null} value and <i>NOT</i> an error.
	 *
	 * @param error the final error of this {@code Future}
	 */
	public ReadyFuture(Error error) {
		super(error);
	}

	/**
	 * Create a new {@code ReadyFuture} set to an error. If {@code error} is {@code null} the
	 *  {@code Future} will be set to a {@code null} value and <i>NOT</i> an error.
	 *
	 * @param error the final error of this {@code Future}
	 */
	public ReadyFuture(RuntimeException error) {
		super(error);
	}

	/**
	 * Create a new {@code ReadyFuture} set to a value.
	 *
	 * @param value the final value of this {@code Future}
	 * @param executor the executor with which to execute callbacks
	 */
	public ReadyFuture(T value, Executor executor) {
		super(value, executor);
	}

	/**
	 * Create a new {@code ReadyFuture} set to an error. If {@code error} is {@code null} the
	 *  {@code Future} will be set to a {@code null} value and <i>NOT</i> an error.
	 *
	 * @param error the final error of this {@code Future}
	 * @param executor the executor with which to execute callbacks
	 */
	public ReadyFuture(Error error, Executor executor) {
		super(error, executor);
	}

	/**
	 * Create a new {@code ReadyFuture} set to an error. If {@code error} is {@code null} the
	 *  {@code Future} will be set to a {@code null} value and <i>NOT</i> an error.
	 *
	 * @param error the final error of this {@code Future}
	 * @param executor the executor with which to execute callbacks
	 */
	public ReadyFuture(RuntimeException error, Executor executor) {
		super(error, executor);
	}

	@Override
	public T get() {
		try {
			return super.get();
		} catch (Error e) {
			throw e;
		} catch (Throwable t) {
			throw (RuntimeException)t;
		}
	}

	@Override
	public T getInterruptibly() {
		return this.get();
	}

	@Override
	public <V> Future<V> map(Function<? super T, V> m) {
		try {
			V mapped = m.apply(get());
			return new ReadyFuture<V>(mapped, executor);
		} catch(RuntimeException t) {
			return new ReadyFuture<V>(t, executor);
		}
	}

	@Override
	public <V> Future<V> flatMap(Function<? super T, Future<V>> m) {
		try {
			return m.apply(get());
		} catch(RuntimeException t) {
			return new ReadyFuture<V>(t, executor);
		}
	}

	@Override
	public Future<T> rescueRuntime(Function<? super RuntimeException, Future<T>> m) {
		if(!isError()) {
			return new ReadyFuture<T>(get(), executor);
		}

		try {
			Throwable error = getError();
			if (error instanceof Error)
				throw (Error)error;
			return m.apply( (RuntimeException)error );
		} catch(RuntimeException t) {
			return new ReadyFuture<T>(t, executor);
		}
	}
}
