/*
 * Future.java
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

/**
 * The typed result of an asynchronous process.
 *  A {@code Future} will be set exactly once to either a "completed" {@code T}
 *  or to an unchecked exception. A {@code Future} is a special case of a
 *  {@link PartialFuture} that cannot result in a checked exception.<br>
 * <br>
 * A {@code Future} can be waited on by blocking, or by using asynchronous callbacks.
 *
 * @param <T> the type of the eventual value of the {@code Future}
 */
public interface Future<T> extends PartialFuture<T> {
	/**
	 * Apply a function to the successful result of this operation. This version of
	 *  {@code map()}, since it accepts only {@code Function}s that cannot throw checked
	 *  exceptions, returns a {@code Future}. Errors encountered
	 *  in the execution of this operation are sent to the resulting {@code Future}.<br>
	 * <br>
	 * If the returned {@code Future} is {@link Future#cancel() cancelled} the input
	 * {@code Future} will also be cancelled.
	 *
	 * @param m the map to the conversion to be run
	 * @return a new {@code Future} that passes errors and converts output of this {@code Future}.
	 */
	public <V> Future<V> map(final Function<? super T, V> m);

	/**
	 * Applies the successful result of this operation as the input into another asynchronous
	 *  operation and returns a handle to that result.
	 *  That is, when the operation represented by this {@code Future} is complete and a result
	 *  is returned, the {@code Mapper} {@code m} is invoked to begin a new asynchronous process
	 *  using the result.  This version of
	 *  {@code map()}, since it accepts only {@code Function}s that cannot throw checked
	 *  exceptions, returns a {@code Future}.
	 *  Errors in the execution of either operation are sent to the resulting {@code Future}.<br>
	 * <br>
	 * If the returned {@code Future} is {@link Future#cancel() cancelled} the input
	 * {@code Future} will also be cancelled.
	 *
	 * @param m the map to the asynchronous process to be run
	 * @return a new {@code Future} that passes errors and converts output of this {@code Future}
	 *  to the output of another asynchronous process.
	 */
	public <V> Future<V> flatMap(final Function<? super T, Future<V>> m);

	/**
	 * Returns a {@code Future} that modifies the error behavior of this {@code Future}.
	 *  Since {@code Future} cannot be set to an {@link Exception} other than a
	 *  {@code RuntimeException}, the handling {@code Function} need not handle other,
	 *  more general, types. If an {@link Error} is the output of this process, this handler
	 *  is not invoked and that {@code Error} will be passed on to the resulting {@code Future}.
	 *
	 * @param m the process to run that maps an error into another asynchronous operation
	 *
	 * @return a newly created {@code Future} with modified error behavior
	 */
	public Future<T> rescueRuntime(Function<? super RuntimeException, Future<T>> m);

	/**
	 * Blocks until a value is set on this {@code Future} and returns it.
	 * If this {@code Future} is set to a RuntimeException or Error, throws it.
	 * A {@code Future} cannot be set to a checked exception, so there is no
	 * need to throw from this call.
	 */
	@Override
	public T get();

	/**
	 * Blocks until a value is set on this {@code Future} and returns it.
	 *  If this {@code PartialFuture} is set to an {@link Exception} or {@link Error}
	 *  this call will throw that error.
	 *
	 * @return the output value of the asynchronous process
	 *
	 * @throws InterruptedException if the blocked thread is interrupted
	 */
	@Override
	public T getInterruptibly() throws InterruptedException;

	/**
	 * Gets the error from a {@code Future} if it has been set to this state. If
	 *  this {@code Future} is not yet set, or has not been set to an error, throws
	 *  an exception. Although this method returns a {@code Throwable}, the set of
	 *  possible return types is more limited. A {@code Future} is limited to being
	 *  set to an unchecked exceptions -- that is an {@link Error} or a
	 *  {@link RuntimeException} (or one of its subclasses).
	 *
	 * @return the error output of the {@code Future}
	 *
	 * @throws IllegalStateException if this {@code Future} is not in an error state
	 */
	@Override
	public Throwable getError();
}
