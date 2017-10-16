/*
 * PartialFuture.java
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

import com.apple.cie.foundationdb.Disposable;

/**
 * The typed result of an asynchronous process.
 *  A {@code PartialFuture} will be set exactly once to either a "completed" {@code T}
 *  or to an {@link Exception}. A {@code PartialFuture} is "partial" in that some
 *  outcomes of the process involve the error output being a checked exception. A
 *  {@link Future} is a special case of a {@code PartialFuture} that cannot result in
 *  a checked exception.<br>
 * <br>
 * A {@code PartialFuture} can be waited on by blocking, or by using asynchronous callbacks.
 *
 * @param <T> the type of the eventual value of the {@code PartialFuture}
 */
public interface PartialFuture<T> extends Cancellable, Disposable {
	/**
	 * Registers a {@code Runnable} to run as the callback for this {@code Future}
	 *  on a default thread. It will be called after the future becomes ready, including
	 *  if the future is set to a value, set to an error, or cancelled.<br>
	 *  <br>
	 *  Consider using {@link #map(PartialFunction)} and {@link #flatMap(PartialFunction)}
	 *   instead, as they automatically provide error propagation and cancellation.
	 *
	 * @param r the code to run when the future is set either to value or error
	 */
	public void onReady(Runnable r);

	/**
	 * Checks if this {@code Future} is ready and, if not, atomically registers a
	 *  {@code Runnable} to run as the callback for this future on a default thread. The
	 *  callback will not be called if the future is ready. Callers can rely on the guarantee
	 *  that if this function returns {@code false} their callback will be executed on
	 *  completion (including if the future is set to a value, set to an error, or cancelled),
	 *  and if {@code true} they can then call {@link #get()} without blocking.
	 *
	 * @param r the code to run when the future is set if it is not currently ready
	 *
	 * @return {@code true} if the future is ready, {@code false} otherwise
	 */
	public boolean onReadyAlready(Runnable r);

	/**
	 * Apply a function to the successful result of this operation. Errors
	 *  in the execution of this operation are sent to the resulting {@code Future}.<br>
	 * <br>
	 * If the returned {@code Future} is {@link PartialFuture#cancel() cancelled} the input
	 *  input {@code Future} will also be cancelled.
	 *
	 * @param m the map to the conversion to be run
	 * @return a new {@code Future} that passes errors and converts output of this {@code Future}.
	 */
	public <V> PartialFuture<V> map(PartialFunction<? super T, V> m);

	/**
	 * Applies the successful result of this operation as the input into another asynchronous
	 *  operation and returns a handle to that result.
	 *  That is, when the operation represented by this {@code Future} is complete and a result
	 *  is returned, the {@code Mapper} {@code m} is invoked to begin a new asynchronous process
	 *  using the result.
	 *  Errors in the execution of either operation are sent to the resulting {@code Future}.<br>
	 * <br>
	 * If the returned {@code Future} is {@link PartialFuture#cancel() cancelled} the input
	 *  input {@code Future} will also be cancelled.
	 *
	 * @param m the map to the asynchronous process to be run
	 * @return a new {@code Future} that passes errors and converts output of this {@code Future}
	 *  to the output of another asynchronous process.
	 */
	public <V> PartialFuture<V> flatMap(PartialFunction<? super T, ? extends PartialFuture<V>> m);

	/**
	 * Map any error result of this {@code Future} to another asynchronous process. This can be
	 *  used when an error is expected or can be logically dealt with in some way. If the
	 *  rescue process cannot handle the error, or if the rescue process itself throws an error,
	 *  that error will be set on this {@code Future}. If the result
	 *  of this call is used in place of this {@code Future}, the user of the output
	 *  {@code Future}
	 *  will see successes of this process, successes of the rescue process, or errors from
	 *  the rescue process.
	 *
	 * @param m the process to run that maps an error into another asynchronous operation
	 *
	 * @return a newly created {@code Future} with modified error behavior
	 */
	public PartialFuture<T> rescue(PartialFunction<? super Exception, ? extends PartialFuture<T>> m);

	/**
	 * Convert an error output of this operation into another asynchronous process.
	 *
	 * @param m the process to run on completion into error state.
	 *
	 * @return the non-checked {@code Future} output with modified error behavior
	 */
	public Future<T> rescue(Function<? super Exception, Future<T>> m);

	/**
	 * Blocks until this {@code Future} is set to either a value or an error.
	 *  When this function returns without an error, {@link #isDone()} will return
	 *  {@code true}.
	 * <br>
	 * Code should never block for a future to become ready in code invoked as a callback.
	 *  Blocking in callback code will generally block client networking and cause a deadlock.
	 *
	 */
	public void blockUntilReady();

	/**
	 * Blocks until this {@code Future} is set to either a value or an error.
	 *  When this function returns without an error, {@link #isDone()} will return
	 *  {@code true}.
	 * <br>
	 * Code should never block for a future to become ready in code invoked as a callback.
	 *  Blocking in callback code will generally block client networking and cause a deadlock.
	 *
	 * @throws InterruptedException if the blocked thread is interrupted
	 */
	public void blockInterruptibly() throws InterruptedException;

	/**
	 * Blocks until a value is set on this {@code PartialFuture} and returns it.
	 *  If this {@code PartialFuture} is set to an {@link Exception} or {@link Error}
	 *  this call will throw that error.
	 *
	 * @return the output value of the asynchronous process
	 *
	 * @throws Exception if the process was not successfully executed.
	 * @throws InterruptedException if the blocked thread is interrupted
	 * @throws Error if the process encountered a serious {@code Error} during execution.
	 */
	public T get() throws Exception;

	/**
	 * Blocks until a value is set on this {@code PartialFuture} and returns it.
	 *  If this {@code PartialFuture} is set to an {@link Exception} or {@link Error}
	 *  this call will throw that error.
	 *
	 * @return the output value of the asynchronous process
	 *
	 * @throws Exception if the process was not successfully executed.
	 * @throws InterruptedException if the blocked thread is interrupted
	 * @throws Error if the process encountered a serious {@code Error} during execution.
	 */
	public T getInterruptibly() throws Exception;

	/**
	 * Cancels this asynchronous operation. If called before the {@code Future} is
	 *  ready, subsequent attempts to access its value will throw.
	 *  Cancelling a {@code Future} which is already set has no effect.
	 *  Cancelling a {@code Future} should not be assumed to eliminate the affects
	 *  of launching the operation. Many asynchronous operations start work on remote
	 *  machines -- this work will generally not be stopped if the local {@code Future}
	 *  is cancelled.
	 */
	@Override
	public void cancel();

	/**
	 * Gets the readiness of this {@code Future}.  A {@code Future} is ready
	 *  if the value has been set or an error has been set.
	 *
	 * @return <code>true</code> if the {@code Future} is set to a value or error
	 */
	public boolean isDone();

	/**
	 * Tests if this {@code Future} is set to an error
	 * @return <code>true</code> if in error state, <code>false</code> otherwise
	 */
	public boolean isError();

	/**
	 * Gets the error from a {@code Future} if it has been set to this state. If
	 *  this {@code Future} is not yet set, or has not been set to an error, throws
	 *  an exception. Although this method returns a {@code Throwable}, the set of
	 *  possible types is more limited. A {@code PartialFuture} is limited to be set in
	 *  an error state to an {@code Exception} (or a subclass thereof) or an unchecked
	 *  {@link Error}.
	 *
	 * @return the error output of the {@code Future}
	 *
	 *  @throws IllegalStateException if this {@code PartialFuture} is not in an error state
	 */
	public Throwable getError();

	/**
	 * Equivalent to calling {@link #cancel()}.
	 */
	@Override
	public void dispose();
}
