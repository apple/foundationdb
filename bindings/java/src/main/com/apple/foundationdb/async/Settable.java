/*
 * Settable.java
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

/**
 * Used to communicate output to consumers of an asynchronous process. This is the general
 *  case of the concept {@code Promise}, where {@link SettablePartial} is a special case
 *  that can also accept an unchecked {@link Exception}.
 *
 * @param <T> the type of output of a successful process
 */
public interface Settable<T> {
	/**
	 * Set this {@code Settable} to a value. This will invoke callbacks, and unblock
	 *  all {@code Thread}s waiting on associated {@code Future}s. A {@code Settable}
	 *  can be set only once and this call
	 *  will throw an exception if this object is already set to a value or error. It
	 *  is not, however, an error to call this if the operation has been cancelled.
	 *  If the task has been cancelled, this call will have no effect.
	 *
	 * @param value the output of this successful process
	 *
	 * @throws IllegalStateException if {@code set()} or {@code setError()} has
	 *  already been called.
	 */
	public abstract void set(T value);

	/**
	 * Set this {@code Settable} to an error. This call will invoke callbacks, and unblock
	 *  all {@code Thread}s waiting on associated {@code Future}s. A {@code Settable}
	 *  can be set only once and this call will
	 *  throw an exception if this object is already set to a value or error. It
	 *  is not, however, an error to call this method if the operation has been cancelled.
	 *  If the task has been cancelled, this call will have no effect.
	 *
	 * @param error the error encountered in the course of this process
	 *
	 * @throws IllegalStateException if {@code set()} or {@code setError()} has
	 *  already been called.
	 */
	public abstract void setError(RuntimeException error);

	/**
	 * Set this {@code Settable} to an error. This call will invoke callbacks, and unblock
	 *  all {@code Thread}s waiting on associated {@code Future}s. A {@code Settable}
	 *  can be set only once and this call will
	 *  throw an exception if this object is already set to a value or error. It
	 *  is not, however, an error to call this method if the operation has been cancelled.
	 *  If the task has been cancelled, this call will have no effect.
	 *
	 * @param error the error encountered in the course of this process
	 *
	 * @throws IllegalStateException if {@code set()} or {@code setError()} has
	 *  already been called.
	 */
	public abstract void setError(Error error);

	/**
	 * Returns {@code true} if this {@code Settable} has been set to a value or error.
	 *
	 * @return {@code true} if a value or error has been set, {@code false} otherwise
	 */
	public abstract boolean isSet();

	/**
	 * Register code to be called if this operation is cancelled. {@code Settable}s can
	 *  be cancelled by calls to {@link Future#cancel()} on associated {@code Future}s.
	 *  If this {@code Settable} is already set, the
	 *  callback will not be registered and this call will return {@code false}. If this
	 *  call returns {@code true} the registered callback is guaranteed to be called if
	 *  the associated {@code Future} is cancelled.
	 *
	 * @param r routine to execute if cancelled.
	 *
	 * @return {@code true} if the callback was registered, {@code false} otherwise.
	 */
	public abstract boolean onCancelled(Runnable r);

	/**
	 * Link an operation to cancel in the event that this {@code Settable} is cancelled.
	 *  This is a convenience method (for more complicated calls to
	 *  {@link #onCancelled(Runnable)}) with possible internal optimizations.
	 *
	 * @param c the operation to cancel
	 */
	public abstract void onCancelledCancel(Cancellable c);
}