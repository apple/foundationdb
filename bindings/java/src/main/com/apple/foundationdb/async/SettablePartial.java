/*
 * SettablePartial.java
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
 * Used to communicate output to consumers of an asynchronous process. This is a
 *  specialization of {@link Settable} -- one that can also accept a checked
 *  {@link Exception} as a possible error outcome.
 *
 * @param <T> the type of output of a successful process
 */
public interface SettablePartial<T> extends Settable<T> {
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
	public abstract void setError(Exception error);
}