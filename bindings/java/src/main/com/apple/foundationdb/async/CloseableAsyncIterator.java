/*
 * CloseableAsyncIterator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
 * A version of {@link AsyncIterator} that must be closed once no longer in use in order to free
 *  any associated resources.
 *
 * @param <T> the type of object yielded by {@code next()}
 */
public interface CloseableAsyncIterator<T> extends AutoCloseable, AsyncIterator<T> {
	/**
	 * Cancels any outstanding asynchronous work, closes the iterator, and frees any associated
	 *  resources. This must be called at least once after the object is no longer in use. This
	 *  can be called multiple times, but care should be taken that an object is not in use
	 *  in another thread at the time of the call.
	 */
	@Override
	void close();

	/**
	 * Alias for {@link #close}.
	 */
	@Override
	default void cancel() {
		close();
	}
}
