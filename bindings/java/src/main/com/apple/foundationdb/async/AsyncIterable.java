/*
 * AsyncIterable.java
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

import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * A collection of elements that can be iterated over in a non-blocking fashion.
 *
 * @param <T> the type of element yielded from iteration
 */
public interface AsyncIterable<T> extends Iterable<T> {
	/**
	 * Gets a non-blocking iterator to be used to enumerate all values.
	 *
	 * @return a handle to be used for non-blocking iteration
	 */
	@Override
	AsyncIterator<T> iterator();

	/**
	 * Asynchronously return the results of this operation as a {@code List}. This is
	 *  added as a convenience to users and an opportunity for providers
	 *  of this interface to optimize large operations.
	 *
	 * @see AsyncUtil#collect(AsyncIterable)
	 *
	 * @return a {@code CompletableFuture} that will be set to contents of this operation
	 */
	CompletableFuture<List<T>> asList();
}
