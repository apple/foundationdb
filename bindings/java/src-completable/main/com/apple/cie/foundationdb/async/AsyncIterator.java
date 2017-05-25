/*
 * AsyncIterator.java
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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import com.apple.cie.foundationdb.Disposable;

/**
 * A version of {@code Iterator} that allows for non-blocking iteration over elements.
 *  Calls to {@link #next()} will not block if {@link #onHasNext()} has been called
 *  since the last call to {@code next()} and the {@code CompletableFuture} returned from
 *  {@code onHasNext()} has completed.
 *
 * @param <T> the type of object yielded by {@code next()}
 */
public interface AsyncIterator<T> extends Iterator<T>, Disposable {
	/**
	 * Returns a asynchronous signal for the presence of more elements in the sequence.
	 * Once the future returned by {@link #onHasNext()} is ready, the next call to
	 * {@link #next} will not block.
	 *
	 * @return a {@code CompletableFuture} that will be set to {@code true} if {@code next()}
	 *  would return another element without blocking or to {@code false} if there are
	 *  no more elements in the sequence.
	 */
	public CompletableFuture<Boolean> onHasNext();

	/**
	 * Blocking call to determine if the sequence contains more elements. This call
	 *  is equivalent to calling {@code onHasNext().get()}.
	 *
	 * @see AsyncIterator#onHasNext()
	 *
	 * @return {@code true} if there are more elements in the sequence, {@code false}
	 *  otherwise.
	 */
	@Override
	public boolean hasNext();

	/**
	 * Returns the next element in the sequence. This will not block if, since the
	 *  last call to {@code next()}, {@link #onHasNext()} was called and the resulting
	 *  <h1>FIXME!!!!</h1> has completed or the blocking call {@link #hasNext()} was called
	 *  and has returned. It is legal, therefore, to make a call to {@code next()} without a
	 *  preceding call to
	 *  {@link #hasNext()} or {@link #onHasNext()}, but that invocation of {@code next()}
	 *  may block on remote operations.
	 *
	 * @return the next element in the sequence, blocking if necessary.
	 *
	 * @throws NoSuchElementException if the sequence has been exhausted.
	 */
	@Override
	public T next();

	/**
	 * Cancels any outstanding asynchronous work associated with this {@code AsyncIterator}.
	 */
	public void cancel();

	/**
	 * Cancel this {@code AsyncIterable} and dispose of associated resources. Equivalent
	 *  to calling {@link AsyncIterator#cancel()}.
	 */
	@Override
	public void dispose();
}
