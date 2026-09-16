/*
 * RangeQueryIterator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import com.apple.foundationdb.EventKeeper.Events;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;

/** Maintains range pagination, prefetch, and native-future ownership for a single iterator. */
abstract class RangeQueryIterator<T extends KeyValue, I> implements AsyncIterator<T> {
	private final FDBTransaction tr;
	private final EventKeeper eventKeeper;
	private final boolean rowsLimited;
	private final boolean reverse;

	private Chunk<T> chunk = null;
	private Chunk<T> nextChunk = null;
	private boolean fetchOutstanding = false;
	private byte[] prevKey = null;
	private int index = 0;
	private int iteration = 0;
	private KeySelector begin;
	private KeySelector end;

	private int rowsRemaining;

	private NativeFuture<I> fetchingChunk;
	private CompletableFuture<Boolean> nextFuture;
	private boolean isCancelled = false;

	RangeQueryIterator(FDBTransaction tr, KeySelector begin, KeySelector end, int rowLimit, boolean reverse,
	                   EventKeeper eventKeeper) {
		this.tr = tr;
		this.eventKeeper = eventKeeper;
		this.begin = begin;
		this.end = end;
		this.rowsLimited = rowLimit != 0;
		this.rowsRemaining = rowLimit;
		this.reverse = reverse;
	}

	static final class Chunk<T extends KeyValue> {
		private final List<T> values;
		private final boolean more;

		Chunk(List<T> values, boolean more) {
			this.values = values;
			this.more = more;
		}
	}

	protected abstract NativeFuture<I> fetch(KeySelector begin, KeySelector end, int rowLimit, int iteration);
	protected abstract Chunk<T> getChunk(I result);

	// Fetch callbacks can run immediately, so concrete adapters must finish construction first.
	protected final void start() { startNextFetch(); }

	private synchronized boolean mainChunkIsTheLast() { return !chunk.more || (rowsLimited && rowsRemaining < 1); }

	private final class FetchComplete implements BiConsumer<I, Throwable> {
		private final NativeFuture<I> fetchingChunk;
		private final CompletableFuture<Boolean> promise;

		FetchComplete(NativeFuture<I> fetch, CompletableFuture<Boolean> promise) {
			this.fetchingChunk = fetch;
			this.promise = promise;
		}

		@Override
		public void accept(I data, Throwable error) {
			try {
				if (error != null) {
					if (eventKeeper != null) {
						eventKeeper.increment(Events.RANGE_QUERY_CHUNK_FAILED);
					}
					promise.completeExceptionally(error);
					if (error instanceof Error) {
						throw (Error)error;
					}

					return;
				}

				final Chunk<T> rangeResult = getChunk(data);
				final int keyCount = rangeResult.values.size();
				final byte[] lastKey = keyCount > 0 ? rangeResult.values.get(keyCount - 1).getKey() : null;
				if (lastKey == null) {
					promise.complete(Boolean.FALSE);
					return;
				}

				synchronized (RangeQueryIterator.this) {
					fetchOutstanding = false;

					rowsRemaining -= keyCount;

					if (reverse) {
						end = KeySelector.firstGreaterOrEqual(lastKey);
					} else {
						begin = KeySelector.firstGreaterThan(lastKey);
					}

					if (chunk == null || index == chunk.values.size()) {
						nextChunk = null;
						chunk = rangeResult;
						index = 0;
					} else {
						nextChunk = rangeResult;
					}
				}

				promise.complete(Boolean.TRUE);
			} finally {
				fetchingChunk.close();
			}
		}
	}

	private synchronized void startNextFetch() {
		if (fetchOutstanding) throw new IllegalStateException("Reentrant call not allowed");
		if (isCancelled) return;

		if (chunk != null && mainChunkIsTheLast()) return;

		fetchOutstanding = true;
		nextChunk = null;

		nextFuture = new CompletableFuture<>();
		final long sTime = System.nanoTime();
		fetchingChunk = fetch(begin, end, rowsLimited ? rowsRemaining : 0, ++iteration);

		BiConsumer<I, Throwable> cons = new FetchComplete(fetchingChunk, nextFuture);
		if (eventKeeper != null) {
			eventKeeper.increment(Events.RANGE_QUERY_FETCHES);
			cons = cons.andThen(
			    (r, t) -> { eventKeeper.timeNanos(Events.RANGE_QUERY_FETCH_TIME_NANOS, System.nanoTime() - sTime); });
		}

		fetchingChunk.whenComplete(cons);
	}

	@Override
	public synchronized CompletableFuture<Boolean> onHasNext() {
		if (isCancelled) throw new CancellationException();

		if (chunk == null) {
			return nextFuture;
		}

		if (index < chunk.values.size()) {
			return AsyncUtil.READY_TRUE;
		}

		return mainChunkIsTheLast() ? AsyncUtil.READY_FALSE : nextFuture;
	}

	@Override
	public boolean hasNext() {
		return onHasNext().join();
	}

	@Override
	public T next() {
		CompletableFuture<Boolean> nextFuture;
		synchronized (this) {
			if (isCancelled) throw new CancellationException();

			if (chunk != null && index < chunk.values.size()) {
				boolean initialNext = index == 0;

				T result = chunk.values.get(index);
				prevKey = result.getKey();
				index++;

				if (eventKeeper != null) {
					// We record the BYTES_FETCHED here, rather than at a lower level,
					// because some parts of the construction of a RangeResult occur underneath
					// the JNI boundary, and we don't want to pass the eventKeeper down there
					// (note: account for the length fields as well when recording the bytes
					// fetched)
					eventKeeper.count(Events.BYTES_FETCHED, result.getKey().length + result.getValue().length + 8);
					eventKeeper.increment(Events.RANGE_QUERY_RECORDS_FETCHED);
				}

				// If this is the first call to next() on a chunk there cannot
				//  be another waiting, since we could not have issued a request
				assert (!(initialNext && nextChunk != null));

				if (index == chunk.values.size() && nextChunk != null) {
					index = 0;
					chunk = nextChunk;
					nextChunk = null;
				}

				if (initialNext) {
					startNextFetch();
				}

				return result;
			}

			nextFuture = onHasNext();
		}

		// Do not hold the iterator monitor while waiting for a delayed fetch.
		return nextFuture
		    .thenApply(hasNext -> {
			    if (hasNext) {
				    return next();
			    }
			    throw new NoSuchElementException();
		    })
		    .join();
	}

	@Override
	public synchronized void remove() {
		if (prevKey == null) throw new IllegalStateException("No value has been fetched from database");

		tr.clear(prevKey);
	}

	@Override
	public synchronized void cancel() {
		isCancelled = true;
		nextFuture.cancel(true);
		fetchingChunk.cancel(true);
	}
}
