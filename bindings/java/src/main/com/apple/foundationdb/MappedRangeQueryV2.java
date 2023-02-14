/*
 * RangeQuery.java
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

package com.apple.foundationdb;

import com.apple.foundationdb.EventKeeper.Events;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

// TODO: Share code with RangeQuery?
/**
 * Represents a query against FoundationDB for a range of keys. The
 *  result of this query can be iterated over in a blocking fashion with a call to
 *  {@link #iterator()} (as specified by {@link Iterable}).
 *  If the calling program uses an asynchronous paradigm, a non-blocking
 *  {@link AsyncIterator} is returned from {@link #iterator()}. Both of these
 *  constructions will not begin to query the database until the first call to
 *  {@code hasNext()}. As the query uses its {@link Transaction} of origin to fetch
 *  all the data, the use of this query object must not span more than a few seconds.
 *
 * <br><br><b>NOTE:</b> although resulting {@code Iterator}s do support the {@code remove()}
 *   operation, the remove is not durable until {@code commit()} on the {@code Transaction}
 *   that yielded this query returns <code>true</code>.
 */
class MappedRangeQueryV2 implements AsyncIterable<MappedKeyValueV2> {
	private final FDBTransaction tr;
	private final KeySelector begin;
	private final KeySelector end;
	private final byte[] mapper; // Nonnull
	private final boolean snapshot;
	private final int rowLimit;
	private final int matchIndex;
	private final boolean fetchLocalOnly;
	private final boolean reverse;
	private final StreamingMode streamingMode;
	private final EventKeeper eventKeeper;

	private final int code_uint8 = 1;
	private final int code_bool = 2;

	private static final int matchIndexPos = 1;
	private static final int fetchLocalOnlyPos = 3;

	MappedRangeQueryV2(FDBTransaction transaction, boolean isSnapshot, KeySelector begin, KeySelector end,
	                   byte[] mapper, int rowLimit, int matchIndex, boolean fetchLocalOnly, boolean reverse,
	                   StreamingMode streamingMode, EventKeeper eventKeeper) {
		this.tr = transaction;
		this.begin = begin;
		this.end = end;
		this.mapper = mapper;
		this.matchIndex = matchIndex;
		this.fetchLocalOnly = fetchLocalOnly;
		this.snapshot = isSnapshot;
		this.rowLimit = rowLimit;
		this.reverse = reverse;
		this.streamingMode = streamingMode;
		this.eventKeeper = eventKeeper;
	}

	byte[] getParamsBytes(int matchIndex, boolean fetchLocalOnly) {
		byte[] paramsBytes = new byte[] { 0x02, 0, 0, 0, 0 };
		paramsBytes[matchIndexPos] = code_uint8;
		paramsBytes[matchIndexPos + 1] = (byte)matchIndex;
		paramsBytes[fetchLocalOnlyPos] = code_bool;
		paramsBytes[fetchLocalOnlyPos + 1] = (byte)(fetchLocalOnly ? 1 : 0);
		return paramsBytes;
	}

	/**
	 * Returns all the results from the range requested as a {@code List}. If there were no
	 *  limits on the original query and there is a large amount of data in the database
	 *  this call could use a very large amount of memory.
	 *
	 * @return a {@code CompletableFuture} that will be set to the contents of the database
	 *  constrained by the query parameters.
	 */
	@Override
	public CompletableFuture<List<MappedKeyValueV2>> asList() {
		StreamingMode mode = this.streamingMode;
		if (mode == StreamingMode.ITERATOR) mode = (this.rowLimit == 0) ? StreamingMode.WANT_ALL : StreamingMode.EXACT;
		// if the streaming mode is EXACT, try and grab things as one chunk
		if (mode == StreamingMode.EXACT) {
			byte[] paramsBytes = getParamsBytes(this.matchIndex, this.fetchLocalOnly);
			FutureMappedResultsV2 range =
			    tr.getMappedRange_internal_v2(this.begin, this.end, this.mapper, paramsBytes, this.rowLimit, 0,
			                                  StreamingMode.EXACT.code(), 1, this.snapshot, this.reverse);
			return range.thenApply(result -> result.get().values).whenComplete((result, e) -> range.close());
		}
		// If the streaming mode is not EXACT, simply collect the results of an
		// iteration into a list
		return AsyncUtil.collect(new MappedRangeQueryV2(tr, snapshot, begin, end, mapper, rowLimit, matchIndex,
		                                                fetchLocalOnly, reverse, mode, eventKeeper),
		                         tr.getExecutor());
	}

	/**
	 *  Returns an {@code Iterator} over the results of this query against FoundationDB.
	 *
	 *  @return an {@code Iterator} over type {@code MappedKeyValueV2}.
	 */
	@Override
	public AsyncRangeIteratorV2 iterator() {
		return new AsyncRangeIteratorV2(this.rowLimit, getParamsBytes(this.matchIndex, this.fetchLocalOnly),
		                                this.reverse, this.streamingMode);
	}

	private class AsyncRangeIteratorV2 implements AsyncIterator<MappedKeyValueV2> {
		// immutable aspects of this iterator
		private final boolean rowsLimited;
		private final boolean reverse;
		private final StreamingMode streamingMode;
		private final byte[] paramsBytes;

		// There is the chance for parallelism in the two "chunks" for fetched data
		private MappedRangeResultV2 chunk = null;
		private MappedRangeResultV2 nextChunk = null;
		private boolean fetchOutstanding = false;
		private byte[] prevKey = null;
		private int index = 0;
		private int iteration = 0;
		private KeySelector begin;
		private KeySelector end;

		private int rowsRemaining;

		private FutureMappedResultsV2 fetchingChunk;
		private CompletableFuture<Boolean> nextFuture;
		private boolean isCancelled = false;

		private AsyncRangeIteratorV2(int rowLimit, byte[] paramsBytes, boolean reverse, StreamingMode streamingMode) {
			this.begin = MappedRangeQueryV2.this.begin;
			this.end = MappedRangeQueryV2.this.end;
			this.rowsLimited = rowLimit != 0;
			this.rowsRemaining = rowLimit;
			this.reverse = reverse;
			this.paramsBytes = paramsBytes;
			this.streamingMode = streamingMode;
			startNextFetch();
		}

		private synchronized boolean mainChunkIsTheLast() { return !chunk.more || (rowsLimited && rowsRemaining < 1); }

		class FetchComplete implements BiConsumer<MappedRangeResultInfoV2, Throwable> {
			final FutureMappedResultsV2 fetchingChunk;
			final CompletableFuture<Boolean> promise;

			FetchComplete(FutureMappedResultsV2 fetch, CompletableFuture<Boolean> promise) {
				this.fetchingChunk = fetch;
				this.promise = promise;
			}

			@Override
			public void accept(MappedRangeResultInfoV2 data, Throwable error) {
				try {
					if (error != null) {
						if (eventKeeper != null) {
							eventKeeper.increment(Events.RANGE_QUERY_CHUNK_FAILED);
						}
						promise.completeExceptionally(error);
						if (error instanceof Error) {
							throw(Error) error;
						}

						return;
					}

					final MappedRangeResultV2 rangeResult = data.get();
					final RangeResultSummary summary = rangeResult.getSummary();
					if (summary.lastKey == null) {
						promise.complete(Boolean.FALSE);
						return;
					}

					synchronized (AsyncRangeIteratorV2.this) {
						fetchOutstanding = false;

						// adjust the total number of rows we should ever fetch
						rowsRemaining -= summary.keyCount;

						// set up the next fetch
						if (reverse) {
							end = KeySelector.firstGreaterOrEqual(summary.lastKey);
						} else {
							begin = KeySelector.firstGreaterThan(summary.lastKey);
						}

						// If this is the first fetch or the main chunk is exhausted
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
			if (fetchOutstanding)
				throw new IllegalStateException("Reentrant call not allowed"); // This can not be called reentrantly
			if (isCancelled) return;

			if (chunk != null && mainChunkIsTheLast()) return;

			fetchOutstanding = true;
			nextChunk = null;

			nextFuture = new CompletableFuture<>();
			final long sTime = System.nanoTime();
			fetchingChunk =
			    tr.getMappedRange_internal_v2(begin, end, mapper, paramsBytes, rowsLimited ? rowsRemaining : 0, 0,
			                                  streamingMode.code(), ++iteration, snapshot, reverse);
			BiConsumer<MappedRangeResultInfoV2, Throwable> cons = new FetchComplete(fetchingChunk, nextFuture);
			if (eventKeeper != null) {
				eventKeeper.increment(Events.RANGE_QUERY_FETCHES);
				cons = cons.andThen((r, t) -> {
					eventKeeper.timeNanos(Events.RANGE_QUERY_FETCH_TIME_NANOS, System.nanoTime() - sTime);
				});
			}

			fetchingChunk.whenComplete(cons);
		}

		@Override
		public synchronized CompletableFuture<Boolean> onHasNext() {
			if (isCancelled) throw new CancellationException();

			// This will only happen before the first fetch has completed
			if (chunk == null) {
				return nextFuture;
			}

			// We have a chunk and are still working though it
			if (index < chunk.values.size()) {
				return AsyncUtil.READY_TRUE;
			}

			// If we are at the end of the current chunk there is either:
			//   - no more data -or-
			//   - we are already fetching the next block
			return mainChunkIsTheLast() ? AsyncUtil.READY_FALSE : nextFuture;
		}

		@Override
		public boolean hasNext() {
			return onHasNext().join();
		}

		@Override
		public MappedKeyValueV2 next() {
			CompletableFuture<Boolean> nextFuture;
			synchronized (this) {
				if (isCancelled) throw new CancellationException();

				// at least the first chunk has been fetched and there is at least one
				//  available result
				if (chunk != null && index < chunk.values.size()) {
					// If this is the first call to next() on a chunk, then we will want to
					//  start fetching the data for the next block
					boolean initialNext = index == 0;

					MappedKeyValueV2 result = chunk.values.get(index);
					prevKey = result.getKey();
					index++;

					if (eventKeeper != null) {
						// We record the BYTES_FETCHED here, rather than at a lower level,
						// because some parts of the construction of a MappedRangeResult occur underneath
						// the JNI boundary, and we don't want to pass the eventKeeper down there
						// (note: account for the length fields as well when recording the bytes
						// fetched)
						eventKeeper.count(Events.BYTES_FETCHED, result.getKey().length + result.getValue().length + 8);
						eventKeeper.increment(Events.RANGE_QUERY_RECORDS_FETCHED);
					}

					// If this is the first call to next() on a chunk there cannot
					//  be another waiting, since we could not have issued a request
					assert (!(initialNext && nextChunk != null));

					// we are at the end of the current chunk and there is more to be had already
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

			// If there was no result ready then we need to wait on the future
			//  and return the proper result, throwing if there are no more elements
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
}
