/*
 * RangeQuery.java
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
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;

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
class RangeQuery implements AsyncIterable<KeyValue> {
	private final FDBTransaction tr;
	private final KeySelector begin;
	private final KeySelector end;
	private final boolean snapshot;
	private final int rowLimit;
	private final boolean reverse;
	private final StreamingMode streamingMode;
	private final EventKeeper eventKeeper;

	RangeQuery(FDBTransaction transaction, boolean isSnapshot, KeySelector begin, KeySelector end, int rowLimit,
	           boolean reverse, StreamingMode streamingMode, EventKeeper eventKeeper) {
		this.tr = transaction;
		this.begin = begin;
		this.end = end;
		this.snapshot = isSnapshot;
		this.rowLimit = rowLimit;
		this.reverse = reverse;
		this.streamingMode = streamingMode;
		this.eventKeeper = eventKeeper;
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
	public CompletableFuture<List<KeyValue>> asList() {
		StreamingMode mode = this.streamingMode;
		if(mode == StreamingMode.ITERATOR)
			mode = (this.rowLimit == 0) ? StreamingMode.WANT_ALL : StreamingMode.EXACT;

		// if the streaming mode is EXACT, try and grab things as one chunk
		if(mode == StreamingMode.EXACT) {

			FutureResults range = tr.getRange_internal(this.begin, this.end, this.rowLimit, 0,
			                                           StreamingMode.EXACT.code(), 1, this.snapshot, this.reverse);
			return range.thenApply(result -> result.get().values)
					.whenComplete((result, e) -> range.close());
		}

		// If the streaming mode is not EXACT, simply collect the results of an
		// iteration into a list
		return AsyncUtil.collect(new RangeQuery(tr, snapshot, begin, end, rowLimit, reverse, mode, eventKeeper),
		                         tr.getExecutor());
	}

	/**
	 *  Returns an {@code Iterator} over the results of this query against FoundationDB.
	 *
	 *  @return an {@code Iterator} over type {@code KeyValue}.
	 */
	@Override
	public AsyncRangeIterator iterator() {
		return new AsyncRangeIterator();
	}

	private class AsyncRangeIterator extends RangeQueryIterator<KeyValue, RangeResultInfo> {
		private AsyncRangeIterator() {
			super(RangeQuery.this.tr, RangeQuery.this.begin, RangeQuery.this.end, RangeQuery.this.rowLimit,
			      RangeQuery.this.reverse, RangeQuery.this.eventKeeper);
			start();
		}

		@Override
		protected FutureResults fetch(KeySelector begin, KeySelector end, int rowLimit, int iteration) {
			return RangeQuery.this.tr.getRange_internal(begin, end, rowLimit, 0, streamingMode.code(), iteration,
			                                            snapshot, RangeQuery.this.reverse);
		}

		@Override
		protected Chunk<KeyValue> getChunk(RangeResultInfo data) {
			RangeResult result = data.get();
			return new Chunk<>(result.values, result.more);
		}
	}
}
