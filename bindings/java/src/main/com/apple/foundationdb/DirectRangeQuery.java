/*
 * DirectRangeQuery.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
 *
 * NOTE: This differs from {@link RangeQuery}, by the fact that it uses much higher performance 
 *    {@link DirectByteBuffer} and {@link getDirectRange_internal} instead of {@link getRange_internal}
 *    which avoid several JNI calls, and memory allocations.
 */
public class DirectRangeQuery implements AsyncIterable<KeyValue>, Iterable<KeyValue> {
	private final FDBTransaction tr;
	private KeySelector begin;
	private KeySelector end;
	private final boolean snapshot;
	private final int rowLimit;
	private final boolean reverse;
	private final StreamingMode streamingMode;

	DirectRangeQuery(FDBTransaction transaction, boolean isSnapshot, KeySelector begin, KeySelector end, int rowLimit,
			boolean reverse, StreamingMode streamingMode) {
		this.tr = transaction;
		this.begin = begin;
		this.end = end;
		this.snapshot = isSnapshot;
		this.rowLimit = rowLimit;
		this.reverse = reverse;
		this.streamingMode = streamingMode;
	}

	@Override
	public AsyncIterator<KeyValue> iterator() {
		return new DirectRangeIterator(tr, snapshot, begin, end, rowLimit, reverse, streamingMode);
	}

	@Override
	public CompletableFuture<List<KeyValue>> asList() {
		StreamingMode mode = this.streamingMode;
		if (mode == StreamingMode.ITERATOR)
			mode = (this.rowLimit == 0) ? StreamingMode.WANT_ALL : StreamingMode.EXACT;

		// TODO (Vishesh) Update for DirectRangeQuery? We may not have big enough `DirectByteBuffer`.
		// If the streaming mode is EXACT, try and grab things as one chunk
		if (mode == StreamingMode.EXACT) {
			FutureResults range = tr.getRange_internal(this.begin, this.end, this.rowLimit, 0,
					StreamingMode.EXACT.code(), 1, this.snapshot, this.reverse);
			return range.thenApply(result -> result.get().values).whenComplete((result, e) -> range.close());
		}

		// If the streaming mode is not EXACT, simply collect the results of an
		// iteration into a list
		return AsyncUtil.collect(new DirectRangeQuery(tr, snapshot, begin, end, rowLimit, reverse, mode),
				tr.getExecutor());
	}
}
