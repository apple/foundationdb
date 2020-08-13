/*
 * DirectRangeIterator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;

/**
 * Represents the {@code Iterator} used by {@link DirectRangeQuery}. This class is
 * responsible to actually make the requests to FDB server as we stream along. Make sure
 * {@code Close()} is called to avoid leaks of {@link DirectBufferIterator} from {@link BufferPool}.
 */
// TODO (Vishesh): DRY. Shares lot of code with RangeQuery::Iterator.
class DirectRangeIterator implements AsyncIterator<KeyValue> {
	private final FDBTransaction tr;

	// immutable aspects of this iterator
	private final boolean rowsLimited;
	private final boolean reverse;
	private final StreamingMode streamingMode;
	private final boolean snapshot;

	// There is the chance for parallelism in the two "chunks" for fetched data
	private Queue<DirectBufferIterator> resultChunks = new ArrayDeque<DirectBufferIterator>();
	private CompletableFuture<DirectBufferIterator> fetchingChunk;
	private CompletableFuture<Boolean> nextFuture;

	private boolean isCancelled = false;
	private boolean fetchOutstanding = false;

	private byte[] prevKey = null;
	private int iteration = 0;
	private KeySelector begin;
	private KeySelector end;

	private int rowsRemaining;

	DirectRangeIterator(FDBTransaction transaction, boolean isSnapshot, KeySelector begin, KeySelector end,
			int rowLimit, boolean reverse, StreamingMode streamingMode) {
		this.tr = transaction;
		this.begin = begin;
		this.end = end;
		this.snapshot = isSnapshot;
		this.rowsLimited = rowLimit != 0;
		this.rowsRemaining = rowLimit;
		this.reverse = reverse;
		this.streamingMode = streamingMode;

		startNextFetch();
	}

	private DirectBufferIterator currentChunk() {
		return resultChunks.peek();
	}

	private synchronized void startNextFetch() {
		if (fetchOutstanding)
			throw new IllegalStateException("Reentrant call not allowed"); // This can not be called reentrantly

		if (isCancelled) {
			return;
		}

		if (currentChunk() != null && mainChunkIsTheLast()) {
			return;
		}

		fetchOutstanding = true;

		DirectBufferIterator iter = new DirectBufferIterator();
		nextFuture = iter.onResultReady();

		fetchingChunk = CompletableFuture.supplyAsync(() -> {
			try {
				iter.init();
				tr.getDirectRange_internal(iter, begin, end, rowsLimited ? rowsRemaining : 0, 0, streamingMode.code(),
						++iteration, snapshot, reverse);
				return iter;
			} catch (InterruptedException e) {
				synchronized (DirectRangeIterator.this) {
					iter.close();
				}
				throw new CompletionException(e);
			}
		}, tr.getExecutor());

		fetchingChunk.whenComplete(new FetchComplete(fetchingChunk));
	}

	@Override
	public synchronized CompletableFuture<Boolean> onHasNext() {
		if (isCancelled)
			throw new CancellationException();

		// This will only happen before the first fetch has completed
		if (currentChunk() == null) {
			return nextFuture;
		}

		// We have a chunk and are still working though it
		if (currentChunk().hasNext()) {
			return AsyncUtil.READY_TRUE;
		}

		// If we are at the end of the current chunk there is either:
		// - no more data -or-
		// - we are already fetching the next block
		return mainChunkIsTheLast() ? AsyncUtil.READY_FALSE : nextFuture;
	}

	@Override
	public boolean hasNext() {
		return onHasNext().join();
	}

	@Override
	public KeyValue next() {
		CompletableFuture<Boolean> nextFuture;
		synchronized (this) {
			if (isCancelled)
				throw new CancellationException();

			freeChunks();

			if (currentChunk() != null && currentChunk().hasNext()) {
				// If this is the first call to next() on a chunk, then we will want to
				// start fetching the data for the next block
				boolean fetchNextChunk = (currentChunk().currentIndex() == 0);

				KeyValue result = currentChunk().next();
				prevKey = result.getKey();

				freeChunks();

				if (fetchNextChunk) {
					startNextFetch();
				}

				return result;
			}

			nextFuture = onHasNext();
		}

		// If there was no result ready then we need to wait on the future
		// and return the proper result, throwing if there are no more elements
		return nextFuture.thenApply(hasNext -> {
			if (hasNext) {
				return next();
			}
			throw new NoSuchElementException();
		}).join();
	}

	@Override
	public synchronized void remove() {
		if (prevKey == null)
			throw new IllegalStateException("No value has been fetched from database");

		tr.clear(prevKey);
	}

	@Override
	public synchronized void cancel() {
		isCancelled = true;
		fetchingChunk.cancel(true);
		nextFuture.cancel(true);
		while (!resultChunks.isEmpty()) {
			DirectBufferIterator it = resultChunks.poll();
			it.onResultReady().cancel(true);
			it.close();
		}
	}

	private synchronized boolean mainChunkIsTheLast() {
		return !currentChunk().getSummary().more || (rowsLimited && rowsRemaining < 1);
	}

	private boolean isNextChunkReady() {
		return resultChunks.size() > 1;
	}

	private void freeChunks() {
		if (currentChunk() != null && !currentChunk().hasNext() && isNextChunkReady()) {
			resultChunks.poll().close();
		}
	}

	class FetchComplete implements BiConsumer<DirectBufferIterator, Throwable> {
		final CompletableFuture<DirectBufferIterator> fetchingChunk;

		FetchComplete(CompletableFuture<DirectBufferIterator> fetch) {
			this.fetchingChunk = fetch;
		}

		@Override
		public void accept(DirectBufferIterator iter, Throwable error) {
			CompletableFuture<Boolean> promise = iter.onResultReady();
			if (error != null) {
				promise.completeExceptionally(error);
				if (error instanceof Error) {
					throw (Error) error;
				}

				return;
			}

			synchronized (DirectRangeIterator.this) {
				iter.readSummary();
			}

			final RangeResultSummary summary = iter.getSummary();
			if (summary.lastKey == null) {
				promise.complete(Boolean.FALSE);
				iter.close();
				return;
			}

			synchronized (DirectRangeIterator.this) {
				// adjust the total number of rows we should ever fetch
				rowsRemaining -= summary.keyCount;

				// set up the next fetch
				if (reverse) {
					end = KeySelector.firstGreaterOrEqual(summary.lastKey);
				} else {
					begin = KeySelector.firstGreaterThan(summary.lastKey);
				}

				fetchOutstanding = false;
				freeChunks();
				resultChunks.offer(iter);
			}

			promise.complete(Boolean.TRUE);
		}
	}
}
