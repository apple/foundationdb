/*
 * LocalityUtil.java
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

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.tuple.ByteArrayUtil;

/**
 * The FoundationDB API comes with a set of functions for discovering the
 * storage locations of keys within your cluster. This information can be useful
 * for advanced users who wish to take into account the location of keys in the
 * design of applications or processes.
 *
 */
public class LocalityUtil {
	/**
	 * Returns a {@code CloseableAsyncIterator} of keys {@code k} such that
	 * {@code begin <= k < end} and {@code k} is located at the start of a
	 * contiguous range stored on a single server.<br>
	 *<br>
	 * This method is not transactional. The returned boundaries are an estimate
	 * and may not represent the exact boundary locations at any database version.
	 *
	 * @param db the database to query
	 * @param begin the inclusive start of the range
	 * @param end the exclusive end of the range
	 *
	 * @return an sequence of keys denoting the start of single-server ranges
	 */
	public static CloseableAsyncIterator<byte[]> getBoundaryKeys(Database db, byte[] begin, byte[] end) {
		return getBoundaryKeys_internal(db.createTransaction(), begin, end);
	}

	/**
	 * Returns a {@code CloseableAsyncIterator} of keys {@code k} such that
	 * {@code begin <= k < end} and {@code k} is located at the start of a
	 * contiguous range stored on a single server.<br>
	 *<br>
	 * This method is not transactional. The returned boundaries
	 * are an estimate and may not represent the exact boundary locations at
	 * any database version. The passed {@code Transaction} is not used
	 * for reads directly, instead it is used to get access to associated
	 * {@link Database}. As a result, options (such as retry limit) set on the
	 * passed {@code Transaction} will not be applied. If, however, the passed
	 * {@code Transaction} has already gotten a read version there is some
	 * latency advantage to using this form of the method. Also, if the database
	 * is unavailable prior to the function call, any timeout set on the
	 * passed {@code Transaction} will still trigger.
	 *
	 * @param tr the transaction on which to base the query
	 * @param begin the inclusive start of the range
	 * @param end the exclusive end of the range
	 *
	 * @return an sequence of keys denoting the start of single-server ranges
	 */
	public static CloseableAsyncIterator<byte[]> getBoundaryKeys(Transaction tr, byte[] begin, byte[] end) {
		Transaction local = tr.getDatabase().createTransaction(tr.getExecutor());
		CompletableFuture<Long> readVersion = tr.getReadVersion();
		if(readVersion.isDone() && !readVersion.isCompletedExceptionally()) {
			local.setReadVersion(readVersion.getNow(null));
		}
		return new BoundaryIterator(local, begin, end);
	}

	/**
	 * Returns a list of public network addresses as strings, one for each of
	 * the storage servers responsible for storing {@code key} and its associated
	 * value.
	 *
	 * The storage server network ports are not included to the list unless
	 * tr.options().{@link com.apple.foundationdb.TransactionOptions#setIncludePortInAddress() setIncludePortInAddress()}
	 * has been called.
	 *
	 * If locality information is not available, the returned future will carry a
	 *  {@link FDBException} locality_information_unavailable.
	 *
	 * @param tr the transaction in which to gather location information
	 * @param key the key for which to gather location information
	 *
	 * @return a list of addresses in string form
	 */
	public static CompletableFuture<String[]> getAddressesForKey(Transaction tr, byte[] key) {
		if (!(tr instanceof FDBTransaction)) {
			CompletableFuture<String[]> future = new CompletableFuture<>();
			future.completeExceptionally(new FDBException("locality_information_unavailable", 1033));
			return future;
		}
		return ((FDBTransaction)tr).getAddressesForKey(key);
	}

	private static CloseableAsyncIterator<byte[]> getBoundaryKeys_internal(Transaction tr, byte[] begin, byte[] end) {
		return new BoundaryIterator(tr, begin, end);
	}

	static class BoundaryIterator implements CloseableAsyncIterator<byte[]> {
		Transaction tr;
		byte[] begin;
		byte[] lastBegin;
		final byte[] end;
		final AsyncIterable<KeyValue> firstGet;

		AsyncIterator<KeyValue> block;
		private CompletableFuture<Boolean> nextFuture;
		private boolean closed;

		BoundaryIterator(Transaction tr, byte[] begin, byte[] end) {
			this.tr = tr;
			this.begin = Arrays.copyOf(begin, begin.length);
			this.end = Arrays.copyOf(end, end.length);

			lastBegin = begin;

			tr.options().setReadSystemKeys();
			tr.options().setLockAware();

			firstGet = tr.getRange(keyServersForKey(begin), keyServersForKey(end));
			block = firstGet.iterator();
			nextFuture = AsyncUtil.composeHandleAsync(block.onHasNext(), handler, tr.getExecutor());

			closed = false;
		}

		@Override
		public CompletableFuture<Boolean> onHasNext() {
			return nextFuture;
		}

		@Override
		public boolean hasNext() {
			return nextFuture.join();
		}

		CompletableFuture<Boolean> restartGet() {
			if(ByteArrayUtil.compareUnsigned(begin, end) >= 0) {
				return AsyncUtil.READY_FALSE;
			}
			lastBegin = begin;
			tr.options().setReadSystemKeys();
			block = tr.getRange(
					keyServersForKey(begin),
					keyServersForKey(end)).iterator();
			nextFuture = AsyncUtil.composeHandleAsync(block.onHasNext(), handler, tr.getExecutor());
			return nextFuture;
		}

		BiFunction<Boolean, Throwable, CompletableFuture<Boolean>> handler = new BiFunction<Boolean, Throwable, CompletableFuture<Boolean>>() {
			@Override
			public CompletableFuture<Boolean> apply(Boolean b, Throwable o) {
				if(b != null) {
					return CompletableFuture.completedFuture(b);
				}
				if(o instanceof FDBException) {
					FDBException err = (FDBException) o;
					if(err.getCode() == 1007 && !Arrays.equals(begin, lastBegin)) {
						Executor executor = BoundaryIterator.this.tr.getExecutor();
						BoundaryIterator.this.tr.close();
						BoundaryIterator.this.tr = BoundaryIterator.this.tr.getDatabase().createTransaction(executor);
						return restartGet();
					}
				}

				if(!(o instanceof RuntimeException))
					throw new CompletionException(o);

				CompletableFuture<Transaction> onError = BoundaryIterator.this.tr.onError(o);
				return onError.thenComposeAsync(tr -> {
					BoundaryIterator.this.tr = tr;
					return restartGet();
				}, tr.getExecutor());
			}
		};

		@Override
		public byte[] next() {
			if(!nextFuture.isDone()) {
				throw new IllegalStateException("Call to next without hasNext()=true");
			}
			KeyValue o = block.next();
			byte[] key = o.getKey();
			byte[] suffix = Arrays.copyOfRange(key, 13, key.length);
			BoundaryIterator.this.begin = ByteArrayUtil.join(suffix, new byte[] { (byte)0 });
			nextFuture = AsyncUtil.composeHandleAsync(block.onHasNext(), handler, tr.getExecutor());
			return suffix;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Boundary keys are read-only");
		}

		@Override
		public void close() {
			BoundaryIterator.this.tr.close();
			closed = true;
		}

		@Override
		protected void finalize() throws Throwable {
			try {
				if(FDB.instance().warnOnUnclosed && !closed) {
					System.err.println("CloseableAsyncIterator not closed (getBoundaryKeys)");
				}
				if(!closed) {
					close();
				}
			}
			finally {
				super.finalize();
			}
		}
	}

	private static final Charset ASCII = Charset.forName("US-ASCII");
	static byte[] keyServersForKey(byte[] key) {
		return ByteArrayUtil.join(new byte[] { (byte)255 },
							  "/keyServers/".getBytes(ASCII),
							  key);
	}

	private LocalityUtil() {}
}
