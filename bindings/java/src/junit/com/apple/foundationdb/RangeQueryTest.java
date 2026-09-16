/*
 * RangeQueryTest.java
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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests around the Range Query logic.
 *
 * These tests do _not_ require a running FDB server to function. Instead, we
 * are operating on a good-faith "The underlying native library is correct"
 * functionality. For end-to-end tests which require a running server, see the
 * src/tests source folder.
 */
class RangeQueryTest {
	private static Executor EXECUTOR = new Executor() {
		@Override
		public void execute(Runnable command) {
			command.run();
		}
	};

	private static Database makeFakeDatabase(List<Map.Entry<byte[], byte[]>> data) {
		return new Database() {
			// the choice of 3 is arbitrary, just trying to make sure it's unique(ish) in case we
			// need to test that uniqueness later.
			private long txnCounter = 3;

			@Override
			public Transaction createTransaction() {
				long tId = txnCounter;
				txnCounter++;
				return new FakeFDBTransaction(data, tId, this, EXECUTOR);
			}

			@Override
			protected void finalize() throws Throwable {
				// no-op
			}

			@Override
			public void close() {
				// no-op
			}

			@Override
			public Executor getExecutor() {
				throw new UnsupportedOperationException("Unimplemented method 'getExecutor'");
			}

			@Override
			public Transaction createTransaction(Executor e) {
				throw new UnsupportedOperationException("Unimplemented method 'createTransaction'");
			}

			@Override
			public Transaction createTransaction(Executor e, EventKeeper eventKeeper) {
				throw new UnsupportedOperationException("Unimplemented method 'createTransaction'");
			}

			@Override
			public DatabaseOptions options() {
				throw new UnsupportedOperationException("Unimplemented method 'options'");
			}

			@Override
			public double getMainThreadBusyness() {
				throw new UnsupportedOperationException("Unimplemented method 'getMainThreadBusyness'");
			}

			@Override
			public <T> T read(Function<? super ReadTransaction, T> retryable, Executor e) {
				throw new UnsupportedOperationException("Unimplemented method 'read'");
			}

			@Override
			public <T> CompletableFuture<T> readAsync(
					Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable, Executor e) {
				throw new UnsupportedOperationException("Unimplemented method 'readAsync'");
			}

			@Override
			public <T> T run(Function<? super Transaction, T> retryable, Executor e) {
				throw new UnsupportedOperationException("Unimplemented method 'run'");
			}

			@Override
			public <T> CompletableFuture<T> runAsync(
					Function<? super Transaction, ? extends CompletableFuture<T>> retryable, Executor e) {
				throw new UnsupportedOperationException("Unimplemented method 'runAsync'");
			}

			@Override
			public CompletableFuture<byte[]> getClientStatus(Executor e) {
				throw new UnsupportedOperationException("Unimplemented method 'getClientStatus'");
			}
		};
	}

	@ParameterizedTest
	@EnumSource(StreamingMode.class)
	void testRangeScansWorkWithoutRowLimit(StreamingMode mode) throws Exception {
		/*
		 * Test that the Range scan will return all the rows without the row limit.
		 */
		List<Map.Entry<byte[], byte[]>> data = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			data.add(new AbstractMap.SimpleEntry<>(("apple" + i).getBytes(), ("crunchy" + i).getBytes()));
		}

		try (Database db = makeFakeDatabase(data)) {
			try (Transaction tr = db.createTransaction()) {
				byte[] val = tr.get("apple4".getBytes()).join();
				Assertions.assertNotNull(val, "Missing entry for 'apple4'!");
				Assertions.assertArrayEquals(val, "crunchy4".getBytes(), "incorrect entry for 'apple4'~");

				// now do a range scan on the whole data set
				AsyncIterable<KeyValue> iter = tr.getRange("a".getBytes(), "b".getBytes(), 0, false, mode);
				List<KeyValue> kvs = iter.asList().join();
				for (Map.Entry<byte[], byte[]> entry : data) {
					boolean found = false;
					for (KeyValue actualKv : kvs) {
						if (ByteArrayUtil.compareTo(entry.getKey(), 0, entry.getKey().length, actualKv.getKey(), 0,
						                            actualKv.getKey().length) == 0) {
							String errorMsg =
							    String.format("Incorrect value for key '%s'; Expected: <%s>, Actual: <%s>",
							                  new String(entry.getKey()), new String(entry.getValue()),
							                  new String(actualKv.getValue()));
							Assertions.assertEquals(
							    0,
							    ByteArrayUtil.compareTo(entry.getValue(), 0, entry.getValue().length,
							                            actualKv.getValue(), 0, actualKv.getValue().length),
							    errorMsg);
							found = true;
							break;
						}
					}
					Assertions.assertTrue(found, "Did not find key '" + new String(entry.getKey()) + "'");
				}
			}
		}
	}

	@ParameterizedTest
	@EnumSource(StreamingMode.class)
	void testRangeScansWorkWithRowLimit(StreamingMode mode) throws Exception {
		/*
		 * Basic test to make sure that we don't ask for too many records or return too
		 * much data when exercising the row limit
		 */
		List<Map.Entry<byte[], byte[]>> data = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			data.add(new AbstractMap.SimpleEntry<>(("apple" + i).getBytes(), ("crunchy" + i).getBytes()));
		}

		try (Database db = makeFakeDatabase(data)) {
			try (Transaction tr = db.createTransaction()) {
				byte[] val = tr.get("apple4".getBytes()).join();
				Assertions.assertNotNull(val, "Missing entry for 'apple4'!");
				Assertions.assertArrayEquals(val, "crunchy4".getBytes(), "incorrect entry for 'apple4'~");

				// now do a range scan on the whole data set
				int limit = 3;
				AsyncIterable<KeyValue> iter = tr.getRange("a".getBytes(), "b".getBytes(), limit, false, mode);
				List<KeyValue> kvs = iter.asList().join();
				Assertions.assertEquals(limit, kvs.size(), "incorrect number of kvs returned!");
				int cnt = 0;
				for (Map.Entry<byte[], byte[]> entry : data) {
					boolean found = false;
					for (KeyValue actualKv : kvs) {
						if (ByteArrayUtil.compareTo(entry.getKey(), 0, entry.getKey().length, actualKv.getKey(), 0,
						                            actualKv.getKey().length) == 0) {
							String erroMsg = String.format("Incorrect value for key '%s'; Expected: <%s>, Actual: <%s>",
							                               new String(entry.getKey()), new String(entry.getValue()),
							                               new String(actualKv.getValue()));
							Assertions.assertEquals(
							    0,
							    ByteArrayUtil.compareTo(entry.getValue(), 0, entry.getValue().length,
							                            actualKv.getValue(), 0, actualKv.getValue().length),
							    erroMsg);
							found = true;
							break;
						}
					}
					Assertions.assertTrue(found, "Did not find key '" + new String(entry.getKey()) + "'");
					cnt++;
					if (cnt == limit) {
						break;
					}
				}

				Assertions.assertEquals(1, ((FakeFDBTransaction)tr).getNumRangeCalls(),
				                        "Did not do the correct number of range requests");
			}
		}
	}

	@ParameterizedTest
	@EnumSource(StreamingMode.class)
	void testRangeScansWorkWithoutRowLimitReversed(StreamingMode mode) throws Exception {
		/*
		 * Test that the Range scan will return all the rows without the row limit.
		 */
		List<Map.Entry<byte[], byte[]>> data = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			data.add(new AbstractMap.SimpleEntry<>(("apple" + i).getBytes(), ("crunchy" + i).getBytes()));
		}

		try (Database db = makeFakeDatabase(data)) {
			try (Transaction tr = db.createTransaction()) {
				byte[] val = tr.get("apple4".getBytes()).join();
				Assertions.assertNotNull(val, "Missing entry for 'apple4'!");
				Assertions.assertArrayEquals(val, "crunchy4".getBytes(), "incorrect entry for 'apple4'~");

				// now do a range scan on the whole data set
				AsyncIterable<KeyValue> iter = tr.getRange("a".getBytes(), "b".getBytes(), 0, true, mode);
				List<KeyValue> kvs = iter.asList().join();
				for (Map.Entry<byte[], byte[]> entry : data) {
					boolean found = false;
					for (KeyValue actualKv : kvs) {
						if (ByteArrayUtil.compareTo(entry.getKey(), 0, entry.getKey().length, actualKv.getKey(), 0,
						                            actualKv.getKey().length) == 0) {
							String erroMsg = String.format("Incorrect value for key '%s'; Expected: <%s>, Actual: <%s>",
							                               new String(entry.getKey()), new String(entry.getValue()),
							                               new String(actualKv.getValue()));
							Assertions.assertEquals(
							    0,
							    ByteArrayUtil.compareTo(entry.getValue(), 0, entry.getValue().length,
							                            actualKv.getValue(), 0, actualKv.getValue().length),
							    erroMsg);
							found = true;
							break;
						}
					}
					Assertions.assertTrue(found, "Did not find key '" + new String(entry.getKey()) + "'");
				}
			}
		}
	}

	@ParameterizedTest
	@EnumSource(StreamingMode.class)
	void testRangeScansWorkWithRowLimitReversed(StreamingMode mode) throws Exception {
		/*
		 * Basic test to make sure that we don't ask for too many records or return too
		 * much data when exercising the row limit
		 */
		NavigableMap<byte[], byte[]> data = new TreeMap<>(ByteArrayUtil.comparator());
		for (int i = 0; i < 10; i++) {
			data.put(("apple" + i).getBytes(), ("crunchy" + i).getBytes());
		}

		try (Database db = makeFakeDatabase(new ArrayList<>(data.entrySet()))) {
			try (Transaction tr = db.createTransaction()) {
				byte[] val = tr.get("apple4".getBytes()).join();
				Assertions.assertNotNull(val, "Missing entry for 'apple4'!");
				Assertions.assertArrayEquals(val, "crunchy4".getBytes(), "incorrect entry for 'apple4'~");

				// now do a range scan on the whole data set
				int limit = 3;
				AsyncIterable<KeyValue> iter = tr.getRange("a".getBytes(), "b".getBytes(), limit, true, mode);
				List<KeyValue> kvs = iter.asList().join();
				Assertions.assertEquals(limit, kvs.size(), "incorrect number of kvs returned!");
				int cnt = 0;
				for (Map.Entry<byte[], byte[]> entry : data.descendingMap().entrySet()) {
					boolean found = false;
					for (KeyValue actualKv : kvs) {
						if (ByteArrayUtil.compareTo(entry.getKey(), 0, entry.getKey().length, actualKv.getKey(), 0,
						                            actualKv.getKey().length) == 0) {
							String erroMsg = String.format("Incorrect value for key '%s'; Expected: <%s>, Actual: <%s>",
							                               new String(entry.getKey()), new String(entry.getValue()),
							                               new String(actualKv.getValue()));
							Assertions.assertEquals(
							    0,
							    ByteArrayUtil.compareTo(entry.getValue(), 0, entry.getValue().length,
							                            actualKv.getValue(), 0, actualKv.getValue().length),
							    erroMsg);
							found = true;
							break;
						}
					}
					Assertions.assertTrue(found, "Did not find key '" + new String(entry.getKey()) + "'");
					cnt++;
					if (cnt == limit) {
						break;
					}
				}

				Assertions.assertEquals(1, ((FakeFDBTransaction)tr).getNumRangeCalls(),
				                        "Did not do the correct number of range requests");
			}
		}
	}

	private static class PendingRangeTransaction extends FakeFDBTransaction {
		final List<NativeFuture<?>> fetches = new ArrayList<>();
		final List<Runnable> completions = new ArrayList<>();
		final Set<Integer> closed = new HashSet<>();

		PendingRangeTransaction() { super(Collections.<KeyValue>emptyList(), 0, null, Runnable::run); }

		AsyncIterator<? extends KeyValue> iterator(boolean mapped) {
			KeySelector begin = KeySelector.firstGreaterOrEqual(new byte[] { 0 });
			KeySelector end = KeySelector.firstGreaterOrEqual(new byte[] { 9 });
			return mapped ? getMappedRange(begin, end, new byte[] { 1 }, 0, false, StreamingMode.ITERATOR).iterator()
			              : getRange(begin, end, 0, false, StreamingMode.ITERATOR).iterator();
		}

		@Override
		protected FutureResults getRange_internal(KeySelector begin, KeySelector end, int rowLimit, int targetBytes,
		                                          int streamingMode, int iteration, boolean snapshot, boolean reverse) {
			int page = fetches.size();
			FutureResults future = new FutureResults(0, false, Runnable::run, null) {
				@Override
				public RangeResult getResults() {
					return new RangeResult(
					    Arrays.asList(new KeyValue(new byte[] { (byte)(page * 2) }, new byte[0]),
					                  new KeyValue(new byte[] { (byte)(page * 2 + 1) }, new byte[0])),
					    page == 0);
				}

				@Override
				public void close() {
					closed.add(page);
				}
			};
			fetches.add(future);
			completions.add(() -> future.complete(new RangeResultInfo(future)));
			return future;
		}

		@Override
		protected FutureMappedResults getMappedRange_internal(KeySelector begin, KeySelector end, byte[] mapper,
		                                                      int rowLimit, int targetBytes, int streamingMode,
		                                                      int iteration, boolean snapshot, boolean reverse) {
			int page = fetches.size();
			FutureMappedResults future = new FutureMappedResults(0, false, Runnable::run, null) {
				@Override
				public MappedRangeResult getResults() {
					MappedKeyValue[] values = new MappedKeyValue[2];
					for (int i = 0; i < values.length; i++) {
						values[i] = new MappedKeyValue(new byte[] { (byte)(page * 2 + i) }, new byte[0], new byte[0],
						                               new byte[0], Collections.emptyList());
					}
					return new MappedRangeResult(values, page == 0);
				}

				@Override
				public void close() {
					closed.add(page);
				}
			};
			fetches.add(future);
			completions.add(() -> future.complete(new MappedRangeResultInfo(future)));
			return future;
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void delayedRangeFetchPreservesReentrantCompletionAndCleanup(boolean mapped) {
		PendingRangeTransaction tr = new PendingRangeTransaction();
		AsyncIterator<? extends KeyValue> iterator = tr.iterator(mapped);
		CompletableFuture<Boolean> firstReady = iterator.onHasNext();
		Assertions.assertFalse(firstReady.isDone());
		CompletableFuture<KeyValue> first = firstReady.thenApply(ready -> {
			Assertions.assertTrue(ready);
			Assertions.assertFalse(tr.closed.contains(0));
			return iterator.next();
		});
		tr.completions.get(0).run();
		Assertions.assertArrayEquals(new byte[] { 0 }, first.join().getKey());
		Assertions.assertEquals(2, tr.fetches.size());
		Assertions.assertEquals(Collections.singleton(0), tr.closed);
		Assertions.assertArrayEquals(new byte[] { 1 }, iterator.next().getKey());

		CompletableFuture<Boolean> secondReady = iterator.onHasNext();
		Assertions.assertFalse(secondReady.isDone());
		tr.completions.get(1).run();
		Assertions.assertTrue(secondReady.join());
		Assertions.assertArrayEquals(new byte[] { 2 }, iterator.next().getKey());
		Assertions.assertArrayEquals(new byte[] { 3 }, iterator.next().getKey());
		Assertions.assertFalse(iterator.hasNext());
		Assertions.assertEquals(2, tr.fetches.size());
		Assertions.assertEquals(new HashSet<>(Arrays.asList(0, 1)), tr.closed);
	}

	@ParameterizedTest
	@CsvSource({ "false, false", "false, true", "true, false", "true, true" })
	void cancellationCancelsAndClosesOutstandingRangeFetch(boolean mapped, boolean prefetch) {
		PendingRangeTransaction tr = new PendingRangeTransaction();
		AsyncIterator<? extends KeyValue> iterator = tr.iterator(mapped);
		if (prefetch) {
			tr.completions.get(0).run();
			iterator.next();
			iterator.next();
		}
		int pending = prefetch ? 1 : 0;
		CompletableFuture<Boolean> ready = iterator.onHasNext();
		Assertions.assertFalse(ready.isDone());
		iterator.cancel();
		Assertions.assertTrue(ready.isCancelled());
		Assertions.assertTrue(tr.fetches.get(pending).isCancelled());
		Assertions.assertTrue(tr.closed.contains(pending));
		tr.completions.get(pending).run();
		Assertions.assertThrows(CancellationException.class, iterator::onHasNext);
		Assertions.assertThrows(CancellationException.class, iterator::next);
		Assertions.assertEquals(pending + 1, tr.fetches.size());
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void failedRangeFetchPropagatesErrorAndClosesFuture(boolean mapped) {
		PendingRangeTransaction tr = new PendingRangeTransaction();
		AsyncIterator<? extends KeyValue> iterator = tr.iterator(mapped);
		CompletableFuture<Boolean> ready = iterator.onHasNext();
		RuntimeException failure = new RuntimeException("range fetch failed");
		tr.fetches.get(0).completeExceptionally(failure);
		Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class, ready::join).getCause());
		Assertions.assertEquals(Collections.singleton(0), tr.closed);
	}
}
