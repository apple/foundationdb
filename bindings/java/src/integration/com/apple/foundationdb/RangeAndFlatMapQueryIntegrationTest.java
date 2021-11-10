/*
 * RangeAndFlatMapQueryIntegrationTest.java
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RequiresDatabase.class)
class RangeAndFlatMapQueryIntegrationTest {
	private static final FDB fdb = FDB.selectAPIVersion(710);
	public String databaseArg = null;
	private Database openFDB() { return fdb.open(databaseArg); }

	@BeforeEach
	@AfterEach
	void clearDatabase() throws Exception {
		/*
		 * Empty the database before and after each run, just in case
		 */
		try (Database db = openFDB()) {
			db.run(tr -> {
				tr.clear(Range.startsWith(new byte[] { (byte)0x00 }));
				return null;
			});
		}
	}

	static private final byte[] EMPTY = Tuple.from().pack();
	static private final String PREFIX = "prefix";
	static private final String RECORD = "RECORD";
	static private final String INDEX = "INDEX";
	static private String primaryKey(int i) { return String.format("primary-key-of-record-%08d", i); }
	static private String indexKey(int i) { return String.format("index-key-of-record-%08d", i); }
	static private String dataOfRecord(int i) { return String.format("data-of-record-%08d", i); }

	static byte[] MAPPER = Tuple.from(PREFIX, RECORD, "{K[3]}").pack();
	static private byte[] indexEntryKey(final int i) {
		return Tuple.from(PREFIX, INDEX, indexKey(i), primaryKey(i)).pack();
	}
	static private byte[] recordKey(final int i) { return Tuple.from(PREFIX, RECORD, primaryKey(i)).pack(); }
	static private byte[] recordValue(final int i) { return Tuple.from(dataOfRecord(i)).pack(); }

	static private void insertRecordWithIndex(final Transaction tr, final int i) {
		tr.set(indexEntryKey(i), EMPTY);
		tr.set(recordKey(i), recordValue(i));
	}

	private static String getArgFromEnv() {
		String[] clusterFiles = MultiClientHelper.readClusterFromEnv();
		String cluster = clusterFiles[0];
		System.out.printf("Using Cluster: %s\n", cluster);
		return cluster;
	}
	public static void main(String[] args) throws Exception {
		final RangeAndFlatMapQueryIntegrationTest test = new RangeAndFlatMapQueryIntegrationTest();
		test.databaseArg = getArgFromEnv();
		test.clearDatabase();
		test.comparePerformance();
		test.clearDatabase();
	}

	int numRecords = 10000;
	int numQueries = 10000;
	int numRecordsPerQuery = 100;
	boolean validate = false;
	@Test
	void comparePerformance() {
		FDB fdb = FDB.selectAPIVersion(710);
		try (Database db = openFDB()) {
			insertRecordsWithIndexes(numRecords, db);
			instrument(rangeQueryAndGet, "rangeQueryAndGet", db);
			instrument(rangeQueryAndFlatMap, "rangeQueryAndFlatMap", db);
		}
	}

	private void instrument(final RangeQueryWithIndex query, final String name, final Database db) {
		System.out.printf("Starting %s (numQueries:%d, numRecordsPerQuery:%d)\n", name, numQueries, numRecordsPerQuery);
		long startTime = System.currentTimeMillis();
		for (int queryId = 0; queryId < numQueries; queryId++) {
			int begin = ThreadLocalRandom.current().nextInt(numRecords - numRecordsPerQuery);
			query.run(begin, begin + numRecordsPerQuery, db);
		}
		long time = System.currentTimeMillis() - startTime;
		System.out.printf("Finished %s, it takes %d ms for %d queries (%d qps)\n", name, time, numQueries,
		                  numQueries * 1000L / time);
	}

	static private final int RECORDS_PER_TXN = 100;
	static private void insertRecordsWithIndexes(int n, Database db) {
		int i = 0;
		while (i < n) {
			int begin = i;
			int end = Math.min(n, i + RECORDS_PER_TXN);
			// insert [begin, end) in one transaction
			db.run(tr -> {
				for (int t = begin; t < end; t++) {
					insertRecordWithIndex(tr, t);
				}
				return null;
			});
			i = end;
		}
	}

	public interface RangeQueryWithIndex {
		void run(int begin, int end, Database db);
	}

	RangeQueryWithIndex rangeQueryAndGet = (int begin, int end, Database db) -> db.run(tr -> {
		try {
			List<KeyValue> kvs = tr.getRange(KeySelector.firstGreaterOrEqual(indexEntryKey(begin)),
			                                 KeySelector.firstGreaterOrEqual(indexEntryKey(end)),
			                                 ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL)
			                         .asList()
			                         .get();
			Assertions.assertEquals(end - begin, kvs.size());

			// Get the records of each index entry IN PARALLEL.
			List<CompletableFuture<byte[]>> resultFutures = new ArrayList<>();
			// In reality, we need to get the record key by parsing the index entry key. But considering this is a
			// performance test, we just ignore the returned key and simply generate it from recordKey.
			for (int id = begin; id < end; id++) {
				resultFutures.add(tr.get(recordKey(id)));
			}
			AsyncUtil.whenAll(resultFutures).get();

			if (validate) {
				final Iterator<KeyValue> indexes = kvs.iterator();
				final Iterator<CompletableFuture<byte[]>> records = resultFutures.iterator();
				for (int id = begin; id < end; id++) {
					Assertions.assertTrue(indexes.hasNext());
					assertByteArrayEquals(indexEntryKey(id), indexes.next().getKey());
					Assertions.assertTrue(records.hasNext());
					assertByteArrayEquals(recordValue(id), records.next().get());
				}
				Assertions.assertFalse(indexes.hasNext());
				Assertions.assertFalse(records.hasNext());
			}
		} catch (Exception e) {
			Assertions.fail("Unexpected exception", e);
		}
		return null;
	});

	RangeQueryWithIndex rangeQueryAndFlatMap = (int begin, int end, Database db) -> db.run(tr -> {
		try {
			tr.options().setReadYourWritesDisable();
			List<KeyValue> kvs =
			    tr.getRangeAndFlatMap(KeySelector.firstGreaterOrEqual(indexEntryKey(begin)),
			                          KeySelector.firstGreaterOrEqual(indexEntryKey(end)), MAPPER,
			                          ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL)
			        .asList()
			        .get();
			Assertions.assertEquals(end - begin, kvs.size());

			if (validate) {
				final Iterator<KeyValue> results = kvs.iterator();
				for (int id = begin; id < end; id++) {
					Assertions.assertTrue(results.hasNext());
					assertByteArrayEquals(recordValue(id), results.next().getValue());
				}
				Assertions.assertFalse(results.hasNext());
			}
		} catch (Exception e) {
			Assertions.fail("Unexpected exception", e);
		}
		return null;
	});

	void assertByteArrayEquals(byte[] expected, byte[] actual) {
		Assertions.assertEquals(ByteArrayUtil.printable(expected), ByteArrayUtil.printable(actual));
	}

	@Test
	void rangeAndFlatMapQueryOverMultipleRows() throws Exception {
		try (Database db = openFDB()) {
			insertRecordsWithIndexes(3, db);

			List<byte[]> expected_data_of_records = new ArrayList<>();
			for (int i = 0; i <= 1; i++) {
				expected_data_of_records.add(recordValue(i));
			}

			db.run(tr -> {
				// getRangeAndFlatMap is only support without RYW. This is a must!!!
				tr.options().setReadYourWritesDisable();

				Iterator<KeyValue> kvs =
				    tr.getRangeAndFlatMap(KeySelector.firstGreaterOrEqual(indexEntryKey(0)),
				                          KeySelector.firstGreaterThan(indexEntryKey(1)), MAPPER,
				                          ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL)
				        .iterator();
				Iterator<byte[]> expected_data_of_records_iter = expected_data_of_records.iterator();
				while (expected_data_of_records_iter.hasNext()) {
					Assertions.assertTrue(kvs.hasNext(), "iterator ended too early");
					KeyValue kv = kvs.next();
					byte[] actual_data_of_record = kv.getValue();
					byte[] expected_data_of_record = expected_data_of_records_iter.next();

					// System.out.println("result key:" + ByteArrayUtil.printable(kv.getKey()) + " value:" +
					// ByteArrayUtil.printable(kv.getValue())); Output:
					// result
					// key:\x02prefix\x00\x02INDEX\x00\x02index-key-of-record-0\x00\x02primary-key-of-record-0\x00
					// value:\x02data-of-record-0\x00
					// result
					// key:\x02prefix\x00\x02INDEX\x00\x02index-key-of-record-1\x00\x02primary-key-of-record-1\x00
					// value:\x02data-of-record-1\x00

					// For now, we don't guarantee what that the returned keys mean.
					Assertions.assertArrayEquals(expected_data_of_record, actual_data_of_record,
					                             "Incorrect data of record!");
				}
				Assertions.assertFalse(kvs.hasNext(), "Iterator returned too much data");

				return null;
			});
		}
	}
}
