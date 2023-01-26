/*
 * MappedRangeQueryIntegrationTest.java
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
class MappedRangeQueryIntegrationTest {
	public static final int API_VERSION = 720;
	private static final FDB fdb = FDB.selectAPIVersion(API_VERSION);
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

	private static final byte[] EMPTY = Tuple.from().pack();
	private static final String PREFIX = "prefix";
	private static final String RECORD = "RECORD";
	private static final String INDEX = "INDEX";
	private static final String NOTFOUND = "NOT_FOUND";
	private static String primaryKey(int i) { return String.format("primary-key-of-record-%08d", i); }
	private static String indexKey(int i) { return String.format("index-key-of-record-%08d", i); }
	private static String dataOfRecord(int i) { return String.format("data-of-record-%08d", i); }

	static byte[] MAPPER = Tuple.from(PREFIX, RECORD, "{K[3]}", "{...}").pack();
	static byte[] MAPPER_ALL_MISSING = Tuple.from(PREFIX, NOTFOUND, "{K[3]}", "{...}").pack();

	static int SPLIT_SIZE = 3;

	private static byte[] notFoundMapperEntryKey(final int i) {
		return Tuple.from(PREFIX, NOTFOUND, primaryKey(i)).pack();
	}
	private static byte[] indexEntryKey(final int i) {
		return Tuple.from(PREFIX, INDEX, indexKey(i), primaryKey(i)).pack();
	}
	private static byte[] recordKeyPrefix(final int i) { return Tuple.from(PREFIX, RECORD, primaryKey(i)).pack(); }
	private static byte[] recordKey(final int i, final int split) {
		return Tuple.from(PREFIX, RECORD, primaryKey(i), split).pack();
	}
	private static byte[] recordValue(final int i, final int split) {
		return Tuple.from(dataOfRecord(i), split).pack();
	}

	private static int getRandomMatchIndex() {
		double r = Math.random();
		if (r < 0.25) {
			return Transaction.MATCH_INDEX_ALL;
		} else if (r < 0.5) {
			return Transaction.MATCH_INDEX_NONE;
		} else if (r < 0.75) {
			return Transaction.MATCH_INDEX_MATCHED_ONLY;
		} else {
			return Transaction.MATCH_INDEX_UNMATCHED_ONLY;
		}
	}

	private static void insertRecordWithIndex(final Transaction tr, final int i) {
		tr.set(indexEntryKey(i), EMPTY);
		for (int split = 0; split < SPLIT_SIZE; split++) {
			tr.set(recordKey(i, split), recordValue(i, split));
		}
	}

	private static String getArgFromEnv() {
		String[] clusterFiles = MultiClientHelper.readClusterFromEnv();
		String cluster = clusterFiles[0];
		System.out.printf("Using Cluster: %s\n", cluster);
		return cluster;
	}
	public static void main(String[] args) throws Exception {
		final MappedRangeQueryIntegrationTest test = new MappedRangeQueryIntegrationTest();
		test.databaseArg = getArgFromEnv();
		test.clearDatabase();
		test.comparePerformance();
		test.clearDatabase();
	}

	int numRecords = 10000;
	int numQueries = 1;
	int numRecordsPerQuery = 100;
	boolean validate = true;
	@Test
	void comparePerformance() {
		FDB fdb = FDB.selectAPIVersion(API_VERSION);
		try (Database db = openFDB()) {
			insertRecordsWithIndexes(numRecords, db);
			instrument(rangeQueryAndThenRangeQueries, "rangeQueryAndThenRangeQueries", db);
			instrument(mappedRangeQuery, "mappedRangeQuery", db);
			instrument(mappedRangeQueryV2, "mappedRangeQueryV2", db);
		}
	}

	private void instrument(final RangeQueryWithIndex query, final String name, final Database db) {
		System.out.printf("Starting %s (numQueries:%d, numRecordsPerQuery:%d, validation:%s)\n", name, numQueries, numRecordsPerQuery, validate ? "on" : "off");
		long startTime = System.currentTimeMillis();
		for (int queryId = 0; queryId < numQueries; queryId++) {
			int begin = ThreadLocalRandom.current().nextInt(numRecords - numRecordsPerQuery);
			query.run(begin, begin + numRecordsPerQuery, db);
		}
		long time = System.currentTimeMillis() - startTime;
		System.out.printf("Finished %s, it takes %d ms for %d queries (%d qps)\n", name, time, numQueries,
		                  numQueries * 1000L / time);
	}

	private static final int RECORDS_PER_TXN = 100;
	private static void insertRecordsWithIndexes(int n, Database db) {
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

	RangeQueryWithIndex rangeQueryAndThenRangeQueries = (int begin, int end, Database db) -> db.run(tr -> {
		try {
			List<KeyValue> kvs = tr.getRange(KeySelector.firstGreaterOrEqual(indexEntryKey(begin)),
			                                 KeySelector.firstGreaterOrEqual(indexEntryKey(end)),
			                                 ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL)
			                         .asList()
			                         .get();
			Assertions.assertEquals(end - begin, kvs.size());

			// Get the records of each index entry IN PARALLEL.
			List<CompletableFuture<List<KeyValue>>> resultFutures = new ArrayList<>();
			// In reality, we need to get the record key by parsing the index entry key. But considering this is a
			// performance test, we just ignore the returned key and simply generate it from recordKey.
			for (int id = begin; id < end; id++) {
				resultFutures.add(tr.getRange(Range.startsWith(recordKeyPrefix(id)),
				                              ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL).asList());
			}
			AsyncUtil.whenAll(resultFutures).get();

			if (validate) {
				final Iterator<KeyValue> indexes = kvs.iterator();
				final Iterator<CompletableFuture<List<KeyValue>>> records = resultFutures.iterator();
				for (int id = begin; id < end; id++) {
					Assertions.assertTrue(indexes.hasNext());
					assertByteArrayEquals(indexEntryKey(id), indexes.next().getKey());

					Assertions.assertTrue(records.hasNext());
					List<KeyValue> rangeResult = records.next().get();
					validateRangeResult(id, rangeResult, false);
				}
				Assertions.assertFalse(indexes.hasNext());
				Assertions.assertFalse(records.hasNext());
			}
		} catch (Exception e) {
			Assertions.fail("Unexpected exception", e);
		}
		return null;
	});

	RangeQueryWithIndex mappedRangeQueryV2 = (int begin, int end, Database db) -> db.run(tr -> {
		try {
			int matchIndex = getRandomMatchIndex();
			boolean fetchLocalOnly = Math.random() < 0.5;
			boolean missing = Math.random() < 0.5;
			byte[] m = missing ? MAPPER_ALL_MISSING : MAPPER;
			List<MappedKeyValueV2> kvs = tr.getMappedRangeV2(KeySelector.firstGreaterOrEqual(indexEntryKey(begin)),
			                                                 KeySelector.firstGreaterOrEqual(indexEntryKey(end)), m,
			                                                 ReadTransaction.ROW_LIMIT_UNLIMITED, false,
			                                                 StreamingMode.WANT_ALL, matchIndex, fetchLocalOnly)
			                                 .asList()
			                                 .get();
			Assertions.assertEquals(end - begin, kvs.size());
			// assuming "local" is always true in the reply
			byte[] expectResponseBytes = new byte[] { 0x02, 0x02, 0x01 };

			if (validate) {
				final Iterator<MappedKeyValueV2> results = kvs.iterator();
				for (int id = begin; id < end; id++) {
					Assertions.assertTrue(results.hasNext());
					MappedKeyValueV2 mappedKeyValue = results.next();
					if (matchIndex == Transaction.MATCH_INDEX_ALL || id == begin || id == end - 1) {
						assertByteArrayEquals(indexEntryKey(id), mappedKeyValue.getKey());
					} else if (matchIndex == Transaction.MATCH_INDEX_MATCHED_ONLY) {
						assertByteArrayEquals(missing ? EMPTY : indexEntryKey(id), mappedKeyValue.getKey());
					} else if (matchIndex == Transaction.MATCH_INDEX_UNMATCHED_ONLY) {
						assertByteArrayEquals(missing ? indexEntryKey(id) : EMPTY, mappedKeyValue.getKey());
					} else {
						assertByteArrayEquals(EMPTY, mappedKeyValue.getKey());
					}
					assertByteArrayEquals(EMPTY, mappedKeyValue.getValue());
					byte[] prefix = missing ? notFoundMapperEntryKey(id) : recordKeyPrefix(id);
					assertByteArrayEquals(prefix, mappedKeyValue.getRangeBegin());
					prefix[prefix.length - 1] = (byte)0x01;
					assertByteArrayEquals(prefix, mappedKeyValue.getRangeEnd());

					List<KeyValue> rangeResult = mappedKeyValue.getRangeResult();
					validateRangeResult(id, rangeResult, missing);

					byte[] responseBytes = mappedKeyValue.getResponseBytes();
					assertByteArrayEquals(expectResponseBytes, responseBytes);
				}
				Assertions.assertFalse(results.hasNext());
			}
		} catch (Exception e) {
			Assertions.fail("Unexpected exception", e);
		}
		return null;
	});

	RangeQueryWithIndex mappedRangeQuery = (int begin, int end, Database db) -> db.run(tr -> {
		try {

			List<MappedKeyValue> kvs =
			    tr.getMappedRange(KeySelector.firstGreaterOrEqual(indexEntryKey(begin)),
			                      KeySelector.firstGreaterOrEqual(indexEntryKey(end)), MAPPER,
			                      ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL)
			        .asList()
			        .get();
			Assertions.assertEquals(end - begin, kvs.size());

			if (validate) {
				final Iterator<MappedKeyValue> results = kvs.iterator();
				for (int id = begin; id < end; id++) {
					Assertions.assertTrue(results.hasNext());
					MappedKeyValue mappedKeyValue = results.next();
					assertByteArrayEquals(indexEntryKey(id), mappedKeyValue.getKey());
					assertByteArrayEquals(EMPTY, mappedKeyValue.getValue());
					assertByteArrayEquals(indexEntryKey(id), mappedKeyValue.getKey());
					byte[] prefix = recordKeyPrefix(id);
					assertByteArrayEquals(prefix, mappedKeyValue.getRangeBegin());
					prefix[prefix.length - 1] = (byte)0x01;
					assertByteArrayEquals(prefix, mappedKeyValue.getRangeEnd());

					List<KeyValue> rangeResult = mappedKeyValue.getRangeResult();
					validateRangeResult(id, rangeResult, false);
				}
				Assertions.assertFalse(results.hasNext());
			}
		} catch (Exception e) {
			Assertions.fail("Unexpected exception", e);
		}
		return null;
	});

	void validateRangeResult(int id, List<KeyValue> rangeResult, boolean missing) {
		if (missing) {
			Assertions.assertEquals(rangeResult.size(), 0);
			return;
		}
		Assertions.assertEquals(rangeResult.size(), SPLIT_SIZE);
		for (int split = 0; split < SPLIT_SIZE; split++) {
			KeyValue keyValue = rangeResult.get(split);
			assertByteArrayEquals(recordKey(id, split), keyValue.getKey());
			assertByteArrayEquals(recordValue(id, split), keyValue.getValue());
		}
	}

	void assertByteArrayEquals(byte[] expected, byte[] actual) {
		Assertions.assertEquals(ByteArrayUtil.printable(expected), ByteArrayUtil.printable(actual));
	}
}
