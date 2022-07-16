/*
 * RangeQueryIntegrationTest.java
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

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.ByteArrayUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests around Range Queries. This requires a running FDB instance to work properly;
 * all tests will be skipped if it can't connect to a running instance relatively quickly.
 */
@ExtendWith(RequiresDatabase.class)
class RangeQueryIntegrationTest {
	private static final FDB fdb = FDB.selectAPIVersion(720);

	@BeforeEach
	@AfterEach
	void clearDatabase() throws Exception {
		/*
		 * Empty the database before and after each run, just in case
		 */
		try (Database db = fdb.open()) {
			db.run(tr -> {
				tr.clear(Range.startsWith(new byte[] { (byte)0x00 }));
				return null;
			});
		}
	}

	private void loadData(Database db, Map<byte[], byte[]> dataToLoad) {
		db.run(tr -> {
			for (Map.Entry<byte[], byte[]> entry : dataToLoad.entrySet()) {
				tr.set(entry.getKey(), entry.getValue());
			}
			return null;
		});
	}

	@Test
	public void canGetRowWithKeySelector() throws Exception {
		Random rand = new Random();
		byte[] key = new byte[128];
		byte[] value = new byte[128];
		rand.nextBytes(key);
		key[0] = (byte)0xEE;
		rand.nextBytes(value);

		NavigableMap<byte[], byte[]> data = new TreeMap<>(ByteArrayUtil.comparator());
		data.put(key, value);
		try (Database db = fdb.open()) {
			loadData(db, data);
			db.run(tr -> {
				byte[] actualValue = tr.get(key).join();
				Assertions.assertNotNull(actualValue, "Missing key!");
				Assertions.assertArrayEquals(value, actualValue, "incorrect value!");

				KeySelector start = KeySelector.firstGreaterOrEqual(new byte[] { key[0] });
				KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(start.getKey()));
				AsyncIterable<KeyValue> kvIterable = tr.getRange(start, end);
				AsyncIterator<KeyValue> kvs = kvIterable.iterator();

				Assertions.assertTrue(kvs.hasNext(), "Did not return a record!");
				KeyValue n = kvs.next();
				Assertions.assertArrayEquals(key, n.getKey(), "Did not return a key correctly!");
				Assertions.assertArrayEquals(value, n.getValue(), "Did not return the corect value!");

				return null;
			});
		}
	}

	@Test
	void rangeQueryReturnsResults() throws Exception {
		/*
		 * A quick test that if you insert a record, then do a range query which includes
		 * the record, it'll be returned
		 */
		try (Database db = fdb.open()) {
			db.run(tr -> {
				tr.set("vcount".getBytes(), "zz".getBytes());
				return null;
			});

			db.run(tr -> {
				AsyncIterable<KeyValue> kvs = tr.getRange("v".getBytes(), "y".getBytes());
				int cnt = 0;
				for (KeyValue kv : kvs) {
					Assertions.assertArrayEquals("vcount".getBytes(), kv.getKey(), "Incorrect key returned!");
					Assertions.assertArrayEquals("zz".getBytes(), kv.getValue(), "Incorrect value returned!");
					cnt++;
				}
				Assertions.assertEquals(1, cnt, "Incorrect number of KeyValues returned");

				return null;
			});
		}
	}

	@Test
	void rangeQueryReturnsEmptyOutsideRange() throws Exception {
		/*
		 * A quick test that if you insert a record, then do a range query which does
		 * not include the record, it won't be returned
		 */
		try (Database db = fdb.open()) {
			db.run(tr -> {
				tr.set("rangeEmpty".getBytes(), "zz".getBytes());
				return null;
			});

			db.run(tr -> {
				AsyncIterator<KeyValue> kvs = tr.getRange("b".getBytes(), "c".getBytes()).iterator();
				if (kvs.hasNext()) {
					Assertions.fail("Found kvs when it really shouldn't: returned key = " +
					                ByteArrayUtil.printable(kvs.next().getKey()));
				}

				return null;
			});
		}
	}

	@Test
	void rangeQueryOverMultipleRows() throws Exception {
		/*
		 * Make sure that you can return multiple rows if you ask for it.
		 * Hopefully this is large enough to force multiple batches
		 */
		int numRows = 100;
		Map<byte[], byte[]> expectedKvs = new TreeMap<>(ByteArrayUtil.comparator());
		try (Database db = fdb.open()) {
			db.run(tr -> {
				for (int i = 0; i < numRows; i++) {
					byte[] key = ("multiRow" + i).getBytes();
					byte[] value = ("multiValue" + i).getBytes();
					tr.set(key, value);
					expectedKvs.put(key, value);
				}
				return null;
			});

			db.run(tr -> {
				Iterator<KeyValue> kvs = tr.getRange("multi".getBytes(), "multj".getBytes()).iterator();
				Iterator<Map.Entry<byte[], byte[]>> expectedKvIter = expectedKvs.entrySet().iterator();
				while (expectedKvIter.hasNext()) {
					Assertions.assertTrue(kvs.hasNext(), "iterator ended too early");
					KeyValue actualKv = kvs.next();
					Map.Entry<byte[], byte[]> expected = expectedKvIter.next();

					Assertions.assertArrayEquals(expected.getKey(), actualKv.getKey(), "Incorrect key!");
					Assertions.assertArrayEquals(expected.getValue(), actualKv.getValue(), "Incorrect value!");
				}
				Assertions.assertFalse(kvs.hasNext(), "Iterator returned too much data");

				return null;
			});
		}
	}
}
