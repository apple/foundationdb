/*
 * EventKeeperTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apple.foundationdb.EventKeeper.Events;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.ByteArrayUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;


/**
 * Basic test code for testing basic Transaction Timer logic.
 *
 * These tests don't check for a whole lot, they just verify that
 * instrumentation works as expected for specific patterns.
 */
class EventKeeperTest {

	@Test
	@Disabled("Ignored because ctest will actually add the library and cause this to segfault")
	void testSetVersion() throws Exception {

		EventKeeper timer = new MapEventKeeper();

		try (FDBTransaction txn = new FDBTransaction(1, null, null, timer)) {
			Assertions.assertThrows(UnsatisfiedLinkError.class,
			                        () -> { txn.setReadVersion(1L); }, "Test should call a bad native method");
			long jniCalls = timer.getCount(Events.JNI_CALL);

			Assertions.assertEquals(1L, jniCalls, "Unexpected number of JNI calls:");
		}catch(UnsatisfiedLinkError ignored){
			//this is necessary to prevent an exception being thrown at close time
		}
	}

	@Test
	@Disabled("Ignored because ctest will actually add the library and cause this to segfault")
	void testGetReadVersion() throws Exception {
		EventKeeper timer = new MapEventKeeper();

		try (FDBTransaction txn = new FDBTransaction(1, null, null, timer)) {
			Assertions.assertThrows(UnsatisfiedLinkError.class,
			                        () -> { txn.getReadVersion(); }, "Test should call a bad native method");
			long jniCalls = timer.getCount(Events.JNI_CALL);

			Assertions.assertEquals(1L, jniCalls, "Unexpected number of JNI calls:");
		}catch(UnsatisfiedLinkError ignored){
			//required to prevent an extra exception being thrown at close time
		}
	}

	@Test
	void testGetRangeRecordsFetches() throws Exception {
		EventKeeper timer = new MapEventKeeper();
		List<KeyValue> testKvs = Arrays.asList(new KeyValue("hello".getBytes(), "goodbye".getBytes()));

		FDBTransaction txn = new FakeFDBTransaction(testKvs, 1L, null, null);

		RangeQuery query = new RangeQuery(txn, true, KeySelector.firstGreaterOrEqual(new byte[] { 0x00 }),
		                                  KeySelector.firstGreaterOrEqual(new byte[] { (byte)0xFF }), -1, false,
		                                  StreamingMode.ITERATOR, timer);
		AsyncIterator<KeyValue> iter = query.iterator();

		List<KeyValue> iteratedItems = new ArrayList<>();
		while (iter.hasNext()) {
			iteratedItems.add(iter.next());
		}

		// basic verification that we got back what we expected to get back.
		Assertions.assertEquals(testKvs.size(), iteratedItems.size(), "Incorrect iterated list, size incorrect.");

		int expectedByteSize = 0;
		for (KeyValue expected : testKvs) {
			byte[] eKey = expected.getKey();
			byte[] eVal = expected.getValue();
			expectedByteSize += eKey.length + 4;
			expectedByteSize += eVal.length + 4;
			boolean found = false;
			for (KeyValue actual : iteratedItems) {
				byte[] aKey = actual.getKey();
				byte[] aVal = actual.getValue();
				if (ByteArrayUtil.compareTo(eKey, 0, eKey.length, aKey, 0, aKey.length) == 0) {
					int cmp = ByteArrayUtil.compareTo(eVal, 0, eVal.length, aVal, 0, aVal.length);
					Assertions.assertEquals(0, cmp, "Incorrect value returned");
					found = true;
					break;
				}
			}

			Assertions.assertTrue(found, "missing key!");
		}

		// now check the timer and see if it recorded any events
		Assertions.assertEquals(1, timer.getCount(Events.RANGE_QUERY_FETCHES), "Unexpected number of chunk fetches");
		Assertions.assertEquals(testKvs.size(), timer.getCount(Events.RANGE_QUERY_RECORDS_FETCHED),
		                        "Unexpected number of tuples fetched");
		Assertions.assertEquals(expectedByteSize, timer.getCount(Events.BYTES_FETCHED),
		                        "Incorrect number of bytes fetched");
	}

}
