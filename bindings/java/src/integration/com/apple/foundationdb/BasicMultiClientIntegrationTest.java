/*
 * BasicMultiClientIntegrationTest
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

import java.util.Collection;
import java.util.Random;

import com.apple.foundationdb.tuple.Tuple;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Simple class to test multi-client logic.
 *
 * Note that all Multi-client-only tests _must_ be tagged with "MultiClient", which will ensure that they are excluded
 * from non-multi-threaded tests.
 */
public class BasicMultiClientIntegrationTest {
	@RegisterExtension public  static final MultiClientHelper clientHelper = new MultiClientHelper();

	@Test
	@Tag("MultiClient")
	void testMultiClientWritesAndReadsData() throws Exception {
		FDB fdb = FDB.selectAPIVersion(630);
		fdb.options().setKnob("min_trace_severity=5");

		Collection<Database> dbs = clientHelper.openDatabases(fdb); // the clientHelper will close the databases for us
		System.out.print("Starting tests.");
		Random rand = new Random();
		for (int counter = 0; counter < 25; ++counter) {
			for (Database db : dbs) {
				String key = Integer.toString(rand.nextInt(100000000));
				String val = Integer.toString(rand.nextInt(100000000));

				db.run(tr -> {
					tr.set(Tuple.from(key).pack(), Tuple.from(val).pack());
					return null;
				});

				String fetchedVal = db.run(tr -> {
					byte[] result = tr.get(Tuple.from(key).pack()).join();
					return Tuple.fromBytes(result).getString(0);
				});
				Assertions.assertEquals(val, fetchedVal, "Wrong result!");
			}
			Thread.sleep(200);
		}
	}
}
