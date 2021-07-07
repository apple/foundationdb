/*
 * CycleMultiClientIntegrationTest
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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import com.apple.foundationdb.tuple.Tuple;

import org.junit.jupiter.api.Assertions;

/**
 * Setup: Generating a cycle 0 -> 1 -> 2 -> 3 -> 0, its length is 4
 * Process: randomly choose an element, reverse 2nd and 4rd element, considering the chosen one as the 1st element.
 * Check: verify no element is lost or added, and they are still a cycle.
 * 
 * This test is to verify the atomicity of transactions. 
 */
public class CycleMultiClientIntegrationTest {
	public static final MultiClientHelper clientHelper = new MultiClientHelper();

	private static final int len = 4;
	private static List<String> expected = new ArrayList<>(Arrays.asList("0", "1", "2", "3"));

	public static void main(String[] args) throws Exception {
		FDB fdb = FDB.selectAPIVersion(630);
		setupThreads(fdb);			
		Collection<Database> dbs = clientHelper.openDatabases(fdb); // the clientHelper will close the databases for us
		System.out.print("Starting tests\n");
		setup(dbs);
		System.out.print("Start processing\n");
		process(dbs);
		System.out.print("Start validataing\n");
		check(dbs);
		System.out.print("Test finished\n");
	}

	private static synchronized void setupThreads(FDB fdb) {
		int clientThreadsPerVersion = clientHelper.readClusterFromEnv().length;
		fdb.options().setClientThreadsPerVersion(clientThreadsPerVersion);
		System.out.printf("thread per version is %d\n", clientThreadsPerVersion);
		fdb.options().setExternalClientDirectory("/var/dynamic-conf/lib");
		fdb.options().setTraceEnable("/tmp");
		fdb.options().setKnob("min_trace_severity=5");
	}

	private static void setup(Collection<Database> dbs) {
		// 0 -> 1 -> 2 -> 3 -> 0
		for (int k = 0; k < len; k++) {
			String key = Integer.toString(k);
			String value = Integer.toString((k + 1) % len);

			for (Database db : dbs) {
				db.run(tr -> {
					tr.set(Tuple.from(key).pack(), Tuple.from(value).pack());
					return null;
				});
			}
		}
	}

	private static void process(Collection<Database> dbs) {
		for (int i = 0; i < len; i++) {
			int k = ThreadLocalRandom.current().nextInt(len);
			String key = Integer.toString(k);

			for (Database db : dbs) {
				db.run(tr -> {
					byte[] result1 = tr.get(Tuple.from(key).pack()).join();
					String value1 = Tuple.fromBytes(result1).getString(0);

					byte[] result2 = tr.get(Tuple.from(value1).pack()).join();
					String value2 = Tuple.fromBytes(result2).getString(0);

					byte[] result3 = tr.get(Tuple.from(value2).pack()).join();
					String value3 = Tuple.fromBytes(result3).getString(0);

					byte[] result4 = tr.get(Tuple.from(value3).pack()).join();
					String value4 = Tuple.fromBytes(result4).getString(0);

					tr.set(Tuple.from(key).pack(), Tuple.from(value2).pack());
					tr.set(Tuple.from(value2).pack(), Tuple.from(value1).pack());
					tr.set(Tuple.from(value1).pack(), Tuple.from(value3).pack());
					return null;
				});
			}
		}
	}

	private static void check(Collection<Database> dbs) {
		for (int i = 0; i < len + 1; i++) {
			int k = ThreadLocalRandom.current().nextInt(len);
			String key = Integer.toString(k);

			for (Database db : dbs) {
				db.run(tr -> {
					byte[] result1 = tr.get(Tuple.from(key).pack()).join();
					String value1 = Tuple.fromBytes(result1).getString(0);

					byte[] result2 = tr.get(Tuple.from(value1).pack()).join();
					String value2 = Tuple.fromBytes(result2).getString(0);

					byte[] result3 = tr.get(Tuple.from(value2).pack()).join();
					String value3 = Tuple.fromBytes(result3).getString(0);

					byte[] result4 = tr.get(Tuple.from(value3).pack()).join();
					String value4 = Tuple.fromBytes(result4).getString(0);

					Assertions.assertEquals(key, value4, "not a cycle anymore");
					List<String> actual = new ArrayList<>(Arrays.asList(value1, value2, value3, value4));

					Collections.sort(actual);
					Assertions.assertEquals(expected, actual, "Wrong result!");
					return null;
				});
			}
		}
	}
}
