/*
 * MultiThreadedClientTest.java
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

package com.apple.foundationdb.test;

import java.util.Vector;
import java.util.Random;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;

// Usage
// java -cp "<path-to-jar>" MultiThreadedClient logDir=client-logs externalClientDirectory=libfdb numThreads=3 - <path-to-cluster-files> ...
public class MultiThreadedClientTest {
	public static void main(String[] args) {
		// TODO: Use an actual argparse library.
		String logDir = "client-logs/";
		String externalClientDirectory = "libfdb/";
		int numThreads = 3;
		String[] clusters;

		int argIndex = 0;
		for (argIndex = 0; argIndex < args.length && !args[argIndex].equals("-"); ++argIndex) {
			String prefix = "externalClientDirectory=";
			if (args[argIndex].startsWith(prefix)) {
				externalClientDirectory = args[argIndex].substring(prefix.length());
			}

			prefix = "logDir=";
			if (args[argIndex].startsWith(prefix)) {
				logDir = args[argIndex].substring(prefix.length());
			}

			prefix = "numThreads=";
			if (args[argIndex].startsWith(prefix)) {
				numThreads = Integer.parseInt(args[argIndex].substring(prefix.length()));
			}
		}

		clusters = new String[args.length - argIndex - 1];
		if (clusters.length == 0) {
			System.err.println("Cluster files not given!");
			return;
		}

		for (int i = 0; i < args.length - argIndex - 1; ++i) {
			clusters[i] = args[argIndex + i + 1];
		}

		FDB fdb = FDB.selectAPIVersion(630);
		fdb.options().setTraceEnable(logDir);
		fdb.options().setKnob("min_trace_severity=5");
		fdb.options().setClientThreadsPerVersion(numThreads);
		fdb.options().setExternalClientDirectory(externalClientDirectory);

		Vector<Database> dbs = new Vector<Database>();
		for (String arg : clusters) {
			System.out.printf("Opening Cluster: %s\n", arg);
			dbs.add(fdb.open(arg));
		}

		System.out.print("Starting tests.");
		Random rand = new Random();
		try {
			for (int counter = 0; ; ++counter) {
				for (Database db : dbs) {
					String key = Integer.toString(rand.nextInt(100000000));
					String val = Integer.toString(rand.nextInt(100000000));
					System.out.printf("[%d] Writing Key = %s, Value = %s. ",
									  counter, key, val);

					db.run(tr -> {
							tr.set(Tuple.from(key).pack(), Tuple.from(val).pack());
							return null;
						});
					System.out.println("Done.");

					String fetchedVal = db.run(tr -> {
							byte[] result = tr.get(Tuple.from(key).pack()).join();
							return Tuple.fromBytes(result).getString(0);
						});
					if (!fetchedVal.equals(val)) {
						System.err.printf("Wrong result! Expected = %s, Actual = %s\n",
										  val, fetchedVal);
					}
				}
				Thread.sleep(200);
			}
		} catch (Exception e) {
			System.err.println("Error running test!");
			e.printStackTrace();
		} finally {
			for (Database db : dbs) {
				db.close();
			}
		}
	}

	private MultiThreadedClientTest() {}
}
