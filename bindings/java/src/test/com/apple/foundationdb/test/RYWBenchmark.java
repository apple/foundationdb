/*
 * RYWBenchmark.java
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

package com.apple.foundationdb.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;

public class RYWBenchmark extends AbstractTester {
	private int keyCount;

	public static final int DEFAULT_KEY_COUNT = 10_000;
	public static final int DEFAULT_KEY_SIZE = 16;

	private final String keyFormat;

	private enum Tests {
		GET_SINGLE("RYW Java Completable: get single cached value throughput"),
		GET_MANY_SEQUENTIAL("RYW Java Completable: get sequential cached value throughput"),
		GET_RANGE_BASIC("RYW Java Completable: get range cached values throughput"),
		SINGLE_CLEAR_GET_RANGE("RYW Java Completable: get range cached values with clears throughput"),
		CLEAR_RANGE_GET_RANGE("RYW Java Completable: get range cached values with clear ranges throughput"),
		INTERLEAVED_SETS_GETS("RYW Java Completable: interleaved sets and gets on a single key throughput");

		private String kpi;
		private Function<? super Transaction, ? extends Double> function;

		Tests(String kpi) {
			this.kpi = kpi;
		}

		public void setFunction(Function<?super Transaction, ? extends Double> function) {
			this.function = function;
		}

		public Function<? super Transaction, ? extends Double> getFunction() {
			return function;
		}

		public String getKpi() {
			return kpi;
		}
	}

	public RYWBenchmark() {
		this(DEFAULT_KEY_COUNT, DEFAULT_KEY_SIZE);
	}

	public RYWBenchmark(int keyCount, int keySize) {
		super();
		this.keyCount = keyCount;

		keyFormat = "%0" + keySize + "d";

		Tests.GET_SINGLE.setFunction(tr -> getSingle(tr, 10_000));
		Tests.GET_MANY_SEQUENTIAL.setFunction(tr -> getManySequential(tr, 10_000));
		Tests.GET_RANGE_BASIC.setFunction(tr -> getRangeBasic(tr, 1_000));
		Tests.SINGLE_CLEAR_GET_RANGE.setFunction(tr -> singleClearGetRange(tr, 1_000));
		Tests.CLEAR_RANGE_GET_RANGE.setFunction(tr -> clearRangeGetRange(tr, 1_000));
		Tests.INTERLEAVED_SETS_GETS.setFunction(tr -> interleavedSetsGets(tr, 10_000));
	}

	@Override
	public void testPerformance(Database db) {
		try(Transaction tr = db.createTransaction()) {
			insertData(tr);

			List<String> testsToRun;
			if(args.getTestsToRun().isEmpty()) {
				testsToRun = Arrays.stream(Tests.values()).map(Tests::name).map(String::toLowerCase).sorted().collect(Collectors.toList());
			}
			else {
				testsToRun = args.getTestsToRun();
			}

			for(String test : testsToRun) {
				Tests testObj;
				try {
					testObj = Tests.valueOf(test.toUpperCase());
				}
				catch(IllegalArgumentException e) {
					result.addError(new IllegalArgumentException("Test " + test + " not implemented"));
					continue;
				}

				Function<? super Transaction, ? extends Double> function = testObj.getFunction();

				try {
					Thread.sleep(5_000);
				}
				catch(InterruptedException e) {
					result.addError(wrapAndPrintError(e, "Interrupted while sleeping"));
				}

				System.out.println("Running test " + test);

				List<Double> results = new ArrayList<>(NUM_RUNS);

				for(int i = 0; i < NUM_RUNS; i++) {
					try {
						results.add(function.apply(tr));
					}
					catch(Exception e) {
						result.addError(wrapAndPrintError(e, "Performance test failed: " + test));
						break;
					}
				}

				if(results.size() == NUM_RUNS) {
					Collections.sort(results);
					result.addKpi(String.format("%s", testObj.getKpi()), results.get(results.size() / 2).intValue(), "keys/s");
				}
			}

			tr.cancel();
		}
	}

	public Double getSingle(Transaction tr, int count) {
		long start = System.nanoTime();
		for (int i = 0; i < count; i++) {
			tr.get(key(5001)).join();
		}
		long end = System.nanoTime();

		return count*1_000_000_000.0/(end - start);
	}

	public Double getManySequential(Transaction tr, int count) {
		long start = System.nanoTime();
		for (int i = 0; i < count; i++) {
			tr.get(key(i)).join();
		}
		long end = System.nanoTime();

		return count*1_000_000_000.0/(end - start);
	}

	public Double getRangeBasic(Transaction tr, int count) {
		long start = System.nanoTime();
		for (int i = 0; i < count; i++) {
			tr.getRange(key(0), key(keyCount)).asList().join();
		}
		long end = System.nanoTime();

		return count * 1_000_000_000.0 * keyCount/(end - start);
	}

	public Double singleClearGetRange(Transaction tr, int count) {
		for (int i = 0; i < keyCount; i += 2) {
			tr.clear(("" + i).getBytes(ASCII));
		}
		long start = System.nanoTime();
		for (int i = 0; i < count; i++) {
			tr.getRange(key(0), key(keyCount)).asList().join();
		}
		long end = System.nanoTime();

		Double kpi = count * 1_000_000_000.0 * keyCount / 2 / (end - start);
		insertData(tr);
		return kpi;
	}

	public Double clearRangeGetRange(Transaction tr, int count) {
		for (int i = 0; i < keyCount; i += 4) {
			tr.clear(key(i), key(i+1));
		}
		long start = System.nanoTime();
		for (int i = 0; i < count; i++) {
			tr.getRange(key(0), key(keyCount)).asList().join();
		}
		long end = System.nanoTime();

		Double kpi = count * 1_000_000_000.0 * keyCount * 3 / 4 / (end - start);
		insertData(tr);
		return kpi;
	}

	public Double interleavedSetsGets(Transaction tr, int count) {
		long start = System.nanoTime();
		byte[] keyBytes = "foo".getBytes(ASCII);
		tr.set(keyBytes, "1".getBytes(ASCII));
		for (int i = 0; i < count; i++) {
			int old = Integer.parseInt(new String(tr.get(keyBytes).join(), ASCII));
			tr.set(keyBytes, ("" + (old + 1)).getBytes(ASCII));
		}
		long end = System.nanoTime();

		return count * 1_000_000_000.0/(end - start);
	}

	public void insertData(Transaction tr) {
		tr.clear(new byte[0], new byte[]{(byte)0xff}); // Clear user space.
		for (int i = 0; i < keyCount; i++) {
			tr.set(key(i), "foo".getBytes(ASCII));
		}
	}

	public byte[] key(int i) {
		return ByteArrayUtil.join(args.getSubspace().pack(), String.format(keyFormat, i).getBytes(ASCII));
	}

	public static void main(String[] args) {
		System.out.println("Running Java RYW benchmark on Java version " + System.getProperty("java.version"));
		try {
			new RYWBenchmark().run(args);
		} catch (IllegalArgumentException e) {
			System.out.println("Could not run test due to malformed arguments.");
			System.exit(1);
		} catch (Exception e) {
			System.out.println("Fatal error encountered during run: " + e);
			e.printStackTrace();
			System.exit(2);
		}
	}
}
