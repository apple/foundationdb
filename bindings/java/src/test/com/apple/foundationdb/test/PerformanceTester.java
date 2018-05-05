/*
 * PerformanceTester.java
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

package com.apple.foundationdb.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil;

public class PerformanceTester extends AbstractTester {
	private final int keyCount;
	private final int keySize;
	private final int valueSize;

	private final String keyFormat;
	private final byte[] valueBytes;

	public static final int DEFAULT_KEY_COUNT = 10_000;
	public static final int DEFAULT_KEY_SIZE = 16;
	public static final int DEFAULT_VALUE_SIZE = 100;

	private enum Tests {
		FUTURE_LATENCY("Java Completable API future throughput"),
		SET("Java Completable API set throughput"),
		CLEAR("Java Completable API clear throughput"),
		CLEAR_RANGE("Java Completable API clear_range throughput"),
		PARALLEL_GET("Java Completable API parallel get throughput"),
		SERIAL_GET("Java Completable API serial get throughput"),
		GET_RANGE("Java Completable API get_range throughput"),
		GET_KEY("Java Completable API get_key throughput"),
		GET_SINGLE_KEY_RANGE("Java Completable API get_single_key_range throughput"),
		ALTERNATING_GET_SET("Java Completable API alternating get and set throughput"),
		WRITE_TRANSACTION("Java Completable API single-key transaction throughput");

		private String kpi;
		private Function<? super Database, ? extends Double> function;

		Tests(String kpi) {
			this.kpi = kpi;
		}

		public void setFunction(Function<?super Database, ? extends Double> function) {
			this.function = function;
		}

		public Function<? super Database, ? extends Double> getFunction() {
			return function;
		}

		public String getKpi() {
			return kpi;
		}
	}

	public PerformanceTester() {
		this(DEFAULT_KEY_COUNT, DEFAULT_KEY_SIZE, DEFAULT_VALUE_SIZE);
	}

	public PerformanceTester(int keyCount, int keySize, int valueSize) {
		super();
		this.keyCount = keyCount;
		this.keySize = keySize;
		this.valueSize = valueSize;

		keyFormat = "%0" + keySize + "d";

		valueBytes = new byte[valueSize];
		Arrays.fill(valueBytes, (byte)'x');

		// Initialize tests.
		Tests.FUTURE_LATENCY.setFunction(db -> futureLatency(db, 100_000));
		Tests.SET.setFunction(db -> set(db, 100_000));
		Tests.CLEAR.setFunction(db -> clear(db, 100_000));
		Tests.CLEAR_RANGE.setFunction(db -> clearRange(db, 100_000));
		Tests.PARALLEL_GET.setFunction(db -> parallelGet(db, 10_000));
		Tests.SERIAL_GET.setFunction(db -> serialGet(db, 2_000));
		Tests.GET_RANGE.setFunction(db -> getRange(db, 1_000));
		Tests.GET_KEY.setFunction(db -> getKey(db, 2_000));
		Tests.GET_SINGLE_KEY_RANGE.setFunction(db -> getSingleKeyRange(db, 2_000));
		Tests.ALTERNATING_GET_SET.setFunction(db -> alternatingGetSet(db, 2_000));
		Tests.WRITE_TRANSACTION.setFunction(db -> writeTransaction(db, 1_000));
	}

	@Override
	public void testPerformance(Database db) {
		insertData(db);

		List<String> testsToRun;
		if (args.getTestsToRun().isEmpty()) {
			testsToRun = Arrays.stream(Tests.values()).map(Tests::name).map(String::toLowerCase).sorted().collect(Collectors.toList());
		} else {
			testsToRun = args.getTestsToRun();
		}

		for (String test : testsToRun) {
			Tests testObj;
			try {
				testObj = Tests.valueOf(test.toUpperCase());
			} catch (IllegalArgumentException e) {
				result.addError(new IllegalArgumentException("Test " + test + " not implemented"));
				continue;
			}

			Function<? super Database, ? extends Double> function = testObj.getFunction();

			try {
				Thread.sleep(5_000);
			} catch (InterruptedException e) {
				result.addError(wrapAndPrintError(e, "Interrupted while sleeping"));
			}

			System.out.println("Running test " + test);

			List<Double> results = new ArrayList<>(NUM_RUNS);

			for (int i = 0; i < NUM_RUNS; i++) {
				try {
					results.add(function.apply(db));
				} catch (Exception e) {
					result.addError(wrapAndPrintError(e, "Performance test failed: " + test));
					break;
				}
			}

			if (results.size() == NUM_RUNS) {
				Collections.sort(results);
				result.addKpi(String.format("%s (%s)", testObj.getKpi(), multiVersionDescription()), results.get(results.size()/2).intValue(), "keys/s");
			}
		}
	}

	public void insertData(Database db) {
		System.out.println("Loading database");

		db.run(tr -> {
			byte[] subspacePrefix = args.getSubspace().pack();
			if (subspacePrefix.length == 0) {
				// Clear user space.
				tr.clear(new byte[0], new byte[]{(byte)0xff});
			} else {
				tr.clear(args.getSubspace().range());
			}
			return null;
		});

		int keysPerActor = 100_000 / (keySize + valueSize);
		int numActors = (int)Math.ceil(keyCount*1.0/keysPerActor);

		List<CompletableFuture<Void>> futures = IntStream.range(0, numActors).mapToObj(i -> {
			int startKey = keysPerActor * i;
			int endKey = (i + 1 == numActors) ? (keyCount) : (keysPerActor * (i+1));
			return db.runAsync(tr -> {
				IntStream.range(startKey, endKey).forEach(keyIndex -> tr.set(key(keyIndex), value(keyIndex)));
				return CompletableFuture.completedFuture((Void)null);
			});
		}).collect(Collectors.toList());

		try {
			AsyncUtil.whenAll(futures).get();
		} catch (InterruptedException | ExecutionException e) {
			result.addError(wrapAndPrintError(e, "Data insertion failed"));
		}

		// Give the database time to re-balance
		try {
			Thread.sleep(15_000);
		} catch (InterruptedException e) {
			result.addError(wrapAndPrintError(e, "Interrupted while waiting for quiescence"));
		}
	}

	public Double futureLatency(Database db, int count) {
		return db.run(tr -> {
			tr.options().setRetryLimit(5);
			tr.getReadVersion().join();

			long start = System.nanoTime();

			for (int i = 0; i < count; i++) {
				tr.getReadVersion().join();
			}

			long end = System.nanoTime();

			return count*1_000_000_000.0/(end - start);
		});
	}

	public Double clear(Database db, int count) {
		try(Transaction tr = db.createTransaction()) {
			long start = System.nanoTime();
			for(int i = 0; i < count; i++) {
				tr.clear(randomKey());
			}
			long end = System.nanoTime();
			tr.cancel();

			return count * 1_000_000_000.0 / (end - start);
		}
	}

	public Double clearRange(Database db, int count) {
		try(Transaction tr = db.createTransaction()) {
			long start = System.nanoTime();
			for(int i = 0; i < count; i++) {
				int keyIndex = randomKeyIndex();
				tr.clear(key(keyIndex), key(keyIndex + 1));
			}
			long end = System.nanoTime();
			tr.cancel();

			return count * 1_000_000_000.0 / (end - start);
		}
	}

	public Double set(Database db, int count) {
		try(Transaction tr = db.createTransaction()) {
			long start = System.nanoTime();
			for(int i = 0; i < count; i++) {
				int keyIndex = randomKeyIndex();
				tr.set(key(keyIndex), value(keyIndex));
			}
			long end = System.nanoTime();
			tr.cancel();

			return count * 1_000_000_000.0 / (end - start);
		}
	}

	public Double parallelGet(TransactionContext tcx, int count) {
		return tcx.run(tr -> {
			tr.options().setRetryLimit(5);
			long start = System.nanoTime();

			List<CompletableFuture<byte[]>> futures = IntStream.range(0, count)
					.mapToObj(ignore -> tr.get(randomKey()))
					.collect(Collectors.toList());
			AsyncUtil.whenAll(futures).join();
			long end = System.nanoTime();

			return count*1_000_000_000.0/(end - start);
		});
	}

	public Double alternatingGetSet(TransactionContext tcx, int count) {
		return tcx.run(tr -> {
			tr.options().setRetryLimit(5);
			long start = System.nanoTime();

			List<CompletableFuture<byte[]>> futures = IntStream.range(0, count)
					.mapToObj(ignore -> {
						int keyIndex = randomKeyIndex();
						byte[] keyBytes = key(keyIndex);
						byte[] valBytes = value(keyIndex);

						tr.set(keyBytes, valBytes);
						return tr.get(keyBytes);
					}).collect(Collectors.toList());
			AsyncUtil.whenAll(futures).join();
			long end = System.nanoTime();

			return count*1_000_000_000.0/(end - start);
		});
	}

	public Double serialGet(TransactionContext tcx, int count) {
		return tcx.run(tr -> {
			tr.options().setRetryLimit(5);

			List<byte[]> keys;
			if (count > keyCount/2) {
				keys = Stream.generate(this::randomKey).limit(count).collect(Collectors.toList());
			} else {
				Set<Integer> keySet = new HashSet<>();
				while (keySet.size() < count) {
					keySet.add(randomKeyIndex());
				}
				keys = keySet.stream().map(this::key).collect(Collectors.toList());
			}

			long start = System.nanoTime();
			for (byte[] key : keys) {
				tr.get(key).join();
			}
			long end = System.nanoTime();

			return count*1_000_000_000.0/(end - start);
		});
	}

	public Double getRange(TransactionContext tcx, int count) {
		return tcx.run(tr -> {
			tr.options().setRetryLimit(5);
			int startIndex = random.nextInt(keyCount - count);

			long start = System.nanoTime();
			tr.getRange(key(startIndex), key(startIndex+count)).asList().join();
			long end = System.nanoTime();

			return count*1_000_000_000.0/(end - start);
		});
	}

	public Double getKey(TransactionContext tcx, int count) {
		return tcx.run(tr -> {
			tr.options().setRetryLimit(5);

			long start = System.nanoTime();
			for (int i = 0; i < count; i++) {
				tr.getKey(new KeySelector(randomKey(), true, random.nextInt(20) - 10)).join();
			}
			long end = System.nanoTime();

			return count*1_000_000_000.0/(end - start);
		});
	}

	public Double getSingleKeyRange(TransactionContext tcx, int count) {
		return tcx.run(tr -> {
			tr.options().setRetryLimit(5);
			long start = System.nanoTime();
			for (int i = 0; i < count; i++) {
				int keyIndex = randomKeyIndex();
				tr.getRange(key(keyIndex), key(keyIndex + 1), 2).asList().join();
			}
			long end = System.nanoTime();

			return count*1_000_000_000.0/(end - start);
		});
	}

	public Double writeTransaction(TransactionContext tcx, int count) {
		long start = System.nanoTime();
		for (int i = 0; i < count; i++) {
			tcx.run(tr -> {
				int keyIndex = randomKeyIndex();
				tr.set(key(keyIndex), value(keyIndex));
				return null;
			});
		}
		long end = System.nanoTime();

		return count*1_000_000_000.0/(end - start);
	}

	public byte[] key(int i) {
		return ByteArrayUtil.join(args.getSubspace().pack(), String.format(keyFormat, i).getBytes(ASCII));
	}

	public int randomKeyIndex() {
		return random.nextInt(keyCount);
	}

	public byte[] randomKey() {
		return key(randomKeyIndex());
	}

	public byte[] value(int key) {
		return valueBytes;
	}

	public static void main(String[] args) {
		System.out.println("Running Java performance test on Java version " + System.getProperty("java.version"));
		try {
			new PerformanceTester().run(args);
		} catch (IllegalArgumentException e) {
			System.out.println("Could not run test due to malformed arguments.");
			System.out.println(e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			System.out.println("Fatal error encountered during run: " + e);
			e.printStackTrace();
			System.exit(2);
		}
	}

}