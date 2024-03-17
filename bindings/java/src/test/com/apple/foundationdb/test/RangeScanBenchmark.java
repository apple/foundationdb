/*
 * RangeScanBenchmark.java
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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.EventKeeper;
import com.apple.foundationdb.EventKeeper.Events;
import com.apple.foundationdb.test.ContinuousSample.DoubleSample;
import com.apple.foundationdb.test.ContinuousSample.LongSample;
import com.apple.foundationdb.tuple.ByteArrayUtil;

/**
 * A benchmarking tool for running a variety of different RangeQuery configurations
 * on a single client machine.
 *
 * There are several dimensions to cover in this benchmark, in two different categories:
 *
 * 1. Data configuration
 * 2. Query configuration
 *
 * In the category of Data configuration, we want to know:
 *
 * 1. number of records in database
 * 2. total size of each record (or distribution, if you want it to be non-uniform)
 *
 * In the category of query configuration, we want:
 *
 * 1. number of records in scan
 * 2. byte limit configuration(i.e. TargetBytesStrategy)
 * 3. number of concurrent scans
 */
public class RangeScanBenchmark {

	public static void main(String... args) throws Exception {
		FDB fdb = FDB.selectAPIVersion(630);
		fdb.enableDirectBufferQuery(true);

		MapTimer timer = new MapTimer();
		RunStatistics statistics = new RunStatistics(timer);
		StatisticsFormat formatter = new SimpleComparisonFormat();
		try (Database db = fdb.open(null, timer)) {
			DataConfiguration config = configureDataLoad();
			formatter.printDataSetup("Load" + config.numRows, config);
			if (config != null) {
				loadData(db, config);
			}

			Collection<QueryConfiguration> configuration = configureQueryRuns(config);

			int runCnt = 0;
			for (QueryConfiguration queryConfig : configuration) {
				runQueryTest("queryTest" + runCnt, db, config, queryConfig, statistics, formatter);
				runCnt++;
			}

			if (config.cleanUpAfter()) {
				cleanUp(db, config);
			}
		}
	}

	private static DataConfiguration configureDataLoad() {
		return new DataConfiguration(150, 1000, new Random(), true);
	}

	private static Collection<QueryConfiguration> configureQueryRuns(DataConfiguration dataCfg) {
		List<QueryConfiguration> configs = new ArrayList<>();
		for (int rowSize = 0; rowSize <= dataCfg.numRows; rowSize += 5) {
			configs.add(new QueryConfiguration(1, 50, rowSize, false, StreamingMode.ITERATOR));
		}
		return configs;
		//	return Arrays.asList(
		// full table scans
		//		    new QueryConfiguration(1, 1, 0, false, StreamingMode.ITERATOR, TargetBytesStrategy.noLimit())
		/*
		new QueryConfiguration(2, 100, 0, false, StreamingMode.ITERATOR, TargetBytesStrategy.powerOf2(256)),
		new QueryConfiguration(2, 100, 0, false, StreamingMode.ITERATOR, TargetBytesStrategy.powerOf2(512)),
		new QueryConfiguration(2, 100, 0, false, StreamingMode.ITERATOR, TargetBytesStrategy.powerOf2(1024)),
		new QueryConfiguration(2, 100, 0, false, StreamingMode.ITERATOR, TargetBytesStrategy.fixed(256)),
		new QueryConfiguration(2, 100, 0, false, StreamingMode.ITERATOR, TargetBytesStrategy.fixed(512)),
		new QueryConfiguration(2, 100, 0, false, StreamingMode.ITERATOR, TargetBytesStrategy.fixed(1024)),

		//short scans
		new QueryConfiguration(2, 100,50, false, StreamingMode.ITERATOR, TargetBytesStrategy.noLimit()),
		new QueryConfiguration(2, 100,50, false, StreamingMode.ITERATOR, TargetBytesStrategy.powerOf2(256)),
		new QueryConfiguration(2, 100,50, false, StreamingMode.ITERATOR, TargetBytesStrategy.powerOf2(512)),
		new QueryConfiguration(2, 100,50, false, StreamingMode.ITERATOR, TargetBytesStrategy.powerOf2(1024)),
		new QueryConfiguration(2, 100,50, false, StreamingMode.ITERATOR, TargetBytesStrategy.fixed(256)),
		new QueryConfiguration(2, 100,50, false, StreamingMode.ITERATOR, TargetBytesStrategy.fixed(512)),
		new QueryConfiguration(2, 100,50, false, StreamingMode.ITERATOR, TargetBytesStrategy.fixed(1024)),

		//tiny scans
		new QueryConfiguration(2, 100,5, false, StreamingMode.ITERATOR, TargetBytesStrategy.noLimit()),
		new QueryConfiguration(2, 100,5, false, StreamingMode.ITERATOR, TargetBytesStrategy.powerOf2(256)),
		new QueryConfiguration(2, 100,5, false, StreamingMode.ITERATOR, TargetBytesStrategy.powerOf2(512)),
		new QueryConfiguration(2, 100,5, false, StreamingMode.ITERATOR, TargetBytesStrategy.powerOf2(1024)),
		new QueryConfiguration(2, 100,5, false, StreamingMode.ITERATOR, TargetBytesStrategy.fixed(256)),
		new QueryConfiguration(2, 100,5, false, StreamingMode.ITERATOR, TargetBytesStrategy.fixed(512)),
		new QueryConfiguration(2, 100,5, false, StreamingMode.ITERATOR, TargetBytesStrategy.fixed(1024))
		*/
		//		);
	}

	private static void doQueryRun(Transaction tr, DataConfiguration dataConfig, QueryConfiguration config,
	                               RunStatistics statistics) {
		// select a random key
		byte[] startKey = dataConfig.getMinKey();
		byte[] endKey = ByteArrayUtil.strinc(dataConfig.getMaxKey()); // include the ending key

		statistics.scanStarted();
		Iterable<KeyValue> kvs =
		    tr.getRange(startKey, endKey, config.rowsPerScan, config.reversed, config.mode);
		int cnt = 0;
		int byteSize = 0;
		for (KeyValue kv : kvs) {
			byte[] key = kv.getKey();
			byte[] value = kv.getValue();
			byteSize += key.length + value.length;
			cnt++;
		}
		statistics.countRuntime(cnt, byteSize);
	}

	private static void runQueryTest(String testName, Database db, DataConfiguration dataConfig,
	                                 QueryConfiguration config, RunStatistics statistics, StatisticsFormat formatter)
	    throws Exception {
		formatter.printTestSetup(testName, config);
		// first warm up the query engine. Do this on a single thread
		for (int i = 0; i < config.numWarmupRuns; i++) {
			db.run(tr -> {
				doQueryRun(tr, dataConfig, config, statistics);
				return null;
			});
		}

		// formatter.printTestRun("Warmup", statistics);
		statistics.clear();

		for (int i = 0; i < config.numIterations; i++) {
			db.run(tr -> {
				doQueryRun(tr, dataConfig, config, statistics);
				return null;
			});
		}
		formatter.printTestRun("Benchmark", statistics);
		statistics.clear();

		formatter.printTestComplete();
	}

	private static void loadData(Database db, DataConfiguration configuration) throws Exception {
		db.run(tr -> {
			while (configuration.hasNext()) {
				KeyValue n = configuration.next();

				tr.set(n.getKey(), n.getValue());
			}

			return null;
		});
	}

	private static void cleanUp(Database db, DataConfiguration configuration) throws Exception {
		db.run(tr -> {
			tr.clear(new Range(configuration.getMinKey(), ByteArrayUtil.strinc(configuration.getMaxKey())));
			return null;
		});
	}

	private static class QueryConfiguration {
		private final StreamingMode mode;
		private final boolean reversed;
		private final int rowsPerScan;
		private final int numWarmupRuns;
		private final int numIterations;

		public QueryConfiguration(int numWarmupRuns, int numIterations, int rowsPerScan, boolean reversed,
		                          StreamingMode mode) {
			this.mode = mode;
			this.reversed = reversed;
			this.rowsPerScan = rowsPerScan;
			this.numWarmupRuns = numWarmupRuns;
			this.numIterations = numIterations;
		}
	}

	private static class DataConfiguration implements Iterator<KeyValue> {
		private final int numRows;
		private final int recordSizeBytes;
		private final Random random;
		private final boolean cleanUpAfter;

		public DataConfiguration(int numRows, int recordSizeBytes, Random random, boolean cleanUpAfter) {
			this.numRows = numRows;
			this.recordSizeBytes = recordSizeBytes;
			this.cleanUpAfter = cleanUpAfter;
			this.random = random;
		}

		private byte[] generateRandomKey() {
			int size = recordSizeBytes / 2; // random.nextInt(recordSizeBytes - 1);
			while (size == 0) {
				size = random.nextInt(recordSizeBytes - 1);
			}
			byte[] key = new byte[size];
			random.nextBytes(key);
			while (key[0] == (byte)0xFF) {
				key[0] = (byte)(random.nextInt() & 0x000000FF);
			}

			return key;
		}

		private int generated = 0;
		private byte[] minKey;
		private byte[] maxKey;

		boolean cleanUpAfter() { return cleanUpAfter; }

		public byte[] getMinKey() { return minKey; }

		public byte[] getMaxKey() { return maxKey; }

		@Override
		public boolean hasNext() {
			return generated < numRows;
		}

		@Override
		public KeyValue next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			generated++;
			byte[] key = generateRandomKey();
			byte[] value = new byte[recordSizeBytes - key.length];
			random.nextBytes(value);

			if (minKey == null) {
				minKey = Arrays.copyOf(key, key.length);
			}
			if (ByteArrayUtil.compareUnsigned(minKey, key) > 0) {
				if (minKey.length != key.length) {
					minKey = Arrays.copyOf(key, key.length);
				} else {
					System.arraycopy(key, 0, minKey, 0, key.length);
				}
			}
			if (maxKey == null) {
				maxKey = Arrays.copyOf(key, key.length);
			}
			if (ByteArrayUtil.compareUnsigned(key, maxKey) > 0) {
				if (maxKey.length != key.length) {
					maxKey = Arrays.copyOf(key, key.length);
				} else {
					System.arraycopy(key, 0, maxKey, 0, key.length);
				}
			}
			return new KeyValue(key, value);
		}
	}

	private static class RunStatistics {
		private final MapTimer txnTimer;
		private LongSample queryFetchTimes = new LongSample(128); // for tracking query fetch times across runs
		private LongSample recordsRead = new LongSample(128); // for tracking query fetch times across runs
		private DoubleSample scanRuntime = new DoubleSample(128); // track number of batches required per 100
		                                                          // records
		private DoubleSample fullScanQueryThroughput =
		    new DoubleSample(128); // for tracking query fetch times across runs

		private long startTime;

		private final MapTimer totalTimer = new MapTimer();

		public RunStatistics(MapTimer timer) { this.txnTimer = timer; }

		public void clear() {
			txnTimer.clear();
			totalTimer.clear();
			queryFetchTimes = new LongSample(128);
			fullScanQueryThroughput = new DoubleSample(128);
			recordsRead = new LongSample(128);
			scanRuntime = new DoubleSample(128);
		}

		public void countRuntime(int rowsReturned, int byteSize) {
			// byteSize is mainly there to make sure that the key and value fields aren't optimized away
			long totalRuntime = System.nanoTime() - startTime;
			if (totalRuntime != 0) {
				fullScanQueryThroughput.add(((double)rowsReturned) / totalRuntime);
				scanRuntime.add(totalRuntime);
			}
			recordsRead.add(rowsReturned);
			queryFetchTimes.add(txnTimer.getTimeNanos(EventKeeper.Events.RANGE_QUERY_FETCH_TIME_NANOS));
			totalTimer.timeNanos(TOTAL_TIME_NANOS, totalRuntime);
			totalTimer.count(Events.JNI_CALL, txnTimer.getCount(Events.JNI_CALL));
			totalTimer.count(Events.BYTES_FETCHED, txnTimer.getCount(Events.BYTES_FETCHED));
			totalTimer.count(Events.RANGE_QUERY_RECORDS_FETCHED, txnTimer.getCount(Events.RANGE_QUERY_RECORDS_FETCHED));
			totalTimer.count(Events.RANGE_QUERY_FETCHES, txnTimer.getCount(Events.RANGE_QUERY_FETCHES));
			totalTimer.increment(RANGE_QUERY_COUNT);
		}

		public void scanStarted() {
			txnTimer.clear();
			startTime = System.nanoTime();
		}
	}

	private static final EventKeeper.Event RANGE_QUERY_COUNT = new EventKeeper.Event() {
		@Override
		public String name() {
			return "RANGE_QUERY_COUNT";
		}

		@Override
		public boolean isTimeEvent() {
			return false;
		}
		@Override
		public String toString() {
			return name();
		}
	};
	private static final EventKeeper.Event TOTAL_TIME_NANOS = new EventKeeper.Event() {
		@Override
		public String name() {
			return "TOTAL_TIME_NANOS";
		}

		@Override
		public boolean isTimeEvent() {
			return true;
		}
		@Override
		public String toString() {
			return name();
		}
	};

	private static String toTimeString(long timeNanos) {
		double seconds = timeNanos / (double)1e9;
		return String.format("%.9f", seconds);
	}

	private interface StatisticsFormat {
		void printDataSetup(String label, DataConfiguration dataCfg);

		void printTestSetup(String label, QueryConfiguration queryCfg);

		void printTestRun(String label, RunStatistics statistics);

		void printTestComplete();

		void printSummary(RunStatistics statistics);

		void printDetails(RunStatistics statistics);
	}

	private static abstract class BaseFormat implements StatisticsFormat {
		protected final DecimalFormat lf = new DecimalFormat("#######");
		protected final DecimalFormat df = new DecimalFormat("#######.###");

		protected abstract String getHline(int numCols);
		protected abstract String getColumnPattern(int numCols);
		protected abstract String getHeaderPattern(int numCols);

		@Override
		public void printDataSetup(String label, DataConfiguration dataCfg) {
			String cfgPattern = getColumnPattern(2);
			String cfgHeaderPattern = getColumnPattern(2);
			String hline = getHline(2);
			System.out.printf(cfgHeaderPattern, "Data config", "value");
			System.out.println(hline);
			System.out.printf(cfgPattern, "Num records", dataCfg.numRows);
			System.out.printf(cfgPattern, "Record size(bytes)", dataCfg.recordSizeBytes);
			System.out.println(hline);
			System.out.println();
		}

		@Override
		public void printTestRun(String testName, RunStatistics statistics) {
			System.out.println(testName);
			printSummary(statistics);
			printDetails(statistics);
		}
	}

	private static class SimpleComparisonFormat extends BaseFormat {
		private QueryConfiguration testCfg;
		private DataConfiguration dataCfg;

		@Override
		public void printTestSetup(String label, QueryConfiguration queryCfg) {
			this.testCfg = queryCfg;
		}

		@Override
		public void printTestComplete() {}

		@Override
		public void printTestRun(String testName, RunStatistics statistics) {
			// System.out.println(testName);
			printSummary(statistics);
			printDetails(statistics);
		}

		@Override
		public void printSummary(RunStatistics statistics) {

			String columnPattern = getColumnPattern(15);
			EventKeeper timer = statistics.totalTimer;
			System.out.printf(
			    columnPattern, dataCfg.numRows, dataCfg.recordSizeBytes,
			    timer.getCount(RANGE_QUERY_COUNT), testCfg.rowsPerScan, timer.getCount(Events.JNI_CALL),
			    lf.format(timer.getCount(Events.RANGE_QUERY_FETCHES) / timer.getCount(RANGE_QUERY_COUNT)),
			    df.format(Math.floor(timer.getCount(Events.RANGE_QUERY_RECORDS_FETCHED) /
			                         (double)timer.getCount(Events.RANGE_QUERY_FETCHES))),
			    df.format(statistics.queryFetchTimes.median() / 1000f),
			    df.format(statistics.queryFetchTimes.percentile(.90) / 1000f),
			    df.format(statistics.scanRuntime.median() / 1000f), df.format(statistics.scanRuntime.p(.90) / 1000f),
			    df.format(statistics.fullScanQueryThroughput.median() * 1e9),
			    df.format(timer.getCount(Events.RANGE_QUERY_RECORDS_FETCHED) /
			              (double)timer.getTimeNanos(TOTAL_TIME_NANOS) * 1e9),
			    timer.getCount(Events.BYTES_FETCHED),
			    df.format(timer.getCount(Events.BYTES_FETCHED) / timer.getCount(RANGE_QUERY_COUNT)));
		}

		@Override
		public void printDetails(RunStatistics statistics) {
			// no-op, all data is held in summary
		}
		@Override
		protected String getHline(int numCols) {
			return "";
		}
		@Override
		protected String getColumnPattern(int numCols) {
			String line = "%s";
			for (int i = 0; i < numCols - 1; i++) {
				line += ",%s";
			}
			line += "%n";
			return line;
		}
		@Override
		protected String getHeaderPattern(int numCols) {
			return getColumnPattern(numCols);
		}

		@Override
		public void printDataSetup(String label, DataConfiguration dataCfg) {
			super.printDataSetup(label, dataCfg);

			this.dataCfg = dataCfg;
			String headerPattern = getHeaderPattern(16);
			System.out.printf(headerPattern, "Num Records", "Record Size(bytes)", "Batch cfg", "Num Scans",
			                  "Records/Scan", "Num JNI", "Num Fetches/Scan", "Records/Fetch",
			                  "Median fetch time(micros)", "p90 fetch time(micros)", "Median scan runtime(micros)",
			                  "p90 scan runtime(micros)", "Median scan throughput", "Overall scan throughput",
			                  "Num bytes", "bytes/scan");
		}
	}

	private static abstract class BaseDetailedTableFormat extends BaseFormat {

		@Override
		public void printTestSetup(String label, QueryConfiguration queryCfg) {
			String cfgPattern = getColumnPattern(2);
			String cfgHeaderPattern = getColumnPattern(2);
			String hline = getHline(2);
			System.out.printf(cfgHeaderPattern, "Query config", "value");
			System.out.println(hline);
			System.out.printf(cfgPattern, "warmup runs", queryCfg.numWarmupRuns);
			System.out.printf(cfgPattern, "test runs", queryCfg.numIterations);
			System.out.printf(cfgPattern, "max records/scan", queryCfg.rowsPerScan);
			System.out.printf(cfgPattern, "reversed", queryCfg.reversed);
			System.out.printf(cfgPattern, "mode", queryCfg.mode);

			System.out.println(hline);
			System.out.println();
		}

		@Override
		public void printSummary(RunStatistics statistics) {
			EventKeeper totalTimer = statistics.totalTimer;
			long rangeQueries = totalTimer.getCount(RANGE_QUERY_COUNT);
			long totalRecordsRead = totalTimer.getCount(Events.RANGE_QUERY_RECORDS_FETCHED);
			long totalTimeNanos = totalTimer.getTimeNanos(TOTAL_TIME_NANOS);
			long totalFetches = totalTimer.getCount(Events.RANGE_QUERY_FETCHES);
			long totalJni = totalTimer.getCount(Events.JNI_CALL);

			String summaryPattern = getColumnPattern(2);
			String summaryHline = getHline(2);
			System.out.printf(getHeaderPattern(2), "Total Measures", "value");
			System.out.println(summaryHline);
			System.out.printf(summaryPattern, "scans", lf.format(rangeQueries));
			System.out.printf(summaryPattern, "records", lf.format(totalRecordsRead));
			System.out.printf(summaryPattern, "records/scan", (totalRecordsRead / rangeQueries));
			System.out.printf(summaryPattern, "runtime(s)", toTimeString(totalTimeNanos));
			System.out.printf(summaryPattern, "fetches", totalFetches);
			System.out.printf(summaryPattern, "records/fetch", (totalRecordsRead / totalFetches));
			System.out.printf(summaryPattern, "fetches/scan", (totalFetches / rangeQueries));
			System.out.printf(summaryPattern, "JNI calls", totalJni);
			System.out.printf(summaryPattern, "JNI calls/scan", (totalJni / rangeQueries));

			double tputPerSec = ((double)totalRecordsRead / totalTimeNanos) * 1e9;
			System.out.printf(summaryPattern, "Throughput(records/s):", df.format(tputPerSec));
			System.out.printf(summaryPattern, "Bytes read", lf.format(totalTimer.getCount(Events.BYTES_FETCHED)));
			System.out.printf(summaryPattern, "Bytes/scan",
			                  df.format(totalTimer.getCount(Events.BYTES_FETCHED) / rangeQueries));
			System.out.printf(summaryPattern, "Bytes/fetch",
			                  df.format(totalTimer.getCount(Events.BYTES_FETCHED) / totalFetches));
			System.out.println(summaryHline);
		}

		@Override
		public void printDetails(RunStatistics statistics) {
			DoubleSample fsqt = statistics.fullScanQueryThroughput;
			LongSample qft = statistics.queryFetchTimes;
			DoubleSample runtime = statistics.scanRuntime;

			System.out.println("Detail Statistics");
			String pattern = getColumnPattern(8); //
			String headerPattern = getHeaderPattern(8);
			String detailHline = getHline(8);
			System.out.printf(headerPattern, "measurement", "min", "median", "p75", "p90", "p95", "max", "mean");
			System.out.println(detailHline);
			System.out.printf(pattern, "Throughput(records/s)", df.format(fsqt.min() * 1e9),
			                  df.format(fsqt.median() * 1e9), df.format(fsqt.p(.75) * 1e9), df.format(fsqt.p(.9) * 1e9),
			                  df.format(fsqt.p(.95) * 1e9), df.format(fsqt.max() * 1e9), df.format(fsqt.mean() * 1e9));
			System.out.printf(pattern, "fetch latency(micros)", df.format(qft.min() / 1000f),
			                  df.format(qft.median() / 1000f), df.format(qft.percentile(.75) / 1000f),
			                  df.format(qft.percentile(.90) / 1000f), df.format(qft.percentile(.95) / 1000f),
			                  df.format(qft.max() / 1000f), df.format(qft.mean() / 1000f));
			System.out.printf(pattern, "scan runtime(micros)", df.format(runtime.min() / 1000f),
			                  df.format(runtime.median() / 1000f), df.format(runtime.percentile(.75) / 1000f),
			                  df.format(runtime.percentile(.90) / 1000f), df.format(runtime.percentile(.95) / 1000f),
			                  df.format(runtime.max() / 1000f), df.format(runtime.mean() / 1000f));

			System.out.println(detailHline);
			System.out.println();
		}
		@Override
		public void printTestComplete() {
			// TODO Auto-generated method stub
		}
	}
	private static class CsvTableFormat extends BaseDetailedTableFormat {

		@Override
		public void printTestComplete() {
			System.out.println();
		}

		@Override
		protected String getHline(int numCols) {
			return ""; // no hlines in CSV format
		}

		@Override
		protected String getColumnPattern(int numCols) {
			String sep = ",";
			String pattern = "%s";
			for (int i = 0; i < numCols - 1; i++) {
				pattern += sep + "%s";
			}
			pattern += "%n";
			return pattern;
		}

		@Override
		protected String getHeaderPattern(int numCols) {
			return getColumnPattern(numCols);
		}
	}
	private static class PrettyTableFormat extends BaseDetailedTableFormat {

		private static String makeHline(int length) {
			String hline = "=";
			for (int i = 0; i < length; i++) {
				hline += "=";
			}
			return hline;
		}

		@Override
		public void printTestComplete() {
			System.out.println();
		}

		@Override
		protected String getColumnPattern(int numCols) {
			String basePattern = "%-22s";
			for (int i = 0; i < numCols - 1; i++) {
				basePattern += " | %11s";
			}
			basePattern += "%n";
			return basePattern;
		}

		@Override
		protected String getHeaderPattern(int numCols) {
			String basePattern = "%-22s";
			for (int i = 0; i < numCols - 1; i++) {
				basePattern += " |    %-8s";
			}
			basePattern += "%n";
			return basePattern;
		}

		@Override
		protected String getHline(int numCols) {
			return makeHline(numCols * 14 + 8);
		}
	}

	private static class MapTimer implements EventKeeper {
		private final ConcurrentMap<Event, AtomicLong> counterMap = new ConcurrentHashMap<>();

		@Override
		public void count(Event event, long amt) {
			AtomicLong l = counterMap.get(event);
			if (l == null) {
				l = new AtomicLong(0L);
				AtomicLong old = counterMap.putIfAbsent(event, l);
				if (old != null) {
					l = old;
				}
			}
			l.addAndGet(amt);
		}

		public void clear() { counterMap.clear(); }

		public void printEvents(String prefix) {
			System.out.println("[" + prefix + "] Events: ");
			for (Map.Entry<Event, AtomicLong> entry : counterMap.entrySet()) {
				Event event = entry.getKey();
				long cnt = entry.getValue().get();
				if (event.isTimeEvent()) {
					System.out.printf("%s: %.3f micros%n", event.name(), cnt / 1000f);
				} else {
					System.out.printf("%s: %d %n", event.name(), cnt);
				}
			}
			System.out.println("-------------------");
		}

		@Override
		public void timeNanos(Event event, long nanos) {
			count(event, nanos);
		}

		@Override
		public long getCount(Event event) {
			AtomicLong al = counterMap.get(event);
			if (al == null) {
				return 0L;
			} else {
				return al.get();
			}
		}

		@Override
		public long getTimeNanos(Event event) {
			return getCount(event);
		}
	}
}
