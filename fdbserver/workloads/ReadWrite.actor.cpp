/*
 * ReadWrite.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include <boost/lexical_cast.hpp>
#include <utility>
#include <vector>

#include "fdbrpc/ContinuousSample.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/TDMetric.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const int sampleSize = 10000;
static Future<Version> nextRV;
static Version lastRV = invalidVersion;

ACTOR static Future<Version> getNextRV(Database db) {
	state Transaction tr(db);
	loop {
		try {
			Version v = wait(tr.getReadVersion());
			return v;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}
static Future<Version> getInconsistentReadVersion(Database const& db) {
	if (!nextRV.isValid() || nextRV.isReady()) { // if no getNextRV() running
		if (nextRV.isValid())
			lastRV = nextRV.get();
		nextRV = getNextRV(db);
	}
	if (lastRV == invalidVersion)
		return nextRV;
	else
		return lastRV;
}

DESCR struct TransactionSuccessMetric {
	int64_t totalLatency; // ns
	int64_t startLatency; // ns
	int64_t commitLatency; // ns
	int64_t retries; // count
};

DESCR struct TransactionFailureMetric {
	int64_t startLatency; // ns
	int64_t errorCode; // flow error code
};

DESCR struct ReadMetric {
	int64_t readLatency; // ns
};

struct ReadWriteWorkload : KVWorkload {
	int readsPerTransactionA, writesPerTransactionA;
	int readsPerTransactionB, writesPerTransactionB;
	int extraReadConflictRangesPerTransaction, extraWriteConflictRangesPerTransaction;
	double testDuration, transactionsPerSecond, alpha, warmingDelay, loadTime, maxInsertRate, debugInterval, debugTime;
	double metricsStart, metricsDuration, clientBegin;
	std::string valueString;

	bool dependentReads;
	bool enableReadLatencyLogging;
	double periodicLoggingInterval;
	bool cancelWorkersAtDuration;
	bool inconsistentReads;
	bool adjacentReads;
	bool adjacentWrites;
	bool rampUpLoad;
	int rampSweepCount;
	double hotKeyFraction, forceHotProbability;
	bool rangeReads;
	bool useRYW;
	bool rampTransactionType;
	bool rampUpConcurrency;
	bool batchPriority;

	Standalone<StringRef> descriptionString;

	Int64MetricHandle totalReadsMetric;
	Int64MetricHandle totalRetriesMetric;
	EventMetricHandle<TransactionSuccessMetric> transactionSuccessMetric;
	EventMetricHandle<TransactionFailureMetric> transactionFailureMetric;
	EventMetricHandle<ReadMetric> readMetric;

	std::vector<Future<Void>> clients;
	PerfIntCounter aTransactions, bTransactions, retries;
	ContinuousSample<double> latencies, readLatencies, commitLatencies, GRVLatencies, fullReadLatencies;
	double readLatencyTotal;
	int readLatencyCount;

	std::vector<uint64_t> insertionCountsToMeasure;
	std::vector<std::pair<uint64_t, double>> ratesAtKeyCounts;

	std::vector<PerfMetric> periodicMetrics;

	bool doSetup;

	ReadWriteWorkload(WorkloadContext const& wcx)
	  : KVWorkload(wcx), loadTime(0.0), clientBegin(0), dependentReads(false), adjacentReads(false),
	    adjacentWrites(false), totalReadsMetric(LiteralStringRef("RWWorkload.TotalReads")),
	    totalRetriesMetric(LiteralStringRef("RWWorkload.TotalRetries")), aTransactions("A Transactions"),
	    bTransactions("B Transactions"), retries("Retries"), latencies(sampleSize), readLatencies(sampleSize),
	    commitLatencies(sampleSize), GRVLatencies(sampleSize), fullReadLatencies(sampleSize), readLatencyTotal(0),
	    readLatencyCount(0) {
		transactionSuccessMetric.init(LiteralStringRef("RWWorkload.SuccessfulTransaction"));
		transactionFailureMetric.init(LiteralStringRef("RWWorkload.FailedTransaction"));
		readMetric.init(LiteralStringRef("RWWorkload.Read"));

		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 5000.0) / clientCount;
		double allowedLatency = getOption(options, LiteralStringRef("allowedLatency"), 0.250);
		actorCount = ceil(transactionsPerSecond * allowedLatency);
		actorCount = getOption(options, LiteralStringRef("actorCountPerTester"), actorCount);

		readsPerTransactionA = getOption(options, LiteralStringRef("readsPerTransactionA"), 10);
		writesPerTransactionA = getOption(options, LiteralStringRef("writesPerTransactionA"), 0);
		readsPerTransactionB = getOption(options, LiteralStringRef("readsPerTransactionB"), 1);
		writesPerTransactionB = getOption(options, LiteralStringRef("writesPerTransactionB"), 9);
		alpha = getOption(options, LiteralStringRef("alpha"), 0.1);

		extraReadConflictRangesPerTransaction =
		    getOption(options, LiteralStringRef("extraReadConflictRangesPerTransaction"), 0);
		extraWriteConflictRangesPerTransaction =
		    getOption(options, LiteralStringRef("extraWriteConflictRangesPerTransaction"), 0);

		valueString = std::string(maxValueBytes, '.');
		if (nodePrefix > 0) {
			keyBytes += 16;
		}

		metricsStart = getOption(options, LiteralStringRef("metricsStart"), 0.0);
		metricsDuration = getOption(options, LiteralStringRef("metricsDuration"), testDuration);
		if (getOption(options, LiteralStringRef("discardEdgeMeasurements"), true)) {
			// discardEdgeMeasurements keeps the metrics from the middle 3/4 of the test
			metricsStart += testDuration * 0.125;
			metricsDuration *= 0.75;
		}

		dependentReads = getOption(options, LiteralStringRef("dependentReads"), false);
		warmingDelay = getOption(options, LiteralStringRef("warmingDelay"), 0.0);
		maxInsertRate = getOption(options, LiteralStringRef("maxInsertRate"), 1e12);
		debugInterval = getOption(options, LiteralStringRef("debugInterval"), 0.0);
		debugTime = getOption(options, LiteralStringRef("debugTime"), 0.0);
		enableReadLatencyLogging = getOption(options, LiteralStringRef("enableReadLatencyLogging"), false);
		periodicLoggingInterval = getOption(options, LiteralStringRef("periodicLoggingInterval"), 5.0);
		cancelWorkersAtDuration = getOption(options, LiteralStringRef("cancelWorkersAtDuration"), true);
		inconsistentReads = getOption(options, LiteralStringRef("inconsistentReads"), false);
		adjacentReads = getOption(options, LiteralStringRef("adjacentReads"), false);
		adjacentWrites = getOption(options, LiteralStringRef("adjacentWrites"), false);
		rampUpLoad = getOption(options, LiteralStringRef("rampUpLoad"), false);
		useRYW = getOption(options, LiteralStringRef("useRYW"), false);
		rampSweepCount = getOption(options, LiteralStringRef("rampSweepCount"), 1);
		rangeReads = getOption(options, LiteralStringRef("rangeReads"), false);
		rampTransactionType = getOption(options, LiteralStringRef("rampTransactionType"), false);
		rampUpConcurrency = getOption(options, LiteralStringRef("rampUpConcurrency"), false);
		doSetup = getOption(options, LiteralStringRef("setup"), true);
		batchPriority = getOption(options, LiteralStringRef("batchPriority"), false);
		descriptionString = getOption(options, LiteralStringRef("description"), LiteralStringRef("ReadWrite"));

		if (rampUpConcurrency)
			ASSERT(rampSweepCount == 2); // Implementation is hard coded to ramp up and down

		// Validate that keyForIndex() is monotonic
		for (int i = 0; i < 30; i++) {
			int64_t a = deterministicRandom()->randomInt64(0, nodeCount);
			int64_t b = deterministicRandom()->randomInt64(0, nodeCount);
			if (a > b) {
				std::swap(a, b);
			}
			ASSERT(a <= b);
			ASSERT((keyForIndex(a, false) <= keyForIndex(b, false)));
		}

		std::vector<std::string> insertionCountsToMeasureString =
		    getOption(options, LiteralStringRef("insertionCountsToMeasure"), std::vector<std::string>());
		for (int i = 0; i < insertionCountsToMeasureString.size(); i++) {
			try {
				uint64_t count = boost::lexical_cast<uint64_t>(insertionCountsToMeasureString[i]);
				insertionCountsToMeasure.push_back(count);
			} catch (...) {
			}
		}

		{
			// with P(hotTrafficFraction) an access is directed to one of a fraction
			//   of hot keys, else it is directed to a disjoint set of cold keys
			hotKeyFraction = getOption(options, LiteralStringRef("hotKeyFraction"), 0.0);
			double hotTrafficFraction = getOption(options, LiteralStringRef("hotTrafficFraction"), 0.0);
			ASSERT(hotKeyFraction >= 0 && hotTrafficFraction <= 1);
			ASSERT(hotKeyFraction <= hotTrafficFraction); // hot keys should be actually hot!
			// p(Cold key) = (1-FHP) * (1-hkf)
			// p(Cold key) = (1-htf)
			// solving for FHP gives:
			forceHotProbability = (hotTrafficFraction - hotKeyFraction) / (1 - hotKeyFraction);
		}
	}

	std::string description() const override { return descriptionString.toString(); }
	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	ACTOR static Future<bool> traceDumpWorkers(Reference<AsyncVar<ServerDBInfo> const> db) {
		try {
			loop {
				choose {
					when(wait(db->onChange())) {}

					when(ErrorOr<std::vector<WorkerDetails>> workerList =
					         wait(db->get().clusterInterface.getWorkers.tryGetReply(GetWorkersRequest()))) {
						if (workerList.present()) {
							std::vector<Future<ErrorOr<Void>>> dumpRequests;
							dumpRequests.reserve(workerList.get().size());
							for (int i = 0; i < workerList.get().size(); i++)
								dumpRequests.push_back(workerList.get()[i].interf.traceBatchDumpRequest.tryGetReply(
								    TraceBatchDumpRequest()));
							wait(waitForAll(dumpRequests));
							return true;
						}
						wait(delay(1.0));
					}
				}
			}
		} catch (Error& e) {
			TraceEvent(SevError, "FailedToDumpWorkers").error(e);
			throw;
		}
	}

	Future<bool> check(Database const& cx) override {
		clients.clear();

		if (!cancelWorkersAtDuration && now() < metricsStart + metricsDuration)
			metricsDuration = now() - metricsStart;

		g_traceBatch.dump();
		if (clientId == 0)
			return traceDumpWorkers(dbInfo);
		else
			return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		double duration = metricsDuration;
		int reads =
		    (aTransactions.getValue() * readsPerTransactionA) + (bTransactions.getValue() * readsPerTransactionB);
		int writes =
		    (aTransactions.getValue() * writesPerTransactionA) + (bTransactions.getValue() * writesPerTransactionB);
		m.emplace_back("Measured Duration", duration, Averaged::True);
		m.emplace_back(
		    "Transactions/sec", (aTransactions.getValue() + bTransactions.getValue()) / duration, Averaged::False);
		m.emplace_back("Operations/sec", ((reads + writes) / duration), Averaged::False);
		m.push_back(aTransactions.getMetric());
		m.push_back(bTransactions.getMetric());
		m.push_back(retries.getMetric());
		m.emplace_back("Mean load time (seconds)", loadTime, Averaged::True);
		m.emplace_back("Read rows", reads, Averaged::False);
		m.emplace_back("Write rows", writes, Averaged::False);

		if (!rampUpLoad) {
			m.emplace_back("Mean Latency (ms)", 1000 * latencies.mean(), Averaged::True);
			m.emplace_back("Median Latency (ms, averaged)", 1000 * latencies.median(), Averaged::True);
			m.emplace_back("90% Latency (ms, averaged)", 1000 * latencies.percentile(0.90), Averaged::True);
			m.emplace_back("98% Latency (ms, averaged)", 1000 * latencies.percentile(0.98), Averaged::True);
			m.emplace_back("Max Latency (ms, averaged)", 1000 * latencies.max(), Averaged::True);

			m.emplace_back("Mean Row Read Latency (ms)", 1000 * readLatencies.mean(), Averaged::True);
			m.emplace_back("Median Row Read Latency (ms, averaged)", 1000 * readLatencies.median(), Averaged::True);
			m.emplace_back("Max Row Read Latency (ms, averaged)", 1000 * readLatencies.max(), Averaged::True);

			m.emplace_back("Mean Total Read Latency (ms)", 1000 * fullReadLatencies.mean(), Averaged::True);
			m.emplace_back(
			    "Median Total Read Latency (ms, averaged)", 1000 * fullReadLatencies.median(), Averaged::True);
			m.emplace_back("Max Total Latency (ms, averaged)", 1000 * fullReadLatencies.max(), Averaged::True);

			m.emplace_back("Mean GRV Latency (ms)", 1000 * GRVLatencies.mean(), Averaged::True);
			m.emplace_back("Median GRV Latency (ms, averaged)", 1000 * GRVLatencies.median(), Averaged::True);
			m.emplace_back("Max GRV Latency (ms, averaged)", 1000 * GRVLatencies.max(), Averaged::True);

			m.emplace_back("Mean Commit Latency (ms)", 1000 * commitLatencies.mean(), Averaged::True);
			m.emplace_back("Median Commit Latency (ms, averaged)", 1000 * commitLatencies.median(), Averaged::True);
			m.emplace_back("Max Commit Latency (ms, averaged)", 1000 * commitLatencies.max(), Averaged::True);
		}

		m.emplace_back("Read rows/sec", reads / duration, Averaged::False);
		m.emplace_back("Write rows/sec", writes / duration, Averaged::False);
		m.emplace_back(
		    "Bytes read/sec", (reads * (keyBytes + (minValueBytes + maxValueBytes) * 0.5)) / duration, Averaged::False);
		m.emplace_back("Bytes written/sec",
		               (writes * (keyBytes + (minValueBytes + maxValueBytes) * 0.5)) / duration,
		               Averaged::False);
		m.insert(m.end(), periodicMetrics.begin(), periodicMetrics.end());

		std::vector<std::pair<uint64_t, double>>::iterator ratesItr = ratesAtKeyCounts.begin();
		for (; ratesItr != ratesAtKeyCounts.end(); ratesItr++)
			m.emplace_back(format("%lld keys imported bytes/sec", ratesItr->first), ratesItr->second, Averaged::False);
	}

	Value randomValue() {
		return StringRef((uint8_t*)valueString.c_str(),
		                 deterministicRandom()->randomInt(minValueBytes, maxValueBytes + 1));
	}

	Standalone<KeyValueRef> operator()(uint64_t n) { return KeyValueRef(keyForIndex(n, false), randomValue()); }

	template <class Trans>
	void setupTransaction(Trans* tr) {
		if (batchPriority) {
			tr->setOption(FDBTransactionOptions::PRIORITY_BATCH);
		}
	}

	ACTOR static Future<Void> tracePeriodically(ReadWriteWorkload* self) {
		state double start = now();
		state double elapsed = 0.0;
		state int64_t last_ops = 0;

		loop {
			elapsed += self->periodicLoggingInterval;
			wait(delayUntil(start + elapsed));

			TraceEvent((self->description() + "_RowReadLatency").c_str())
			    .detail("Mean", self->readLatencies.mean())
			    .detail("Median", self->readLatencies.median())
			    .detail("Percentile5", self->readLatencies.percentile(.05))
			    .detail("Percentile95", self->readLatencies.percentile(.95))
			    .detail("Percentile99", self->readLatencies.percentile(.99))
			    .detail("Percentile99_9", self->readLatencies.percentile(.999))
			    .detail("Max", self->readLatencies.max())
			    .detail("Count", self->readLatencyCount)
			    .detail("Elapsed", elapsed);

			TraceEvent((self->description() + "_GRVLatency").c_str())
			    .detail("Mean", self->GRVLatencies.mean())
			    .detail("Median", self->GRVLatencies.median())
			    .detail("Percentile5", self->GRVLatencies.percentile(.05))
			    .detail("Percentile95", self->GRVLatencies.percentile(.95))
			    .detail("Percentile99", self->GRVLatencies.percentile(.99))
			    .detail("Percentile99_9", self->GRVLatencies.percentile(.999))
			    .detail("Max", self->GRVLatencies.max());

			TraceEvent((self->description() + "_CommitLatency").c_str())
			    .detail("Mean", self->commitLatencies.mean())
			    .detail("Median", self->commitLatencies.median())
			    .detail("Percentile5", self->commitLatencies.percentile(.05))
			    .detail("Percentile95", self->commitLatencies.percentile(.95))
			    .detail("Percentile99", self->commitLatencies.percentile(.99))
			    .detail("Percentile99_9", self->commitLatencies.percentile(.999))
			    .detail("Max", self->commitLatencies.max());

			TraceEvent((self->description() + "_TotalLatency").c_str())
			    .detail("Mean", self->latencies.mean())
			    .detail("Median", self->latencies.median())
			    .detail("Percentile5", self->latencies.percentile(.05))
			    .detail("Percentile95", self->latencies.percentile(.95))
			    .detail("Percentile99", self->latencies.percentile(.99))
			    .detail("Percentile99_9", self->latencies.percentile(.999))
			    .detail("Max", self->latencies.max());

			int64_t ops =
			    (self->aTransactions.getValue() * (self->readsPerTransactionA + self->writesPerTransactionA)) +
			    (self->bTransactions.getValue() * (self->readsPerTransactionB + self->writesPerTransactionB));
			bool recordBegin = self->shouldRecord(std::max(now() - self->periodicLoggingInterval, self->clientBegin));
			bool recordEnd = self->shouldRecord(now());
			if (recordBegin && recordEnd) {
				std::string ts = format("T=%04.0fs:", elapsed);
				self->periodicMetrics.emplace_back(
				    ts + "Operations/sec", (ops - last_ops) / self->periodicLoggingInterval, Averaged::False);

				// if(self->rampUpLoad) {
				self->periodicMetrics.emplace_back(
				    ts + "Mean Latency (ms)", 1000 * self->latencies.mean(), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "Median Latency (ms, averaged)", 1000 * self->latencies.median(), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "5% Latency (ms, averaged)", 1000 * self->latencies.percentile(.05), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "95% Latency (ms, averaged)", 1000 * self->latencies.percentile(.95), Averaged::True);

				self->periodicMetrics.emplace_back(
				    ts + "Mean Row Read Latency (ms)", 1000 * self->readLatencies.mean(), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "Median Row Read Latency (ms, averaged)", 1000 * self->readLatencies.median(), Averaged::True);
				self->periodicMetrics.emplace_back(ts + "5% Row Read Latency (ms, averaged)",
				                                   1000 * self->readLatencies.percentile(.05),
				                                   Averaged::True);
				self->periodicMetrics.emplace_back(ts + "95% Row Read Latency (ms, averaged)",
				                                   1000 * self->readLatencies.percentile(.95),
				                                   Averaged::True);

				self->periodicMetrics.emplace_back(
				    ts + "Mean Total Read Latency (ms)", 1000 * self->fullReadLatencies.mean(), Averaged::True);
				self->periodicMetrics.emplace_back(ts + "Median Total Read Latency (ms, averaged)",
				                                   1000 * self->fullReadLatencies.median(),
				                                   Averaged::True);
				self->periodicMetrics.emplace_back(ts + "5% Total Read Latency (ms, averaged)",
				                                   1000 * self->fullReadLatencies.percentile(.05),
				                                   Averaged::True);
				self->periodicMetrics.emplace_back(ts + "95% Total Read Latency (ms, averaged)",
				                                   1000 * self->fullReadLatencies.percentile(.95),
				                                   Averaged::True);

				self->periodicMetrics.emplace_back(
				    ts + "Mean GRV Latency (ms)", 1000 * self->GRVLatencies.mean(), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "Median GRV Latency (ms, averaged)", 1000 * self->GRVLatencies.median(), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "5% GRV Latency (ms, averaged)", 1000 * self->GRVLatencies.percentile(.05), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "95% GRV Latency (ms, averaged)", 1000 * self->GRVLatencies.percentile(.95), Averaged::True);

				self->periodicMetrics.emplace_back(
				    ts + "Mean Commit Latency (ms)", 1000 * self->commitLatencies.mean(), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "Median Commit Latency (ms, averaged)", 1000 * self->commitLatencies.median(), Averaged::True);
				self->periodicMetrics.emplace_back(ts + "5% Commit Latency (ms, averaged)",
				                                   1000 * self->commitLatencies.percentile(.05),
				                                   Averaged::True);
				self->periodicMetrics.emplace_back(ts + "95% Commit Latency (ms, averaged)",
				                                   1000 * self->commitLatencies.percentile(.95),
				                                   Averaged::True);
				//}

				self->periodicMetrics.emplace_back(
				    ts + "Max Latency (ms, averaged)", 1000 * self->latencies.max(), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "Max Row Read Latency (ms, averaged)", 1000 * self->readLatencies.max(), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "Max Total Read Latency (ms, averaged)", 1000 * self->fullReadLatencies.max(), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "Max GRV Latency (ms, averaged)", 1000 * self->GRVLatencies.max(), Averaged::True);
				self->periodicMetrics.emplace_back(
				    ts + "Max Commit Latency (ms, averaged)", 1000 * self->commitLatencies.max(), Averaged::True);
			}
			last_ops = ops;

			// if(self->rampUpLoad) {
			self->latencies.clear();
			self->readLatencies.clear();
			self->fullReadLatencies.clear();
			self->GRVLatencies.clear();
			self->commitLatencies.clear();
			//}

			self->readLatencyTotal = 0.0;
			self->readLatencyCount = 0;
		}
	}

	ACTOR static Future<Void> logLatency(Future<Optional<Value>> f,
	                                     ContinuousSample<double>* latencies,
	                                     double* totalLatency,
	                                     int* latencyCount,
	                                     EventMetricHandle<ReadMetric> readMetric,
	                                     bool shouldRecord) {
		state double readBegin = now();
		Optional<Value> value = wait(f);

		double latency = now() - readBegin;
		readMetric->readLatency = latency * 1e9;
		readMetric->log();

		if (shouldRecord) {
			*totalLatency += latency;
			++*latencyCount;
			latencies->addSample(latency);
		}
		return Void();
	}

	ACTOR static Future<Void> logLatency(Future<RangeResult> f,
	                                     ContinuousSample<double>* latencies,
	                                     double* totalLatency,
	                                     int* latencyCount,
	                                     EventMetricHandle<ReadMetric> readMetric,
	                                     bool shouldRecord) {
		state double readBegin = now();
		RangeResult value = wait(f);

		double latency = now() - readBegin;
		readMetric->readLatency = latency * 1e9;
		readMetric->log();

		if (shouldRecord) {
			*totalLatency += latency;
			++*latencyCount;
			latencies->addSample(latency);
		}
		return Void();
	}

	ACTOR template <class Trans>
	Future<Void> readOp(Trans* tr, std::vector<int64_t> keys, ReadWriteWorkload* self, bool shouldRecord) {
		if (!keys.size())
			return Void();
		if (!self->dependentReads) {
			std::vector<Future<Void>> readers;
			if (self->rangeReads) {
				for (int op = 0; op < keys.size(); op++) {
					++self->totalReadsMetric;
					readers.push_back(logLatency(
					    tr->getRange(KeyRangeRef(self->keyForIndex(keys[op]), Key(strinc(self->keyForIndex(keys[op])))),
					                 GetRangeLimits(-1, 80000)),
					    &self->readLatencies,
					    &self->readLatencyTotal,
					    &self->readLatencyCount,
					    self->readMetric,
					    shouldRecord));
				}
			} else {
				for (int op = 0; op < keys.size(); op++) {
					++self->totalReadsMetric;
					readers.push_back(logLatency(tr->get(self->keyForIndex(keys[op])),
					                             &self->readLatencies,
					                             &self->readLatencyTotal,
					                             &self->readLatencyCount,
					                             self->readMetric,
					                             shouldRecord));
				}
			}
			wait(waitForAll(readers));
		} else {
			state int op;
			for (op = 0; op < keys.size(); op++) {
				++self->totalReadsMetric;
				wait(logLatency(tr->get(self->keyForIndex(keys[op])),
				                &self->readLatencies,
				                &self->readLatencyTotal,
				                &self->readLatencyCount,
				                self->readMetric,
				                shouldRecord));
			}
		}
		return Void();
	}

	ACTOR Future<Void> _setup(Database cx, ReadWriteWorkload* self) {
		if (!self->doSetup)
			return Void();

		state Promise<double> loadTime;
		state Promise<std::vector<std::pair<uint64_t, double>>> ratesAtKeyCounts;

		wait(bulkSetup(cx,
		               self,
		               self->nodeCount,
		               loadTime,
		               self->insertionCountsToMeasure.empty(),
		               self->warmingDelay,
		               self->maxInsertRate,
		               self->insertionCountsToMeasure,
		               ratesAtKeyCounts));

		self->loadTime = loadTime.getFuture().get();
		self->ratesAtKeyCounts = ratesAtKeyCounts.getFuture().get();

		return Void();
	}

	ACTOR Future<Void> _start(Database cx, ReadWriteWorkload* self) {
		// Read one record from the database to warm the cache of keyServers
		state std::vector<int64_t> keys;
		keys.push_back(deterministicRandom()->randomInt64(0, self->nodeCount));
		state double startTime = now();
		loop {
			state Transaction tr(cx);

			try {
				self->setupTransaction(&tr);
				wait(self->readOp(&tr, keys, self, false));
				wait(tr.warmRange(allKeys));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		wait(delay(std::max(0.1, 1.0 - (now() - startTime))));

		std::vector<Future<Void>> clients;
		if (self->enableReadLatencyLogging)
			clients.push_back(tracePeriodically(self));

		self->clientBegin = now();
		for (int c = 0; c < self->actorCount; c++) {
			Future<Void> worker;
			if (self->useRYW)
				worker = self->randomReadWriteClient<ReadYourWritesTransaction>(
				    cx, self, self->actorCount / self->transactionsPerSecond, c);
			else
				worker = self->randomReadWriteClient<Transaction>(
				    cx, self, self->actorCount / self->transactionsPerSecond, c);
			clients.push_back(worker);
		}

		if (!self->cancelWorkersAtDuration)
			self->clients = clients; // Don't cancel them until check()

		wait(self->cancelWorkersAtDuration ? timeout(waitForAll(clients), self->testDuration, Void())
		                                   : delay(self->testDuration));
		return Void();
	}

	bool shouldRecord() { return shouldRecord(now()); }

	bool shouldRecord(double checkTime) {
		double timeSinceStart = checkTime - clientBegin;
		return timeSinceStart >= metricsStart && timeSinceStart < (metricsStart + metricsDuration);
	}

	int64_t getRandomKey(uint64_t nodeCount) {
		if (forceHotProbability && deterministicRandom()->random01() < forceHotProbability)
			return deterministicRandom()->randomInt64(0, nodeCount * hotKeyFraction) /
			       hotKeyFraction; // spread hot keys over keyspace
		else
			return deterministicRandom()->randomInt64(0, nodeCount);
	}

	double sweepAlpha(double startTime) {
		double sweepDuration = testDuration / rampSweepCount;
		double numSweeps = (now() - startTime) / sweepDuration;
		int currentSweep = (int)numSweeps;
		double alpha = numSweeps - currentSweep;
		if (currentSweep % 2)
			alpha = 1 - alpha;
		return alpha;
	}

	ACTOR template <class Trans>
	Future<Void> randomReadWriteClient(Database cx, ReadWriteWorkload* self, double delay, int clientIndex) {
		state double startTime = now();
		state double lastTime = now();
		state double GRVStartTime;
		state UID debugID;

		if (self->rampUpConcurrency) {
			wait(::delay(self->testDuration / 2 *
			             (double(clientIndex) / self->actorCount +
			              double(self->clientId) / self->clientCount / self->actorCount)));
			TraceEvent("ClientStarting")
			    .detail("ActorIndex", clientIndex)
			    .detail("ClientIndex", self->clientId)
			    .detail("NumActors", clientIndex * self->clientCount + self->clientId + 1);
		}

		loop {
			wait(poisson(&lastTime, delay));

			if (self->rampUpConcurrency) {
				if (now() - startTime >= self->testDuration / 2 *
				                             (2 - (double(clientIndex) / self->actorCount +
				                                   double(self->clientId) / self->clientCount / self->actorCount))) {
					TraceEvent("ClientStopping")
					    .detail("ActorIndex", clientIndex)
					    .detail("ClientIndex", self->clientId)
					    .detail("NumActors", clientIndex * self->clientCount + self->clientId);
					wait(Never());
				}
			}

			if (!self->rampUpLoad || deterministicRandom()->random01() < self->sweepAlpha(startTime)) {
				state double tstart = now();
				state bool aTransaction = deterministicRandom()->random01() >
				                          (self->rampTransactionType ? self->sweepAlpha(startTime) : self->alpha);

				state std::vector<int64_t> keys;
				state std::vector<Value> values;
				state std::vector<KeyRange> extra_ranges;
				int reads = aTransaction ? self->readsPerTransactionA : self->readsPerTransactionB;
				state int writes = aTransaction ? self->writesPerTransactionA : self->writesPerTransactionB;
				state int extra_read_conflict_ranges = writes ? self->extraReadConflictRangesPerTransaction : 0;
				state int extra_write_conflict_ranges = writes ? self->extraWriteConflictRangesPerTransaction : 0;
				if (!self->adjacentReads) {
					for (int op = 0; op < reads; op++)
						keys.push_back(self->getRandomKey(self->nodeCount));
				} else {
					int startKey = self->getRandomKey(self->nodeCount - reads);
					for (int op = 0; op < reads; op++)
						keys.push_back(startKey + op);
				}

				values.reserve(writes);
				for (int op = 0; op < writes; op++)
					values.push_back(self->randomValue());

				extra_ranges.reserve(extra_read_conflict_ranges + extra_write_conflict_ranges);
				for (int op = 0; op < extra_read_conflict_ranges + extra_write_conflict_ranges; op++)
					extra_ranges.push_back(singleKeyRange(deterministicRandom()->randomUniqueID().toString()));

				state Trans tr(cx);

				if (tstart - self->clientBegin > self->debugTime &&
				    tstart - self->clientBegin <= self->debugTime + self->debugInterval) {
					debugID = deterministicRandom()->randomUniqueID();
					tr.debugTransaction(debugID);
					g_traceBatch.addEvent(
					    "TransactionDebug", debugID.first(), "ReadWrite.randomReadWriteClient.Before");
				} else {
					debugID = UID();
				}

				self->transactionSuccessMetric->retries = 0;
				self->transactionSuccessMetric->commitLatency = -1;

				loop {
					try {
						self->setupTransaction(&tr);

						GRVStartTime = now();
						self->transactionFailureMetric->startLatency = -1;

						Version v =
						    wait(self->inconsistentReads ? getInconsistentReadVersion(cx) : tr.getReadVersion());
						if (self->inconsistentReads)
							tr.setVersion(v);

						double grvLatency = now() - GRVStartTime;
						self->transactionSuccessMetric->startLatency = grvLatency * 1e9;
						self->transactionFailureMetric->startLatency = grvLatency * 1e9;
						if (self->shouldRecord())
							self->GRVLatencies.addSample(grvLatency);

						state double readStart = now();
						wait(self->readOp(&tr, keys, self, self->shouldRecord()));

						double readLatency = now() - readStart;
						if (self->shouldRecord())
							self->fullReadLatencies.addSample(readLatency);

						if (!writes)
							break;

						if (self->adjacentWrites) {
							int64_t startKey = self->getRandomKey(self->nodeCount - writes);
							for (int op = 0; op < writes; op++)
								tr.set(self->keyForIndex(startKey + op, false), values[op]);
						} else {
							for (int op = 0; op < writes; op++)
								tr.set(self->keyForIndex(self->getRandomKey(self->nodeCount), false), values[op]);
						}
						for (int op = 0; op < extra_read_conflict_ranges; op++)
							tr.addReadConflictRange(extra_ranges[op]);
						for (int op = 0; op < extra_write_conflict_ranges; op++)
							tr.addWriteConflictRange(extra_ranges[op + extra_read_conflict_ranges]);

						state double commitStart = now();
						wait(tr.commit());

						double commitLatency = now() - commitStart;
						self->transactionSuccessMetric->commitLatency = commitLatency * 1e9;
						if (self->shouldRecord())
							self->commitLatencies.addSample(commitLatency);

						break;
					} catch (Error& e) {
						self->transactionFailureMetric->errorCode = e.code();
						self->transactionFailureMetric->log();

						wait(tr.onError(e));

						++self->transactionSuccessMetric->retries;
						++self->totalRetriesMetric;

						if (self->shouldRecord())
							++self->retries;
					}
				}

				if (debugID != UID())
					g_traceBatch.addEvent("TransactionDebug", debugID.first(), "ReadWrite.randomReadWriteClient.After");

				tr = Trans();

				double transactionLatency = now() - tstart;
				self->transactionSuccessMetric->totalLatency = transactionLatency * 1e9;
				self->transactionSuccessMetric->log();

				if (self->shouldRecord()) {
					if (aTransaction)
						++self->aTransactions;
					else
						++self->bTransactions;

					self->latencies.addSample(transactionLatency);
				}
			}
		}
	}
};

ACTOR Future<std::vector<std::pair<uint64_t, double>>> trackInsertionCount(Database cx,
                                                                           std::vector<uint64_t> countsOfInterest,
                                                                           double checkInterval) {
	state KeyRange keyPrefix = KeyRangeRef(std::string("keycount"), std::string("keycount") + char(255));
	state KeyRange bytesPrefix = KeyRangeRef(std::string("bytesstored"), std::string("bytesstored") + char(255));
	state Transaction tr(cx);
	state uint64_t lastInsertionCount = 0;
	state int currentCountIndex = 0;

	state std::vector<std::pair<uint64_t, double>> countInsertionRates;

	state double startTime = now();

	while (currentCountIndex < countsOfInterest.size()) {
		try {
			state Future<RangeResult> countFuture = tr.getRange(keyPrefix, 1000000000);
			state Future<RangeResult> bytesFuture = tr.getRange(bytesPrefix, 1000000000);
			wait(success(countFuture) && success(bytesFuture));

			RangeResult counts = countFuture.get();
			RangeResult bytes = bytesFuture.get();

			uint64_t numInserted = 0;
			for (int i = 0; i < counts.size(); i++)
				numInserted += *(uint64_t*)counts[i].value.begin();

			uint64_t bytesInserted = 0;
			for (int i = 0; i < bytes.size(); i++)
				bytesInserted += *(uint64_t*)bytes[i].value.begin();

			while (currentCountIndex < countsOfInterest.size() &&
			       countsOfInterest[currentCountIndex] > lastInsertionCount &&
			       countsOfInterest[currentCountIndex] <= numInserted)
				countInsertionRates.emplace_back(countsOfInterest[currentCountIndex++],
				                                 bytesInserted / (now() - startTime));

			lastInsertionCount = numInserted;
			wait(delay(checkInterval));
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return countInsertionRates;
}

WorkloadFactory<ReadWriteWorkload> ReadWriteWorkloadFactory("ReadWrite");
