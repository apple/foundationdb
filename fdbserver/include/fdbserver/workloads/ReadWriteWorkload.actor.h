/*
 * ReadWriteWorkload.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_READWRITEWORKLOAD_ACTOR_G_H)
#define FDBSERVER_READWRITEWORKLOAD_ACTOR_G_H
#include "fdbserver/workloads/ReadWriteWorkload.actor.g.h"
#elif !defined(FDBSERVER_READWRITEWORKLOAD_ACTOR_H)
#define FDBSERVER_READWRITEWORKLOAD_ACTOR_H

#include "fdbrpc/DDSketch.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/TDMetric.actor.h"
#include <boost/lexical_cast.hpp>
#include "flow/actorcompiler.h" // This must be the last #include.
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

// Common ReadWrite test settings
struct ReadWriteCommon : KVWorkload {
	static constexpr double sampleError = 0.01;
	friend struct ReadWriteCommonImpl;

	// general test setting
	Standalone<StringRef> descriptionString;
	bool doSetup, cancelWorkersAtDuration;
	double testDuration, transactionsPerSecond, warmingDelay, maxInsertRate, debugInterval, debugTime;
	double metricsStart, metricsDuration;
	std::vector<uint64_t> insertionCountsToMeasure; // measure the speed of sequential insertion when bulkSetup

	// test log setting
	bool enableReadLatencyLogging;
	double periodicLoggingInterval;

	// two type of transaction
	int readsPerTransactionA, writesPerTransactionA;
	int readsPerTransactionB, writesPerTransactionB;
	double alpha; // probability for run TransactionA type
	// transaction setting
	bool useRYW;

	// states of metric
	Int64MetricHandle totalReadsMetric;
	Int64MetricHandle totalRetriesMetric;
	EventMetricHandle<TransactionSuccessMetric> transactionSuccessMetric;
	EventMetricHandle<TransactionFailureMetric> transactionFailureMetric;
	EventMetricHandle<ReadMetric> readMetric;
	PerfIntCounter aTransactions, bTransactions, retries;
	DDSketch<double> latencies, readLatencies, commitLatencies, GRVLatencies, fullReadLatencies;
	double readLatencyTotal;
	int readLatencyCount;
	std::vector<PerfMetric> periodicMetrics;
	std::vector<std::pair<uint64_t, double>> ratesAtKeyCounts; // sequential insertion speed

	// other internal states
	std::vector<Future<Void>> clients;
	double loadTime, clientBegin;

	explicit ReadWriteCommon(WorkloadContext const& wcx)
	  : KVWorkload(wcx), totalReadsMetric("ReadWrite.TotalReads"_sr), totalRetriesMetric("ReadWrite.TotalRetries"_sr),
	    aTransactions("A Transactions"), bTransactions("B Transactions"), retries("Retries"), latencies(sampleError),
	    readLatencies(sampleError), commitLatencies(sampleError), GRVLatencies(sampleError),
	    fullReadLatencies(sampleError), readLatencyTotal(0), readLatencyCount(0), loadTime(0.0), clientBegin(0) {

		transactionSuccessMetric.init("ReadWrite.SuccessfulTransaction"_sr);
		transactionFailureMetric.init("ReadWrite.FailedTransaction"_sr);
		readMetric.init("ReadWrite.Read"_sr);

		testDuration = getOption(options, "testDuration"_sr, 10.0);
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 5000.0) / clientCount;
		double allowedLatency = getOption(options, "allowedLatency"_sr, 0.250);
		actorCount = ceil(transactionsPerSecond * allowedLatency);
		actorCount = getOption(options, "actorCountPerTester"_sr, actorCount);

		readsPerTransactionA = getOption(options, "readsPerTransactionA"_sr, 10);
		writesPerTransactionA = getOption(options, "writesPerTransactionA"_sr, 0);
		readsPerTransactionB = getOption(options, "readsPerTransactionB"_sr, 1);
		writesPerTransactionB = getOption(options, "writesPerTransactionB"_sr, 9);
		alpha = getOption(options, "alpha"_sr, 0.1);

		if (nodePrefix > 0) {
			keyBytes += 16;
		}

		metricsStart = getOption(options, "metricsStart"_sr, 0.0);
		metricsDuration = getOption(options, "metricsDuration"_sr, testDuration);
		if (getOption(options, "discardEdgeMeasurements"_sr, true)) {
			// discardEdgeMeasurements keeps the metrics from the middle 3/4 of the test
			metricsStart += testDuration * 0.125;
			metricsDuration *= 0.75;
		}

		warmingDelay = getOption(options, "warmingDelay"_sr, 0.0);
		maxInsertRate = getOption(options, "maxInsertRate"_sr, 1e12);
		debugInterval = getOption(options, "debugInterval"_sr, 0.0);
		debugTime = getOption(options, "debugTime"_sr, 0.0);
		enableReadLatencyLogging = getOption(options, "enableReadLatencyLogging"_sr, false);
		periodicLoggingInterval = getOption(options, "periodicLoggingInterval"_sr, 5.0);
		cancelWorkersAtDuration = getOption(options, "cancelWorkersAtDuration"_sr, true);

		useRYW = getOption(options, "useRYW"_sr, false);
		doSetup = getOption(options, "setup"_sr, true);

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
		    getOption(options, "insertionCountsToMeasure"_sr, std::vector<std::string>());
		for (int i = 0; i < insertionCountsToMeasureString.size(); i++) {
			try {
				uint64_t count = boost::lexical_cast<uint64_t>(insertionCountsToMeasureString[i]);
				insertionCountsToMeasure.push_back(count);
			} catch (...) {
			}
		}
	}

	Future<Void> tracePeriodically();
	Future<Void> logLatency(Future<Optional<Value>> f, bool shouldRecord);
	Future<Void> logLatency(Future<RangeResult> f, bool shouldRecord);

	Future<Void> setup(Database const& cx) override;
	Future<bool> check(Database const& cx) override;
	void getMetrics(std::vector<PerfMetric>& m) override;

	Standalone<KeyValueRef> operator()(uint64_t n);
	bool shouldRecord(double checkTime = now());
};

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_READWRITEWORKLOAD_ACTOR_H
