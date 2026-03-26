/*
 * BulkSetup.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_BULK_SETUP_H
#define FDBSERVER_BULK_SETUP_H

#include <string>
#include <utility>
#include <vector>

#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/tester/workloads.actor.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/core/QuietDatabase.actor.h"
#include "fdbserver/tester/WorkloadUtils.h"
#include "fdbrpc/simulator.h"

template <class T>
struct sfinae_true : std::true_type {};

template <class T>
auto testAuthToken(int) -> sfinae_true<decltype(std::declval<T>().setAuthToken(std::declval<Transaction&>()))>;
template <class>
auto testAuthToken(long) -> std::false_type;

template <class T>
struct hasAuthToken : decltype(testAuthToken<T>(0)) {};

template <class T>
void setAuthToken(T const& self, Transaction& tr) {
	if constexpr (hasAuthToken<T>::value) {
		self.setAuthToken(tr);
	}
}

template <class T>
Future<bool> checkRangeSimpleValueSize(Database cx, T* workload, uint64_t begin, uint64_t end) {
	while (true) {
		Transaction tr(cx);
		Error err;
		setAuthToken(*workload, tr);
		try {
			Standalone<KeyValueRef> firstKV = (*workload)(begin);
			Standalone<KeyValueRef> lastKV = (*workload)(end - 1);
			Future<Optional<Value>> first = tr.get(firstKV.key);
			Future<Optional<Value>> last = tr.get(lastKV.key);
			co_await (success(first) && success(last));
			co_return first.get().present() && last.get().present();
		} catch (Error& e) {
			TraceEvent("CheckRangeError").error(e).detail("Begin", begin).detail("End", end);
			err = e;
		}
		if (err.isValid()) {
			co_await tr.onError(err);
		}
	}
}

// Returns true if the range was added
template <class T>
Future<uint64_t> setupRange(Database cx, T* workload, uint64_t begin, uint64_t end) {
	uint64_t bytesInserted = 0;
	while (true) {
		Transaction tr(cx);
		Error err;
		setAuthToken(*workload, tr);
		try {
			if (deterministicRandom()->random01() < 0.001) {
				tr.debugTransaction(deterministicRandom()->randomUniqueID());
			}

			Standalone<KeyValueRef> sampleKV = (*workload)(begin);
			if (!g_network->isSimulated() || !g_simulator->speedUpSimulation) {
				// Don't issue reads if speedUpSimulation is true. So this transaction
				// becomes blind writes, to avoid transaction_too_old errors.
				Optional<Value> f = co_await tr.get(sampleKV.key);
				if (f.present()) {
					//				if( sampleKV.value.size() == f.get().size() ) {
					//TraceEvent("BulkSetupRangeAlreadyPresent")
					//	.detail("Begin", begin)
					//	.detail("End", end)
					//	.detail("Version", tr.getReadVersion().get())
					//	.detail("Key", printable((*workload)(begin).key))
					//	.detailf("From", "%016llx", debug_lastLoadBalanceResultEndpointToken);
					co_return bytesInserted; // The transaction already completed!
					//}
					//TraceEvent(SevError, "BulkSetupConflict")
					//	.detail("Begin", begin)
					//	.detail("End", end)
					//	.detail("ExpectedLength", sampleKV.value.size())
					//	.detail("FoundLength", f.get().size());
					// throw operation_failed();
				}
			}
			// Predefine a single large write conflict range over the whole key space
			tr.addWriteConflictRange(KeyRangeRef(workload->keyForIndex(begin), keyAfter(workload->keyForIndex(end))));
			bytesInserted = 0;
			for (uint64_t n = begin; n < end; n++) {
				Standalone<KeyValueRef> kv = (*workload)(n);
				tr.set(kv.key, kv.value, AddConflictRange::False);
				bytesInserted += kv.key.size() + kv.value.size();
			}
			co_await tr.commit();
			co_return bytesInserted;
		} catch (Error& e) {
			err = e;
		}
		if (err.isValid()) {
			co_await tr.onError(err);
		}
	}
}

template <class T>
Future<uint64_t> setupRangeWorker(Database cx,
                                  T* workload,
                                  std::vector<std::pair<uint64_t, uint64_t>>* jobs,
                                  double maxKeyInsertRate,
                                  int keySaveIncrement,
                                  int actorId) {
	double nextStart;
	uint64_t loadedRanges = 0;
	int lastStoredKeysLoaded = 0;
	uint64_t keysLoaded = 0;
	uint64_t bytesStored = 0;
	while (jobs->size()) {
		std::pair<uint64_t, uint64_t> job = jobs->back();
		jobs->pop_back();
		nextStart = now() + (job.second - job.first) / maxKeyInsertRate;
		uint64_t numBytes = co_await setupRange(cx, workload, job.first, job.second);
		if (numBytes > 0) {
			loadedRanges++;
		}

		if (keySaveIncrement > 0) {
			keysLoaded += job.second - job.first;
			bytesStored += numBytes;

			if (keysLoaded - lastStoredKeysLoaded >= keySaveIncrement || jobs->size() == 0) {
				Transaction tr(cx);
				Error err;
				setAuthToken(*workload, tr);
				try {
					std::string countKey = format("keycount|%d|%d", workload->clientId, actorId);
					std::string bytesKey = format("bytesstored|%d|%d", workload->clientId, actorId);

					tr.set(StringRef(countKey), StringRef((uint8_t*)&keysLoaded, sizeof(uint64_t)));
					tr.set(StringRef(bytesKey), StringRef((uint8_t*)&bytesStored, sizeof(uint64_t)));

					co_await tr.commit();
					lastStoredKeysLoaded = keysLoaded;
				} catch (Error& e) {
					err = e;
				}
				if (err.isValid()) {
					co_await tr.onError(err);
				}
			}
		}

		if (now() < nextStart) {
			co_await delayUntil(nextStart);
		}
	}
	co_return loadedRanges;
}

// Periodically determines how many keys have been inserted.  If the count has just exceeded a count of interest,
// computes the time taken to reach that mark.  Returns a vector of times (in seconds) corresponding to the counts in
// the countsOfInterest vector.

// Expects countsOfInterest to be sorted in ascending order
Future<std::vector<std::pair<uint64_t, double>>> trackInsertionCount(Database cx,
                                                                     std::vector<uint64_t> countsOfInterest,
                                                                     double checkInterval);

template <class T>
Future<Void> waitForLowInFlight(Database cx, T* workload) {
	Future<Void> timeout = delay(600.0);
	while (true) {
		bool shouldDelay = false;
		try {
			if (timeout.isReady()) {
				throw timed_out();
			}

			int64_t inFlight = co_await getDataInFlight(cx, workload->dbInfo);
			TraceEvent("DynamicWarming").detail("InFlight", inFlight);
			if (inFlight > 1e6) { // Wait for just 1 MB to be in flight
				co_await delay(1.0);
			} else {
				co_await delay(1.0);
				TraceEvent("DynamicWarmingDone").log();
				break;
			}
		} catch (Error& e) {
			if (e.code() == error_code_attribute_not_found ||
			    (e.code() == error_code_timed_out && !timeout.isReady())) {
				// DD may not be initialized yet and attribute "DataInFlight" can be missing
				shouldDelay = true;
			} else {
				TraceEvent(SevWarn, "WaitForLowInFlightError").error(e);
				throw;
			}
		}
		if (shouldDelay) {
			co_await delay(1.0);
		}
	}
}

template <class T>
Future<Void> bulkSetup(Database cx,
                       T* workload,
                       uint64_t nodeCount,
                       Promise<double> setupTime,
                       bool valuesInconsequential = false,
                       double postSetupWarming = 0.0,
                       double maxKeyInsertRate = 1e12,
                       std::vector<uint64_t> insertionCountsToMeasure = std::vector<uint64_t>(),
                       Promise<std::vector<std::pair<uint64_t, double>>> ratesAtKeyCounts =
                           Promise<std::vector<std::pair<uint64_t, double>>>(),
                       int keySaveIncrement = 0,
                       double keyCheckInterval = 0.1,
                       uint64_t startNodeIdx = 0,
                       uint64_t endNodeIdx = 0) {
	std::vector<std::pair<uint64_t, uint64_t>> jobs;
	uint64_t startNode = startNodeIdx ? startNodeIdx : (nodeCount * workload->clientId) / workload->clientCount;
	uint64_t endNode = endNodeIdx ? endNodeIdx : (nodeCount * (workload->clientId + 1)) / workload->clientCount;

	double start = now();

	TraceEvent("BulkSetupStart")
	    .detail("NodeCount", nodeCount)
	    .detail("ValuesInconsequential", valuesInconsequential)
	    .detail("PostSetupWarming", postSetupWarming)
	    .detail("MaxKeyInsertRate", maxKeyInsertRate);

	// For bulk data schemes where the value of the key is not critical to operation, check to
	//  see if the database has already been set up.
	if (valuesInconsequential) {
		bool present = co_await checkRangeSimpleValueSize(cx, workload, startNode, endNode);
		if (present) {
			TraceEvent("BulkSetupRangeAlreadyPresent")
			    .detail("Begin", startNode)
			    .detail("End", endNode)
			    .detail("CheckMethod", "SimpleValueSize");
			setupTime.send(0.0);
			ratesAtKeyCounts.send(std::vector<std::pair<uint64_t, double>>());
			co_return;
		} else {
			TraceEvent("BulkRangeNotFound")
			    .detail("Begin", startNode)
			    .detail("End", endNode)
			    .detail("CheckMethod", "SimpleValueSize");
		}
	}

	co_await delay(deterministicRandom()->random01() / 4); // smear over .25 seconds

	int BULK_SETUP_WORKERS = 40;
	// See that each chunk inserted is about 10KB
	int size_total = 0;
	for (int i = 0; i < 100; i++) {
		Standalone<KeyValueRef> sampleKV =
		    (*workload)(startNode + (uint64_t)(deterministicRandom()->random01() * (endNode - startNode)));
		size_total += sampleKV.key.size() + sampleKV.value.size();
	}
	int BULK_SETUP_RANGE_SIZE = size_total == 0 ? 50 : std::max(1, 10000 / (size_total / 100));

	TraceEvent((workload->description() + "SetupStart").c_str())
	    .detail("ClientIdx", workload->clientId)
	    .detail("ClientCount", workload->clientCount)
	    .detail("StartingNode", startNode)
	    .detail("NodesAssigned", endNode - startNode)
	    .detail("NodesPerInsertion", BULK_SETUP_RANGE_SIZE)
	    .detail("SetupActors", BULK_SETUP_WORKERS);

	// create a random vector of range-create jobs
	for (uint64_t n = startNode; n < endNode; n += BULK_SETUP_RANGE_SIZE) {
		jobs.emplace_back(n, std::min(endNode, n + BULK_SETUP_RANGE_SIZE));
	}
	deterministicRandom()->randomShuffle(jobs);

	// fire up the workers and wait for them to eat all the jobs
	double maxWorkerInsertRate = maxKeyInsertRate / BULK_SETUP_WORKERS / workload->clientCount;
	std::vector<Future<uint64_t>> fs;

	Future<std::vector<std::pair<uint64_t, double>>> insertionTimes = std::vector<std::pair<uint64_t, double>>();

	if (insertionCountsToMeasure.size() > 0) {
		std::sort(insertionCountsToMeasure.begin(), insertionCountsToMeasure.end());
		for (int i = 0; i < insertionCountsToMeasure.size(); i++) {
			if (insertionCountsToMeasure[i] > nodeCount) {
				insertionCountsToMeasure.erase(insertionCountsToMeasure.begin() + i, insertionCountsToMeasure.end());
			}
		}

		if (workload->clientId == 0) {
			insertionTimes = trackInsertionCount(cx, insertionCountsToMeasure, keyCheckInterval);
		}

		if (keySaveIncrement <= 0) {
			keySaveIncrement = BULK_SETUP_RANGE_SIZE;
		}
	} else {
		keySaveIncrement = 0;
	}

	for (int j = 0; j < BULK_SETUP_WORKERS; j++) {
		fs.push_back(setupRangeWorker(cx, workload, &jobs, maxWorkerInsertRate, keySaveIncrement, j));
	}
	try {
		co_await (success(insertionTimes) && waitForAll(fs));
	} catch (Error& e) {
		if (e.code() == error_code_operation_failed) {
			TraceEvent(SevError, "BulkSetupFailed").error(e);
		}
		throw;
	}

	ratesAtKeyCounts.send(insertionTimes.get());

	uint64_t rangesInserted = 0;
	for (int i = 0; i < fs.size(); i++) {
		rangesInserted += fs[i].get();
	}

	double elapsed = now() - start;
	setupTime.send(elapsed);
	TraceEvent("SetupLoadComplete")
	    .detail("LoadedRanges", rangesInserted)
	    .detail("Duration", elapsed)
	    .detail("Nodes", nodeCount);

	// Here we wait for data in flight to go to 0 (this will not work on a database with other users)
	if (postSetupWarming != 0) {
		bool shouldWarmDatabase = false;
		try {
			co_await delay(5.0);
			co_await waitForLowInFlight(cx, workload); // Wait for the data distribution in a small test to start
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			TraceEvent("DynamicWarmingError").error(e);
			shouldWarmDatabase = postSetupWarming > 0;
		}
		if (shouldWarmDatabase) {
			co_await timeout(databaseWarmer(cx), postSetupWarming, Void());
		}
	}

	TraceEvent((workload->description() + "SetupOK").c_str())
	    .detail("ClientIdx", workload->clientId)
	    .detail("WarmingDelay", postSetupWarming)
	    .detail("KeyLoadElapsedTime", elapsed);
}

#endif
