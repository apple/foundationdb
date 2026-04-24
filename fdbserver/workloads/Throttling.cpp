/*
 * Throttling.cpp
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

#include <boost/lexical_cast.hpp>

#include "fdbclient/ManagementAPI.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/Schemas.h"
#include "fdbserver/tester/workloads.h"

struct TokenBucket {
	static constexpr const double addTokensInterval = 0.1;
	static constexpr const double maxSleepTime = 60.0;

	double transactionRate;
	double maxBurst;
	double bucketSize;
	Future<Void> tokenAdderActor;

	Future<Void> tokenAdder() {
		while (true) {
			bucketSize = std::min(bucketSize + transactionRate * addTokensInterval, maxBurst);
			if (deterministicRandom()->randomInt(0, 100) == 0)
				TraceEvent("AddingTokensx100")
				    .detail("BucketSize", bucketSize)
				    .detail("TransactionRate", transactionRate);
			co_await delay(addTokensInterval);
		}
	}

	explicit TokenBucket(double maxBurst = 1000) : transactionRate(0), maxBurst(maxBurst), bucketSize(maxBurst) {
		tokenAdderActor = tokenAdder();
	}

	Future<Void> startTransaction() {
		double sleepTime = addTokensInterval;
		while (true) {
			if (bucketSize >= 1.0) {
				--bucketSize;
				co_return;
			}
			if (deterministicRandom()->randomInt(0, 100) == 0)
				TraceEvent("ThrottlingTransactionx100").detail("SleepTime", sleepTime);
			co_await delay(sleepTime);
			sleepTime = std::min(sleepTime * 2, maxSleepTime);
		}
	}
};

// workload description:
// The Throttling workload runs a simple random read-write workload while throttling using a token bucket algorithm
// using the TPS limit obtained from the health metrics namespace. It periodically reads health metrics from the special
// key and tests whether or not the received health metrics are reasonable.
struct ThrottlingWorkload : KVWorkload {

	double testDuration;
	int actorsPerClient;
	int writesPerTransaction;
	int readsPerTransaction;
	double throttlingMultiplier;
	int transactionsCommitted;
	TokenBucket tokenBucket;
	bool correctSpecialKeys = true;

	static constexpr auto NAME = "Throttling";

	explicit ThrottlingWorkload(WorkloadContext const& wcx) : KVWorkload(wcx), transactionsCommitted(0) {
		testDuration = getOption(options, "testDuration"_sr, 60.0);
		actorsPerClient = getOption(options, "actorsPerClient"_sr, 10);
		writesPerTransaction = getOption(options, "writesPerTransaction"_sr, 10);
		readsPerTransaction = getOption(options, "readsPerTransaction"_sr, 10);
		throttlingMultiplier = getOption(options, "throttlingMultiplier"_sr, 0.5);
		int maxBurst = getOption(options, "maxBurst"_sr, 1000);
		tokenBucket.maxBurst = maxBurst;
	}

	static Value getRandomValue() {
		return Standalone<StringRef>(format("Value/%d", deterministicRandom()->randomInt(0, 10e6)));
	}

	Future<Void> clientActor(Database cx) {
		ReadYourWritesTransaction tr(cx);

		while (true) {
			co_await tokenBucket.startTransaction();
			tr.reset();
			try {
				for (int i = 0; i < readsPerTransaction; ++i) {
					Optional<Value> value = co_await tr.get(getRandomKey());
				}
				for (int i = 0; i < writesPerTransaction; ++i) {
					tr.set(getRandomKey(), getRandomValue());
				}
				co_await tr.commit();
				if (deterministicRandom()->randomInt(0, 1000) == 0)
					TraceEvent("TransactionCommittedx1000").log();
				++transactionsCommitted;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;
				// ignore failing transactions
			}
		}
	}

	Future<Void> specialKeysActor(Database cx) {
		ReadYourWritesTransaction tr(cx);
		json_spirit::mValue aggregateSchema = readJSONStrictly(JSONSchemas::aggregateHealthSchema.toString()).get_obj();
		json_spirit::mValue storageSchema = readJSONStrictly(JSONSchemas::storageHealthSchema.toString()).get_obj();
		json_spirit::mValue logSchema = readJSONStrictly(JSONSchemas::logHealthSchema.toString()).get_obj();
		while (true) {
			Error err;
			try {
				RangeResult result =
				    co_await tr.getRange(prefixRange("\xff\xff/metrics/health/"_sr), CLIENT_KNOBS->TOO_MANY);
				ASSERT(!result.more);
				for (const auto& [k, v] : result) {
					ASSERT(k.startsWith("\xff\xff/metrics/health/"_sr));
					auto valueObj = readJSONStrictly(v.toString()).get_obj();
					if (k.removePrefix("\xff\xff/metrics/health/"_sr) == "aggregate"_sr) {
						CODE_PROBE(true, "Test aggregate health metrics schema");
						std::string errorStr;
						if (!schemaMatch(aggregateSchema, valueObj, errorStr, SevError, true)) {
							TraceEvent(SevError, "AggregateHealthSchemaValidationFailed")
							    .detail("ErrorStr", errorStr.c_str())
							    .detail("JSON", json_spirit::write_string(json_spirit::mValue(v.toString())));
							correctSpecialKeys = false;
						}
						auto tpsLimit = valueObj.at("tps_limit").get_real();
						tokenBucket.transactionRate = tpsLimit * throttlingMultiplier / clientCount;
					} else if (k.removePrefix("\xff\xff/metrics/health/"_sr).startsWith("storage/"_sr)) {
						CODE_PROBE(true, "Test storage health metrics schema");
						UID::fromString(k.removePrefix("\xff\xff/metrics/health/storage/"_sr)
						                    .toString()); // Will throw if it's not a valid uid
						std::string errorStr;
						if (!schemaMatch(storageSchema, valueObj, errorStr, SevError, true)) {
							TraceEvent(SevError, "StorageHealthSchemaValidationFailed")
							    .detail("ErrorStr", errorStr.c_str())
							    .detail("JSON", json_spirit::write_string(json_spirit::mValue(v.toString())));
							correctSpecialKeys = false;
						}
					} else if (k.removePrefix("\xff\xff/metrics/health/"_sr).startsWith("log/"_sr)) {
						CODE_PROBE(true, "Test log health metrics schema");
						UID::fromString(k.removePrefix("\xff\xff/metrics/health/log/"_sr)
						                    .toString()); // Will throw if it's not a valid uid
						std::string errorStr;
						if (!schemaMatch(logSchema, valueObj, errorStr, SevError, true)) {
							TraceEvent(SevError, "LogHealthSchemaValidationFailed")
							    .detail("ErrorStr", errorStr.c_str())
							    .detail("JSON", json_spirit::write_string(json_spirit::mValue(v.toString())));
							correctSpecialKeys = false;
						}
					} else {
						ASSERT(false); // Unrecognized key
					}
				}
				co_await delayJittered(5);
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				co_await tr.onError(err);
			}
		}
	}

	Future<Void> start(Database const& cx) override {
		std::vector<Future<Void>> clientActors;
		for (int actorId = 0; actorId < actorsPerClient; ++actorId) {
			clientActors.push_back(timeout(clientActor(cx), testDuration, Void()));
		}
		clientActors.push_back(timeout(specialKeysActor(cx), testDuration, Void()));
		clientActors.push_back(timeout(tokenBucket.tokenAdderActor, testDuration, Void()));
		co_await delay(testDuration);
	}

	Future<bool> check(Database const& cx) override { return correctSpecialKeys; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("TransactionsCommitted", transactionsCommitted, Averaged::False);
	}
};

WorkloadFactory<ThrottlingWorkload> ThrottlingWorkloadFactory;
