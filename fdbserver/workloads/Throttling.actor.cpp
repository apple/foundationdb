/*
 * Throttling.actor.cpp
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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/Schemas.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last include

struct TokenBucket {
	static constexpr const double addTokensInterval = 0.1;
	static constexpr const double maxSleepTime = 60.0;

	double transactionRate;
	double maxBurst;
	double bucketSize;
	Future<Void> tokenAdderActor;

	ACTOR static Future<Void> tokenAdder(TokenBucket* self) {
		loop {
			self->bucketSize = std::min(self->bucketSize + self->transactionRate * addTokensInterval, self->maxBurst);
			if (deterministicRandom()->randomInt(0, 100) == 0)
				TraceEvent("AddingTokensx100")
				    .detail("BucketSize", self->bucketSize)
				    .detail("TransactionRate", self->transactionRate);
			wait(delay(addTokensInterval));
		}
	}

	TokenBucket(double maxBurst = 1000) : transactionRate(0), maxBurst(maxBurst), bucketSize(maxBurst) {
		tokenAdderActor = tokenAdder(this);
	}

	ACTOR static Future<Void> startTransaction(TokenBucket* self) {
		state double sleepTime = addTokensInterval;
		loop {
			if (self->bucketSize >= 1.0) {
				--self->bucketSize;
				return Void();
			}
			if (deterministicRandom()->randomInt(0, 100) == 0)
				TraceEvent("ThrottlingTransactionx100").detail("SleepTime", sleepTime);
			wait(delay(sleepTime));
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

	static constexpr const char* NAME = "Throttling";

	ThrottlingWorkload(WorkloadContext const& wcx) : KVWorkload(wcx), transactionsCommitted(0) {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 60.0);
		actorsPerClient = getOption(options, LiteralStringRef("actorsPerClient"), 10);
		writesPerTransaction = getOption(options, LiteralStringRef("writesPerTransaction"), 10);
		readsPerTransaction = getOption(options, LiteralStringRef("readsPerTransaction"), 10);
		throttlingMultiplier = getOption(options, LiteralStringRef("throttlingMultiplier"), 0.5);
		int maxBurst = getOption(options, LiteralStringRef("maxBurst"), 1000);
		tokenBucket.maxBurst = maxBurst;
	}

	static Value getRandomValue() {
		return Standalone<StringRef>(format("Value/%d", deterministicRandom()->randomInt(0, 10e6)));
	}

	ACTOR static Future<Void> clientActor(Database cx, ThrottlingWorkload* self) {
		state ReadYourWritesTransaction tr(cx);

		loop {
			wait(TokenBucket::startTransaction(&self->tokenBucket));
			tr.reset();
			try {
				state int i;
				for (i = 0; i < self->readsPerTransaction; ++i) {
					state Optional<Value> value = wait(tr.get(self->getRandomKey()));
				}
				for (i = 0; i < self->writesPerTransaction; ++i) {
					tr.set(self->getRandomKey(), getRandomValue());
				}
				wait(tr.commit());
				if (deterministicRandom()->randomInt(0, 1000) == 0)
					TraceEvent("TransactionCommittedx1000").log();
				++self->transactionsCommitted;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;
				// ignore failing transactions
			}
		}
	}

	ACTOR static Future<Void> specialKeysActor(Database cx, ThrottlingWorkload* self) {
		state ReadYourWritesTransaction tr(cx);
		state json_spirit::mValue aggregateSchema =
		    readJSONStrictly(JSONSchemas::aggregateHealthSchema.toString()).get_obj();
		state json_spirit::mValue storageSchema =
		    readJSONStrictly(JSONSchemas::storageHealthSchema.toString()).get_obj();
		state json_spirit::mValue logSchema = readJSONStrictly(JSONSchemas::logHealthSchema.toString()).get_obj();
		loop {
			try {
				RangeResult result = wait(
				    tr.getRange(prefixRange(LiteralStringRef("\xff\xff/metrics/health/")), CLIENT_KNOBS->TOO_MANY));
				ASSERT(!result.more);
				for (const auto& [k, v] : result) {
					ASSERT(k.startsWith(LiteralStringRef("\xff\xff/metrics/health/")));
					auto valueObj = readJSONStrictly(v.toString()).get_obj();
					if (k.removePrefix(LiteralStringRef("\xff\xff/metrics/health/")) == LiteralStringRef("aggregate")) {
						TEST(true); // Test aggregate health metrics schema
						std::string errorStr;
						if (!schemaMatch(aggregateSchema, valueObj, errorStr, SevError, true)) {
							TraceEvent(SevError, "AggregateHealthSchemaValidationFailed")
							    .detail("ErrorStr", errorStr.c_str())
							    .detail("JSON", json_spirit::write_string(json_spirit::mValue(v.toString())));
							self->correctSpecialKeys = false;
						}
						auto tpsLimit = valueObj.at("tps_limit").get_real();
						self->tokenBucket.transactionRate = tpsLimit * self->throttlingMultiplier / self->clientCount;
					} else if (k.removePrefix(LiteralStringRef("\xff\xff/metrics/health/"))
					               .startsWith(LiteralStringRef("storage/"))) {
						TEST(true); // Test storage health metrics schema
						UID::fromString(k.removePrefix(LiteralStringRef("\xff\xff/metrics/health/storage/"))
						                    .toString()); // Will throw if it's not a valid uid
						std::string errorStr;
						if (!schemaMatch(storageSchema, valueObj, errorStr, SevError, true)) {
							TraceEvent(SevError, "StorageHealthSchemaValidationFailed")
							    .detail("ErrorStr", errorStr.c_str())
							    .detail("JSON", json_spirit::write_string(json_spirit::mValue(v.toString())));
							self->correctSpecialKeys = false;
						}
					} else if (k.removePrefix(LiteralStringRef("\xff\xff/metrics/health/"))
					               .startsWith(LiteralStringRef("log/"))) {
						TEST(true); // Test log health metrics schema
						UID::fromString(k.removePrefix(LiteralStringRef("\xff\xff/metrics/health/log/"))
						                    .toString()); // Will throw if it's not a valid uid
						std::string errorStr;
						if (!schemaMatch(logSchema, valueObj, errorStr, SevError, true)) {
							TraceEvent(SevError, "LogHealthSchemaValidationFailed")
							    .detail("ErrorStr", errorStr.c_str())
							    .detail("JSON", json_spirit::write_string(json_spirit::mValue(v.toString())));
							self->correctSpecialKeys = false;
						}
					} else {
						ASSERT(false); // Unrecognized key
					}
				}
				wait(delayJittered(5));
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> _start(Database cx, ThrottlingWorkload* self) {
		state std::vector<Future<Void>> clientActors;
		state int actorId;
		for (actorId = 0; actorId < self->actorsPerClient; ++actorId) {
			clientActors.push_back(timeout(clientActor(cx, self), self->testDuration, Void()));
		}
		clientActors.push_back(timeout(specialKeysActor(cx, self), self->testDuration, Void()));
		clientActors.push_back(timeout(self->tokenBucket.tokenAdderActor, self->testDuration, Void()));
		wait(delay(self->testDuration));
		return Void();
	}

	std::string description() const override { return ThrottlingWorkload::NAME; }
	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	Future<bool> check(Database const& cx) override { return correctSpecialKeys; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("TransactionsCommitted", transactionsCommitted, Averaged::False);
	}
};

WorkloadFactory<ThrottlingWorkload> ThrottlingWorkloadFactory(ThrottlingWorkload::NAME);
