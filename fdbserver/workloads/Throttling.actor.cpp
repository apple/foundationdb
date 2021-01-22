/*
 * Throttling.actor.cpp
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

constexpr const double TokenBucket::addTokensInterval;
constexpr const double TokenBucket::maxSleepTime;

struct ThrottlingWorkload : KVWorkload {

	double testDuration;
	double healthMetricsCheckInterval;
	int actorsPerClient;
	int writesPerTransaction;
	int readsPerTransaction;
	double throttlingMultiplier;
	int transactionsCommitted;
	int64_t worstStorageQueue;
	int64_t worstStorageDurabilityLag;
	int64_t worstTLogQueue;
	int64_t detailedWorstStorageQueue;
	int64_t detailedWorstStorageDurabilityLag;
	int64_t detailedWorstTLogQueue;
	double detailedWorstCpuUsage;
	double detailedWorstDiskUsage;
	bool sendDetailedHealthMetrics;
	double maxAllowedStaleness;
	TokenBucket tokenBucket;
	bool healthMetricsStoppedUpdating;

	ThrottlingWorkload(WorkloadContext const& wcx)
	  : KVWorkload(wcx), transactionsCommitted(0), worstStorageQueue(0), worstStorageDurabilityLag(0), worstTLogQueue(0),
	    detailedWorstStorageQueue(0), detailedWorstStorageDurabilityLag(0), detailedWorstTLogQueue(0), detailedWorstCpuUsage(0.0),
	    detailedWorstDiskUsage(0.0), healthMetricsStoppedUpdating(false) {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 60.0);
		healthMetricsCheckInterval = getOption(options, LiteralStringRef("healthMetricsCheckInterval"), 1.0);
		actorsPerClient = getOption(options, LiteralStringRef("actorsPerClient"), 10);
		writesPerTransaction = getOption(options, LiteralStringRef("writesPerTransaction"), 10);
		readsPerTransaction = getOption(options, LiteralStringRef("readsPerTransaction"), 10);
		throttlingMultiplier = getOption(options, LiteralStringRef("throttlingMultiplier"), 0.5);
		sendDetailedHealthMetrics = getOption(options, LiteralStringRef("sendDetailedHealthMetrics"), true);
		maxAllowedStaleness = getOption(options, LiteralStringRef("maxAllowedStaleness"), 10.0);
		int maxBurst = getOption(options, LiteralStringRef("maxBurst"), 1000);
		tokenBucket.maxBurst = maxBurst;
	}

	ACTOR static Future<Void> healthMetricsChecker(Database cx, ThrottlingWorkload* self) {
		state int repeated = 0;
		state HealthMetrics healthMetrics;

		loop {
			wait(delay(self->healthMetricsCheckInterval));
			HealthMetrics newHealthMetrics = wait(cx->getHealthMetrics(self->sendDetailedHealthMetrics));
			if (healthMetrics == newHealthMetrics)
			{
				if (++repeated > self->maxAllowedStaleness / self->healthMetricsCheckInterval)
					self->healthMetricsStoppedUpdating = true;
			}
			else
				repeated = 0;
			healthMetrics = newHealthMetrics;

			self->tokenBucket.transactionRate = healthMetrics.tpsLimit * self->throttlingMultiplier / self->clientCount;
			self->worstStorageQueue = std::max(self->worstStorageQueue, healthMetrics.worstStorageQueue);
			self->worstStorageDurabilityLag = std::max(self->worstStorageDurabilityLag, healthMetrics.worstStorageDurabilityLag);
			self->worstTLogQueue = std::max(self->worstTLogQueue, healthMetrics.worstTLogQueue);

			TraceEvent("HealthMetrics")
			    .detail("WorstStorageQueue", healthMetrics.worstStorageQueue)
			    .detail("WorstStorageDurabilityLag", healthMetrics.worstStorageDurabilityLag)
			    .detail("WorstTLogQueue", healthMetrics.worstTLogQueue)
			    .detail("TpsLimit", healthMetrics.tpsLimit);

			TraceEvent traceStorageQueue("StorageQueue");
			TraceEvent traceStorageDurabilityLag("StorageDurabilityLag");
			TraceEvent traceCpuUsage("CpuUsage");
			TraceEvent traceDiskUsage("DiskUsage");
			for (const auto& ss : healthMetrics.storageStats) {
				auto storageStats = ss.second;
				self->detailedWorstStorageQueue = std::max(self->detailedWorstStorageQueue, storageStats.storageQueue);
				traceStorageQueue.detail(format("Storage/%s", ss.first.toString().c_str()), storageStats.storageQueue);
				self->detailedWorstStorageDurabilityLag = std::max(self->detailedWorstStorageDurabilityLag, storageStats.storageDurabilityLag);
				traceStorageDurabilityLag.detail(format("Storage/%s", ss.first.toString().c_str()), storageStats.storageDurabilityLag);
				self->detailedWorstCpuUsage = std::max(self->detailedWorstCpuUsage, storageStats.cpuUsage);
				traceCpuUsage.detail(format("Storage/%s", ss.first.toString().c_str()), storageStats.cpuUsage);
				self->detailedWorstDiskUsage = std::max(self->detailedWorstDiskUsage, storageStats.diskUsage);
				traceDiskUsage.detail(format("Storage/%s", ss.first.toString().c_str()), storageStats.diskUsage);
			}

			TraceEvent traceTLogQueue("TLogQueue");
			for (const auto& ss : healthMetrics.tLogQueue) {
				self->detailedWorstTLogQueue = std::max(self->detailedWorstTLogQueue, ss.second);
				traceTLogQueue.detail(format("TLog/%s", ss.first.toString().c_str()), ss.second);
			}
		}
	}

	static Value getRandomValue() { return Standalone<StringRef>(format("Value/%d", deterministicRandom()->randomInt(0, 10e6))); }

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
				if (deterministicRandom()->randomInt(0, 1000) == 0) TraceEvent("TransactionCommittedx1000");
				++self->transactionsCommitted;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) throw;
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
				Standalone<RangeResultRef> result = wait(
				    tr.getRange(prefixRange(LiteralStringRef("\xff\xff/metrics/health/")), CLIENT_KNOBS->TOO_MANY));
				ASSERT(!result.more);
				for (const auto& [k, v] : result) {
					ASSERT(k.startsWith(LiteralStringRef("\xff\xff/metrics/health/")));
					auto valueObj = readJSONStrictly(v.toString()).get_obj();
					if (k.removePrefix(LiteralStringRef("\xff\xff/metrics/health/")) == LiteralStringRef("aggregate")) {
						TEST(true); // Test aggregate health metrics schema
						std::string errorStr;
						if (!schemaMatch(aggregateSchema, valueObj, errorStr, SevError, true))
							TraceEvent(SevError, "AggregateHealthSchemaValidationFailed")
							    .detail("ErrorStr", errorStr.c_str())
							    .detail("JSON", json_spirit::write_string(json_spirit::mValue(v.toString())));
					} else if (k.removePrefix(LiteralStringRef("\xff\xff/metrics/health/"))
					               .startsWith(LiteralStringRef("storage/"))) {
						TEST(true); // Test storage health metrics schema
						UID::fromString(k.removePrefix(LiteralStringRef("\xff\xff/metrics/health/storage/"))
						                    .toString()); // Will throw if it's not a valid uid
						std::string errorStr;
						if (!schemaMatch(storageSchema, valueObj, errorStr, SevError, true))
							TraceEvent(SevError, "StorageHealthSchemaValidationFailed")
							    .detail("ErrorStr", errorStr.c_str())
							    .detail("JSON", json_spirit::write_string(json_spirit::mValue(v.toString())));
					} else if (k.removePrefix(LiteralStringRef("\xff\xff/metrics/health/"))
					               .startsWith(LiteralStringRef("log/"))) {
						TEST(true); // Test log health metrics schema
						UID::fromString(k.removePrefix(LiteralStringRef("\xff\xff/metrics/health/log/"))
						                    .toString()); // Will throw if it's not a valid uid
						std::string errorStr;
						if (!schemaMatch(logSchema, valueObj, errorStr, SevError, true))
							TraceEvent(SevError, "LogHealthSchemaValidationFailed")
							    .detail("ErrorStr", errorStr.c_str())
							    .detail("JSON", json_spirit::write_string(json_spirit::mValue(v.toString())));
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

	ACTOR static Future<Void> _setup(Database cx, ThrottlingWorkload* self) {
		if (!self->sendDetailedHealthMetrics) {
			// Clear detailed health metrics that are already populated
			wait(delay(2 * CLIENT_KNOBS->DETAILED_HEALTH_METRICS_MAX_STALENESS));
			cx->healthMetrics.storageStats.clear();
			cx->healthMetrics.tLogQueue.clear();
		}
		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, ThrottlingWorkload* self) {
		state Future<Void> hmChecker = timeout(healthMetricsChecker(cx, self), self->testDuration, Void());

		state vector<Future<Void>> clientActors;
		state int actorId;
		for (actorId = 0; actorId < self->actorsPerClient; ++actorId) {
			clientActors.push_back(timeout(clientActor(cx, self), self->testDuration, Void()));
		}
		clientActors.push_back(timeout(specialKeysActor(cx, self), self->testDuration, Void()));
		wait(hmChecker);
		return Void();
	}

	virtual std::string description() { return "Throttling"; }
	virtual Future<Void> setup(Database const& cx) { return _setup(cx, this); }
	virtual Future<Void> start(Database const& cx) { return _start(cx, this); }
	virtual Future<bool> check(Database const& cx) {
		if (healthMetricsStoppedUpdating) {
			TraceEvent(SevError, "HealthMetricsStoppedUpdating");
			return false;
		}
		if (transactionsCommitted == 0) {
			TraceEvent(SevError, "NoTransactionsCommitted");
			return false;
		}
		bool correctHealthMetricsState = true;
		if (worstStorageQueue == 0 || worstStorageDurabilityLag == 0 || worstTLogQueue == 0 || transactionsCommitted == 0)
			correctHealthMetricsState = false;
		if (sendDetailedHealthMetrics) {
			if (detailedWorstStorageQueue == 0 || detailedWorstStorageDurabilityLag == 0 || detailedWorstTLogQueue == 0 ||
			    detailedWorstCpuUsage == 0.0 || detailedWorstDiskUsage == 0.0)
				correctHealthMetricsState = false;
		} else {
			if (detailedWorstStorageQueue != 0 || detailedWorstStorageDurabilityLag != 0 || detailedWorstTLogQueue != 0 ||
			    detailedWorstCpuUsage != 0.0 || detailedWorstDiskUsage != 0.0)
				correctHealthMetricsState = false;
		}
		if (!correctHealthMetricsState) {
			TraceEvent(SevError, "IncorrectHealthMetricsState")
				.detail("WorstStorageQueue", worstStorageQueue)
				.detail("WorstStorageDurabilityLag", worstStorageDurabilityLag)
				.detail("WorstTLogQueue", worstTLogQueue)
				.detail("DetailedWorstStorageQueue", detailedWorstStorageQueue)
				.detail("DetailedWorstStorageDurabilityLag", detailedWorstStorageDurabilityLag)
				.detail("DetailedWorstTLogQueue", detailedWorstTLogQueue)
				.detail("DetailedWorstCpuUsage", detailedWorstCpuUsage)
				.detail("DetailedWorstDiskUsage", detailedWorstDiskUsage)
				.detail("SendingDetailedHealthMetrics", sendDetailedHealthMetrics);
		}
		return correctHealthMetricsState;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
		m.push_back(PerfMetric("TransactionsCommitted", transactionsCommitted, false));
		m.push_back(PerfMetric("WorstStorageQueue", worstStorageQueue, true));
		m.push_back(PerfMetric("DetailedWorstStorageQueue", detailedWorstStorageQueue, true));
		m.push_back(PerfMetric("WorstStorageDurabilityLag", worstStorageDurabilityLag, true));
		m.push_back(PerfMetric("DetailedWorstStorageDurabilityLag", detailedWorstStorageDurabilityLag, true));
		m.push_back(PerfMetric("WorstTLogQueue", worstTLogQueue, true));
		m.push_back(PerfMetric("DetailedWorstTLogQueue", detailedWorstTLogQueue, true));
		m.push_back(PerfMetric("DetailedWorstCpuUsage", detailedWorstCpuUsage, true));
		m.push_back(PerfMetric("DetailedWorstDiskUsage", detailedWorstDiskUsage, true));
	}
};

WorkloadFactory<ThrottlingWorkload> ThrottlingWorkloadFactory("Throttling");
