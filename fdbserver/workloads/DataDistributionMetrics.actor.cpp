/*
 * DataDistributionMetrics.actor.cpp
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

struct DataDistributionMetricsWorkload : KVWorkload {
	static constexpr auto NAME = "DataDistributionMetrics";

	int numShards, readPerTx, writePerTx;
	int64_t avgBytes, transactionTimeLimit;
	double testDuration;
	std::string keyPrefix;
	PerfIntCounter commits, errors;
	double delayPerLoop;

	DataDistributionMetricsWorkload(WorkloadContext const& wcx)
	  : KVWorkload(wcx), numShards(0), avgBytes(0), commits("Commits"), errors("Errors") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		// transaction time out duration(ms)
		transactionTimeLimit = getOption(options, "transactionTimeLimit"_sr, 10000);
		keyPrefix = getOption(options, "keyPrefix"_sr, "DDMetrics"_sr).toString();
		readPerTx = getOption(options, "readPerTransaction"_sr, 1);
		writePerTx = getOption(options, "writePerTransaction"_sr, 5 * readPerTx);
		delayPerLoop = getOption(options, "delayPerLoop"_sr, 0.1); // throttling dd rpc calls
		ASSERT(nodeCount > 1);
	}

	static Value getRandomValue() {
		return Standalone<StringRef>(format("Value/%08d", deterministicRandom()->randomInt(0, 10e6)));
	}

	Key keyForIndex(int n) { return doubleToTestKey((double)n / nodeCount, keyPrefix); }

	ACTOR static Future<Void> ddRWClient(Database cx, DataDistributionMetricsWorkload* self) {
		loop {
			state ReadYourWritesTransaction tr(cx);
			state int i;
			try {
				for (i = 0; i < self->readPerTx; ++i)
					wait(success(
					    tr.get(self->keyForIndex(deterministicRandom()->randomInt(0, self->nodeCount))))); // read
				for (i = 0; i < self->writePerTx; ++i)
					tr.set(self->keyForIndex(deterministicRandom()->randomInt(0, self->nodeCount)),
					       getRandomValue()); // write
				wait(tr.commit());
				++self->commits;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> resultConsistencyCheckClient(Database cx, DataDistributionMetricsWorkload* self) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			tr->setOption(FDBTransactionOptions::TIMEOUT,
			              StringRef((uint8_t*)&self->transactionTimeLimit, sizeof(int64_t)));
			try {
				wait(delay(self->delayPerLoop));
				int startIndex = deterministicRandom()->randomInt(0, self->nodeCount - 1);
				int endIndex = deterministicRandom()->randomInt(startIndex + 1, self->nodeCount);
				state Key startKey = self->keyForIndex(startIndex);
				state Key endKey = self->keyForIndex(endIndex);
				// Find the last key <= startKey and use as the begin of the range. Since "Key()" is always the starting
				// point, this key selector will never do cross_module_range_read. In addition, the first key in the
				// result will be the last one <= startKey (Condition #1)
				state KeySelector begin =
				    KeySelectorRef(startKey.withPrefix(ddStatsRange.begin, startKey.arena()), true, 0);
				// Find the last key less than endKey, move forward 2 keys, and use this key as the (exclusive) end of
				// the range. If we didn't read through the end of the range, then the second last key
				// in the result will be the last key less than endKey. (Condition #2)
				state KeySelector end = KeySelectorRef(endKey.withPrefix(ddStatsRange.begin, endKey.arena()), false, 2);
				RangeResult result = wait(tr->getRange(begin, end, GetRangeLimits(CLIENT_KNOBS->TOO_MANY)));
				// Condition #1 and #2 can be broken if multiple rpc calls happened in one getRange
				if (result.size() > 1) {
					if (result[0].key > begin.getKey() || result[1].key <= begin.getKey()) {
						++self->errors;
						TraceEvent(SevError, "TestFailure")
						    .detail("Reason", "Result mismatches the given begin selector")
						    .detail("Size", result.size())
						    .detail("FirstKey", result[0].key)
						    .detail("SecondKey", result[1].key)
						    .detail("BeginKeySelector", begin);
					}
					if (!result.readThroughEnd && (result[result.size() - 1].key < end.getKey() ||
					                               result[result.size() - 2].key >= end.getKey())) {
						++self->errors;
						TraceEvent(SevError, "TestFailure")
						    .detail("Reason", "Result mismatches the given end selector")
						    .detail("Size", result.size())
						    .detail("LastKey", result[result.size() - 1].key)
						    .detail("SecondLastKey", result[result.size() - 2].key)
						    .detail("EndKeySelector", end);
					}
					// Debugging traces
					// TraceEvent(SevDebug, "DDMetricsConsistencyTest")
					// 	    .detail("Size", result.size())
					// 	    .detail("FirstKey", result[0].key)
					// 	    .detail("SecondKey", result[1].key)
					// 	    .detail("BeginKeySelector", begin);
					// TraceEvent(SevDebug, "DDMetricsConsistencyTest")
					// 	    .detail("Size", result.size())
					// 	    .detail("LastKey", result[result.size() - 1].key)
					// 	    .detail("SecondLastKey", result[result.size() - 2].key)
					// 	    .detail("EndKeySelector", end);
				}
			} catch (Error& e) {
				// Ignore timed_out error and cross_module_read, the end key selector may read through the end
				if (e.code() == error_code_timed_out || e.code() == error_code_transaction_timed_out) {
					tr->reset();
					continue;
				}
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<bool> _check(Database cx, DataDistributionMetricsWorkload* self) {
		if (self->errors.getValue() > 0) {
			TraceEvent(SevError, "TestFailure").detail("Reason", "GetRange Results Inconsistent");
			return false;
		}
		// TODO : find why this not work
		// wait(quietDatabase(cx, self->dbInfo, "PopulateTPCC"));
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		state int i;
		state int retries = 0;
		loop {
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			tr->setOption(FDBTransactionOptions::TIMEOUT,
			              StringRef((uint8_t*)&self->transactionTimeLimit, sizeof(int64_t)));
			try {
				state RangeResult result = wait(tr->getRange(ddStatsRange, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!result.more);
				self->numShards = result.size();
				// There's no guarantee that #shards <= CLIENT_KNOBS->SHARD_COUNT_LIMIT all the time
				ASSERT(self->numShards >= 1);
				state int64_t totalBytes = 0;
				auto schema = readJSONStrictly(JSONSchemas::dataDistributionStatsSchema.toString()).get_obj();
				for (i = 0; i < result.size(); ++i) {
					ASSERT(result[i].key.startsWith(ddStatsRange.begin));
					std::string errorStr;
					auto valueObj = readJSONStrictly(result[i].value.toString()).get_obj();
					CODE_PROBE(true, "data_distribution_stats schema validation");
					if (!schemaMatch(schema, valueObj, errorStr, SevError, true)) {
						TraceEvent(SevError, "DataDistributionStatsSchemaValidationFailed")
						    .detail("ErrorStr", errorStr.c_str())
						    .detail("JSON", json_spirit::write_string(json_spirit::mValue(result[i].value.toString())));
						return false;
					}
					totalBytes += valueObj["shard_bytes"].get_int64();
				}
				self->avgBytes = totalBytes / self->numShards;
				break;
			} catch (Error& e) {
				if (e.code() == error_code_timed_out || e.code() == error_code_transaction_timed_out) {
					tr->reset();
					// The RPC call may in some corner cases get no response
					if (++retries > 10)
						break;
					continue;
				}
				wait(tr->onError(e));
			}
		}
		return true;
	}

	ACTOR Future<Void> _start(Database cx, DataDistributionMetricsWorkload* self) {
		std::vector<Future<Void>> clients;
		clients.push_back(self->resultConsistencyCheckClient(cx, self));
		for (int i = 0; i < self->actorCount; ++i)
			clients.push_back(self->ddRWClient(cx, self));
		wait(timeout(waitForAll(clients), self->testDuration, Void()));
		wait(delay(5.0));
		return Void();
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	Future<bool> check(Database const& cx) override {
		if (clientId == 0)
			return _check(cx, this);
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("NumShards", numShards, Averaged::True);
		m.emplace_back("AvgBytes", avgBytes, Averaged::True);
		m.push_back(commits.getMetric());
	}
};

WorkloadFactory<DataDistributionMetricsWorkload> DataDistributionMetricsWorkloadFactory;
