/*
 * DataDistributionMetrics.cpp
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
#include "fdbserver/tester/workloads.actor.h"
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

	Future<Void> ddRWClient(Database cx) {
		while (true) {
			ReadYourWritesTransaction tr(cx);
			int i{ 0 };
			Error err;
			try {
				for (i = 0; i < readPerTx; ++i)
					co_await tr.get(keyForIndex(deterministicRandom()->randomInt(0, nodeCount))); // read
				for (i = 0; i < writePerTx; ++i)
					tr.set(keyForIndex(deterministicRandom()->randomInt(0, nodeCount)),
					       getRandomValue()); // write
				co_await tr.commit();
				++commits;
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				co_await tr.onError(err);
			}
		}
	}

	Future<Void> resultConsistencyCheckClient(Database cx, DataDistributionMetricsWorkload* self) {
		auto tr = makeReference<ReadYourWritesTransaction>(cx);
		while (true) {
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			tr->setOption(FDBTransactionOptions::TIMEOUT, StringRef((uint8_t*)&transactionTimeLimit, sizeof(int64_t)));
			Error err;
			try {
				co_await delay(self->delayPerLoop);
				int startIndex = deterministicRandom()->randomInt(0, self->nodeCount - 1);
				int endIndex = deterministicRandom()->randomInt(startIndex + 1, self->nodeCount);
				Key startKey = self->keyForIndex(startIndex);
				Key endKey = self->keyForIndex(endIndex);
				// Find the last key <= startKey and use as the begin of the range. Since "Key()" is always the
				// starting point, this key selector will never do cross_module_range_read. In addition, the first
				// key in the result will be the last one <= startKey (Condition #1)
				KeySelector begin = KeySelectorRef(startKey.withPrefix(ddStatsRange.begin, startKey.arena()), true, 0);
				// Find the last key less than endKey, move forward 2 keys, and use this key as the (exclusive) end
				// of the range. If we didn't read through the end of the range, then the second last key in the
				// result will be the last key less than endKey. (Condition #2)
				KeySelector end = KeySelectorRef(endKey.withPrefix(ddStatsRange.begin, endKey.arena()), false, 2);
				RangeResult result = co_await tr->getRange(begin, end, GetRangeLimits(CLIENT_KNOBS->TOO_MANY));
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
				err = e;
			}
			// Ignore timed_out error and cross_module_read, the end key selector may read through the end
			if (err.isValid() &&
			    (err.code() == error_code_timed_out || err.code() == error_code_transaction_timed_out)) {
				tr->reset();
				continue;
			}
			if (err.isValid()) {
				co_await tr->onError(err);
			}
		}
	}

	Future<bool> _check(Database cx) {
		if (errors.getValue() > 0) {
			TraceEvent(SevError, "TestFailure").detail("Reason", "GetRange Results Inconsistent");
			co_return false;
		}
		// TODO : find why this not work
		// wait(quietDatabase(cx, self->dbInfo, "PopulateTPCC"));
		auto tr = makeReference<ReadYourWritesTransaction>(cx);
		int i{ 0 };
		int retries = 0;
		while (true) {
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			tr->setOption(FDBTransactionOptions::TIMEOUT, StringRef((uint8_t*)&transactionTimeLimit, sizeof(int64_t)));
			Error err;
			try {
				RangeResult result = co_await tr->getRange(ddStatsRange, CLIENT_KNOBS->TOO_MANY);
				ASSERT(!result.more);
				numShards = result.size();
				// There's no guarantee that #shards <= CLIENT_KNOBS->SHARD_COUNT_LIMIT all the time
				ASSERT(numShards >= 1);
				int64_t totalBytes = 0;
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
						co_return false;
					}
					totalBytes += valueObj["shard_bytes"].get_int64();
				}
				avgBytes = totalBytes / numShards;
				break;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_timed_out || err.code() == error_code_transaction_timed_out) {
				tr->reset();
				// The RPC call may in some corner cases get no response
				if (++retries > 10)
					break;
				continue;
			}
			co_await tr->onError(err);
		}
		co_return true;
	}

	Future<Void> start(Database const& cx) override {
		std::vector<Future<Void>> clients;
		clients.push_back(resultConsistencyCheckClient(cx, this));
		for (int i = 0; i < actorCount; ++i)
			clients.push_back(ddRWClient(cx));
		co_await timeout(waitForAll(clients), testDuration, Void());
		co_await delay(5.0);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<bool> check(Database const& cx) override {
		if (clientId == 0)
			return _check(cx);
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("NumShards", numShards, Averaged::True);
		m.emplace_back("AvgBytes", avgBytes, Averaged::True);
		m.push_back(commits.getMetric());
	}
};

WorkloadFactory<DataDistributionMetricsWorkload> DataDistributionMetricsWorkloadFactory;
