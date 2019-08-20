/*
 * RequestStats.actor.cpp
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

#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last include

struct RequestStatsWorkload : KVWorkload {
	double testDuration;
	int keySize;
	int valueSize;
	int keysPopulated;
	int readTransactions;
	int pointReadsPerTransaction;
	int rangeReadsPerTransaction;
	int rangeReadSize;
	double samplingProbability;
	bool testFailed;

	RequestStatsWorkload(WorkloadContext const& wcx) : KVWorkload(wcx), testFailed(false) {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 60.0);
		keySize = getOption(options, LiteralStringRef("keySize"), 16);
		valueSize = getOption(options, LiteralStringRef("valueSize"), 64);
		keysPopulated = getOption(options, LiteralStringRef("keysPopulated"), 100000);
		readTransactions = getOption(options, LiteralStringRef("readTransactions"), 100);
		pointReadsPerTransaction = getOption(options, LiteralStringRef("pointReadsPerTransaction"), 10);
		rangeReadsPerTransaction = getOption(options, LiteralStringRef("rangeReadsPerTransaction"), 1);
		rangeReadSize = getOption(options, LiteralStringRef("rangeReadSize"), 10);
		samplingProbability = getOption(options, LiteralStringRef("samplingProbability"), 0.01);

		ASSERT(rangeReadSize <= keysPopulated);
	}

	ACTOR static Future<Void> _setup(Database cx, RequestStatsWorkload* self) {
		state int keyStart = 0;
		state int key = keyStart;
		state Transaction tr(cx);

		while (key < self->keysPopulated) {
			loop {
				key = keyStart;
				state int numWrites = std::min(100, self->keysPopulated - keyStart);
				try {
					tr.reset();
					while (key < keyStart + numWrites) {
						tr.set(self->keyForIndex(key), Value(std::string(self->valueSize, '0')));
						++key;
					}
					wait(tr.commit());
					keyStart = key;
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
		const_cast<ClientKnobs*>(CLIENT_KNOBS)->CSI_SAMPLING_PROBABILITY = self->samplingProbability;
		return Void();
	}

	bool checkRequestStats(Value reqStatsVal) {
		json_spirit::mObject reqStats = readJSONStrictly(reqStatsVal.toString()).get_obj();
		json_spirit::mArray reads = reqStats.at("readStatistics").get_array();
		json_spirit::mArray proxies = reqStats.at("proxiesContacted").get_array();

		for (int i = 0; i < reads.size(); ++i) {
			auto read = reads.at(i).get_obj();
			auto storageContacted = read.at("storageContacted").get_str();
			auto beginKey = read.at("beginKey").get_str();
			auto endKey = read.at("endKey").get_str();
			auto bytesFetched = read.at("bytesFetched").get_int64();
			auto keysFetched = read.at("keysFetched").get_int64();

			if ((keysFetched != 0) && (keysFetched == 1) != (beginKey == endKey)) return false;
			if (bytesFetched != keysFetched * (keySize + valueSize)) return false;

			// check address
			try {
				NetworkAddress::parse(storageContacted);
			} catch (Error& e) {
				return false;
			}
		}

		if (proxies.size() == 0) return false;

		for (int i = 0; i < proxies.size(); ++i) {
			auto proxyContacted = proxies.at(i).get_str();
			try {
				NetworkAddress::parse(proxyContacted);
			} catch (Error& e) {
				return false;
			}
		}

		return true;
	}

	ACTOR static Future<Void> _start(Database cx, RequestStatsWorkload* self) {
		state ReadYourWritesTransaction tr(cx);

		state int i = 0;
		for (; i < self->readTransactions; ++i) {
			loop {
				try {
					tr.reset();
					try {
						tr.setOption(FDBTransactionOptions::TRACK_REQUEST_STATS);
					} catch(Error &e) {
						if (e.code() != error_code_client_invalid_operation) {
							throw e;
						}
					}
					state int j = 0;
					for (; j < self->pointReadsPerTransaction; ++j) {
						Optional<Value> v =
						    wait(tr.get(self->keyForIndex(deterministicRandom()->randomInt(0, self->keysPopulated))));
						ASSERT(v.present());
					}
					for (j = 0; j < self->rangeReadsPerTransaction; ++j) {
						int startIndex = deterministicRandom()->randomInt(0, self->keysPopulated - self->rangeReadSize + 1);
						int endIndex = startIndex + self->rangeReadSize;
						KeyRange kr(KeyRangeRef(self->keyForIndex(startIndex), self->keyForIndex(endIndex)));
						Standalone<RangeResultRef> r = wait(tr.getRange(kr, 1000));
						ASSERT(!r.empty());
					}
					Optional<Value> reqStats = wait(tr.get(LiteralStringRef("\xff\xff/request_stats")));
					if (reqStats.present())
						self->testFailed = self->testFailed || !self->checkRequestStats(reqStats.get());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
		return Void();
	}

	virtual std::string description() override { return "RequestStats"; }
	virtual Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	virtual Future<Void> start(Database const& cx) override { return _start(cx, this); }
	virtual Future<bool> check(Database const& cx) override { return !testFailed; }
	virtual void getMetrics(vector<PerfMetric>& m) override {}
};

WorkloadFactory<RequestStatsWorkload> RequestStatsWorkloadFactory("RequestStats");
