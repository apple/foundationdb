/*
 * DataDistributionMetrics.actor.cpp
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

struct DataDistributionMetricsWorkload : KVWorkload {

	int numTransactions;
	int writesPerTransaction;
	int transactionsCommitted;
	int numShards;
	int64_t avgBytes;
	int64_t avgBytesPerKSec;
	int64_t avgIOsPerKSec;

	DataDistributionMetricsWorkload(WorkloadContext const& wcx)
	  : KVWorkload(wcx), transactionsCommitted(0), numShards(0), avgBytes(0), avgBytesPerKSec(0), avgIOsPerKSec(0) {
		numTransactions = getOption(options, LiteralStringRef("numTransactions"), 100);
		writesPerTransaction = getOption(options, LiteralStringRef("writesPerTransaction"), 1000);
	}

	static Value getRandomValue() { return Standalone<StringRef>(format("Value/%08d", deterministicRandom()->randomInt(0, 10e6))); }

	ACTOR static Future<Void> _start(Database cx, DataDistributionMetricsWorkload* self) {
		state int tNum;
		for (tNum = 0; tNum < self->numTransactions; ++tNum) {
			loop {
				state ReadYourWritesTransaction tr(cx);
				try {
					state int i;
					for (i = 0; i < self->writesPerTransaction; ++i) {
						tr.set(StringRef(format("Key/%08d", tNum * self->writesPerTransaction + i)), getRandomValue());
					}
					wait(tr.commit());
					++self->transactionsCommitted;
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
		return Void();
	}

	ACTOR static Future<bool> _check(Database cx, DataDistributionMetricsWorkload* self) {
		if (self->transactionsCommitted == 0) {
			TraceEvent(SevError, "NoTransactionsCommitted");
			return false;
		}
		ReadYourWritesTransaction tr(cx);
		try {
			std::string start = "\xff\xff/dd_stats/Key/";
			std::string end = "\xff\xff/dd_stats/Key0";
			Standalone<RangeResultRef> result = wait(tr.getRange(KeyRangeRef(KeyRef(start), KeyRef(end)), 100));
			self->numShards = result.size();
			if (self->numShards < 1) return false;
			int64_t totalBytes = 0;
			int64_t totalBytesPerKSec = 0;
			int64_t totalIOsPerKSec = 0;
			for (auto& kv : result) {
				json_spirit::mObject statsObj = readJSONStrictly(kv.value.toString()).get_obj()["Stats"].get_obj();
				totalBytes += statsObj["Bytes"].get_int64();
				totalBytesPerKSec += statsObj["BytesPerKSecond"].get_int64();
				totalIOsPerKSec += statsObj["IOsPerKSecond"].get_int64();
			}
			self->avgBytes = totalBytes / self->numShards;
			self->avgBytesPerKSec = totalBytesPerKSec / self->numShards;
			self->avgIOsPerKSec = totalIOsPerKSec / self->numShards;
		} catch (Error& e) {
			TraceEvent(SevError, "FailedToRetrieveDDMetrics").detail("Error", e.what());
			return false;
		}
		return true;
	}

	virtual std::string description() { return "DataDistributionMetrics"; }
	virtual Future<Void> setup(Database const& cx) { return Void(); }
	virtual Future<Void> start(Database const& cx) { return _start(cx, this); }
	virtual Future<bool> check(Database const& cx) { return _check(cx, this); }

	virtual void getMetrics(vector<PerfMetric>& m) {
		m.push_back(PerfMetric("NumShards", numShards, true));
		m.push_back(PerfMetric("AvgBytes", avgBytes, true));
		m.push_back(PerfMetric("AvgBytesPerKSec", avgBytesPerKSec, true));
		m.push_back(PerfMetric("AvgIOsPerKSec", avgIOsPerKSec, true));
	}
};

WorkloadFactory<DataDistributionMetricsWorkload> DataDistributionMetricsWorkloadFactory("DataDistributionMetrics");
