/*
 * ClearSingleRange.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "BulkSetup.h"

struct ClearSingleRange : TestWorkload {
	static constexpr auto NAME = "ClearSingleRange";
	Key begin;
	Key end;
	double startDelay;

	explicit ClearSingleRange(WorkloadContext const& wcx) : TestWorkload(wcx) {
		begin = getOption(options, "begin"_sr, normalKeys.begin);
		end = getOption(options, "end"_sr, normalKeys.end);
		startDelay = getOption(options, "beginClearRange"_sr, 10.0);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return clientId != 0 ? Void() : fdbClientClearRange(cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> fdbClientClearRange(Database db) {
		Transaction tr(db);
		Error err;
		try {
			TraceEvent("ClearSingleRange")
			    .detail("Begin", printable(begin))
			    .detail("End", printable(end))
			    .detail("StartDelay", startDelay);
			tr.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
			co_await delay(startDelay);
			tr.clear(KeyRangeRef(begin, end));
			co_await tr.commit();
		} catch (Error& e) {
			err = e;
		}
		TraceEvent("ClearRangeError").error(err);
		co_await tr.onError(err);
	}
};

WorkloadFactory<ClearSingleRange> ClearSingleRangeWorkloadFactory;
