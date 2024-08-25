/*
 * ClearSingleRange.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ClearSingleRange : TestWorkload {
	static constexpr auto NAME = "ClearSingleRange";
	Key begin;
	Key end;
	double startDelay;

	ClearSingleRange(WorkloadContext const& wcx) : TestWorkload(wcx) {
		begin = getOption(options, "begin"_sr, normalKeys.begin);
		end = getOption(options, "end"_sr, normalKeys.end);
		startDelay = getOption(options, "beginClearRange"_sr, 10.0);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return clientId != 0 ? Void() : fdbClientClearRange(cx, this); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> fdbClientClearRange(Database db, ClearSingleRange* self) {
		state Transaction tr(db);
		try {
			TraceEvent("ClearSingleRange")
			    .detail("Begin", printable(self->begin))
			    .detail("End", printable(self->end))
			    .detail("StartDelay", self->startDelay);
			tr.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
			wait(delay(self->startDelay));
			tr.clear(KeyRangeRef(self->begin, self->end));
			wait(tr.commit());
		} catch (Error& e) {
			TraceEvent("ClearRangeError").error(e);
			wait(tr.onError(e));
		}
		return Void();
	}
};

WorkloadFactory<ClearSingleRange> ClearSingleRangeWorkloadFactory;
