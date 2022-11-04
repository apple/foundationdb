/*
 * PerpetualWiggleStatsWorkload.actor.cpp
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

#include "fdbserver/DDTeamCollection.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/DDSharedContext.h"
#include "fdbserver/DDTxnProcessor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/VersionedMap.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// a wrapper for test protected method
struct DDTeamCollectionTester : public DDTeamCollection {
	ActorCollection actors;
	Future<Void> testPerpetualWiggleStats() {
		actors.add(this->monitorPerpetualStorageWiggle());
		return timeout(actors.getResult(), 60.0, Void());
	}
	DDTeamCollectionTester(DDTeamCollectionInitParams const& params) : DDTeamCollection(params) {}
};

struct PerpetualWiggleStatsWorkload : public TestWorkload {

	static constexpr auto NAME = "PerpetualWiggleStatsWorkload";

	PerpetualWiggleStatsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	ACTOR static Future<Void> _setup(Database cx) {
		int oldMode = wait(setDDMode(cx, 0));
		MoveKeysLock lock = wait(takeMoveKeysLock(cx, UID())); // force current DD to quit
		return Void();
	}
	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(cx); // force to disable DD
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(this, cx);
		}
		return Void();
	};

	Future<bool> check(Database const& cx) override { return true; };

	ACTOR static Future<Void> _start(PerpetualWiggleStatsWorkload* self, Database cx) {
		state Reference<IDDTxnProcessor> db = makeReference<DDTxnProcessor>(cx);
		state DatabaseConfiguration conf;
		state DDTeamCollectionTester tester({ db,
		                                      UID(0, 0),
		                                      MoveKeysLock(),
		                                      PromiseStream<RelocateShard>(),
		                                      makeReference<ShardsAffectedByTeamFailure>(),
		                                      conf,
		                                      {},
		                                      {},
		                                      Future<Void>(Void()),
		                                      makeReference<AsyncVar<bool>>(true),
		                                      IsPrimary::True,
		                                      makeReference<AsyncVar<bool>>(false),
		                                      makeReference<AsyncVar<bool>>(false),
		                                      PromiseStream<GetMetricsRequest>(),
		                                      Promise<UID>(),
		                                      PromiseStream<Promise<int>>() });
		tester.configuration.storageTeamSize = 3;
		tester.configuration.perpetualStorageWiggleSpeed = 1;
		wait(tester.testPerpetualWiggleStats());
		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override { return; }
};

WorkloadFactory<PerpetualWiggleStatsWorkload> PerpetualWiggleStatsWorkload;