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
#include "fdbclient/ReadYourWrites.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// just compare the int part and smoothed duration is enough for the test.
bool storageWiggleStatsEqual(StorageWiggleMetrics const& a, StorageWiggleMetrics const& b) {

	bool res = a.finished_wiggle == b.finished_wiggle && a.finished_round == b.finished_round &&
	           std::abs(a.smoothed_wiggle_duration.getTotal() - b.smoothed_wiggle_duration.getTotal()) < 0.0001 &&
	           std::abs(a.smoothed_round_duration.getTotal() - b.smoothed_round_duration.getTotal()) < 0.0001;
	if (!res) {
		std::cout << a.finished_wiggle << " | " << b.finished_wiggle << "\n";
		std::cout << a.finished_round << " | " << b.finished_round << "\n";
		std::cout << a.smoothed_wiggle_duration.getTotal() << " | " << b.smoothed_wiggle_duration.getTotal() << "\n";
		std::cout << a.smoothed_round_duration.getTotal() << " | " << b.smoothed_round_duration.getTotal() << "\n";
	}
	return res;
}

Future<ConfigurationResult> IssueConfigurationChange(Database cx, const std::string& config, bool force) {
	printf("Issuing configuration change: %s\n", config.c_str());
	return ManagementAPI::changeConfig(cx.getReference(), config, force);
}

// a wrapper for test protected method
struct DDTeamCollectionTester : public DDTeamCollection {
	ACTOR static Future<Void> testRestoreAndResetStats(DDTeamCollectionTester* self, StorageWiggleMetrics metrics) {
		state Future<Void> wiggleFuture = self->monitorPerpetualStorageWiggle();
		std::cout << "testRestoreAndResetStats ...\n";
		wait(delay(5.0)); // wait for restore txn finish
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
		// disable PW
		wait(success(IssueConfigurationChange(
		    self->dbContext(), "perpetual_storage_wiggle=0", deterministicRandom()->coinflip())));

		// restore from FDB
		metrics.reset();
		wiggleFuture = self->monitorPerpetualStorageWiggle();
		wait(delay(5.0));
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
		return Void();
	}

	ACTOR static Future<Void> switchPerpetualWiggleWhenDDIsDead(DDTeamCollectionTester* self,
	                                                            StorageWiggleMetrics metrics) {
		state Future<Void> wiggleFuture = self->monitorPerpetualStorageWiggle();
		std::cout << "switchPerpetualWiggleWhenDDIsDead ...\n";
		wait(delay(5.0)); // wait for restore txn finish
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
		wiggleFuture.cancel(); // mock dead DD
		// disable PW
		wait(success(IssueConfigurationChange(
		    self->dbContext(), "perpetual_storage_wiggle=0", deterministicRandom()->coinflip())));
		// enable PW
		wait(success(IssueConfigurationChange(
		    self->dbContext(), "perpetual_storage_wiggle=1", deterministicRandom()->coinflip())));
		// restart
		wiggleFuture = self->monitorPerpetualStorageWiggle();
		wait(delay(5.0)); // wait for reset txn finish
		metrics.reset();
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
		return Void();
	}

	// reset stats shouldn't be overwritten when PW is disabled
	ACTOR static Future<Void> finishWiggleAfterPWDisabled(DDTeamCollectionTester* self, StorageWiggleMetrics metrics) {
		state Future<Void> wiggleFuture = self->monitorPerpetualStorageWiggle();
		std::cout << "finishWiggleAfterPWDisabled ...\n";
		wait(delay(5.0)); // wait for restore txn finish
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
		// disable PW
		wait(success(IssueConfigurationChange(
		    self->dbContext(), "perpetual_storage_wiggle=0", deterministicRandom()->coinflip())));
		wait(self->storageWiggler->finishWiggle());

		// restart perpetual wiggle
		wait(success(IssueConfigurationChange(
		    self->dbContext(), "perpetual_storage_wiggle=1", deterministicRandom()->coinflip())));
		wiggleFuture = self->monitorPerpetualStorageWiggle();
		wait(delay(5.0)); // wait for reset txn finish
		metrics.reset();
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
		return Void();
	}

	DDTeamCollectionTester(DDTeamCollectionInitParams const& params) : DDTeamCollection(params) {}
};

StorageWiggleMetrics getRandomWiggleMetrics() {
	StorageWiggleMetrics res;
	res.smoothed_round_duration.reset(deterministicRandom()->randomUInt32());
	res.smoothed_wiggle_duration.reset(deterministicRandom()->randomUInt32());
	res.finished_round = deterministicRandom()->randomUInt32();
	res.finished_wiggle = deterministicRandom()->randomUInt32();
	return res;
}

struct PerpetualWiggleStatsWorkload : public TestWorkload {

	static constexpr auto NAME = "PerpetualWiggleStatsWorkload";
	StorageWiggleMetrics lastMetrics;

	PerpetualWiggleStatsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	ACTOR static Future<Void> _setup(PerpetualWiggleStatsWorkload* self, Database cx) {
		int oldMode = wait(setDDMode(cx, 0));
		MoveKeysLock lock = wait(takeMoveKeysLock(cx, UID())); // force current DD to quit
		return Void();
	}

	ACTOR static Future<Void> prepareTestEnv(PerpetualWiggleStatsWorkload* self, Database cx) {
		// enable perpetual wiggle
		wait(success(ManagementAPI::changeConfig(cx.getReference(), "perpetual_storage_wiggle=1", true)));
		// update wiggle metrics
		self->lastMetrics = getRandomWiggleMetrics();
		auto& lastMetrics = self->lastMetrics;
		wait(success(runRYWTransaction(cx, [&lastMetrics](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			return updateStorageWiggleMetrics(tr, lastMetrics, PrimaryRegion(true));
		})));
		return Void();
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(this, cx); // force to disable DD
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
		                                      UID(8, 6),
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

		wait(prepareTestEnv(self, cx));
		wait(DDTeamCollectionTester::testRestoreAndResetStats(&tester, self->lastMetrics));

		wait(prepareTestEnv(self, cx));
		wait(DDTeamCollectionTester::switchPerpetualWiggleWhenDDIsDead(&tester, self->lastMetrics));

		wait(prepareTestEnv(self, cx));
		wait(DDTeamCollectionTester::finishWiggleAfterPWDisabled(&tester, self->lastMetrics));

		wait(success(setDDMode(cx, 1)));
		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override { return; }
};

WorkloadFactory<PerpetualWiggleStatsWorkload> PerpetualWiggleStatsWorkload;