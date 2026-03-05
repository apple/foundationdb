/*
 * PerpetualWiggleStatsWorkload.actor.cpp
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

namespace {
Future<bool> IssueConfigurationChange(Database cx, std::string config, bool force) {
	printf("Issuing configuration change: %s\n", config.c_str());
	ConfigurationResult res = co_await ManagementAPI::changeConfig(cx.getReference(), config, force);
	if (res != ConfigurationResult::SUCCESS) {
		co_return false;
	}
	co_await delay(5.0); // wait for read window
	co_return true;
}
} // namespace

// a wrapper for test protected method
struct DDTeamCollectionTester : public DDTeamCollection {
	ActorCollection actors;
	void resetStorageWiggleState() {
		wigglingId.reset();
		storageWiggler = makeReference<StorageWiggler>(this);
	}
	static Future<Void> testRestoreAndResetStats(DDTeamCollectionTester* self, StorageWiggleMetrics metrics) {
		std::cout << "testRestoreAndResetStats ...\n";
		co_await self->storageWiggler->restoreStats();
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
		// disable PW
		{
			bool success = co_await IssueConfigurationChange(
			    self->dbContext(), "perpetual_storage_wiggle=0", deterministicRandom()->coinflip());
			if (!success)
				co_return;
		}

		// restore from FDB
		metrics.reset();
		co_await self->storageWiggler->restoreStats();
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
	}

	static Future<Void> switchPerpetualWiggleWhenDDIsDead(DDTeamCollectionTester* self, StorageWiggleMetrics metrics) {
		std::cout << "switchPerpetualWiggleWhenDDIsDead ...\n";
		co_await self->storageWiggler->restoreStats();
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
		// disable PW
		{
			bool success = co_await IssueConfigurationChange(
			    self->dbContext(), "perpetual_storage_wiggle=0", deterministicRandom()->coinflip());
			if (!success)
				co_return;
		}
		// enable PW
		{
			bool success = co_await IssueConfigurationChange(
			    self->dbContext(), "perpetual_storage_wiggle=1", deterministicRandom()->coinflip());
			if (!success)
				co_return;
		}
		// restart
		co_await self->storageWiggler->restoreStats();
		metrics.reset();
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
	}

	// reset stats shouldn't be overwritten when PW is disabled
	static Future<Void> finishWiggleAfterPWDisabled(DDTeamCollectionTester* self, StorageWiggleMetrics metrics) {
		std::cout << "finishWiggleAfterPWDisabled ...\n";
		co_await self->storageWiggler->restoreStats();
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
		// disable PW
		{
			bool success = co_await IssueConfigurationChange(
			    self->dbContext(), "perpetual_storage_wiggle=0", deterministicRandom()->coinflip());
			if (!success)
				co_return;
		}
		co_await self->storageWiggler->finishWiggle();

		// restart perpetual wiggle
		{
			bool success = co_await IssueConfigurationChange(
			    self->dbContext(), "perpetual_storage_wiggle=1", deterministicRandom()->coinflip());
			if (!success)
				co_return;
		}
		co_await self->storageWiggler->restoreStats();
		metrics.reset();
		ASSERT(storageWiggleStatsEqual(self->storageWiggler->metrics, metrics));
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

	static Future<Void> _setup(PerpetualWiggleStatsWorkload* self, Database cx) {
		co_await success(setDDMode(cx, 0));
		co_await success(takeMoveKeysLock(cx, UID())); // force current DD to quit
		bool success = co_await IssueConfigurationChange(cx, "storage_migration_type=disabled", true);
		ASSERT(success);
		co_await delay(30.0); // make sure the DD has already quit before the test start
	}

	static Future<Void> prepareTestEnv(PerpetualWiggleStatsWorkload* self, Database cx) {
		// enable perpetual wiggle
		bool change = co_await IssueConfigurationChange(cx, "perpetual_storage_wiggle=1", true);
		ASSERT(change);
		// update wiggle metrics
		self->lastMetrics = getRandomWiggleMetrics();
		auto& lastMetrics = self->lastMetrics;
		co_await success(runRYWTransaction(cx, [&lastMetrics](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			StorageWiggleData wiggleData;
			return wiggleData.updateStorageWiggleMetrics(tr, lastMetrics, PrimaryRegion(true));
		}));
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

	static Future<Void> _start(PerpetualWiggleStatsWorkload* self, Database cx) {
		Reference<IDDTxnProcessor> db = makeReference<DDTxnProcessor>(cx);
		DatabaseConfiguration conf;
		DDTeamCollectionTester tester({ db,
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
		                                PromiseStream<Promise<int>>(),
		                                PromiseStream<Promise<int64_t>>(),
		                                PromiseStream<RebalanceStorageQueueRequest>(),
		                                makeReference<BulkLoadTaskCollection>(UID(8, 6)) });
		tester.configuration.storageTeamSize = 3;
		tester.configuration.perpetualStorageWiggleSpeed = 1;

		co_await prepareTestEnv(self, cx);
		tester.resetStorageWiggleState();
		co_await DDTeamCollectionTester::testRestoreAndResetStats(&tester, self->lastMetrics);

		co_await prepareTestEnv(self, cx);
		tester.resetStorageWiggleState();
		co_await DDTeamCollectionTester::switchPerpetualWiggleWhenDDIsDead(&tester, self->lastMetrics);

		co_await prepareTestEnv(self, cx);
		tester.resetStorageWiggleState();
		co_await DDTeamCollectionTester::finishWiggleAfterPWDisabled(&tester, self->lastMetrics);

		co_await success(setDDMode(cx, 1));
	}

	void getMetrics(std::vector<PerfMetric>& m) override { return; }
};

WorkloadFactory<PerpetualWiggleStatsWorkload> PerpetualWiggleStatsWorkload;