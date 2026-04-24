/*
 * PerpetualWiggleStatsWorkload.cpp
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

#include "fdbserver/datadistributor/DDTeamCollection.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbserver/datadistributor/DDSharedContext.h"
#include "fdbserver/datadistributor/DDTxnProcessor.h"
#include "fdbserver/core/MoveKeys.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/tester/workloads.h"
#include "fdbclient/VersionedMap.h"
#include "fdbclient/ReadYourWrites.h"

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
	Future<Void> testRestoreAndResetStats(StorageWiggleMetrics metrics) {
		std::cout << "testRestoreAndResetStats ...\n";
		co_await storageWiggler->restoreStats();
		ASSERT(storageWiggleStatsEqual(storageWiggler->metrics, metrics));
		// disable PW
		{
			bool success = co_await IssueConfigurationChange(
			    dbContext(), "perpetual_storage_wiggle=0", deterministicRandom()->coinflip());
			if (!success)
				co_return;
		}

		// restore from FDB
		metrics.reset();
		co_await storageWiggler->restoreStats();
		ASSERT(storageWiggleStatsEqual(storageWiggler->metrics, metrics));
	}

	Future<Void> switchPerpetualWiggleWhenDDIsDead(StorageWiggleMetrics metrics) {
		std::cout << "switchPerpetualWiggleWhenDDIsDead ...\n";
		co_await storageWiggler->restoreStats();
		ASSERT(storageWiggleStatsEqual(storageWiggler->metrics, metrics));
		// disable PW
		{
			bool success = co_await IssueConfigurationChange(
			    dbContext(), "perpetual_storage_wiggle=0", deterministicRandom()->coinflip());
			if (!success)
				co_return;
		}
		// enable PW
		{
			bool success = co_await IssueConfigurationChange(
			    dbContext(), "perpetual_storage_wiggle=1", deterministicRandom()->coinflip());
			if (!success)
				co_return;
		}
		// restart
		co_await storageWiggler->restoreStats();
		metrics.reset();
		ASSERT(storageWiggleStatsEqual(storageWiggler->metrics, metrics));
	}

	// reset stats shouldn't be overwritten when PW is disabled
	Future<Void> finishWiggleAfterPWDisabled(StorageWiggleMetrics metrics) {
		std::cout << "finishWiggleAfterPWDisabled ...\n";
		co_await storageWiggler->restoreStats();
		ASSERT(storageWiggleStatsEqual(storageWiggler->metrics, metrics));
		// disable PW
		{
			bool success = co_await IssueConfigurationChange(
			    dbContext(), "perpetual_storage_wiggle=0", deterministicRandom()->coinflip());
			if (!success)
				co_return;
		}
		co_await storageWiggler->finishWiggle();

		// restart perpetual wiggle
		{
			bool success = co_await IssueConfigurationChange(
			    dbContext(), "perpetual_storage_wiggle=1", deterministicRandom()->coinflip());
			if (!success)
				co_return;
		}
		co_await storageWiggler->restoreStats();
		metrics.reset();
		ASSERT(storageWiggleStatsEqual(storageWiggler->metrics, metrics));
	}

	explicit DDTeamCollectionTester(DDTeamCollectionInitParams const& params) : DDTeamCollection(params) {}
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

	explicit PerpetualWiggleStatsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	Future<Void> _setup(Database cx) {
		co_await setDDMode(cx, 0);
		co_await takeMoveKeysLock(cx, UID()); // force current DD to quit
		bool success = co_await IssueConfigurationChange(cx, "storage_migration_type=disabled", true);
		ASSERT(success);
		co_await delay(30.0); // make sure the DD has already quit before the test start
	}

	Future<Void> prepareTestEnv(Database cx) {
		// enable perpetual wiggle
		bool change = co_await IssueConfigurationChange(cx, "perpetual_storage_wiggle=1", true);
		ASSERT(change);
		// update wiggle metrics
		lastMetrics = getRandomWiggleMetrics();
		auto& lastMetrics = this->lastMetrics;
		co_await runRYWTransaction(cx, [&lastMetrics](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			StorageWiggleData wiggleData;
			return wiggleData.updateStorageWiggleMetrics(tr, lastMetrics, PrimaryRegion(true));
		});
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(cx); // force to disable DD
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(cx);
		}
		return Void();
	};

	Future<bool> check(Database const& cx) override { return true; };

	Future<Void> _start(Database cx) {
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

		co_await prepareTestEnv(cx);
		tester.resetStorageWiggleState();
		co_await tester.testRestoreAndResetStats(lastMetrics);

		co_await prepareTestEnv(cx);
		tester.resetStorageWiggleState();
		co_await tester.switchPerpetualWiggleWhenDDIsDead(lastMetrics);

		co_await prepareTestEnv(cx);
		tester.resetStorageWiggleState();
		co_await tester.finishWiggleAfterPWDisabled(lastMetrics);

		co_await setDDMode(cx, 1);
	}

	void getMetrics(std::vector<PerfMetric>& m) override { return; }
};

WorkloadFactory<PerpetualWiggleStatsWorkload> PerpetualWiggleStatsWorkload;
