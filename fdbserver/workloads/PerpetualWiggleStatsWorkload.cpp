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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.h"
#include "fdbclient/StorageWiggleMetrics.h"
#include "fdbserver/core/MoveKeys.h"
#include "fdbserver/tester/workloads.h"

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

	Future<StorageWiggleMetrics> readStorageWiggleMetrics(Database cx) {
		StorageWiggleData wiggleData;
		co_return co_await wiggleData.storageWiggleMetrics(PrimaryRegion(true))
		    .getD(cx.getReference(), Snapshot::False, StorageWiggleMetrics());
	}

	Future<Void> updateStorageWiggleMetrics(Database cx, StorageWiggleMetrics metrics) {
		co_await runRYWTransaction(cx, [metrics](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			StorageWiggleData wiggleData;
			return wiggleData.updateStorageWiggleMetrics(tr, metrics, PrimaryRegion(true));
		});
	}

	Future<Void> testRestoreAndResetStats(Database cx, StorageWiggleMetrics metrics) {
		ASSERT(storageWiggleStatsEqual(co_await readStorageWiggleMetrics(cx), metrics));
		if (!co_await IssueConfigurationChange(cx, "perpetual_storage_wiggle=0", deterministicRandom()->coinflip())) {
			co_return;
		}

		metrics.reset();
		ASSERT(storageWiggleStatsEqual(co_await readStorageWiggleMetrics(cx), metrics));
	}

	Future<Void> switchPerpetualWiggleWhenDDIsDead(Database cx, StorageWiggleMetrics metrics) {
		ASSERT(storageWiggleStatsEqual(co_await readStorageWiggleMetrics(cx), metrics));
		if (!co_await IssueConfigurationChange(cx, "perpetual_storage_wiggle=0", deterministicRandom()->coinflip())) {
			co_return;
		}
		if (!co_await IssueConfigurationChange(cx, "perpetual_storage_wiggle=1", deterministicRandom()->coinflip())) {
			co_return;
		}

		metrics.reset();
		ASSERT(storageWiggleStatsEqual(co_await readStorageWiggleMetrics(cx), metrics));
	}

	Future<Void> finishWiggleAfterPWDisabled(Database cx, StorageWiggleMetrics metrics) {
		ASSERT(storageWiggleStatsEqual(co_await readStorageWiggleMetrics(cx), metrics));
		if (!co_await IssueConfigurationChange(cx, "perpetual_storage_wiggle=0", deterministicRandom()->coinflip())) {
			co_return;
		}

		++metrics.finished_wiggle;
		++metrics.finished_round;
		co_await updateStorageWiggleMetrics(cx, metrics);

		if (!co_await IssueConfigurationChange(cx, "perpetual_storage_wiggle=1", deterministicRandom()->coinflip())) {
			co_return;
		}
		metrics.reset();
		ASSERT(storageWiggleStatsEqual(co_await readStorageWiggleMetrics(cx), metrics));
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
		co_await prepareTestEnv(cx);
		co_await testRestoreAndResetStats(cx, lastMetrics);

		co_await prepareTestEnv(cx);
		co_await switchPerpetualWiggleWhenDDIsDead(cx, lastMetrics);

		co_await prepareTestEnv(cx);
		co_await finishWiggleAfterPWDisabled(cx, lastMetrics);

		co_await setDDMode(cx, 1);
	}

	void getMetrics(std::vector<PerfMetric>& m) override { return; }
};

WorkloadFactory<PerpetualWiggleStatsWorkload> PerpetualWiggleStatsWorkload;
