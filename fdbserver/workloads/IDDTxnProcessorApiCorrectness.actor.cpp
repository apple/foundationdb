/*
 * IDDTxnProcessorApiCorrectness.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/DDSharedContext.h"
#include "fdbserver/DDTxnProcessor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

bool compareShardInfo(const DDShardInfo& a, const DDShardInfo& other) {
	std::cout << a.key.toHexString() << " | " << other.key.toHexString() << "\n";
	std::cout << a.hasDest << " | " << other.hasDest << "\n";
	std::cout << describe(a.primarySrc) << " | " << describe(other.primarySrc) << "\n";
	std::cout << describe(a.primaryDest) << " | " << describe(other.primaryDest) << "\n";
	std::cout << describe(a.remoteSrc) << " | " << describe(other.remoteSrc) << "\n";
	std::cout << describe(a.remoteDest) << " | " << describe(other.remoteDest) << "\n";

	// Mock DD just care about the server<->key mapping in DDShardInfo
	return a.key == other.key && a.hasDest == other.hasDest && a.primaryDest == other.primaryDest &&
	       a.primarySrc == other.primarySrc && a.remoteSrc == other.remoteSrc && a.remoteDest == other.remoteDest;
}

void verifyInitDataEqual(Reference<InitialDataDistribution> real, Reference<InitialDataDistribution> mock) {
	// Mock DD just care about the team list and server<->key mapping are consistent with the real cluster
	ASSERT(std::equal(
	    real->shards.begin(), real->shards.end(), mock->shards.begin(), mock->shards.end(), compareShardInfo));
	std::cout << describe(real->primaryTeams) << " | " << describe(mock->primaryTeams) << "\n";
	ASSERT(real->primaryTeams == mock->primaryTeams);
	ASSERT(real->remoteTeams == mock->remoteTeams);
	ASSERT_EQ(real->shards.size(), mock->shards.size());
}

// Verify that all IDDTxnProcessor API implementations has consistent result
struct IDDTxnProcessorApiWorkload : TestWorkload {
	static const char* desc;
	bool enabled;
	double testDuration;
	DDSharedContext ddContext;

	std::shared_ptr<IDDTxnProcessor> real;
	std::shared_ptr<MockGlobalState> mgs;
	std::shared_ptr<DDMockTxnProcessor> mock;

	Reference<InitialDataDistribution> realInitDD;

	IDDTxnProcessorApiWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), ddContext(UID()) {
		enabled = !clientId && g_network->isSimulated(); // only do this on the "first" client
		testDuration = getOption(options, "testDuration"_sr, 10.0);
	}

	std::string description() const override { return desc; }
	Future<Void> setup(Database const& cx) override { return enabled ? _setup(cx, this) : Void(); }
	Future<Void> start(Database const& cx) override { return enabled ? _start(cx, this) : Void(); }

	// This workload is not compatible with RandomMoveKeys workload because they will race in changing the DD mode.
	// Other workload injections may make no sense because this workload only use the DB at beginning to reading the
	// real world key-server mappings. It's not harmful to leave other workload injection enabled for now, though.
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("RandomMoveKeys"); }

	ACTOR Future<Void> _setup(Database cx, IDDTxnProcessorApiWorkload* self) {
		self->real = std::make_shared<DDTxnProcessor>(cx);
		// Get the database configuration so as to use proper team size
		wait(store(self->ddContext.configuration, self->real->getDatabaseConfiguration()));
		ASSERT(self->ddContext.configuration.storageTeamSize > 0);
		// FIXME: add support for generating random teams across DCs
		ASSERT_EQ(self->ddContext.usableRegions(), 1);

		loop {
			wait(store(self->ddContext.lock, ::readMoveKeysLock(cx)));
			// read real InitialDataDistribution
			try {
				wait(store(self->realInitDD,
				           self->real->getInitialDataDistribution(
				               self->ddContext.id(), self->ddContext.lock, {}, self->ddContext.ddEnabledState.get())));
				std::cout << "Finish read real InitialDataDistribution: server size "
				          << self->realInitDD->allServers.size() << ", shard size: " << self->realInitDD->shards.size()
				          << std::endl;
				break;
			} catch (Error& e) {
				if (e.code() != error_code_movekeys_conflict)
					throw;
			}
		}
		return Void();
	}

	ACTOR Future<Void> _start(Database cx, IDDTxnProcessorApiWorkload* self) {
		int oldMode = wait(setDDMode(cx, 0));
		TraceEvent("IDDTxnApiTestStartModeSetting").detail("OldValue", oldMode).log();
		self->mgs = std::make_shared<MockGlobalState>();
		self->mgs->configuration = self->ddContext.configuration;
		self->mock = std::make_shared<DDMockTxnProcessor>(self->mgs);
		self->mock->setupMockGlobalState(self->realInitDD);

		Reference<InitialDataDistribution> mockInitData =
		    self->mock
		        ->getInitialDataDistribution(
		            self->ddContext.id(), self->ddContext.lock, {}, self->ddContext.ddEnabledState.get())
		        .get();

		verifyInitDataEqual(self->realInitDD, mockInitData);

		// wait(timeout(reportErrors(self->worker(cx, self), "IDDTxnProcessorApiWorkload"), self->testDuration,
		// Void()));

		// Always set the DD mode back, even if we die with an error
		TraceEvent("IDDTxnApiTestDoneMoving").log();
		wait(success(setDDMode(cx, 1)));
		TraceEvent("IDDTxnApiTestDoneModeSetting").log();
		return Void();
	}

	// ACTOR Future<Void> worker(Database cx, IDDTxnProcessorApiWorkload* self) { return Void(); }

	Future<bool> check(Database const& cx) override {
		return tag(delay(testDuration / 2), true);
	} // Give the database time to recover from our damage
	void getMetrics(std::vector<PerfMetric>& m) override {}
};
const char* IDDTxnProcessorApiWorkload::desc = "IDDTxnProcessorApiCorrectness";

WorkloadFactory<IDDTxnProcessorApiWorkload> IDDTxnProcessorApiWorkload(IDDTxnProcessorApiWorkload::desc);