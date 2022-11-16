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
#include "fdbclient/VersionedMap.h"
#include "flow/actorcompiler.h" // This must be the last #include.

bool compareShardInfo(const DDShardInfo& a, const DDShardInfo& other) {
	// Mock DD just care about the server<->key mapping in DDShardInfo
	bool result = a.key == other.key && a.hasDest == other.hasDest && a.primaryDest == other.primaryDest &&
	              a.primarySrc == other.primarySrc && a.remoteSrc == other.remoteSrc &&
	              a.remoteDest == other.remoteDest;
	if (!result) {
		std::cout << a.key.toHexString() << " | " << other.key.toHexString() << "\n";
		std::cout << a.hasDest << " | " << other.hasDest << "\n";
		std::cout << describe(a.primarySrc) << " | " << describe(other.primarySrc) << "\n";
		std::cout << describe(a.primaryDest) << " | " << describe(other.primaryDest) << "\n";
		std::cout << describe(a.remoteSrc) << " | " << describe(other.remoteSrc) << "\n";
		std::cout << describe(a.remoteDest) << " | " << describe(other.remoteDest) << "\n";
	}
	return result;
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

// testers expose protected methods
class DDMockTxnProcessorTester : public DDMockTxnProcessor {
public:
	explicit DDMockTxnProcessorTester(std::shared_ptr<MockGlobalState> mgs = nullptr) : DDMockTxnProcessor(mgs) {}
	void testRawStartMovement(MoveKeysParams& params, std::map<UID, StorageServerInterface>& tssMapping) {
		rawStartMovement(params, tssMapping);
	}

	void testRawFinishMovement(MoveKeysParams& params, const std::map<UID, StorageServerInterface>& tssMapping) {
		rawFinishMovement(params, tssMapping);
	}
};

class DDTxnProcessorTester : public DDTxnProcessor {
public:
	explicit DDTxnProcessorTester(Database cx) : DDTxnProcessor(cx) {}

	Future<Void> testRawStartMovement(MoveKeysParams& params, std::map<UID, StorageServerInterface>& tssMapping) {
		return this->rawStartMovement(params, tssMapping);
	}

	Future<Void> testRawFinishMovement(MoveKeysParams& params,
	                                   const std::map<UID, StorageServerInterface>& tssMapping) {
		return this->rawFinishMovement(params, tssMapping);
	}
};

// Verify that all IDDTxnProcessor API implementations has consistent result
struct IDDTxnProcessorApiWorkload : TestWorkload {
	static constexpr auto NAME = "IDDTxnProcessorApiCorrectness";
	bool enabled;
	double testDuration;
	double meanDelay = 0.05;
	double maxKeyspace = 0.1;
	DDSharedContext ddContext;

	std::shared_ptr<DDTxnProcessorTester> real;
	std::shared_ptr<MockGlobalState> mgs;
	std::shared_ptr<DDMockTxnProcessorTester> mock;

	Reference<InitialDataDistribution> realInitDD;

	IDDTxnProcessorApiWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), ddContext(UID()) {
		enabled = !clientId && g_network->isSimulated(); // only do this on the "first" client
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		meanDelay = getOption(options, "meanDelay"_sr, meanDelay);
		maxKeyspace = getOption(options, "maxKeyspace"_sr, maxKeyspace);
	}

	Future<Void> setup(Database const& cx) override { return enabled ? _setup(cx, this) : Void(); }
	Future<Void> start(Database const& cx) override { return enabled ? _start(cx, this) : Void(); }

	// This workload is not compatible with RandomMoveKeys workload because they will race in changing the DD mode.
	// Other workload injections may make no sense because this workload only use the DB at beginning to reading the
	// real world key-server mappings. It's not harmful to leave other workload injection enabled for now, though.
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("RandomMoveKeys"); }

	ACTOR static Future<Void> readRealInitialDataDistribution(IDDTxnProcessorApiWorkload* self) {
		loop {
			wait(store(self->ddContext.lock, ::readMoveKeysLock(self->real->context())));
			// read real InitialDataDistribution
			try {
				wait(store(self->realInitDD,
				           self->real->getInitialDataDistribution(self->ddContext.id(),
				                                                  self->ddContext.lock,
				                                                  {},
				                                                  self->ddContext.ddEnabledState.get(),
				                                                  SkipDDModeCheck::True)));
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

	KeyRange getRandomKeys() const {
		double len = deterministicRandom()->random01() * this->maxKeyspace;
		double pos = deterministicRandom()->random01() * (1.0 - len);
		return KeyRangeRef(doubleToTestKey(pos), doubleToTestKey(pos + len));
	}

	std::vector<UID> getRandomTeam() {
		int& teamSize = ddContext.configuration.storageTeamSize;
		if (realInitDD->allServers.size() < teamSize) {
			TraceEvent(SevWarnAlways, "CandidatesLessThanTeamSize").log();
			throw operation_failed();
		}
		deterministicRandom()->randomShuffle(realInitDD->allServers, teamSize);
		std::vector<UID> result(teamSize);
		for (int i = 0; i < teamSize; ++i) {
			result[i] = realInitDD->allServers[i].first.id();
		}
		return result;
	}

	ACTOR Future<Void> _setup(Database cx, IDDTxnProcessorApiWorkload* self) {
		int oldMode = wait(setDDMode(cx, 0));
		TraceEvent("IDDTxnApiTestStartModeSetting").detail("OldValue", oldMode).log();

		self->real = std::make_shared<DDTxnProcessorTester>(cx);
		// Get the database configuration so as to use proper team size
		wait(store(self->ddContext.configuration, self->real->getDatabaseConfiguration()));
		ASSERT(self->ddContext.configuration.storageTeamSize > 0);
		// FIXME: add support for generating random teams across DCs
		ASSERT_EQ(self->ddContext.usableRegions(), 1);
		wait(readRealInitialDataDistribution(self));

		return Void();
	}

	ACTOR Future<Void> _start(Database cx, IDDTxnProcessorApiWorkload* self) {

		self->mgs = std::make_shared<MockGlobalState>();
		self->mgs->configuration = self->ddContext.configuration;
		self->mgs->restrictSize = false; // no need to check the validity of shard size

		self->mock = std::make_shared<DDMockTxnProcessorTester>(self->mgs);
		self->mock->setupMockGlobalState(self->realInitDD);

		Reference<InitialDataDistribution> mockInitData =
		    self->mock
		        ->getInitialDataDistribution(self->ddContext.id(),
		                                     self->ddContext.lock,
		                                     {},
		                                     self->ddContext.ddEnabledState.get(),
		                                     SkipDDModeCheck::True)
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

	ACTOR static Future<Void> testRawMovementApi(IDDTxnProcessorApiWorkload* self) {
		state TraceInterval relocateShardInterval("RelocateShard");
		state FlowLock fl1(1);
		state FlowLock fl2(1);
		state std::map<UID, StorageServerInterface> emptyTssMapping;
		state Reference<InitialDataDistribution> mockInitData;
		state MoveKeysParams params = wait(generateMoveKeysParams(self));
		params.startMoveKeysParallelismLock = &fl1;
		params.finishMoveKeysParallelismLock = &fl2;
		params.relocationIntervalId = relocateShardInterval.pairID;

		// test start
		self->mock->testRawStartMovement(params, emptyTssMapping);
		wait(self->real->testRawStartMovement(params, emptyTssMapping));

		// read initial data again
		wait(readRealInitialDataDistribution(self));
		mockInitData = self->mock
		                   ->getInitialDataDistribution(self->ddContext.id(),
		                                                self->ddContext.lock,
		                                                {},
		                                                self->ddContext.ddEnabledState.get(),
		                                                SkipDDModeCheck::True)
		                   .get();

		verifyInitDataEqual(self->realInitDD, mockInitData);

		// test finish or started but cancelled movement
		if (deterministicRandom()->coinflip()) {
			CODE_PROBE(true, "RawMovementApi partial started", probe::decoration::rare);
			return Void();
		}

		self->mock->testRawFinishMovement(params, emptyTssMapping);
		wait(self->real->testRawFinishMovement(params, emptyTssMapping));

		// read initial data again
		wait(readRealInitialDataDistribution(self));
		mockInitData = self->mock
		                   ->getInitialDataDistribution(self->ddContext.id(),
		                                                self->ddContext.lock,
		                                                {},
		                                                self->ddContext.ddEnabledState.get(),
		                                                SkipDDModeCheck::True)
		                   .get();

		verifyInitDataEqual(self->realInitDD, mockInitData);
		return Void();
	}

	ACTOR static Future<MoveKeysParams> generateMoveKeysParams(IDDTxnProcessorApiWorkload* self) { // always empty
		state MoveKeysLock lock = wait(takeMoveKeysLock(self->real->context(), UID()));

		KeyRange keys = self->getRandomKeys();
		std::vector<UID> destTeams = self->getRandomTeam();
		return MoveKeysParams{ deterministicRandom()->randomUniqueID(),
			                   keys,
			                   destTeams,
			                   destTeams,
			                   lock,
			                   Promise<Void>(),
			                   nullptr,
			                   nullptr,
			                   false,
			                   UID(),
			                   self->ddContext.ddEnabledState.get(),
			                   CancelConflictingDataMoves::True };
	}

	ACTOR static Future<Void> testMoveKeys(IDDTxnProcessorApiWorkload* self) {
		state TraceInterval relocateShardInterval("RelocateShard");
		state FlowLock fl1(1);
		state FlowLock fl2(1);
		state std::map<UID, StorageServerInterface> emptyTssMapping;
		state Reference<InitialDataDistribution> mockInitData;
		state MoveKeysParams params = wait(generateMoveKeysParams(self));
		params.startMoveKeysParallelismLock = &fl1;
		params.finishMoveKeysParallelismLock = &fl2;
		params.relocationIntervalId = relocateShardInterval.pairID;

		self->mock->moveKeys(params);
		wait(self->real->moveKeys(params));

		// read initial data again
		wait(readRealInitialDataDistribution(self));
		mockInitData = self->mock
		                   ->getInitialDataDistribution(self->ddContext.id(),
		                                                self->ddContext.lock,
		                                                {},
		                                                self->ddContext.ddEnabledState.get(),
		                                                SkipDDModeCheck::True)
		                   .get();

		verifyInitDataEqual(self->realInitDD, mockInitData);

		return Void();
	}
	ACTOR Future<Void> worker(Database cx, IDDTxnProcessorApiWorkload* self) {
		state double lastTime = now();
		state int choice = 0;
		loop {
			choice = deterministicRandom()->randomInt(0, 2);
			if (choice == 0) { // test rawStartMovement and rawFinishMovement separately
				wait(testRawMovementApi(self));
			} else if (choice == 1) { // test moveKeys
				wait(testMoveKeys(self));
			} else {
				ASSERT(false);
			}
			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
			// Keep trying to get the moveKeysLock
		}
	}

	Future<bool> check(Database const& cx) override {
		return tag(delay(testDuration / 2), true);
	} // Give the database time to recover from our damage

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<IDDTxnProcessorApiWorkload> IDDTxnProcessorApiWorkload;
