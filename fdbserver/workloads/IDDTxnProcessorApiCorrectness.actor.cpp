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

std::string describe(const DDShardInfo& a) {
	std::string res = "key: " + a.key.toString() + "\n";
	res += "\tprimarySrc: " + describe(a.primarySrc) + "\n";
	res += "\tprimaryDest: " + describe(a.primaryDest) + "\n";
	res += "\tremoteSrc: " + describe(a.remoteSrc) + "\n";
	res += "\tremoteDest: " + describe(a.remoteDest) + "\n";
	return res;
}
bool compareShardInfo(const DDShardInfo& a, const DDShardInfo& other) {
	// Mock DD just care about the server<->key mapping in DDShardInfo
	bool result = a.key == other.key && a.hasDest == other.hasDest && a.primaryDest == other.primaryDest &&
	              a.primarySrc == other.primarySrc && a.remoteSrc == other.remoteSrc &&
	              a.remoteDest == other.remoteDest;
	if (!result) {
		std::cout << a.key.toStringView() << " | " << other.key.toStringView() << "\n";
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
	if (real->shards.size() != mock->shards.size()) {
		std::cout << "shardBoundaries: real v.s. mock \n";
		for (auto& shard : real->shards) {
			std::cout << describe(shard);
		}
		std::cout << " ------- \n";
		for (auto& shard : mock->shards) {
			std::cout << describe(shard);
		}
	}
	ASSERT_EQ(real->shards.size(), mock->shards.size());
	ASSERT(std::equal(
	    real->shards.begin(), real->shards.end(), mock->shards.begin(), mock->shards.end(), compareShardInfo));

	if (real->primaryTeams != mock->primaryTeams) {
		std::cout << describe(real->primaryTeams) << " | " << describe(mock->primaryTeams) << "\n";
		ASSERT(false);
	}

	ASSERT(real->remoteTeams == mock->remoteTeams);
}

// testers expose protected methods
class DDMockTxnProcessorTester : public DDMockTxnProcessor {
public:
	explicit DDMockTxnProcessorTester(std::shared_ptr<MockGlobalState> mgs = nullptr) : DDMockTxnProcessor(mgs) {}
	Future<Void> testRawStartMovement(MoveKeysParams& params, std::map<UID, StorageServerInterface>& tssMapping) {
		return rawStartMovement(params, tssMapping);
	}

	Future<Void> testRawFinishMovement(MoveKeysParams& params,
	                                   const std::map<UID, StorageServerInterface>& tssMapping) {
		return rawFinishMovement(params, tssMapping);
	}
};

class DDTxnProcessorTester : public DDTxnProcessor {
public:
	explicit DDTxnProcessorTester(Database cx) : DDTxnProcessor(cx) {}

	Future<Void> testRawStartMovement(MoveKeysParams& params, std::map<UID, StorageServerInterface>& tssMapping) {
		return rawStartMovement(params, tssMapping);
	}

	Future<Void> testRawFinishMovement(MoveKeysParams& params,
	                                   const std::map<UID, StorageServerInterface>& tssMapping) {
		return rawFinishMovement(params, tssMapping);
	}
};

// Verify that all IDDTxnProcessor API implementations has consistent result
struct IDDTxnProcessorApiWorkload : TestWorkload {
	static constexpr auto NAME = "IDDTxnProcessorApiCorrectness";
	bool enabled;
	bool testStartOnly;
	double testDuration;
	double meanDelay = 0.05;
	double maxKeyspace = 0.1;
	DDSharedContext ddContext;

	std::shared_ptr<DDTxnProcessorTester> real;
	std::shared_ptr<MockGlobalState> mgs;
	std::shared_ptr<DDMockTxnProcessorTester> mock;

	Reference<InitialDataDistribution> realInitDD;
	std::set<Key> boundaries;

	IDDTxnProcessorApiWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), ddContext(UID()) {
		enabled = !clientId && g_network->isSimulated(); // only do this on the "first" client
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		meanDelay = getOption(options, "meanDelay"_sr, meanDelay);
		maxKeyspace = getOption(options, "maxKeyspace"_sr, maxKeyspace);
		testStartOnly = getOption(options, "testStartOnly"_sr, false);
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
		self->updateBoundaries();
		return Void();
	}

	// according to boundaries, generate valid ranges for moveKeys operation
	KeyRange getRandomKeys() const {
		// merge or split operations
		Key begin, end;
		if (deterministicRandom()->coinflip()) {
			// pure move
			if (boundaries.size() == 2) {
				begin = *boundaries.begin();
				end = *boundaries.rbegin();
			} else {
				// merge shard
				int a = deterministicRandom()->randomInt(0, boundaries.size() - 1);
				int b = deterministicRandom()->randomInt(a + 1, boundaries.size());
				auto it = boundaries.begin();
				std::advance(it, a);
				begin = *it;
				std::advance(it, b - a);
				end = *it;
			}
		} else {
			// split
			double start = deterministicRandom()->random01() * this->maxKeyspace;
			begin = doubleToTestKey(start);
			auto it = boundaries.upper_bound(begin);
			ASSERT(it != boundaries.end()); // allKeys.end is larger than any random keys here

			double len = deterministicRandom()->random01() * (1 - maxKeyspace);
			end = doubleToTestKey(start + len);
			if (end > *it || deterministicRandom()->coinflip()) {
				end = *it;
			}
		}

		return KeyRangeRef(begin, end);
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

	void updateBoundaries() {
		boundaries.clear();
		for (auto& shard : realInitDD->shards) {
			boundaries.insert(boundaries.end(), shard.key);
		}
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

		wait(timeout(reportErrors(self->worker(cx, self), "IDDTxnProcessorApiWorkload"), self->testDuration, Void()));

		// Always set the DD mode back, even if we die with an error
		TraceEvent("IDDTxnApiTestDoneMoving").log();
		int oldValue = wait(setDDMode(cx, 1));
		TraceEvent("IDDTxnApiTestDoneModeSetting").detail("OldValue", oldValue);
		return Void();
	}

	void verifyServerKeyDest(MoveKeysParams& params) const {
		KeyRangeRef keys;
		if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
			ASSERT(params.ranges.present());
			// TODO: make startMoveShards work with multiple ranges.
			ASSERT(params.ranges.get().size() == 1);
			keys = params.ranges.get().at(0);
		} else {
			ASSERT(params.keys.present());
			keys = params.keys.get();
		}

		// check destination servers
		for (auto& id : params.destinationTeam) {
			ASSERT(mgs->serverIsDestForShard(id, keys));
		}
	}
	ACTOR static Future<Void> testRawMovementApi(IDDTxnProcessorApiWorkload* self) {
		state TraceInterval relocateShardInterval("RelocateShard_TestRawMovementApi");
		state FlowLock fl1(1);
		state FlowLock fl2(1);
		state std::map<UID, StorageServerInterface> emptyTssMapping;
		state Reference<InitialDataDistribution> mockInitData;
		state MoveKeysParams params = wait(generateMoveKeysParams(self));
		params.startMoveKeysParallelismLock = &fl1;
		params.finishMoveKeysParallelismLock = &fl2;
		params.relocationIntervalId = relocateShardInterval.pairID;
		TraceEvent(SevDebug, relocateShardInterval.begin(), relocateShardInterval.pairID)
		    .detail("Key", params.keys)
		    .detail("Dest", params.destinationTeam);

		loop {
			params.dataMovementComplete.reset();
			wait(store(params.lock, self->real->takeMoveKeysLock(UID())));
			try {
				// test start
				wait(self->mock->testRawStartMovement(params, emptyTssMapping));
				wait(self->real->testRawStartMovement(params, emptyTssMapping));

				self->verifyServerKeyDest(params);
				// test finish or started but cancelled movement
				if (self->testStartOnly || deterministicRandom()->coinflip()) {
					CODE_PROBE(true, "RawMovementApi partial started", probe::decoration::rare);
					break;
				}

				wait(self->mock->testRawFinishMovement(params, emptyTssMapping));
				wait(self->real->testRawFinishMovement(params, emptyTssMapping));
				break;
			} catch (Error& e) {
				if (e.code() != error_code_movekeys_conflict)
					throw;
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				// Keep trying to get the moveKeysLock
			}
		}

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
		TraceEvent(SevDebug, relocateShardInterval.end(), relocateShardInterval.pairID);
		// The simulator have chances generating a scenario when after the first setupMockGlobalState call, there is a
		// new storage server join the cluster, there's no way for mock DD to know the new storage server without
		// calling setupMockGlobalState again.
		self->mock->setupMockGlobalState(self->realInitDD);
		return Void();
	}

	ACTOR static Future<MoveKeysParams> generateMoveKeysParams(IDDTxnProcessorApiWorkload* self) { // always empty
		state MoveKeysLock lock = wait(takeMoveKeysLock(self->real->context(), UID()));

		KeyRange keys = self->getRandomKeys();
		std::vector<UID> destTeam = self->getRandomTeam();
		std::sort(destTeam.begin(), destTeam.end());
		return MoveKeysParams(deterministicRandom()->randomUniqueID(),
		                      keys,
		                      destTeam,
		                      destTeam,
		                      lock,
		                      Promise<Void>(),
		                      nullptr,
		                      nullptr,
		                      false,
		                      UID(),
		                      self->ddContext.ddEnabledState.get(),
		                      CancelConflictingDataMoves::True);
	}

	ACTOR static Future<Void> testMoveKeys(IDDTxnProcessorApiWorkload* self) {
		state TraceInterval relocateShardInterval("RelocateShard_TestMoveKeys");
		state FlowLock fl1(1);
		state FlowLock fl2(1);
		state std::map<UID, StorageServerInterface> emptyTssMapping;
		state Reference<InitialDataDistribution> mockInitData;
		state MoveKeysParams params = wait(generateMoveKeysParams(self));
		params.startMoveKeysParallelismLock = &fl1;
		params.finishMoveKeysParallelismLock = &fl2;
		params.relocationIntervalId = relocateShardInterval.pairID;
		TraceEvent(SevDebug, relocateShardInterval.begin(), relocateShardInterval.pairID)
		    .detail("Key", params.keys)
		    .detail("Dest", params.destinationTeam);

		loop {
			params.dataMovementComplete.reset();
			wait(store(params.lock, self->real->takeMoveKeysLock(UID())));
			try {
				wait(self->mock->moveKeys(params));
				wait(self->real->moveKeys(params));
				break;
			} catch (Error& e) {
				if (e.code() != error_code_movekeys_conflict)
					throw;
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				// Keep trying to get the moveKeysLock
			}
		}

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
		TraceEvent(SevDebug, relocateShardInterval.end(), relocateShardInterval.pairID);
		self->mock->setupMockGlobalState(self->realInitDD); // in case SS remove or recruit
		return Void();
	}

	ACTOR Future<Void> worker(Database cx, IDDTxnProcessorApiWorkload* self) {
		state double lastTime = now();
		state int choice = 0;
		state int maxChoice = self->testStartOnly ? 1 : 2;
		loop {
			choice = deterministicRandom()->randomInt(0, maxChoice);
			if (choice == 0) { // test rawStartMovement and rawFinishMovement separately
				wait(testRawMovementApi(self));
			} else if (choice == 1) { // test moveKeys
				wait(testMoveKeys(self));
			} else {
				ASSERT(false);
			}
			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
		}
	}

	Future<bool> check(Database const& cx) override {
		return tag(delay(testDuration / 2), true);
	} // Give the database time to recover from our damage

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<IDDTxnProcessorApiWorkload> IDDTxnProcessorApiWorkload;
