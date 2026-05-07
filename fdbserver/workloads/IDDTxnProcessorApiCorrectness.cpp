/*
 * IDDTxnProcessorApiCorrectness.cpp
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
#include "fdbserver/datadistributor/DDSharedContext.h"
#include "fdbserver/datadistributor/DDTxnProcessor.h"
#include "fdbserver/core/MoveKeys.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/core/Knobs.h"
#include "fdbclient/VersionedMap.h"

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
		std::cout << a.key.printable() << " | " << other.key.printable() << "\n";
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
		for (auto& id : params.destinationTeam) {
			mgs->allServers.at(id)->setShardStatus(params.keys.get(), MockShardStatus::FETCHED);
		}
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
	DDSharedContext ddContext;

	std::shared_ptr<DDTxnProcessorTester> real;
	std::shared_ptr<MockGlobalState> mgs;
	std::shared_ptr<DDMockTxnProcessorTester> mock;

	Reference<InitialDataDistribution> realInitDD;
	std::vector<Key> boundaries;

	// counters
	int testRawStart = 0, testAll = 0, testRawFinish = 0;

	explicit IDDTxnProcessorApiWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), ddContext(UID()) {
		enabled = !clientId && g_network->isSimulated(); // only do this on the "first" client
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		testStartOnly = getOption(options, "testStartOnly"_sr, false);
	}

	Future<Void> setup(Database const& cx) override { return enabled ? _setup(cx, this) : Void(); }
	Future<Void> start(Database const& cx) override { return enabled ? _start(cx) : Void(); }

	// This workload is not compatible with RandomMoveKeys workload because they will race in changing the DD mode.
	// Other workload injections may make no sense because this workload only use the DB at beginning to reading the
	// real world key-server mappings. It's not harmful to leave other workload injection enabled for now, though.
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomMoveKeys", "Attrition" });
	}

	Future<Void> readRealInitialDataDistribution() {
		while (true) {
			ddContext.lock = co_await ::readMoveKeysLock(real->context());
			// read real InitialDataDistribution
			try {
				realInitDD = co_await real->getInitialDataDistribution(
				    ddContext.id(), ddContext.lock, {}, ddContext.ddEnabledState.get(), SkipDDModeCheck::True);
				std::cout << "Finish read real InitialDataDistribution: server size " << realInitDD->allServers.size()
				          << ", shard size: " << realInitDD->shards.size() << std::endl;
				break;
			} catch (Error& e) {
				if (e.code() != error_code_movekeys_conflict)
					throw;
			}
		}
		updateBoundaries();
	}

	// according to boundaries, generate valid ranges for moveKeys operation
	KeyRange getRandomKeys() const {
		int choice = deterministicRandom()->randomInt(0, 3);
		// merge or split operations
		Key begin, end;
		if (choice == 0) {
			// pure move
			int idx = deterministicRandom()->randomInt(0, boundaries.size() - 1);
			begin = boundaries[idx];
			end = boundaries[idx + 1];
		} else if (choice == 1) {
			// merge shard
			int a = deterministicRandom()->randomInt(0, boundaries.size() - 1);
			int b = deterministicRandom()->randomInt(a + 1, boundaries.size());
			begin = boundaries[a];
			end = boundaries[b];
		} else {
			// split
			double start = deterministicRandom()->random01();
			begin = doubleToTestKey(start);
			auto it = std::upper_bound(boundaries.begin(), boundaries.end(), begin);
			ASSERT(it != boundaries.end()); // allKeys.end is larger than any random keys here
			end = *it;
		}

		return KeyRangeRef(begin, end);
	}

	std::vector<UID> getRandomTeam() {
		int& teamSize = ddContext.configuration.storageTeamSize;
		if (realInitDD->allServers.size() < teamSize) {
			TraceEvent(SevWarnAlways, "CandidatesLessThanTeamSize").log();
			throw operation_failed();
		}
		deterministicRandom()->randomShuffle(realInitDD->allServers, 0, teamSize);
		std::vector<UID> result(teamSize);
		for (int i = 0; i < teamSize; ++i) {
			result[i] = realInitDD->allServers[i].first.id();
		}
		return result;
	}

	void updateBoundaries() {
		boundaries.clear();
		std::set<Key> tempBoundaries;
		for (auto& shard : realInitDD->shards) {
			tempBoundaries.insert(tempBoundaries.end(), shard.key);
		}
		boundaries = std::vector<Key>(tempBoundaries.begin(), tempBoundaries.end());
	}

	Future<Void> _setup(Database cx, IDDTxnProcessorApiWorkload* self) {
		int oldMode = co_await setDDMode(cx, 0);
		TraceEvent("IDDTxnApiTestStartModeSetting").detail("OldValue", oldMode).log();

		self->real = std::make_shared<DDTxnProcessorTester>(cx);
		// Get the database configuration so as to use proper team size
		self->ddContext.configuration = co_await self->real->getDatabaseConfiguration();
		ASSERT(self->ddContext.configuration.storageTeamSize > 0);
		// FIXME: add support for generating random teams across DCs
		ASSERT_EQ(self->ddContext.usableRegions(), 1);
		co_await readRealInitialDataDistribution();
	}

	Future<Void> _start(Database cx) {

		mgs = std::make_shared<MockGlobalState>();
		mgs->configuration = ddContext.configuration;
		mgs->restrictSize = false; // no need to check the validity of shard size

		mock = std::make_shared<DDMockTxnProcessorTester>(mgs);
		mock->setupMockGlobalState(realInitDD);

		Reference<InitialDataDistribution> mockInitData =
		    mock->getInitialDataDistribution(
		            ddContext.id(), ddContext.lock, {}, ddContext.ddEnabledState.get(), SkipDDModeCheck::True)
		        .get();

		verifyInitDataEqual(realInitDD, mockInitData);

		co_await timeout(reportErrors(worker(cx, this), "IDDTxnProcessorApiWorkload"), testDuration, Void());

		// Always set the DD mode back, even if we die with an error
		TraceEvent("IDDTxnApiTestDoneMoving").log();
		int oldValue = co_await setDDMode(cx, 1);
		TraceEvent("IDDTxnApiTestDoneModeSetting").detail("OldValue", oldValue);
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
	Future<Void> testRawMovementApi() {
		TraceInterval relocateShardInterval("RelocateShard_TestRawMovementApi");
		FlowLock fl1(1);
		FlowLock fl2(1);
		std::map<UID, StorageServerInterface> emptyTssMapping;
		Reference<InitialDataDistribution> mockInitData;
		MoveKeysParams realParams = co_await generateMoveKeysParams();
		realParams.startMoveKeysParallelismLock = &fl1;
		realParams.finishMoveKeysParallelismLock = &fl2;
		realParams.relocationIntervalId = relocateShardInterval.pairID;
		TraceEvent(SevDebug, relocateShardInterval.begin(), relocateShardInterval.pairID)
		    .detail("Key", realParams.keys)
		    .detail("Dest", realParams.destinationTeam);

		MoveKeysParams mockParams = realParams;
		mockParams.dataMovementComplete = Promise<Void>();

		while (true) {
			realParams.dataMovementComplete.reset();
			mockParams.dataMovementComplete.reset();

			realParams.lock = co_await real->takeMoveKeysLock(UID());
			Error err;
			try {
				// test start
				co_await mock->testRawStartMovement(mockParams, emptyTssMapping);
				co_await real->testRawStartMovement(realParams, emptyTssMapping);

				verifyServerKeyDest(realParams);
				// test finish or started but cancelled movement
				if (testStartOnly || deterministicRandom()->coinflip()) {
					CODE_PROBE(true, "RawMovementApi partial started");
					testRawStart++;
					break;
				}

				// The real transaction should finish first because mock transaction will always success.
				co_await real->testRawFinishMovement(realParams, emptyTssMapping);
				co_await mock->testRawFinishMovement(mockParams, emptyTssMapping);
				testRawFinish++;
				break;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() != error_code_movekeys_conflict)
				throw err;
			co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
			// Keep trying to get the moveKeysLock
		}

		// read initial data again
		co_await readRealInitialDataDistribution();
		mockInitData =
		    mock->getInitialDataDistribution(
		            ddContext.id(), ddContext.lock, {}, ddContext.ddEnabledState.get(), SkipDDModeCheck::True)
		        .get();

		verifyInitDataEqual(realInitDD, mockInitData);
		TraceEvent(SevDebug, relocateShardInterval.end(), relocateShardInterval.pairID);
		// The simulator have chances generating a scenario when after the first setupMockGlobalState call, there is a
		// new storage server join the cluster, there's no way for mock DD to know the new storage server without
		// calling setupMockGlobalState again.
		mock->setupMockGlobalState(realInitDD);
	}

	Future<MoveKeysParams> generateMoveKeysParams() { // always empty
		MoveKeysLock lock = co_await takeMoveKeysLock(real->context(), UID());

		KeyRange keys = getRandomKeys();
		std::vector<UID> destTeam = getRandomTeam();
		std::sort(destTeam.begin(), destTeam.end());
		const UID dataMoveId = newDataMoveId(deterministicRandom()->randomUInt64(),
		                                     AssignEmptyRange(false),
		                                     DataMoveType::LOGICAL,
		                                     DataMovementReason::INVALID,
		                                     UnassignShard(false));
		if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
			co_return MoveKeysParams(dataMoveId,
			                         std::vector<KeyRange>{ keys },
			                         destTeam,
			                         destTeam,
			                         lock,
			                         Promise<Void>(),
			                         nullptr,
			                         nullptr,
			                         false,
			                         UID(),
			                         ddContext.ddEnabledState.get(),
			                         CancelConflictingDataMoves::True,
			                         Optional<BulkLoadTaskState>());
		} else {
			co_return MoveKeysParams(dataMoveId,
			                         keys,
			                         destTeam,
			                         destTeam,
			                         lock,
			                         Promise<Void>(),
			                         nullptr,
			                         nullptr,
			                         false,
			                         UID(),
			                         ddContext.ddEnabledState.get(),
			                         CancelConflictingDataMoves::True,
			                         Optional<BulkLoadTaskState>());
		}
	}

	Future<Void> testMoveKeys() {
		TraceInterval relocateShardInterval("RelocateShard_TestMoveKeys");
		FlowLock fl1(1);
		FlowLock fl2(1);
		std::map<UID, StorageServerInterface> emptyTssMapping;
		Reference<InitialDataDistribution> mockInitData;
		MoveKeysParams realParams = co_await generateMoveKeysParams();
		realParams.startMoveKeysParallelismLock = &fl1;
		realParams.finishMoveKeysParallelismLock = &fl2;
		realParams.relocationIntervalId = relocateShardInterval.pairID;
		TraceEvent(SevDebug, relocateShardInterval.begin(), relocateShardInterval.pairID)
		    .detail("Key", realParams.keys)
		    .detail("Dest", realParams.destinationTeam);

		MoveKeysParams mockParams = realParams;
		mockParams.dataMovementComplete = Promise<Void>();

		while (true) {
			realParams.dataMovementComplete.reset();
			mockParams.dataMovementComplete.reset();
			realParams.lock = co_await real->takeMoveKeysLock(UID());
			Error err;
			try {
				co_await mock->moveKeys(mockParams);
				co_await real->moveKeys(realParams);
				testAll++;
				break;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() != error_code_movekeys_conflict)
				throw err;
			co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
			// Keep trying to get the moveKeysLock
		}

		// read initial data again
		co_await readRealInitialDataDistribution();
		mockInitData =
		    mock->getInitialDataDistribution(
		            ddContext.id(), ddContext.lock, {}, ddContext.ddEnabledState.get(), SkipDDModeCheck::True)
		        .get();

		verifyInitDataEqual(realInitDD, mockInitData);
		TraceEvent(SevDebug, relocateShardInterval.end(), relocateShardInterval.pairID);
		mock->setupMockGlobalState(realInitDD); // in case SS remove or recruit
	}

	Future<Void> worker(Database cx, IDDTxnProcessorApiWorkload* self) {
		int choice = 0;
		int maxChoice = self->testStartOnly ? 1 : 2;
		while (true) {
			choice = deterministicRandom()->randomInt(0, maxChoice);
			if (choice == 0) { // test rawStartMovement and rawFinishMovement separately
				co_await testRawMovementApi();
			} else if (choice == 1) { // test moveKeys
				co_await testMoveKeys();
			} else {
				ASSERT(false);
			}
			co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
		}
	}

	Future<bool> check(Database const& cx) override {
		return tag(delay(testDuration / 2), true);
	} // Give the database time to recover from our damage

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("TestRawStart", testRawStart, Averaged::False);
		m.emplace_back("TestRawFinish", testRawFinish, Averaged::False);
		m.emplace_back("TestRawAll", testAll, Averaged::False);
	}
};

WorkloadFactory<IDDTxnProcessorApiWorkload> IDDTxnProcessorApiWorkload;
