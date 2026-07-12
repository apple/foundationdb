/*
 * MoveKeysStartOnly.cpp
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

#include <algorithm>
#include <map>
#include <set>
#include <vector>

#include "fdbclient/ManagementAPI.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/MoveKeys.h"
#include "fdbserver/datadistributor/DataDistribution.h"
#include "fdbserver/datadistributor/DDTxnProcessor.h"
#include "fdbserver/tester/workloads.h"
#include "flow/DeterministicRandom.h"

class MoveKeysStartOnlyWorkload : public TestWorkload {
public:
	static constexpr auto NAME = "MoveKeysStartOnly";

	explicit MoveKeysStartOnlyWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), enabled(!clientId && g_network->isSimulated()) {}

	Future<Void> setup(Database const& cx) override {
		if (!enabled) {
			return Void();
		}
		return setupImpl(cx);
	}

	Future<Void> start(Database const& cx) override {
		if (!enabled) {
			return Void();
		}
		return startImpl(cx);
	}

	Future<bool> check(Database const& cx) override { return enabled ? tag(delay(5.0), true) : Future<bool>(true); }

	void getMetrics(std::vector<PerfMetric>& metrics) override {}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomMoveKeys", "Attrition" });
	}

private:
	bool enabled;
	int oldDDMode = 1;
	DDEnabledState ddEnabledState;
	DatabaseConfiguration configuration;
	Reference<InitialDataDistribution> initialData;

	Future<Void> setupImpl(Database cx) {
		oldDDMode = co_await setDDMode(cx, 0);
		Reference<IDDTxnProcessor> processor = makeReference<DDTxnProcessor>(cx);
		configuration = co_await processor->getDatabaseConfiguration();
		ASSERT_GT(configuration.storageTeamSize, 0);
		ASSERT_EQ(configuration.usableRegions, 1);
		co_await readInitialDataDistribution(cx);
	}

	Future<Void> readInitialDataDistribution(Database cx) {
		Reference<IDDTxnProcessor> processor = makeReference<DDTxnProcessor>(cx);
		MoveKeysLock lock = co_await readMoveKeysLock(cx);
		initialData =
		    co_await processor->getInitialDataDistribution(UID(), lock, {}, &ddEnabledState, SkipDDModeCheck::True);
	}

	Future<Void> startImpl(Database cx) {
		Error error;
		try {
			co_await startAndVerify(cx);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			error = e;
		}

		co_await setDDMode(cx, oldDDMode);
		if (error.isValid()) {
			throw error;
		}
	}

	Future<Void> startAndVerify(Database cx) {
		ASSERT_GE(initialData->shards.size(), 2);
		ASSERT_GE(initialData->allServers.size(), configuration.storageTeamSize);

		const Key begin = doubleToTestKey(deterministicRandom()->random01());
		auto end = std::upper_bound(initialData->shards.begin(),
		                            initialData->shards.end(),
		                            begin,
		                            [](const Key& key, const DDShardInfo& shard) { return key < shard.key; });
		ASSERT(end != initialData->shards.end());
		const KeyRange keys = KeyRangeRef(begin, end->key);

		auto candidates = initialData->allServers;
		deterministicRandom()->randomShuffle(candidates, 0, configuration.storageTeamSize);
		std::vector<UID> destinationTeam;
		destinationTeam.reserve(configuration.storageTeamSize);
		for (int i = 0; i < configuration.storageTeamSize; ++i) {
			destinationTeam.push_back(candidates[i].first.id());
		}
		std::sort(destinationTeam.begin(), destinationTeam.end());

		FlowLock startMoveKeysLock(1);
		FlowLock finishMoveKeysLock(1);
		MoveKeysLock lock = co_await takeMoveKeysLock(cx, UID());
		const UID dataMoveId = newDataMoveId(deterministicRandom()->randomUInt64(),
		                                     AssignEmptyRange(false),
		                                     DataMoveType::LOGICAL,
		                                     DataMovementReason::TEAM_HEALTHY,
		                                     UnassignShard(false));
		MoveKeysParams params;
		if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
			params = MoveKeysParams(dataMoveId,
			                        std::vector<KeyRange>{ keys },
			                        destinationTeam,
			                        destinationTeam,
			                        lock,
			                        Promise<Void>(),
			                        &startMoveKeysLock,
			                        &finishMoveKeysLock,
			                        false,
			                        UID(),
			                        &ddEnabledState,
			                        CancelConflictingDataMoves::True,
			                        Optional<BulkLoadTaskState>());
		} else {
			params = MoveKeysParams(dataMoveId,
			                        keys,
			                        destinationTeam,
			                        destinationTeam,
			                        lock,
			                        Promise<Void>(),
			                        &startMoveKeysLock,
			                        &finishMoveKeysLock,
			                        false,
			                        UID(),
			                        &ddEnabledState,
			                        CancelConflictingDataMoves::True,
			                        Optional<BulkLoadTaskState>());
		}

		std::map<UID, StorageServerInterface> tssMapping;
		co_await rawStartMovement(cx, params, tssMapping);
		co_await readInitialDataDistribution(cx);

		auto first = std::lower_bound(initialData->shards.begin(),
		                              initialData->shards.end(),
		                              keys.begin,
		                              [](const DDShardInfo& shard, const Key& key) { return shard.key < key; });
		auto last = std::lower_bound(initialData->shards.begin(),
		                             initialData->shards.end(),
		                             keys.end,
		                             [](const DDShardInfo& shard, const Key& key) { return shard.key < key; });
		ASSERT(first != initialData->shards.end() && first->key == keys.begin);
		ASSERT(last != initialData->shards.end() && last->key == keys.end);
		ASSERT(first != last);
		for (auto shard = first; shard != last; ++shard) {
			ASSERT(shard->hasDest);
			ASSERT(shard->primaryDest == destinationTeam);
			ASSERT(shard->remoteDest.empty());
			ASSERT(!shard->primarySrc.empty());
		}
	}
};

WorkloadFactory<MoveKeysStartOnlyWorkload> MoveKeysStartOnlyWorkloadFactory;
