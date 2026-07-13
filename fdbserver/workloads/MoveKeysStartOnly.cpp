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
	  : TestWorkload(wcx), enabled(!clientId && g_network->isSimulated()),
	    testDuration(getOption(options, "testDuration"_sr, 50.0)) {}

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
	enum class MoveShape { Split, Aligned, Merge };

	bool enabled;
	double testDuration;
	int oldDDMode = 1;
	DDEnabledState ddEnabledState;
	DatabaseConfiguration configuration;
	Reference<InitialDataDistribution> initialData;

	Future<Void> readInitialDataDistribution(Database cx) {
		Reference<IDDTxnProcessor> processor = makeReference<DDTxnProcessor>(cx);
		while (true) {
			MoveKeysLock lock = co_await readMoveKeysLock(cx);
			try {
				initialData = co_await processor->getInitialDataDistribution(
				    UID(), lock, {}, &ddEnabledState, SkipDDModeCheck::True);
				co_return;
			} catch (Error& e) {
				if (e.code() != error_code_movekeys_conflict) {
					throw;
				}
			}
			co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
		}
	}

	Future<Void> startImpl(Database cx) {
		oldDDMode = co_await setDDMode(cx, 0);
		Error error;
		try {
			co_await timeoutError(startAndVerify(cx), testDuration);
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
		Reference<IDDTxnProcessor> processor = makeReference<DDTxnProcessor>(cx);
		configuration = co_await processor->getDatabaseConfiguration();
		ASSERT_GT(configuration.storageTeamSize, 0);
		ASSERT_EQ(configuration.usableRegions, 1);
		co_await readInitialDataDistribution(cx);

		co_await moveAndVerify(cx, MoveShape::Split, false);
		co_await moveAndVerify(cx, MoveShape::Aligned, false);
		co_await moveAndVerify(cx, MoveShape::Merge, true);
	}

	KeyRange getMoveRange(MoveShape shape) const {
		ASSERT_GE(initialData->shards.size(), 2);
		const int shardCount = initialData->shards.size() - 1;

		if (shape == MoveShape::Split) {
			const Key begin = doubleToTestKey(deterministicRandom()->random01());
			auto end = std::upper_bound(initialData->shards.begin(),
			                            initialData->shards.end(),
			                            begin,
			                            [](const Key& key, const DDShardInfo& shard) { return key < shard.key; });
			ASSERT(end != initialData->shards.end());
			return KeyRangeRef(begin, end->key);
		}

		if (shape == MoveShape::Aligned) {
			const int shard = deterministicRandom()->randomInt(0, shardCount);
			return KeyRangeRef(initialData->shards[shard].key, initialData->shards[shard + 1].key);
		}

		ASSERT_GE(shardCount, 2);
		const int begin = deterministicRandom()->randomInt(0, shardCount - 1);
		const int end = deterministicRandom()->randomInt(begin + 2, shardCount + 1);
		return KeyRangeRef(initialData->shards[begin].key, initialData->shards[end].key);
	}

	Future<Void> moveAndVerify(Database cx, MoveShape shape, bool finishMovement) {
		ASSERT_GE(initialData->allServers.size(), configuration.storageTeamSize);

		const KeyRange keys = getMoveRange(shape);

		auto candidates = initialData->allServers;
		deterministicRandom()->randomShuffle(candidates, 0, configuration.storageTeamSize);
		std::vector<UID> destinationTeam;
		destinationTeam.reserve(configuration.storageTeamSize);
		for (int i = 0; i < configuration.storageTeamSize; ++i) {
			destinationTeam.push_back(candidates[i].first.id());
		}
		std::sort(destinationTeam.begin(), destinationTeam.end());

		// Conflicting encoded data moves are cleaned up while startMoveShards holds one permit.
		FlowLock startMoveKeysLock(2);
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
		while (true) {
			try {
				if (finishMovement) {
					co_await moveKeys(cx, params);
				} else {
					co_await rawStartMovement(cx, params, tssMapping);
				}
				break;
			} catch (Error& e) {
				if (e.code() != error_code_movekeys_conflict &&
				    (!finishMovement || e.code() != error_code_finish_move_keys_too_many_retries)) {
					throw;
				}
			}
			co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
			params.dataMovementComplete.reset();
			params.lock = co_await takeMoveKeysLock(cx, UID());
		}
		co_await readInitialDataDistribution(cx);

		if (!finishMovement) {
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
		}

		bool found = false;
		for (int shard = 0; shard < initialData->shards.size() - 1; ++shard) {
			const KeyRangeRef shardRange(initialData->shards[shard].key, initialData->shards[shard + 1].key);
			if (shardRange.end <= keys.begin || shardRange.begin >= keys.end) {
				continue;
			}
			found = true;
			const DDShardInfo& info = initialData->shards[shard];
			if (finishMovement) {
				ASSERT(!info.hasDest);
				ASSERT(info.primarySrc == destinationTeam);
				ASSERT(info.primaryDest.empty());
			} else {
				ASSERT(info.hasDest);
				ASSERT(info.primaryDest == destinationTeam);
				ASSERT(!info.primarySrc.empty());
			}
			ASSERT(info.remoteSrc.empty());
			ASSERT(info.remoteDest.empty());
		}
		ASSERT(found);
	}
};

WorkloadFactory<MoveKeysStartOnlyWorkload> MoveKeysStartOnlyWorkloadFactory;
