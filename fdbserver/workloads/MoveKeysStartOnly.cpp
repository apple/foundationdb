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
#include <utility>
#include <vector>

#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Replication.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/MoveKeys.h"
#include "fdbserver/core/ServerDBInfo.h"
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
	struct ShardInfo {
		Key key;
		std::vector<UID> source;
		std::vector<UID> destination;
	};
	struct DataDistributionSnapshot {
		std::vector<std::pair<StorageServerInterface, ProcessClass>> allServers;
		std::vector<ShardInfo> shards;
	};

	bool enabled;
	double testDuration;
	int oldDDMode = 1;
	DDEnabledState ddEnabledState;
	DatabaseConfiguration configuration;
	DataDistributionSnapshot initialData;

	Future<Void> readDataDistributionSnapshot(Database cx, MoveKeysLock lock) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				co_await checkMoveKeysLockReadOnly(&tr, lock, &ddEnabledState);

				DataDistributionSnapshot snapshot;
				snapshot.allServers = co_await NativeAPI::getServerListAndProcessClasses(&tr);
				RangeResult serverTags = co_await tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY);
				ASSERT(!serverTags.more && serverTags.size() < CLIENT_KNOBS->TOO_MANY);
				RangeResult keyServers = co_await krmGetRanges(
				    &tr, keyServersPrefix, allKeys, CLIENT_KNOBS->TOO_MANY, CLIENT_KNOBS->TOO_MANY);
				ASSERT(!keyServers.empty() && !keyServers.more);

				for (int i = 0; i < keyServers.size() - 1; ++i) {
					std::vector<UID> source;
					std::vector<UID> destination;
					UID sourceId, destinationId;
					decodeKeyServersValue(
					    serverTags, keyServers[i].value, source, destination, sourceId, destinationId);
					snapshot.shards.push_back({ keyServers[i].key, std::move(source), std::move(destination) });
				}
				snapshot.shards.push_back({ allKeys.end, {}, {} });
				initialData = std::move(snapshot);
				co_return;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> readInitialDataDistribution(Database cx) {
		while (true) {
			MoveKeysLock lock = co_await readMoveKeysLock(cx);
			try {
				co_await readDataDistributionSnapshot(cx, lock);
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
		configuration = co_await getDatabaseConfiguration(cx);
		ASSERT_GT(configuration.storageTeamSize, 0);
		ASSERT_EQ(configuration.usableRegions, 1);
		co_await readInitialDataDistribution(cx);

		co_await moveAndVerify(cx, MoveShape::Split, false);
		co_await moveAndVerify(cx, MoveShape::Aligned, false);
		co_await moveAndVerify(cx, MoveShape::Merge, true);
	}

	KeyRange getMoveRange(MoveShape shape) const {
		ASSERT_GE(initialData.shards.size(), 2);
		const int shardCount = initialData.shards.size() - 1;

		if (shape == MoveShape::Split) {
			const Key begin = doubleToTestKey(deterministicRandom()->random01());
			auto end = std::upper_bound(initialData.shards.begin(),
			                            initialData.shards.end(),
			                            begin,
			                            [](const Key& key, const ShardInfo& shard) { return key < shard.key; });
			ASSERT(end != initialData.shards.end());
			return KeyRangeRef(begin, end->key);
		}

		if (shape == MoveShape::Aligned) {
			const int shard = deterministicRandom()->randomInt(0, shardCount);
			return KeyRangeRef(initialData.shards[shard].key, initialData.shards[shard + 1].key);
		}

		ASSERT_GE(shardCount, 2);
		const int begin = deterministicRandom()->randomInt(0, shardCount - 1);
		const int end = deterministicRandom()->randomInt(begin + 2, shardCount + 1);
		return KeyRangeRef(initialData.shards[begin].key, initialData.shards[end].key);
	}

	Future<Void> moveAndVerify(Database cx, MoveShape shape, bool finishMovement) {
		ASSERT_GE(initialData.allServers.size(), configuration.storageTeamSize);

		const KeyRange keys = getMoveRange(shape);

		Optional<Key> primaryDcId;
		if (!configuration.regions.empty()) {
			primaryDcId = dbInfo->get().master.locality.dcId();
			auto activeRegion =
			    std::find_if(configuration.regions.begin(), configuration.regions.end(), [&](const auto& region) {
				    return primaryDcId.present() && region.dcId == primaryDcId.get();
			    });
			if (activeRegion == configuration.regions.end()) {
				primaryDcId = configuration.regions.front().dcId;
			}
		}

		LocalityMap<StorageServerInterface> candidates;
		for (auto& [server, _] : initialData.allServers) {
			if ((primaryDcId.present() && server.locality.dcId() != primaryDcId) ||
			    configuration.isExcludedServer(server.getValue.getEndpoint().addresses, server.locality) ||
			    !IFailureMonitor::failureMonitor().getState(server.waitFailure.getEndpoint()).isAvailable()) {
				continue;
			}
			candidates.add(server.locality, &server);
		}

		std::vector<StorageServerInterface*> selectedServers;
		ASSERT(candidates.selectReplicas(configuration.storagePolicy, selectedServers));
		ASSERT_EQ(selectedServers.size(), configuration.storageTeamSize);
		std::vector<UID> destinationTeam;
		destinationTeam.reserve(configuration.storageTeamSize);
		for (auto* server : selectedServers) {
			destinationTeam.push_back(server->id());
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
			auto first = std::lower_bound(initialData.shards.begin(),
			                              initialData.shards.end(),
			                              keys.begin,
			                              [](const ShardInfo& shard, const Key& key) { return shard.key < key; });
			auto last = std::lower_bound(initialData.shards.begin(),
			                             initialData.shards.end(),
			                             keys.end,
			                             [](const ShardInfo& shard, const Key& key) { return shard.key < key; });
			ASSERT(first != initialData.shards.end() && first->key == keys.begin);
			ASSERT(last != initialData.shards.end() && last->key == keys.end);
			ASSERT(first != last);
		}

		bool found = false;
		for (int shard = 0; shard < initialData.shards.size() - 1; ++shard) {
			const KeyRangeRef shardRange(initialData.shards[shard].key, initialData.shards[shard + 1].key);
			if (shardRange.end <= keys.begin || shardRange.begin >= keys.end) {
				continue;
			}
			found = true;
			const ShardInfo& info = initialData.shards[shard];
			if (finishMovement) {
				ASSERT(info.destination.empty());
				ASSERT(info.source == destinationTeam);
			} else {
				ASSERT(info.destination == destinationTeam);
				ASSERT(!info.source.empty());
			}
		}
		ASSERT(found);
	}
};

WorkloadFactory<MoveKeysStartOnlyWorkload> MoveKeysStartOnlyWorkloadFactory;
