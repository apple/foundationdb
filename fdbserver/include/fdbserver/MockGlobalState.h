/*
 * MockGlobalState.h
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

#ifndef FOUNDATIONDB_MOCKGLOBALSTATE_H
#define FOUNDATIONDB_MOCKGLOBALSTATE_H

#include "StorageMetrics.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "SimulatedCluster.h"
#include "ShardsAffectedByTeamFailure.h"

template <class Metric>
struct ShardSizeMetric {
	template <typename pair_type>
	Metric operator()(pair_type const& p) const {
		return Metric(p.value.shardSize);
	}
};

enum class MockShardStatus { EMPTY = 0, COMPLETED, INFLIGHT };

class MockStorageServer {
public:
	struct ShardInfo {
		MockShardStatus status;
		uint64_t shardSize;
	};

	static constexpr uint64_t DEFAULT_DISK_SPACE = 1000LL * 1024 * 1024 * 1024;

	// control plane statistics associated with a real storage server
	uint64_t usedDiskSpace = 0, availableDiskSpace = DEFAULT_DISK_SPACE;

	// In-memory counterpart of the `serverKeys` in system keyspace
	// the value ShardStatus is [InFlight, Completed, Empty] and metrics uint64_t is the shard size
	KeyRangeMap<ShardInfo, uint64_t, ShardSizeMetric<uint64_t>> serverKeys;

	// sampled metrics
	StorageServerMetrics metrics;
	CoalescedKeyRangeMap<bool, int64_t, KeyBytesMetric<int64_t>> byteSampleClears;

	StorageServerInterface ssi; // serve RPC requests
	UID id;
	bool primary = true; // Only support single region MGS for now

	MockStorageServer() = default;

	MockStorageServer(StorageServerInterface ssi, uint64_t availableDiskSpace, uint64_t usedDiskSpace = 0)
	  : usedDiskSpace(usedDiskSpace), availableDiskSpace(availableDiskSpace), ssi(ssi), id(ssi.id()) {}

	MockStorageServer(const UID& id, uint64_t availableDiskSpace, uint64_t usedDiskSpace = 0)
	  : MockStorageServer(StorageServerInterface(id), availableDiskSpace, usedDiskSpace) {}

	decltype(serverKeys)::Ranges getAllRanges() { return serverKeys.ranges(); }

	bool allShardStatusEqual(KeyRangeRef range, MockShardStatus status);
};

class MockGlobalState {
public:
	typedef ShardsAffectedByTeamFailure::Team Team;
	// In-memory counterpart of the `keyServers` in system keyspace
	Reference<ShardsAffectedByTeamFailure> shardMapping;
	// In-memory counterpart of the `serverListKeys` in system keyspace
	std::map<UID, MockStorageServer> allServers;
	DatabaseConfiguration configuration;

	// user defined parameters for mock workload purpose
	double emptyProb; // probability of doing an empty read
	uint32_t minByteSize, maxByteSize; // the size band of a point data operation

	MockGlobalState() : shardMapping(new ShardsAffectedByTeamFailure) {}

	static UID indexToUID(uint64_t a) { return UID(a, a); }
	void initializeAsEmptyDatabaseMGS(const DatabaseConfiguration& conf,
	                                  uint64_t defaultDiskSpace = MockStorageServer::DEFAULT_DISK_SPACE);

	void addStorageServer(StorageServerInterface server, uint64_t diskSpace = MockStorageServer::DEFAULT_DISK_SPACE);

	// check methods
	/* Shard status contract:
	 * Shard is static.
	 * * In mgs.shardMapping, the destination teams is empty for the given shard;
	 * * For each MSS belonging to the source teams, mss.serverKeys[shard] = Completed
	 * Shard is in-flight.
	 * * In mgs.shardMapping,the destination teams is non-empty for a given shard;
	 * * For each MSS belonging to the source teams, mss.serverKeys[shard] = Completed
	 * * For each MSS belonging to the destination teams, mss.serverKeys[shard] = InFlight
	 * Shard is lost.
	 * * In mgs.shardMapping,  the destination teams is empty for the given shard;
	 * * For each MSS belonging to the source teams, mss.serverKeys[shard] = Empty
	 */
	bool serverIsSourceForShard(const UID& serverId, KeyRangeRef shard, bool inFlightShard = false);
	bool serverIsDestForShard(const UID& serverId, KeyRangeRef shard);

	/* Server status contract:
	 * Server X  is removed
	 * * mgs.shardMapping doesn’t have any information about X
	 * * mgs.allServer doesn’t contain X
	 * Server X is healthy
	 * * mgs.allServer[X] is existed
	 * Server X is failed but haven’t been removed (a temporary status between healthy and removed)
	 * * mgs.shardMapping doesn’t have any information about X
	 * * mgs.allServer[X] is existed
	 */
	bool allShardRemovedFromServer(const UID& serverId);
};

#endif // FOUNDATIONDB_MOCKGLOBALSTATE_H
