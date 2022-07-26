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

class MockStorageServer {
public:
	// control plane statistics associated with a real storage server
	uint64_t usedDiskSpace = 0, availableDiskSpace;
	KeyRangeMap<KeyRange, uint64_t> shardTotalBytes; // randomly generated in setup phase

	// sampled metrics
	StorageServerMetrics metrics;
	CoalescedKeyRangeMap<bool, int64_t, KeyBytesMetric<int64_t>> byteSampleClears;

	StorageServerInterface ssi; // serve RPC requests
	UID id;

	MockStorageServer() = default;
	MockStorageServer(const UID& id, uint64_t availableDiskSpace, uint64_t usedDiskSpace = 0)
	  : usedDiskSpace(usedDiskSpace), availableDiskSpace(availableDiskSpace), id(id) {
		ssi.uniqueID = id;
	}
};

class MockGlobalState {
public:
	// Index starting from 1. 0 indicates invalid index;
	typedef uint32_t TeamIndex;
	typedef uint64_t ServerIndex;
	struct Team {
		TeamIndex teamIdx;
		std::vector<ServerIndex> serverIdx;

		std::vector<UID> getServerIds() const;
	};

	struct ShardTeamValue {
		TeamIndex srcIdx;
		Optional<TeamIndex> destIdx;
	};

	std::map<Key, ShardTeamValue> keyServers; // a shard belongs to which teams, key is the beginning key of a shard
	std::map<ServerIndex, MockStorageServer> servers; // all mock servers
	std::map<TeamIndex, Team> teams;
	DatabaseConfiguration configuration;

	// user defined parameters for mock workload purpose
	double emptyProb; // probability of doing an empty read
	uint32_t minByteSize, maxByteSize; // the size band of a point data operation

	static UID indexToUID(uint64_t a) { return UID(a, a); }
	void initialAsEmptyDatabaseMGS(const DatabaseConfiguration& conf,
	                               uint64_t defaultDiskSpace = 1000LL * 1024 * 1024 * 1024);
};

#endif // FOUNDATIONDB_MOCKGLOBALSTATE_H
