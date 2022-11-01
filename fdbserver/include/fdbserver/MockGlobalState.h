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

#include "StorageMetrics.actor.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/KeyLocationService.h"
#include "SimulatedCluster.h"
#include "ShardsAffectedByTeamFailure.h"

struct MockGlobalStateTester;

// the status is roughly order by transition order, except for UNSET and EMPTY
enum class MockShardStatus {
	EMPTY = 0, // data loss
	UNSET,
	INFLIGHT,
	FETCHED, // finish fetch but not change the serverKey mapping. Only can be set by MSS itself.
	COMPLETED
};

inline bool isStatusTransitionValid(MockShardStatus from, MockShardStatus to) {
	if (from == to)
		return true;

	switch (from) {
	case MockShardStatus::UNSET:
	case MockShardStatus::EMPTY:
		return to >= MockShardStatus::INFLIGHT;
	case MockShardStatus::INFLIGHT:
		return to == MockShardStatus::FETCHED || to == MockShardStatus::EMPTY;
	case MockShardStatus::FETCHED:
		return to == MockShardStatus::COMPLETED;
	case MockShardStatus::COMPLETED:
		return to == MockShardStatus::EMPTY;
	default:
		ASSERT(false);
	}
	return false;
}

class MockStorageServerImpl;
class MockStorageServer : public IStorageMetricsService {
	friend struct MockGlobalStateTester;
	friend class MockStorageServerImpl;

	ActorCollection actors;

public:
	struct ShardInfo {
		MockShardStatus status;
		uint64_t shardSize;

		bool operator==(const ShardInfo& a) const { return shardSize == a.shardSize && status == a.status; }
		bool operator!=(const ShardInfo& a) const { return !(a == *this); }
	};

	struct FetchKeysParams {
		KeyRange keys;
		int64_t totalRangeBytes;
	};

	static constexpr uint64_t DEFAULT_DISK_SPACE = 1000LL * 1024 * 1024 * 1024;

	// control plane statistics associated with a real storage server
	uint64_t totalDiskSpace = DEFAULT_DISK_SPACE, usedDiskSpace = DEFAULT_DISK_SPACE;

	// In-memory counterpart of the `serverKeys` in system keyspace
	// the value ShardStatus is [InFlight, Completed, Empty] and metrics uint64_t is the shard size, the caveat is the
	// size() and nthRange() would use the metrics as index instead
	KeyRangeMap<ShardInfo> serverKeys;

	CoalescedKeyRangeMap<bool, int64_t, KeyBytesMetric<int64_t>> byteSampleClears;

	StorageServerInterface ssi; // serve RPC requests
	UID id;
	bool primary = true; // Only support single region MGS for now

	MockStorageServer() = default;

	MockStorageServer(StorageServerInterface ssi, uint64_t availableDiskSpace, uint64_t usedDiskSpace = 0)
	  : totalDiskSpace(usedDiskSpace + availableDiskSpace), usedDiskSpace(usedDiskSpace), ssi(ssi), id(ssi.id()) {}

	MockStorageServer(const UID& id, uint64_t availableDiskSpace, uint64_t usedDiskSpace = 0)
	  : MockStorageServer(StorageServerInterface(id), availableDiskSpace, usedDiskSpace) {}

	decltype(serverKeys)::Ranges getAllRanges() { return serverKeys.ranges(); }

	bool allShardStatusEqual(const KeyRangeRef& range, MockShardStatus status);
	bool allShardStatusIn(const KeyRangeRef& range, const std::set<MockShardStatus>& status);

	// change the status of range. This function may result in split to make the shard boundary align with range.begin
	// and range.end. In this case, if restrictSize==true, the sum of the split shard size is strictly equal to the old
	// large shard. Otherwise, the size are randomly generated between (min_shard_size, max_shard_size)
	void setShardStatus(const KeyRangeRef& range, MockShardStatus status, bool restrictSize);

	// this function removed an aligned range from server
	void removeShard(const KeyRangeRef& range);

	// intersecting range size
	uint64_t sumRangeSize(const KeyRangeRef& range) const;

	void addActor(Future<Void> future) override;

	void getSplitPoints(SplitRangeRequest const& req) override;

	Future<Void> waitMetricsTenantAware(const WaitMetricsRequest& req) override;

	void getStorageMetrics(const GetStorageMetricsRequest& req) override;

	template <class Reply>
	static constexpr bool isLoadBalancedReply = std::is_base_of_v<LoadBalancedReply, Reply>;

	template <class Reply>
	typename std::enable_if_t<isLoadBalancedReply<Reply>, void> sendErrorWithPenalty(const ReplyPromise<Reply>& promise,
	                                                                                 const Error& err,
	                                                                                 double penalty) {
		Reply reply;
		reply.error = err;
		reply.penalty = penalty;
		promise.send(reply);
	}

	template <class Reply>
	typename std::enable_if_t<!isLoadBalancedReply<Reply>, void>
	sendErrorWithPenalty(const ReplyPromise<Reply>& promise, const Error& err, double) {
		promise.sendError(err);
	}

	Future<Void> run();

	// data operation APIs - change the metrics sample, disk space and shard size

	// Set key with a new value, the total bytes change from oldBytes to bytes
	void set(KeyRef const& key, int64_t bytes, int64_t oldBytes);
	// Clear key and its value of which the size is bytes
	void clear(KeyRef const& key, int64_t bytes);
	// Clear range, assuming the first and last shard within the range having size `beginShardBytes` and `endShardBytes`
	// return the total range size
	int64_t clearRange(KeyRangeRef const& range, int64_t beginShardBytes, int64_t endShardBytes);

	// modify the metrics as like doing an n-bytes read op
	// Read key and cause bytes read overhead
	void get(KeyRef const& key, int64_t bytes);
	// Read range, assuming the first and last shard within the range having size `beginShardBytes` and `endShardBytes`,
	// return the total range size;
	int64_t getRange(KeyRangeRef const& range, int64_t beginShardBytes, int64_t endShardBytes);

	// trigger the asynchronous fetch keys operation
	void signalFetchKeys(const KeyRangeRef& range, int64_t rangeTotalBytes);

protected:
	PromiseStream<FetchKeysParams> fetchKeysRequests;

	void threeWayShardSplitting(const KeyRangeRef& outerRange,
	                            const KeyRangeRef& innerRange,
	                            uint64_t outerRangeSize,
	                            bool restrictSize);

	void twoWayShardSplitting(const KeyRangeRef& range,
	                          const KeyRef& splitPoint, uint64_t rangeSize, bool restrictSize);

	// Assuming the first and last shard within the range having size `beginShardBytes` and `endShardBytes`
	int64_t estimateRangeTotalBytes(KeyRangeRef const& range, int64_t beginShardBytes, int64_t endShardBytes);
	// Decrease the intersecting shard bytes as if delete the data
	void clearRangeTotalBytes(KeyRangeRef const& range, int64_t beginShardBytes, int64_t endShardBytes);

	// Update the storage metrics as if we write a k-v pair of `size` bytes.
	void notifyWriteMetrics(KeyRef const& key, int64_t size);

	// Randomly generate keys and kv size between the fetch range, updating the byte sample.
	// Once the fetchKeys return, the shard status will become FETCHED.
	Future<Void> fetchKeys(const FetchKeysParams&);

	// Update byte sample as if set a key value pair of which the size is kvSize
	void byteSampleApplySet(KeyRef const& key, int64_t kvSize);

	// Update byte sample as if clear a whole range
	void byteSampleApplyClear(KeyRangeRef const& range);
};

class MockGlobalStateImpl;

class MockGlobalState : public IKeyLocationService {
	friend struct MockGlobalStateTester;
	friend class MockGlobalStateImpl;

	std::vector<StorageServerInterface> extractStorageServerInterfaces(const std::vector<UID>& ids) const;

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
	bool restrictSize = true;

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
	 * * For each MSS belonging to the destination teams, mss.serverKeys[shard] = InFlight | Fetched | Completed
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
	bool allShardsRemovedFromServer(const UID& serverId);

	// SOMEDAY: NativeAPI::waitStorageMetrics should share the code in the future, this is a simpler version of it
	Future<std::pair<Optional<StorageMetrics>, int>> waitStorageMetrics(KeyRange const& keys,
	                                                                    StorageMetrics const& min,
	                                                                    StorageMetrics const& max,
	                                                                    StorageMetrics const& permittedError,
	                                                                    int shardLimit,
	                                                                    int expectedShardCount);

	Future<Standalone<VectorRef<KeyRef>>> splitStorageMetrics(const KeyRange& keys,
	                                                          const StorageMetrics& limit,
	                                                          const StorageMetrics& estimated,
	                                                          const Optional<int>& minSplitBytes);

	Future<KeyRangeLocationInfo> getKeyLocation(TenantInfo tenant,
	                                            Key key,
	                                            SpanContext spanContext,
	                                            Optional<UID> debugID,
	                                            UseProvisionalProxies useProvisionalProxies,
	                                            Reverse isBackward,
	                                            Version version) override;

	Future<std::vector<KeyRangeLocationInfo>> getKeyRangeLocations(TenantInfo tenant,
	                                                               KeyRange keys,
	                                                               int limit,
	                                                               Reverse reverse,
	                                                               SpanContext spanContext,
	                                                               Optional<UID> debugID,
	                                                               UseProvisionalProxies useProvisionalProxies,
	                                                               Version version) override;

	// data ops - the key is not accurate, only the shard the key locate in matters.

	// MGS finds the shard X contains this key, randomly generates a N-bytes read operation on that shard, which may
	// change the read sampling stats of shard X. return the random size of value
	int64_t get(KeyRef const& key);
	// For the edge shards contains the range boundaries, randomly do N1 byte and N2 byte read operations. For other
	// shards fully within the range, mock a full shard read op.
	int64_t getRange(KeyRangeRef const& range);
	// MGS finds the shard X contains this key, mock an N-bytes write to shard X, where N = valueSize + key.size().
	// Return a random number representing the old kv size
	int64_t set(KeyRef const& key, int valueSize, bool insert);
	// MGS finds the shard X contains this key, randomly generate an N-byte clear operation.
	// Return a random number representing the old kv size
	int64_t clear(KeyRef const& key);
	// Similar as getRange, but need to change shardTotalBytes because this is a clear operation.
	int64_t clearRange(KeyRangeRef const& range);

	// convenient shortcuts for test
	std::vector<Future<Void>> runAllMockServers();
	Future<Void> runMockServer(const UID& id);
};

#endif // FOUNDATIONDB_MOCKGLOBALSTATE_H
