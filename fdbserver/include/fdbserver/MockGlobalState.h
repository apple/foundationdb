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

#include "fdbserver/StorageMetrics.actor.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/KeyLocationService.h"
#include "fdbserver/SimulatedCluster.h"
#include "fdbserver/ShardsAffectedByTeamFailure.h"

constexpr const char* MOCK_DD_TEST_CLASS = "MockDD";

struct MockGlobalStateTester;

// the status is roughly order by transition order, except for UNSET and EMPTY
enum class MockShardStatus {
	UNSET = 0,
	EMPTY, // data loss
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
class MockStorageServer : public IStorageMetricsService, public ReferenceCounted<MockStorageServer> {
	friend struct MockGlobalStateTester;
	friend class MockStorageServerImpl;

	ActorCollection actors;

	CommonStorageCounters counters{ /*name=*/"", /*id=*/"", &metrics };

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

	// 100k/s small reads = 100% CPU
	// 500MB/s of read throughput = 100% CPU
	// 50k/s random small writes = 100% CPU
	// 100MB/s write throughput = 100% CPU
	// assume above operation will saturate the CPU linearly. We can change the model to a better one in the future.
	static constexpr double DEFAULT_READ_OP_CPU_MULTIPLIER = 100.0 / 100000;
	static constexpr double DEFAULT_WRITE_OP_CPU_MULTIPLIER = 100.0 / 50000;
	static constexpr double DEFAULT_READ_BYTE_CPU_MULTIPLIER = 100.0 / 500000000;
	static constexpr double DEFAULT_WRITE_BYTE_CPU_MULTIPLIER = 100.0 / 100000000;

	// can be set by workloads
	double read_op_cpu_multiplier = DEFAULT_READ_OP_CPU_MULTIPLIER;
	double write_op_cpu_multiplier = DEFAULT_WRITE_OP_CPU_MULTIPLIER;
	double read_byte_cpu_multiplier = DEFAULT_READ_BYTE_CPU_MULTIPLIER;
	double write_byte_cpu_multiplier = DEFAULT_WRITE_BYTE_CPU_MULTIPLIER;

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
	  : counters("MockStorageServer", ssi.id().toString()), totalDiskSpace(usedDiskSpace + availableDiskSpace),
	    usedDiskSpace(usedDiskSpace), ssi(ssi), id(ssi.id()) {}

	MockStorageServer(const UID& id, uint64_t availableDiskSpace, uint64_t usedDiskSpace = 0)
	  : MockStorageServer(StorageServerInterface(id), availableDiskSpace, usedDiskSpace) {}

	decltype(serverKeys)::Ranges getAllRanges() { return serverKeys.ranges(); }

	bool allShardStatusEqual(const KeyRangeRef& range, MockShardStatus status) const;
	bool allShardStatusIn(const KeyRangeRef& range, const std::set<MockShardStatus>& status) const;

	// change the status of range. This function may result in split to make the shard boundary align with range.begin
	// and range.end. If split happened, the shard size will be equally split.
	void setShardStatus(const KeyRangeRef& range, MockShardStatus status);

	void coalesceCompletedRange(const KeyRangeRef& range);

	// this function removed an aligned range from server
	void removeShard(const KeyRangeRef& range);

	// intersecting range size
	uint64_t sumRangeSize(const KeyRangeRef& range) const;

	void addActor(Future<Void> future) override;

	void getSplitPoints(SplitRangeRequest const& req) override;

	Future<Void> waitMetricsTenantAware(const WaitMetricsRequest& req) override;

	void getStorageMetrics(const GetStorageMetricsRequest& req) override;

	void getSplitMetrics(const SplitMetricsRequest& req) override;

	void getHotRangeMetrics(const ReadHotSubRangeRequest& req) override;

	int64_t getHotShardsMetrics(const KeyRange& range) override;

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

	HealthMetrics::StorageStats getStorageStats() const;

protected:
	PromiseStream<FetchKeysParams> fetchKeysRequests;

	void threeWayShardSplitting(const KeyRangeRef& outerRange, const KeyRangeRef& innerRange, uint64_t outerRangeSize);

	void twoWayShardSplitting(const KeyRangeRef& range, const KeyRef& splitPoint, uint64_t rangeSize);

	// Assuming the first and last shard within the range having size `beginShardBytes` and `endShardBytes`
	int64_t estimateRangeTotalBytes(KeyRangeRef const& range, int64_t beginShardBytes, int64_t endShardBytes);
	// Decrease the intersecting shard bytes as if delete the data
	void clearRangeTotalBytes(KeyRangeRef const& range, int64_t beginShardBytes, int64_t endShardBytes);

	// Update the storage metrics as if we write a k-v pair of `size` bytes.
	void notifyWriteMetrics(KeyRef const& key, int64_t size);

	// Update byte sample as if set a key value pair of which the size is kvSize
	void byteSampleApplySet(KeyRef const& key, int64_t kvSize);

	// Update byte sample as if clear a whole range
	void byteSampleApplyClear(KeyRangeRef const& range);

	double calculateCpuUsage() const;
};

class MockGlobalStateImpl;

namespace mock {
// This struct is only used in mock DD. For convenience of tracking the cluster topology and generating correct process
// locality
struct TopologyObject {
	enum Type { PROCESS, MACHINE, ZONE, DATA_HALL, DATA_CENTER } type;
	// corresponding to LocalityData field for each type
	Standalone<StringRef> topoId;
	std::shared_ptr<TopologyObject> parent;
	std::vector<std::shared_ptr<TopologyObject>> children;
	TopologyObject(Type type, Standalone<StringRef> id, std::shared_ptr<TopologyObject> parent = nullptr)
	  : type(type), topoId(id), parent(parent) {}
};

struct Process : public TopologyObject {
	LocalityData locality;
	std::vector<StorageServerInterface> ssInterfaces;
	Process(LocalityData locality, Standalone<StringRef> id, std::shared_ptr<TopologyObject> parent = nullptr)
	  : TopologyObject(PROCESS, id, parent), locality(locality) {}
};
} // namespace mock

class MockGlobalState : public IKeyLocationService {
	friend struct MockGlobalStateTester;
	friend class MockGlobalStateImpl;

	std::vector<std::shared_ptr<mock::TopologyObject>> clusterLayout;
	std::vector<std::shared_ptr<mock::Process>> processes;
	std::vector<std::shared_ptr<mock::Process>> seedProcesses;

	std::vector<StorageServerInterface> extractStorageServerInterfaces(const std::vector<UID>& ids) const;

public:
	typedef ShardsAffectedByTeamFailure::Team Team;
	// In-memory counterpart of the `keyServers` in system keyspace
	Reference<ShardsAffectedByTeamFailure> shardMapping;
	// In-memory counterpart of the `serverListKeys` in system keyspace
	std::map<UID, Reference<MockStorageServer>> allServers;
	DatabaseConfiguration configuration;

	// user defined parameters for mock workload purpose
	double emptyProb; // probability of doing an empty read
	int minByteSize, maxByteSize; // the size band of a point data operation
	bool restrictSize = true;

	MockGlobalState() : shardMapping(new ShardsAffectedByTeamFailure) {}

	// A globally shared state
	static std::shared_ptr<MockGlobalState>& g_mockState();

	static UID indexToUID(uint64_t a) { return UID(a, a); }
	void initializeClusterLayout(const BasicSimulationConfig&);
	void initializeAsEmptyDatabaseMGS(const DatabaseConfiguration& conf,
	                                  uint64_t defaultDiskSpace = MockStorageServer::DEFAULT_DISK_SPACE);
	// create a storage server interface on each process
	void addStoragePerProcess(uint64_t defaultDiskSpace = MockStorageServer::DEFAULT_DISK_SPACE);

	void addStorageServer(StorageServerInterface server, uint64_t diskSpace = MockStorageServer::DEFAULT_DISK_SPACE);

	// check methods
	/* Shard status contract:
	 * Shard is static.
	 * * In sharedMgs.shardMapping, the destination teams is empty for the given shard;
	 * * For each MSS belonging to the source teams, mss.serverKeys[shard] = Completed
	 * Shard is in-flight.
	 * * In sharedMgs.shardMapping,the destination teams is non-empty for a given shard;
	 * * For each MSS belonging to the source teams, mss.serverKeys[shard] = Completed
	 * * For each MSS belonging to the destination teams, mss.serverKeys[shard] = InFlight | Fetched | Completed
	 * Shard is lost.
	 * * In sharedMgs.shardMapping,  the destination teams is empty for the given shard;
	 * * For each MSS belonging to the source teams, mss.serverKeys[shard] = Empty
	 */
	bool serverIsSourceForShard(const UID& serverId, KeyRangeRef shard, bool inFlightShard = false);
	bool serverIsDestForShard(const UID& serverId, KeyRangeRef shard);

	/* Server status contract:
	 * Server X  is removed
	 * * sharedMgs.shardMapping doesn’t have any information about X
	 * * sharedMgs.allServer doesn’t contain X
	 * Server X is healthy
	 * * sharedMgs.allServer[X] is existed
	 * Server X is failed but haven’t been removed (a temporary status between healthy and removed)
	 * * sharedMgs.shardMapping doesn’t have any information about X
	 * * sharedMgs.allServer[X] is existed
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

	int getRangeSize(KeyRangeRef const& range);

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
