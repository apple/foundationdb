/*
 * DatabaseContext.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef DatabaseContext_h
#define DatabaseContext_h
#include "fdbclient/Notified.h"
#include "flow/ApiVersion.h"
#include "flow/FastAlloc.h"
#include "flow/FastRef.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"
#include <vector>
#include <unordered_map>
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/VersionVector.h"
#include "fdbclient/IKeyValueStore.actor.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/MultiInterface.h"
#include "flow/TDMetric.actor.h"
#include "fdbclient/EventTypes.actor.h"
#include "fdbrpc/Smoother.h"
#include "fdbrpc/DDSketch.h"

class StorageServerInfo : public ReferencedInterface<StorageServerInterface> {
public:
	static Reference<StorageServerInfo> getInterface(DatabaseContext* cx,
	                                                 StorageServerInterface const& interf,
	                                                 LocalityData const& locality);
	void notifyContextDestroyed();

	~StorageServerInfo() override;

private:
	DatabaseContext* cx;
	StorageServerInfo(DatabaseContext* cx, StorageServerInterface const& interf, LocalityData const& locality)
	  : ReferencedInterface<StorageServerInterface>(interf, locality), cx(cx) {}
};

struct LocationInfo : MultiInterface<ReferencedInterface<StorageServerInterface>>, FastAllocated<LocationInfo> {
	using Locations = MultiInterface<ReferencedInterface<StorageServerInterface>>;
	explicit LocationInfo(const std::vector<Reference<ReferencedInterface<StorageServerInterface>>>& v)
	  : Locations(v) {}
	LocationInfo(const std::vector<Reference<ReferencedInterface<StorageServerInterface>>>& v, bool hasCaches)
	  : Locations(v), hasCaches(hasCaches) {}
	LocationInfo(const LocationInfo&) = delete;
	LocationInfo(LocationInfo&&) = delete;
	LocationInfo& operator=(const LocationInfo&) = delete;
	LocationInfo& operator=(LocationInfo&&) = delete;
	bool hasCaches = false;
	Reference<Locations> locations() { return Reference<Locations>::addRef(this); }
};

using CommitProxyInfo = ModelInterface<CommitProxyInterface>;
using GrvProxyInfo = ModelInterface<GrvProxyInterface>;

class ClientTagThrottleData : NonCopyable {
private:
	double tpsRate;
	double expiration;
	double lastCheck;
	bool rateSet = false;

	Smoother smoothRate;
	Smoother smoothReleased;

public:
	ClientTagThrottleData(ClientTagThrottleLimits const& limits)
	  : tpsRate(limits.tpsRate), expiration(limits.expiration), lastCheck(now()),
	    smoothRate(CLIENT_KNOBS->TAG_THROTTLE_SMOOTHING_WINDOW),
	    smoothReleased(CLIENT_KNOBS->TAG_THROTTLE_SMOOTHING_WINDOW) {
		ASSERT(tpsRate >= 0);
		smoothRate.reset(tpsRate);
	}

	void update(ClientTagThrottleLimits const& limits) {
		ASSERT(limits.tpsRate >= 0);
		this->tpsRate = limits.tpsRate;

		if (!rateSet || expired()) {
			rateSet = true;
			smoothRate.reset(limits.tpsRate);
		} else {
			smoothRate.setTotal(limits.tpsRate);
		}

		expiration = limits.expiration;
	}

	void addReleased(int released) { smoothReleased.addDelta(released); }

	bool expired() const { return expiration <= now(); }

	void updateChecked() { lastCheck = now(); }

	bool canRecheck() const { return lastCheck < now() - CLIENT_KNOBS->TAG_THROTTLE_RECHECK_INTERVAL; }

	double throttleDuration() const;
};

struct WatchParameters : public ReferenceCounted<WatchParameters> {
	const TenantInfo tenant;
	const Key key;
	const Optional<Value> value;

	const Version version;
	const TagSet tags;
	const SpanContext spanContext;
	const TaskPriority taskID;
	const Optional<UID> debugID;
	const UseProvisionalProxies useProvisionalProxies;

	WatchParameters(TenantInfo tenant,
	                Key key,
	                Optional<Value> value,
	                Version version,
	                TagSet tags,
	                SpanContext spanContext,
	                TaskPriority taskID,
	                Optional<UID> debugID,
	                UseProvisionalProxies useProvisionalProxies)
	  : tenant(tenant), key(key), value(value), version(version), tags(tags), spanContext(spanContext), taskID(taskID),
	    debugID(debugID), useProvisionalProxies(useProvisionalProxies) {}
};

class WatchMetadata : public ReferenceCounted<WatchMetadata> {
public:
	Promise<Version> watchPromise;
	Future<Void> watchFutureSS;

	Reference<const WatchParameters> parameters;

	WatchMetadata(Reference<const WatchParameters> parameters) : parameters(parameters) {}
};

struct MutationAndVersionStream {
	Standalone<MutationsAndVersionRef> next;
	PromiseStream<Standalone<MutationsAndVersionRef>> results;
	bool operator<(MutationAndVersionStream const& rhs) const { return next.version > rhs.next.version; }
};

struct ChangeFeedStorageData : ReferenceCounted<ChangeFeedStorageData> {
	UID id;
	Future<Void> updater;
	NotifiedVersion version;
	NotifiedVersion desired;
	UID interfToken;
	DatabaseContext* context;
	double created;

	~ChangeFeedStorageData();
};

struct ChangeFeedData : ReferenceCounted<ChangeFeedData> {
	PromiseStream<Standalone<VectorRef<MutationsAndVersionRef>>> mutations;
	std::vector<ReplyPromiseStream<ChangeFeedStreamReply>> streams;

	Version getVersion();
	Future<Void> whenAtLeast(Version version);

	UID dbgid;
	DatabaseContext* context;
	NotifiedVersion lastReturnedVersion;
	std::vector<Reference<ChangeFeedStorageData>> storageData;
	AsyncVar<int> notAtLatest;
	Promise<Void> refresh;
	Version maxSeenVersion;
	Version endVersion = invalidVersion;
	Version popVersion =
	    invalidVersion; // like TLog pop version, set by SS and client can check it to see if they missed data
	double created = 0;

	explicit ChangeFeedData(DatabaseContext* context = nullptr);
	~ChangeFeedData();
};

struct EndpointFailureInfo {
	double startTime = 0;
	double lastRefreshTime = 0;
};

struct KeyRangeLocationInfo {
	KeyRange range;
	Reference<LocationInfo> locations;

	KeyRangeLocationInfo() {}
	KeyRangeLocationInfo(KeyRange range, Reference<LocationInfo> locations) : range(range), locations(locations) {}
};

struct OverlappingChangeFeedsInfo {
	Arena arena;
	VectorRef<OverlappingChangeFeedEntry> feeds;
	// would prefer to use key range map but it complicates copy/move constructors
	std::vector<std::pair<KeyRangeRef, Version>> feedMetadataVersions;

	// for a feed that wasn't present, returns the metadata version it would have been fetched at.
	Version getFeedMetadataVersion(const KeyRangeRef& feedRange) const;
};

struct ChangeFeedCacheRange {
	Key tenantPrefix;
	Key rangeId;
	KeyRange range;

	ChangeFeedCacheRange(Key tenantPrefix, Key rangeId, KeyRange range)
	  : tenantPrefix(tenantPrefix), rangeId(rangeId), range(range) {}
	ChangeFeedCacheRange(std::tuple<Key, Key, KeyRange> range)
	  : tenantPrefix(std::get<0>(range)), rangeId(std::get<1>(range)), range(std::get<2>(range)) {}

	bool operator==(const ChangeFeedCacheRange& rhs) const {
		return tenantPrefix == rhs.tenantPrefix && rangeId == rhs.rangeId && range == rhs.range;
	}
};

struct ChangeFeedCacheData : ReferenceCounted<ChangeFeedCacheData> {
	Version version = -1; // The first version durably stored in the cache
	Version latest =
	    -1; // The last version durably store in the cache; this version will not be readable from disk before a commit
	Version popped = -1; // The popped version of this change feed
	bool active = false;
	double inactiveTime = 0;
};

namespace std {
template <>
struct hash<ChangeFeedCacheRange> {
	static constexpr std::hash<StringRef> hashFunc{};
	std::size_t operator()(ChangeFeedCacheRange const& range) const {
		std::size_t seed = 0;
		boost::hash_combine(seed, hashFunc(range.rangeId));
		boost::hash_combine(seed, hashFunc(range.range.begin));
		boost::hash_combine(seed, hashFunc(range.range.end));
		return seed;
	}
};
} // namespace std

class DatabaseContext : public ReferenceCounted<DatabaseContext>, public FastAllocated<DatabaseContext>, NonCopyable {
public:
	static DatabaseContext* allocateOnForeignThread() {
		return (DatabaseContext*)DatabaseContext::operator new(sizeof(DatabaseContext));
	}

	// Static constructor used by server processes to create a DatabaseContext
	// For internal (fdbserver) use only
	static Database create(Reference<AsyncVar<ClientDBInfo>> clientInfo,
	                       Future<Void> clientInfoMonitor,
	                       LocalityData clientLocality,
	                       EnableLocalityLoadBalance,
	                       TaskPriority taskID = TaskPriority::DefaultEndpoint,
	                       LockAware = LockAware::False,
	                       int _apiVersion = ApiVersion::LATEST_VERSION,
	                       IsSwitchable = IsSwitchable::False);

	~DatabaseContext();

	// Constructs a new copy of this DatabaseContext from the parameters of this DatabaseContext
	Database clone() const {
		Database cx = Database(new DatabaseContext(connectionRecord,
		                                           clientInfo,
		                                           coordinator,
		                                           clientInfoMonitor,
		                                           taskID,
		                                           clientLocality,
		                                           enableLocalityLoadBalance,
		                                           lockAware,
		                                           internal,
		                                           apiVersion.version(),
		                                           switchable,
		                                           defaultTenant));
		cx->globalConfig->init(Reference<AsyncVar<ClientDBInfo> const>(cx->clientInfo),
		                       std::addressof(cx->clientInfo->get()));
		return cx;
	}

	Optional<KeyRangeLocationInfo> getCachedLocation(const TenantInfo& tenant,
	                                                 const KeyRef&,
	                                                 Reverse isBackward = Reverse::False);
	bool getCachedLocations(const TenantInfo& tenant,
	                        const KeyRangeRef&,
	                        std::vector<KeyRangeLocationInfo>&,
	                        int limit,
	                        Reverse reverse);
	Reference<LocationInfo> setCachedLocation(const KeyRangeRef&, const std::vector<struct StorageServerInterface>&);
	void invalidateCache(const Optional<KeyRef>& tenantPrefix, const KeyRef& key, Reverse isBackward = Reverse::False);
	void invalidateCache(const Optional<KeyRef>& tenantPrefix, const KeyRangeRef& keys);

	// Records that `endpoint` is failed on a healthy server.
	void setFailedEndpointOnHealthyServer(const Endpoint& endpoint);

	// Updates `endpoint` refresh time if the `endpoint` is a failed endpoint. If not, this does nothing.
	void updateFailedEndpointRefreshTime(const Endpoint& endpoint);
	Optional<EndpointFailureInfo> getEndpointFailureInfo(const Endpoint& endpoint);
	void clearFailedEndpointOnHealthyServer(const Endpoint& endpoint);

	bool sampleReadTags() const;
	bool sampleOnCost(uint64_t cost) const;

	void updateProxies();
	Reference<CommitProxyInfo> getCommitProxies(UseProvisionalProxies useProvisionalProxies);
	Future<Reference<CommitProxyInfo>> getCommitProxiesFuture(UseProvisionalProxies useProvisionalProxies);
	Reference<GrvProxyInfo> getGrvProxies(UseProvisionalProxies useProvisionalProxies);
	bool isCurrentGrvProxy(UID proxyId) const;
	Future<Void> onProxiesChanged();
	Future<HealthMetrics> getHealthMetrics(bool detailed);
	// Get storage stats of a storage server from the cached healthy metrics if now() - lastUpdate < maxStaleness.
	// Otherwise, ask GRVProxy for the up-to-date health metrics.
	Future<Optional<HealthMetrics::StorageStats>> getStorageStats(const UID& id, double maxStaleness);
	// Pass a negative value for `shardLimit` to indicate no limit on the shard number.
	// Pass a valid `trState` with `hasTenant() == true` to make the function tenant-aware.
	Future<StorageMetrics> getStorageMetrics(
	    KeyRange const& keys,
	    int shardLimit,
	    Optional<Reference<TransactionState>> trState = Optional<Reference<TransactionState>>());
	Future<std::pair<Optional<StorageMetrics>, int>> waitStorageMetrics(
	    KeyRange const& keys,
	    StorageMetrics const& min,
	    StorageMetrics const& max,
	    StorageMetrics const& permittedError,
	    int shardLimit,
	    int expectedShardCount,
	    Optional<Reference<TransactionState>> trState = Optional<Reference<TransactionState>>());
	Future<Void> splitStorageMetricsStream(PromiseStream<Key> const& resultsStream,
	                                       KeyRange const& keys,
	                                       StorageMetrics const& limit,
	                                       StorageMetrics const& estimated,
	                                       Optional<int> const& minSplitBytes = {});
	Future<Standalone<VectorRef<KeyRef>>> splitStorageMetrics(KeyRange const& keys,
	                                                          StorageMetrics const& limit,
	                                                          StorageMetrics const& estimated,
	                                                          Optional<int> const& minSplitBytes = {});

	Future<Standalone<VectorRef<ReadHotRangeWithMetrics>>> getReadHotRanges(KeyRange const& keys);
	Future<Standalone<VectorRef<ReadHotRangeWithMetrics>>> getHotRangeMetrics(StorageServerInterface ssi,
	                                                                          KeyRange const& keys,
	                                                                          ReadHotSubRangeRequest::SplitType type,
	                                                                          int splitCount);

	// Returns the protocol version reported by the coordinator this client is connected to
	// If an expected version is given, the future won't return until the protocol version is different than expected
	// Note: this will never return if the server is running a protocol from FDB 5.0 or older
	Future<ProtocolVersion> getClusterProtocol(Optional<ProtocolVersion> expectedVersion = Optional<ProtocolVersion>());

	// Increases the counter of the number of watches in this DatabaseContext by 1. If the number of watches is too
	// many, throws too_many_watches.
	void increaseWatchCounter();

	// Decrease the counter of the number of watches in this DatabaseContext by 1
	void decreaseWatchCounter();

	// watch map operations

	// Gets the watch metadata per tenant id and key
	Reference<WatchMetadata> getWatchMetadata(int64_t tenantId, KeyRef key) const;

	// Refreshes the watch metadata. If the same watch is used (this is determined by the tenant id and the key), the
	// metadata will be updated.
	void setWatchMetadata(Reference<WatchMetadata> metadata);

	// Removes the watch metadata
	// If removeReferenceCount is set to be true, the corresponding WatchRefCount record is removed, too.
	void deleteWatchMetadata(int64_t tenant, KeyRef key, bool removeReferenceCount = false);

	// Increases reference count to the given watch. Returns the number of references to the watch.
	int32_t increaseWatchRefCount(const int64_t tenant, KeyRef key, const Version& version);

	// Decreases reference count to the given watch. If the reference count is dropped to 0, the watch metadata will be
	// removed. Returns the number of references to the watch.
	int32_t decreaseWatchRefCount(const int64_t tenant, KeyRef key, const Version& version);

	void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value);

	Error deferredError;
	LockAware lockAware{ LockAware::False };

	bool isError() const { return deferredError.code() != invalid_error_code; }

	void checkDeferredError() const {
		if (isError()) {
			throw deferredError;
		}
	}

	int apiVersionAtLeast(int minVersion) const { return apiVersion.version() >= minVersion; }

	Future<Void> onConnected()
	    const; // Returns after a majority of coordination servers are available and have reported a
	           // leader. The cluster file therefore is valid, but the database might be unavailable.
	Reference<IClusterConnectionRecord> getConnectionRecord();

	// Switch the database to use the new connection file, and recreate all pending watches for committed transactions.
	//
	// Meant to be used as part of a 'hot standby' solution to switch to the standby. A correct switch will involve
	// advancing the version on the new cluster sufficiently far that any transaction begun with a read version from the
	// old cluster will fail to commit. Assuming the above version-advancing is done properly, a call to
	// switchConnectionRecord guarantees that any read with a version from the old cluster will not be attempted on the
	// new cluster.
	Future<Void> switchConnectionRecord(Reference<IClusterConnectionRecord> standby);
	Future<Void> connectionFileChanged();
	IsSwitchable switchable{ false };

	// Management API, Attempt to kill or suspend a process, return 1 for request sent out, 0 for failure
	Future<int64_t> rebootWorker(StringRef address, bool check = false, int duration = 0);
	// Management API, force the database to recover into DCID, causing the database to lose the most recently committed
	// mutations
	Future<Void> forceRecoveryWithDataLoss(StringRef dcId);
	// Management API, create snapshot
	Future<Void> createSnapshot(StringRef uid, StringRef snapshot_command);

	Future<Void> getChangeFeedStream(Reference<ChangeFeedData> results,
	                                 Key rangeID,
	                                 Version begin = 0,
	                                 Version end = std::numeric_limits<Version>::max(),
	                                 KeyRange range = allKeys,
	                                 int replyBufferSize = -1,
	                                 bool canReadPopped = true,
	                                 ReadOptions readOptions = { ReadType::NORMAL, CacheResult::False },
	                                 bool encrypted = false,
	                                 Future<Key> tenantPrefix = Key());

	Future<OverlappingChangeFeedsInfo> getOverlappingChangeFeeds(KeyRangeRef ranges, Version minVersion);
	Future<Void> popChangeFeedMutations(Key rangeID, Version version);

	// BlobGranule API.
	Future<Key> purgeBlobGranules(KeyRange keyRange,
	                              Version purgeVersion,
	                              Optional<Reference<Tenant>> tenant,
	                              bool force = false);
	Future<Void> waitPurgeGranulesComplete(Key purgeKey);

	Future<bool> blobbifyRange(KeyRange range, Optional<Reference<Tenant>> tenant = {});
	Future<bool> blobbifyRangeBlocking(KeyRange range, Optional<Reference<Tenant>> tenant = {});
	Future<bool> unblobbifyRange(KeyRange range, Optional<Reference<Tenant>> tenant = {});
	Future<Standalone<VectorRef<KeyRangeRef>>> listBlobbifiedRanges(KeyRange range,
	                                                                int rangeLimit,
	                                                                Optional<Reference<Tenant>> tenant = {});
	Future<Version> verifyBlobRange(const KeyRange& range,
	                                Optional<Version> version,
	                                Optional<Reference<Tenant>> tenant = {});
	Future<bool> flushBlobRange(const KeyRange& range,
	                            bool compact,
	                            Optional<Version> version,
	                            Optional<Reference<Tenant>> tenant = {});
	Future<bool> blobRestore(const KeyRange range, Optional<Version> version);

	// private:
	explicit DatabaseContext(Reference<AsyncVar<Reference<IClusterConnectionRecord>>> connectionRecord,
	                         Reference<AsyncVar<ClientDBInfo>> clientDBInfo,
	                         Reference<AsyncVar<Optional<ClientLeaderRegInterface>> const> coordinator,
	                         Future<Void> clientInfoMonitor,
	                         TaskPriority taskID,
	                         LocalityData const& clientLocality,
	                         EnableLocalityLoadBalance,
	                         LockAware,
	                         IsInternal = IsInternal::True,
	                         int _apiVersion = ApiVersion::LATEST_VERSION,
	                         IsSwitchable = IsSwitchable::False,
	                         Optional<TenantName> defaultTenant = Optional<TenantName>());

	explicit DatabaseContext(const Error& err);

	void expireThrottles();

	UID dbId;

	// Key DB-specific information
	Reference<AsyncVar<Reference<IClusterConnectionRecord>>> connectionRecord;
	AsyncTrigger proxiesChangeTrigger;
	Future<Void> clientDBInfoMonitor;
	Future<Void> monitorTssInfoChange;
	Future<Void> tssMismatchHandler;
	PromiseStream<std::pair<UID, std::vector<DetailedTSSMismatch>>> tssMismatchStream;
	Future<Void> grvUpdateHandler;
	Reference<CommitProxyInfo> commitProxies;
	Reference<GrvProxyInfo> grvProxies;
	bool proxyProvisional; // Provisional commit proxy and grv proxy are used at the same time.
	UID proxiesLastChange;
	LocalityData clientLocality;
	QueueModel queueModel;
	EnableLocalityLoadBalance enableLocalityLoadBalance{ EnableLocalityLoadBalance::False };

	// The tenant used when none is specified for a transaction. Ordinarily this is unspecified, in which case the raw
	// key-space is used.
	Optional<TenantName> defaultTenant;

	struct VersionRequest {
		SpanContext spanContext;
		Promise<GetReadVersionReply> reply;
		TagSet tags;
		Optional<UID> debugID;

		VersionRequest(SpanContext spanContext, TagSet tags = TagSet(), Optional<UID> debugID = Optional<UID>())
		  : spanContext(spanContext), tags(tags), debugID(debugID) {}
	};

	// Transaction start request batching
	struct VersionBatcher {
		PromiseStream<VersionRequest> stream;
		Future<Void> actor;
	};
	std::map<uint32_t, VersionBatcher> versionBatcher;

	AsyncTrigger connectionFileChangedTrigger;

	// Disallow any reads at a read version lower than minAcceptableReadVersion.  This way the client does not have to
	// trust that the read version (possibly set manually by the application) is actually from the correct cluster.
	// Updated every time we get a GRV response
	Version minAcceptableReadVersion = std::numeric_limits<Version>::max();
	void validateVersion(Version) const;

	// Client status updater
	struct ClientStatusUpdater {
		std::vector<std::pair<std::string, BinaryWriter>> inStatusQ;
		std::vector<std::pair<std::string, BinaryWriter>> outStatusQ;
		Future<Void> actor;
	};
	ClientStatusUpdater clientStatusUpdater;

	// Cache of location information
	int locationCacheSize;
	CoalescedKeyRangeMap<Reference<LocationInfo>> locationCache;
	std::unordered_map<Endpoint, EndpointFailureInfo> failedEndpointsOnHealthyServersInfo;

	std::map<UID, StorageServerInfo*> server_interf;
	std::map<UID, BlobWorkerInterface> blobWorker_interf; // blob workers don't change endpoints for the same ID

	// map from ssid -> tss interface
	std::unordered_map<UID, StorageServerInterface> tssMapping;
	// map from tssid -> metrics for that tss pair
	std::unordered_map<UID, Reference<TSSMetrics>> tssMetrics;
	// map from changeFeedId -> changeFeedRange
	std::unordered_map<Key, KeyRange> changeFeedCache;
	std::unordered_map<UID, ChangeFeedStorageData*> changeFeedUpdaters;
	std::map<UID, ChangeFeedData*> notAtLatestChangeFeeds;

	IKeyValueStore* storage = nullptr;
	Future<Void> changeFeedStorageCommitter;
	Future<Void> initializeChangeFeedCache = Void();
	int64_t uncommittedCFBytes = 0;
	Reference<AsyncVar<bool>> commitChangeFeedStorage;

	std::unordered_map<ChangeFeedCacheRange, Reference<ChangeFeedCacheData>> changeFeedCaches;
	std::unordered_map<Key, std::unordered_map<ChangeFeedCacheRange, Reference<ChangeFeedCacheData>>> rangeId_cacheData;

	void setStorage(IKeyValueStore* storage);

	Reference<ChangeFeedStorageData> getStorageData(StorageServerInterface interf);
	Version getMinimumChangeFeedVersion();
	void setDesiredChangeFeedVersion(Version v);

	// map from ssid -> ss tag
	// @note this map allows the client to identify the latest commit versions
	// of storage servers (note that "ssVersionVectorCache" identifies storage
	// servers by their tags).
	std::unordered_map<UID, Tag> ssidTagMapping;

	IsInternal internal; // Only contexts created through the C client and fdbcli are non-internal

	PrioritizedTransactionTagMap<ClientTagThrottleData> throttledTags;

	CounterCollection cc;

	Counter transactionReadVersions;
	Counter transactionReadVersionsThrottled;
	Counter transactionReadVersionsCompleted;
	Counter transactionReadVersionBatches;
	Counter transactionBatchReadVersions;
	Counter transactionDefaultReadVersions;
	Counter transactionImmediateReadVersions;
	Counter transactionBatchReadVersionsCompleted;
	Counter transactionDefaultReadVersionsCompleted;
	Counter transactionImmediateReadVersionsCompleted;
	Counter transactionLogicalReads;
	Counter transactionPhysicalReads;
	Counter transactionPhysicalReadsCompleted;
	Counter transactionGetKeyRequests;
	Counter transactionGetValueRequests;
	Counter transactionGetRangeRequests;
	Counter transactionGetMappedRangeRequests;
	Counter transactionGetRangeStreamRequests;
	Counter transactionWatchRequests;
	Counter transactionGetAddressesForKeyRequests;
	Counter transactionBytesRead;
	Counter transactionKeysRead;
	Counter transactionMetadataVersionReads;
	Counter transactionCommittedMutations;
	Counter transactionCommittedMutationBytes;
	Counter transactionSetMutations;
	Counter transactionClearMutations;
	Counter transactionAtomicMutations;
	Counter transactionsCommitStarted;
	Counter transactionsCommitCompleted;
	Counter transactionKeyServerLocationRequests;
	Counter transactionKeyServerLocationRequestsCompleted;
	Counter transactionBlobGranuleLocationRequests;
	Counter transactionBlobGranuleLocationRequestsCompleted;
	Counter transactionStatusRequests;
	Counter transactionTenantLookupRequests;
	Counter transactionTenantLookupRequestsCompleted;
	Counter transactionsTooOld;
	Counter transactionsFutureVersions;
	Counter transactionsNotCommitted;
	Counter transactionsMaybeCommitted;
	Counter transactionsResourceConstrained;
	Counter transactionsProcessBehind;
	Counter transactionsThrottled;
	Counter transactionsLockRejected;
	Counter transactionsExpensiveClearCostEstCount;
	Counter transactionGrvFullBatches;
	Counter transactionGrvTimedOutBatches;
	Counter transactionCommitVersionNotFoundForSS;

	// Blob Granule Read metrics. Omit from logging if not used.
	bool anyBGReads;
	CounterCollection ccBG;
	Counter bgReadInputBytes;
	Counter bgReadOutputBytes;
	Counter bgReadSnapshotRows;
	Counter bgReadRowsCleared;
	Counter bgReadRowsInserted;
	Counter bgReadRowsUpdated;
	DDSketch<double> bgLatencies, bgGranulesPerRequest;

	// Change Feed metrics. Omit change feed metrics from logging if not used
	bool usedAnyChangeFeeds;
	CounterCollection ccFeed;
	Counter feedStreamStarts;
	Counter feedMergeStreamStarts;
	Counter feedErrors;
	Counter feedNonRetriableErrors;
	Counter feedPops;
	Counter feedPopsFallback;

	DDSketch<double> latencies, readLatencies, commitLatencies, GRVLatencies, mutationsPerCommit, bytesPerCommit;

	int outstandingWatches;
	int maxOutstandingWatches;

	// Manage any shared state that may be used by MVC
	DatabaseSharedState* sharedStatePtr;
	Future<DatabaseSharedState*> initSharedState();
	void setSharedState(DatabaseSharedState* p);

	// GRV Cache
	// Database-level read version cache storing the most recent successful GRV as well as the time it was requested.
	double lastGrvTime;
	Version cachedReadVersion;
	void updateCachedReadVersion(double t, Version v);
	Version getCachedReadVersion();
	double getLastGrvTime();
	double lastRkBatchThrottleTime;
	double lastRkDefaultThrottleTime;
	// Cached RVs can be updated through commits, and using cached RVs avoids the proxies altogether
	// Because our checks for ratekeeper throttling requires communication with the proxies,
	// we want to track the last time in order to periodically contact the proxy to check for throttling
	double lastProxyRequestTime;

	int snapshotRywEnabled;

	bool transactionTracingSample;
	double verifyCausalReadsProp = 0.0;
	bool blobGranuleNoMaterialize = false;

	Future<Void> logger;
	Future<Void> throttleExpirer;

	TaskPriority taskID;

	Int64MetricHandle getValueSubmitted;
	EventMetricHandle<GetValueComplete> getValueCompleted;

	Reference<AsyncVar<ClientDBInfo>> clientInfo;
	Future<Void> clientInfoMonitor;
	Future<Void> connected;

	// An AsyncVar that reports the coordinator this DatabaseContext is interacting with
	Reference<AsyncVar<Optional<ClientLeaderRegInterface>> const> coordinator;

	Reference<AsyncVar<Optional<ClusterInterface>>> statusClusterInterface;
	Future<Void> statusLeaderMon;
	double lastStatusFetch;

	ApiVersion apiVersion;

	int mvCacheInsertLocation;
	std::vector<std::pair<Version, Optional<Value>>> metadataVersionCache;

	HealthMetrics healthMetrics;
	double healthMetricsLastUpdated;
	double detailedHealthMetricsLastUpdated;
	Smoother smoothMidShardSize;
	bool useConfigDatabase{ false };

	UniqueOrderedOptionList<FDBTransactionOptions> transactionDefaults;

	Future<Void> cacheListMonitor;
	AsyncTrigger updateCache;
	std::vector<std::unique_ptr<SpecialKeyRangeReadImpl>> specialKeySpaceModules;
	std::unique_ptr<SpecialKeySpace> specialKeySpace;
	void registerSpecialKeysImpl(SpecialKeySpace::MODULE module,
	                             SpecialKeySpace::IMPLTYPE type,
	                             std::unique_ptr<SpecialKeyRangeReadImpl>&& impl,
	                             int deprecatedVersion = -1);

	static bool debugUseTags;
	static const std::vector<std::string> debugTransactionTagChoices;

	// Cache of the latest commit versions of storage servers.
	VersionVector ssVersionVectorCache;

	// Introduced mainly to optimize out the version vector related code (on the client side)
	// when the version vector feature is disabled (on the server side).
	// @param ssVersionVectorDelta version vector changes sent by GRV proxy
	inline bool versionVectorCacheActive(const VersionVector& ssVersionVectorDelta) {
		return (ssVersionVectorCache.getMaxVersion() != invalidVersion ||
		        ssVersionVectorDelta.getMaxVersion() != invalidVersion);
	}

	// Adds or updates the specified (SS, TSS) pair in the TSS mapping (if not already present).
	// Requests to the storage server will be duplicated to the TSS.
	void addTssMapping(StorageServerInterface const& ssi, StorageServerInterface const& tssi);

	// Removes the storage server and its TSS pair from the TSS mapping (if present).
	// Requests to the storage server will no longer be duplicated to its pair TSS.
	void removeTssMapping(StorageServerInterface const& ssi);

	// Adds or updates the specified (UID, Tag) pair in the tag mapping.
	void addSSIdTagMapping(const UID& uid, const Tag& tag);

	// Returns the latest commit version that mutated the specified storage server.
	// @in ssid id of the storage server interface
	// @out tag storage server's tag, if an entry exists for "ssid" in "ssidTagMapping"
	// @out commitVersion latest commit version that mutated the storage server
	void getLatestCommitVersionForSSID(const UID& ssid, Tag& tag, Version& commitVersion);

	// Returns the latest commit versions that mutated the specified storage servers
	/// @note returns the latest commit version for a storage server only if the latest
	// commit version of that storage server is below the transaction's readVersion.
	void getLatestCommitVersions(const Reference<LocationInfo>& locationInfo,
	                             Reference<TransactionState> info,
	                             VersionVector& latestCommitVersions);

	// Returns the latest commit version that mutated the specified storage server.
	// @note this is a lightweight version of "getLatestCommitVersions()", to be used
	// when the state ("TransactionState") of the transaction that fetched the read
	// version is not available.
	void getLatestCommitVersion(const StorageServerInterface& ssi,
	                            Version readVersion,
	                            VersionVector& latestCommitVersion);

	TenantMode getTenantMode() const {
		CODE_PROBE(clientInfo->get().grvProxies.empty() || clientInfo->get().grvProxies[0].provisional,
		           "Accessing tenant mode in provisional ClientDBInfo",
		           probe::decoration::rare);
		return clientInfo->get().tenantMode;
	}

	// used in template functions to create a transaction
	using TransactionT = ReadYourWritesTransaction;
	Reference<TransactionT> createTransaction();

	std::unique_ptr<GlobalConfig> globalConfig;
	EventCacheHolder connectToDatabaseEventCacheHolder;

	Future<int64_t> lookupTenant(TenantName tenant);

	// Get client-side status information as a JSON string with the following schema:
	// { "Healthy" : <overall health status: true or false>,
	//   "ClusterID" : <UUID>,
	//   "Coordinators" : [ <address>, ...  ],
	//   "CurrentCoordinator" : <address>
	//   "GrvProxies" : [ <address>, ...  ],
	//   "CommitProxies" : [ <address>", ... ],
	//   "StorageServers" : [ { "Address" : <address>, "SSID" : <Storage Server ID> }, ... ],
	//   "Connections" : [
	//     { "Address" : "<address>",
	//       "Status" : <failed|connected|connecting|disconnected>,
	//       "Compatible" : <is protocol version compatible with the client>,
	//       "ConnectFailedCount" : <number of failed connection attempts>,
	//       "LastConnectTime" : <elapsed time in seconds since the last connection attempt>,
	//       "PingCount" : <total ping count>,
	//       "PingTimeoutCount" : <number of ping timeouts>,
	//       "BytesSampleTime" : <elapsed time of the reported the bytes received and sent values>,
	//       "BytesReceived" : <bytes received>,
	//       "BytesSent" : <bytes sent>,
	//       "ProtocolVersion" : <protocol version of the server, missing if unknown>
	//     },
	//     ...
	//   ]
	// }
	//
	// The addresses in the Connections array match the addresses of Coordinators, GrvProxies,
	// CommitProxies and StorageServers, there is one entry per different address
	//
	// If the database context is initialized with an error, the JSON contains just the error code
	// { "InitializationError" : <error code> }
	Standalone<StringRef> getClientStatus();

	// Gets a database level backoff delay future, time in seconds.
	Future<Void> getBackoff() const { return backoffDelay > 0.0 ? delay(backoffDelay) : Future<Void>(Void()); }

	// Updates internal Backoff state when a request fails or succeeds.
	// E.g., commit_proxy_memory_limit_exceeded error means the database is overloaded
	// and the client should back off more significantly than transaction-level errors.
	void updateBackoff(const Error& err);

private:
	using WatchMapKey = std::pair<int64_t, Key>;
	using WatchMapKeyHasher = boost::hash<WatchMapKey>;
	using WatchMapValue = Reference<WatchMetadata>;
	using WatchMap_t = std::unordered_map<WatchMapKey, WatchMapValue, WatchMapKeyHasher>;

	WatchMap_t watchMap;

	// The reason of using a multiset of Versions as counter instead of a simpler integer counter is due to the
	// possible race condition:
	//
	//    1. A watch to key A is set, the watchValueMap ACTOR, noted as X, starts waiting.
	//    2. All watches are cleared due to connection string change.
	//    3. The watch to key A is restarted with watchValueMap ACTOR Y.
	//    4. X receives the cancel exception, and tries to dereference the counter. This causes Y gets cancelled.
	//
	// By introducing versions, this race condition is solved.
	using WatchCounterMapValue = std::multiset<Version>;
	using WatchCounterMap_t = std::unordered_map<WatchMapKey, WatchCounterMapValue, WatchMapKeyHasher>;
	// Maps the number of the WatchMapKey being used.
	WatchCounterMap_t watchCounterMap;
	double backoffDelay = 0.0;

	void initializeSpecialCounters();
};

// Similar to tr.onError(), but doesn't require a DatabaseContext.
struct Backoff {
	Backoff(double backoff = CLIENT_KNOBS->DEFAULT_BACKOFF, double maxBackoff = CLIENT_KNOBS->DEFAULT_MAX_BACKOFF)
	  : backoff(backoff), maxBackoff(maxBackoff) {}

	Future<Void> onError() {
		double currentBackoff = backoff;
		backoff = std::min(backoff * CLIENT_KNOBS->BACKOFF_GROWTH_RATE, maxBackoff);
		return delay(currentBackoff * deterministicRandom()->random01());
	}

private:
	double backoff;
	double maxBackoff;
};

#endif
