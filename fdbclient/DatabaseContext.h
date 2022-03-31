/*
 * DatabaseContext.h
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

#ifndef DatabaseContext_h
#define DatabaseContext_h
#include "fdbclient/Notified.h"
#include "flow/FastAlloc.h"
#include "flow/FastRef.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/genericactors.actor.h"
#include <vector>
#include <unordered_map>
#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/VersionVector.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/MultiInterface.h"
#include "flow/TDMetric.actor.h"
#include "fdbclient/EventTypes.actor.h"
#include "fdbrpc/ContinuousSample.h"
#include "fdbrpc/Smoother.h"

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

	double throttleDuration() const {
		if (expiration <= now()) {
			return 0.0;
		}

		double capacity =
		    (smoothRate.smoothTotal() - smoothReleased.smoothRate()) * CLIENT_KNOBS->TAG_THROTTLE_SMOOTHING_WINDOW;
		if (capacity >= 1) {
			return 0.0;
		}

		if (tpsRate == 0) {
			return std::max(0.0, expiration - now());
		}

		return std::min(expiration - now(), capacity / tpsRate);
	}
};

struct WatchParameters : public ReferenceCounted<WatchParameters> {
	const TenantInfo tenant;
	const Key key;
	const Optional<Value> value;

	const Version version;
	const TagSet tags;
	const SpanID spanID;
	const TaskPriority taskID;
	const Optional<UID> debugID;
	const UseProvisionalProxies useProvisionalProxies;

	WatchParameters(TenantInfo tenant,
	                Key key,
	                Optional<Value> value,
	                Version version,
	                TagSet tags,
	                SpanID spanID,
	                TaskPriority taskID,
	                Optional<UID> debugID,
	                UseProvisionalProxies useProvisionalProxies)
	  : tenant(tenant), key(key), value(value), version(version), tags(tags), spanID(spanID), taskID(taskID),
	    debugID(debugID), useProvisionalProxies(useProvisionalProxies) {}
};

class WatchMetadata : public ReferenceCounted<WatchMetadata> {
public:
	Promise<Version> watchPromise;
	Future<Version> watchFuture;
	Future<Void> watchFutureSS;

	Reference<const WatchParameters> parameters;

	WatchMetadata(Reference<const WatchParameters> parameters)
	  : watchFuture(watchPromise.getFuture()), parameters(parameters) {}
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
	Promise<Void> destroyed;
	UID interfToken;

	~ChangeFeedStorageData() { destroyed.send(Void()); }
};

struct ChangeFeedData : ReferenceCounted<ChangeFeedData> {
	PromiseStream<Standalone<VectorRef<MutationsAndVersionRef>>> mutations;
	std::vector<ReplyPromiseStream<ChangeFeedStreamReply>> streams;

	Version getVersion();
	Future<Void> whenAtLeast(Version version);

	NotifiedVersion lastReturnedVersion;
	std::vector<Reference<ChangeFeedStorageData>> storageData;
	AsyncVar<int> notAtLatest;
	Promise<Void> refresh;
	Version maxSeenVersion;
	Version endVersion = invalidVersion;
	Version popVersion =
	    invalidVersion; // like TLog pop version, set by SS and client can check it to see if they missed data

	ChangeFeedData() : notAtLatest(1) {}
};

struct EndpointFailureInfo {
	double startTime = 0;
	double lastRefreshTime = 0;
};

struct KeyRangeLocationInfo {
	TenantMapEntry tenantEntry;
	KeyRange range;
	Reference<LocationInfo> locations;

	KeyRangeLocationInfo() {}
	KeyRangeLocationInfo(TenantMapEntry tenantEntry, KeyRange range, Reference<LocationInfo> locations)
	  : tenantEntry(tenantEntry), range(range), locations(locations) {}
};

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
	                       int apiVersion = Database::API_VERSION_LATEST,
	                       IsSwitchable = IsSwitchable::False);

	~DatabaseContext();

	// Constructs a new copy of this DatabaseContext from the parameters of this DatabaseContext
	Database clone() const {
		return Database(new DatabaseContext(connectionRecord,
		                                    clientInfo,
		                                    coordinator,
		                                    clientInfoMonitor,
		                                    taskID,
		                                    clientLocality,
		                                    enableLocalityLoadBalance,
		                                    lockAware,
		                                    internal,
		                                    apiVersion,
		                                    switchable,
		                                    defaultTenant));
	}

	Optional<KeyRangeLocationInfo> getCachedLocation(const Optional<TenantName>& tenant,
	                                                 const KeyRef&,
	                                                 Reverse isBackward = Reverse::False);
	bool getCachedLocations(const Optional<TenantName>& tenant,
	                        const KeyRangeRef&,
	                        std::vector<KeyRangeLocationInfo>&,
	                        int limit,
	                        Reverse reverse);
	void cacheTenant(const TenantName& tenant, const TenantMapEntry& tenantEntry);
	Reference<LocationInfo> setCachedLocation(const Optional<TenantName>& tenant,
	                                          const TenantMapEntry& tenantEntry,
	                                          const KeyRangeRef&,
	                                          const std::vector<struct StorageServerInterface>&);
	void invalidateCachedTenant(const TenantNameRef& tenant);
	void invalidateCache(const KeyRef& tenantPrefix, const KeyRef& key, Reverse isBackward = Reverse::False);
	void invalidateCache(const KeyRef& tenantPrefix, const KeyRangeRef& keys);

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
	Future<Void> onProxiesChanged() const;
	Future<HealthMetrics> getHealthMetrics(bool detailed);
	// Pass a negative value for `shardLimit` to indicate no limit on the shard number.
	Future<StorageMetrics> getStorageMetrics(KeyRange const& keys, int shardLimit);
	Future<std::pair<Optional<StorageMetrics>, int>> waitStorageMetrics(KeyRange const& keys,
	                                                                    StorageMetrics const& min,
	                                                                    StorageMetrics const& max,
	                                                                    StorageMetrics const& permittedError,
	                                                                    int shardLimit,
	                                                                    int expectedShardCount);
	Future<Void> splitStorageMetricsStream(PromiseStream<Key> const& resultsStream,
	                                       KeyRange const& keys,
	                                       StorageMetrics const& limit,
	                                       StorageMetrics const& estimated);
	Future<Standalone<VectorRef<KeyRef>>> splitStorageMetrics(KeyRange const& keys,
	                                                          StorageMetrics const& limit,
	                                                          StorageMetrics const& estimated);

	Future<Standalone<VectorRef<ReadHotRangeWithMetrics>>> getReadHotRanges(KeyRange const& keys);

	// Returns the protocol version reported by the coordinator this client is connected to
	// If an expected version is given, the future won't return until the protocol version is different than expected
	// Note: this will never return if the server is running a protocol from FDB 5.0 or older
	Future<ProtocolVersion> getClusterProtocol(Optional<ProtocolVersion> expectedVersion = Optional<ProtocolVersion>());

	// Update the watch counter for the database
	void addWatch();
	void removeWatch();

	// watch map operations
	Reference<WatchMetadata> getWatchMetadata(int64_t tenantId, KeyRef key) const;
	void setWatchMetadata(Reference<WatchMetadata> metadata);
	void deleteWatchMetadata(int64_t tenant, KeyRef key);
	void clearWatchMetadata();

	void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value);

	Error deferredError;
	LockAware lockAware{ LockAware::False };

	bool isError() const { return deferredError.code() != invalid_error_code; }

	void checkDeferredError() const {
		if (isError()) {
			throw deferredError;
		}
	}

	int apiVersionAtLeast(int minVersion) const { return apiVersion < 0 || apiVersion >= minVersion; }

	Future<Void> onConnected(); // Returns after a majority of coordination servers are available and have reported a
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
	                                 bool canReadPopped = true);

	Future<std::vector<OverlappingChangeFeedEntry>> getOverlappingChangeFeeds(KeyRangeRef ranges, Version minVersion);
	Future<Void> popChangeFeedMutations(Key rangeID, Version version);

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
	                         int apiVersion = Database::API_VERSION_LATEST,
	                         IsSwitchable = IsSwitchable::False,
	                         Optional<TenantName> defaultTenant = Optional<TenantName>());

	explicit DatabaseContext(const Error& err);

	void expireThrottles();

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
		SpanID spanContext;
		Promise<GetReadVersionReply> reply;
		TagSet tags;
		Optional<UID> debugID;

		VersionRequest(SpanID spanContext, TagSet tags = TagSet(), Optional<UID> debugID = Optional<UID>())
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
	// Updated everytime we get a GRV response
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
	int tenantCacheSize;
	CoalescedKeyRangeMap<Reference<LocationInfo>> locationCache;
	std::unordered_map<Endpoint, EndpointFailureInfo> failedEndpointsOnHealthyServersInfo;
	std::unordered_map<TenantName, TenantMapEntry> tenantCache;

	std::map<UID, StorageServerInfo*> server_interf;
	std::map<UID, BlobWorkerInterface> blobWorker_interf; // blob workers don't change endpoints for the same ID

	// map from ssid -> tss interface
	std::unordered_map<UID, StorageServerInterface> tssMapping;
	// map from tssid -> metrics for that tss pair
	std::unordered_map<UID, Reference<TSSMetrics>> tssMetrics;
	// map from changeFeedId -> changeFeedRange
	std::unordered_map<Key, KeyRange> changeFeedCache;
	std::unordered_map<UID, Reference<ChangeFeedStorageData>> changeFeedUpdaters;

	Reference<ChangeFeedStorageData> getStorageData(StorageServerInterface interf);

	// map from ssid -> ss tag
	// @note this map allows the client to identify the latest commit versions
	// of storage servers (note that "ssVersionVectorCache" identifies storage
	// servers by their tags).
	std::unordered_map<UID, Tag> ssidTagMapping;

	UID dbId;
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
	Counter transactionStatusRequests;
	Counter transactionsTooOld;
	Counter transactionsFutureVersions;
	Counter transactionsNotCommitted;
	Counter transactionsMaybeCommitted;
	Counter transactionsResourceConstrained;
	Counter transactionsProcessBehind;
	Counter transactionsThrottled;
	Counter transactionsExpensiveClearCostEstCount;
	Counter transactionGrvFullBatches;
	Counter transactionGrvTimedOutBatches;
	Counter transactionsStaleVersionVectors;

	ContinuousSample<double> latencies, readLatencies, commitLatencies, GRVLatencies, mutationsPerCommit,
	    bytesPerCommit, bgLatencies, bgGranulesPerRequest;

	int outstandingWatches;
	int maxOutstandingWatches;

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
	bool anyBlobGranuleRequests = false;

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

	int apiVersion;

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
	void registerSpecialKeySpaceModule(SpecialKeySpace::MODULE module,
	                                   SpecialKeySpace::IMPLTYPE type,
	                                   std::unique_ptr<SpecialKeyRangeReadImpl>&& impl);

	static bool debugUseTags;
	static const std::vector<std::string> debugTransactionTagChoices;

	// Cache of the latest commit versions of storage servers.
	VersionVector ssVersionVectorCache;

	// Adds or updates the specified (SS, TSS) pair in the TSS mapping (if not already present).
	// Requests to the storage server will be duplicated to the TSS.
	void addTssMapping(StorageServerInterface const& ssi, StorageServerInterface const& tssi);

	// Removes the storage server and its TSS pair from the TSS mapping (if present).
	// Requests to the storage server will no longer be duplicated to its pair TSS.
	void removeTssMapping(StorageServerInterface const& ssi);

	// Adds or updates the specified (UID, Tag) pair in the tag mapping.
	void addSSIdTagMapping(const UID& uid, const Tag& tag);

	// Returns the latest commit versions that mutated the specified storage servers
	/// @note returns the latest commit version for a storage server only if the latest
	// commit version of that storage server is below the specified "readVersion".
	void getLatestCommitVersions(const Reference<LocationInfo>& locationInfo,
	                             Version readVersion,
	                             Reference<TransactionState> info,
	                             VersionVector& latestCommitVersions);

	// used in template functions to create a transaction
	using TransactionT = ReadYourWritesTransaction;
	Reference<TransactionT> createTransaction();

	EventCacheHolder connectToDatabaseEventCacheHolder;

private:
	std::unordered_map<std::pair<int64_t, Key>, Reference<WatchMetadata>, boost::hash<std::pair<int64_t, Key>>>
	    watchMap;
};

#endif
