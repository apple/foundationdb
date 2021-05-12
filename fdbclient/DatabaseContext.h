/*
 * DatabaseContext.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
	  : cx(cx), ReferencedInterface<StorageServerInterface>(interf, locality) {}
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

	bool expired() { return expiration <= now(); }

	void updateChecked() { lastCheck = now(); }

	bool canRecheck() { return lastCheck < now() - CLIENT_KNOBS->TAG_THROTTLE_RECHECK_INTERVAL; }

	double throttleDuration() {
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

class WatchMetadata : public ReferenceCounted<WatchMetadata> {
public:
	Key key;
	Optional<Value> value;
	Version version;
	Promise<Version> watchPromise;
	Future<Version> watchFuture;
	Future<Void> watchFutureSS;

	TransactionInfo info;
	TagSet tags;

	WatchMetadata(Key key, Optional<Value> value, Version version, TransactionInfo info, TagSet tags);
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
	                       bool enableLocalityLoadBalance,
	                       TaskPriority taskID = TaskPriority::DefaultEndpoint,
	                       bool lockAware = false,
	                       int apiVersion = Database::API_VERSION_LATEST,
	                       bool switchable = false);

	~DatabaseContext();

	// Constructs a new copy of this DatabaseContext from the parameters of this DatabaseContext
	Database clone() const {
		return Database(new DatabaseContext(connectionFile,
		                                    clientInfo,
		                                    coordinator,
		                                    clientInfoMonitor,
		                                    taskID,
		                                    clientLocality,
		                                    enableLocalityLoadBalance,
		                                    lockAware,
		                                    internal,
		                                    apiVersion,
		                                    switchable));
	}

	std::pair<KeyRange, Reference<LocationInfo>> getCachedLocation(const KeyRef&, bool isBackward = false);
	bool getCachedLocations(const KeyRangeRef&,
	                        vector<std::pair<KeyRange, Reference<LocationInfo>>>&,
	                        int limit,
	                        bool reverse);
	Reference<LocationInfo> setCachedLocation(const KeyRangeRef&, const vector<struct StorageServerInterface>&);
	void invalidateCache(const KeyRef&, bool isBackward = false);
	void invalidateCache(const KeyRangeRef&);

	bool sampleReadTags() const;
	bool sampleOnCost(uint64_t cost) const;

	void updateProxies();
	Reference<CommitProxyInfo> getCommitProxies(bool useProvisionalProxies);
	Future<Reference<CommitProxyInfo>> getCommitProxiesFuture(bool useProvisionalProxies);
	Reference<GrvProxyInfo> getGrvProxies(bool useProvisionalProxies);
	Future<Void> onProxiesChanged();
	Future<HealthMetrics> getHealthMetrics(bool detailed);

	// Returns the protocol version reported by the coordinator this client is connected to
	// If an expected version is given, the future won't return until the protocol version is different than expected
	// Note: this will never return if the server is running a protocol from FDB 5.0 or older
	Future<ProtocolVersion> getClusterProtocol(Optional<ProtocolVersion> expectedVersion = Optional<ProtocolVersion>());

	// Update the watch counter for the database
	void addWatch();
	void removeWatch();

	// watch map operations
	Reference<WatchMetadata> getWatchMetadata(KeyRef key) const;
	KeyRef setWatchMetadata(Reference<WatchMetadata> metadata);
	void deleteWatchMetadata(KeyRef key);
	void clearWatchMetadata();

	void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value);

	Error deferredError;
	bool lockAware;

	bool isError() { return deferredError.code() != invalid_error_code; }

	void checkDeferredError() {
		if (isError()) {
			throw deferredError;
		}
	}

	int apiVersionAtLeast(int minVersion) { return apiVersion < 0 || apiVersion >= minVersion; }

	Future<Void> onConnected(); // Returns after a majority of coordination servers are available and have reported a
	                            // leader. The cluster file therefore is valid, but the database might be unavailable.
	Reference<ClusterConnectionFile> getConnectionFile();

	// Switch the database to use the new connection file, and recreate all pending watches for committed transactions.
	//
	// Meant to be used as part of a 'hot standby' solution to switch to the standby. A correct switch will involve
	// advancing the version on the new cluster sufficiently far that any transaction begun with a read version from the
	// old cluster will fail to commit. Assuming the above version-advancing is done properly, a call to
	// switchConnectionFile guarantees that any read with a version from the old cluster will not be attempted on the
	// new cluster.
	Future<Void> switchConnectionFile(Reference<ClusterConnectionFile> standby);
	Future<Void> connectionFileChanged();
	bool switchable = false;

	// Management API, Attempt to kill or suspend a process, return 1 for request sent out, 0 for failure
	Future<int64_t> rebootWorker(StringRef address, bool check = false, int duration = 0);
	// Management API, force the database to recover into DCID, causing the database to lose the most recently committed
	// mutations
	Future<Void> forceRecoveryWithDataLoss(StringRef dcId);
	// Management API, create snapshot
	Future<Void> createSnapshot(StringRef uid, StringRef snapshot_command);

	// private:
	explicit DatabaseContext(Reference<AsyncVar<Reference<ClusterConnectionFile>>> connectionFile,
	                         Reference<AsyncVar<ClientDBInfo>> clientDBInfo,
	                         Reference<AsyncVar<Optional<ClientLeaderRegInterface>>> coordinator,
	                         Future<Void> clientInfoMonitor,
	                         TaskPriority taskID,
	                         LocalityData const& clientLocality,
	                         bool enableLocalityLoadBalance,
	                         bool lockAware,
	                         bool internal = true,
	                         int apiVersion = Database::API_VERSION_LATEST,
	                         bool switchable = false);

	explicit DatabaseContext(const Error& err);

	void expireThrottles();

	// Key DB-specific information
	Reference<AsyncVar<Reference<ClusterConnectionFile>>> connectionFile;
	AsyncTrigger proxiesChangeTrigger;
	Future<Void> monitorProxiesInfoChange;
	Future<Void> monitorTssInfoChange;
	Future<Void> tssMismatchHandler;
	PromiseStream<UID> tssMismatchStream;
	Reference<CommitProxyInfo> commitProxies;
	Reference<GrvProxyInfo> grvProxies;
	bool proxyProvisional; // Provisional commit proxy and grv proxy are used at the same time.
	UID proxiesLastChange;
	LocalityData clientLocality;
	QueueModel queueModel;
	bool enableLocalityLoadBalance;

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
	void validateVersion(Version);

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

	std::map<UID, StorageServerInfo*> server_interf;

	std::map<UID, Reference<TSSMetrics>> tssMetrics;

	UID dbId;
	bool internal; // Only contexts created through the C client and fdbcli are non-internal

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

	ContinuousSample<double> latencies, readLatencies, commitLatencies, GRVLatencies, mutationsPerCommit,
	    bytesPerCommit;

	int outstandingWatches;
	int maxOutstandingWatches;

	int snapshotRywEnabled;

	int transactionTracingEnabled;

	Future<Void> logger;
	Future<Void> throttleExpirer;

	TaskPriority taskID;

	Int64MetricHandle getValueSubmitted;
	EventMetricHandle<GetValueComplete> getValueCompleted;

	Reference<AsyncVar<ClientDBInfo>> clientInfo;
	Future<Void> clientInfoMonitor;
	Future<Void> connected;

	// An AsyncVar that reports the coordinator this DatabaseContext is interacting with
	Reference<AsyncVar<Optional<ClientLeaderRegInterface>>> coordinator;

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
	std::unordered_map<KeyRef, Reference<WatchMetadata>> watchMap;

	void maybeAddTssMapping(StorageServerInterface const& ssi);
	void addTssMapping(StorageServerInterface const& ssi, StorageServerInterface const& tssi);
};

#endif
