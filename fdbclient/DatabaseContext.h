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
#pragma once

#include "NativeAPI.h"
#include "KeyRangeMap.h"
#include "MasterProxyInterface.h"
#include "ClientDBInfo.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/MultiInterface.h"
#include "flow/TDMetric.actor.h"
#include "EventTypes.actor.h"
#include "fdbrpc/ContinuousSample.h"

class LocationInfo : public MultiInterface<StorageServerInterface> {
public:
	static Reference<LocationInfo> getInterface( DatabaseContext *cx, std::vector<StorageServerInterface> const& alternatives, LocalityData const& clientLocality );
	void notifyContextDestroyed();
	
	virtual ~LocationInfo();

private:
	DatabaseContext *cx;
	LocationInfo( DatabaseContext* cx, vector<StorageServerInterface> const& shards, LocalityData const& clientLocality ) : cx(cx), MultiInterface( shards, clientLocality ) {}
};

class ProxyInfo : public MultiInterface<MasterProxyInterface> {
public:
	ProxyInfo( vector<MasterProxyInterface> const& proxies, LocalityData const& clientLocality ) : MultiInterface( proxies, clientLocality, ALWAYS_FRESH ) {}
};

class DatabaseContext : public ReferenceCounted<DatabaseContext>, NonCopyable {
public:
	static Future<Database> createDatabase( Reference<AsyncVar<Optional<ClusterInterface>>> clusterInterface, Reference<Cluster> cluster, Standalone<StringRef> dbName, LocalityData const& clientLocality ); 
	//static Future< Void > configureDatabase( ZookeeperInterface const& zk, int configScope, int configMode, Standalone<StringRef> dbName = Standalone<StringRef>() );

	// For internal (fdbserver) use only: create a database context for a DB with already known client info
	static Database create( Reference<AsyncVar<ClientDBInfo>> info, Future<Void> dependency, LocalityData clientLocality, bool enableLocalityLoadBalance, int taskID = TaskDefaultEndpoint, bool lockAware = false );

	~DatabaseContext();

	Database clone() const { return Database(new DatabaseContext( clientInfo, cluster, clientInfoMonitor, dbName, dbId, taskID, clientLocality, enableLocalityLoadBalance, lockAware )); }

	pair<KeyRange,Reference<LocationInfo>> getCachedLocation( const KeyRef&, bool isBackward = false );
	bool getCachedLocations( const KeyRangeRef&, vector<std::pair<KeyRange,Reference<LocationInfo>>>&, int limit, bool reverse );
	Reference<LocationInfo> setCachedLocation( const KeyRangeRef&, const vector<struct StorageServerInterface>& );
	void invalidateCache( const KeyRef&, bool isBackward = false );
	void invalidateCache( const KeyRangeRef& );

	Reference<ProxyInfo> getMasterProxies();
	Future<Reference<ProxyInfo>> getMasterProxiesFuture();
	Future<Void> onMasterProxiesChanged();

	// Update the watch counter for the database
	void addWatch();
	void removeWatch();
	
	void setOption( FDBDatabaseOptions::Option option, Optional<StringRef> value );

	Error deferred_error;
	bool lockAware;

	void checkDeferredError() {
		if( cluster )
			cluster->checkDeferredError();
		if( deferred_error.code() != invalid_error_code )
			throw deferred_error;
	}

//private: friend class ClientInfoMonitorActor;
	explicit DatabaseContext( Reference<AsyncVar<ClientDBInfo>> clientInfo, 
		Reference<Cluster> cluster, Future<Void> clientInfoMonitor,
		Standalone<StringRef> dbName, Standalone<StringRef> dbId, int taskID, LocalityData clientLocality, bool enableLocalityLoadBalance, bool lockAware );

	// These are reference counted
	Reference<Cluster> cluster;
	Future<Void> clientInfoMonitor; // or sometimes an outside dependency that does the same thing!

	// Key DB-specific information
	Reference<AsyncVar<ClientDBInfo>> clientInfo;
	AsyncTrigger masterProxiesChangeTrigger;
	Future<Void> monitorMasterProxiesInfoChange;
	Reference<ProxyInfo> masterProxies;
	UID masterProxiesLastChange;
	LocalityData clientLocality;
	QueueModel queueModel;
	bool enableLocalityLoadBalance;

	// Transaction start request batching
	struct VersionBatcher {
		PromiseStream< std::pair< Promise<GetReadVersionReply>, Optional<UID> > > stream;
		Future<Void> actor;
	};
	std::map<uint32_t, VersionBatcher> versionBatcher;

	// Client status updater
	struct ClientStatusUpdater {
		std::vector<BinaryWriter> inStatusQ;
		std::vector<BinaryWriter> outStatusQ;
		Future<Void> actor;
	};
	ClientStatusUpdater clientStatusUpdater;

	// Cache of location information
	int locationCacheSize;
	CoalescedKeyRangeMap< Reference<LocationInfo> > locationCache;

	std::map< std::vector<UID>, LocationInfo* > ssid_locationInfo;

	// for logging/debugging (relic of multi-db support)
	Standalone<StringRef> dbName;
	Standalone<StringRef> dbId;

	int64_t transactionReadVersions;
	int64_t transactionLogicalReads;
	int64_t transactionPhysicalReads;
	int64_t transactionCommittedMutations;
	int64_t transactionCommittedMutationBytes;
	int64_t transactionsCommitStarted;
	int64_t transactionsCommitCompleted;
	int64_t transactionsTooOld;
	int64_t transactionsFutureVersions;
	int64_t transactionsNotCommitted;
	int64_t transactionsMaybeCommitted; 
	ContinuousSample<double> latencies, readLatencies, commitLatencies, GRVLatencies, mutationsPerCommit, bytesPerCommit;

	int outstandingWatches;
	int maxOutstandingWatches;

	Future<Void> logger;

	int taskID;

	Int64MetricHandle getValueSubmitted;
	EventMetricHandle<GetValueComplete> getValueCompleted;
};

#endif
