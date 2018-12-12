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

#include "fdbclient/NativeAPI.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/MasterProxyInterface.h"
#include "fdbclient/ClientDBInfo.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/MultiInterface.h"
#include "flow/TDMetric.actor.h"
#include "fdbclient/EventTypes.actor.h"
#include "fdbrpc/ContinuousSample.h"

class StorageServerInfo : public ReferencedInterface<StorageServerInterface> {
public:
	static Reference<StorageServerInfo> getInterface( DatabaseContext *cx, StorageServerInterface const& interf, LocalityData const& locality );
	void notifyContextDestroyed();

	virtual ~StorageServerInfo();
private:
	DatabaseContext *cx;
	StorageServerInfo( DatabaseContext *cx, StorageServerInterface const& interf, LocalityData const& locality ) : cx(cx), ReferencedInterface<StorageServerInterface>(interf, locality) {}
};

typedef MultiInterface<ReferencedInterface<StorageServerInterface>> LocationInfo;
typedef MultiInterface<MasterProxyInterface> ProxyInfo;

class DatabaseContext : public ReferenceCounted<DatabaseContext>, NonCopyable {
public:
	// For internal (fdbserver) use only
	static Database create( Reference<AsyncVar<Optional<ClusterInterface>>> clusterInterface, Reference<ClusterConnectionFile> connFile, LocalityData const& clientLocality );
	static Database create( Reference<AsyncVar<ClientDBInfo>> clientInfo, Future<Void> clientInfoMonitor, LocalityData clientLocality, bool enableLocalityLoadBalance, int taskID=TaskDefaultEndpoint, bool lockAware=false, int apiVersion=Database::API_VERSION_LATEST );

	~DatabaseContext();

	Database clone() const { return Database(new DatabaseContext( cluster, clientInfo, clientInfoMonitor, dbId, taskID, clientLocality, enableLocalityLoadBalance, lockAware, apiVersion )); }

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

	Error deferredError;
	bool lockAware;

	void checkDeferredError() {
		if( deferredError.code() != invalid_error_code ) {
			throw deferredError;
		}
	}

	int apiVersionAtLeast(int minVersion) { return apiVersion < 0 || apiVersion >= minVersion; }

	Future<Void> onConnected(); // Returns after a majority of coordination servers are available and have reported a leader. The cluster file therefore is valid, but the database might be unavailable.
	Reference<ClusterConnectionFile> getConnectionFile();

//private: 
	explicit DatabaseContext( Reference<Cluster> cluster, Reference<AsyncVar<ClientDBInfo>> clientDBInfo,
		Future<Void> clientInfoMonitor, Standalone<StringRef> dbId, int taskID, LocalityData const& clientLocality, 
		bool enableLocalityLoadBalance, bool lockAware, int apiVersion = Database::API_VERSION_LATEST );

	// Key DB-specific information
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

	std::map< UID, StorageServerInfo* > server_interf;

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
	int64_t transactionsResourceConstrained;
	ContinuousSample<double> latencies, readLatencies, commitLatencies, GRVLatencies, mutationsPerCommit, bytesPerCommit;

	int outstandingWatches;
	int maxOutstandingWatches;

	Future<Void> logger;

	int taskID;

	Int64MetricHandle getValueSubmitted;
	EventMetricHandle<GetValueComplete> getValueCompleted;

	Reference<AsyncVar<ClientDBInfo>> clientInfo;
	Future<Void> clientInfoMonitor;

	Reference<Cluster> cluster;

	int apiVersion;
};

#endif
