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

#include "fdbclient/ClientDBInfoRef.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/MultiInterface.h"
#include "fdbrpc/Locality.h"
#include "flow/TDMetric.actor.h"
#include "fdbclient/EventTypes.actor.h"
#include "fdbrpc/ContinuousSample.h"

struct DatabaseContextImpl;
using LocationInfo = MultiInterface<ReferencedInterface<StorageServerInterface>>;
using ProxyInfo = MultiInterface<struct MasterProxyInterface>;


class DatabaseContext : public ReferenceCounted<DatabaseContext>, public FastAllocated<DatabaseContext>, NonCopyable {
	DatabaseContextImpl* impl = nullptr;
	explicit DatabaseContext() {}
public:
	static DatabaseContext* allocateOnForeignThread() {
		return (DatabaseContext*)DatabaseContext::operator new(sizeof(DatabaseContext));
	}

	// For internal (fdbserver) use only
	static Database create(ClientDBInfoRef clientInfo, Future<Void> clientInfoMonitor,
	                       LocalityData clientLocality, bool enableLocalityLoadBalance,
	                       TaskPriority taskID = TaskPriority::DefaultEndpoint, bool lockAware = false,
	                       int apiVersion = Database::API_VERSION_LATEST, bool switchable = false);

	~DatabaseContext();

	Database clone() const;

	void invalidateCache( const KeyRef&, bool isBackward = false );
	void invalidateCache( const KeyRangeRef& );

	Reference<ProxyInfo> getMasterProxies(bool useProvisionalProxies);
	Future<Reference<ProxyInfo>> getMasterProxiesFuture(bool useProvisionalProxies);
	Future<Void> onMasterProxiesChanged();
	Future<HealthMetrics> getHealthMetrics(bool detailed);

	// Update the watch counter for the database
	void addWatch();
	void removeWatch();
	
	void setOption( FDBDatabaseOptions::Option option, Optional<StringRef> value );

	bool isError() const;

	void checkDeferredError() const;

	int apiVersionAtLeast(int minVersion) const;

	void validateVersion(Version version) const;
	TaskPriority taskID() const;
	bool switchable() const;
	bool& enableLocalityLoadBalance();
	int& apiVersion();
	HealthMetrics& healthMetrics();
	int& maxOutstandingWatches();

	Future<Void> onConnected(); // Returns after a majority of coordination servers are available and have reported a leader. The cluster file therefore is valid, but the database might be unavailable.
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
	int snapshotRywEnabled() const;
	void setLocationCacheSize(int sz);

//private: 
	explicit DatabaseContext( Reference<AsyncVar<Reference<ClusterConnectionFile>>> connectionFile, ClientDBInfoRef clientDBInfo,
		Future<Void> clientInfoMonitor, TaskPriority taskID, LocalityData const& clientLocality, 
		bool enableLocalityLoadBalance, bool lockAware, bool internal = true, int apiVersion = Database::API_VERSION_LATEST, bool switchable = false );

	explicit DatabaseContext( const Error &err );
	// for NativeAPI use only
	DatabaseContextImpl* getImpl() { return impl; }
	const DatabaseContextImpl* getImpl() const { return impl; }

	Error deferredError;
};

#endif
