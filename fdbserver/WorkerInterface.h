/*
 * WorkerInterface.h
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

#ifndef FDBSERVER_WORKERINTERFACE_H
#define FDBSERVER_WORKERINTERFACE_H
#pragma once

#include "MasterInterface.h"
#include "TLogInterface.h"
#include "ResolverInterface.h"
#include "fdbclient/StorageServerInterface.h"
#include "TesterInterface.h"
#include "fdbclient/FDBTypes.h"
#include "LogSystemConfig.h"
#include "fdbrpc/MultiInterface.h"
#include "fdbclient/ClientWorkerInterface.h"

#define DUMPTOKEN( name ) TraceEvent("DumpToken", recruited.id()).detail("Name", #name).detail("Token", name.getEndpoint().token)

struct WorkerInterface {
	ClientWorkerInterface clientInterface;
	LocalityData locality;
	RequestStream< struct InitializeTLogRequest > tLog;
	RequestStream< struct RecruitMasterRequest > master;
	RequestStream< struct InitializeMasterProxyRequest > masterProxy;
	RequestStream< struct InitializeResolverRequest > resolver;
	RequestStream< struct InitializeStorageRequest > storage;

	RequestStream< struct DebugQueryRequest > debugQuery;
	RequestStream< struct LoadedPingRequest > debugPing;
	RequestStream< struct CoordinationPingMessage > coordinationPing;
	RequestStream< ReplyPromise<Void> > waitFailure;
	RequestStream< struct SetMetricsLogRateRequest > setMetricsRate;
	RequestStream< struct EventLogRequest > eventLogRequest;
	RequestStream< struct TraceBatchDumpRequest > traceBatchDumpRequest;
	RequestStream< struct DiskStoreRequest > diskStoreRequest;

	TesterInterface testerInterface;

	UID id() const { return tLog.getEndpoint().token; }
	NetworkAddress address() const { return tLog.getEndpoint().address; }

	WorkerInterface() {}
	WorkerInterface( LocalityData locality ) : locality( locality ) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & clientInterface & locality & tLog & master & masterProxy & resolver & storage & debugQuery & debugPing & coordinationPing & waitFailure & setMetricsRate & eventLogRequest & traceBatchDumpRequest & testerInterface & diskStoreRequest;
	}
};

struct InitializeTLogRequest {
	UID recruitmentID;
	LogSystemConfig recoverFrom;
	Version recoverAt;
	Version knownCommittedVersion;
	LogEpoch epoch;
	std::vector<Tag> recoverTags;
	KeyValueStoreType storeType;
	ReplyPromise< struct TLogInterface > reply;

	InitializeTLogRequest() {}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & recruitmentID & recoverFrom & recoverAt & knownCommittedVersion & epoch & recoverTags & storeType & reply;
	}
};

// FIXME: Rename to InitializeMasterRequest, etc
struct RecruitMasterRequest {
	Arena arena;
	LifetimeToken lifetime;
	ReplyPromise< struct MasterInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		ASSERT( ar.protocolVersion() >= 0x0FDB00A200040001LL );
		ar & lifetime & reply & arena;
	}
};

struct InitializeMasterProxyRequest {
	MasterInterface master;
	uint64_t recoveryCount;
	Version recoveryTransactionVersion;
	bool firstProxy;
	ReplyPromise<MasterProxyInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & master & recoveryCount & recoveryTransactionVersion & firstProxy & reply;
	}
};

struct InitializeResolverRequest {
	uint64_t recoveryCount;
	int proxyCount;
	int resolverCount;
	ReplyPromise<ResolverInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & recoveryCount & proxyCount & resolverCount & reply;
	}
};

struct InitializeStorageRequest {
	Tag seedTag;									//< If this server will be passed to seedShardServers, this will be a tag, otherwise it is invalidTag
	UID reqId;
	UID interfaceId;
	KeyValueStoreType storeType;
	ReplyPromise< struct StorageServerInterface > reply;

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & seedTag & reqId & interfaceId & storeType & reply;
	}
};

struct TraceBatchDumpRequest {
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & reply;
	}
};

struct LoadedReply {
	Standalone<StringRef> payload;
	UID id;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & payload & id;
	}
};

struct LoadedPingRequest {
	UID id;
	bool loadReply;
	Standalone<StringRef> payload;
	ReplyPromise<LoadedReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & id & loadReply & payload & reply;
	}
};

struct CoordinationPingMessage {
	UID clusterControllerId;
	int64_t timeStep;

	CoordinationPingMessage() : timeStep(0) {}
	CoordinationPingMessage(UID ccId, uint64_t step) : clusterControllerId( ccId ), timeStep( step ) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & clusterControllerId & timeStep;
	}
};

struct SetMetricsLogRateRequest {
	uint32_t metricsLogsPerSecond;

	SetMetricsLogRateRequest() : metricsLogsPerSecond( 1 ) {}
	explicit SetMetricsLogRateRequest(uint32_t logsPerSecond) : metricsLogsPerSecond( logsPerSecond ) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & metricsLogsPerSecond;
	}
};

struct EventLogRequest {
	bool getLastError;
	Standalone<StringRef> eventName;
	ReplyPromise< Standalone<StringRef> > reply;

	EventLogRequest() : getLastError(true) {}
	explicit EventLogRequest( Standalone<StringRef> eventName ) : eventName( eventName ), getLastError( false ) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & getLastError & eventName & reply;
	}
};

struct DebugQueryRequest {
	Standalone<StringRef> search;
	ReplyPromise< Standalone< VectorRef<struct DebugEntryRef> > > reply;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & search & reply;
	}
};

struct DebugEntryRef {
	double time;
	NetworkAddress address;
	StringRef context;
	Version version;
	MutationRef mutation;
	DebugEntryRef() {}
	DebugEntryRef( const char* c, Version v, MutationRef const& m ) : context((const uint8_t*)c,strlen(c)), version(v), mutation(m), time(now()), address( g_network->getLocalAddress() ) {}
	DebugEntryRef( Arena& a, DebugEntryRef const& d ) : time(d.time), address(d.address), context(d.context), version(d.version), mutation(a, d.mutation) {}

	size_t expectedSize() const {
		return context.expectedSize() + mutation.expectedSize();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & time & address & context & version & mutation;
	}
};

struct DiskStoreRequest {
	bool includePartialStores;
	ReplyPromise<Standalone<VectorRef<UID>>> reply;

	DiskStoreRequest(bool includePartialStores=false) : includePartialStores(includePartialStores) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & includePartialStores & reply;
	}
};

void startRole(UID roleId, UID workerId, std::string as, std::map<std::string, std::string> details = std::map<std::string, std::string>(), std::string origination = "Recruited");
void endRole(UID id, std::string as, std::string reason, bool ok = true, Error e = Error());

struct ServerDBInfo;

class Database openDBOnServer( Reference<AsyncVar<ServerDBInfo>> const& db, int taskID = TaskDefaultEndpoint, bool enableLocalityLoadBalance = true, bool lockAware = false );
Future<Void> extractClusterInterface( Reference<AsyncVar<Optional<struct ClusterControllerFullInterface>>> const& a, Reference<AsyncVar<Optional<struct ClusterInterface>>> const& b );

Future<Void> fdbd( Reference<ClusterConnectionFile> const&, LocalityData const& localities, ProcessClass const& processClass, std::string const& dataFolder, std::string const& coordFolder, int64_t const& memoryLimit, std::string const& metricsConnFile, std::string const& metricsPrefix );
Future<Void> workerServer( Reference<ClusterConnectionFile> const&, Reference<AsyncVar<Optional<struct ClusterControllerFullInterface>>> const& ccInterface, LocalityData const& localities, Reference<AsyncVar<ProcessClass>> const& asyncProcessClass, ProcessClass const& initialClass, Reference<AsyncVar<bool>> const& asyncIsExcluded, std::string const& filename, int64_t const& memoryLimit, Future<Void> const& forceFailure, std::string const& metricsConnFile, std::string const& metricsPrefix );
Future<Void> clusterController( Reference<ClusterConnectionFile> const&, Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> const& currentCC, Reference<AsyncVar<ProcessClass>> const& asyncProcessClass, Reference<AsyncVar<bool>> const& asyncIsExcluded );

// These servers are started by workerServer
Future<Void> storageServer(
				class IKeyValueStore* const& persistentData,
				StorageServerInterface const& ssi,
				Tag const& seedTag,
				ReplyPromise<StorageServerInterface> const& recruitReply,
				Reference<AsyncVar<ServerDBInfo>> const& db,
				std::string const& folder );
Future<Void> storageServer(
				class IKeyValueStore* const& persistentData,
				StorageServerInterface const& ssi,
				Reference<AsyncVar<ServerDBInfo>> const& db,
				std::string const& folder,
				Promise<Void> const& recovered);  // changes pssi->id() to be the recovered ID
Future<Void> masterServer( MasterInterface const& mi, Reference<AsyncVar<ServerDBInfo>> const& db, class ServerCoordinators const&, LifetimeToken const& lifetime );
Future<Void> masterProxyServer(MasterProxyInterface const& proxy, InitializeMasterProxyRequest const& req, Reference<AsyncVar<ServerDBInfo>> const& db);
Future<Void> tLog( class IKeyValueStore* const& persistentData, class IDiskQueue* const& persistentQueue, Reference<AsyncVar<ServerDBInfo>> const& db, LocalityData const& locality, PromiseStream<InitializeTLogRequest> const& tlogRequests, UID const& tlogId, bool const& restoreFromDisk, Promise<Void> const& oldLog, Promise<Void> const& recovered );  // changes tli->id() to be the recovered ID
Future<Void> debugQueryServer( DebugQueryRequest const& req );
Future<Void> monitorServerDBInfo( Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> const& ccInterface, Reference<ClusterConnectionFile> const&, LocalityData const&, Reference<AsyncVar<ServerDBInfo>> const& dbInfo );
Future<Void> resolver( ResolverInterface const& proxy, InitializeResolverRequest const&, Reference<AsyncVar<ServerDBInfo>> const& db );

void registerThreadForProfiling();
void updateCpuProfiler(ProfilerRequest req);

namespace oldTLog {
	Future<Void> tLog( IKeyValueStore* const& persistentData, IDiskQueue* const& persistentQueue, TLogInterface const& tli, Reference<AsyncVar<ServerDBInfo>> const& db );
}

#endif
