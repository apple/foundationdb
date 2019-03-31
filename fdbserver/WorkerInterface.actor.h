/*
 * WorkerInterface.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_WORKERINTERFACE_ACTOR_G_H)
	#define FDBSERVER_WORKERINTERFACE_ACTOR_G_H
	#include "fdbserver/WorkerInterface.actor.g.h"
#elif !defined(FDBSERVER_WORKERINTERFACE_ACTOR_H)
	#define FDBSERVER_WORKERINTERFACE_ACTOR_H

#include "fdbserver/DataDistributorInterface.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/ResolverInterface.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbrpc/MultiInterface.h"
#include "fdbclient/ClientWorkerInterface.h"
#include "flow/actorcompiler.h"

#define DUMPTOKEN( name ) TraceEvent("DumpToken", recruited.id()).detail("Name", #name).detail("Token", name.getEndpoint().token)

struct WorkerInterface {
	ClientWorkerInterface clientInterface;
	LocalityData locality;
	RequestStream< struct InitializeTLogRequest > tLog;
	RequestStream< struct RecruitMasterRequest > master;
	RequestStream< struct InitializeMasterProxyRequest > masterProxy;
	RequestStream< struct InitializeDataDistributorRequest > dataDistributor;
	RequestStream< struct InitializeRatekeeperRequest > ratekeeper;
	RequestStream< struct InitializeResolverRequest > resolver;
	RequestStream< struct InitializeStorageRequest > storage;
	RequestStream< struct InitializeLogRouterRequest > logRouter;

	RequestStream< struct LoadedPingRequest > debugPing;
	RequestStream< struct CoordinationPingMessage > coordinationPing;
	RequestStream< ReplyPromise<Void> > waitFailure;
	RequestStream< struct SetMetricsLogRateRequest > setMetricsRate;
	RequestStream< struct EventLogRequest > eventLogRequest;
	RequestStream< struct TraceBatchDumpRequest > traceBatchDumpRequest;
	RequestStream< struct DiskStoreRequest > diskStoreRequest;

	TesterInterface testerInterface;

	UID id() const { return tLog.getEndpoint().token; }
	NetworkAddress address() const { return tLog.getEndpoint().getPrimaryAddress(); }

	WorkerInterface() {}
	WorkerInterface( const LocalityData& locality ) : locality( locality ) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, clientInterface, locality, tLog, master, masterProxy, dataDistributor, ratekeeper, resolver, storage, logRouter, debugPing, coordinationPing, waitFailure, setMetricsRate, eventLogRequest, traceBatchDumpRequest, testerInterface, diskStoreRequest);
	}
};

struct WorkerDetails {
	WorkerInterface interf;
	ProcessClass processClass;
	bool degraded;

	WorkerDetails() : degraded(false) {}
	WorkerDetails(const WorkerInterface& interf, ProcessClass processClass, bool degraded) : interf(interf), processClass(processClass), degraded(degraded) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, interf, processClass, degraded);
	}
};

struct InitializeTLogRequest {
	UID recruitmentID;
	LogSystemConfig recoverFrom;
	Version recoverAt;
	Version knownCommittedVersion;
	LogEpoch epoch;
	std::vector<Tag> recoverTags;
	std::vector<Tag> allTags;
	TLogVersion logVersion;
	KeyValueStoreType storeType;
	TLogSpillType spillType;
	Tag remoteTag;
	int8_t locality;
	bool isPrimary;
	Version startVersion;
	int logRouterTags;

	ReplyPromise< struct TLogInterface > reply;

	InitializeTLogRequest() {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, recruitmentID, recoverFrom, recoverAt, knownCommittedVersion, epoch, recoverTags, allTags, storeType, remoteTag, locality, isPrimary, startVersion, logRouterTags, reply, logVersion, spillType);
	}
};

struct InitializeLogRouterRequest {
	uint64_t recoveryCount;
	Tag routerTag;
	Version startVersion;
	std::vector<LocalityData> tLogLocalities;
	Reference<IReplicationPolicy> tLogPolicy;
	int8_t locality;
	ReplyPromise<struct TLogInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, recoveryCount, routerTag, startVersion, tLogLocalities, tLogPolicy, locality, reply);
	}
};

// FIXME: Rename to InitializeMasterRequest, etc
struct RecruitMasterRequest {
	Arena arena;
	LifetimeToken lifetime;
	bool forceRecovery;
	ReplyPromise< struct MasterInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		ASSERT( ar.protocolVersion() >= 0x0FDB00A200040001LL );
		serializer(ar, lifetime, forceRecovery, reply, arena);
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
		serializer(ar, master, recoveryCount, recoveryTransactionVersion, firstProxy, reply);
	}
};

struct InitializeDataDistributorRequest {
	UID reqId;
	ReplyPromise<DataDistributorInterface> reply;

	InitializeDataDistributorRequest() {}
	explicit InitializeDataDistributorRequest(UID uid) : reqId(uid) {}
	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, reqId, reply);
	}
};

struct InitializeRatekeeperRequest {
	UID reqId;
	ReplyPromise<RatekeeperInterface> reply;

	InitializeRatekeeperRequest() {}
	explicit InitializeRatekeeperRequest(UID uid) : reqId(uid) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reqId, reply);
	}
};

struct InitializeResolverRequest {
	uint64_t recoveryCount;
	int proxyCount;
	int resolverCount;
	ReplyPromise<ResolverInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, recoveryCount, proxyCount, resolverCount, reply);
	}
};

struct InitializeStorageReply {
	StorageServerInterface interf;
	Version addedVersion;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, interf, addedVersion);
	}
};

struct InitializeStorageRequest {
	Tag seedTag;									//< If this server will be passed to seedShardServers, this will be a tag, otherwise it is invalidTag
	UID reqId;
	UID interfaceId;
	KeyValueStoreType storeType;
	ReplyPromise< InitializeStorageReply > reply;

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, seedTag, reqId, interfaceId, storeType, reply);
	}
};

struct TraceBatchDumpRequest {
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, reply);
	}
};

struct LoadedReply {
	Standalone<StringRef> payload;
	UID id;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, payload, id);
	}
};

struct LoadedPingRequest {
	UID id;
	bool loadReply;
	Standalone<StringRef> payload;
	ReplyPromise<LoadedReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, loadReply, payload, reply);
	}
};

struct CoordinationPingMessage {
	UID clusterControllerId;
	int64_t timeStep;

	CoordinationPingMessage() : timeStep(0) {}
	CoordinationPingMessage(UID ccId, uint64_t step) : clusterControllerId( ccId ), timeStep( step ) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, clusterControllerId, timeStep);
	}
};

struct SetMetricsLogRateRequest {
	uint32_t metricsLogsPerSecond;

	SetMetricsLogRateRequest() : metricsLogsPerSecond( 1 ) {}
	explicit SetMetricsLogRateRequest(uint32_t logsPerSecond) : metricsLogsPerSecond( logsPerSecond ) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, metricsLogsPerSecond);
	}
};

struct EventLogRequest {
	bool getLastError;
	Standalone<StringRef> eventName;
	ReplyPromise< TraceEventFields > reply;

	EventLogRequest() : getLastError(true) {}
	explicit EventLogRequest( Standalone<StringRef> eventName ) : eventName( eventName ), getLastError( false ) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, getLastError, eventName, reply);
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
		serializer(ar, time, address, context, version, mutation);
	}
};

struct DiskStoreRequest {
	bool includePartialStores;
	ReplyPromise<Standalone<VectorRef<UID>>> reply;

	DiskStoreRequest(bool includePartialStores=false) : includePartialStores(includePartialStores) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, includePartialStores, reply);
	}
};

struct Role {
	static const Role WORKER;
	static const Role STORAGE_SERVER;
	static const Role TRANSACTION_LOG;
	static const Role SHARED_TRANSACTION_LOG;
	static const Role MASTER_PROXY;
	static const Role MASTER;
	static const Role RESOLVER;
	static const Role CLUSTER_CONTROLLER;
	static const Role TESTER;
	static const Role LOG_ROUTER;
	static const Role DATA_DISTRIBUTOR;
	static const Role RATEKEEPER;

	std::string roleName;
	std::string abbreviation;
	bool includeInTraceRoles;

	bool operator==(const Role &r) const {
		return roleName == r.roleName;
	}
	bool operator!=(const Role &r) const {
		return !(*this == r);
	}

private:
	Role(std::string roleName, std::string abbreviation, bool includeInTraceRoles=true) : roleName(roleName), abbreviation(abbreviation), includeInTraceRoles(includeInTraceRoles) {
		ASSERT(abbreviation.size() == 2); // Having a fixed size makes log queries more straightforward
	}
};

void startRole(const Role &role, UID roleId, UID workerId, std::map<std::string, std::string> details = std::map<std::string, std::string>(), std::string origination = "Recruited");
void endRole(const Role &role, UID id, std::string reason, bool ok = true, Error e = Error());

struct ServerDBInfo;

class Database openDBOnServer( Reference<AsyncVar<ServerDBInfo>> const& db, int taskID = TaskDefaultEndpoint, bool enableLocalityLoadBalance = true, bool lockAware = false );
ACTOR Future<Void> extractClusterInterface(Reference<AsyncVar<Optional<struct ClusterControllerFullInterface>>> a,
                                           Reference<AsyncVar<Optional<struct ClusterInterface>>> b);

ACTOR Future<Void> fdbd(Reference<ClusterConnectionFile> ccf, LocalityData localities, ProcessClass processClass,
                        std::string dataFolder, std::string coordFolder, int64_t memoryLimit,
                        std::string metricsConnFile, std::string metricsPrefix);
ACTOR Future<Void> clusterController(Reference<ClusterConnectionFile> ccf,
                                     Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> currentCC,
                                     Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo,
                                     Future<Void> recoveredDiskFiles, LocalityData locality);

// These servers are started by workerServer
class IKeyValueStore;
class ServerCoordinators;
class IDiskQueue;
ACTOR Future<Void> storageServer(IKeyValueStore* persistentData, StorageServerInterface ssi, Tag seedTag,
                                 ReplyPromise<InitializeStorageReply> recruitReply,
                                 Reference<AsyncVar<ServerDBInfo>> db, std::string folder);
ACTOR Future<Void> storageServer(IKeyValueStore* persistentData, StorageServerInterface ssi,
                                 Reference<AsyncVar<ServerDBInfo>> db, std::string folder,
                                 Promise<Void> recovered); // changes pssi->id() to be the recovered ID
ACTOR Future<Void> masterServer(MasterInterface mi, Reference<AsyncVar<ServerDBInfo>> db,
                                ServerCoordinators serverCoordinators, LifetimeToken lifetime, bool forceRecovery);
ACTOR Future<Void> masterProxyServer(MasterProxyInterface proxy, InitializeMasterProxyRequest req,
                                     Reference<AsyncVar<ServerDBInfo>> db);
ACTOR Future<Void> tLog(IKeyValueStore* persistentData, IDiskQueue* persistentQueue,
                        Reference<AsyncVar<ServerDBInfo>> db, LocalityData locality,
                        PromiseStream<InitializeTLogRequest> tlogRequests, UID tlogId, bool restoreFromDisk,
                        Promise<Void> oldLog, Promise<Void> recovered, Reference<AsyncVar<bool>> degraded); // changes tli->id() to be the recovered ID
ACTOR Future<Void> monitorServerDBInfo(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> ccInterface,
                                       Reference<ClusterConnectionFile> ccf, LocalityData locality,
                                       Reference<AsyncVar<ServerDBInfo>> dbInfo);
ACTOR Future<Void> resolver(ResolverInterface proxy, InitializeResolverRequest initReq,
                            Reference<AsyncVar<ServerDBInfo>> db);
ACTOR Future<Void> logRouter(TLogInterface interf, InitializeLogRouterRequest req,
                             Reference<AsyncVar<ServerDBInfo>> db);
ACTOR Future<Void> dataDistributor(DataDistributorInterface ddi, Reference<AsyncVar<ServerDBInfo>> db);
ACTOR Future<Void> ratekeeper(RatekeeperInterface rki, Reference<AsyncVar<ServerDBInfo>> db);

void registerThreadForProfiling();
void updateCpuProfiler(ProfilerRequest req);

namespace oldTLog_4_6 {
ACTOR Future<Void> tLog(IKeyValueStore* persistentData, IDiskQueue* persistentQueue,
                        Reference<AsyncVar<ServerDBInfo>> db, LocalityData locality, UID tlogId);
}
namespace oldTLog_6_0 {
ACTOR Future<Void> tLog(IKeyValueStore* persistentData, IDiskQueue* persistentQueue,
                        Reference<AsyncVar<ServerDBInfo>> db, LocalityData locality,
                        PromiseStream<InitializeTLogRequest> tlogRequests, UID tlogId, bool restoreFromDisk,
                        Promise<Void> oldLog, Promise<Void> recovered, Reference<AsyncVar<bool>> degraded);
}

typedef decltype(&tLog) TLogFn;

#include "flow/unactorcompiler.h"
#endif
