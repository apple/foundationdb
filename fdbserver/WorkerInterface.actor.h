/*
 * WorkerInterface.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_WORKERINTERFACE_ACTOR_G_H)
#define FDBSERVER_WORKERINTERFACE_ACTOR_G_H
#include "fdbserver/WorkerInterface.actor.g.h"
#elif !defined(FDBSERVER_WORKERINTERFACE_ACTOR_H)
#define FDBSERVER_WORKERINTERFACE_ACTOR_H

#include "fdbserver/BackupInterface.h"
#include "fdbserver/DataDistributorInterface.h"
#include "fdbserver/EncryptKeyProxyInterface.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/BlobManagerInterface.h"
#include "fdbserver/ResolverInterface.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbrpc/MultiInterface.h"
#include "fdbclient/ClientWorkerInterface.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ConfigBroadcastInterface.h"
#include "flow/actorcompiler.h"

struct WorkerInterface {
	constexpr static FileIdentifier file_identifier = 14712718;
	ClientWorkerInterface clientInterface;
	LocalityData locality;
	RequestStream<struct InitializeTLogRequest> tLog;
	RequestStream<struct RecruitMasterRequest> master;
	RequestStream<struct InitializeCommitProxyRequest> commitProxy;
	RequestStream<struct InitializeGrvProxyRequest> grvProxy;
	RequestStream<struct InitializeDataDistributorRequest> dataDistributor;
	RequestStream<struct InitializeRatekeeperRequest> ratekeeper;
	RequestStream<struct InitializeBlobManagerRequest> blobManager;
	RequestStream<struct InitializeBlobWorkerRequest> blobWorker;
	RequestStream<struct InitializeResolverRequest> resolver;
	RequestStream<struct InitializeStorageRequest> storage;
	RequestStream<struct InitializeLogRouterRequest> logRouter;
	RequestStream<struct InitializeBackupRequest> backup;
	RequestStream<struct InitializeEncryptKeyProxyRequest> encryptKeyProxy;

	RequestStream<struct LoadedPingRequest> debugPing;
	RequestStream<struct CoordinationPingMessage> coordinationPing;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct SetMetricsLogRateRequest> setMetricsRate;
	RequestStream<struct EventLogRequest> eventLogRequest;
	RequestStream<struct TraceBatchDumpRequest> traceBatchDumpRequest;
	RequestStream<struct DiskStoreRequest> diskStoreRequest;
	RequestStream<struct ExecuteRequest> execReq;
	RequestStream<struct WorkerSnapRequest> workerSnapReq;
	RequestStream<struct UpdateServerDBInfoRequest> updateServerDBInfo;

	ConfigBroadcastInterface configBroadcastInterface;
	TesterInterface testerInterface;

	UID id() const { return tLog.getEndpoint().token; }
	NetworkAddress address() const { return tLog.getEndpoint().getPrimaryAddress(); }
	NetworkAddress stableAddress() const { return tLog.getEndpoint().getStableAddress(); }
	Optional<NetworkAddress> secondaryAddress() const { return tLog.getEndpoint().addresses.secondaryAddress; }
	NetworkAddressList addresses() const { return tLog.getEndpoint().addresses; }

	WorkerInterface() {}
	WorkerInterface(const LocalityData& locality) : locality(locality) {}

	void initEndpoints() {
		clientInterface.initEndpoints();
		tLog.getEndpoint(TaskPriority::Worker);
		master.getEndpoint(TaskPriority::Worker);
		commitProxy.getEndpoint(TaskPriority::Worker);
		grvProxy.getEndpoint(TaskPriority::Worker);
		resolver.getEndpoint(TaskPriority::Worker);
		logRouter.getEndpoint(TaskPriority::Worker);
		debugPing.getEndpoint(TaskPriority::Worker);
		coordinationPing.getEndpoint(TaskPriority::Worker);
		updateServerDBInfo.getEndpoint(TaskPriority::Worker);
		eventLogRequest.getEndpoint(TaskPriority::Worker);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           clientInterface,
		           locality,
		           tLog,
		           master,
		           commitProxy,
		           grvProxy,
		           dataDistributor,
		           ratekeeper,
		           blobManager,
		           blobWorker,
		           resolver,
		           storage,
		           logRouter,
		           debugPing,
		           coordinationPing,
		           waitFailure,
		           setMetricsRate,
		           eventLogRequest,
		           traceBatchDumpRequest,
		           testerInterface,
		           diskStoreRequest,
		           execReq,
		           workerSnapReq,
		           backup,
		           encryptKeyProxy,
		           updateServerDBInfo,
		           configBroadcastInterface);
	}
};

struct WorkerDetails {
	constexpr static FileIdentifier file_identifier = 9973980;
	WorkerInterface interf;
	ProcessClass processClass;
	bool degraded;

	WorkerDetails() : degraded(false) {}
	WorkerDetails(const WorkerInterface& interf, ProcessClass processClass, bool degraded)
	  : interf(interf), processClass(processClass), degraded(degraded) {}

	bool operator<(const WorkerDetails& r) const { return interf.id() < r.interf.id(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, interf, processClass, degraded);
	}
};

// This interface and its serialization depend on slicing, since the client will deserialize only the first part of this
// structure
struct ClusterControllerFullInterface {
	constexpr static FileIdentifier file_identifier = ClusterControllerClientInterface::file_identifier;
	ClusterInterface clientInterface;
	RequestStream<struct RecruitFromConfigurationRequest> recruitFromConfiguration;
	RequestStream<struct RecruitRemoteFromConfigurationRequest> recruitRemoteFromConfiguration;
	RequestStream<struct RecruitStorageRequest> recruitStorage;
	RequestStream<struct RecruitBlobWorkerRequest> recruitBlobWorker;
	RequestStream<struct RegisterWorkerRequest> registerWorker;
	RequestStream<struct GetWorkersRequest> getWorkers;
	RequestStream<struct RegisterMasterRequest> registerMaster;
	RequestStream<struct GetServerDBInfoRequest>
	    getServerDBInfo; // only used by testers; the cluster controller will send the serverDBInfo to workers
	RequestStream<struct UpdateWorkerHealthRequest> updateWorkerHealth;
	RequestStream<struct TLogRejoinRequest>
	    tlogRejoin; // sent by tlog (whether or not rebooted) to communicate with a new controller
	RequestStream<struct BackupWorkerDoneRequest> notifyBackupWorkerDone;
	RequestStream<struct ChangeCoordinatorsRequest> changeCoordinators;

	UID id() const { return clientInterface.id(); }
	bool operator==(ClusterControllerFullInterface const& r) const { return id() == r.id(); }
	bool operator!=(ClusterControllerFullInterface const& r) const { return id() != r.id(); }

	NetworkAddress address() const { return clientInterface.address(); }

	bool hasMessage() const {
		return clientInterface.hasMessage() || recruitFromConfiguration.getFuture().isReady() ||
		       recruitRemoteFromConfiguration.getFuture().isReady() || recruitStorage.getFuture().isReady() ||
		       recruitBlobWorker.getFuture().isReady() || registerWorker.getFuture().isReady() ||
		       getWorkers.getFuture().isReady() || registerMaster.getFuture().isReady() ||
		       getServerDBInfo.getFuture().isReady() || updateWorkerHealth.getFuture().isReady() ||
		       tlogRejoin.getFuture().isReady() || notifyBackupWorkerDone.getFuture().isReady() ||
		       changeCoordinators.getFuture().isReady();
	}

	void initEndpoints() {
		clientInterface.initEndpoints();
		recruitFromConfiguration.getEndpoint(TaskPriority::ClusterControllerRecruit);
		recruitRemoteFromConfiguration.getEndpoint(TaskPriority::ClusterControllerRecruit);
		recruitStorage.getEndpoint(TaskPriority::ClusterController);
		recruitBlobWorker.getEndpoint(TaskPriority::ClusterController);
		registerWorker.getEndpoint(TaskPriority::ClusterControllerWorker);
		getWorkers.getEndpoint(TaskPriority::ClusterController);
		registerMaster.getEndpoint(TaskPriority::ClusterControllerRegister);
		getServerDBInfo.getEndpoint(TaskPriority::ClusterController);
		updateWorkerHealth.getEndpoint(TaskPriority::ClusterController);
		tlogRejoin.getEndpoint(TaskPriority::MasterTLogRejoin);
		notifyBackupWorkerDone.getEndpoint(TaskPriority::ClusterController);
		changeCoordinators.getEndpoint(TaskPriority::DefaultEndpoint);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (!is_fb_function<Ar>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar,
		           clientInterface,
		           recruitFromConfiguration,
		           recruitRemoteFromConfiguration,
		           recruitStorage,
		           recruitBlobWorker,
		           registerWorker,
		           getWorkers,
		           registerMaster,
		           getServerDBInfo,
		           updateWorkerHealth,
		           tlogRejoin,
		           notifyBackupWorkerDone,
		           changeCoordinators);
	}
};

struct RegisterWorkerReply {
	constexpr static FileIdentifier file_identifier = 16475696;
	ProcessClass processClass;
	ClusterControllerPriorityInfo priorityInfo;

	RegisterWorkerReply()
	  : priorityInfo(ProcessClass::UnsetFit, false, ClusterControllerPriorityInfo::FitnessUnknown) {}
	RegisterWorkerReply(ProcessClass processClass, ClusterControllerPriorityInfo priorityInfo)
	  : processClass(processClass), priorityInfo(priorityInfo) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, processClass, priorityInfo);
	}
};

struct RegisterMasterRequest {
	constexpr static FileIdentifier file_identifier = 10773445;
	UID id;
	LocalityData mi;
	LogSystemConfig logSystemConfig;
	std::vector<CommitProxyInterface> commitProxies;
	std::vector<GrvProxyInterface> grvProxies;
	std::vector<ResolverInterface> resolvers;
	DBRecoveryCount recoveryCount;
	int64_t registrationCount;
	Optional<DatabaseConfiguration> configuration;
	std::vector<UID> priorCommittedLogServers;
	RecoveryState recoveryState;
	bool recoveryStalled;
	UID clusterId;

	ReplyPromise<Void> reply;

	RegisterMasterRequest() : logSystemConfig(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (!is_fb_function<Ar>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar,
		           id,
		           mi,
		           logSystemConfig,
		           commitProxies,
		           grvProxies,
		           resolvers,
		           recoveryCount,
		           registrationCount,
		           configuration,
		           priorCommittedLogServers,
		           recoveryState,
		           recoveryStalled,
		           clusterId,
		           reply);
	}
};

struct RecruitFromConfigurationReply {
	constexpr static FileIdentifier file_identifier = 2224085;
	std::vector<WorkerInterface> backupWorkers;
	std::vector<WorkerInterface> tLogs;
	std::vector<WorkerInterface> satelliteTLogs;
	std::vector<WorkerInterface> commitProxies;
	std::vector<WorkerInterface> grvProxies;
	std::vector<WorkerInterface> resolvers;
	std::vector<WorkerInterface> storageServers;
	std::vector<WorkerInterface> oldLogRouters; // During recovery, log routers for older generations will be recruited.
	Optional<Key> dcId; // dcId is where master is recruited. It prefers to be in configuration.primaryDcId, but
	                    // it can be recruited from configuration.secondaryDc: The dcId will be the secondaryDcId and
	                    // this generation's primaryDC in memory is different from configuration.primaryDcId.
	bool satelliteFallback;

	RecruitFromConfigurationReply() : satelliteFallback(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           tLogs,
		           satelliteTLogs,
		           commitProxies,
		           grvProxies,
		           resolvers,
		           storageServers,
		           oldLogRouters,
		           dcId,
		           satelliteFallback,
		           backupWorkers);
	}
};

struct RecruitFromConfigurationRequest {
	constexpr static FileIdentifier file_identifier = 2023046;
	DatabaseConfiguration configuration;
	bool recruitSeedServers;
	int maxOldLogRouters;
	ReplyPromise<RecruitFromConfigurationReply> reply;

	RecruitFromConfigurationRequest() {}
	explicit RecruitFromConfigurationRequest(DatabaseConfiguration const& configuration,
	                                         bool recruitSeedServers,
	                                         int maxOldLogRouters)
	  : configuration(configuration), recruitSeedServers(recruitSeedServers), maxOldLogRouters(maxOldLogRouters) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, configuration, recruitSeedServers, maxOldLogRouters, reply);
	}
};

struct RecruitRemoteFromConfigurationReply {
	constexpr static FileIdentifier file_identifier = 9091392;
	std::vector<WorkerInterface> remoteTLogs;
	std::vector<WorkerInterface> logRouters;
	Optional<UID> dbgId;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, remoteTLogs, logRouters, dbgId);
	}
};

struct RecruitRemoteFromConfigurationRequest {
	constexpr static FileIdentifier file_identifier = 3235995;
	DatabaseConfiguration configuration;
	Optional<Key> dcId;
	int logRouterCount;
	std::vector<UID> exclusionWorkerIds;
	Optional<UID> dbgId;
	ReplyPromise<RecruitRemoteFromConfigurationReply> reply;

	RecruitRemoteFromConfigurationRequest() {}
	RecruitRemoteFromConfigurationRequest(DatabaseConfiguration const& configuration,
	                                      Optional<Key> const& dcId,
	                                      int logRouterCount,
	                                      const std::vector<UID>& exclusionWorkerIds)
	  : configuration(configuration), dcId(dcId), logRouterCount(logRouterCount),
	    exclusionWorkerIds(exclusionWorkerIds) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, configuration, dcId, logRouterCount, exclusionWorkerIds, dbgId, reply);
	}
};

struct RecruitStorageReply {
	constexpr static FileIdentifier file_identifier = 15877089;
	WorkerInterface worker;
	ProcessClass processClass;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, worker, processClass);
	}
};

struct RecruitStorageRequest {
	constexpr static FileIdentifier file_identifier = 905920;
	std::vector<Optional<Standalone<StringRef>>> excludeMachines; //< Don't recruit any of these machines
	std::vector<AddressExclusion> excludeAddresses; //< Don't recruit any of these addresses
	std::vector<Optional<Standalone<StringRef>>> includeDCs;
	bool criticalRecruitment; //< True if machine classes are to be ignored
	ReplyPromise<RecruitStorageReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, excludeMachines, excludeAddresses, includeDCs, criticalRecruitment, reply);
	}
};

struct RecruitBlobWorkerReply {
	constexpr static FileIdentifier file_identifier = 9908409;
	WorkerInterface worker;
	ProcessClass processClass;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, worker, processClass);
	}
};

struct RecruitBlobWorkerRequest {
	constexpr static FileIdentifier file_identifier = 72435;
	std::vector<AddressExclusion> excludeAddresses;
	ReplyPromise<RecruitBlobWorkerReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, excludeAddresses, reply);
	}
};

struct RegisterWorkerRequest {
	constexpr static FileIdentifier file_identifier = 14332605;
	WorkerInterface wi;
	ProcessClass initialClass;
	ProcessClass processClass;
	ClusterControllerPriorityInfo priorityInfo;
	Generation generation;
	Optional<DataDistributorInterface> distributorInterf;
	Optional<RatekeeperInterface> ratekeeperInterf;
	Optional<BlobManagerInterface> blobManagerInterf;
	Optional<EncryptKeyProxyInterface> encryptKeyProxyInterf;
	Standalone<VectorRef<StringRef>> issues;
	std::vector<NetworkAddress> incompatiblePeers;
	ReplyPromise<RegisterWorkerReply> reply;
	bool degraded;
	Version lastSeenKnobVersion;
	ConfigClassSet knobConfigClassSet;
	bool requestDbInfo;

	RegisterWorkerRequest()
	  : priorityInfo(ProcessClass::UnsetFit, false, ClusterControllerPriorityInfo::FitnessUnknown), degraded(false) {}
	RegisterWorkerRequest(WorkerInterface wi,
	                      ProcessClass initialClass,
	                      ProcessClass processClass,
	                      ClusterControllerPriorityInfo priorityInfo,
	                      Generation generation,
	                      Optional<DataDistributorInterface> ddInterf,
	                      Optional<RatekeeperInterface> rkInterf,
	                      Optional<BlobManagerInterface> bmInterf,
	                      Optional<EncryptKeyProxyInterface> ekpInterf,
	                      bool degraded,
	                      Version lastSeenKnobVersion,
	                      ConfigClassSet knobConfigClassSet)
	  : wi(wi), initialClass(initialClass), processClass(processClass), priorityInfo(priorityInfo),
	    generation(generation), distributorInterf(ddInterf), ratekeeperInterf(rkInterf), blobManagerInterf(bmInterf),
	    encryptKeyProxyInterf(ekpInterf), degraded(degraded), lastSeenKnobVersion(lastSeenKnobVersion),
	    knobConfigClassSet(knobConfigClassSet), requestDbInfo(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           wi,
		           initialClass,
		           processClass,
		           priorityInfo,
		           generation,
		           distributorInterf,
		           ratekeeperInterf,
		           blobManagerInterf,
		           encryptKeyProxyInterf,
		           issues,
		           incompatiblePeers,
		           reply,
		           degraded,
		           lastSeenKnobVersion,
		           knobConfigClassSet,
		           requestDbInfo);
	}
};

struct GetWorkersRequest {
	constexpr static FileIdentifier file_identifier = 1254174;
	enum { TESTER_CLASS_ONLY = 0x1, NON_EXCLUDED_PROCESSES_ONLY = 0x2 };

	int flags;
	ReplyPromise<std::vector<WorkerDetails>> reply;

	GetWorkersRequest() : flags(0) {}
	explicit GetWorkersRequest(int fl) : flags(fl) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, flags, reply);
	}
};

struct UpdateWorkerHealthRequest {
	constexpr static FileIdentifier file_identifier = 5789927;
	NetworkAddress address;
	std::vector<NetworkAddress> degradedPeers;

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (!is_fb_function<Ar>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, address, degradedPeers);
	}
};

struct TLogRejoinReply {
	constexpr static FileIdentifier file_identifier = 11;

	// false means someone else registered, so we should re-register.  true means this master is recovered, so don't
	// send again to the same master.
	bool masterIsRecovered;
	TLogRejoinReply() = default;
	explicit TLogRejoinReply(bool masterIsRecovered) : masterIsRecovered(masterIsRecovered) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, masterIsRecovered);
	}
};

struct TLogRejoinRequest {
	constexpr static FileIdentifier file_identifier = 15692200;
	TLogInterface myInterface;
	ReplyPromise<TLogRejoinReply> reply;

	TLogRejoinRequest() {}
	explicit TLogRejoinRequest(const TLogInterface& interf) : myInterface(interf) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, myInterface, reply);
	}
};

struct BackupWorkerDoneRequest {
	constexpr static FileIdentifier file_identifier = 8736351;
	UID workerUID;
	LogEpoch backupEpoch;
	ReplyPromise<Void> reply;

	BackupWorkerDoneRequest() : workerUID(), backupEpoch(-1) {}
	BackupWorkerDoneRequest(UID id, LogEpoch epoch) : workerUID(id), backupEpoch(epoch) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, workerUID, backupEpoch, reply);
	}
};

struct InitializeTLogRequest {
	constexpr static FileIdentifier file_identifier = 15604392;
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
	int txsTags;
	UID clusterId;

	ReplyPromise<struct TLogInterface> reply;

	InitializeTLogRequest() : recoverFrom(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           recruitmentID,
		           recoverFrom,
		           recoverAt,
		           knownCommittedVersion,
		           epoch,
		           recoverTags,
		           allTags,
		           storeType,
		           remoteTag,
		           locality,
		           isPrimary,
		           startVersion,
		           logRouterTags,
		           reply,
		           logVersion,
		           spillType,
		           txsTags,
		           clusterId);
	}
};

struct InitializeLogRouterRequest {
	constexpr static FileIdentifier file_identifier = 2976228;
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

struct InitializeBackupReply {
	constexpr static FileIdentifier file_identifier = 13511909;
	struct BackupInterface interf;
	LogEpoch backupEpoch;

	InitializeBackupReply() = default;
	InitializeBackupReply(BackupInterface bi, LogEpoch e) : interf(bi), backupEpoch(e) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, interf, backupEpoch);
	}
};

struct InitializeBackupRequest {
	constexpr static FileIdentifier file_identifier = 1245415;
	UID reqId;
	LogEpoch recruitedEpoch; // The epoch the worker is recruited.
	LogEpoch backupEpoch; // The epoch the worker should work on. If different from the recruitedEpoch, then it refers
	                      // to some previous epoch with unfinished work.
	Tag routerTag;
	int totalTags;
	Version startVersion;
	Optional<Version> endVersion;
	ReplyPromise<struct InitializeBackupReply> reply;

	InitializeBackupRequest() = default;
	explicit InitializeBackupRequest(UID id) : reqId(id) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reqId, recruitedEpoch, backupEpoch, routerTag, totalTags, startVersion, endVersion, reply);
	}
};

// FIXME: Rename to InitializeMasterRequest, etc
struct RecruitMasterRequest {
	constexpr static FileIdentifier file_identifier = 12684574;
	LifetimeToken lifetime;
	bool forceRecovery;
	ReplyPromise<struct MasterInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (!is_fb_function<Ar>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, lifetime, forceRecovery, reply);
	}
};

struct InitializeCommitProxyRequest {
	constexpr static FileIdentifier file_identifier = 10344153;
	MasterInterface master;
	LifetimeToken masterLifetime;
	uint64_t recoveryCount;
	Version recoveryTransactionVersion;
	bool firstProxy;
	ReplyPromise<CommitProxyInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, master, masterLifetime, recoveryCount, recoveryTransactionVersion, firstProxy, reply);
	}
};

struct InitializeGrvProxyRequest {
	constexpr static FileIdentifier file_identifier = 8265613;
	MasterInterface master;
	LifetimeToken masterLifetime;
	uint64_t recoveryCount;
	ReplyPromise<GrvProxyInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, master, masterLifetime, recoveryCount, reply);
	}
};

struct InitializeDataDistributorRequest {
	constexpr static FileIdentifier file_identifier = 8858952;
	UID reqId;
	ReplyPromise<DataDistributorInterface> reply;

	InitializeDataDistributorRequest() {}
	explicit InitializeDataDistributorRequest(UID uid) : reqId(uid) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reqId, reply);
	}
};

struct InitializeRatekeeperRequest {
	constexpr static FileIdentifier file_identifier = 6416816;
	UID reqId;
	ReplyPromise<RatekeeperInterface> reply;

	InitializeRatekeeperRequest() {}
	explicit InitializeRatekeeperRequest(UID uid) : reqId(uid) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reqId, reply);
	}
};

struct InitializeBlobManagerRequest {
	constexpr static FileIdentifier file_identifier = 2567474;
	UID reqId;
	int64_t epoch;
	ReplyPromise<BlobManagerInterface> reply;

	InitializeBlobManagerRequest() {}
	explicit InitializeBlobManagerRequest(UID uid, int64_t epoch) : reqId(uid), epoch(epoch) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reqId, epoch, reply);
	}
};

struct InitializeResolverRequest {
	constexpr static FileIdentifier file_identifier = 7413317;
	LifetimeToken masterLifetime;
	uint64_t recoveryCount;
	int commitProxyCount;
	int resolverCount;
	UID masterId; // master's UID
	ReplyPromise<ResolverInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, masterLifetime, recoveryCount, commitProxyCount, resolverCount, masterId, reply);
	}
};

struct InitializeStorageReply {
	constexpr static FileIdentifier file_identifier = 10390645;
	StorageServerInterface interf;
	Version addedVersion;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, interf, addedVersion);
	}
};

struct InitializeStorageRequest {
	constexpr static FileIdentifier file_identifier = 16665642;
	Tag seedTag; //< If this server will be passed to seedShardServers, this will be a tag, otherwise it is invalidTag
	UID reqId;
	UID interfaceId;
	KeyValueStoreType storeType;
	Optional<std::pair<UID, Version>>
	    tssPairIDAndVersion; // Only set if recruiting a tss. Will be the UID and Version of its SS pair.
	UID clusterId; // Unique cluster identifier. Only needed at recruitment, will be read from txnStateStore on recovery
	ReplyPromise<InitializeStorageReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, seedTag, reqId, interfaceId, storeType, reply, tssPairIDAndVersion, clusterId);
	}
};

struct InitializeBlobWorkerReply {
	constexpr static FileIdentifier file_identifier = 6095215;
	BlobWorkerInterface interf;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, interf);
	}
};

struct InitializeBlobWorkerRequest {
	constexpr static FileIdentifier file_identifier = 5838547;
	UID reqId;
	UID interfaceId;
	ReplyPromise<InitializeBlobWorkerReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reqId, interfaceId, reply);
	}
};

struct InitializeEncryptKeyProxyRequest {
	constexpr static FileIdentifier file_identifier = 4180191;
	UID reqId;
	UID interfaceId;
	ReplyPromise<EncryptKeyProxyInterface> reply;

	InitializeEncryptKeyProxyRequest() {}
	explicit InitializeEncryptKeyProxyRequest(UID uid) : reqId(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reqId, interfaceId, reply);
	}
};

struct TraceBatchDumpRequest {
	constexpr static FileIdentifier file_identifier = 8184121;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct ExecuteRequest {
	constexpr static FileIdentifier file_identifier = 8184128;
	ReplyPromise<Void> reply;

	Arena arena;
	StringRef execPayload;

	ExecuteRequest(StringRef execPayload) : execPayload(execPayload) {}

	ExecuteRequest() : execPayload() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply, execPayload, arena);
	}
};

struct WorkerSnapRequest {
	constexpr static FileIdentifier file_identifier = 8194122;
	ReplyPromise<Void> reply;
	Arena arena;
	StringRef snapPayload;
	UID snapUID;
	StringRef role;

	WorkerSnapRequest(StringRef snapPayload, UID snapUID, StringRef role)
	  : snapPayload(snapPayload), snapUID(snapUID), role(role) {}
	WorkerSnapRequest() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply, snapPayload, snapUID, role, arena);
	}
};

struct LoadedReply {
	constexpr static FileIdentifier file_identifier = 9956350;
	Standalone<StringRef> payload;
	UID id;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, payload, id);
	}
};

struct LoadedPingRequest {
	constexpr static FileIdentifier file_identifier = 4590979;
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
	constexpr static FileIdentifier file_identifier = 9982747;
	UID clusterControllerId;
	int64_t timeStep;

	CoordinationPingMessage() : timeStep(0) {}
	CoordinationPingMessage(UID ccId, uint64_t step) : clusterControllerId(ccId), timeStep(step) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, clusterControllerId, timeStep);
	}
};

struct SetMetricsLogRateRequest {
	constexpr static FileIdentifier file_identifier = 4245995;
	uint32_t metricsLogsPerSecond;

	SetMetricsLogRateRequest() : metricsLogsPerSecond(1) {}
	explicit SetMetricsLogRateRequest(uint32_t logsPerSecond) : metricsLogsPerSecond(logsPerSecond) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, metricsLogsPerSecond);
	}
};

struct EventLogRequest {
	constexpr static FileIdentifier file_identifier = 122319;
	bool getLastError;
	Standalone<StringRef> eventName;
	ReplyPromise<TraceEventFields> reply;

	EventLogRequest() : getLastError(true) {}
	explicit EventLogRequest(Standalone<StringRef> eventName) : getLastError(false), eventName(eventName) {}

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
	DebugEntryRef(const char* c, Version v, MutationRef const& m)
	  : time(now()), address(g_network->getLocalAddress()), context((const uint8_t*)c, strlen(c)), version(v),
	    mutation(m) {}
	DebugEntryRef(Arena& a, DebugEntryRef const& d)
	  : time(d.time), address(d.address), context(d.context), version(d.version), mutation(a, d.mutation) {}

	size_t expectedSize() const { return context.expectedSize() + mutation.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, time, address, context, version, mutation);
	}
};

struct DiskStoreRequest {
	constexpr static FileIdentifier file_identifier = 1986262;
	bool includePartialStores;
	ReplyPromise<Standalone<VectorRef<UID>>> reply;

	DiskStoreRequest(bool includePartialStores = false) : includePartialStores(includePartialStores) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, includePartialStores, reply);
	}
};

struct Role {
	static const Role WORKER;
	static const Role STORAGE_SERVER;
	static const Role TESTING_STORAGE_SERVER;
	static const Role TRANSACTION_LOG;
	static const Role SHARED_TRANSACTION_LOG;
	static const Role COMMIT_PROXY;
	static const Role GRV_PROXY;
	static const Role MASTER;
	static const Role RESOLVER;
	static const Role CLUSTER_CONTROLLER;
	static const Role TESTER;
	static const Role LOG_ROUTER;
	static const Role DATA_DISTRIBUTOR;
	static const Role RATEKEEPER;
	static const Role BLOB_MANAGER;
	static const Role BLOB_WORKER;
	static const Role STORAGE_CACHE;
	static const Role COORDINATOR;
	static const Role BACKUP;
	static const Role ENCRYPT_KEY_PROXY;

	std::string roleName;
	std::string abbreviation;
	bool includeInTraceRoles;

	static const Role& get(ProcessClass::ClusterRole role) {
		switch (role) {
		case ProcessClass::Storage:
			return STORAGE_SERVER;
		case ProcessClass::TLog:
			return TRANSACTION_LOG;
		case ProcessClass::CommitProxy:
			return COMMIT_PROXY;
		case ProcessClass::GrvProxy:
			return GRV_PROXY;
		case ProcessClass::Master:
			return MASTER;
		case ProcessClass::Resolver:
			return RESOLVER;
		case ProcessClass::LogRouter:
			return LOG_ROUTER;
		case ProcessClass::ClusterController:
			return CLUSTER_CONTROLLER;
		case ProcessClass::DataDistributor:
			return DATA_DISTRIBUTOR;
		case ProcessClass::Ratekeeper:
			return RATEKEEPER;
		case ProcessClass::BlobManager:
			return BLOB_MANAGER;
		case ProcessClass::BlobWorker:
			return BLOB_WORKER;
		case ProcessClass::StorageCache:
			return STORAGE_CACHE;
		case ProcessClass::Backup:
			return BACKUP;
		case ProcessClass::EncryptKeyProxy:
			return ENCRYPT_KEY_PROXY;
		case ProcessClass::Worker:
			return WORKER;
		case ProcessClass::NoRole:
		default:
			ASSERT(false);
			throw internal_error();
		}
	}

	bool operator==(const Role& r) const { return roleName == r.roleName; }
	bool operator!=(const Role& r) const { return !(*this == r); }

private:
	Role(std::string roleName, std::string abbreviation, bool includeInTraceRoles = true)
	  : roleName(roleName), abbreviation(abbreviation), includeInTraceRoles(includeInTraceRoles) {
		ASSERT(abbreviation.size() == 2); // Having a fixed size makes log queries more straightforward
	}
};

void startRole(const Role& role,
               UID roleId,
               UID workerId,
               const std::map<std::string, std::string>& details = std::map<std::string, std::string>(),
               const std::string& origination = "Recruited");
void endRole(const Role& role, UID id, std::string reason, bool ok = true, Error e = Error());
ACTOR Future<Void> traceRole(Role role, UID roleId);

struct ServerDBInfo;

class Database openDBOnServer(Reference<AsyncVar<ServerDBInfo> const> const& db,
                              TaskPriority taskID = TaskPriority::DefaultEndpoint,
                              LockAware = LockAware::False,
                              EnableLocalityLoadBalance = EnableLocalityLoadBalance::True);
ACTOR Future<Void> extractClusterInterface(
    Reference<AsyncVar<Optional<struct ClusterControllerFullInterface>> const> in,
    Reference<AsyncVar<Optional<struct ClusterInterface>>> out);

ACTOR Future<Void> fdbd(Reference<IClusterConnectionRecord> ccr,
                        LocalityData localities,
                        ProcessClass processClass,
                        std::string dataFolder,
                        std::string coordFolder,
                        int64_t memoryLimit,
                        std::string metricsConnFile,
                        std::string metricsPrefix,
                        int64_t memoryProfilingThreshold,
                        std::string whitelistBinPaths,
                        std::string configPath,
                        std::map<std::string, std::string> manualKnobOverrides,
                        ConfigDBType configDBType);

ACTOR Future<Void> clusterController(Reference<IClusterConnectionRecord> ccr,
                                     Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> currentCC,
                                     Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo,
                                     Future<Void> recoveredDiskFiles,
                                     LocalityData locality,
                                     ConfigDBType configDBType);

ACTOR Future<Void> blobWorker(BlobWorkerInterface bwi,
                              ReplyPromise<InitializeBlobWorkerReply> blobWorkerReady,
                              Reference<AsyncVar<ServerDBInfo> const> dbInfo);
ACTOR Future<Void> encryptKeyProxyServer(EncryptKeyProxyInterface ei, Reference<AsyncVar<ServerDBInfo>> db);

// These servers are started by workerServer
class IKeyValueStore;
class ServerCoordinators;
class IDiskQueue;
ACTOR Future<Void> storageServer(IKeyValueStore* persistentData,
                                 StorageServerInterface ssi,
                                 Tag seedTag,
                                 UID clusterId,
                                 Version tssSeedVersion,
                                 ReplyPromise<InitializeStorageReply> recruitReply,
                                 Reference<AsyncVar<ServerDBInfo> const> db,
                                 std::string folder);
ACTOR Future<Void> storageServer(
    IKeyValueStore* persistentData,
    StorageServerInterface ssi,
    Reference<AsyncVar<ServerDBInfo> const> db,
    std::string folder,
    Promise<Void> recovered,
    Reference<IClusterConnectionRecord>
        connRecord); // changes pssi->id() to be the recovered ID); // changes pssi->id() to be the recovered ID
ACTOR Future<Void> masterServer(MasterInterface mi,
                                Reference<AsyncVar<ServerDBInfo> const> db,
                                Reference<AsyncVar<Optional<ClusterControllerFullInterface>> const> ccInterface,
                                ServerCoordinators serverCoordinators,
                                LifetimeToken lifetime,
                                bool forceRecovery);
ACTOR Future<Void> commitProxyServer(CommitProxyInterface proxy,
                                     InitializeCommitProxyRequest req,
                                     Reference<AsyncVar<ServerDBInfo> const> db,
                                     std::string whitelistBinPaths);
ACTOR Future<Void> grvProxyServer(GrvProxyInterface proxy,
                                  InitializeGrvProxyRequest req,
                                  Reference<AsyncVar<ServerDBInfo> const> db);
ACTOR Future<Void> tLog(IKeyValueStore* persistentData,
                        IDiskQueue* persistentQueue,
                        Reference<AsyncVar<ServerDBInfo> const> db,
                        LocalityData locality,
                        PromiseStream<InitializeTLogRequest> tlogRequests,
                        UID tlogId,
                        UID workerID,
                        bool restoreFromDisk,
                        Promise<Void> oldLog,
                        Promise<Void> recovered,
                        std::string folder,
                        Reference<AsyncVar<bool>> degraded,
                        Reference<AsyncVar<UID>> activeSharedTLog);
ACTOR Future<Void> resolver(ResolverInterface resolver,
                            InitializeResolverRequest initReq,
                            Reference<AsyncVar<ServerDBInfo> const> db);
ACTOR Future<Void> logRouter(TLogInterface interf,
                             InitializeLogRouterRequest req,
                             Reference<AsyncVar<ServerDBInfo> const> db);
ACTOR Future<Void> dataDistributor(DataDistributorInterface ddi, Reference<AsyncVar<ServerDBInfo> const> db);
ACTOR Future<Void> ratekeeper(RatekeeperInterface rki, Reference<AsyncVar<ServerDBInfo> const> db);
ACTOR Future<Void> blobManager(BlobManagerInterface bmi, Reference<AsyncVar<ServerDBInfo> const> db, int64_t epoch);
ACTOR Future<Void> storageCacheServer(StorageServerInterface interf,
                                      uint16_t id,
                                      Reference<AsyncVar<ServerDBInfo> const> db);
ACTOR Future<Void> backupWorker(BackupInterface bi,
                                InitializeBackupRequest req,
                                Reference<AsyncVar<ServerDBInfo> const> db);

void registerThreadForProfiling();

// Returns true if `address` is used in the db (indicated by `dbInfo`) transaction system and in the db's remote DC.
bool addressInDbAndRemoteDc(const NetworkAddress& address, Reference<AsyncVar<ServerDBInfo> const> dbInfo);

void updateCpuProfiler(ProfilerRequest req);

namespace oldTLog_4_6 {
ACTOR Future<Void> tLog(IKeyValueStore* persistentData,
                        IDiskQueue* persistentQueue,
                        Reference<AsyncVar<ServerDBInfo> const> db,
                        LocalityData locality,
                        UID tlogId,
                        UID workerID);
}
namespace oldTLog_6_0 {
ACTOR Future<Void> tLog(IKeyValueStore* persistentData,
                        IDiskQueue* persistentQueue,
                        Reference<AsyncVar<ServerDBInfo> const> db,
                        LocalityData locality,
                        PromiseStream<InitializeTLogRequest> tlogRequests,
                        UID tlogId,
                        UID workerID,
                        bool restoreFromDisk,
                        Promise<Void> oldLog,
                        Promise<Void> recovered,
                        std::string folder,
                        Reference<AsyncVar<bool>> degraded,
                        Reference<AsyncVar<UID>> activeSharedTLog);
}
namespace oldTLog_6_2 {
ACTOR Future<Void> tLog(IKeyValueStore* persistentData,
                        IDiskQueue* persistentQueue,
                        Reference<AsyncVar<ServerDBInfo> const> db,
                        LocalityData locality,
                        PromiseStream<InitializeTLogRequest> tlogRequests,
                        UID tlogId,
                        UID workerID,
                        bool restoreFromDisk,
                        Promise<Void> oldLog,
                        Promise<Void> recovered,
                        std::string folder,
                        Reference<AsyncVar<bool>> degraded,
                        Reference<AsyncVar<UID>> activeSharedTLog);
}

typedef decltype(&tLog) TLogFn;

ACTOR template <class T>
Future<T> ioTimeoutError(Future<T> what, double time) {
	// Before simulation is sped up, IO operations can take a very long time so limit timeouts
	// to not end until at least time after simulation is sped up.
	if (g_network->isSimulated() && !g_simulator.speedUpSimulation) {
		time += std::max(0.0, FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS - now());
	}
	Future<Void> end = lowPriorityDelay(time);
	choose {
		when(T t = wait(what)) { return t; }
		when(wait(end)) {
			Error err = io_timeout();
			if (g_network->isSimulated() && !g_simulator.getCurrentProcess()->isReliable()) {
				err = err.asInjectedFault();
			}
			TraceEvent(SevError, "IoTimeoutError").error(err);
			throw err;
		}
	}
}

ACTOR template <class T>
Future<T> ioDegradedOrTimeoutError(Future<T> what,
                                   double errTime,
                                   Reference<AsyncVar<bool>> degraded,
                                   double degradedTime) {
	// Before simulation is sped up, IO operations can take a very long time so limit timeouts
	// to not end until at least time after simulation is sped up.
	if (g_network->isSimulated() && !g_simulator.speedUpSimulation) {
		double timeShift = std::max(0.0, FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS - now());
		errTime += timeShift;
		degradedTime += timeShift;
	}

	if (degradedTime < errTime) {
		Future<Void> degradedEnd = lowPriorityDelay(degradedTime);
		choose {
			when(T t = wait(what)) { return t; }
			when(wait(degradedEnd)) {
				TEST(true); // TLog degraded
				TraceEvent(SevWarnAlways, "IoDegraded").log();
				degraded->set(true);
			}
		}
	}

	Future<Void> end = lowPriorityDelay(errTime - degradedTime);
	choose {
		when(T t = wait(what)) { return t; }
		when(wait(end)) {
			Error err = io_timeout();
			if (g_network->isSimulated() && !g_simulator.getCurrentProcess()->isReliable()) {
				err = err.asInjectedFault();
			}
			TraceEvent(SevError, "IoTimeoutError").error(err);
			throw err;
		}
	}
}

#include "fdbserver/ServerDBInfo.h"
#include "flow/unactorcompiler.h"
#endif
