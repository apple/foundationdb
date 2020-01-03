/*
 * ClusterRecruitmentInterface.h
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

#ifndef FDBSERVER_CLUSTERRECRUITMENTINTERFACE_H
#define FDBSERVER_CLUSTERRECRUITMENTINTERFACE_H
#pragma once

#include "fdbclient/ClusterInterface.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/MasterProxyInterface.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbserver/DataDistributorInterface.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/Knobs.h"

// This interface and its serialization depend on slicing, since the client will deserialize only the first part of this structure
struct ClusterControllerFullInterface {
    constexpr static FileIdentifier file_identifier =
        ClusterControllerClientInterface::file_identifier;
	ClusterInterface clientInterface;
	RequestStream< struct RecruitFromConfigurationRequest > recruitFromConfiguration;
	RequestStream< struct RecruitRemoteFromConfigurationRequest > recruitRemoteFromConfiguration;
	RequestStream< struct RecruitStorageRequest > recruitStorage;
	RequestStream< struct RegisterWorkerRequest > registerWorker;
	RequestStream< struct GetWorkersRequest > getWorkers;
	RequestStream< struct RegisterMasterRequest > registerMaster;
	RequestStream< struct GetServerDBInfoRequest > getServerDBInfo;

	UID id() const { return clientInterface.id(); }
	bool operator == (ClusterControllerFullInterface const& r) const { return id() == r.id(); }
	bool operator != (ClusterControllerFullInterface const& r) const { return id() != r.id(); }

	bool hasMessage() {
		return clientInterface.hasMessage() ||
				recruitFromConfiguration.getFuture().isReady() ||
				recruitRemoteFromConfiguration.getFuture().isReady() ||
				recruitStorage.getFuture().isReady() ||
				registerWorker.getFuture().isReady() || 
				getWorkers.getFuture().isReady() || 
				registerMaster.getFuture().isReady() ||
				getServerDBInfo.getFuture().isReady();
	}

	void initEndpoints() {
		clientInterface.initEndpoints();
		recruitFromConfiguration.getEndpoint( TaskPriority::ClusterControllerRecruit );
		recruitRemoteFromConfiguration.getEndpoint( TaskPriority::ClusterControllerRecruit );
		recruitStorage.getEndpoint( TaskPriority::ClusterController );
		registerWorker.getEndpoint( TaskPriority::ClusterControllerWorker );
		getWorkers.getEndpoint( TaskPriority::ClusterController );
		registerMaster.getEndpoint( TaskPriority::ClusterControllerRegister );
		getServerDBInfo.getEndpoint( TaskPriority::ClusterController );
	}

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (!is_fb_function<Ar>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, clientInterface, recruitFromConfiguration, recruitRemoteFromConfiguration, recruitStorage,
		           registerWorker, getWorkers, registerMaster, getServerDBInfo);
	}
};

struct RecruitFromConfigurationReply {
	constexpr static FileIdentifier file_identifier = 2224085;
	vector<WorkerInterface> tLogs;
	vector<WorkerInterface> satelliteTLogs;
	vector<WorkerInterface> proxies;
	vector<WorkerInterface> resolvers;
	vector<WorkerInterface> storageServers;
	vector<WorkerInterface> oldLogRouters;
	Optional<Key> dcId;
	bool satelliteFallback;

	RecruitFromConfigurationReply() : satelliteFallback(false) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, tLogs, satelliteTLogs, proxies, resolvers, storageServers, oldLogRouters, dcId, satelliteFallback);
	}
};

struct RecruitFromConfigurationRequest {
	constexpr static FileIdentifier file_identifier = 2023046;
	DatabaseConfiguration configuration;
	bool recruitSeedServers;
	int maxOldLogRouters;
	ReplyPromise< struct RecruitFromConfigurationReply > reply;

	RecruitFromConfigurationRequest() {}
	explicit RecruitFromConfigurationRequest(DatabaseConfiguration const& configuration, bool recruitSeedServers, int maxOldLogRouters)
		: configuration(configuration), recruitSeedServers(recruitSeedServers), maxOldLogRouters(maxOldLogRouters) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, configuration, recruitSeedServers, maxOldLogRouters, reply);
	}
};

struct RecruitRemoteFromConfigurationReply {
	constexpr static FileIdentifier file_identifier = 9091392;
	vector<WorkerInterface> remoteTLogs;
	vector<WorkerInterface> logRouters;

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, remoteTLogs, logRouters);
	}
};

struct RecruitRemoteFromConfigurationRequest {
	constexpr static FileIdentifier file_identifier = 3235995;
	DatabaseConfiguration configuration;
	Optional<Key> dcId;
	int logRouterCount;
	std::vector<UID> exclusionWorkerIds;
	ReplyPromise< struct RecruitRemoteFromConfigurationReply > reply;

	RecruitRemoteFromConfigurationRequest() {}
	RecruitRemoteFromConfigurationRequest(DatabaseConfiguration const& configuration, Optional<Key> const& dcId, int logRouterCount, const std::vector<UID> &exclusionWorkerIds) : configuration(configuration), dcId(dcId), logRouterCount(logRouterCount), exclusionWorkerIds(exclusionWorkerIds){}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, configuration, dcId, logRouterCount, exclusionWorkerIds, reply);
	}
};

struct RecruitStorageReply {
	constexpr static FileIdentifier file_identifier = 15877089;
	WorkerInterface worker;
	ProcessClass processClass;

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, worker, processClass);
	}
};

struct RecruitStorageRequest {
	constexpr static FileIdentifier file_identifier = 905920;
	std::vector<Optional<Standalone<StringRef>>> excludeMachines;	//< Don't recruit any of these machines
	std::vector<AddressExclusion> excludeAddresses;		//< Don't recruit any of these addresses
	std::vector<Optional<Standalone<StringRef>>> includeDCs;
	bool criticalRecruitment;							//< True if machine classes are to be ignored
	ReplyPromise< RecruitStorageReply > reply;

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, excludeMachines, excludeAddresses, includeDCs, criticalRecruitment, reply);
	}
};

struct RegisterWorkerReply {
	constexpr static FileIdentifier file_identifier = 16475696;
	ProcessClass processClass;
	ClusterControllerPriorityInfo priorityInfo;

	RegisterWorkerReply() : priorityInfo(ProcessClass::UnsetFit, false, ClusterControllerPriorityInfo::FitnessUnknown) {}
	RegisterWorkerReply(ProcessClass processClass, ClusterControllerPriorityInfo priorityInfo) : processClass(processClass), priorityInfo(priorityInfo) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, processClass, priorityInfo);
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
	ReplyPromise<RegisterWorkerReply> reply;
	bool degraded;

	RegisterWorkerRequest() : priorityInfo(ProcessClass::UnsetFit, false, ClusterControllerPriorityInfo::FitnessUnknown), degraded(false) {}
	RegisterWorkerRequest(WorkerInterface wi, ProcessClass initialClass, ProcessClass processClass, ClusterControllerPriorityInfo priorityInfo, Generation generation, Optional<DataDistributorInterface> ddInterf, Optional<RatekeeperInterface> rkInterf, bool degraded) :
	wi(wi), initialClass(initialClass), processClass(processClass), priorityInfo(priorityInfo), generation(generation), distributorInterf(ddInterf), ratekeeperInterf(rkInterf), degraded(degraded) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, wi, initialClass, processClass, priorityInfo, generation, distributorInterf, ratekeeperInterf, reply, degraded);
	}
};

struct GetWorkersRequest {
	constexpr static FileIdentifier file_identifier = 1254174;
	enum { TESTER_CLASS_ONLY = 0x1, NON_EXCLUDED_PROCESSES_ONLY = 0x2 };

	int flags;
	ReplyPromise<vector<WorkerDetails>> reply;

	GetWorkersRequest() : flags(0) {}
	explicit GetWorkersRequest(int fl) : flags(fl) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, flags, reply);
	}
};

struct RegisterMasterRequest {
	constexpr static FileIdentifier file_identifier = 10773445;
	UID id;
	LocalityData mi;
	LogSystemConfig logSystemConfig;
	vector<MasterProxyInterface> proxies;
	vector<ResolverInterface> resolvers;
	DBRecoveryCount recoveryCount;
	int64_t registrationCount;
	Optional<DatabaseConfiguration> configuration;
	vector<UID> priorCommittedLogServers;
	RecoveryState recoveryState;
	bool recoveryStalled;

	ReplyPromise<Void> reply;

	RegisterMasterRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (!is_fb_function<Ar>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, id, mi, logSystemConfig, proxies, resolvers, recoveryCount, registrationCount, configuration,
		           priorCommittedLogServers, recoveryState, recoveryStalled, reply);
	}
};

#include "fdbserver/ServerDBInfo.h" // include order hack

#endif
