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
#include "fdbserver/MasterInterface.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/WorkerInterface.h"
#include "fdbserver/Knobs.h"

// This interface and its serialization depend on slicing, since the client will deserialize only the first part of this structure
struct ClusterControllerFullInterface {
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

	void initEndpoints() {
		clientInterface.initEndpoints();
		recruitFromConfiguration.getEndpoint( TaskClusterController );
		recruitRemoteFromConfiguration.getEndpoint( TaskClusterController );
		recruitStorage.getEndpoint( TaskClusterController );
		registerWorker.getEndpoint( TaskClusterController );
		getWorkers.getEndpoint( TaskClusterController );
		registerMaster.getEndpoint( TaskClusterController );
		getServerDBInfo.getEndpoint( TaskClusterController );
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		ASSERT( ar.protocolVersion() >= 0x0FDB00A200040001LL );
		serializer(ar, clientInterface, recruitFromConfiguration, recruitRemoteFromConfiguration, recruitStorage, registerWorker, getWorkers, registerMaster, getServerDBInfo);
	}
};

struct RecruitFromConfigurationRequest {
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

struct RecruitFromConfigurationReply {
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

struct RecruitRemoteFromConfigurationRequest {
	DatabaseConfiguration configuration;
	Optional<Key> dcId;
	int logRouterCount;
	ReplyPromise< struct RecruitRemoteFromConfigurationReply > reply;

	RecruitRemoteFromConfigurationRequest() {}
	RecruitRemoteFromConfigurationRequest(DatabaseConfiguration const& configuration, Optional<Key> const& dcId, int logRouterCount) : configuration(configuration), dcId(dcId), logRouterCount(logRouterCount) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, configuration, dcId, logRouterCount, reply);
	}
};

struct RecruitRemoteFromConfigurationReply {
	vector<WorkerInterface> remoteTLogs;
	vector<WorkerInterface> logRouters;

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, remoteTLogs, logRouters);
	}
};

struct RecruitStorageReply {
	WorkerInterface worker;
	ProcessClass processClass;

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, worker, processClass);
	}
};

struct RecruitStorageRequest {
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
	WorkerInterface wi;
	ProcessClass initialClass;
	ProcessClass processClass;
	ClusterControllerPriorityInfo priorityInfo;
	Generation generation;
	ReplyPromise<RegisterWorkerReply> reply;

	RegisterWorkerRequest() : priorityInfo(ProcessClass::UnsetFit, false, ClusterControllerPriorityInfo::FitnessUnknown) {}
	RegisterWorkerRequest(WorkerInterface wi, ProcessClass initialClass, ProcessClass processClass, ClusterControllerPriorityInfo priorityInfo, Generation generation) : 
	wi(wi), initialClass(initialClass), processClass(processClass), priorityInfo(priorityInfo), generation(generation) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, wi, initialClass, processClass, priorityInfo, generation, reply);
	}
};

struct GetWorkersRequest {
	enum { TESTER_CLASS_ONLY = 0x1, NON_EXCLUDED_PROCESSES_ONLY = 0x2 };

	int flags;
	ReplyPromise<vector<std::pair<WorkerInterface, ProcessClass>>> reply;

	GetWorkersRequest() : flags(0) {}
	explicit GetWorkersRequest(int fl) : flags(fl) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, flags, reply);
	}
};

struct RegisterMasterRequest {
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
	void serialize( Ar& ar ) {
		ASSERT( ar.protocolVersion() >= 0x0FDB00A200040001LL );
		serializer(ar, id, mi, logSystemConfig, proxies, resolvers, recoveryCount, registrationCount, configuration, priorCommittedLogServers, recoveryState, recoveryStalled, reply);
	}
};

struct GetServerDBInfoRequest {
	UID knownServerInfoID;
	Standalone<StringRef> issues;
	std::vector<NetworkAddress> incompatiblePeers;
	ReplyPromise< struct ServerDBInfo > reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, knownServerInfoID, issues, incompatiblePeers, reply);
	}
};

#include "fdbserver/ServerDBInfo.h" // include order hack

#endif
