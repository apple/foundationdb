/*
 * WorkerInfo.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/Locality.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Arena.h"
#include "flow/genericactors.actor.h"

#pragma once

struct WorkerInfo : NonCopyable {
	Future<Void> watcher;
	ReplyPromise<RegisterWorkerReply> reply;
	Generation gen;
	int reboots;
	ProcessClass initialClass;
	ClusterControllerPriorityInfo priorityInfo;
	WorkerDetails details;
	Future<Void> haltRatekeeper;
	Future<Void> haltDistributor;
	Future<Void> haltBlobManager;
	Future<Void> haltBlobMigrator;
	Future<Void> haltEncryptKeyProxy;
	Future<Void> haltConsistencyScan;
	Standalone<VectorRef<StringRef>> issues;

	WorkerInfo()
	  : gen(-1), reboots(0),
	    priorityInfo(ProcessClass::UnsetFit, false, ClusterControllerPriorityInfo::FitnessUnknown) {}
	WorkerInfo(Future<Void> watcher,
	           ReplyPromise<RegisterWorkerReply> reply,
	           Generation gen,
	           WorkerInterface interf,
	           ProcessClass initialClass,
	           ProcessClass processClass,
	           ClusterControllerPriorityInfo priorityInfo,
	           bool degraded,
	           bool recoveredDiskFiles,
	           Standalone<VectorRef<StringRef>> issues)
	  : watcher(watcher), reply(reply), gen(gen), reboots(0), initialClass(initialClass), priorityInfo(priorityInfo),
	    details(interf, processClass, degraded, recoveredDiskFiles), issues(issues) {}

	WorkerInfo(WorkerInfo&& r) noexcept
	  : watcher(std::move(r.watcher)), reply(std::move(r.reply)), gen(r.gen), reboots(r.reboots),
	    initialClass(r.initialClass), priorityInfo(r.priorityInfo), details(std::move(r.details)),
	    haltRatekeeper(r.haltRatekeeper), haltDistributor(r.haltDistributor), haltBlobManager(r.haltBlobManager),
	    haltEncryptKeyProxy(r.haltEncryptKeyProxy), haltConsistencyScan(r.haltConsistencyScan), issues(r.issues) {}
	void operator=(WorkerInfo&& r) noexcept {
		watcher = std::move(r.watcher);
		reply = std::move(r.reply);
		gen = r.gen;
		reboots = r.reboots;
		initialClass = r.initialClass;
		priorityInfo = r.priorityInfo;
		details = std::move(r.details);
		haltRatekeeper = r.haltRatekeeper;
		haltDistributor = r.haltDistributor;
		haltBlobManager = r.haltBlobManager;
		haltEncryptKeyProxy = r.haltEncryptKeyProxy;
		issues = r.issues;
	}
};

struct WorkerFitnessInfo {
	WorkerDetails worker;
	ProcessClass::Fitness fitness;
	int used;

	WorkerFitnessInfo() : fitness(ProcessClass::NeverAssign), used(0) {}
	WorkerFitnessInfo(WorkerDetails worker, ProcessClass::Fitness fitness, int used)
	  : worker(worker), fitness(fitness), used(used) {}
};

struct RecruitmentInfo {
	DatabaseConfiguration configuration;
	bool recruitSeedServers;
	int maxOldLogRouters;

	RecruitmentInfo() {}
	explicit RecruitmentInfo(DatabaseConfiguration const& configuration, bool recruitSeedServers, int maxOldLogRouters)
	  : configuration(configuration), recruitSeedServers(recruitSeedServers), maxOldLogRouters(maxOldLogRouters) {}
};

// Stores the interfaces for all the workers required for a functioning
// transaction subsystem.
struct WorkerRecruitment {
	std::vector<WorkerInterface> grvProxies;
	std::vector<WorkerInterface> commitProxies;
	std::vector<WorkerInterface> resolvers;
	std::vector<WorkerInterface> tLogs;
	std::vector<WorkerInterface> satelliteTLogs;
	std::vector<WorkerInterface> storageServers;
	// During recovery, log routers for older generations will be recruited.
	std::vector<WorkerInterface> oldLogRouters;
	std::vector<WorkerInterface> backupWorkers;
	// dcId is where the master is recruited. It prefers to be in
	// configuration.primaryDcId, but it can be recruited from
	// configuration.secondaryDc: The dcId will be the secondaryDcId and this
	// generation's primaryDC in memory is different from
	// configuration.primaryDcId.
	Optional<Key> dcId;

	// Satellite fallback mode is automatically enabled when one of two
	// satellite datacenters become unavailable. In satellite fallback mode,
	// mutations will only be sent to the single remaining satellite
	// datacenter.
	bool satelliteFallback;

	WorkerRecruitment() : satelliteFallback(false) {}

	// TODO(ljoswiak): Implement when refactoring `betterMasterExists`
	bool operator==(WorkerRecruitment const& rhs) const { return true; }
	bool operator<(WorkerRecruitment const& rhs) const { return true; }
};

struct RemoteRecruitmentInfo {
	DatabaseConfiguration configuration;
	Optional<Key> dcId;
	int logRouterCount;
	std::vector<UID> exclusionWorkerIds;
	Optional<UID> dbgId;

	RemoteRecruitmentInfo() {}
	RemoteRecruitmentInfo(DatabaseConfiguration const& configuration,
	                      Optional<Key> const& dcId,
	                      int logRouterCount,
	                      const std::vector<UID>& exclusionWorkerIds)
	  : configuration(configuration), dcId(dcId), logRouterCount(logRouterCount),
	    exclusionWorkerIds(exclusionWorkerIds) {}
};

struct RemoteWorkerRecruitment {
	std::vector<WorkerInterface> remoteTLogs;
	std::vector<WorkerInterface> logRouters;
	Optional<UID> dbgId;
};
