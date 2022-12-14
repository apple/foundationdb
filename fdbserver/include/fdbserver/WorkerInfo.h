/*
 * WorkerInfo.h
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

#include "fdbrpc/Locality.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Arena.h"
#include "flow/genericactors.actor.h"

#include <unordered_set>

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

struct RecruitWorkersInfo : ReferenceCounted<RecruitWorkersInfo> {
	RecruitFromConfigurationRequest req;
	RecruitFromConfigurationReply rep;
	AsyncTrigger waitForCompletion;
	Optional<UID> dbgId;

	RecruitWorkersInfo(RecruitFromConfigurationRequest const& req) : req(req) {}
};

struct RecruitRemoteWorkersInfo : ReferenceCounted<RecruitRemoteWorkersInfo> {
	RecruitRemoteFromConfigurationRequest req;
	RecruitRemoteFromConfigurationReply rep;
	AsyncTrigger waitForCompletion;
	Optional<UID> dbgId;

	RecruitRemoteWorkersInfo(RecruitRemoteFromConfigurationRequest const& req) : req(req) {}
};

struct DegradationInfo {
	std::unordered_set<NetworkAddress>
	    degradedServers; // The servers that the cluster controller is considered as degraded. The servers in this
	// list are not excluded unless they are added to `excludedDegradedServers`.
	std::unordered_set<NetworkAddress>
	    disconnectedServers; // Similar to the above list, but the servers experiencing connection issue.

	bool degradedSatellite = false; // Indicates that the entire satellite DC is degraded.
};
