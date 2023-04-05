/*
 * RestoreRoleCommon.h
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

// This file delcares common struct and functions shared by restore roles, i.e.,
// RestoreController, RestoreLoader, RestoreApplier

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RestoreRoleCommon_G_H)
#define FDBSERVER_RestoreRoleCommon_G_H
#include "fdbserver/RestoreRoleCommon.actor.g.h"
#elif !defined(FDBSERVER_RestoreRoleCommon_H)
#define FDBSERVER_RestoreRoleCommon_H

#include <sstream>
#include "flow/SystemMonitor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/Notified.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/RestoreWorkerInterface.actor.h"
#include "fdbserver/RestoreUtil.h"

#include "flow/actorcompiler.h" // has to be last include

struct RestoreRoleInterface;
struct RestoreLoaderInterface;
struct RestoreApplierInterface;

struct RestoreRoleData;
struct RestoreControllerData;

struct RestoreSimpleRequest;

// Key is the (version, subsequence) of parsed backup mutations.
// Value MutationsVec is the vector of parsed backup mutations.
// For old mutation logs, the subsequence number is always 0.
// For partitioned mutation logs, each mutation has a unique LogMessageVersion.
// Note for partitioned logs, one LogMessageVersion can have multiple mutations,
// because a clear mutation may be split into several smaller clear mutations by
// backup workers.
using VersionedMutationsMap = std::map<LogMessageVersion, MutationsVec>;

ACTOR Future<Void> isSchedulable(Reference<RestoreRoleData> self, int actorBatchIndex, std::string name);
ACTOR Future<Void> handleHeartbeat(RestoreSimpleRequest req, UID id);
ACTOR Future<Void> handleInitVersionBatchRequest(RestoreVersionBatchRequest req, Reference<RestoreRoleData> self);
void handleFinishRestoreRequest(const RestoreFinishRequest& req, Reference<RestoreRoleData> self);

class RoleVersionBatchState {
public:
	static const int INVALID = -1;

	virtual int get() { return vbState; }

	virtual void operator=(int newState) { vbState = newState; }

	explicit RoleVersionBatchState() : vbState(INVALID) {}
	explicit RoleVersionBatchState(int newState) : vbState(newState) {}

	virtual ~RoleVersionBatchState() = default;

	int vbState;
};

struct RestoreRoleData : NonCopyable, public ReferenceCounted<RestoreRoleData> {
public:
	RestoreRole role;
	UID nodeID;
	int nodeIndex;

	double cpuUsage;
	double memory;
	double residentMemory;

	AsyncTrigger checkMemory;
	int delayedActors; // actors that are delayed to release because of low memory

	std::map<UID, RestoreLoaderInterface> loadersInterf; // UID: loaderInterf's id
	std::map<UID, RestoreApplierInterface> appliersInterf; // UID: applierInterf's id
	Promise<Void> recruitedRoles; // sent when loaders and appliers are recruited

	NotifiedVersion versionBatchId; // The index of the version batch that has been initialized and put into pipeline
	NotifiedVersion finishedBatch; // The highest batch index all appliers have applied mutations

	RestoreRoleData() : role(RestoreRole::Invalid), cpuUsage(0.0), memory(0.0), residentMemory(0.0), delayedActors(0){};

	virtual ~RestoreRoleData() = default;

	UID id() const { return nodeID; }

	virtual void initVersionBatch(int batchIndex) = 0;
	virtual void resetPerRestoreRequest() = 0;
	virtual int getVersionBatchState(int batchIndex) = 0;
	virtual void setVersionBatchState(int batchIndex, int vbState) = 0;

	void clearInterfaces() {
		loadersInterf.clear();
		appliersInterf.clear();
	}

	virtual std::string describeNode() = 0;
};

void updateProcessStats(Reference<RestoreRoleData> self);
ACTOR Future<Void> updateProcessMetrics(Reference<RestoreRoleData> self);
ACTOR Future<Void> traceProcessMetrics(Reference<RestoreRoleData> self, std::string role);
ACTOR Future<Void> traceRoleVersionBatchProgress(Reference<RestoreRoleData> self, std::string role);

#include "flow/unactorcompiler.h"
#endif
