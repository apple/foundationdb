/*
 * RestoreRoleCommon.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"

#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreLoader.actor.h"
#include "fdbserver/RestoreApplier.actor.h"
#include "fdbserver/RestoreMaster.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

class Database;
struct RestoreWorkerData;

// id is the id of the worker to be monitored
// This actor is used for both restore loader and restore applier
ACTOR Future<Void> handleHeartbeat(RestoreSimpleRequest req, UID id) {
	wait(delayJittered(5.0)); // Random jitter reduces heat beat monitor's pressure
	req.reply.send(RestoreCommonReply(id));
	return Void();
}

void handleFinishRestoreRequest(const RestoreFinishRequest& req, Reference<RestoreRoleData> self) {
	self->resetPerRestoreRequest();
	TraceEvent("FastRestoreRolePhaseFinishRestoreRequest", self->id())
	    .detail("FinishRestoreRequest", req.terminate)
	    .detail("Role", getRoleStr(self->role));

	req.reply.send(RestoreCommonReply(self->id()));
}

// Multiple version batches may execute in parallel and init their version batches
ACTOR Future<Void> handleInitVersionBatchRequest(RestoreVersionBatchRequest req, Reference<RestoreRoleData> self) {
	TraceEvent("FastRestoreRolePhaseInitVersionBatch", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("Role", getRoleStr(self->role))
	    .detail("VersionBatchNotifiedVersion", self->versionBatchId.get());
	// batchId is continuous. (req.batchIndex-1) is the id of the just finished batch.
	wait(self->versionBatchId.whenAtLeast(req.batchIndex - 1));

	if (self->versionBatchId.get() == req.batchIndex - 1) {
		self->initVersionBatch(req.batchIndex);
		self->setVersionBatchState(req.batchIndex, ApplierVersionBatchState::INIT);
		TraceEvent("FastRestoreInitVersionBatch")
		    .detail("BatchIndex", req.batchIndex)
		    .detail("Role", getRoleStr(self->role))
		    .detail("Node", self->id());
		self->versionBatchId.set(req.batchIndex);
	}

	req.reply.send(RestoreCommonReply(self->id()));
	return Void();
}

void updateProcessStats(Reference<RestoreRoleData> self) {
	if (g_network->isSimulated()) {
		// memUsage and cpuUsage are not relevant in the simulator,
		// and relying on the actual values could break seed determinism
		self->cpuUsage = 100.0;
		self->memory = 100.0;
		self->residentMemory = 100.0;
		return;
	}

	SystemStatistics sysStats = getSystemStatistics();
	if (sysStats.initialized) {
		self->cpuUsage = 100 * sysStats.processCPUSeconds / sysStats.elapsed;
		self->memory = sysStats.processMemory;
		self->residentMemory = sysStats.processResidentMemory;
	}
}

// An actor is schedulable to run if the current worker has enough resourc, i.e.,
// the worker's memory usage is below the threshold;
// Exception: If the actor is working on the current version batch, we have to schedule
// the actor to run to avoid dead-lock.
// Future: When we release the actors that are blocked by memory usage, we should release them
// in increasing order of their version batch.
ACTOR Future<Void> isSchedulable(Reference<RestoreRoleData> self, int actorBatchIndex, std::string name) {
	self->delayedActors++;
	loop {
		double memory = getSystemStatistics().processMemory;
		if (g_network->isSimulated() && BUGGIFY) {
			// Intentionally randomly block actors for low memory reason.
			// memory will be larger than threshold when deterministicRandom()->random01() > 1/2
			memory = SERVER_KNOBS->FASTRESTORE_MEMORY_THRESHOLD_MB_SOFT * 2 * deterministicRandom()->random01();
		}
		if (memory < SERVER_KNOBS->FASTRESTORE_MEMORY_THRESHOLD_MB_SOFT ||
		    self->finishedBatch.get() + 1 == actorBatchIndex) {
			if (memory >= SERVER_KNOBS->FASTRESTORE_MEMORY_THRESHOLD_MB_SOFT) {
				TraceEvent(SevWarn, "FastRestoreMemoryUsageAboveThreshold")
				    .detail("BatchIndex", actorBatchIndex)
				    .detail("Actor", name);
			}
			self->delayedActors--;
			break;
		} else {
			TraceEvent(SevDebug, "FastRestoreMemoryUsageAboveThresholdWait")
			    .detail("BatchIndex", actorBatchIndex)
			    .detail("Actor", name)
			    .detail("CurrentMemory", memory);
			wait(delay(SERVER_KNOBS->FASTRESTORE_WAIT_FOR_MEMORY_LATENCY) || self->checkMemory.onTrigger());
		}
	}
	return Void();
}

ACTOR Future<Void> traceProcessMetrics(Reference<RestoreRoleData> self, std::string role) {
	loop {
		TraceEvent("FastRestoreTraceProcessMetrics")
		    .detail("Role", role)
		    .detail("Node", self->nodeID)
		    .detail("CpuUsage", self->cpuUsage)
		    .detail("UsedMemory", self->memory)
		    .detail("ResidentMemory", self->residentMemory);
		wait(delay(SERVER_KNOBS->FASTRESTORE_ROLE_LOGGING_DELAY));
	}
}

ACTOR Future<Void> traceRoleVersionBatchProgress(Reference<RestoreRoleData> self, std::string role) {
	loop {
		int batchIndex = self->finishedBatch.get();
		int maxBatchIndex = self->versionBatchId.get();

		TraceEvent ev("FastRestoreVersionBatchProgressState", self->nodeID);
		ev.detail("Role", role).detail("Node", self->nodeID).detail("FinishedBatch", batchIndex).detail("InitializedBatch", maxBatchIndex);
		while (batchIndex <= maxBatchIndex) {
			std::stringstream typeName;
			typeName << "VersionBatch" << batchIndex;
			ev.detail(typeName.str(), self->getVersionBatchState(batchIndex));
			batchIndex++;
		}

		wait(delay(SERVER_KNOBS->FASTRESTORE_ROLE_LOGGING_DELAY));
	}
}

//-------Helper functions
std::string getHexString(StringRef input) {
	std::stringstream ss;
	for (int i = 0; i < input.size(); i++) {
		if (i % 4 == 0) ss << " ";
		if (i == 12) { // The end of 12bytes, which is the version size for value
			ss << "|";
		}
		if (i == (12 + 12)) { // The end of version + header
			ss << "@";
		}
		ss << std::setfill('0') << std::setw(2) << std::hex
		   << (int)input[i]; // [] operator moves the pointer in step of unit8
	}
	return ss.str();
}
