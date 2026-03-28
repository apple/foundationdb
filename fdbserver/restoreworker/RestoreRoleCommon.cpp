/*
 * RestoreRoleCommon.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/RunRYWTransaction.h"

#include "fdbserver/restoreworker/RestoreUtil.h"
#include "RestoreRoleCommon.h"
#include "RestoreLoader.h"
#include "RestoreApplier.h"
#include "RestoreController.h"

class Database;
struct RestoreWorkerData;

// id is the id of the worker to be monitored
// This actor is used for both restore loader and restore applier
Future<Void> handleHeartbeat(RestoreSimpleRequest req, UID id) {
	co_await delayJittered(5.0); // Random jitter reduces heat beat monitor's pressure
	req.reply.send(RestoreCommonReply(id));
}

void handleFinishRestoreRequest(const RestoreFinishRequest& req, Reference<RestoreRoleData> self) {
	self->resetPerRestoreRequest();
	TraceEvent("FastRestoreRolePhaseFinishRestoreRequest", self->id())
	    .detail("FinishRestoreRequest", req.terminate)
	    .detail("Role", getRoleStr(self->role));

	req.reply.send(RestoreCommonReply(self->id()));
}

// Multiple version batches may execute in parallel and init their version batches
Future<Void> handleInitVersionBatchRequest(RestoreVersionBatchRequest req, Reference<RestoreRoleData> self) {
	TraceEvent("FastRestoreRolePhaseInitVersionBatch", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("Role", getRoleStr(self->role))
	    .detail("VersionBatchNotifiedVersion", self->versionBatchId.get());
	// Loader destroy batchData once the batch finishes and self->finishedBatch.set(req.batchIndex);
	ASSERT(self->finishedBatch.get() < req.batchIndex);

	// batchId is continuous. (req.batchIndex-1) is the id of the just finished batch.
	co_await self->versionBatchId.whenAtLeast(req.batchIndex - 1);

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
}

void updateProcessStats(Reference<RestoreRoleData> self) {
	if (g_network->isSimulated()) {
		// memUsage and cpuUsage are not relevant in the simulator,
		// and relying on the actual values could break seed determinism
		if (deterministicRandom()->random01() < 0.2) { // not fully utilized cpu
			self->cpuUsage = deterministicRandom()->random01() * SERVER_KNOBS->FASTRESTORE_SCHED_TARGET_CPU_PERCENT;
		} else if (deterministicRandom()->random01() < 0.6) { // achieved target cpu but cpu is not busy
			self->cpuUsage = SERVER_KNOBS->FASTRESTORE_SCHED_TARGET_CPU_PERCENT +
			                 deterministicRandom()->random01() * (SERVER_KNOBS->FASTRESTORE_SCHED_MAX_CPU_PERCENT -
			                                                      SERVER_KNOBS->FASTRESTORE_SCHED_TARGET_CPU_PERCENT);
		} else { // reach desired max cpu usage; use max cpu as 200 to simulate incorrect cpu profiling
			self->cpuUsage =
			    SERVER_KNOBS->FASTRESTORE_SCHED_MAX_CPU_PERCENT +
			    deterministicRandom()->random01() * (200 - SERVER_KNOBS->FASTRESTORE_SCHED_MAX_CPU_PERCENT);
		}
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

// An actor is schedulable to run if the current worker has enough resources, i.e.,
// the worker's memory usage is below the threshold;
// Exception: If the actor is working on the current version batch, we have to schedule
// the actor to run to avoid dead-lock.
// Future: When we release the actors that are blocked by memory usage, we should release them
// in increasing order of their version batch.
Future<Void> isSchedulable(Reference<RestoreRoleData> self, int actorBatchIndex, std::string name) {
	self->delayedActors++;
	double memoryThresholdBytes = SERVER_KNOBS->FASTRESTORE_MEMORY_THRESHOLD_MB_SOFT * 1024 * 1024;
	while (true) {
		double memory = getSystemStatistics().processMemory;
		if (g_network->isSimulated() && BUGGIFY) {
			// Intentionally randomly block actors for low memory reason.
			// memory will be larger than threshold when deterministicRandom()->random01() > 1/2
			if (deterministicRandom()->random01() < 0.4) { // enough memory
				memory = SERVER_KNOBS->FASTRESTORE_MEMORY_THRESHOLD_MB_SOFT * deterministicRandom()->random01();
			} else { // used too much memory, needs throttling
				memory = SERVER_KNOBS->FASTRESTORE_MEMORY_THRESHOLD_MB_SOFT +
				         deterministicRandom()->random01() * SERVER_KNOBS->FASTRESTORE_MEMORY_THRESHOLD_MB_SOFT;
			}
		}
		if (memory < memoryThresholdBytes || self->finishedBatch.get() + 1 == actorBatchIndex) {
			if (memory >= memoryThresholdBytes) {
				TraceEvent(SevWarn, "FastRestoreMemoryUsageAboveThreshold", self->id())
				    .suppressFor(5.0)
				    .detail("Role", getRoleStr(self->role))
				    .detail("BatchIndex", actorBatchIndex)
				    .detail("FinishedBatch", self->finishedBatch.get())
				    .detail("Actor", name)
				    .detail("Memory", memory);
			}
			self->delayedActors--;
			break;
		} else {
			TraceEvent(SevInfo, "FastRestoreMemoryUsageAboveThresholdWait", self->id())
			    .suppressFor(5.0)
			    .detail("Role", getRoleStr(self->role))
			    .detail("BatchIndex", actorBatchIndex)
			    .detail("Actor", name)
			    .detail("CurrentMemory", memory);
			// TODO: Set FASTRESTORE_WAIT_FOR_MEMORY_LATENCY to a large value. It should be able to avoided
			co_await (delay(SERVER_KNOBS->FASTRESTORE_WAIT_FOR_MEMORY_LATENCY) || self->checkMemory.onTrigger());
		}
	}
}

// Updated process metrics will be used by scheduler for throttling as well
Future<Void> updateProcessMetrics(Reference<RestoreRoleData> self) {
	while (true) {
		updateProcessStats(self);
		co_await delay(SERVER_KNOBS->FASTRESTORE_UPDATE_PROCESS_STATS_INTERVAL);
	}
}

Future<Void> traceProcessMetrics(Reference<RestoreRoleData> self, std::string role) {
	while (true) {
		TraceEvent("FastRestoreTraceProcessMetrics", self->nodeID)
		    .detail("Role", role)
		    .detail("PipelinedMaxVersionBatchIndex", self->versionBatchId.get())
		    .detail("FinishedVersionBatchIndex", self->finishedBatch.get())
		    .detail("CurrentVersionBatchPhase", self->getVersionBatchState(self->finishedBatch.get() + 1))
		    .detail("CpuUsage", self->cpuUsage)
		    .detail("UsedMemory", self->memory)
		    .detail("ResidentMemory", self->residentMemory);
		co_await delay(SERVER_KNOBS->FASTRESTORE_ROLE_LOGGING_DELAY);
	}
}

Future<Void> traceRoleVersionBatchProgress(Reference<RestoreRoleData> self, std::string role) {
	while (true) {
		int batchIndex = self->finishedBatch.get();
		int maxBatchIndex = self->versionBatchId.get();
		int maxPrintBatchIndex = batchIndex + SERVER_KNOBS->FASTRESTORE_VB_PARALLELISM;

		TraceEvent ev("FastRestoreVersionBatchProgressState", self->nodeID);
		ev.detail("Role", role)
		    .detail("Node", self->nodeID)
		    .detail("FinishedBatch", batchIndex)
		    .detail("InitializedBatch", maxBatchIndex);
		while (batchIndex <= maxBatchIndex) {
			if (batchIndex > maxPrintBatchIndex) {
				ev.detail("SkipVersionBatches", maxBatchIndex - batchIndex + 1);
				break;
			}
			std::stringstream typeName;
			typeName << "VersionBatch" << batchIndex;
			ev.detail(typeName.str(), self->getVersionBatchState(batchIndex));
			batchIndex++;
		}

		co_await delay(SERVER_KNOBS->FASTRESTORE_ROLE_LOGGING_DELAY);
	}
}
