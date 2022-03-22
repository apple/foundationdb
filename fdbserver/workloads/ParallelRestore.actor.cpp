/*
 * ParallelRestore.actor.cpp
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

#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbserver/RestoreWorkerInterface.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// A workload which test the correctness of backup and restore process
struct RunRestoreWorkerWorkload : TestWorkload {
	Future<Void> worker;
	RunRestoreWorkerWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		TraceEvent("RunRestoreWorkerWorkloadMX").log();
	}

	std::string description() const override { return "RunRestoreWorkerWorkload"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		int num_myWorkers = SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS + SERVER_KNOBS->FASTRESTORE_NUM_LOADERS + 1;
		TraceEvent("RunParallelRestoreWorkerWorkload")
		    .detail("Start", "RestoreToolDB")
		    .detail("Workers", num_myWorkers);
		printf("RunParallelRestoreWorkerWorkload, we will start %d restore workers\n", num_myWorkers);
		std::vector<Future<Void>> myWorkers;
		myWorkers.reserve(num_myWorkers);
		for (int i = 0; i < num_myWorkers; ++i) {
			myWorkers.push_back(_restoreWorker(cx, LocalityData()));
		}
		printf("RunParallelRestoreWorkerWorkload, wait on reply from %ld restore workers\n", myWorkers.size());
		worker = waitForAll(myWorkers);
		printf("RunParallelRestoreWorkerWorkload, got all replies from restore workers\n");
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RunRestoreWorkerWorkload> RunRestoreWorkerWorkloadFactory("RunRestoreWorkerWorkload");
