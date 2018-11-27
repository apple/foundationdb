/*
 * ParallelRestore.actor.cpp
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

#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/workloads/workloads.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbserver/RestoreInterface.h"
#include "flow/actorcompiler.h"  // This must be the last #include.


//A workload which test the correctness of backup and restore process
struct RunRestoreWorkerWorkload : TestWorkload {
	Future<Void> worker;
	RunRestoreWorkerWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx) {
		TraceEvent("RunRestoreWorkerWorkloadMX");
	}

	virtual std::string description() {
		return "RunRestoreWorkerWorkload";
	}

	virtual Future<Void> setup(Database const& cx) {
		return Void();
	}

	virtual Future<Void> start(Database const& cx) {
		TraceEvent("RunRestoreWorkerWorkloadMX").detail("Start", "RestoreAgentDB");
		worker = _restoreWorker(cx, LocalityData());
		return Void();
	}

	virtual Future<bool> check(Database const& cx) {
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}
};

WorkloadFactory<RunRestoreWorkerWorkload> RunRestoreWorkerWorkloadFactory("RunRestoreWorkerWorkload");
