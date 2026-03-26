/*
 * RestoreWorker.h
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

#pragma once

#include "fdbclient/Tuple.h"
#include <cstdint>
#include <cstdarg>

#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Stats.h"
#include "flow/flow.h"
#include "flow/IAsyncFile.h"
#include "fdbserver/restoreworker/RestoreUtil.h"
#include "fdbserver/restoreworker/RestoreCommon.h"
#include "RestoreRoleCommon.h"
#include "RestoreLoader.h"
#include "RestoreApplier.h"
#include "fdbserver/restoreworker/RestoreWorkerInterface.h"

// Each restore worker (a process) is assigned for a role.
// MAYBE Later: We will support multiple restore roles on a worker
struct RestoreWorkerData : NonCopyable, public ReferenceCounted<RestoreWorkerData> {
	UID workerID;
	std::map<UID, RestoreWorkerInterface>
	    workerInterfaces; // UID is worker's node id, RestoreWorkerInterface is worker's communication workerInterface

	// Restore Roles
	Optional<RestoreControllerInterface> controllerInterf;
	Optional<RestoreLoaderInterface> loaderInterf;
	Optional<RestoreApplierInterface> applierInterf;

	UID id() const { return workerID; };

	RestoreWorkerData() = default;

	~RestoreWorkerData() {
		TraceEvent("RestoreWorkerDataDeleted").detail("WorkerID", workerID.toString());
		printf("[Exit] Worker:%s RestoreWorkerData is deleted\n", workerID.toString().c_str());
	}

	std::string describeNode() {
		std::stringstream ss;
		ss << "RestoreWorker workerID:" << workerID.toString();
		return ss.str();
	}
};
