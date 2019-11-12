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

void handleFinishRestoreRequest(const RestoreVersionBatchRequest& req, Reference<RestoreRoleData> self) {
	if (self->versionBatchStart) {
		self->versionBatchStart = false;
	}

	TraceEvent("FastRestore")
	    .detail("FinishRestoreRequest", req.batchID)
	    .detail("Role", getRoleStr(self->role))
	    .detail("Node", self->id());

	req.reply.send(RestoreCommonReply(self->id()));
}

ACTOR Future<Void> handleInitVersionBatchRequest(RestoreVersionBatchRequest req, Reference<RestoreRoleData> self) {
	// batchId is continuous. (req.batchID-1) is the id of the just finished batch.
	wait(self->versionBatchId.whenAtLeast(req.batchID - 1));

	if (self->versionBatchId.get() == req.batchID - 1) {
		self->resetPerVersionBatch();
		TraceEvent("FastRestore")
		    .detail("InitVersionBatch", req.batchID)
		    .detail("Role", getRoleStr(self->role))
		    .detail("Node", self->id());
		self->versionBatchId.set(req.batchID);
	}

	req.reply.send(RestoreCommonReply(self->id()));
	return Void();
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
