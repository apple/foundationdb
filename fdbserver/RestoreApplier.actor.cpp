/*
 * RestoreApplier.actor.cpp
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

// This file defines the functions used by the RestoreApplier role.
// RestoreApplier role starts at restoreApplierCore actor

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/RestoreCommon.actor.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreApplier.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR static Future<Void> handleSendMutationVectorRequest(RestoreSendMutationVectorVersionedRequest req,
                                                          Reference<RestoreApplierData> self);
ACTOR static Future<Void> handleApplyToDBRequest(RestoreVersionBatchRequest req, Reference<RestoreApplierData> self,
                                                 Database cx);

ACTOR Future<Void> restoreApplierCore(RestoreApplierInterface applierInterf, int nodeIndex, Database cx) {
	state Reference<RestoreApplierData> self =
	    Reference<RestoreApplierData>(new RestoreApplierData(applierInterf.id(), nodeIndex));

	state ActorCollection actors(false);
	state Future<Void> exitRole = Never();
	loop {
		state std::string requestTypeStr = "[Init]";

		try {
			choose {
				when(RestoreSimpleRequest req = waitNext(applierInterf.heartbeat.getFuture())) {
					requestTypeStr = "heartbeat";
					actors.add(handleHeartbeat(req, applierInterf.id()));
				}
				when(RestoreSendMutationVectorVersionedRequest req =
				         waitNext(applierInterf.sendMutationVector.getFuture())) {
					requestTypeStr = "sendMutationVector";
					actors.add(handleSendMutationVectorRequest(req, self));
				}
				when(RestoreVersionBatchRequest req = waitNext(applierInterf.applyToDB.getFuture())) {
					requestTypeStr = "applyToDB";
					actors.add(handleApplyToDBRequest(req, self, cx));
				}
				when(RestoreVersionBatchRequest req = waitNext(applierInterf.initVersionBatch.getFuture())) {
					requestTypeStr = "initVersionBatch";
					actors.add(handleInitVersionBatchRequest(req, self));
				}
				when(RestoreVersionBatchRequest req = waitNext(applierInterf.finishRestore.getFuture())) {
					requestTypeStr = "finishRestore";
					exitRole = handleFinishRestoreRequest(req, self);
				}
				when(wait(exitRole)) {
					TraceEvent("FastRestore").detail("RestoreApplierCore", "ExitRole").detail("NodeID", self->id());
					break;
				}
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "FastRestore")
			    .detail("RestoreLoaderError", e.what())
			    .detail("RequestType", requestTypeStr);
			break;
		}
	}

	return Void();
}

double getNotifiedDoubleValue(Version ver, int fileIndex) {
	return ver * SERVER_KNOBS->FASTRESTORE_MAX_FILES_IN_VB + fileIndex;
}

void getDetailsOfNotifiedDoubleValue(double val, Version &version, int &fileIndex) {
	version = val / SERVER_KNOBS->FASTRESTORE_MAX_FILES_IN_VB;
	fileIndex = val - version * SERVER_KNOBS->FASTRESTORE_MAX_FILES_IN_VB;
	return;
}

// The actor may be invovked multiple times and executed async.
// No race condition as long as we do not wait or yield when operate the shared data, it should be fine,
// because all actors run on 1 thread.
ACTOR static Future<Void> handleSendMutationVectorRequest(RestoreSendMutationVectorVersionedRequest req,
                                                          Reference<RestoreApplierData> self) {
	state int numMutations = 0;
	state double prevDoubleVersion = getNotifiedDoubleValue(req.prevVersion, req.prevFileIndex);
	state double doubleVersion = getNotifiedDoubleValue(req.version, req.fileIndex);
	state Version logVersion, rangeVersion;
	state int logFileIndex, rangeFileIndex;
	getDetailsOfNotifiedDoubleValue(self->logVersion.get(), logVersion, logFileIndex);
	getDetailsOfNotifiedDoubleValue(self->rangeVersion.get(), rangeVersion, rangeFileIndex);

	TraceEvent("FastRestore")
	    .detail("ApplierNode", self->id())
	    .detail("LogVersion", logVersion)
		.detail("LogFileIndex", logFileIndex)
	    .detail("RangeVersion", rangeVersion)
		.detail("RangeFileIndex", rangeFileIndex)
	    .detail("Request", req.toString());

	if (req.isRangeFile) {
		wait(self->rangeVersion.whenAtLeast(prevDoubleVersion));
	} else {
		wait(self->logVersion.whenAtLeast(prevDoubleVersion));
	}

	// Not a duplicate (check relies on no waiting between here and self->version.set() below!)
	if ((req.isRangeFile && self->rangeVersion.get() == prevDoubleVersion) ||
	    (!req.isRangeFile && self->logVersion.get() == prevDoubleVersion)) { 
		// Applier will cache the mutations at each version. Once receive all mutations, applier will apply them to DB
		state Version commitVersion = req.version;
		VectorRef<MutationRef> mutations(req.mutations);
		if (self->kvOps.find(commitVersion) == self->kvOps.end()) {
			self->kvOps.insert(std::make_pair(commitVersion, VectorRef<MutationRef>()));
		}
		state int mIndex = 0;
		for (mIndex = 0; mIndex < mutations.size(); mIndex++) {
			MutationRef mutation = mutations[mIndex];
			self->kvOps[commitVersion].push_back_deep(self->kvOps[commitVersion].arena(), mutation);
			numMutations++;
		}

		// Notify the same actor and unblock the request at the next version
		if (req.isRangeFile) {
			self->rangeVersion.set(doubleVersion);
		} else {
			self->logVersion.set(doubleVersion);
		}
	}

	req.reply.send(RestoreCommonReply(self->id()));
	return Void();
}

ACTOR Future<Void> applyToDB(Reference<RestoreApplierData> self, Database cx) {
	state std::string typeStr = "";

	// Assume the process will not crash when it apply mutations to DB. The reply message can be lost though
	if (self->kvOps.empty()) {
		TraceEvent("FastRestore").detail("ApplierApplyToDBEmpty", self->id());
		return Void();
	}
	ASSERT_WE_THINK(self->kvOps.size());
	std::map<Version, Standalone<VectorRef<MutationRef>>>::iterator begin = self->kvOps.begin();
	TraceEvent("FastRestore")
	    .detail("ApplierApplyToDB", self->id())
	    .detail("FromVersion", begin->first)
	    .detail("EndVersion", self->kvOps.rbegin()->first);

	self->sanityCheckMutationOps();

	state std::map<Version, Standalone<VectorRef<MutationRef>>>::iterator it = self->kvOps.begin();
	state std::map<Version, Standalone<VectorRef<MutationRef>>>::iterator prevIt = it;
	state int index = 0;
	state int prevIndex = index;
	state int count = 0;
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state int numVersion = 0;
	state double transactionSize = 0;
	state int numAtomicOp = 0;
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			transactionSize = 0;
			numAtomicOp = 0;

			for (; it != self->kvOps.end(); ++it) {
				numVersion++;
				//TraceEvent("FastRestore").detail("Applier", self->id()).detail("ApplyKVsToDBVersion", it->first);
				state MutationRef m;
				for (; index < it->second.size(); ++index) {
					m = it->second[index];
					if (m.type >= MutationRef::Type::SetValue && m.type <= MutationRef::Type::MAX_ATOMIC_OP) {
						typeStr = typeString[m.type];
					}
					else {
						TraceEvent(SevError, "FastRestore").detail("InvalidMutationType", m.type);
					}

					TraceEvent(SevDebug, "FastRestore_Debug").detail("Applier", self->describeNode()).detail("Version", it->first).detail("Mutation", m.toString());
					if (m.type == MutationRef::SetValue) {
						tr->set(m.param1, m.param2);
					} else if (m.type == MutationRef::ClearRange) {
						KeyRangeRef mutationRange(m.param1, m.param2);
						tr->clear(mutationRange);
					} else if (isAtomicOp((MutationRef::Type)m.type)) {
						tr->atomicOp(m.param1, m.param2, m.type);
						numAtomicOp++;
					} else {
						TraceEvent(SevError, "FastRestore")
						    .detail("UnhandledMutationType", m.type)
						    .detail("TypeName", typeStr);
					}
					++count;
					transactionSize += m.expectedSize();

					if (transactionSize >= opConfig.transactionBatchSizeThreshold) { // commit per 1000 mutations
						wait(tr->commit());
						tr->reset();
						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);
						prevIt = it;
						prevIndex = index;
						transactionSize = 0;
						numAtomicOp = 0;
					}
				}

				if (transactionSize > 0) { // the commit batch should NOT across versions
					wait(tr->commit());
					tr->reset();
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					prevIt = it;
					prevIndex = index;
					transactionSize = 0;
					numAtomicOp = 0;
				}
				index = 0;
			}
			// Last transaction
			if (transactionSize > 0) {
				wait(tr->commit());
			}
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
			it = prevIt;
			index = prevIndex;
			transactionSize = 0;
			TraceEvent(SevError, "FastRestoreError").detail("Reason", "Apply to DB Transaction with atomicOps failed").detail("ApplierApplyToDB", self->id()).detail("NumIncludedAtomicOps", numAtomicOp);
		}
	}

	self->kvOps.clear();

	return Void();
}

ACTOR static Future<Void> handleApplyToDBRequest(RestoreVersionBatchRequest req, Reference<RestoreApplierData> self,
                                                 Database cx) {
	TraceEvent("FastRestore")
	    .detail("ApplierApplyToDB", self->id())
	    .detail("DBApplierPresent", self->dbApplier.present());
	if (!self->dbApplier.present()) {
		self->dbApplier = applyToDB(self, cx);
	}

	ASSERT(self->dbApplier.present());

	wait(self->dbApplier.get());
	req.reply.send(RestoreCommonReply(self->id()));

	return Void();
}