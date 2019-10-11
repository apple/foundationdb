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

// The actor may be invovked multiple times and executed async.
// No race condition as long as we do not wait or yield when operate the shared data, it should be fine,
// because all actors run on 1 thread.
ACTOR static Future<Void> handleSendMutationVectorRequest(RestoreSendMutationVectorVersionedRequest req,
                                                          Reference<RestoreApplierData> self) {
	state int numMutations = 0;

	TraceEvent("FastRestore")
	    .detail("ApplierNode", self->id())
	    .detail("LogVersion", self->logVersion.get())
	    .detail("RangeVersion", self->rangeVersion.get())
	    .detail("Request", req.toString());

	if (req.isRangeFile) {
		wait(self->rangeVersion.whenAtLeast(req.prevVersion));
	} else {
		wait(self->logVersion.whenAtLeast(req.prevVersion));
	}

	// Not a duplicate (check relies on no waiting between here and self->version.set() below!)
	if ((req.isRangeFile && self->rangeVersion.get() == req.prevVersion) ||
	    (!req.isRangeFile && self->logVersion.get() == req.prevVersion)) { 
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
			self->rangeVersion.set(req.version);
		} else {
			self->logVersion.set(req.version);
		}
	}

	req.reply.send(RestoreCommonReply(self->id()));
	return Void();
}

ACTOR Future<Void> applyToDB(Reference<RestoreApplierData> self, Database cx) {
	state std::string typeStr = "";

	// Assume the process will not crash when it apply mutations to DB. The reply message can be lost though
	if (self->kvOps.empty()) {
		TraceEvent("FastRestore_ApplierTxn")
		    .detail("ApplierApplyToDBFinished", self->id())
		    .detail("Reason", "EmptyVersionMutation");
		return Void();
	}
	ASSERT_WE_THINK(self->kvOps.size());
	std::map<Version, Standalone<VectorRef<MutationRef>>>::iterator begin = self->kvOps.begin();
	TraceEvent("FastRestore")
	    .detail("ApplierApplyToDB", self->id())
	    .detail("FromVersion", begin->first)
	    .detail("EndVersion", self->kvOps.rbegin()->first);

	self->sanityCheckMutationOps();

	// When the current txn fails and retries, startItInUncommittedTxn is the starting iterator in retry;
	// startIndexInUncommittedTxn is the starting index in retry;
	state std::map<Version, Standalone<VectorRef<MutationRef>>>::iterator curItInCurTxn = self->kvOps.begin();
	state int curIndexInCurTxn = 0; // current index in current txn; it increases per mutation

	// In case a version has 0 txns
	while (curItInCurTxn != self->kvOps.end() && curIndexInCurTxn >= curItInCurTxn->second.size()) {
		curIndexInCurTxn = 0;
		curItInCurTxn++;
	}
	if (curItInCurTxn == self->kvOps.end()) {
		TraceEvent("FastRestore_ApplierTxn")
		    .detail("ApplierApplyToDBFinished", self->id())
		    .detail("Reason", "NoMutationAtVersions");
		return Void();
	}
	// Save the starting point for current txn
	// startItInUncommittedTxn is starting iterator in the most recent succeeded txn
	// startIndexInUncommittedTxn is start index in the most recent succeeded txn. Note: Txns have different number of mutations
	state std::map<Version, Standalone<VectorRef<MutationRef>>>::iterator startItInUncommittedTxn = curItInCurTxn;
	state int startIndexInUncommittedTxn = curIndexInCurTxn;

	// Track txn succeess or fail; Handle commit_unknown_result in txn commit
	// curTxnId: The id of the current uncommitted txn, which monotonically increase for each successful transaction
	// uncommittedTxnId: The id of the most recent succeeded txn. Used to recover the failed txn id in retry
	// lastTxnHasError: Does the last txn has error. TODO: Only need to handle txn_commit_unknown error
	state Version curTxnId = 0; 
	state Version uncommittedTxnId = 0; 
	state bool lastTxnHasError = false; 

	// Decide when to commit a transaction. We buffer enough mutations in a txn before commit the txn
	state bool startNextVersion = false; // The next txn will include mutations in next version
	state int numAtomicOps = 0;
	state double transactionSize = 0;

	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

	loop { // Transaction retry loop
		try {
			// Check if the transaction succeeds
			if (lastTxnHasError) {
				tr->reset();
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> txnSucceeded = wait(tr->get(restoreApplierKeyFor(self->id(), curTxnId)));
				if (!txnSucceeded.present()) {
					TraceEvent(SevWarn, "FastRestore_ApplyTxnError")
					    .detail("TxnStatusFailed", curTxnId)
					    .detail("ApplierApplyToDB", self->id())
					    .detail("CurrentFailedTxnId", curIndexInCurTxn)
					    .detail("UncommittedTxnId", uncommittedTxnId)
					    .detail("CurIteratorVersion", curItInCurTxn->first)
					    .detail("StartIteratorVersionInUncommittedTxn", startItInUncommittedTxn->first)
					    .detail("CurrentIndexInFailedTxn", curIndexInCurTxn)
					    .detail("StartIndexInUncommittedTxn", startIndexInUncommittedTxn)
					    .detail("NumIncludedAtomicOps", numAtomicOps);
					// Re-execute uncommitted txn
					curItInCurTxn = startItInUncommittedTxn;
					curIndexInCurTxn = startIndexInUncommittedTxn;
					curTxnId = uncommittedTxnId;

					numAtomicOps = 0;
					transactionSize = 0;
					startNextVersion = false;

					lastTxnHasError = false;
					continue;
				} else {
					TraceEvent(SevWarn, "FastRestore_ApplyTxnError")
					    .detail("TxnStatusSucceeded", curTxnId)
					    .detail("ApplierApplyToDB", self->id())
					    .detail("CurrentSucceedTxnId", curIndexInCurTxn)
					    .detail("CurIteratorVersion", curItInCurTxn->first)
					    .detail("CurrentIteratorMutations", curItInCurTxn->second.size())
					    .detail("CurrentIndexInSucceedTxn", curIndexInCurTxn)
					    .detail("NumIncludedAtomicOps", numAtomicOps);

					// Skip else, and execute the logic when a txn succeed
				}
			} else { // !lastTxnHasError: accumulate mutations in a txn
				tr->reset();
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				TraceEvent("FastRestore_ApplierTxn")
				    .detail("ApplierApplyToDB", self->id())
				    .detail("TxnId", curTxnId)
				    .detail("StartIndexInCurrentTxn", curIndexInCurTxn)
				    .detail("CurrentIteratorMutations", curItInCurTxn->second.size())
				    .detail("Version", curItInCurTxn->first);

				// restoreApplierKeyFor(self->id(), curTxnId) to tell if txn succeeds at an unknown error
				tr->set(restoreApplierKeyFor(self->id(), curTxnId), restoreApplierTxnValue);

				loop { // Loop: Accumulate mutations in a transaction
					state MutationRef m;
					ASSERT_WE_THINK(curIndexInCurTxn < curItInCurTxn->second.size());

					m = curItInCurTxn->second[curIndexInCurTxn];
					if (m.type >= MutationRef::Type::SetValue && m.type <= MutationRef::Type::MAX_ATOMIC_OP) {
						typeStr = typeString[m.type];
					} else {
						TraceEvent(SevError, "FastRestore").detail("InvalidMutationType", m.type);
					}

					//TraceEvent(SevDebug, "FastRestore_Debug").detail("ApplierApplyToDB", self->describeNode()).detail("Version", it->first).detail("Mutation", m.toString());
					if (m.type == MutationRef::SetValue) {
						tr->set(m.param1, m.param2);
					} else if (m.type == MutationRef::ClearRange) {
						KeyRangeRef mutationRange(m.param1, m.param2);
						tr->clear(mutationRange);
					} else if (isAtomicOp((MutationRef::Type)m.type)) {
						tr->atomicOp(m.param1, m.param2, m.type);
						numAtomicOps++;
					} else {
						TraceEvent(SevError, "FastRestore")
						    .detail("UnhandledMutationType", m.type)
						    .detail("TypeName", typeStr);
					}

					transactionSize += m.expectedSize();

					if (transactionSize >= opConfig.transactionBatchSizeThreshold) { // commit per 512B
						break; // Got enough mutation in the txn
					} else {
						curIndexInCurTxn++;
						while (curItInCurTxn != self->kvOps.end() && curIndexInCurTxn >= curItInCurTxn->second.size()) {
							curIndexInCurTxn = 0;
							curItInCurTxn++;
							startNextVersion = true;
						}

						if (startNextVersion || curItInCurTxn == self->kvOps.end()) {
							break;
						}
					}
				}
			} // !lastTxnHasError

			// Commit the txn and prepare the starting point for next txn
			if (!lastTxnHasError && (startNextVersion || transactionSize > 0 || curItInCurTxn == self->kvOps.end())) {
				wait(tr->commit());
			}

			// Logic for a successful transaction: Update current txn info and uncommitted txn info
			lastTxnHasError = false;
			curIndexInCurTxn++;
			while (curItInCurTxn != self->kvOps.end() && curIndexInCurTxn >= curItInCurTxn->second.size()) {
				curIndexInCurTxn = 0;
				curItInCurTxn++;
			}
			if (curItInCurTxn == self->kvOps.end()) {
				break;
			}
			curTxnId++;

			startIndexInUncommittedTxn = curIndexInCurTxn;
			startItInUncommittedTxn = curItInCurTxn;
			uncommittedTxnId = curTxnId;

			transactionSize = 0;
			numAtomicOps = 0;
			startNextVersion = false;
			//}
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "FastRestore_ApplyTxnError")
			    .detail("Error", e.what())
			    .detail("TxnStatus", "?")
			    .detail("ApplierApplyToDB", self->id())
			    .detail("TxnId", curTxnId)
			    .detail("StartIndexInCurrentTxn", curIndexInCurTxn)
			    .detail("Version", curItInCurTxn->first);
			lastTxnHasError = true;
			// if (e.code() == commit_unknown_result) {
			// 	lastTxnHasError = true;
			// }
			wait(tr->onError(e));
		}
	}

	TraceEvent("FastRestore_ApplierTxn")
	    .detail("ApplierApplyToDBFinished", self->id())
	    .detail("CleanupCurTxnIds", curTxnId);
	// House cleaning
	self->kvOps.clear();
	// clean up txn ids
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->clear(KeyRangeRef(restoreApplierKeyFor(self->id(), 0), restoreApplierKeyFor(self->id(), curTxnId + 1)));
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
	TraceEvent("FastRestore_ApplierTxn").detail("ApplierApplyToDBFinished", self->id());

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