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

ACTOR static Future<Void> handleSendMutationVectorRequest(RestoreSendVersionedMutationsRequest req,
                                                          Reference<RestoreApplierData> self);
ACTOR static Future<Void> handleApplyToDBRequest(RestoreVersionBatchRequest req, Reference<RestoreApplierData> self,
                                                 Database cx);

ACTOR Future<Void> restoreApplierCore(RestoreApplierInterface applierInterf, int nodeIndex, Database cx) {
	state Reference<RestoreApplierData> self =
	    Reference<RestoreApplierData>(new RestoreApplierData(applierInterf.id(), nodeIndex));
	state ActorCollection actors(false);
	state Future<Void> exitRole = Never();
	state Future<Void> updateProcessStatsTimer = delay(SERVER_KNOBS->FASTRESTORE_UPDATE_PROCESS_STATS_INTERVAL);

	actors.add(traceProcessMetrics(self, "Applier"));

	loop {
		state std::string requestTypeStr = "[Init]";

		try {
			choose {
				when(RestoreSimpleRequest req = waitNext(applierInterf.heartbeat.getFuture())) {
					requestTypeStr = "heartbeat";
					actors.add(handleHeartbeat(req, applierInterf.id()));
				}
				when(RestoreSendVersionedMutationsRequest req =
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
				when(RestoreFinishRequest req = waitNext(applierInterf.finishRestore.getFuture())) {
					requestTypeStr = "finishRestore";
					handleFinishRestoreRequest(req, self);
					if (req.terminate) {
						exitRole = Void();
					}
				}
				when(wait(updateProcessStatsTimer)) {
					updateProcessStats(self);
					updateProcessStatsTimer = delay(SERVER_KNOBS->FASTRESTORE_UPDATE_PROCESS_STATS_INTERVAL);
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
// No race condition as long as we do not wait or yield when operate the shared data.
// Multiple such actors can run on different fileIDs, because mutations in different files belong to different versions;
// Only one actor can process mutations from the same file
ACTOR static Future<Void> handleSendMutationVectorRequest(RestoreSendVersionedMutationsRequest req,
                                                          Reference<RestoreApplierData> self) {
	state Reference<ApplierBatchData> batchData = self->batch[req.batchIndex];
	// Assume: processedFileState[req.asset] will not be erased while the actor is active.
	// Note: Insert new items into processedFileState will not invalidate the reference.
	state NotifiedVersion& curFilePos = batchData->processedFileState[req.asset];

	TraceEvent(SevFRMutationInfo, "FastRestoreApplierPhaseReceiveMutations", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("RestoreAsset", req.asset.toString())
	    .detail("ProcessedFileVersion", curFilePos.get())
	    .detail("Request", req.toString());

	wait(curFilePos.whenAtLeast(req.prevVersion));

	state bool isDuplicated = true;
	if (curFilePos.get() == req.prevVersion) {
		isDuplicated = false;
		Version commitVersion = req.version;
		MutationsVec mutations(req.mutations);
		// Sanity check: mutations in range file is in [beginVersion, endVersion);
		// mutations in log file is in [beginVersion, endVersion], both inclusive.
		ASSERT_WE_THINK(commitVersion >= req.asset.beginVersion);
		// Loader sends the endVersion to ensure all useful versions are sent
		ASSERT_WE_THINK((req.isRangeFile && commitVersion <= req.asset.endVersion) ||
		                (!req.isRangeFile && commitVersion <= req.asset.endVersion));

		if (batchData->kvOps.find(commitVersion) == batchData->kvOps.end()) {
			batchData->kvOps.insert(std::make_pair(commitVersion, MutationsVec()));
		}
		for (int mIndex = 0; mIndex < mutations.size(); mIndex++) {
			MutationRef mutation = mutations[mIndex];
			TraceEvent(SevFRMutationInfo, "FastRestore")
			    .detail("ApplierNode", self->id())
			    .detail("RestoreAsset", req.asset.toString())
			    .detail("Version", commitVersion)
			    .detail("Index", mIndex)
			    .detail("MutationReceived", mutation.toString());
			batchData->counters.receivedBytes += mutation.totalSize();
			batchData->counters.receivedWeightedBytes += mutation.weightedTotalSize(); // atomicOp will be amplified
			batchData->counters.receivedMutations += 1;
			batchData->counters.receivedAtomicOps += isAtomicOp((MutationRef::Type)mutation.type) ? 1 : 0;
			// Sanity check
			if (g_network->isSimulated()) {
				if (isRangeMutation(mutation)) {
					ASSERT(mutation.param1 >= req.asset.range.begin &&
					       mutation.param2 <= req.asset.range.end); // Range mutation's right side is exclusive
				} else {
					ASSERT(mutation.param1 >= req.asset.range.begin && mutation.param1 < req.asset.range.end);
				}
			}
			batchData->kvOps[commitVersion].push_back_deep(batchData->kvOps[commitVersion].arena(), mutation);
			// TODO: What if log file's mutations are delivered out-of-order (behind) the range file's mutations?!
		}
		curFilePos.set(req.version);
	}

	req.reply.send(RestoreCommonReply(self->id(), isDuplicated));
	return Void();
}

// Progress and checkpoint for applying (atomic) mutations in transactions to DB
struct DBApplyProgress {
	// Mutation state in the current uncommitted transaction
	VersionedMutationsMap::iterator curItInCurTxn;
	int curIndexInCurTxn;

	// Save the starting point for current txn to handle (commit_unknown_result) error in txn commit
	// startItInUncommittedTxn is starting iterator in the most recent uncommitted (and failed) txn
	// startIndexInUncommittedTxn is start index in the most recent uncommitted (and failed) txn.
	// Note: Txns have different number of mutations
	VersionedMutationsMap::iterator startItInUncommittedTxn;
	int startIndexInUncommittedTxn;

	// State to decide if a txn succeeds or not when txn error (commit_unknown_result) happens;
	// curTxnId: The id of the current uncommitted txn, which monotonically increase for each successful transaction
	// uncommittedTxnId: The id of the most recent succeeded txn. Used to recover the failed txn id in retry
	// lastTxnHasError: Does the last txn has error. TODO: Only need to handle txn_commit_unknown error
	Version curTxnId;
	Version uncommittedTxnId;
	bool lastTxnHasError;

	// Decide when to commit a transaction. We buffer enough mutations in a txn before commit the txn
	bool startNextVersion; // The next txn will include mutations in next version
	int numAtomicOps; // Status counter
	double txnBytes; // Decide when to commit a txn
	double txnMutations; // Status counter

	Reference<ApplierBatchData> batchData;
	UID applierId;

	DBApplyProgress() = default;
	explicit DBApplyProgress(UID applierId, Reference<ApplierBatchData> batchData)
	  : applierId(applierId), batchData(batchData), curIndexInCurTxn(0), startIndexInUncommittedTxn(0), curTxnId(0),
	    uncommittedTxnId(0), lastTxnHasError(false), startNextVersion(false), numAtomicOps(0), txnBytes(0),
	    txnMutations(0) {
		curItInCurTxn = batchData->kvOps.begin();
		while (curItInCurTxn != batchData->kvOps.end() && curItInCurTxn->second.empty()) {
			curItInCurTxn++;
		}
		startItInUncommittedTxn = curItInCurTxn;
	}

	// Has all mutations been committed?
	bool isDone() { return curItInCurTxn == batchData->kvOps.end(); }

	// Set cursor for next mutation
	void nextMutation() {
		curIndexInCurTxn++;
		while (curItInCurTxn != batchData->kvOps.end() && curIndexInCurTxn >= curItInCurTxn->second.size()) {
			curIndexInCurTxn = 0;
			curItInCurTxn++;
			startNextVersion = true;
		}
	}

	// Setup for the next transaction; This should be done after nextMutation()
	void nextTxn() {
		txnBytes = 0;
		txnMutations = 0;
		numAtomicOps = 0;
		lastTxnHasError = false;
		startNextVersion = false;

		curTxnId++;

		startIndexInUncommittedTxn = curIndexInCurTxn;
		startItInUncommittedTxn = curItInCurTxn;
		uncommittedTxnId = curTxnId;
	}

	// Rollback to the starting point of the uncommitted-and-failed transaction to
	// re-execute uncommitted txn
	void rollback() {
		TraceEvent(SevWarn, "FastRestoreApplyTxnError")
		    .detail("TxnStatusFailed", curTxnId)
		    .detail("ApplierApplyToDB", applierId)
		    .detail("UncommittedTxnId", uncommittedTxnId)
		    .detail("CurIteratorVersion", curItInCurTxn->first)
		    .detail("StartIteratorVersionInUncommittedTxn", startItInUncommittedTxn->first)
		    .detail("CurrentIndexInFailedTxn", curIndexInCurTxn)
		    .detail("StartIndexInUncommittedTxn", startIndexInUncommittedTxn)
		    .detail("NumIncludedAtomicOps", numAtomicOps);
		curItInCurTxn = startItInUncommittedTxn;
		curIndexInCurTxn = startIndexInUncommittedTxn;
		curTxnId = uncommittedTxnId;

		numAtomicOps = 0;
		txnBytes = 0;
		txnMutations = 0;
		startNextVersion = false;
		lastTxnHasError = false;
	}

	bool shouldCommit() {
		return (!lastTxnHasError && (startNextVersion || txnBytes >= SERVER_KNOBS->FASTRESTORE_TXN_BATCH_MAX_BYTES ||
		                             curItInCurTxn == batchData->kvOps.end()));
	}

	bool hasError() { return lastTxnHasError; }

	void setTxnError(Error& e) {
		TraceEvent(SevWarnAlways, "FastRestoreApplyTxnError")
		    .detail("TxnStatus", "?")
		    .detail("ApplierApplyToDB", applierId)
		    .detail("TxnId", curTxnId)
		    .detail("StartIndexInCurrentTxn", curIndexInCurTxn)
		    .detail("Version", curItInCurTxn->first)
		    .error(e, true);
		lastTxnHasError = true;
	}

	MutationRef getCurrentMutation() {
		ASSERT_WE_THINK(curIndexInCurTxn < curItInCurTxn->second.size());
		return curItInCurTxn->second[curIndexInCurTxn];
	}
};

ACTOR Future<Void> applyToDB(UID applierID, int64_t batchIndex, Reference<ApplierBatchData> batchData, Database cx) {
	// state variables must be defined at the start of actor to be initialized in the actor constructor
	state std::string typeStr = "";
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state DBApplyProgress progress(applierID, batchData);

	TraceEvent("FastRestoreApplerPhaseApplyTxn", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("FromVersion", batchData->kvOps.empty() ? -1 : batchData->kvOps.begin()->first)
	    .detail("EndVersion", batchData->kvOps.empty() ? -1 : batchData->kvOps.rbegin()->first);

	// Assume the process will not crash when it apply mutations to DB. The reply message can be lost though
	if (batchData->kvOps.empty()) {
		TraceEvent("FastRestoreApplerPhaseApplyTxnDone", applierID)
		    .detail("BatchIndex", batchIndex)
		    .detail("Reason", "NoMutationAtVersions");
		return Void();
	}

	batchData->sanityCheckMutationOps();

	if (progress.isDone()) {
		TraceEvent("FastRestoreApplerPhaseApplyTxnDone", applierID)
		    .detail("BatchIndex", batchIndex)
		    .detail("Reason", "NoMutationAtVersions");
		return Void();
	}

	// Sanity check the restoreApplierKeys, which should be empty at this point
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			Key begin = restoreApplierKeyFor(applierID, batchIndex, 0);
			Key end = restoreApplierKeyFor(applierID, batchIndex, std::numeric_limits<int64_t>::max());
			Standalone<RangeResultRef> txnIds = wait(tr->getRange(KeyRangeRef(begin, end), CLIENT_KNOBS->TOO_MANY));
			if (txnIds.size() > 0) {
				TraceEvent(SevError, "FastRestoreApplyTxnStateNotClean").detail("TxnIds", txnIds.size());
				for (auto& kv : txnIds) {
					UID id;
					int64_t index;
					Version txnId;
					std::tie(id, index, txnId) = decodeRestoreApplierKey(kv.key);
					TraceEvent(SevError, "FastRestoreApplyTxnStateNotClean")
					    .detail("Applier", id)
					    .detail("BatchIndex", index)
					    .detail("ResidueTxnID", txnId);
				}
			}
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	loop { // Transaction retry loop
		try {
			// Check if the transaction succeeds
			if (progress.hasError()) {
				tr->reset();
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> txnSucceeded =
				    wait(tr->get(restoreApplierKeyFor(applierID, batchIndex, progress.curTxnId)));
				if (!txnSucceeded.present()) {
					progress.rollback();
					continue;
				} else {
					TraceEvent(SevWarn, "FastRestoreApplyTxnError")
					    .detail("TxnStatusSucceeded", progress.curTxnId)
					    .detail("ApplierApplyToDB", applierID)
					    .detail("CurIteratorVersion", progress.curItInCurTxn->first)
					    .detail("CurrentIteratorMutations", progress.curItInCurTxn->second.size())
					    .detail("CurrentIndexInSucceedTxn", progress.curIndexInCurTxn)
					    .detail("NumIncludedAtomicOps", progress.numAtomicOps);
					// Txn succeeded and exectue the same logic when txn succeeds
				}
			} else { // !lastTxnHasError: accumulate mutations in a txn
				tr->reset();
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				TraceEvent(SevFRMutationInfo, "FastRestore_ApplierTxn")
				    .detail("ApplierApplyToDB", applierID)
				    .detail("TxnId", progress.curTxnId)
				    .detail("CurrentIndexInCurrentTxn", progress.curIndexInCurTxn)
				    .detail("CurrentIteratorMutations", progress.curItInCurTxn->second.size())
				    .detail("Version", progress.curItInCurTxn->first);

				// restoreApplierKeyFor(self->id(), curTxnId) to tell if txn succeeds at an unknown error
				tr->set(restoreApplierKeyFor(applierID, batchIndex, progress.curTxnId), restoreApplierTxnValue);

				while (1) { // Loop: Accumulate mutations in a transaction
					MutationRef m = progress.getCurrentMutation();

					if (m.type >= MutationRef::Type::SetValue && m.type <= MutationRef::Type::MAX_ATOMIC_OP) {
						typeStr = typeString[m.type];
					} else {
						TraceEvent(SevError, "FastRestore").detail("InvalidMutationType", m.type);
					}

					TraceEvent(SevFRMutationInfo, "FastRestore")
					    .detail("ApplierApplyToDB", applierID)
					    .detail("Version", progress.curItInCurTxn->first)
					    .detail("Index", progress.curIndexInCurTxn)
					    .detail("Mutation", m.toString())
					    .detail("MutationSize", m.totalSize())
					    .detail("TxnSize", progress.txnBytes);
					if (m.type == MutationRef::SetValue) {
						tr->set(m.param1, m.param2);
					} else if (m.type == MutationRef::ClearRange) {
						KeyRangeRef mutationRange(m.param1, m.param2);
						tr->clear(mutationRange);
					} else if (isAtomicOp((MutationRef::Type)m.type)) {
						tr->atomicOp(m.param1, m.param2, m.type);
						progress.numAtomicOps++;
					} else {
						TraceEvent(SevError, "FastRestore")
							.detail("UnhandledMutationType", m.type)
							.detail("TypeName", typeStr);
					}

					progress.txnBytes += m.totalSize(); // Changed expectedSize to totalSize
					progress.txnMutations += 1;

					progress.nextMutation(); // Prepare for the next mutation
					// commit per FASTRESTORE_TXN_BATCH_MAX_BYTES bytes; and commit does not cross version boundary
					if (progress.shouldCommit()) {
						break; // Got enough mutation in the txn
					}
				}
			} // !lastTxnHasError

			// Commit the txn and prepare the starting point for next txn
			if (progress.shouldCommit()) {
				wait(tr->commit());
				// Update status counter appliedWeightedBytes, appliedMutations, atomicOps
				batchData->counters.appliedWeightedBytes += progress.txnBytes;
				batchData->counters.appliedMutations += progress.txnMutations;
				batchData->counters.appliedAtomicOps += progress.numAtomicOps;
				batchData->counters.appliedTxns += 1;
			}

			if (progress.isDone()) { // Are all mutations processed?
				break;
			}
			progress.nextTxn();
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "FastRestoreApplyTxnError")
			    .detail("TxnStatus", "?")
			    .detail("ApplierApplyToDB", applierID)
			    .detail("TxnId", progress.curTxnId)
			    .detail("CurrentIndexInCurrentTxn", progress.curIndexInCurTxn)
			    .detail("Version", progress.curItInCurTxn->first)
			    .error(e, true);
			progress.lastTxnHasError = true;
			wait(tr->onError(e));
		}
	}

	TraceEvent("FastRestoreApplerPhaseApplyTxn", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("CleanupCurTxnIds", progress.curTxnId);
	// clean up txn ids
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			// Clear txnIds in [0, progress.curTxnId). We add 100 to curTxnId just to be safe.
			tr->clear(KeyRangeRef(restoreApplierKeyFor(applierID, batchIndex, 0),
			                      restoreApplierKeyFor(applierID, batchIndex, progress.curTxnId + 100)));
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
	// House cleaning
	batchData->kvOps.clear();
	TraceEvent("FastRestoreApplerPhaseApplyTxnDone", applierID).detail("BatchIndex", batchIndex);

	return Void();
}

ACTOR static Future<Void> handleApplyToDBRequest(RestoreVersionBatchRequest req, Reference<RestoreApplierData> self,
                                                 Database cx) {
	// Ensure batch (i-1) is applied before batch i
	wait(self->finishedBatch.whenAtLeast(req.batchIndex - 1));

	state bool isDuplicated = true;
	Reference<ApplierBatchData> batchData = self->batch[req.batchIndex];
	TraceEvent("FastRestoreApplierPhaseHandleApplyToDB", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("FinishedBatch", self->finishedBatch.get())
	    .detail("HasStarted", batchData->dbApplier.present());
	if (self->finishedBatch.get() == req.batchIndex - 1) {
		ASSERT(batchData.isValid());
		if (!batchData->dbApplier.present()) {
			isDuplicated = false;
			batchData->dbApplier = Never();
			batchData->dbApplier = applyToDB(self->id(), req.batchIndex, batchData, cx);
		}

		ASSERT(batchData->dbApplier.present());

		wait(batchData->dbApplier.get());

		// Multiple actor invokation can wait on req.batchIndex-1;
		// Avoid setting finishedBatch when finishedBatch > req.batchIndex
		if (self->finishedBatch.get() == req.batchIndex - 1) {
			self->finishedBatch.set(req.batchIndex);
		}
	}
	req.reply.send(RestoreCommonReply(self->id(), isDuplicated));

	return Void();
}