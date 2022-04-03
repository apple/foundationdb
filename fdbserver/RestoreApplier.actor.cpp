/*
 * RestoreApplier.actor.cpp
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

// This file defines the functions used by the RestoreApplier role.
// RestoreApplier role starts at restoreApplierCore actor

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RestoreCommon.actor.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreApplier.actor.h"

#include "flow/network.h"

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR static Future<Void> handleSendMutationVectorRequest(RestoreSendVersionedMutationsRequest req,
                                                          Reference<RestoreApplierData> self);
ACTOR static Future<Void> handleApplyToDBRequest(RestoreVersionBatchRequest req,
                                                 Reference<RestoreApplierData> self,
                                                 Database cx);
void handleUpdateRateRequest(RestoreUpdateRateRequest req, Reference<RestoreApplierData> self);

ACTOR Future<Void> restoreApplierCore(RestoreApplierInterface applierInterf, int nodeIndex, Database cx) {
	state Reference<RestoreApplierData> self = makeReference<RestoreApplierData>(applierInterf.id(), nodeIndex);
	state ActorCollection actors(false);
	state Future<Void> exitRole = Never();

	actors.add(updateProcessMetrics(self));
	actors.add(traceProcessMetrics(self, "RestoreApplier"));
	actors.add(traceRoleVersionBatchProgress(self, "RestoreApplier"));

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
					actors.add(handleApplyToDBRequest(
					    req, self, cx)); // TODO: Check how FDB uses TaskPriority for ACTORS. We may need to add
					                     // priority here to avoid requests at later VB block requests at earlier VBs
				}
				when(RestoreUpdateRateRequest req = waitNext(applierInterf.updateRate.getFuture())) {
					requestTypeStr = "updateRate";
					handleUpdateRateRequest(req, self);
				}
				when(RestoreVersionBatchRequest req = waitNext(applierInterf.initVersionBatch.getFuture())) {
					requestTypeStr = "initVersionBatch";
					actors.add(handleInitVersionBatchRequest(req, self));
				}
				when(RestoreFinishRequest req = waitNext(applierInterf.finishRestore.getFuture())) {
					requestTypeStr = "finishRestore";
					actors.clear(false); // cancel all pending actors
					handleFinishRestoreRequest(req, self);
					if (req.terminate) {
						exitRole = Void();
					}
				}
				when(wait(actors.getResult())) {}
				when(wait(exitRole)) {
					TraceEvent("RestoreApplierCoreExitRole", self->id());
					break;
				}
			}
			//TraceEvent("RestoreApplierCore", self->id()).detail("Request", requestTypeStr); // For debug only
		} catch (Error& e) {
			bool isError = e.code() != error_code_operation_cancelled;
			TraceEvent(isError ? SevError : SevWarnAlways, "FastRestoreApplierError", self->id())
			    .errorUnsuppressed(e)
			    .detail("RequestType", requestTypeStr);
			actors.clear(false);
			break;
		}
	}

	return Void();
}

// The actor may be invoked multiple times and executed async.
// No race condition as long as we do not wait or yield when operate the shared
// data. Multiple such actors can run on different fileIDs.
// Different files may contain mutations of the same commit versions, but with
// different subsequence number.
// Only one actor can process mutations from the same file.
ACTOR static Future<Void> handleSendMutationVectorRequest(RestoreSendVersionedMutationsRequest req,
                                                          Reference<RestoreApplierData> self) {
	state Reference<ApplierBatchData> batchData; // initialized as nullptr
	state bool printTrace = false;
	state NotifiedVersion* curMsgIndex = nullptr;

	if (req.batchIndex <= self->finishedBatch.get()) { // Handle duplicate request from batchIndex that has finished
		TraceEvent(SevWarn, "FastRestoreApplierRestoreSendVersionedMutationsRequestTooLate")
		    .detail("RequestBatchIndex", req.batchIndex)
		    .detail("FinishedBatchIndex", self->finishedBatch.get());
		req.reply.send(RestoreCommonReply(self->id(), true));
		ASSERT_WE_THINK(false); // Test to see if simulation can reproduce this
		return Void();
	}

	batchData = self->batch[req.batchIndex];

	ASSERT(batchData.isValid());
	ASSERT(self->finishedBatch.get() < req.batchIndex);
	// wait(delay(0.0, TaskPriority::RestoreApplierReceiveMutations)); // This hurts performance from 100MB/s to 60MB/s
	// on circus

	batchData->receiveMutationReqs += 1;
	// Trace when the receive phase starts at a VB and when it finishes.
	// This can help check if receiveMutations block applyMutation phase.
	// If so, we need more sophisticated scheduler to ensure priority execution
	printTrace = (batchData->receiveMutationReqs % SERVER_KNOBS->FASTRESTORE_NUM_TRACE_EVENTS == 0);
	TraceEvent(printTrace ? SevInfo : SevFRDebugInfo, "FastRestoreApplierPhaseReceiveMutations", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("RestoreAsset", req.asset.toString())
	    .detail("RestoreAssetMesssageIndex", batchData->processedFileState[req.asset].get())
	    .detail("Request", req.toString())
	    .detail("CurrentMemory", getSystemStatistics().processMemory)
	    .detail("PreviousVersionBatchState", batchData->vbState.get())
	    .detail("ReceiveMutationRequests", batchData->receiveMutationReqs);

	wait(isSchedulable(self, req.batchIndex, __FUNCTION__));

	ASSERT(batchData.isValid());
	ASSERT(req.batchIndex > self->finishedBatch.get());
	// Assume: processedFileState[req.asset] will not be erased while the actor is active.
	// Note: Insert new items into processedFileState will not invalidate the reference.
	curMsgIndex = &batchData->processedFileState[req.asset];
	wait(curMsgIndex->whenAtLeast(req.msgIndex - 1));
	batchData->vbState = ApplierVersionBatchState::RECEIVE_MUTATIONS;

	state bool isDuplicated = true;
	if (curMsgIndex->get() == req.msgIndex - 1) {
		isDuplicated = false;

		for (int mIndex = 0; mIndex < req.versionedMutations.size(); mIndex++) {
			const VersionedMutation& versionedMutation = req.versionedMutations[mIndex];
			TraceEvent(SevFRDebugInfo, "FastRestoreApplierPhaseReceiveMutations", self->id())
			    .detail("RestoreAsset", req.asset.toString())
			    .detail("Version", versionedMutation.version.toString())
			    .detail("Index", mIndex)
			    .detail("MutationReceived", versionedMutation.mutation.toString());
			batchData->receivedBytes += versionedMutation.mutation.totalSize();
			batchData->counters.receivedBytes += versionedMutation.mutation.totalSize();
			batchData->counters.receivedWeightedBytes +=
			    versionedMutation.mutation.weightedTotalSize(); // atomicOp will be amplified
			batchData->counters.receivedMutations += 1;
			batchData->counters.receivedAtomicOps +=
			    isAtomicOp((MutationRef::Type)versionedMutation.mutation.type) ? 1 : 0;
			// Sanity check
			ASSERT_WE_THINK(req.asset.isInVersionRange(versionedMutation.version.version));
			ASSERT_WE_THINK(req.asset.isInKeyRange(
			    versionedMutation.mutation)); // mutation is already applied removePrefix and addPrefix

			// Note: Log and range mutations may be delivered out of order. Can we handle it?
			batchData->addMutation(versionedMutation.mutation, versionedMutation.version);

			ASSERT(versionedMutation.mutation.type != MutationRef::SetVersionstampedKey &&
			       versionedMutation.mutation.type != MutationRef::SetVersionstampedValue);
		}
		curMsgIndex->set(req.msgIndex);
	}

	req.reply.send(RestoreCommonReply(self->id(), isDuplicated));
	TraceEvent(printTrace ? SevInfo : SevFRDebugInfo, "FastRestoreApplierPhaseReceiveMutationsDone", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("RestoreAsset", req.asset.toString())
	    .detail("ProcessedMessageIndex", curMsgIndex->get())
	    .detail("Request", req.toString());
	return Void();
}

// Clear all ranges in input ranges
ACTOR static Future<Void> applyClearRangeMutations(Standalone<VectorRef<KeyRangeRef>> ranges,
                                                   double delayTime,
                                                   Database cx,
                                                   UID applierID,
                                                   int batchIndex,
                                                   ApplierBatchData::Counters* cc) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state int retries = 0;
	state double numOps = 0;
	wait(delay(delayTime + deterministicRandom()->random01() * delayTime));
	TraceEvent(delayTime > 5 ? SevWarnAlways : SevDebug, "FastRestoreApplierClearRangeMutationsStart", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("Ranges", ranges.size())
	    .detail("DelayTime", delayTime);
	if (SERVER_KNOBS->FASTRESTORE_NOT_WRITE_DB) {
		TraceEvent("FastRestoreApplierClearRangeMutationsNotWriteDB", applierID)
		    .detail("BatchIndex", batchIndex)
		    .detail("Ranges", ranges.size());
		ASSERT(!g_network->isSimulated());
		return Void();
	}

	loop {
		try {
			// TODO: Consider clearrange traffic in write traffic control
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			for (auto& range : ranges) {
				debugFRMutation("FastRestoreApplierApplyClearRangeMutation",
				                0,
				                MutationRef(MutationRef::ClearRange, range.begin, range.end));
				tr->clear(range);
				cc->clearOps += 1;
				++numOps;
				if (numOps >= SERVER_KNOBS->FASTRESTORE_TXN_CLEAR_MAX) {
					TraceEvent(SevWarn, "FastRestoreApplierClearRangeMutationsTooManyClearsInTxn")
					    .suppressFor(5.0)
					    .detail("Clears", numOps)
					    .detail("Ranges", ranges.size())
					    .detail("Range", range.toString());
				}
			}
			wait(tr->commit());
			cc->clearTxns += 1;
			break;
		} catch (Error& e) {
			retries++;
			if (retries > SERVER_KNOBS->FASTRESTORE_TXN_RETRY_MAX) {
				TraceEvent(SevWarnAlways, "RestoreApplierApplyClearRangeMutationsStuck", applierID)
				    .error(e)
				    .detail("BatchIndex", batchIndex)
				    .detail("ClearRanges", ranges.size());
			}
			wait(tr->onError(e));
		}
	}
	return Void();
}

// Get keys in incompleteStagingKeys and precompute the stagingKey which is stored in batchData->stagingKeys
ACTOR static Future<Void> getAndComputeStagingKeys(
    std::map<Key, std::map<Key, StagingKey>::iterator> incompleteStagingKeys,
    double delayTime,
    Database cx,
    UID applierID,
    int batchIndex,
    ApplierBatchData::Counters* cc) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state std::vector<Future<Optional<Value>>> fValues(incompleteStagingKeys.size(), Never());
	state int retries = 0;
	state UID randomID = deterministicRandom()->randomUniqueID();

	wait(delay(delayTime + deterministicRandom()->random01() * delayTime));

	if (SERVER_KNOBS->FASTRESTORE_NOT_WRITE_DB) { // Get dummy value to short-circut DB
		TraceEvent("FastRestoreApplierGetAndComputeStagingKeysStartNotUseDB", applierID)
		    .detail("RandomUID", randomID)
		    .detail("BatchIndex", batchIndex)
		    .detail("GetKeys", incompleteStagingKeys.size())
		    .detail("DelayTime", delayTime);
		ASSERT(!g_network->isSimulated());
		int i = 0;
		for (auto& key : incompleteStagingKeys) {
			MutationRef m(MutationRef::SetValue, key.first, LiteralStringRef("0"));
			key.second->second.add(m, LogMessageVersion(1));
			key.second->second.precomputeResult("GetAndComputeStagingKeys", applierID, batchIndex);
			i++;
		}
		return Void();
	}

	TraceEvent("FastRestoreApplierGetAndComputeStagingKeysStart", applierID)
	    .detail("RandomUID", randomID)
	    .detail("BatchIndex", batchIndex)
	    .detail("GetKeys", incompleteStagingKeys.size())
	    .detail("DelayTime", delayTime);

	loop {
		try {
			int i = 0;
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			for (auto& key : incompleteStagingKeys) {
				fValues[i++] = tr->get(key.first);
				cc->fetchKeys += 1;
			}
			wait(waitForAll(fValues));
			cc->fetchTxns += 1;
			break;
		} catch (Error& e) {
			cc->fetchTxnRetries += 1;
			if (retries++ > incompleteStagingKeys.size()) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent(SevWarnAlways, "GetAndComputeStagingKeys", applierID)
					    .errorUnsuppressed(e)
					    .suppressFor(1.0)
					    .detail("RandomUID", randomID)
					    .detail("BatchIndex", batchIndex);
				}
			}
			wait(tr->onError(e));
		}
	}

	ASSERT(fValues.size() == incompleteStagingKeys.size());
	int i = 0;
	for (auto& key : incompleteStagingKeys) {
		if (!fValues[i].get().present()) { // Key not exist in DB
			// if condition: fValues[i].Valid() && fValues[i].isReady() && !fValues[i].isError() &&
			TraceEvent(SevDebug, "FastRestoreApplierGetAndComputeStagingKeysNoBaseValueInDB", applierID)
			    .suppressFor(5.0)
			    .detail("BatchIndex", batchIndex)
			    .detail("Key", key.first)
			    .detail("IsReady", fValues[i].isReady())
			    .detail("PendingMutations", key.second->second.pendingMutations.size())
			    .detail("StagingKeyType", getTypeString(key.second->second.type));
			for (auto& vm : key.second->second.pendingMutations) {
				TraceEvent(SevDebug, "FastRestoreApplierGetAndComputeStagingKeysNoBaseValueInDB")
				    .detail("PendingMutationVersion", vm.first.toString())
				    .detail("PendingMutation", vm.second.toString());
			}
			key.second->second.precomputeResult("GetAndComputeStagingKeysNoBaseValueInDB", applierID, batchIndex);
		} else {
			// The key's version ideally should be the most recently committed version.
			// But as long as it is > 1 and less than the start version of the version batch, it is the same result.
			MutationRef m(MutationRef::SetValue, key.first, fValues[i].get().get());
			key.second->second.add(m, LogMessageVersion(1));
			key.second->second.precomputeResult("GetAndComputeStagingKeys", applierID, batchIndex);
		}
		i++;
	}

	TraceEvent("FastRestoreApplierGetAndComputeStagingKeysDone", applierID)
	    .detail("RandomUID", randomID)
	    .detail("BatchIndex", batchIndex)
	    .detail("GetKeys", incompleteStagingKeys.size())
	    .detail("DelayTime", delayTime);

	return Void();
}

ACTOR static Future<Void> precomputeMutationsResult(Reference<ApplierBatchData> batchData,
                                                    UID applierID,
                                                    int64_t batchIndex,
                                                    Database cx) {
	// Apply range mutations (i.e., clearRange) to database cx
	TraceEvent("FastRestoreApplerPhasePrecomputeMutationsResultStart", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("Step", "Applying clear range mutations to DB")
	    .detail("ClearRanges", batchData->stagingKeyRanges.size());
	state std::vector<Future<Void>> fClearRanges;
	Standalone<VectorRef<KeyRangeRef>> clearRanges;
	double curTxnSize = 0;
	{
		double delayTime = 0;
		for (auto& rangeMutation : batchData->stagingKeyRanges) {
			KeyRangeRef range(rangeMutation.mutation.param1, rangeMutation.mutation.param2);
			debugFRMutation("FastRestoreApplierPrecomputeMutationsResultClearRange",
			                rangeMutation.version.version,
			                MutationRef(MutationRef::ClearRange, range.begin, range.end));
			clearRanges.push_back_deep(clearRanges.arena(), range);
			curTxnSize += range.expectedSize();
			if (curTxnSize >= SERVER_KNOBS->FASTRESTORE_TXN_BATCH_MAX_BYTES) {
				fClearRanges.push_back(
				    applyClearRangeMutations(clearRanges, delayTime, cx, applierID, batchIndex, &batchData->counters));
				delayTime += SERVER_KNOBS->FASTRESTORE_TXN_EXTRA_DELAY;
				clearRanges = Standalone<VectorRef<KeyRangeRef>>();
				curTxnSize = 0;
			}
		}
		if (curTxnSize > 0) {
			fClearRanges.push_back(
			    applyClearRangeMutations(clearRanges, delayTime, cx, applierID, batchIndex, &batchData->counters));
		}
	}

	// Apply range mutations (i.e., clearRange) to stagingKeyRanges
	TraceEvent("FastRestoreApplerPhasePrecomputeMutationsResult", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("Step", "Applying clear range mutations to staging keys")
	    .detail("ClearRanges", batchData->stagingKeyRanges.size())
	    .detail("FutureClearRanges", fClearRanges.size());
	for (auto& rangeMutation : batchData->stagingKeyRanges) {
		ASSERT(rangeMutation.mutation.param1 <= rangeMutation.mutation.param2);
		std::map<Key, StagingKey>::iterator lb = batchData->stagingKeys.lower_bound(rangeMutation.mutation.param1);
		std::map<Key, StagingKey>::iterator ub = batchData->stagingKeys.lower_bound(rangeMutation.mutation.param2);
		while (lb != ub) {
			if (lb->first >= rangeMutation.mutation.param2) {
				TraceEvent(SevError, "FastRestoreApplerPhasePrecomputeMutationsResultIncorrectUpperBound")
				    .detail("Key", lb->first)
				    .detail("ClearRangeUpperBound", rangeMutation.mutation.param2)
				    .detail("UsedUpperBound", ub->first);
			}
			// We make the beginKey = endKey for the ClearRange on purpose so that
			// we can sanity check ClearRange mutation when we apply it to DB.
			MutationRef clearKey(MutationRef::ClearRange, lb->first, lb->first);
			lb->second.add(clearKey, rangeMutation.version);
			lb++;
		}
	}
	TraceEvent("FastRestoreApplerPhasePrecomputeMutationsResult", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("Step", "Wait on applying clear range mutations to DB")
	    .detail("FutureClearRanges", fClearRanges.size());

	wait(waitForAll(fClearRanges));
	TraceEvent("FastRestoreApplerPhasePrecomputeMutationsResult", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("Step", "Getting and computing staging keys")
	    .detail("StagingKeys", batchData->stagingKeys.size());

	// Get keys in stagingKeys which does not have a baseline key by reading database cx, and precompute the key's value
	std::vector<Future<Void>> fGetAndComputeKeys;
	std::map<Key, std::map<Key, StagingKey>::iterator> incompleteStagingKeys;
	std::map<Key, StagingKey>::iterator stagingKeyIter = batchData->stagingKeys.begin();
	int numKeysInBatch = 0;
	int numGetTxns = 0;
	{
		double delayTime = 0; // Start transactions at different time to avoid overwhelming FDB.
		for (; stagingKeyIter != batchData->stagingKeys.end(); stagingKeyIter++) {
			if (!stagingKeyIter->second.hasBaseValue()) {
				incompleteStagingKeys.emplace(stagingKeyIter->first, stagingKeyIter);
				numKeysInBatch++;
			}
			if (numKeysInBatch == SERVER_KNOBS->FASTRESTORE_APPLIER_FETCH_KEYS_SIZE) {
				fGetAndComputeKeys.push_back(getAndComputeStagingKeys(
				    incompleteStagingKeys, delayTime, cx, applierID, batchIndex, &batchData->counters));
				numGetTxns++;
				delayTime += SERVER_KNOBS->FASTRESTORE_TXN_EXTRA_DELAY;
				numKeysInBatch = 0;
				incompleteStagingKeys.clear();
			}
		}
		if (numKeysInBatch > 0) {
			numGetTxns++;
			fGetAndComputeKeys.push_back(getAndComputeStagingKeys(
			    incompleteStagingKeys, delayTime, cx, applierID, batchIndex, &batchData->counters));
		}
	}

	TraceEvent("FastRestoreApplerPhasePrecomputeMutationsResult", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("Step", "Compute the other staging keys")
	    .detail("StagingKeys", batchData->stagingKeys.size())
	    .detail("GetStagingKeyBatchTxns", numGetTxns);
	// Pre-compute pendingMutations to other keys in stagingKeys that has base value
	for (stagingKeyIter = batchData->stagingKeys.begin(); stagingKeyIter != batchData->stagingKeys.end();
	     stagingKeyIter++) {
		if (stagingKeyIter->second.hasBaseValue()) {
			stagingKeyIter->second.precomputeResult("HasBaseValue", applierID, batchIndex);
		}
	}

	TraceEvent("FastRestoreApplierGetAndComputeStagingKeysWaitOn", applierID).log();
	wait(waitForAll(fGetAndComputeKeys));

	// Sanity check all stagingKeys have been precomputed
	ASSERT_WE_THINK(batchData->allKeysPrecomputed());

	TraceEvent("FastRestoreApplerPhasePrecomputeMutationsResultDone", applierID).detail("BatchIndex", batchIndex);

	return Void();
}

bool okToReleaseTxns(double targetMB, double applyingDataBytes) {
	return applyingDataBytes < targetMB * 1024 * 1024;
}

ACTOR static Future<Void> shouldReleaseTransaction(double* targetMB,
                                                   double* applyingDataBytes,
                                                   AsyncTrigger* releaseTxns) {
	loop {
		if (okToReleaseTxns(*targetMB, *applyingDataBytes)) {
			break;
		} else {
			wait(releaseTxns->onTrigger());
			wait(delay(0.0)); // Avoid all waiting txns are triggered at the same time and all decide to proceed before
			                  // applyingDataBytes has a chance to update
		}
	}
	return Void();
}

// Apply mutations in batchData->stagingKeys [begin, end).
ACTOR static Future<Void> applyStagingKeysBatch(std::map<Key, StagingKey>::iterator begin,
                                                std::map<Key, StagingKey>::iterator end,
                                                Database cx,
                                                UID applierID,
                                                ApplierBatchData::Counters* cc,
                                                double* appliedBytes,
                                                double* applyingDataBytes,
                                                double* targetMB,
                                                AsyncTrigger* releaseTxnTrigger) {
	if (SERVER_KNOBS->FASTRESTORE_NOT_WRITE_DB) {
		TraceEvent("FastRestoreApplierPhaseApplyStagingKeysBatchSkipped", applierID).detail("Begin", begin->first);
		ASSERT(!g_network->isSimulated());
		return Void();
	}
	wait(shouldReleaseTransaction(targetMB, applyingDataBytes, releaseTxnTrigger));

	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state int sets = 0;
	state int clears = 0;
	state Key endKey = begin->first;
	state double txnSize = 0;
	state double txnSizeUsed = 0; // txn size accounted in applyingDataBytes
	TraceEvent(SevFRDebugInfo, "FastRestoreApplierPhaseApplyStagingKeysBatch", applierID).detail("Begin", begin->first);
	loop {
		try {
			txnSize = 0;
			txnSizeUsed = 0;
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			std::map<Key, StagingKey>::iterator iter = begin;
			while (iter != end) {
				if (iter->second.type == MutationRef::SetValue) {
					tr->set(iter->second.key, iter->second.val);
					txnSize += iter->second.totalSize();
					cc->appliedMutations += 1;
					TraceEvent(SevFRMutationInfo, "FastRestoreApplierPhaseApplyStagingKeysBatch", applierID)
					    .detail("SetKey", iter->second.key);
					sets++;
				} else if (iter->second.type == MutationRef::ClearRange) {
					if (iter->second.key != iter->second.val) {
						TraceEvent(SevError, "FastRestoreApplierPhaseApplyStagingKeysBatchClearTooMuchData", applierID)
						    .detail("KeyBegin", iter->second.key)
						    .detail("KeyEnd", iter->second.val)
						    .detail("Version", iter->second.version.version)
						    .detail("SubVersion", iter->second.version.sub);
					}
					tr->clear(singleKeyRange(iter->second.key));
					txnSize += iter->second.totalSize();
					cc->appliedMutations += 1;
					TraceEvent(SevFRMutationInfo, "FastRestoreApplierPhaseApplyStagingKeysBatch", applierID)
					    .detail("ClearKey", iter->second.key);
					clears++;
				} else {
					ASSERT(false);
				}
				endKey = iter != end ? iter->first : endKey;
				iter++;
				if (sets > 10000000 || clears > 10000000) {
					TraceEvent(SevError, "FastRestoreApplierPhaseApplyStagingKeysBatchInfiniteLoop", applierID)
					    .detail("Begin", begin->first)
					    .detail("Sets", sets)
					    .detail("Clears", clears);
				}
			}
			TraceEvent(SevFRDebugInfo, "FastRestoreApplierPhaseApplyStagingKeysBatchPrecommit", applierID)
			    .detail("Begin", begin->first)
			    .detail("End", endKey)
			    .detail("Sets", sets)
			    .detail("Clears", clears);
			tr->addWriteConflictRange(KeyRangeRef(begin->first, keyAfter(endKey))); // Reduce resolver load
			txnSizeUsed = txnSize;
			*applyingDataBytes += txnSizeUsed; // Must account for applying bytes before wait for write traffic control
			wait(tr->commit());
			cc->appliedTxns += 1;
			cc->appliedBytes += txnSize;
			*appliedBytes += txnSize;
			*applyingDataBytes -= txnSizeUsed;
			if (okToReleaseTxns(*targetMB, *applyingDataBytes)) {
				releaseTxnTrigger->trigger();
			}
			break;
		} catch (Error& e) {
			cc->appliedTxnRetries += 1;
			wait(tr->onError(e));
			*applyingDataBytes -= txnSizeUsed;
		}
	}
	return Void();
}

// Apply mutations in stagingKeys in batches in parallel
ACTOR static Future<Void> applyStagingKeys(Reference<ApplierBatchData> batchData,
                                           UID applierID,
                                           int64_t batchIndex,
                                           Database cx) {
	std::map<Key, StagingKey>::iterator begin = batchData->stagingKeys.begin();
	std::map<Key, StagingKey>::iterator cur = begin;
	state int txnBatches = 0;
	double txnSize = 0;
	std::vector<Future<Void>> fBatches;
	TraceEvent("FastRestoreApplerPhaseApplyStagingKeysStart", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("StagingKeys", batchData->stagingKeys.size());
	batchData->totalBytesToWrite = 0;
	while (cur != batchData->stagingKeys.end()) {
		txnSize += cur->second.totalSize(); // should be consistent with receivedBytes accounting method
		if (txnSize > SERVER_KNOBS->FASTRESTORE_TXN_BATCH_MAX_BYTES) {
			fBatches.push_back(applyStagingKeysBatch(begin,
			                                         cur,
			                                         cx,
			                                         applierID,
			                                         &batchData->counters,
			                                         &batchData->appliedBytes,
			                                         &batchData->applyingDataBytes,
			                                         &batchData->targetWriteRateMB,
			                                         &batchData->releaseTxnTrigger));
			batchData->totalBytesToWrite += txnSize;
			begin = cur;
			txnSize = 0;
			txnBatches++;
		}
		cur++;
	}
	if (begin != batchData->stagingKeys.end()) {
		fBatches.push_back(applyStagingKeysBatch(begin,
		                                         cur,
		                                         cx,
		                                         applierID,
		                                         &batchData->counters,
		                                         &batchData->appliedBytes,
		                                         &batchData->applyingDataBytes,
		                                         &batchData->targetWriteRateMB,
		                                         &batchData->releaseTxnTrigger));
		batchData->totalBytesToWrite += txnSize;
		txnBatches++;
	}

	wait(waitForAll(fBatches));

	TraceEvent("FastRestoreApplerPhaseApplyStagingKeysDone", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("StagingKeys", batchData->stagingKeys.size())
	    .detail("TransactionBatches", txnBatches)
	    .detail("TotalBytesToWrite", batchData->totalBytesToWrite);
	return Void();
}

// Write mutations to the destination DB
ACTOR Future<Void> writeMutationsToDB(UID applierID,
                                      int64_t batchIndex,
                                      Reference<ApplierBatchData> batchData,
                                      Database cx) {
	TraceEvent("FastRestoreApplierPhaseApplyTxnStart", applierID).detail("BatchIndex", batchIndex);
	wait(precomputeMutationsResult(batchData, applierID, batchIndex, cx));

	wait(applyStagingKeys(batchData, applierID, batchIndex, cx));
	TraceEvent("FastRestoreApplierPhaseApplyTxnDone", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("AppliedBytes", batchData->appliedBytes)
	    .detail("ReceivedBytes", batchData->receivedBytes);

	return Void();
}

void handleUpdateRateRequest(RestoreUpdateRateRequest req, Reference<RestoreApplierData> self) {
	TraceEvent ev("FastRestoreApplierUpdateRateRequest", self->id());
	ev.suppressFor(10)
	    .detail("BatchIndex", req.batchIndex)
	    .detail("FinishedBatch", self->finishedBatch.get())
	    .detail("WriteMB", req.writeMB);
	double remainingDataMB = 0;
	if (self->finishedBatch.get() == req.batchIndex - 1) { // current applying batch
		Reference<ApplierBatchData> batchData = self->batch[req.batchIndex];
		ASSERT(batchData.isValid());
		batchData->targetWriteRateMB = req.writeMB;
		remainingDataMB = batchData->totalBytesToWrite > 0
		                      ? std::max(0.0, batchData->totalBytesToWrite - batchData->appliedBytes) / 1024 / 1024
		                      : batchData->receivedBytes / 1024 / 1024;
		ev.detail("TotalBytesToWrite", batchData->totalBytesToWrite)
		    .detail("AppliedBytes", batchData->appliedBytes)
		    .detail("ReceivedBytes", batchData->receivedBytes)
		    .detail("TargetWriteRateMB", batchData->targetWriteRateMB)
		    .detail("RemainingDataMB", remainingDataMB);
	}
	req.reply.send(RestoreUpdateRateReply(self->id(), remainingDataMB));

	return;
}

ACTOR static Future<Void> traceRate(const char* context,
                                    Reference<ApplierBatchData> batchData,
                                    int batchIndex,
                                    UID nodeID,
                                    NotifiedVersion* finishedVB,
                                    bool once = false) {
	loop {
		if ((finishedVB->get() != batchIndex - 1) || !batchData.isValid()) {
			break;
		}
		TraceEvent(context, nodeID)
		    .suppressFor(10)
		    .detail("BatchIndex", batchIndex)
		    .detail("FinishedBatchIndex", finishedVB->get())
		    .detail("TotalDataToWriteMB", batchData->totalBytesToWrite / 1024 / 1024)
		    .detail("AppliedBytesMB", batchData->appliedBytes / 1024 / 1024)
		    .detail("TargetBytesMB", batchData->targetWriteRateMB)
		    .detail("InflightBytesMB", batchData->applyingDataBytes)
		    .detail("ReceivedBytes", batchData->receivedBytes);
		if (once) {
			break;
		}
		wait(delay(5.0));
	}

	return Void();
}

ACTOR static Future<Void> handleApplyToDBRequest(RestoreVersionBatchRequest req,
                                                 Reference<RestoreApplierData> self,
                                                 Database cx) {
	TraceEvent("FastRestoreApplierPhaseHandleApplyToDBStart", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("FinishedBatch", self->finishedBatch.get());

	// Ensure batch (i-1) is applied before batch i
	// TODO: Add a counter to warn when too many requests are waiting on the actor
	wait(self->finishedBatch.whenAtLeast(req.batchIndex - 1));

	state bool isDuplicated = true;
	if (self->finishedBatch.get() == req.batchIndex - 1) {
		// duplicate request from earlier version batch will be ignored
		state Reference<ApplierBatchData> batchData = self->batch[req.batchIndex];
		ASSERT(batchData.isValid());
		TraceEvent("FastRestoreApplierPhaseHandleApplyToDBRunning", self->id())
		    .detail("BatchIndex", req.batchIndex)
		    .detail("FinishedBatch", self->finishedBatch.get())
		    .detail("HasStarted", batchData->dbApplier.present())
		    .detail("WroteToDBDone", batchData->dbApplier.present() ? batchData->dbApplier.get().isReady() : 0)
		    .detail("PreviousVersionBatchState", batchData->vbState.get());

		ASSERT(batchData.isValid());
		if (!batchData->dbApplier.present()) {
			isDuplicated = false;
			batchData->dbApplier = Never();
			batchData->dbApplier = writeMutationsToDB(self->id(), req.batchIndex, batchData, cx);
			batchData->vbState = ApplierVersionBatchState::WRITE_TO_DB;
			batchData->rateTracer = traceRate("FastRestoreApplierTransactionRateControl",
			                                  batchData,
			                                  req.batchIndex,
			                                  self->id(),
			                                  &self->finishedBatch);
		}

		ASSERT(batchData->dbApplier.present());
		ASSERT(!batchData->dbApplier.get().isError()); // writeMutationsToDB actor cannot have error.
		                                               // We cannot blindly retry because it is not idempodent

		wait(batchData->dbApplier.get());

		// Multiple actors can wait on req.batchIndex-1;
		// Avoid setting finishedBatch when finishedBatch > req.batchIndex
		if (self->finishedBatch.get() == req.batchIndex - 1) {
			batchData->rateTracer = traceRate("FastRestoreApplierTransactionRateControlDone",
			                                  batchData,
			                                  req.batchIndex,
			                                  self->id(),
			                                  &self->finishedBatch,
			                                  true /*print once*/); // Track the last rate info
			self->finishedBatch.set(req.batchIndex);
			// self->batch[req.batchIndex]->vbState = ApplierVersionBatchState::DONE;
			// Free memory for the version batch
			self->batch.erase(req.batchIndex);
			if (self->delayedActors > 0) {
				self->checkMemory.trigger();
			}
		}
	}

	req.reply.send(RestoreCommonReply(self->id(), isDuplicated));

	TraceEvent("FastRestoreApplierPhaseHandleApplyToDBDone", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("FinishedBatch", self->finishedBatch.get())
	    .detail("IsDuplicated", isDuplicated);

	return Void();
}

// Copy from WriteDuringRead.actor.cpp with small modifications
// Not all AtomicOps are handled in this function: SetVersionstampedKey, SetVersionstampedValue, and CompareAndClear
Value applyAtomicOp(Optional<StringRef> existingValue, Value value, MutationRef::Type type) {
	Arena arena;
	if (type == MutationRef::AddValue)
		return doLittleEndianAdd(existingValue, value, arena);
	else if (type == MutationRef::AppendIfFits)
		return doAppendIfFits(existingValue, value, arena);
	else if (type == MutationRef::And || type == MutationRef::AndV2)
		return doAndV2(existingValue, value, arena);
	else if (type == MutationRef::Or)
		return doOr(existingValue, value, arena);
	else if (type == MutationRef::Xor)
		return doXor(existingValue, value, arena);
	else if (type == MutationRef::Max)
		return doMax(existingValue, value, arena);
	else if (type == MutationRef::Min || type == MutationRef::MinV2)
		return doMinV2(existingValue, value, arena);
	else if (type == MutationRef::ByteMin)
		return doByteMin(existingValue, value, arena);
	else if (type == MutationRef::ByteMax)
		return doByteMax(existingValue, value, arena);
	else {
		TraceEvent(SevError, "ApplyAtomicOpUnhandledType")
		    .detail("TypeCode", (int)type)
		    .detail("TypeName", getTypeString(type));
		ASSERT(false);
	}
	return Value();
}
