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
#include "fdbserver/Knobs.h"
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
	actors.add(traceRoleVersionBatchProgress(self, "Applier"));

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
// No race condition as long as we do not wait or yield when operate the shared
// data. Multiple such actors can run on different fileIDs.
// Different files may contain mutations of the same commit versions, but with
// different subsequence number.
// Only one actor can process mutations from the same file.
ACTOR static Future<Void> handleSendMutationVectorRequest(RestoreSendVersionedMutationsRequest req,
                                                          Reference<RestoreApplierData> self) {
	state Reference<ApplierBatchData> batchData = self->batch[req.batchIndex];
	// Assume: processedFileState[req.asset] will not be erased while the actor is active.
	// Note: Insert new items into processedFileState will not invalidate the reference.
	state NotifiedVersion& curFilePos = batchData->processedFileState[req.asset];

	TraceEvent(SevDebug, "FastRestoreApplierPhaseReceiveMutations", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("RestoreAsset", req.asset.toString())
	    .detail("ProcessedFileVersion", curFilePos.get())
	    .detail("Request", req.toString())
	    .detail("CurrentMemory", getSystemStatistics().processMemory)
	    .detail("PreviousVersionBatchState", batchData->vbState.get());

	wait(isSchedulable(self, req.batchIndex, __FUNCTION__));

	wait(curFilePos.whenAtLeast(req.prevVersion));
	batchData->vbState = ApplierVersionBatchState::RECEIVE_MUTATIONS;

	state bool isDuplicated = true;
	if (curFilePos.get() == req.prevVersion) {
		isDuplicated = false;
		const Version commitVersion = req.version;
		uint16_t numVersionStampedKV = 0;
		// Sanity check: mutations in range file is in [beginVersion, endVersion);
		// mutations in log file is in [beginVersion, endVersion], both inclusive.
		ASSERT(commitVersion >= req.asset.beginVersion);
		// Loader sends the endVersion to ensure all useful versions are sent
		ASSERT(commitVersion <= req.asset.endVersion);
		ASSERT(req.mutations.size() == req.subs.size());

		for (int mIndex = 0; mIndex < req.mutations.size(); mIndex++) {
			const MutationRef& mutation = req.mutations[mIndex];
			const LogMessageVersion mutationVersion(commitVersion, req.subs[mIndex]);
			TraceEvent(SevFRMutationInfo, "FastRestoreApplierPhaseReceiveMutations", self->id())
			    .detail("RestoreAsset", req.asset.toString())
			    .detail("Version", mutationVersion.toString())
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
			// Note: Log and range mutations may be delivered out of order. Can we handle it?
			if (mutation.type == MutationRef::SetVersionstampedKey ||
			    mutation.type == MutationRef::SetVersionstampedValue) {
				ASSERT(false); // No version stamp mutations in backup logs
				batchData->addVersionStampedKV(mutation, mutationVersion, numVersionStampedKV);
				numVersionStampedKV++;
			} else {
				batchData->addMutation(mutation, mutationVersion);
			}
		}
		curFilePos.set(req.version);
	}

	req.reply.send(RestoreCommonReply(self->id(), isDuplicated));
	TraceEvent(SevDebug, "FastRestoreApplierPhaseReceiveMutationsDone", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("RestoreAsset", req.asset.toString())
	    .detail("ProcessedFileVersion", curFilePos.get())
	    .detail("Request", req.toString());
	return Void();
}

// Clear all ranges in input ranges
ACTOR static Future<Void> applyClearRangeMutations(Standalone<VectorRef<KeyRangeRef>> ranges, Database cx) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			for (auto& range : ranges) {
				tr->clear(range);
			}
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
	return Void();
}

// Get keys in incompleteStagingKeys and precompute the stagingKey which is stored in batchData->stagingKeys
ACTOR static Future<Void> getAndComputeStagingKeys(
    std::map<Key, std::map<Key, StagingKey>::iterator> incompleteStagingKeys, Database cx, UID applierID) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state std::vector<Future<Optional<Value>>> fValues;
	state int retries = 0;

	TraceEvent("FastRestoreApplierGetAndComputeStagingKeysStart", applierID)
	    .detail("GetKeys", incompleteStagingKeys.size());
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			for (auto& key : incompleteStagingKeys) {
				fValues.push_back(tr->get(key.first));
			}
			wait(waitForAll(fValues));
			break;
		} catch (Error& e) {
			retries++;
			TraceEvent(retries > 10 ? SevError : SevWarn, "FastRestoreApplierGetAndComputeStagingKeysUnhandledError")
			    .detail("GetKeys", incompleteStagingKeys.size())
			    .error(e);
			wait(tr->onError(e));
			fValues.clear();
		}
	}

	ASSERT(fValues.size() == incompleteStagingKeys.size());
	int i = 0;
	for (auto& key : incompleteStagingKeys) {
		if (!fValues[i].get().present()) {
			TraceEvent(SevWarnAlways, "FastRestoreApplierGetAndComputeStagingKeysUnhandledError")
			    .detail("Key", key.first)
			    .detail("Reason", "Not found in DB")
			    .detail("PendingMutations", key.second->second.pendingMutations.size())
			    .detail("StagingKeyType", (int)key.second->second.type);
			for (auto& vm : key.second->second.pendingMutations) {
				TraceEvent(SevWarnAlways, "FastRestoreApplierGetAndComputeStagingKeysUnhandledError")
				    .detail("PendingMutationVersion", vm.first.toString())
				    .detail("PendingMutation", vm.second.toString());
			}
			key.second->second.precomputeResult();
			i++;
			continue;
		} else {
			// The key's version ideally should be the most recently committed version.
			// But as long as it is > 1 and less than the start version of the version batch, it is the same result.
			MutationRef m(MutationRef::SetValue, key.first, fValues[i].get().get());
			key.second->second.add(m, LogMessageVersion(1));
			key.second->second.precomputeResult();
			i++;
		}
	}

	TraceEvent("FastRestoreApplierGetAndComputeStagingKeysDone", applierID)
	    .detail("GetKeys", incompleteStagingKeys.size());

	return Void();
}

ACTOR static Future<Void> precomputeMutationsResult(Reference<ApplierBatchData> batchData, UID applierID,
                                                    int64_t batchIndex, Database cx) {
	// Apply range mutations (i.e., clearRange) to database cx
	TraceEvent("FastRestoreApplerPhasePrecomputeMutationsResult", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("Step", "Applying clear range mutations to DB")
	    .detail("ClearRanges", batchData->stagingKeyRanges.size());
	state std::vector<Future<Void>> fClearRanges;
	std::vector<Standalone<VectorRef<KeyRangeRef>>> clearBuf;
	clearBuf.push_back(Standalone<VectorRef<KeyRangeRef>>());
	Standalone<VectorRef<KeyRangeRef>> clearRanges = clearBuf.back();
	double curTxnSize = 0;
	for (auto& rangeMutation : batchData->stagingKeyRanges) {
		KeyRangeRef range(rangeMutation.mutation.param1, rangeMutation.mutation.param2);
		clearRanges.push_back(clearRanges.arena(), range);
		curTxnSize += range.expectedSize();
		if (curTxnSize >= SERVER_KNOBS->FASTRESTORE_TXN_BATCH_MAX_BYTES) {
			fClearRanges.push_back(applyClearRangeMutations(clearRanges, cx));
			clearBuf.push_back(Standalone<VectorRef<KeyRangeRef>>());
			clearRanges = clearBuf.back();
			curTxnSize = 0;
		}
	}
	if (curTxnSize > 0) {
		fClearRanges.push_back(applyClearRangeMutations(clearRanges, cx));
	}

	// Apply range mutations (i.e., clearRange) to stagingKeyRanges
	TraceEvent("FastRestoreApplerPhasePrecomputeMutationsResult", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("Step", "Applying clear range mutations to staging keys")
	    .detail("ClearRanges", batchData->stagingKeyRanges.size());
	for (auto& rangeMutation : batchData->stagingKeyRanges) {
		std::map<Key, StagingKey>::iterator lb = batchData->stagingKeys.lower_bound(rangeMutation.mutation.param1);
		std::map<Key, StagingKey>::iterator ub = batchData->stagingKeys.upper_bound(rangeMutation.mutation.param2);
		while (lb != ub) {
			lb->second.add(rangeMutation.mutation, rangeMutation.version);
			lb++;
		}
	}

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
	for (; stagingKeyIter != batchData->stagingKeys.end(); stagingKeyIter++) {
		if (!stagingKeyIter->second.hasBaseValue()) {
			incompleteStagingKeys.emplace(stagingKeyIter->first, stagingKeyIter);
			batchData->counters.fetchKeys += 1;
			numKeysInBatch++;
		}
		if (numKeysInBatch == SERVER_KNOBS->FASTRESTORE_APPLIER_FETCH_KEYS_SIZE) {
			fGetAndComputeKeys.push_back(getAndComputeStagingKeys(incompleteStagingKeys, cx, applierID));
			numKeysInBatch = 0;
			incompleteStagingKeys.clear();
		}
	}
	if (numKeysInBatch > 0) {
		fGetAndComputeKeys.push_back(getAndComputeStagingKeys(incompleteStagingKeys, cx, applierID));
	}

	TraceEvent("FastRestoreApplerPhasePrecomputeMutationsResult", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("Step", "Compute the other staging keys")
	    .detail("StagingKeys", batchData->stagingKeys.size());
	// Pre-compute pendingMutations to other keys in stagingKeys that has base value
	for (stagingKeyIter = batchData->stagingKeys.begin(); stagingKeyIter != batchData->stagingKeys.end();
	     stagingKeyIter++) {
		if (stagingKeyIter->second.hasBaseValue()) {
			stagingKeyIter->second.precomputeResult();
		}
	}

	TraceEvent("FastRestoreApplierGetAndComputeStagingKeysWaitOn", applierID);
	wait(waitForAll(fGetAndComputeKeys));

	// Sanity check all stagingKeys have been precomputed
	ASSERT_WE_THINK(batchData->allKeysPrecomputed());

	TraceEvent("FastRestoreApplerPhasePrecomputeMutationsResultDone", applierID).detail("BatchIndex", batchIndex);

	return Void();
}

// Apply mutations in batchData->stagingKeys [begin, end).
ACTOR static Future<Void> applyStagingKeysBatch(std::map<Key, StagingKey>::iterator begin,
                                                std::map<Key, StagingKey>::iterator end, Database cx,
                                                FlowLock* applyStagingKeysBatchLock, UID applierID) {
	wait(applyStagingKeysBatchLock->take(TaskPriority::RestoreApplierWriteDB));
	state FlowLock::Releaser releaser(*applyStagingKeysBatchLock);
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state int sets = 0;
	state int clears = 0;
	TraceEvent("FastRestoreApplierPhaseApplyStagingKeysBatch", applierID).detail("Begin", begin->first);
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			std::map<Key, StagingKey>::iterator iter = begin;
			while (iter != end) {
				if (iter->second.type == MutationRef::SetValue) {
					tr->set(iter->second.key, iter->second.val);
					sets++;
				} else if (iter->second.type == MutationRef::ClearRange) {
					tr->clear(KeyRangeRef(iter->second.key, iter->second.val));
					clears++;
				} else {
					ASSERT(false);
				}
				iter++;
				if (sets > 10000000 || clears > 10000000) {
					TraceEvent(SevError, "FastRestoreApplierPhaseApplyStagingKeysBatchInfiniteLoop", applierID)
					    .detail("Begin", begin->first)
					    .detail("Sets", sets)
					    .detail("Clears", clears);
				}
			}
			TraceEvent("FastRestoreApplierPhaseApplyStagingKeysBatchPrecommit", applierID)
			    .detail("Begin", begin->first)
			    .detail("Sets", sets)
			    .detail("Clears", clears);
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
	return Void();
}

// Apply mutations in stagingKeys in batches in parallel
ACTOR static Future<Void> applyStagingKeys(Reference<ApplierBatchData> batchData, UID applierID, int64_t batchIndex,
                                           Database cx) {
	std::map<Key, StagingKey>::iterator begin = batchData->stagingKeys.begin();
	std::map<Key, StagingKey>::iterator cur = begin;
	double txnSize = 0;
	std::vector<Future<Void>> fBatches;
	TraceEvent("FastRestoreApplerPhaseApplyStagingKeys", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("StagingKeys", batchData->stagingKeys.size());
	while (cur != batchData->stagingKeys.end()) {
		txnSize += cur->second.expectedMutationSize();
		if (txnSize > SERVER_KNOBS->FASTRESTORE_TXN_BATCH_MAX_BYTES) {
			fBatches.push_back(applyStagingKeysBatch(begin, cur, cx, &batchData->applyStagingKeysBatchLock, applierID));
			begin = cur;
			txnSize = 0;
		}
		cur++;
	}
	if (begin != batchData->stagingKeys.end()) {
		fBatches.push_back(applyStagingKeysBatch(begin, cur, cx, &batchData->applyStagingKeysBatchLock, applierID));
	}

	wait(waitForAll(fBatches));

	TraceEvent("FastRestoreApplerPhaseApplyStagingKeysDone", applierID)
	    .detail("BatchIndex", batchIndex)
	    .detail("StagingKeys", batchData->stagingKeys.size());
	return Void();
}

// Write mutations to the destination DB
ACTOR Future<Void> writeMutationsToDB(UID applierID, int64_t batchIndex, Reference<ApplierBatchData> batchData,
                                      Database cx) {
	TraceEvent("FastRestoreApplerPhaseApplyTxn", applierID).detail("BatchIndex", batchIndex);
	wait(precomputeMutationsResult(batchData, applierID, batchIndex, cx));

	wait(applyStagingKeys(batchData, applierID, batchIndex, cx));
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
	    .detail("HasStarted", batchData->dbApplier.present())
	    .detail("PreviousVersionBatchState", batchData->vbState.get());
	batchData->vbState = ApplierVersionBatchState::WRITE_TO_DB;
	if (self->finishedBatch.get() == req.batchIndex - 1) {
		ASSERT(batchData.isValid());
		if (!batchData->dbApplier.present()) {
			isDuplicated = false;
			batchData->dbApplier = Never();
			batchData->dbApplier = writeMutationsToDB(self->id(), req.batchIndex, batchData, cx);
		}

		ASSERT(batchData->dbApplier.present());

		wait(batchData->dbApplier.get());

		// Multiple actor invokation can wait on req.batchIndex-1;
		// Avoid setting finishedBatch when finishedBatch > req.batchIndex
		if (self->finishedBatch.get() == req.batchIndex - 1) {
			self->finishedBatch.set(req.batchIndex);
		}
	}

	if (self->delayedActors > 0) {
		self->checkMemory.trigger();
	}
	req.reply.send(RestoreCommonReply(self->id(), isDuplicated));

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
		    .detail("TypeName", typeString[type]);
		ASSERT(false);
	}
	return Value();
}
