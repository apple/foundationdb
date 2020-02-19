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
ACTOR static Future<Void> handleSendMutationVectorRequestV2(RestoreSendVersionedMutationsRequest req,
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
					actors.add(handleSendMutationVectorRequestV2(req, self));
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
ACTOR static Future<Void> handleSendMutationVectorRequestV2(RestoreSendVersionedMutationsRequest req,
                                                            Reference<RestoreApplierData> self) {
	state Reference<ApplierBatchData> batchData = self->batch[req.batchIndex];
	// Assume: processedFileState[req.asset] will not be erased while the actor is active.
	// Note: Insert new items into processedFileState will not invalidate the reference.
	state NotifiedVersion& curFilePos = batchData->processedFileState[req.asset];

	TraceEvent(SevDebug, "FastRestoreApplierPhaseReceiveMutations", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("RestoreAsset", req.asset.toString())
	    .detail("ProcessedFileVersion", curFilePos.get())
	    .detail("Request", req.toString());

	wait(curFilePos.whenAtLeast(req.prevVersion));

	state bool isDuplicated = true;
	if (curFilePos.get() == req.prevVersion) {
		isDuplicated = false;
		Version commitVersion = req.version;
		uint16_t numVersionStampedKV = 0;
		MutationsVec mutations(req.mutations);
		// Sanity check: mutations in range file is in [beginVersion, endVersion);
		// mutations in log file is in [beginVersion, endVersion], both inclusive.
		ASSERT_WE_THINK(commitVersion >= req.asset.beginVersion);
		// Loader sends the endVersion to ensure all useful versions are sent
		ASSERT_WE_THINK((req.isRangeFile && commitVersion <= req.asset.endVersion) ||
		                (!req.isRangeFile && commitVersion <= req.asset.endVersion));

		for (int mIndex = 0; mIndex < mutations.size(); mIndex++) {
			MutationRef mutation = mutations[mIndex];
			TraceEvent(SevFRMutationInfo, "FastRestoreApplierPhaseReceiveMutations")
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
			// Note: Log and range mutations may be delivered out of order. Can we handle it?
			if (mutation.type == MutationRef::SetVersionstampedKey ||
			    mutation.type == MutationRef::SetVersionstampedValue) {
				batchData->addVersionStampedKV(mutation, commitVersion, numVersionStampedKV);
				numVersionStampedKV++;
			} else {
				batchData->addMutation(mutation, commitVersion);
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

// Get keys in imcompleteStagingKeys and precompute the stagingKey which is stored in batchData->stagingKeys
ACTOR static Future<Void> getAndComputeStagingKeys(
    std::map<Key, std::map<Key, StagingKey>::iterator> imcompleteStagingKeys, Database cx, UID applierID) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	// state std::vector<std::pair<Key, Future<Optional<Value>>>> fKVs;
	state std::vector<Future<Optional<Value>>> fValues;
	state std::vector<Optional<Value>> values;
	state int i = 0;
	TraceEvent("FastRestoreApplierGetAndComputeStagingKeysStart", applierID)
	    .detail("GetKeys", imcompleteStagingKeys.size());
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			for (auto& key : imcompleteStagingKeys) {
				// fKVs.push_back(std::make_pair(key.first, tr->get(key.first)));
				// fValues.push_back(fKVs.back().second);
				fValues.push_back(tr->get(key.first));
			}
			for (i = 0; i < fValues.size(); i++) {
				Optional<Value> val = wait(fValues[i]);
				values.push_back(val);
			}
			break;
		} catch (Error& e) {
			TraceEvent(SevError, "FastRestoreApplierGetAndComputeStagingKeysUnhandledError")
			    .detail("GetKeys", imcompleteStagingKeys.size())
			    .detail("Error", e.what())
			    .detail("ErrorCode", e.code());
			wait(tr->onError(e));
			fValues.clear();
		}
	}

	ASSERT(values.size() == imcompleteStagingKeys.size());
	// TODO: Optimize the performance by reducing map lookup: making getKey's future a field in the input map
	int i = 0;
	for (auto& key : imcompleteStagingKeys) {
		if (!values[i].present()) {
			TraceEvent(SevWarnAlways, "FastRestoreApplierGetAndComputeStagingKeysUnhandledError")
			    .detail("Key", key.first)
			    .detail("Reason", "Not found in DB")
			    .detail("PendingMutations", key.second->second.pendingMutations.size())
			    .detail("StagingKeyType", (int)key.second->second.type);
			for (auto& vm : key.second->second.pendingMutations) {
				for (auto& m : vm.second) {
					TraceEvent(SevWarnAlways, "FastRestoreApplierGetAndComputeStagingKeysUnhandledError")
					    .detail("PendingMutationVersion", vm.first)
					    .detail("PendingMutation", m.toString());
				}
			}
			key.second->second.precomputeResult();
			i++;
			continue;
		} else {
			// The key's version ideally should be the most recently committed version.
			// But as long as it is > 1 and less than the start version of the version batch, it is the same result.
			MutationRef m(MutationRef::SetValue, key.first, fValues[i].get().get());
			key.second->second.add(m, (Version)1);
			key.second->second.precomputeResult();
			i++;
		}
	}

	TraceEvent("FastRestoreApplierGetAndComputeStagingKeysDone", applierID)
	    .detail("GetKeys", imcompleteStagingKeys.size());

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
	std::map<Key, std::map<Key, StagingKey>::iterator> imcompleteStagingKeys;
	std::map<Key, StagingKey>::iterator stagingKeyIter = batchData->stagingKeys.begin();
	for (; stagingKeyIter != batchData->stagingKeys.end(); stagingKeyIter++) {
		if (!stagingKeyIter->second.hasBaseValue()) {
			imcompleteStagingKeys.emplace(stagingKeyIter->first, stagingKeyIter);
		}
	}

	Future<Void> fGetAndComputeKeys = getAndComputeStagingKeys(imcompleteStagingKeys, cx, applierID);

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
	wait(fGetAndComputeKeys);

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

ACTOR Future<Void> applyToDBV2(UID applierID, int64_t batchIndex, Reference<ApplierBatchData> batchData, Database cx) {
	TraceEvent("FastRestoreApplerPhaseApplyTxn", applierID).detail("BatchIndex", batchIndex);
	wait(precomputeMutationsResult(batchData, applierID, batchIndex, cx));

	wait(applyStagingKeys(batchData, applierID, batchIndex, cx));
	TraceEvent("FastRestoreApplerPhaseApplyTxnDone", applierID).detail("BatchIndex", batchIndex);

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
			TraceEvent(SevFRMutationInfo, "FastRestoreApplierPhaseReceiveMutations")
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
			// batchData->dbApplier = applyToDB(self->id(), req.batchIndex, batchData, cx);
			batchData->dbApplier = applyToDBV2(self->id(), req.batchIndex, batchData, cx);
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
