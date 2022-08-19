/*
 * RestoreApplier.actor.h
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

// This file declears RestoreApplier interface and actors

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RESTORE_APPLIER_G_H)
#define FDBSERVER_RESTORE_APPLIER_G_H
#include "fdbserver/RestoreApplier.actor.g.h"
#elif !defined(FDBSERVER_RESTORE_APPLIER_H)
#define FDBSERVER_RESTORE_APPLIER_H

#include <sstream>
#include "fdbclient/Atomic.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreWorkerInterface.actor.h"

#include "flow/actorcompiler.h" // has to be last include

Value applyAtomicOp(Optional<StringRef> existingValue, Value value, MutationRef::Type type);

// Key whose mutations are buffered on applier.
// key, value, type and version defines the parsed mutation at version.
// pendingMutations has all versioned mutations to be applied.
// Mutations in pendingMutations whose version is below the version in StagingKey can be ignored in applying phase.
struct StagingKey {
	Key key; // TODO: Maybe not needed?
	Value val;
	MutationRef::Type type; // set or clear
	LogMessageVersion version; // largest version of set or clear for the key
	std::map<LogMessageVersion, Standalone<MutationRef>> pendingMutations; // mutations not set or clear type

	explicit StagingKey(Key key) : key(key), type(MutationRef::MAX_ATOMIC_OP), version(0) {}

	// Add mutation m at newVersion to stagingKey
	// Assume: SetVersionstampedKey and SetVersionstampedValue have been converted to set
	void add(const MutationRef& m, LogMessageVersion newVersion) {
		ASSERT(m.type != MutationRef::SetVersionstampedKey && m.type != MutationRef::SetVersionstampedValue);
		DEBUG_MUTATION("StagingKeyAdd", newVersion.version, m)
		    .detail("SubVersion", version.toString())
		    .detail("NewSubVersion", newVersion.toString());
		if (version == newVersion) {
			// This could happen because the same mutation can be present in
			// overlapping mutation logs, because new TLogs can copy mutations
			// from old generation TLogs (or backup worker is recruited without
			// knowning previously saved progress).
			ASSERT(type == m.type && key == m.param1 && val == m.param2);
			TraceEvent("SameVersion").detail("Version", version.toString()).detail("Mutation", m);
			return;
		}

		// newVersion can be smaller than version as different loaders can send
		// mutations out of order.
		if (m.type == MutationRef::SetValue || m.type == MutationRef::ClearRange) {
			if (m.type == MutationRef::ClearRange) {
				// We should only clear this key! Otherwise, it causes side effect to other keys
				ASSERT(m.param1 == m.param2);
			}
			if (version < newVersion) {
				DEBUG_MUTATION("StagingKeyAdd", newVersion.version, m)
				    .detail("SubVersion", version.toString())
				    .detail("NewSubVersion", newVersion.toString())
				    .detail("MType", getTypeString(type))
				    .detail("Key", key)
				    .detail("Val", val)
				    .detail("NewMutation", m.toString());
				key = m.param1;
				val = m.param2;
				type = (MutationRef::Type)m.type;
				version = newVersion;
			}
		} else {
			auto it = pendingMutations.find(newVersion);
			if (it == pendingMutations.end()) {
				pendingMutations.emplace(newVersion, m);
			} else {
				// Duplicated mutation ignored.
				// TODO: Add SevError here
				TraceEvent("SameVersion")
				    .detail("Version", version.toString())
				    .detail("NewVersion", newVersion.toString())
				    .detail("OldMutation", it->second)
				    .detail("NewMutation", m);
				ASSERT(it->second.type == m.type && it->second.param1 == m.param1 && it->second.param2 == m.param2);
			}
		}
	}

	// Precompute the final value of the key.
	// TODO: Look at the last LogMessageVersion, if it set or clear, we can ignore the rest of versions.
	void precomputeResult(const char* context, UID applierID, int batchIndex) {
		TraceEvent(SevFRMutationInfo, "FastRestoreApplierPrecomputeResult", applierID)
		    .detail("BatchIndex", batchIndex)
		    .detail("Context", context)
		    .detail("Version", version.toString())
		    .detail("Key", key)
		    .detail("Value", val)
		    .detail("MType", type < MutationRef::MAX_ATOMIC_OP ? getTypeString(type) : "[Unset]")
		    .detail("LargestPendingVersion",
		            (pendingMutations.empty() ? "[none]" : pendingMutations.rbegin()->first.toString()))
		    .detail("PendingMutations", pendingMutations.size());
		std::map<LogMessageVersion, Standalone<MutationRef>>::iterator lb = pendingMutations.lower_bound(version);
		if (lb == pendingMutations.end()) {
			return;
		}
		ASSERT(!pendingMutations.empty());
		if (lb->first == version) {
			// Sanity check mutations at version are either atomicOps which can be ignored or the same value as buffered
			MutationRef m = lb->second;
			if (m.type == MutationRef::SetValue || m.type == MutationRef::ClearRange) {
				if (std::tie(type, key, val) != std::tie(m.type, m.param1, m.param2)) {
					TraceEvent(SevError, "FastRestoreApplierPrecomputeResultUnhandledSituation", applierID)
					    .detail("BatchIndex", batchIndex)
					    .detail("Context", context)
					    .detail("BufferedType", getTypeString(type))
					    .detail("PendingType", getTypeString(m.type))
					    .detail("BufferedVal", val.toString())
					    .detail("PendingVal", m.param2.toString());
				}
			}
			lb++;
		}
		for (; lb != pendingMutations.end(); lb++) {
			MutationRef mutation = lb->second;
			if (mutation.type == MutationRef::CompareAndClear) { // Special atomicOp
				Arena arena;
				Optional<StringRef> inputVal;
				if (hasBaseValue()) {
					inputVal = val;
				}
				Optional<ValueRef> retVal = doCompareAndClear(inputVal, mutation.param2, arena);
				if (!retVal.present()) {
					val = key;
					type = MutationRef::ClearRange;
				} // else no-op
			} else if (isAtomicOp((MutationRef::Type)mutation.type)) {
				Optional<StringRef> inputVal;
				if (hasBaseValue()) {
					inputVal = val;
				}
				val = applyAtomicOp(inputVal, mutation.param2, (MutationRef::Type)mutation.type);
				type = MutationRef::SetValue; // Precomputed result should be set to DB.
			} else if (mutation.type == MutationRef::SetValue || mutation.type == MutationRef::ClearRange) {
				type = MutationRef::SetValue;
				TraceEvent(SevError, "FastRestoreApplierPrecomputeResultUnexpectedSet", applierID)
				    .detail("BatchIndex", batchIndex)
				    .detail("Context", context)
				    .detail("MutationType", getTypeString(mutation.type))
				    .detail("Version", lb->first.toString());
			} else {
				TraceEvent(SevError, "FastRestoreApplierPrecomputeResultSkipUnexpectedBackupMutation", applierID)
				    .detail("BatchIndex", batchIndex)
				    .detail("Context", context)
				    .detail("MutationType", getTypeString(mutation.type))
				    .detail("Version", lb->first.toString());
			}
			ASSERT(lb->first > version);
			version = lb->first;
		}
	}

	// Does the key has at least 1 set or clear mutation to get the base value
	bool hasBaseValue() {
		if (version.version > 0) {
			ASSERT(type == MutationRef::SetValue || type == MutationRef::ClearRange);
		}
		return version.version > 0;
	}

	// Has all pendingMutations been pre-applied to the val?
	bool hasPrecomputed() {
		ASSERT(pendingMutations.empty() || pendingMutations.rbegin()->first >= pendingMutations.begin()->first);
		return pendingMutations.empty() || version >= pendingMutations.rbegin()->first;
	}

	int totalSize() { return MutationRef::OVERHEAD_BYTES + key.size() + val.size(); }
};

// The range mutation received on applier.
// Range mutations should be applied both to the destination DB and to the StagingKeys
struct StagingKeyRange {
	Standalone<MutationRef> mutation;
	LogMessageVersion version;

	explicit StagingKeyRange(MutationRef m, LogMessageVersion newVersion) : mutation(m), version(newVersion) {}

	bool operator<(const StagingKeyRange& rhs) const {
		return std::tie(version, mutation.type, mutation.param1, mutation.param2) <
		       std::tie(rhs.version, rhs.mutation.type, rhs.mutation.param1, rhs.mutation.param2);
	}
};

// Applier state in each verion batch
class ApplierVersionBatchState : RoleVersionBatchState {
public:
	static const int NOT_INIT = 0;
	static const int INIT = 1;
	static const int RECEIVE_MUTATIONS = 2;
	static const int WRITE_TO_DB = 3;
	static const int DONE = 4;
	static const int INVALID = 5;

	explicit ApplierVersionBatchState(int newState) { vbState = newState; }

	~ApplierVersionBatchState() override = default;

	void operator=(int newState) override { vbState = newState; }

	int get() override { return vbState; }
};

struct ApplierBatchData : public ReferenceCounted<ApplierBatchData> {
	// processedFileState: key: RestoreAsset; value: largest version of mutation received on the applier
	std::map<RestoreAsset, NotifiedVersion> processedFileState;
	Optional<Future<Void>> dbApplier;
	VersionedMutationsMap kvOps; // Mutations at each version
	std::map<Key, StagingKey> stagingKeys;
	std::set<StagingKeyRange> stagingKeyRanges;

	Future<Void> pollMetrics;

	RoleVersionBatchState vbState;

	long receiveMutationReqs;

	// Stats
	double receivedBytes; // received mutation size
	double appliedBytes; // after coalesce, how many bytes to write to DB
	double targetWriteRateMB; // target amount of data outstanding for DB;
	double totalBytesToWrite; // total amount of data in bytes to write
	double applyingDataBytes; // amount of data in flight of committing
	AsyncTrigger releaseTxnTrigger; // trigger to release more txns
	Future<Void> rateTracer; // trace transaction rate control info

	// Status counters
	struct Counters {
		CounterCollection cc;
		Counter receivedBytes, receivedWeightedBytes, receivedMutations, receivedAtomicOps;
		Counter appliedBytes, appliedWeightedBytes, appliedMutations, appliedAtomicOps;
		Counter appliedTxns, appliedTxnRetries;
		Counter fetchKeys, fetchTxns, fetchTxnRetries; // number of keys to fetch from dest. FDB cluster.
		Counter clearOps, clearTxns;

		Counters(ApplierBatchData* self, UID applierInterfID, int batchIndex)
		  : cc("ApplierBatch", applierInterfID.toString() + ":" + std::to_string(batchIndex)),
		    receivedBytes("ReceivedBytes", cc), receivedWeightedBytes("ReceivedWeightedMutations", cc),
		    receivedMutations("ReceivedMutations", cc), receivedAtomicOps("ReceivedAtomicOps", cc),
		    appliedBytes("AppliedBytes", cc), appliedWeightedBytes("AppliedWeightedBytes", cc),
		    appliedMutations("AppliedMutations", cc), appliedAtomicOps("AppliedAtomicOps", cc),
		    appliedTxns("AppliedTxns", cc), appliedTxnRetries("AppliedTxnRetries", cc), fetchKeys("FetchKeys", cc),
		    fetchTxns("FetchTxns", cc), fetchTxnRetries("FetchTxnRetries", cc), clearOps("ClearOps", cc),
		    clearTxns("ClearTxns", cc) {}
	} counters;

	void addref() { return ReferenceCounted<ApplierBatchData>::addref(); }
	void delref() { return ReferenceCounted<ApplierBatchData>::delref(); }

	explicit ApplierBatchData(UID nodeID, int batchIndex)
	  : vbState(ApplierVersionBatchState::NOT_INIT), receiveMutationReqs(0), receivedBytes(0), appliedBytes(0),
	    targetWriteRateMB(SERVER_KNOBS->FASTRESTORE_WRITE_BW_MB / SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS),
	    totalBytesToWrite(-1), applyingDataBytes(0), counters(this, nodeID, batchIndex) {
		pollMetrics = traceCounters(format("FastRestoreApplierMetrics%d", batchIndex),
		                            nodeID,
		                            SERVER_KNOBS->FASTRESTORE_ROLE_LOGGING_DELAY,
		                            &counters.cc,
		                            nodeID.toString() + "/RestoreApplierMetrics/" + std::to_string(batchIndex));
		TraceEvent("FastRestoreApplierMetricsCreated").detail("Node", nodeID);
	}
	~ApplierBatchData() {
		rateTracer = Void(); // cancel actor
	}

	void addMutation(MutationRef m, LogMessageVersion ver) {
		if (!isRangeMutation(m)) {
			auto item = stagingKeys.emplace(m.param1, StagingKey(m.param1));
			item.first->second.add(m, ver);
		} else {
			stagingKeyRanges.insert(StagingKeyRange(m, ver));
		}
	}

	// Return true if all staging keys have been precomputed
	bool allKeysPrecomputed() {
		for (auto& stagingKey : stagingKeys) {
			if (!stagingKey.second.hasPrecomputed()) {
				TraceEvent("FastRestoreApplierAllKeysPrecomputedFalse")
				    .detail("Key", stagingKey.first)
				    .detail("BufferedVersion", stagingKey.second.version.toString())
				    .detail("MaxPendingVersion", stagingKey.second.pendingMutations.rbegin()->first.toString());
				return false;
			}
		}
		TraceEvent("FastRestoreApplierAllKeysPrecomputed").log();
		return true;
	}

	void reset() {
		kvOps.clear();
		dbApplier = Optional<Future<Void>>();
	}

	void sanityCheckMutationOps() {
		if (kvOps.empty())
			return;

		ASSERT_WE_THINK(isKVOpsSorted());
		ASSERT_WE_THINK(allOpsAreKnown());
	}

	bool isKVOpsSorted() {
		auto prev = kvOps.begin();
		for (auto it = kvOps.begin(); it != kvOps.end(); ++it) {
			if (prev->first > it->first) {
				return false;
			}
			prev = it;
		}
		return true;
	}

	bool allOpsAreKnown() {
		for (auto it = kvOps.begin(); it != kvOps.end(); ++it) {
			for (auto m = it->second.begin(); m != it->second.end(); ++m) {
				if (m->type == MutationRef::SetValue || m->type == MutationRef::ClearRange ||
				    isAtomicOp((MutationRef::Type)m->type))
					continue;
				else {
					TraceEvent(SevError, "FastRestoreApplier").detail("UnknownMutationType", m->type);
					return false;
				}
			}
		}
		return true;
	}
};

struct RestoreApplierData : RestoreRoleData, public ReferenceCounted<RestoreApplierData> {
	// Buffer for uncommitted data at ongoing version batches
	std::map<int, Reference<ApplierBatchData>> batch;

	void addref() { return ReferenceCounted<RestoreApplierData>::addref(); }
	void delref() { return ReferenceCounted<RestoreApplierData>::delref(); }

	explicit RestoreApplierData(UID applierInterfID, int assignedIndex) {
		nodeID = applierInterfID;
		nodeIndex = assignedIndex;

		// Q: Why do we need to initMetric?
		// version.initMetric(LiteralStringRef("RestoreApplier.Version"), cc.id);

		role = RestoreRole::Applier;
	}

	~RestoreApplierData() override = default;

	// getVersionBatchState may be called periodically to dump version batch state,
	// even when no version batch has been started.
	int getVersionBatchState(int batchIndex) final {
		std::map<int, Reference<ApplierBatchData>>::iterator item = batch.find(batchIndex);
		if (item == batch.end()) { // Batch has not been initialized when we blindly profile the state
			return ApplierVersionBatchState::INVALID;
		} else {
			return item->second->vbState.get();
		}
	}
	void setVersionBatchState(int batchIndex, int vbState) final {
		std::map<int, Reference<ApplierBatchData>>::iterator item = batch.find(batchIndex);
		ASSERT(item != batch.end());
		item->second->vbState = vbState;
	}

	void initVersionBatch(int batchIndex) override {
		TraceEvent("FastRestoreApplierInitVersionBatch", id()).detail("BatchIndex", batchIndex);
		batch[batchIndex] = Reference<ApplierBatchData>(new ApplierBatchData(nodeID, batchIndex));
	}

	void resetPerRestoreRequest() override {
		batch.clear();
		finishedBatch = NotifiedVersion(0);
	}

	std::string describeNode() override {
		std::stringstream ss;
		ss << "NodeID:" << nodeID.toString() << " nodeIndex:" << nodeIndex;
		return ss.str();
	}
};

ACTOR Future<Void> restoreApplierCore(RestoreApplierInterface applierInterf, int nodeIndex, Database cx);

#include "flow/unactorcompiler.h"
#endif
