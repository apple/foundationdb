/*
 * RestoreApplier.actor.h
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

// This file declears RestoreApplier interface and actors

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RESTORE_APPLIER_G_H)
#define FDBSERVER_RESTORE_APPLIER_G_H
#include "fdbserver/RestoreApplier.actor.g.h"
#elif !defined(FDBSERVER_RESTORE_APPLIER_H)
#define FDBSERVER_RESTORE_APPLIER_H

#include <sstream>
#include "flow/Stats.h"
#include "fdbclient/Atomic.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbclient/RestoreWorkerInterface.actor.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"

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
	Version version; // largest version of set or clear for the key
	std::map<Version, MutationsVec> pendingMutations; // mutations not set or clear type

	explicit StagingKey() : version(0), type(MutationRef::MAX_ATOMIC_OP) {}

	// Add mutation m at newVersion to stagingKey
	// Assume: SetVersionstampedKey and SetVersionstampedValue have been converted to set
	void add(const MutationRef& m, Version newVersion) {
		ASSERT(m.type != MutationRef::SetVersionstampedKey && m.type != MutationRef::SetVersionstampedValue);
		if (version < newVersion) {
			if (m.type == MutationRef::SetValue || m.type == MutationRef::ClearRange) {
				key = m.param1;
				val = m.param2;
				type = (MutationRef::Type)m.type;
				version = newVersion;
			} else {
				if (pendingMutations.find(newVersion) == pendingMutations.end()) {
					pendingMutations.emplace(newVersion, MutationsVec());
				}
				// TODO: Do we really need deep copy?
				MutationsVec& mutations = pendingMutations[newVersion];
				mutations.push_back_deep(mutations.arena(), m);
			}
		} else if (version == newVersion) { // Sanity check
			TraceEvent("FastRestoreApplierStagingKeyMutationAtSameVersion")
			    .detail("Version", newVersion)
			    .detail("NewMutation", m.toString())
			    .detail("ExistingKeyType", typeString[type]);
			if (m.type == MutationRef::SetValue) {
				if (type == MutationRef::SetValue) {
					if (m.param2 != val) {
						TraceEvent(SevError, "FastRestoreApplierStagingKeyMutationAtSameVersionUnhandled")
						    .detail("Version", newVersion)
						    .detail("NewMutation", m.toString())
						    .detail("ExistingKeyType", typeString[type])
						    .detail("ExitingKeyValue", val)
						    .detail("Investigate",
						            "Why would backup have two sets with different value at same version");
					} // else {} Backup has duplicate set at the same version
				} else {
					TraceEvent(SevWarnAlways, "FastRestoreApplierStagingKeyMutationAtSameVersionOverride")
					    .detail("Version", newVersion)
					    .detail("NewMutation", m.toString())
					    .detail("ExistingKeyType", typeString[type])
					    .detail("ExitingKeyValue", val);
					type = (MutationRef::Type)m.type;
					val = m.param2;
				}
			} else if (m.type == MutationRef::ClearRange) {
				TraceEvent(SevWarnAlways, "FastRestoreApplierStagingKeyMutationAtSameVersionSkipped")
				    .detail("Version", newVersion)
				    .detail("NewMutation", m.toString())
				    .detail("ExistingKeyType", typeString[type])
				    .detail("ExitingKeyValue", val);
			}
		} // else  input mutation is old and can be ignored
	}

	// Precompute the final value of the key.
	void precomputeResult() {
		TraceEvent(SevDebug, "FastRestoreApplierPrecomputeResult")
		    .detail("Key", key)
		    .detail("Version", version)
		    .detail("LargestPendingVersion", (pendingMutations.empty() ? -1 : pendingMutations.rbegin()->first));
		std::map<Version, MutationsVec>::iterator lb = pendingMutations.lower_bound(version);
		if (lb == pendingMutations.end()) {
			return;
		}
		if (lb->first == version) {
			// Sanity check mutations at version are either atomicOps which can be ignored or the same value as buffered
			for (int i = 0; i < lb->second.size(); i++) {
				MutationRef m = lb->second[i];
				if (m.type == MutationRef::SetValue || m.type == MutationRef::ClearRange) {
					if (std::tie(type, key, val) != std::tie(m.type, m.param1, m.param2)) {
						TraceEvent(SevError, "FastRestoreApplierPrecomputeResultUnhandledSituation")
						    .detail("BufferedType", typeString[type])
						    .detail("PendingType", typeString[m.type])
						    .detail("BufferedVal", val.toString())
						    .detail("PendingVal", m.param2.toString());
					}
				}
			}
		}
		while (lb != pendingMutations.end()) {
			if (lb->first == version) {
				lb++;
				continue;
			}
			for (auto& mutation : lb->second) {
				if (type == MutationRef::CompareAndClear) { // Special atomicOp
					Arena arena;
					Optional<ValueRef> retVal = doCompareAndClear(val, mutation.param2, arena);
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
					type = MutationRef::SetValue; // Precomputed result should be set to DB.
					TraceEvent(SevError, "FastRestoreApplierPrecomputeResultUnexpectedSet")
					    .detail("Type", typeString[mutation.type])
					    .detail("Version", lb->first);
				} else {
					TraceEvent(SevWarnAlways, "FastRestoreApplierPrecomputeResultSkipUnexpectedBackupMutation")
					    .detail("Type", typeString[mutation.type])
					    .detail("Version", lb->first);
				}
			}
			version = lb->first;
			lb++;
		}
	}

	// Does the key has at least 1 set or clear mutation to get the base value
	bool hasBaseValue() {
		if (version > 0) {
			ASSERT(type == MutationRef::SetValue || type == MutationRef::ClearRange);
		}
		return version > 0;
	}

	// Has all pendingMutations been pre-applied to the val?
	bool hasPrecomputed() {
		ASSERT(pendingMutations.empty() || pendingMutations.rbegin()->first >= pendingMutations.begin()->first);
		return pendingMutations.empty() || version >= pendingMutations.rbegin()->first;
	}

	int expectedMutationSize() { return key.size() + val.size(); }
};

// The range mutation received on applier.
// Range mutations should be applied both to the destination DB and to the StagingKeys
struct StagingKeyRange {
	Standalone<MutationRef> mutation;
	Version version;

	explicit StagingKeyRange(MutationRef m, Version newVersion) : mutation(m), version(newVersion) {}

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
	static const int INVALID = 4;

	explicit ApplierVersionBatchState(int newState) {
		vbState = newState;
	}

	virtual ~ApplierVersionBatchState() = default;

	virtual void operator=(int newState) { vbState = newState; }

	virtual int get() { return vbState; }
};

struct ApplierBatchData : public ReferenceCounted<ApplierBatchData> {
	// processedFileState: key: RestoreAsset; value: largest version of mutation received on the applier
	std::map<RestoreAsset, NotifiedVersion> processedFileState;
	Optional<Future<Void>> dbApplier;
	VersionedMutationsMap kvOps; // Mutations at each version
	std::map<Key, StagingKey> stagingKeys;
	std::set<StagingKeyRange> stagingKeyRanges;
	FlowLock applyStagingKeysBatchLock;

	Future<Void> pollMetrics;

	RoleVersionBatchState vbState;

	// Status counters
	struct Counters {
		CounterCollection cc;
		Counter receivedBytes, receivedWeightedBytes, receivedMutations, receivedAtomicOps;
		Counter appliedWeightedBytes, appliedMutations, appliedAtomicOps;
		Counter appliedTxns;
		Counter fetchKeys; // number of keys to fetch from dest. FDB cluster.

		Counters(ApplierBatchData* self, UID applierInterfID, int batchIndex)
		  : cc("ApplierBatch", applierInterfID.toString() + ":" + std::to_string(batchIndex)),
		    receivedBytes("ReceivedBytes", cc), receivedMutations("ReceivedMutations", cc),
		    receivedAtomicOps("ReceivedAtomicOps", cc), receivedWeightedBytes("ReceivedWeightedMutations", cc),
		    appliedWeightedBytes("AppliedWeightedBytes", cc), appliedMutations("AppliedMutations", cc),
		    appliedAtomicOps("AppliedAtomicOps", cc), appliedTxns("AppliedTxns", cc), fetchKeys("FetchKeys", cc) {}
	} counters;

	void addref() { return ReferenceCounted<ApplierBatchData>::addref(); }
	void delref() { return ReferenceCounted<ApplierBatchData>::delref(); }

	explicit ApplierBatchData(UID nodeID, int batchIndex)
	  : counters(this, nodeID, batchIndex), applyStagingKeysBatchLock(SERVER_KNOBS->FASTRESTORE_APPLYING_PARALLELISM),
	    vbState(ApplierVersionBatchState::NOT_INIT) {
		pollMetrics =
		    traceCounters("FastRestoreApplierMetrics", nodeID, SERVER_KNOBS->FASTRESTORE_ROLE_LOGGING_DELAY,
		                  &counters.cc, nodeID.toString() + "/RestoreApplierMetrics/" + std::to_string(batchIndex));
		TraceEvent("FastRestoreApplierMetricsCreated").detail("Node", nodeID);
	}
	~ApplierBatchData() = default;

	void addMutation(MutationRef m, Version ver) {
		if (!isRangeMutation(m)) {
			auto item = stagingKeys.emplace(m.param1, StagingKey());
			item.first->second.add(m, ver);
		} else {
			stagingKeyRanges.insert(StagingKeyRange(m, ver));
		}
	}

	void addVersionStampedKV(MutationRef m, Version ver, uint16_t numVersionStampedKV) {
		if (m.type == MutationRef::SetVersionstampedKey) {
			// Assume transactionNumber = 0 does not affect result
			TraceEvent(SevDebug, "FastRestoreApplierAddMutation")
			    .detail("MutationType", typeString[m.type])
			    .detail("FakedTransactionNumber", numVersionStampedKV);
			transformVersionstampMutation(m, &MutationRef::param1, ver, numVersionStampedKV);
			addMutation(m, ver);
		} else if (m.type == MutationRef::SetVersionstampedValue) {
			// Assume transactionNumber = 0 does not affect result
			TraceEvent(SevDebug, "FastRestoreApplierAddMutation")
			    .detail("MutationType", typeString[m.type])
			    .detail("FakedTransactionNumber", numVersionStampedKV);
			transformVersionstampMutation(m, &MutationRef::param2, ver, numVersionStampedKV);
			addMutation(m, ver);
		} else {
			ASSERT(false);
		}
	}

	// Return true if all staging keys have been precomputed
	bool allKeysPrecomputed() {
		for (auto& stagingKey : stagingKeys) {
			if (!stagingKey.second.hasPrecomputed()) {
				TraceEvent("FastRestoreApplierAllKeysPrecomputedFalse")
				    .detail("Key", stagingKey.first)
				    .detail("BufferedVersion", stagingKey.second.version)
				    .detail("MaxPendingVersion", stagingKey.second.pendingMutations.rbegin()->first);
				return false;
			}
		}
		TraceEvent("FastRestoreApplierAllKeysPrecomputed");
		return true;
	}

	void reset() {
		kvOps.clear();
		dbApplier = Optional<Future<Void>>();
	}

	void sanityCheckMutationOps() {
		if (kvOps.empty()) return;

		ASSERT_WE_THINK(isKVOpsSorted());
		ASSERT_WE_THINK(allOpsAreKnown());
	}

	bool isKVOpsSorted() {
		bool ret = true;
		auto prev = kvOps.begin();
		for (auto it = kvOps.begin(); it != kvOps.end(); ++it) {
			if (prev->first > it->first) {
				ret = false;
				break;
			}
			prev = it;
		}
		return ret;
	}

	bool allOpsAreKnown() {
		bool ret = true;
		for (auto it = kvOps.begin(); it != kvOps.end(); ++it) {
			for (auto m = it->second.begin(); m != it->second.end(); ++m) {
				if (m->type == MutationRef::SetValue || m->type == MutationRef::ClearRange ||
				    isAtomicOp((MutationRef::Type)m->type))
					continue;
				else {
					TraceEvent(SevError, "FastRestore").detail("UnknownMutationType", m->type);
					ret = false;
				}
			}
		}
		return ret;
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

	~RestoreApplierData() = default;

	// getVersionBatchState may be called periodically to dump version batch state,
	// even when no version batch has been started.
	int getVersionBatchState(int batchIndex) final {
		std::map<int, Reference<ApplierBatchData>>::iterator item = batch.find(batchIndex);
		if (item == batch.end()) { // Simply caller's effort in when it can call this func.
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

	void initVersionBatch(int batchIndex) {
		TraceEvent("FastRestoreApplierInitVersionBatch", id()).detail("BatchIndex", batchIndex);
		batch[batchIndex] = Reference<ApplierBatchData>(new ApplierBatchData(nodeID, batchIndex));
	}

	void resetPerRestoreRequest() {
		batch.clear();
		finishedBatch = NotifiedVersion(0);
	}

	std::string describeNode() {
		std::stringstream ss;
		ss << "NodeID:" << nodeID.toString() << " nodeIndex:" << nodeIndex;
		return ss.str();
	}
};

ACTOR Future<Void> restoreApplierCore(RestoreApplierInterface applierInterf, int nodeIndex, Database cx);

#include "flow/unactorcompiler.h"
#endif
