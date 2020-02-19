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
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbclient/RestoreWorkerInterface.actor.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"

#include "flow/actorcompiler.h" // has to be last include

struct ApplierBatchData : public ReferenceCounted<ApplierBatchData> {
	// processedFileState: key: RestoreAsset; value: largest version of mutation received on the applier
	std::map<RestoreAsset, NotifiedVersion> processedFileState;
	Optional<Future<Void>> dbApplier;
	VersionedMutationsMap kvOps; // Mutations at each version

	Future<Void> pollMetrics;

	// Status counters
	struct Counters {
		CounterCollection cc;
		Counter receivedBytes, receivedWeightedBytes, receivedMutations, receivedAtomicOps;
		Counter appliedWeightedBytes, appliedMutations, appliedAtomicOps;
		Counter appliedTxns;

		Counters(ApplierBatchData* self, UID applierInterfID, int batchIndex)
		  : cc("ApplierBatch", applierInterfID.toString() + ":" + std::to_string(batchIndex)),
		    receivedBytes("ReceivedBytes", cc), receivedMutations("ReceivedMutations", cc),
		    receivedAtomicOps("ReceivedAtomicOps", cc), receivedWeightedBytes("ReceivedWeightedMutations", cc),
		    appliedWeightedBytes("AppliedWeightedBytes", cc), appliedMutations("AppliedMutations", cc),
		    appliedAtomicOps("AppliedAtomicOps", cc), appliedTxns("AppliedTxns", cc) {}
	} counters;

	void addref() { return ReferenceCounted<ApplierBatchData>::addref(); }
	void delref() { return ReferenceCounted<ApplierBatchData>::delref(); }

	explicit ApplierBatchData(UID nodeID, int batchIndex) : counters(this, nodeID, batchIndex) {
		pollMetrics =
		    traceCounters("FastRestoreApplierMetrics", nodeID, SERVER_KNOBS->FASTRESTORE_ROLE_LOGGING_DELAY,
		                  &counters.cc, nodeID.toString() + "/RestoreApplierMetrics/" + std::to_string(batchIndex));
		TraceEvent("FastRestoreApplierMetricsCreated").detail("Node", nodeID);
	}
	~ApplierBatchData() = default;

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
	NotifiedVersion finishedBatch; // The version batch that has been applied to DB

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
