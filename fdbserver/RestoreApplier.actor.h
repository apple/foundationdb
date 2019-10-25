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

struct RestoreApplierData : RestoreRoleData, public ReferenceCounted<RestoreApplierData> {
	// processedFileState: key: file unique index; value: largest version of mutation received on the applier
	std::map<uint32_t, NotifiedVersion> processedFileState;
	Optional<Future<Void>> dbApplier;

	// rangeToApplier is in master and loader. Loader uses it to determine which applier a mutation should be sent
	//   KeyRef is the inclusive lower bound of the key range the applier (UID) is responsible for
	std::map<Standalone<KeyRef>, UID> rangeToApplier;
	// keyOpsCount is the number of operations per key that is used to determine the key-range boundary for appliers
	std::map<Standalone<KeyRef>, int> keyOpsCount;

	// For master applier to hold the lower bound of key ranges for each appliers
	std::vector<Standalone<KeyRef>> keyRangeLowerBounds;

	// TODO: This block of variables may be moved to RestoreRoleData
	bool inProgressApplyToDB = false;

	// Mutations at each version
	VersionedMutationsMap kvOps;

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

	std::string describeNode() {
		std::stringstream ss;
		ss << "NodeID:" << nodeID.toString() << " nodeIndex:" << nodeIndex;
		return ss.str();
	}

	void resetPerVersionBatch() {
		TraceEvent("FastRestore").detail("ResetPerVersionBatchOnApplier", nodeID);
		inProgressApplyToDB = false;
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

ACTOR Future<Void> restoreApplierCore(RestoreApplierInterface applierInterf, int nodeIndex, Database cx);

#include "flow/unactorcompiler.h"
#endif
