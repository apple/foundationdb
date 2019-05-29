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
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreWorkerInterface.actor.h"

#include "flow/actorcompiler.h" // has to be last include

extern double transactionBatchSizeThreshold;

struct RestoreApplierData : RestoreRoleData, public ReferenceCounted<RestoreApplierData> { 
	NotifiedVersion rangeVersion; // All requests of mutations in range file below this version has been processed
	NotifiedVersion logVersion; // All requests of mutations in log file below this version has been processed
	Optional<Future<Void>> dbApplier;

	// range2Applier is in master and loader node. Loader node uses this to determine which applier a mutation should be sent
	std::map<Standalone<KeyRef>, UID> range2Applier; // KeyRef is the inclusive lower bound of the key range the applier (UID) is responsible for
	std::map<Standalone<KeyRef>, int> keyOpsCount; // The number of operations per key which is used to determine the key-range boundary for appliers
	int numSampledMutations; // The total number of mutations received from sampled data.

	// For master applier to hold the lower bound of key ranges for each appliers
	std::vector<Standalone<KeyRef>> keyRangeLowerBounds;

	// TODO: This block of variables may be moved to RestoreRoleData
	bool inProgressApplyToDB = false;

	// Temporary data structure for parsing range and log files into (version, <K, V, mutationType>)
	std::map<Version, Standalone<VectorRef<MutationRef>>> kvOps;

	void addref() { return ReferenceCounted<RestoreApplierData>::addref(); }
	void delref() { return ReferenceCounted<RestoreApplierData>::delref(); }

	explicit RestoreApplierData(UID applierInterfID, int assignedIndex) {
		nodeID = applierInterfID;
		nodeIndex = assignedIndex;

		// Q: Why do we need to initMetric?
		//version.initMetric(LiteralStringRef("RestoreApplier.Version"), cc.id);

		role = RestoreRole::Applier;
	}

	~RestoreApplierData() = default;

	std::string describeNode() {
		std::stringstream ss;
		ss << "NodeID:" << nodeID.toString() << " nodeIndex:" << nodeIndex;
		return ss.str();
	}

	void resetPerVersionBatch() {
		RestoreRoleData::resetPerVersionBatch();

		inProgressApplyToDB = false;
		kvOps.clear();
		dbApplier = Optional<Future<Void>>();
	}

	void sanityCheckMutationOps() {
		if (kvOps.empty())
			return;

		if ( isKVOpsSorted() ) {
			printf("[CORRECT] KVOps is sorted by version\n");
		} else {
			printf("[ERROR]!!! KVOps is NOT sorted by version\n");
		}

		if ( allOpsAreKnown() ) {
			printf("[CORRECT] KVOps all operations are known.\n");
		} else {
			printf("[ERROR]!!! KVOps has unknown mutation op. Exit...\n");
		}
	}

	bool isKVOpsSorted() {
		bool ret = true;
		auto prev = kvOps.begin();
		for ( auto it = kvOps.begin(); it != kvOps.end(); ++it ) {
			if ( prev->first > it->first ) {
				ret = false;
				break;
			}
			prev = it;
		}
		return ret;
	}

	bool allOpsAreKnown() {
		bool ret = true;
		for ( auto it = kvOps.begin(); it != kvOps.end(); ++it ) {
			for ( auto m = it->second.begin(); m != it->second.end(); ++m ) {
				if ( m->type == MutationRef::SetValue || m->type == MutationRef::ClearRange
					|| isAtomicOp((MutationRef::Type) m->type) )
					continue;
				else {
					printf("[ERROR] Unknown mutation type:%d\n", m->type);
					ret = false;
				}
			}

		}

		return ret;
	}


	std::vector<Standalone<KeyRef>> calculateAppliersKeyRanges(int numAppliers) {
		ASSERT(numAppliers > 0);
		std::vector<Standalone<KeyRef>> lowerBounds;
		int numSampledMutations = 0;
		for (auto &count : keyOpsCount) {
			numSampledMutations += count.second;
		}

		//intervalLength = (numSampledMutations - remainder) / (numApplier - 1)
		int intervalLength = std::max(numSampledMutations / numAppliers, 1); // minimal length is 1
		int curCount = 0;
		int curInterval = 0;

		printf("[INFO] Node:%s calculateAppliersKeyRanges(): numSampledMutations:%d numAppliers:%d intervalLength:%d\n",
				describeNode().c_str(),
				numSampledMutations, numAppliers, intervalLength);
		for (auto &count : keyOpsCount) {
			if (curCount >= curInterval * intervalLength) {
				printf("[INFO] Node:%s calculateAppliersKeyRanges(): Add a new key range  [%d]:%s: curCount:%d\n",
						describeNode().c_str(), curInterval, count.first.toString().c_str(), curCount);
				lowerBounds.push_back(count.first); // The lower bound of the current key range
				curInterval++;
			}
			curCount += count.second;
		}

		if ( lowerBounds.size() != numAppliers ) {
			printf("[WARNING] calculateAppliersKeyRanges() WE MAY NOT USE ALL APPLIERS efficiently! num_keyRanges:%ld numAppliers:%d\n",
					lowerBounds.size(), numAppliers);
			printLowerBounds(lowerBounds);
		}

		//ASSERT(lowerBounds.size() <= numAppliers + 1); // We may have at most numAppliers + 1 key ranges
		if ( lowerBounds.size() > numAppliers ) {
			printf("[WARNING] Key ranges number:%ld > numAppliers:%d. Merge the last ones\n", lowerBounds.size(), numAppliers);
		}

		while ( lowerBounds.size() > numAppliers ) {
			printf("[WARNING] Key ranges number:%ld > numAppliers:%d. Merge the last ones\n", lowerBounds.size(), numAppliers);
			lowerBounds.pop_back();
		}

		return lowerBounds;
	}
};


ACTOR Future<Void> restoreApplierCore(Reference<RestoreApplierData> self, RestoreApplierInterface applierInterf, Database cx);


#include "flow/unactorcompiler.h"
#endif