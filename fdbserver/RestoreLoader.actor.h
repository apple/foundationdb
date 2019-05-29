/*
 * RestoreLoader.h
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

// This file declares the actors used by the RestoreLoader role

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RESTORE_LOADER_G_H)
	#define FDBSERVER_RESTORE_LOADER_G_H
	#include "fdbserver/RestoreLoader.actor.g.h"
#elif !defined(FDBSERVER_RESTORE_LOADER_H)
	#define FDBSERVER_RESTORE_LOADER_H

#include <sstream>
#include "flow/Stats.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbrpc/Locality.h"

#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreCommon.actor.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreWorkerInterface.actor.h"
#include "fdbclient/BackupContainer.h"

#include "flow/actorcompiler.h" // has to be last include

struct RestoreLoaderData : RestoreRoleData, public ReferenceCounted<RestoreLoaderData> {
	std::map<LoadingParam, Future<Void>> processedFileParams;

	// range2Applier is in master and loader node. Loader node uses this to determine which applier a mutation should be sent
	std::map<Standalone<KeyRef>, UID> range2Applier; // KeyRef is the inclusive lower bound of the key range the applier (UID) is responsible for
	std::map<Standalone<KeyRef>, int> keyOpsCount; // The number of operations per key which is used to determine the key-range boundary for appliers
	int numSampledMutations; // The total number of mutations received from sampled data.

	Reference<IBackupContainer> bc; // Backup container is used to read backup files
	Key bcUrl; // The url used to get the bc

    // Performance statistics
    double curWorkloadSize;

	void addref() { return ReferenceCounted<RestoreLoaderData>::addref(); }
	void delref() { return ReferenceCounted<RestoreLoaderData>::delref(); }

	explicit RestoreLoaderData(UID loaderInterfID, int assignedIndex) {
		nodeID = loaderInterfID;
		nodeIndex = assignedIndex;
		role = RestoreRole::Loader;
	}

	~RestoreLoaderData() {}

	std::string describeNode() {
		std::stringstream ss;
		ss << "[Role: Loader] [NodeID:" << nodeID.toString().c_str()
			<< "] [NodeIndex:" << std::to_string(nodeIndex) << "]";
		return ss.str();
	}

    void resetPerVersionBatch() {
		printf("[INFO]Node:%s resetPerVersionBatch\n", nodeID.toString().c_str());
		RestoreRoleData::resetPerVersionBatch();

		range2Applier.clear();
		keyOpsCount.clear();
		numSampledMutations = 0;
		
		processedFileParams.clear();

		curWorkloadSize = 0;
	}

	vector<UID> getBusyAppliers() {
		vector<UID> busyAppliers;
		for (auto &app : range2Applier) {
			busyAppliers.push_back(app.second);
		}
		return busyAppliers;
	}

    std::vector<UID> getWorkingApplierIDs() {
        std::vector<UID> applierIDs;
        for ( auto &applier : range2Applier ) {
            applierIDs.push_back(applier.second);
        }

        ASSERT( !applierIDs.empty() );
        return applierIDs;
    }

	void initBackupContainer(Key url) {
		if ( bcUrl == url && bc.isValid() ) {
			return;
		}
		printf("initBackupContainer, url:%s\n", url.toString().c_str());
		bcUrl = url;
		bc = IBackupContainer::openContainer(url.toString());
	}

	void printAppliersKeyRange() {
		printf("[INFO] The mapping of KeyRange_start --> Applier ID: getHexString\n");
		// applier type: std::map<Standalone<KeyRef>, UID>
		for (auto &applier : range2Applier) {
			printf("\t[INFO]%s -> %s\n", getHexString(applier.first).c_str(), applier.second.toString().c_str());
		}
		printf("[INFO] The mapping of KeyRange_start --> Applier ID: toString\n");
		// applier type: std::map<Standalone<KeyRef>, UID>
		for (auto &applier : range2Applier) {
			printf("\t[INFO]%s -> %s\n", applier.first.toString().c_str(), applier.second.toString().c_str());
		}
	}
};


ACTOR Future<Void> restoreLoaderCore(Reference<RestoreLoaderData> self, RestoreLoaderInterface loaderInterf, Database cx);

#include "flow/unactorcompiler.h"
#endif