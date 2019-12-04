/*
 * RestoreMaster.h
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

// This file declear RestoreMaster interface and actors

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RESTORE_MASTER_G_H)
#define FDBSERVER_RESTORE_MASTER_G_H
#include "fdbserver/RestoreMaster.actor.g.h"
#elif !defined(FDBSERVER_RESTORE_MASTER_H)
#define FDBSERVER_RESTORE_MASTER_H

#include <sstream>
#include "flow/Stats.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreWorker.actor.h"

#include "flow/actorcompiler.h" // has to be last include

extern int restoreStatusIndex;

struct VersionBatch {
	Version beginVersion; // Inclusive
	Version endVersion; // Inclusive if it has log files, exclusive if it has only range file
	std::vector<RestoreFileFR> logFiles;
	std::vector<RestoreFileFR> rangeFiles;

	bool isEmpty() { return logFiles.empty() && rangeFiles.empty(); }
};

struct RestoreMasterData : RestoreRoleData, public ReferenceCounted<RestoreMasterData> {
	// rangeToApplier is in master and loader node. Loader uses this to determine which applier a mutation should be sent.
	//   KeyRef is the inclusive lower bound of the key range the applier (UID) is responsible for
	std::map<Key, UID> rangeToApplier;
	std::map<Version, VersionBatch> versionBatches; // key is the beginVersion of the version batch

	int batchIndex;

	Reference<IBackupContainer> bc; // Backup container is used to read backup files
	Key bcUrl; // The url used to get the bc

	IndexedSet<Key, int64_t> samples; // sample of range and log files
	double samplesSize; // sum of the metric of all samples

	void addref() { return ReferenceCounted<RestoreMasterData>::addref(); }
	void delref() { return ReferenceCounted<RestoreMasterData>::delref(); }

	RestoreMasterData() {
		role = RestoreRole::Master;
		nodeID = UID();
		batchIndex = 1; // starts with 1 because batchId (NotifiedVersion) in loaders and appliers start with 0
	}

	~RestoreMasterData() = default;

	void resetPerVersionBatch() {
		TraceEvent("FastRestore")
		    .detail("RestoreMaster", "ResetPerVersionBatch")
		    .detail("VersionBatchIndex", batchIndex);
		samplesSize = 0;
		samples.clear();
	}

	std::string describeNode() {
		std::stringstream ss;
		ss << "Master versionBatch:" << batchIndex;
		return ss.str();
	}

	// Split allFiles into multiple versionBatches based on files' version
	void buildVersionBatches(const std::vector<RestoreFileFR>& allFiles,
	                         std::map<Version, VersionBatch>* versionBatches) {
		// A version batch includes a log file; Because log file's verion range does not overlap,
		// we use log file's version range as the version range of a version batch.
		Version beginVersion = 0;
		Version maxVersion = 0;
		for (int i = 0; i < allFiles.size(); ++i) {
			if (!allFiles[i].isRange) {
				ASSERT(versionBatches->find(allFiles[i].beginVersion) == versionBatches->end());
				VersionBatch vb;
				vb.beginVersion = beginVersion;
				vb.endVersion = allFiles[i].endVersion;
				versionBatches->insert(std::make_pair(vb.beginVersion, vb));
				//(*versionBatches)[vb.beginVersion] = vb; // Ensure continuous version range across version batches
				beginVersion = allFiles[i].endVersion;
			}
			if (maxVersion < allFiles[i].endVersion) {
				maxVersion = allFiles[i].endVersion;
			}
		}
		// In case there is no log file
		if (versionBatches->empty()) {
			VersionBatch vb;
			vb.beginVersion = 0;
			vb.endVersion = maxVersion + 1; // version batch's endVersion is exclusive
			versionBatches->insert(std::make_pair(vb.beginVersion, vb));
			//(*versionBatches)[vb.beginVersion] = vb; // We ensure the version range are continuous across version batches
		}
		// Put range and log files into its version batch
		for (int i = 0; i < allFiles.size(); ++i) {
			// vbiter's beginVersion > allFiles[i].beginVersion.
			std::map<Version, VersionBatch>::iterator vbIter = versionBatches->upper_bound(allFiles[i].beginVersion);
			--vbIter;
			ASSERT_WE_THINK(vbIter != versionBatches->end());
			if (allFiles[i].isRange) {
				vbIter->second.rangeFiles.push_back(allFiles[i]);
			} else {
				vbIter->second.logFiles.push_back(allFiles[i]);
			}
		}

		// Sort files in each of versionBatches and set fileIndex, which is used in deduplicating mutations sent from
		// loader to applier.
		// Assumption: fileIndex starts at 1. Each loader's initized fileIndex (NotifiedVersion type) starts at 0
		int fileIndex = 0; // fileIndex must be unique; ideally it continuously increase across verstionBatches for
		                   // easier progress tracking
		int versionBatchId = 1;
		for (auto versionBatch = versionBatches->begin(); versionBatch != versionBatches->end(); versionBatch++) {
			std::sort(versionBatch->second.rangeFiles.begin(), versionBatch->second.rangeFiles.end());
			std::sort(versionBatch->second.logFiles.begin(), versionBatch->second.logFiles.end());
			for (auto& logFile : versionBatch->second.logFiles) {
				logFile.fileIndex = ++fileIndex;
				TraceEvent("FastRestore")
				    .detail("VersionBatchId", versionBatchId)
				    .detail("LogFile", logFile.toString());
			}
			for (auto& rangeFile : versionBatch->second.rangeFiles) {
				rangeFile.fileIndex = ++fileIndex;
				TraceEvent("FastRestore")
				    .detail("VersionBatchId", versionBatchId)
				    .detail("RangeFile", rangeFile.toString());
			}
			versionBatchId++;
		}

		TraceEvent("FastRestore").detail("VersionBatches", versionBatches->size());
		// Sanity check
		std::set<uint32_t> fIndexSet;
		for (auto& versionBatch : *versionBatches) {
			Version prevVersion = 0;
			for (auto& logFile : versionBatch.second.logFiles) {
				TraceEvent("FastRestore_Debug")
				    .detail("PrevVersion", prevVersion)
				    .detail("LogFile", logFile.toString());
				ASSERT(logFile.beginVersion >= versionBatch.second.beginVersion);
				ASSERT(logFile.endVersion <= versionBatch.second.endVersion);
				ASSERT(prevVersion <= logFile.beginVersion);
				prevVersion = logFile.endVersion;
				ASSERT(fIndexSet.find(logFile.fileIndex) == fIndexSet.end());
				fIndexSet.insert(logFile.fileIndex);
			}
			prevVersion = 0;
			for (auto& rangeFile : versionBatch.second.rangeFiles) {
				TraceEvent("FastRestore_Debug")
				    .detail("PrevVersion", prevVersion)
				    .detail("RangeFile", rangeFile.toString());
				ASSERT(rangeFile.beginVersion == rangeFile.endVersion);
				ASSERT(rangeFile.beginVersion >= versionBatch.second.beginVersion);
				ASSERT(rangeFile.endVersion < versionBatch.second.endVersion);
				ASSERT(prevVersion <= rangeFile.beginVersion);
				prevVersion = rangeFile.beginVersion;
				ASSERT(fIndexSet.find(rangeFile.fileIndex) == fIndexSet.end());
				fIndexSet.insert(rangeFile.fileIndex);
			}
		}
	}

	void logApplierKeyRange() {
		TraceEvent("FastRestore").detail("ApplierKeyRangeNum", rangeToApplier.size());
		for (auto& applier : rangeToApplier) {
			TraceEvent("FastRestore").detail("KeyRangeLowerBound", applier.first).detail("Applier", applier.second);
		}
	}

	void initBackupContainer(Key url) {
		if (bcUrl == url && bc.isValid()) {
			return;
		}
		printf("initBackupContainer, url:%s\n", url.toString().c_str());
		bcUrl = url;
		bc = IBackupContainer::openContainer(url.toString());
	}
};

ACTOR Future<Void> startRestoreMaster(Reference<RestoreWorkerData> masterWorker, Database cx);

#include "flow/unactorcompiler.h"
#endif