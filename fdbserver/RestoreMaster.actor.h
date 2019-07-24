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
	Version endVersion; // Exclusive
	std::vector<RestoreFileFR> logFiles;
	std::vector<RestoreFileFR> rangeFiles;

	bool isEmpty() {
		return logFiles.empty() && rangeFiles.empty();
	}
};

struct RestoreMasterData :  RestoreRoleData, public ReferenceCounted<RestoreMasterData> {
	// range2Applier is in master and loader node. Loader node uses this to determine which applier a mutation should be sent
	std::map<Standalone<KeyRef>, UID> range2Applier; // KeyRef is the inclusive lower bound of the key range the applier (UID) is responsible for
	std::map<Version, VersionBatch> versionBatches; // key is the beginVersion of the version batch

	int batchIndex;

	Reference<IBackupContainer> bc; // Backup container is used to read backup files
	Key bcUrl; // The url used to get the bc

	void addref() { return ReferenceCounted<RestoreMasterData>::addref(); }
	void delref() { return ReferenceCounted<RestoreMasterData>::delref(); }

	RestoreMasterData() {
		role = RestoreRole::Master;
		nodeID = UID();
		batchIndex = 0;
	}

	std::string describeNode() {
		std::stringstream ss;
		ss << "Master versionBatch:"  << batchIndex;
		return ss.str();
	}

	// Split allFiles into multiple versionBatches based on files' version
	void buildVersionBatches(const std::vector<RestoreFileFR>& allFiles, std::map<Version, VersionBatch>& versionBatches) {
		// A version batch includes a log file 
		// Because log file's verion range does not overlap, we use log file's version range as the version range of a version batch
		// Create a version batch for a log file
		Version beginVersion = 0;
		Version maxVersion = 0;
		for ( int i = 0; i < allFiles.size(); ++i ) {
			if ( !allFiles[i].isRange ) {
				ASSERT( versionBatches.find(allFiles[i].beginVersion) ==  versionBatches.end() );
				VersionBatch vb;
				vb.beginVersion = beginVersion;
				vb.endVersion = allFiles[i].endVersion;
				versionBatches[vb.beginVersion] = vb; // We ensure the version range are continuous across version batches
				beginVersion = allFiles[i].endVersion;
			}
			if ( maxVersion < allFiles[i].endVersion ) {
				maxVersion = allFiles[i].endVersion;
			}
		}
		// In case there is no log file
		if ( versionBatches.empty() ) {
			VersionBatch vb;
			vb.beginVersion = 0;
			vb.endVersion = maxVersion + 1; // version batch's endVersion is exclusive
			versionBatches[vb.beginVersion] = vb; // We ensure the version range are continuous across version batches
		}
		// Put range and log files into its version batch
		for ( int i = 0; i < allFiles.size(); ++i ) {
			std::map<Version, VersionBatch>::iterator vbIter = versionBatches.upper_bound(allFiles[i].beginVersion); // vbiter's beginVersion > allFiles[i].beginVersion
			--vbIter;
			ASSERT_WE_THINK( vbIter != versionBatches.end() );
			if ( allFiles[i].isRange ) {
				vbIter->second.rangeFiles.push_back(allFiles[i]);	
			} else {
				vbIter->second.logFiles.push_back(allFiles[i]);
			}
		}
		printf("versionBatches.size:%d\n", versionBatches.size());
		// Sanity check
		for (auto &versionBatch : versionBatches) {
			for ( auto &logFile : versionBatch.second.logFiles ) {
				ASSERT(logFile.beginVersion >= versionBatch.second.beginVersion);
				ASSERT(logFile.endVersion <= versionBatch.second.endVersion);
			}
			for ( auto &rangeFile : versionBatch.second.rangeFiles ) {
				ASSERT(rangeFile.beginVersion == rangeFile.endVersion);
				ASSERT(rangeFile.beginVersion >= versionBatch.second.beginVersion);
				ASSERT(rangeFile.endVersion < versionBatch.second.endVersion);
			}
		}
	}

	// Parse file's name to get beginVersion and endVersion of the file; and assign files to allFiles
	void constructFilesWithVersionRange(std::vector<RestoreFileFR> &files, std::vector<RestoreFileFR>& allFiles) {
		printf("[INFO] constructFilesWithVersionRange for num_files:%ld\n", files.size());
		allFiles.clear();
		for (int i = 0; i <  files.size(); i++) {
			Version beginVersion = 0;
			Version endVersion = 0;
			if ( files[i].isRange) {
				// No need to parse range filename to get endVersion
				beginVersion =  files[i].version;
				endVersion = beginVersion;
			} else { // Log file
				//Refer to pathToLogFile() in BackupContainer.actor.cpp
				long blockSize, len;
				int pos =  files[i].fileName.find_last_of("/");
				std::string fileName =  files[i].fileName.substr(pos);
				//printf("\t[File:%d] Log filename:%s, pos:%d\n", i, fileName.c_str(), pos);
				sscanf(fileName.c_str(), "/log,%ld,%ld,%*[^,],%lu%ln", &beginVersion, &endVersion, &blockSize, &len);
				//printf("\t[File:%d] Log filename:%s produces beginVersion:%ld endVersion:%ld\n",i, fileName.c_str(), beginVersion, endVersion);
			}
			files[i].beginVersion = beginVersion;
			files[i].endVersion = endVersion;
			ASSERT(beginVersion <= endVersion);
			allFiles.push_back( files[i]);
		}
	}

	void logApplierKeyRange() {
		TraceEvent("FastRestore").detail("ApplierKeyRangeNum", range2Applier.size());
		for (auto &applier : range2Applier) {
			TraceEvent("FastRestore").detail("KeyRangeLowerBound", applier.first).detail("Applier", applier.second);
		}
	}

	void initBackupContainer(Key url) {
		if ( bcUrl == url && bc.isValid() ) {
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