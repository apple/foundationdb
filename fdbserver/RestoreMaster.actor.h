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

#include "flow/actorcompiler.h" // has to be last include

extern double loadBatchSizeThresholdB;
extern int restoreStatusIndex;

struct VersionBatch {
	Version beginVersion; // Inclusive
	Version endVersion; // Exclusive
	std::vector<RestoreFileFR> logFiles;
	std::vector<RestoreFileFR> rangeFiles;
};

struct RestoreMasterData :  RestoreRoleData, public ReferenceCounted<RestoreMasterData> {
	// range2Applier is in master and loader node. Loader node uses this to determine which applier a mutation should be sent
	std::map<Standalone<KeyRef>, UID> range2Applier; // KeyRef is the inclusive lower bound of the key range the applier (UID) is responsible for

	std::map<Version, VersionBatch> versionBatches; // key is the beginVersion of the version batch

	// Temporary variables to hold files and data to restore
	std::vector<RestoreFileFR> allFiles; // All backup files to be processed in all version batches
	std::vector<RestoreFileFR> files; // Backup files to be parsed and applied: range and log files in 1 version batch
	std::map<Version, Version> forbiddenVersions; // forbidden version range [first, second)

	// In each version batch, we process the files in [curBackupFilesBeginIndex, curBackupFilesEndIndex] in RestoreMasterData.allFiles.
	long curBackupFilesBeginIndex;
	long curBackupFilesEndIndex;
	double totalWorkloadSize;
	double curWorkloadSize;
	int batchIndex;

	Reference<IBackupContainer> bc; // Backup container is used to read backup files
	Key bcUrl; // The url used to get the bc

	void addref() { return ReferenceCounted<RestoreMasterData>::addref(); }
	void delref() { return ReferenceCounted<RestoreMasterData>::delref(); }

	void printAllBackupFilesInfo() {
		printf("[INFO] All backup files: num:%ld\n", allFiles.size());
		for (int i = 0; i < allFiles.size(); ++i) {
			printf("\t[INFO][File %d] %s\n", i, allFiles[i].toString().c_str());
		}
	}

	RestoreMasterData() {
		role = RestoreRole::Master;
		nodeID = UID();

		batchIndex = 0;
		curWorkloadSize = 0;
		curBackupFilesBeginIndex = 0;
		curBackupFilesEndIndex = 0;
		totalWorkloadSize = 0;
		curWorkloadSize = 0;
	}

	std::string describeNode() {
		std::stringstream ss;
		ss << "Master versionBatch:"  << batchIndex;
		return ss.str();
	}

	void buildVersionBatches() {
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

	void constructFilesWithVersionRange() {
		printf("[INFO] constructFilesWithVersionRange for num_files:%ld\n", files.size());
		allFiles.clear();
		for (int i = 0; i <  files.size(); i++) {
			printf("\t[File:%d] Start %s\n", i,  files[i].toString().c_str());
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
				printf("\t[File:%d] Log filename:%s, pos:%d\n", i, fileName.c_str(), pos);
				sscanf(fileName.c_str(), "/log,%ld,%ld,%*[^,],%lu%ln", &beginVersion, &endVersion, &blockSize, &len);
				printf("\t[File:%d] Log filename:%s produces beginVersion:%ld endVersion:%ld\n",i, fileName.c_str(), beginVersion, endVersion);
			}
			files[i].beginVersion = beginVersion;
			files[i].endVersion = endVersion;
			printf("\t[File:%d] End %s\n", i,  files[i].toString().c_str());
			ASSERT(beginVersion <= endVersion);
			allFiles.push_back( files[i]);
		}
	}

	void printBackupFilesInfo() {
		printf("[INFO] The backup files for current batch to load and apply: num:%ld\n", files.size());
		for (int i = 0; i < files.size(); ++i) {
			printf("\t[INFO][File %d] %s\n", i, files[i].toString().c_str());
		}
	}

	void buildForbiddenVersionRange() {
		printf("[INFO] Build forbidden version ranges for all backup files: num:%ld\n", allFiles.size());
		for (int i = 0; i < allFiles.size(); ++i) {
			if (!allFiles[i].isRange) {
				forbiddenVersions.insert(std::make_pair(allFiles[i].beginVersion, allFiles[i].endVersion));
			}
		}
	}

	bool isForbiddenVersionRangeOverlapped() {
		printf("[INFO] Check if forbidden version ranges is overlapped: num of ranges:%ld\n", forbiddenVersions.size());
		if (forbiddenVersions.empty()) {
			return false;
		}

		std::map<Version, Version>::iterator prevRange = forbiddenVersions.begin();
		std::map<Version, Version>::iterator curRange = forbiddenVersions.begin();
		curRange++; // Assume forbiddenVersions has at least one element!

		while ( curRange != forbiddenVersions.end() ) {
			if ( curRange->first < prevRange->second ) {
				return true; // overlapped
			}
			curRange++;
		}

		return false; //not overlapped
	}


	void printForbiddenVersionRange() {
		printf("[INFO] Number of forbidden version ranges:%ld\n", forbiddenVersions.size());
		int i = 0;
		for (auto &range : forbiddenVersions) {
			printf("\t[INFO][Range%d] [%ld, %ld)\n", i, range.first, range.second);
			++i;
		}
	}

	// endVersion is begin version for range file, because range file takes snapshot at the same version
	// endVersion is the end version (excluded) for mutations recoselfed in log file
	bool isVersionInForbiddenRange(Version endVersion, bool isRange) {
		bool isForbidden = false;
		for (auto &range : forbiddenVersions) {
			if ( isRange ) { //the range file includes mutations at the endVersion
				if (endVersion >= range.first && endVersion < range.second) {
					isForbidden = true;
					break;
				}
			} else { // the log file does NOT include mutations at the endVersion
				continue; // Log file's endVersion is always a valid version batch boundary as long as the forbidden version ranges do not overlap
			}
		}

		return isForbidden;
	}


	void printAppliersKeyRange() {
		printf("[INFO] The mapping of KeyRange_start --> Applier ID\n");
		// applier type: std::map<Standalone<KeyRef>, UID>
		for (auto &applier : range2Applier) {
			printf("\t[INFO]%s -> %s\n", getHexString(applier.first).c_str(), applier.second.toString().c_str());
		}
	}

	bool isBackupEmpty() {
		for (int i = 0; i < files.size(); ++i) {
			if (files[i].fileSize > 0) {
				return false;
			}
		}
		return true;
	}


	void initBackupContainer(Key url) {
		if ( bcUrl == url && bc.isValid() ) {
			return;
		}
		printf("initBackupContainer, url:%s\n", url.toString().c_str());
		bcUrl = url;
		bc = IBackupContainer::openContainer(url.toString());
		//state BackupDescription desc = wait(self->bc->describeBackup());
		//return Void();
	}

	// Collect the set of backup files to be used for a version batch
	// Return true if there is still files to be restored; false otherwise.
	// This function will change the process' RestoreMasterData
	bool collectFilesForOneVersionBatch() {
		files.clear();
		curWorkloadSize = 0;
		Version endVersion = -1;
		bool isRange = false;
		bool validVersion = false;
		// Step: Find backup files in each version batch and restore them.
		while ( curBackupFilesBeginIndex < allFiles.size() ) {
			// Find the curBackupFilesEndIndex, such that the to-be-loaded files size (curWorkloadSize) is as close to loadBatchSizeThresholdB as possible,
			// and curBackupFilesEndIndex must not belong to the forbidden version range!
			if ( curBackupFilesEndIndex < allFiles.size() ) {
				endVersion =  allFiles[curBackupFilesEndIndex].endVersion;
				isRange = allFiles[curBackupFilesEndIndex].isRange;
				validVersion = !isVersionInForbiddenRange(endVersion, isRange);
				curWorkloadSize  += allFiles[curBackupFilesEndIndex].fileSize;
				printf("[DEBUG][Batch:%d] Calculate backup files for a version batch: endVersion:%lld isRange:%d validVersion:%d curWorkloadSize:%.2fB curBackupFilesBeginIndex:%ld curBackupFilesEndIndex:%ld, files.size:%ld\n",
					batchIndex, (long long) endVersion, isRange, validVersion, curWorkloadSize , curBackupFilesBeginIndex, curBackupFilesEndIndex, allFiles.size());
			}
			if ( (validVersion && curWorkloadSize  >= loadBatchSizeThresholdB) || curBackupFilesEndIndex >= allFiles.size() )  {
				if ( curBackupFilesEndIndex >= allFiles.size() && curWorkloadSize <= 0 ) {
					printf("Restore finishes: curBackupFilesEndIndex:%ld, allFiles.size:%ld, curWorkloadSize:%.2f\n",
							curBackupFilesEndIndex, allFiles.size(), curWorkloadSize );
					//break; // return result
				}
				// Construct the files [curBackupFilesBeginIndex, curBackupFilesEndIndex]
				//resetPerVersionBatch();
				if ( curBackupFilesBeginIndex < allFiles.size()) {
					for (int fileIndex = curBackupFilesBeginIndex; fileIndex <= curBackupFilesEndIndex && fileIndex < allFiles.size(); fileIndex++) {
						files.push_back(allFiles[fileIndex]);
					}
				}
				printBackupFilesInfo();
				totalWorkloadSize += curWorkloadSize;
				break;
			} else if (validVersion && curWorkloadSize < loadBatchSizeThresholdB) {
				curBackupFilesEndIndex++;
			} else if (!validVersion && curWorkloadSize < loadBatchSizeThresholdB) {
				curBackupFilesEndIndex++;
			} else if (!validVersion && curWorkloadSize >= loadBatchSizeThresholdB) {
				// Now: just move to the next file. We will eventually find a valid version but load more than loadBatchSizeThresholdB
				printf("[WARNING] The loading batch size will be larger than expected! curBatchSize:%.2fB, expectedBatchSize:%2.fB, endVersion:%ld\n",
						curWorkloadSize, loadBatchSizeThresholdB, endVersion);
				curBackupFilesEndIndex++;
				// TODO: Roll back to find a valid version
			}
		}

		return (files.size() > 0);
	}
};


ACTOR Future<Void> startRestoreMaster(Reference<RestoreMasterData> self, Database cx);

#include "flow/unactorcompiler.h"
#endif