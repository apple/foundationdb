/*
 * RestoreMaster.actor.cpp
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

// This file implements the functions for RestoreMaster role

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreCommon.actor.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreMaster.actor.h"
#include "fdbserver/RestoreApplier.actor.h"
#include "fdbserver/RestoreLoader.actor.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

ACTOR Future<Standalone<VectorRef<RestoreRequest>>> collectRestoreRequests(Database cx);
ACTOR static Future<Version> processRestoreRequest(RestoreRequest request, Reference<RestoreMasterData> self, Database cx);
ACTOR static Future<Version> processRestoreRequestV2(RestoreRequest request, Reference<RestoreMasterData> self, Database cx);
ACTOR static Future<Void> finishRestore(Reference<RestoreMasterData> self, Database cx, Standalone<VectorRef<RestoreRequest>> restoreRequests);

ACTOR static Future<Void> _collectBackupFiles(Reference<RestoreMasterData> self, Database cx, RestoreRequest request);
ACTOR Future<Void> initializeVersionBatch(Reference<RestoreMasterData> self);
ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, Database cx, RestoreRequest request, Reference<RestoreConfig> restoreConfig);
ACTOR static Future<Void> distributeWorkloadPerVersionBatchV2(Reference<RestoreMasterData> self, Database cx, RestoreRequest request, VersionBatch versionBatch);
ACTOR static Future<Void> _clearDB(Database cx);
ACTOR static Future<Void> registerStatus(Database cx, struct FastRestoreStatus status);
ACTOR Future<Void> notifyAppliersKeyRangeToLoader(Reference<RestoreMasterData> self, Database cx);
ACTOR Future<Void> notifyApplierToApplyMutations(Reference<RestoreMasterData> self);

void dummySampleWorkload(Reference<RestoreMasterData> self);



// The server of the restore master. It drives the restore progress with the following steps:
// 1) Collect interfaces of all RestoreLoader and RestoreApplier roles
// 2) Notify each loader to collect interfaces of all RestoreApplier roles
// 3) Wait on each RestoreRequest, which is sent by RestoreAgent operated by DBA
// 4) Process each restore request in actor processRestoreRequest;
// 5) After process all restore requests, finish restore by cleaning up the restore related system key
//    and ask all restore roles to quit.
ACTOR Future<Void> startRestoreMaster(Reference<RestoreMasterData> self, Database cx) {
	try {
		state int checkNum = 0;
		state UID randomUID = g_random->randomUniqueID();
	
		printf("Node:%s---Wait on restore requests...---\n", self->describeNode().c_str());
		state Standalone<VectorRef<RestoreRequest>> restoreRequests = wait( collectRestoreRequests(cx) );

		// lock DB for restore
		wait(lockDatabase(cx,randomUID));
		wait( _clearDB(cx) );

		printf("Node:%s ---Received  restore requests as follows---\n", self->describeNode().c_str());
		// Step: Perform the restore requests
		for ( auto &it : restoreRequests ) {
			TraceEvent("LeaderGotRestoreRequest").detail("RestoreRequestInfo", it.toString());
			printf("Node:%s Got RestoreRequestInfo:%s\n", self->describeNode().c_str(), it.toString().c_str());
			//Version ver = wait( processRestoreRequest(it, self, cx) );
			Version ver = wait( processRestoreRequestV2(it, self, cx) );
		}

		// Step: Notify all restore requests have been handled by cleaning up the restore keys
		printf("Finish my restore now!\n");
		wait( finishRestore(self, cx, restoreRequests) ); 

		wait(unlockDatabase(cx,randomUID));

		TraceEvent("MXRestoreEndHere");
	} catch (Error &e) {
		fprintf(stdout, "[ERROR] Restoer Master encounters error. error code:%d, error message:%s\n",
				e.code(), e.what());
	}

	return Void();
}


ACTOR static Future<Version> processRestoreRequest(RestoreRequest request, Reference<RestoreMasterData> self, Database cx) {
	//MX: Lock DB if it is not locked
	printf("RestoreRequest lockDB:%d\n", request.lockDB);
	if ( request.lockDB == false ) {
		printf("[WARNING] RestoreRequest lockDB:%d; we will overwrite request.lockDB to true and forcely lock db\n", request.lockDB);
		request.lockDB = true;
		request.lockDB = true;
	}

	state long curBackupFilesBeginIndex = 0;
	state long curBackupFilesEndIndex = 0;

	state double totalWorkloadSize = 0;
	state double totalRunningTime = 0; // seconds
	state double curRunningTime = 0; // seconds
	state double curStartTime = 0;
	state double curEndTime = 0;
	state double curWorkloadSize = 0; //Bytes

	
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state Reference<RestoreConfig> restoreConfig(new RestoreConfig(request.randomUid));

	// // lock DB for restore
	// ASSERT( request.lockDB );
	// wait(lockDatabase(cx, request.randomUid));
	// wait( _clearDB(cx) );

	// Step: Collect all backup files
	printf("===========Restore request start!===========\n");
	state double startTime = now();
	wait( _collectBackupFiles(self, cx, request) );
	printf("[Perf] Node:%s collectBackupFiles takes %.2f seconds\n", self->describeNode().c_str(), now() - startTime);
	self->constructFilesWithVersionRange();
	self->files.clear(); // Ensure no mistakely use self->files
	
	// Sort the backup files based on end version.
	sort(self->allFiles.begin(), self->allFiles.end());
	self->printAllBackupFilesInfo();

	self->buildVersionBatches();

	self->buildForbiddenVersionRange();
	self->printForbiddenVersionRange();
	if ( self->isForbiddenVersionRangeOverlapped() ) {
		fprintf(stderr, "[ERROR] forbidden version ranges are overlapped! Check out the forbidden version range above\n");
	}

	self->batchIndex = 0;
	state int prevBatchIndex = 0;
	state long prevCurBackupFilesBeginIndex = 0;
	state long prevCurBackupFilesEndIndex = 0;
	state double prevCurWorkloadSize = 0;
	state double prevtotalWorkloadSize = 0;


	loop {
		try {
			curStartTime = now();
			self->files.clear();
			self->resetPerVersionBatch();
			// Checkpoint the progress of the previous version batch
			prevBatchIndex = self->batchIndex;
			prevCurBackupFilesBeginIndex = self->curBackupFilesBeginIndex;
			prevCurBackupFilesEndIndex = self->curBackupFilesEndIndex;
			prevCurWorkloadSize = self->curWorkloadSize;
			prevtotalWorkloadSize = self->totalWorkloadSize;
			
			bool hasBackupFilesToProcess = self->collectFilesForOneVersionBatch();
			if ( !hasBackupFilesToProcess ) { // No more backup files to restore
				printf("No backup files to process any more\n");
				break;
			}

			printf("[Progress][Start version batch] Node:%s, restoreBatchIndex:%d, curWorkloadSize:%.2f------\n", self->describeNode().c_str(), self->batchIndex, self->curWorkloadSize);

			wait( initializeVersionBatch(self) );

			wait( distributeWorkloadPerVersionBatch(self, cx, request, restoreConfig) );

			curEndTime = now();
			curRunningTime = curEndTime - curStartTime;
			ASSERT(curRunningTime >= 0);
			totalRunningTime += curRunningTime;

			struct FastRestoreStatus status;
			status.curRunningTime = curRunningTime;
			status.curWorkloadSize = self->curWorkloadSize;
			status.curSpeed = self->curWorkloadSize /  curRunningTime;
			status.totalRunningTime = totalRunningTime;
			status.totalWorkloadSize = self->totalWorkloadSize;
			status.totalSpeed = self->totalWorkloadSize / totalRunningTime;

			printf("[Progress][Finish version batch] restoreBatchIndex:%d, curWorkloadSize:%.2f B, curWorkload:%.2f B curRunningtime:%.2f s curSpeed:%.2f B/s  totalWorkload:%.2f B totalRunningTime:%.2f s totalSpeed:%.2f B/s\n",
					self->batchIndex, self->curWorkloadSize,
					status.curWorkloadSize, status.curRunningTime, status.curSpeed, status.totalWorkloadSize, status.totalRunningTime, status.totalSpeed);

			wait( registerStatus(cx, status) );
			printf("[Progress] Finish 1 version batch. curBackupFilesBeginIndex:%ld curBackupFilesEndIndex:%ld allFiles.size():%ld",
				self->curBackupFilesBeginIndex, self->curBackupFilesEndIndex, self->allFiles.size());

			self->curBackupFilesBeginIndex = self->curBackupFilesEndIndex + 1;
			self->curBackupFilesEndIndex++;
			self->curWorkloadSize = 0;
			self->batchIndex++;

		} catch(Error &e) {
			fprintf(stdout, "!!![MAY HAVE BUG] Reset the version batch state to the start of the current version batch, due to error:%s\n", e.what());
			if(e.code() != error_code_restore_duplicate_tag) {
				wait(tr->onError(e));
			}
			self->batchIndex = prevBatchIndex;
			self->curBackupFilesBeginIndex = prevCurBackupFilesBeginIndex;
			self->curBackupFilesEndIndex = prevCurBackupFilesEndIndex;
			self->curWorkloadSize = prevCurWorkloadSize;
			self->totalWorkloadSize = prevtotalWorkloadSize;
		}
	}

	printf("Finish restore uid:%s \n", request.randomUid.toString().c_str());

	return request.targetVersion;
}


ACTOR static Future<Version> processRestoreRequestV2(RestoreRequest request, Reference<RestoreMasterData> self, Database cx) {
	wait( _collectBackupFiles(self, cx, request) );
	self->constructFilesWithVersionRange();
	self->buildVersionBatches();
	state std::map<Version, VersionBatch>::iterator versionBatch;
	for (versionBatch = self->versionBatches.begin(); versionBatch != self->versionBatches.end(); versionBatch++) {
		wait( initializeVersionBatch(self) );
		wait( distributeWorkloadPerVersionBatchV2(self, cx, request, versionBatch->second) );
	}

	printf("Finish restore uid:%s \n", request.randomUid.toString().c_str());
	return request.targetVersion;
}

enum RestoreFileType { RangeFileType = 0, LogFileType = 1 };
// Distribution workload per version batch
ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, Database cx, RestoreRequest request, Reference<RestoreConfig> restoreConfig) {
	state Key mutationLogPrefix = restoreConfig->mutationLogPrefix();

	if ( self->isBackupEmpty() ) {
		printf("[WARNING] Node:%s distributeWorkloadPerVersionBatch() load an empty batch of backup. Print out the empty backup files info.\n", self->describeNode().c_str());
		self->printBackupFilesInfo();
		return Void();
	}

	printf("[INFO] Node:%s mutationLogPrefix:%s (hex value:%s)\n", self->describeNode().c_str(), mutationLogPrefix.toString().c_str(), getHexString(mutationLogPrefix).c_str());

	// Determine the key range each applier is responsible for
	int numLoaders = self->loadersInterf.size();
	int numAppliers = self->appliersInterf.size();
	ASSERT( numLoaders > 0 );
	ASSERT( numAppliers > 0 );

	state int loadingSizeMB = 0; //numLoaders * 1000; //NOTE: We want to load the entire file in the first version, so we want to make this as large as possible

	state double startTime = now();
	state double startTimeBeforeSampling = now();
	
	dummySampleWorkload(self);

	printf("[Progress] distributeWorkloadPerVersionBatch sampling time:%.2f seconds\n", now() - startTime);
	state double startTimeAfterSampling = now();

	startTime = now();
	wait( notifyAppliersKeyRangeToLoader(self, cx) );
	printf("[Progress] distributeWorkloadPerVersionBatch notifyAppliersKeyRangeToLoader time:%.2f seconds\n", now() - startTime);

	// Determine which backup data block (filename, offset, and length) each loader is responsible for and
	// Prepare the request for each loading request to each loader
	// Send all requests in batch and wait for the ack from loader and repeats
	// NOTE: We must split the workload in the correct boundary:
	// For range file, it's the block boundary;
	// For log file, it is the version boundary. This is because
	// (1) The set of mutations at a version may be encoded in multiple KV pairs in log files.
	// We need to concatenate the related KVs to a big KV before we can parse the value into a vector of mutations at that version
	// (2) The backuped KV are arranged in blocks in range file.
	// For simplicity, we distribute at the granularity of files for now.
	startTime = now();
	state RestoreFileType processedFileType = RestoreFileType::LogFileType; // We should load log file before we do range file
	state int curFileIndex;
	state long curOffset;
	state bool allLoadReqsSent;
	state Version prevVersion;
	
	loop {
		curFileIndex = 0; // The smallest index of the files that has not been FULLY loaded
		curOffset = 0;
		allLoadReqsSent = false;
		prevVersion = 0; // Start version for range or log file is 0
		std::vector<std::pair<UID, RestoreLoadFileRequest>> requests;
		loop {
			if ( allLoadReqsSent ) {
				break; // All load requests have been handled
			}

			printf("[INFO] Number of backup files:%ld curFileIndex:%d\n", self->files.size(), curFileIndex);
			// Future: Load balance the amount of data for loaders
			for (auto &loader : self->loadersInterf) {
				UID loaderID = loader.first;
				RestoreLoaderInterface loaderInterf = loader.second;

				// Skip empty files
				while (  curFileIndex < self->files.size() && self->files[curFileIndex].fileSize == 0 ) {
					printf("[INFO] File %ld:%s filesize:%ld skip the file\n", curFileIndex,
							self->files[curFileIndex].fileName.c_str(), self->files[curFileIndex].fileSize);
					curFileIndex++;
					curOffset = 0;
				}
				// All files under the same type have been loaded
				if ( curFileIndex >= self->files.size() ) {
					allLoadReqsSent = true;
					break;
				}
			
				if ( (processedFileType == RestoreFileType::LogFileType && self->files[curFileIndex].isRange) 
					|| (processedFileType == RestoreFileType::RangeFileType && !self->files[curFileIndex].isRange) ) {
					printf("Skip fileIndex:%d processedFileType:%d file.isRange:%d\n", curFileIndex, processedFileType, self->files[curFileIndex].isRange);
					self->files[curFileIndex].cursor = 0;
					curFileIndex++;
					curOffset = 0;
				} else { // Create the request
					// Prepare loading
					LoadingParam param;
					param.url = request.url;
					param.prevVersion = prevVersion; 
					param.endVersion = self->files[curFileIndex].isRange ? self->files[curFileIndex].version : self->files[curFileIndex].endVersion;
					prevVersion = param.endVersion;
					param.isRangeFile = self->files[curFileIndex].isRange;
					param.version = self->files[curFileIndex].version;
					param.filename = self->files[curFileIndex].fileName;
					param.offset = 0; //curOffset; //self->files[curFileIndex].cursor;
					//param.length = std::min(self->files[curFileIndex].fileSize - curOffset, self->files[curFileIndex].blockSize);
					param.length = self->files[curFileIndex].fileSize; // We load file by file, instead of data block by data block for now
					param.blockSize = self->files[curFileIndex].blockSize;
					param.restoreRange = request.range;
					param.addPrefix = request.addPrefix;
					param.removePrefix = request.removePrefix;
					param.mutationLogPrefix = mutationLogPrefix;
					ASSERT_WE_THINK( param.length > 0 );
					ASSERT_WE_THINK( param.offset >= 0 );
					ASSERT_WE_THINK( param.offset < self->files[curFileIndex].fileSize );
					ASSERT_WE_THINK( param.prevVersion <= param.endVersion );

					requests.push_back( std::make_pair(loader.first, RestoreLoadFileRequest(param)) );
					// Log file to be loaded
					TraceEvent("FastRestore").detail("LoadFileIndex", curFileIndex)
						 .detail("LoadParam", param.toString())
						 .detail("LoaderID", loaderID.toString());
					curFileIndex++;
				}
				
				if ( curFileIndex >= self->files.size() ) {
					allLoadReqsSent = true;
					break;
				}
			}

			if (allLoadReqsSent) {
				printf("[INFO] allLoadReqsSent has finished.\n");
				break;
			}
		}
		// Wait on the batch of load files or log files
		wait( sendBatchRequests(&RestoreLoaderInterface::loadFile, self->loadersInterf, requests) );

		if ( processedFileType ==  RestoreFileType::RangeFileType ) {
			break;
		}
		processedFileType = RestoreFileType::RangeFileType; // The second batch is RangeFile
	}

	printf("[Progress] distributeWorkloadPerVersionBatch loadFiles time:%.2f seconds\n", now() - startTime);
	
	// Notify the applier to applly mutation to DB
	startTime = now();
	wait( notifyApplierToApplyMutations(self) );
	printf("[Progress] distributeWorkloadPerVersionBatch applyToDB time:%.2f seconds\n", now() - startTime);

	state double endTime = now();

	double runningTime = endTime - startTimeBeforeSampling;
	printf("[Progress] Node:%s distributeWorkloadPerVersionBatch runningTime without sampling time:%.2f seconds, with sampling time:%.2f seconds\n",
			self->describeNode().c_str(),
			runningTime, endTime - startTimeAfterSampling);

	return Void();
}

ACTOR static Future<Void> loadFilesOnLoaders(Reference<RestoreMasterData> self, Database cx, RestoreRequest request, VersionBatch versionBatch, bool isRangeFile ) {
	Key mutationLogPrefix;
	std::vector<RestoreFileFR> *files;
	if ( isRangeFile ) {
		files = &versionBatch.rangeFiles;
	} else {
		files = &versionBatch.logFiles;
		Reference<RestoreConfig> restoreConfig(new RestoreConfig(request.randomUid));
		mutationLogPrefix = restoreConfig->mutationLogPrefix();
	}

	std::vector<std::pair<UID, RestoreLoadFileRequest>> requests;
	std::map<UID, RestoreLoaderInterface>::iterator loader = self->loadersInterf.begin();
	Version prevVersion = 0;

	for (auto &file : *files) {
		if (file.fileSize <= 0) {
			continue;
		}
		if ( loader == self->loadersInterf.end() ) {
			loader = self->loadersInterf.begin();
		}
		// Prepare loading
		LoadingParam param;
		param.url = request.url;
		param.prevVersion = prevVersion; 
		param.endVersion = file.isRange ? file.version : file.endVersion;
		prevVersion = param.endVersion;
		param.isRangeFile = file.isRange;
		param.version = file.version;
		param.filename = file.fileName;
		param.offset = 0; //curOffset; //self->files[curFileIndex].cursor;
		//param.length = std::min(self->files[curFileIndex].fileSize - curOffset, self->files[curFileIndex].blockSize);
		param.length = file.fileSize; // We load file by file, instead of data block by data block for now
		param.blockSize = file.blockSize;
		param.restoreRange = request.range;
		param.addPrefix = request.addPrefix;
		param.removePrefix = request.removePrefix;
		param.mutationLogPrefix = mutationLogPrefix;
		ASSERT_WE_THINK( param.length > 0 );
		ASSERT_WE_THINK( param.offset >= 0 );
		ASSERT_WE_THINK( param.offset < file.fileSize );
		ASSERT_WE_THINK( param.prevVersion <= param.endVersion );

		requests.push_back( std::make_pair(loader->first, RestoreLoadFileRequest(param)) );
		// Log file to be loaded
		TraceEvent("FastRestore").detail("LoadParam", param.toString())
				.detail("LoaderID", loader->first.toString());
		loader++;
	}

	// Wait on the batch of load files or log files
	wait( sendBatchRequests(&RestoreLoaderInterface::loadFile, self->loadersInterf, requests) );

	return Void();
}

ACTOR static Future<Void> distributeWorkloadPerVersionBatchV2(Reference<RestoreMasterData> self, Database cx, RestoreRequest request, VersionBatch versionBatch) {
	if ( self->isBackupEmpty() ) {
		printf("[WARNING] Node:%s distributeWorkloadPerVersionBatch() load an empty batch of backup. Print out the empty backup files info.\n", self->describeNode().c_str());
		self->printBackupFilesInfo();
		return Void();
	}

	ASSERT( self->loadersInterf.size() > 0 );
	ASSERT( self->appliersInterf.size() > 0 );
	
	dummySampleWorkload(self);

	wait( notifyAppliersKeyRangeToLoader(self, cx) );

	// Parse log files and send mutations to appliers before we parse range files
	wait( loadFilesOnLoaders(self, cx, request, versionBatch, false) );
	wait( loadFilesOnLoaders(self, cx, request, versionBatch, true) );
	
	wait( notifyApplierToApplyMutations(self) );

	return Void();
}


// Placehold for sample workload
// Produce the key-range for each applier
void dummySampleWorkload(Reference<RestoreMasterData> self) {
	int numAppliers = self->appliersInterf.size();
	std::vector<UID> keyrangeSplitter;
	// We will use the splitter at [1, numAppliers - 1]. The first splitter is normalKeys.begin
	int i;
	for (i = 0; i < numAppliers - 1; i++) {
		keyrangeSplitter.push_back(g_random->randomUniqueID());
	}
	std::sort( keyrangeSplitter.begin(), keyrangeSplitter.end() );
	i = 0;
	for (auto& applier : self->appliersInterf) {
		if ( i == 0 ) {
			self->range2Applier[normalKeys.begin] = applier.first;
		} else {
			self->range2Applier[StringRef(keyrangeSplitter[i].toString())] = applier.first;
		}
	}
}

ACTOR Future<Standalone<VectorRef<RestoreRequest>>> collectRestoreRequests(Database cx) {
	state int restoreId = 0;
	state int checkNum = 0;
	state Standalone<VectorRef<RestoreRequest>> restoreRequests;
	state Future<Void> watch4RestoreRequest;

	//wait for the restoreRequestTriggerKey to be set by the client/test workload
	state ReadYourWritesTransaction tr(cx);
	loop{
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			state Optional<Value> numRequests = wait(tr.get(restoreRequestTriggerKey));
			if ( !numRequests.present() ) {
				watch4RestoreRequest = tr.watch(restoreRequestTriggerKey);
				wait(tr.commit());
				wait( watch4RestoreRequest );
			} else {
				int num = decodeRestoreRequestTriggerValue(numRequests.get());
				//TraceEvent("RestoreRequestKey").detail("NumRequests", num);
				printf("[INFO] RestoreRequestNum:%d\n", num);

				state Standalone<RangeResultRef> restoreRequestValues = wait(tr.getRange(restoreRequestKeys, CLIENT_KNOBS->TOO_MANY));
				printf("Restore worker get restoreRequest: %s\n", restoreRequestValues.toString().c_str());

				ASSERT(!restoreRequestValues.more);

				if(restoreRequestValues.size()) {
					for ( auto &it : restoreRequestValues ) {
						printf("Now decode restore request value...\n");
						restoreRequests.push_back(restoreRequests.arena(), decodeRestoreRequestValue(it.value));
					}
				}
				break;
			}
		} catch(Error &e) {
			wait(tr.onError(e));
		}
	}

	return restoreRequests;
}

// NOTE: This function can now get the backup file descriptors
ACTOR static Future<Void> _collectBackupFiles(Reference<RestoreMasterData> self, Database cx, RestoreRequest request) {
	state Key tagName = request.tagName;
	state Key url = request.url;
	state bool waitForComplete = request.waitForComplete;
	state Version targetVersion = request.targetVersion;
	state bool verbose = request.verbose;
	state KeyRange range = request.range;
	state Key addPrefix = request.addPrefix;
	state Key removePrefix = request.removePrefix;
	state bool lockDB = request.lockDB;
	state UID randomUid = request.randomUid;

	//ASSERT( lockDB == true );

	self->initBackupContainer(url);
	state BackupDescription desc = wait(self->bc->describeBackup());

	wait(desc.resolveVersionTimes(cx));

	printf("[INFO] Backup Description\n%s", desc.toString().c_str());
	printf("[INFO] Restore for url:%s, lockDB:%d\n", url.toString().c_str(), lockDB);
	if(targetVersion == invalidVersion && desc.maxRestorableVersion.present())
		targetVersion = desc.maxRestorableVersion.get();

	printf("[INFO] collectBackupFiles: now getting backup files for restore request: %s\n", request.toString().c_str());
	Optional<RestorableFileSet> restorable = wait(self->bc->getRestoreSet(targetVersion));

	if(!restorable.present()) {
		printf("[WARNING] restoreVersion:%ld (%lx) is not restorable!\n", targetVersion, targetVersion);
		throw restore_missing_data();
	}

	if (!self->files.empty()) {
		printf("[WARNING] global files are not empty! files.size() is %ld. We forcely clear files\n", self->files.size());
		self->files.clear();
	}

	printf("[INFO] Found backup files: num of files:%ld\n", self->files.size());
 	for(const RangeFile &f : restorable.get().ranges) {
 		TraceEvent("FoundRangeFileMX").detail("FileInfo", f.toString());
 		printf("[INFO] FoundRangeFile, fileInfo:%s\n", f.toString().c_str());
		RestoreFileFR file(f.version, f.fileName, true, f.blockSize, f.fileSize, f.version, f.version);
 		self->files.push_back(file);
 	}
 	for(const LogFile &f : restorable.get().logs) {
 		TraceEvent("FoundLogFileMX").detail("FileInfo", f.toString());
		printf("[INFO] FoundLogFile, fileInfo:%s\n", f.toString().c_str());
		RestoreFileFR file(f.beginVersion, f.fileName, false, f.blockSize, f.fileSize, f.endVersion, f.beginVersion);
		self->files.push_back(file);
 	}

	printf("[INFO] Restoring backup to version: %lld\n", (long long) targetVersion);

	return Void();
}

ACTOR static Future<Void> _clearDB(Database cx) {
	wait( runRYWTransaction( cx, [](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->clear(normalKeys);
			return Void();
  		}) );

	return Void();
}


ACTOR Future<Void> initializeVersionBatch(Reference<RestoreMasterData> self) {

	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;
	for (auto &applier : self->appliersInterf) {
		requests.push_back( std::make_pair(applier.first, RestoreVersionBatchRequest(self->batchIndex)) );
	}
	wait( sendBatchRequests(&RestoreApplierInterface::initVersionBatch, self->appliersInterf, requests) );

	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;
	for (auto &loader : self->loadersInterf) {
		requests.push_back( std::make_pair(loader.first, RestoreVersionBatchRequest(self->batchIndex)) );
	}
	wait( sendBatchRequests(&RestoreLoaderInterface::initVersionBatch, self->loadersInterf, requests) );

	return Void();
}


ACTOR Future<Void> notifyApplierToApplyMutations(Reference<RestoreMasterData> self) {
	// Prepare the applyToDB requests
	std::vector<std::pair<UID, RestoreSimpleRequest>> requests;
	for (auto& applier : self->appliersInterf) {
		requests.push_back( std::make_pair(applier.first, RestoreSimpleRequest()) );
	}
	wait( sendBatchRequests(&RestoreApplierInterface::applyToDB, self->appliersInterf, requests) );

	return Void();
}

// Restore Master: Notify loader about appliers' responsible key range
ACTOR Future<Void> notifyAppliersKeyRangeToLoader(Reference<RestoreMasterData> self, Database cx)  {
	std::vector<std::pair<UID, RestoreSetApplierKeyRangeVectorRequest>> requests;
	for (auto& loader : self->loadersInterf) {
		requests.push_back(std::make_pair(loader.first, RestoreSetApplierKeyRangeVectorRequest(self->range2Applier)) );
	}

	wait( sendBatchRequests(&RestoreLoaderInterface::setApplierKeyRangeVectorRequest, self->loadersInterf, requests) );

	return Void();
}


ACTOR static Future<Void> finishRestore(Reference<RestoreMasterData> self, Database cx, Standalone<VectorRef<RestoreRequest>> restoreRequests) {
	std::vector<std::pair<UID, RestoreSimpleRequest>> requests;
	for ( auto &loader : self->loadersInterf ) {
		requests.push_back( std::make_pair(loader.first, RestoreSimpleRequest()) );
	}
	wait( sendBatchRequests(&RestoreLoaderInterface::finishRestore, self->loadersInterf, requests) );

	std::vector<std::pair<UID, RestoreSimpleRequest>> requests;
	for ( auto &applier : self->appliersInterf ) {
		requests.push_back( std::make_pair(applier.first, RestoreSimpleRequest()) );
	}
	wait( sendBatchRequests(&RestoreApplierInterface::finishRestore, self->appliersInterf, requests) );

	// Notify tester that the restore has finished
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->clear(restoreRequestTriggerKey);
			tr->clear(restoreRequestKeys);
			Version readVersion = wait(tr->getReadVersion());
			tr->set(restoreRequestDoneKey, restoreRequestDoneVersionValue(readVersion));
			wait( tr->commit() );
			break;
		}  catch( Error &e ) {
			wait(tr->onError(e));
		}
	}

	TraceEvent("FastRestore").detail("RestoreRequestsSize", restoreRequests.size());

	return Void();
}

ACTOR static Future<Void> registerStatus(Database cx, struct FastRestoreStatus status) {
 	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	loop {
		try {
			printf("[Restore_Status][%d] curWorkload:%.2f curRunningtime:%.2f curSpeed:%.2f totalWorkload:%.2f totalRunningTime:%.2f totalSpeed:%.2f\n",
					restoreStatusIndex, status.curWorkloadSize, status.curRunningTime, status.curSpeed, status.totalWorkloadSize, status.totalRunningTime, status.totalSpeed);

			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			tr->set(restoreStatusKeyFor(StringRef(std::string("curWorkload") + std::to_string(restoreStatusIndex))), restoreStatusValue(status.curWorkloadSize));
			tr->set(restoreStatusKeyFor(StringRef(std::string("curRunningTime") + std::to_string(restoreStatusIndex))), restoreStatusValue(status.curRunningTime));
			tr->set(restoreStatusKeyFor(StringRef(std::string("curSpeed") + std::to_string(restoreStatusIndex))), restoreStatusValue(status.curSpeed));

			tr->set(restoreStatusKeyFor(StringRef(std::string("totalWorkload"))), restoreStatusValue(status.totalWorkloadSize));
			tr->set(restoreStatusKeyFor(StringRef(std::string("totalRunningTime"))), restoreStatusValue(status.totalRunningTime));
			tr->set(restoreStatusKeyFor(StringRef(std::string("totalSpeed"))), restoreStatusValue(status.totalSpeed));

			wait( tr->commit() );
			restoreStatusIndex++;

			break;
		} catch( Error &e ) {
			wait(tr->onError(e));
		}
	 };

	return Void();
}