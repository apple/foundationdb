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
ACTOR static Future<Void> finishRestore(Reference<RestoreMasterData> self, Database cx, Standalone<VectorRef<RestoreRequest>> restoreRequests);

ACTOR static Future<Void> _collectBackupFiles(Reference<RestoreMasterData> self, Database cx, RestoreRequest request);
ACTOR Future<Void> initializeVersionBatch(Reference<RestoreMasterData> self);
ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, Database cx, RestoreRequest request, Reference<RestoreConfig> restoreConfig);
ACTOR static Future<Void> unlockDB(Database cx, UID uid);
ACTOR static Future<Void> _clearDB(Reference<ReadYourWritesTransaction> tr);
ACTOR static Future<Void> _lockDB(Database cx, UID uid, bool lockDB);
ACTOR static Future<Void> registerStatus(Database cx, struct FastRestoreStatus status);
ACTOR static Future<Void> sampleWorkload(Reference<RestoreMasterData> self, RestoreRequest request, Reference<RestoreConfig> restoreConfig, int64_t sampleMB_input);
ACTOR Future<Void> notifyAppliersKeyRangeToLoader(Reference<RestoreMasterData> self, Database cx);
ACTOR Future<Void> assignKeyRangeToAppliers(Reference<RestoreMasterData> self, Database cx);
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
		state int restoreId = 0;
		state int checkNum = 0;
		loop {
			printf("Node:%s---Wait on restore requests...---\n", self->describeNode().c_str());
			state Standalone<VectorRef<RestoreRequest>> restoreRequests = wait( collectRestoreRequests(cx) );

			printf("Node:%s ---Received  restore requests as follows---\n", self->describeNode().c_str());
			// Print out the requests info
			for ( auto &it : restoreRequests ) {
				printf("\t[INFO][Master]Node:%s RestoreRequest info:%s\n", self->describeNode().c_str(), it.toString().c_str());
			}

			// Step: Perform the restore requests
			for ( auto &it : restoreRequests ) {
				TraceEvent("LeaderGotRestoreRequest").detail("RestoreRequestInfo", it.toString());
				printf("Node:%s Got RestoreRequestInfo:%s\n", self->describeNode().c_str(), it.toString().c_str());
				Version ver = wait( processRestoreRequest(it, self, cx) );
			}

			// Step: Notify all restore requests have been handled by cleaning up the restore keys
			wait( delay(5.0) );
			printf("Finish my restore now!\n");
			wait( finishRestore(self, cx, restoreRequests) ); 

			printf("[INFO] MXRestoreEndHere RestoreID:%d\n", restoreId);
			TraceEvent("MXRestoreEndHere").detail("RestoreID", restoreId++);
			//NOTE: we have to break the loop so that the tester.actor can receive the return of this test workload.
			//Otherwise, this special workload never returns and tester will think the test workload is stuck and the tester will timesout
			break;
		}
	} catch (Error &e) {
		fprintf(stdout, "[ERROR] Restoer Master encounters error. error code:%d, error message:%s\n",
				e.code(), e.what());
	}

	return Void();
}


ACTOR static Future<Version> processRestoreRequest(RestoreRequest request, Reference<RestoreMasterData> self, Database cx) {
	// state Key tagName = request.tagName;
	// state Key url = request.url;
	// state bool waitForComplete = request.waitForComplete;
	// state Version targetVersion = request.targetVersion;
	// state bool verbose = request.verbose;
	// state KeyRange range = request.range;
	// state Key addPrefix = request.addPrefix;
	// state Key removePrefix = request.removePrefix;
	// state bool lockDB = request.lockDB;
	// state UID randomUid = request.randomUid;

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

	// lock DB for restore
	wait( _lockDB(cx, request.randomUid, request.lockDB) );
	wait( _clearDB(tr) );

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
			self->cmdID.setBatch(self->batchIndex);
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

			// wait( delay(1.0) );

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

	// Unlock DB  at the end of handling the restore request
	wait( unlockDB(cx, request.randomUid) );
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
	int64_t sampleSizeMB = 0; //loadingSizeMB / 100; // Will be overwritten. The sampleSizeMB will be calculated based on the batch size

	state double startTime = now();
	state double startTimeBeforeSampling = now();
	
	dummySampleWorkload(self);
	//wait( sampleWorkload(self, request, restoreConfig, sampleSizeMB) );
	// wait( delay(1.0) );

	printf("[Progress] distributeWorkloadPerVersionBatch sampling time:%.2f seconds\n", now() - startTime);
	state double startTimeAfterSampling = now();

	// Notify each applier about the key range it is responsible for, and notify appliers to be ready to receive data
	startTime = now();
	wait( assignKeyRangeToAppliers(self, cx) );
	// wait( delay(1.0) );
	printf("[Progress] distributeWorkloadPerVersionBatch assignKeyRangeToAppliers time:%.2f seconds\n", now() - startTime);

	startTime = now();
	wait( notifyAppliersKeyRangeToLoader(self, cx) );
	// wait( delay(1.0) );
	printf("[Progress] distributeWorkloadPerVersionBatch notifyAppliersKeyRangeToLoader time:%.2f seconds\n", now() - startTime);

	// Determine which backup data block (filename, offset, and length) each loader is responsible for and
	// Notify the loader about the data block and send the cmd to the loader to start loading the data
	// Wait for the ack from loader and repeats

	// Prepare the file's loading status
	for (int i = 0; i < self->files.size(); ++i) {
		self->files[i].cursor = 0;
	}

	// Send loading cmd to available loaders whenever loaders become available
	// NOTE: We must split the workload in the correct boundary:
	// For range file, it's the block boundary;
	// For log file, it is the version boundary.
	// This is because
	// (1) The set of mutations at a version may be encoded in multiple KV pairs in log files.
	// We need to concatenate the related KVs to a big KV before we can parse the value into a vector of mutations at that version
	// (2) The backuped KV are arranged in blocks in range file.
	// For simplicity, we distribute at the granularity of files for now.

	state int loadSizeB = loadingSizeMB * 1024 * 1024;
	state int loadingCmdIndex = 0;

	startTime = now();
	// We should load log file before we do range file
	state int typeOfFilesProcessed = 0;
	state RestoreFileType processedFileType = RestoreFileType::LogFileType;
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

			printf("[INFO] Number of backup files:%ld\n", self->files.size());
			for (auto &loader : self->loadersInterf) {
				UID loaderID = loader.first;
				RestoreLoaderInterface loaderInterf = loader.second;

				while (  curFileIndex < self->files.size() && self->files[curFileIndex].fileSize == 0 ) {
					// NOTE: && self->files[curFileIndex].cursor >= self->files[curFileIndex].fileSize
					printf("[INFO] File %ld:%s filesize:%ld skip the file\n", curFileIndex,
							self->files[curFileIndex].fileName.c_str(), self->files[curFileIndex].fileSize);
					curFileIndex++;
					curOffset = 0;
				}
				if ( curFileIndex >= self->files.size() ) {
					allLoadReqsSent = true;
					break;
				}
				LoadingParam param;
				param.url = request.url;
				param.version = self->files[curFileIndex].version;
				param.filename = self->files[curFileIndex].fileName;
				param.offset = 0; //curOffset; //self->files[curFileIndex].cursor;
				//param.length = std::min(self->files[curFileIndex].fileSize - curOffset, self->files[curFileIndex].blockSize);
				//param.cursor = 0;
				param.length = self->files[curFileIndex].fileSize;
				loadSizeB = param.length;
				param.blockSize = self->files[curFileIndex].blockSize;
				param.restoreRange = request.range;
				param.addPrefix = request.addPrefix;
				param.removePrefix = request.removePrefix;
				param.mutationLogPrefix = mutationLogPrefix;
				param.isRangeFile = self->files[curFileIndex].isRange;
				
				if ( !(param.length > 0  &&  param.offset >= 0 && param.offset < self->files[curFileIndex].fileSize) ) {
					printf("[ERROR] param: length:%ld offset:%ld fileSize:%ld for %ldth filename:%s\n",
							param.length, param.offset, self->files[curFileIndex].fileSize, curFileIndex,
							self->files[curFileIndex].fileName.c_str());
				}
				ASSERT( param.length > 0 );
				ASSERT( param.offset >= 0 );
				ASSERT( param.offset < self->files[curFileIndex].fileSize );

				if ( (processedFileType == RestoreFileType::LogFileType && self->files[curFileIndex].isRange) 
					|| (processedFileType == RestoreFileType::RangeFileType && !self->files[curFileIndex].isRange) ) {
					printf("Skip fileIndex:%d processedFileType:%d file.isRange:%d\n", curFileIndex, processedFileType, self->files[curFileIndex].isRange);
					self->files[curFileIndex].cursor = 0;
					curFileIndex++;
					curOffset = 0;
				} else { // Create the request
					param.prevVersion = prevVersion; 
					prevVersion = self->files[curFileIndex].isRange ? self->files[curFileIndex].version : self->files[curFileIndex].endVersion;
					param.endVersion = prevVersion;
					requests.push_back( std::make_pair(loader.first, RestoreLoadFileRequest(self->cmdID, param)) );
					printf("[CMD] Loading fileIndex:%ld fileInfo:%s loadingParam:%s on node %s\n",
						curFileIndex, self->files[curFileIndex].toString().c_str(), 
						param.toString().c_str(), loaderID.toString().c_str()); // VERY USEFUL INFO
					printf("[INFO] Node:%s CMDUID:%s isRange:%d loaderNode:%s\n", self->describeNode().c_str(), self->cmdID.toString().c_str(),
							(int) self->files[curFileIndex].isRange, loaderID.toString().c_str());
					//curOffset += param.length;

					// Reach the end of the file
					if ( param.length + param.offset >= self->files[curFileIndex].fileSize ) {
						curFileIndex++;
						curOffset = 0;
					}
				}
				
				if ( curFileIndex >= self->files.size() ) {
					allLoadReqsSent = true;
					break;
				}
			}

			if (allLoadReqsSent) {
				printf("[INFO] allLoadReqsSent has finished.\n");
				break; // NOTE: need to change when change to wait on any cmdReplies
			}
		}
		// Wait on the batch of load files or log files
		++typeOfFilesProcessed;
		wait( sendBatchRequests(&RestoreLoaderInterface::loadFile, self->loadersInterf, requests) );

		processedFileType = RestoreFileType::RangeFileType; // The second batch is RangeFile

		if ( typeOfFilesProcessed == 2 ) { // We only have 2 types of files
			break;
		}
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

// TODO: Revise the way to collect the restore request. We may make it into 1 transaction
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
			printf("[WARNING] Transaction for restore request in watch restoreRequestTriggerKey. Error:%s\n", e.name());
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

	ASSERT( lockDB == true );

	self->initBackupContainer(url);

	state Reference<IBackupContainer> bc = self->bc;
	state BackupDescription desc = wait(bc->describeBackup());

	wait(desc.resolveVersionTimes(cx));

	printf("[INFO] Backup Description\n%s", desc.toString().c_str());
	printf("[INFO] Restore for url:%s, lockDB:%d\n", url.toString().c_str(), lockDB);
	if(targetVersion == invalidVersion && desc.maxRestorableVersion.present())
		targetVersion = desc.maxRestorableVersion.get();

	printf("[INFO] collectBackupFiles: now getting backup files for restore request: %s\n", request.toString().c_str());
	Optional<RestorableFileSet> restorable = wait(bc->getRestoreSet(targetVersion));

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


ACTOR static Future<Void> _lockDB(Database cx, UID uid, bool lockDB) {
	printf("[Lock] DB will be locked, uid:%s, lockDB:%d\n", uid.toString().c_str(), lockDB);
	
	ASSERT( lockDB );

	loop {
		try {
			wait(lockDatabase(cx, uid));
			break;
		} catch( Error &e ) {
			printf("Transaction Error when we lockDB. Error:%s\n", e.what());
			wait(tr->onError(e));
		}
	}

	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			wait(checkDatabaseLock(tr, uid));

			tr->commit();
			break;
		} catch( Error &e ) {
			printf("Transaction Error when we lockDB. Error:%s\n", e.what());
			wait(tr->onError(e));
		}
	}


	return Void();
}

ACTOR static Future<Void> _clearDB(Reference<ReadYourWritesTransaction> tr) {
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->clear(normalKeys);
			tr->commit();
			break;
		} catch(Error &e) {
			printf("Retry at clean up DB before restore. error code:%d message:%s. Retry...\n", e.code(), e.what());
			if(e.code() != error_code_restore_duplicate_tag) {
				wait(tr->onError(e));
			}
		}
	}

	return Void();
}


ACTOR Future<Void> initializeVersionBatch(Reference<RestoreMasterData> self) {
	self->cmdID.initPhase(RestoreCommandEnum::Reset_VersionBatch);

	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;
	for (auto &applier : self->appliersInterf) {
		self->cmdID.nextCmd();
		requests.push_back( std::make_pair(applier.first, RestoreVersionBatchRequest(self->cmdID, self->batchIndex)) );
	}
	wait( sendBatchRequests(&RestoreApplierInterface::initVersionBatch, self->appliersInterf, requests) );

	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;
	for (auto &loader : self->loadersInterf) {
		self->cmdID.nextCmd();
		requests.push_back( std::make_pair(loader.first, RestoreVersionBatchRequest(self->cmdID, self->batchIndex)) );
	}
	wait( sendBatchRequests(&RestoreLoaderInterface::initVersionBatch, self->loadersInterf, requests) );

	return Void();
}


ACTOR Future<Void> notifyApplierToApplyMutations(Reference<RestoreMasterData> self) {
	loop {
		try {
			self->cmdID.initPhase( RestoreCommandEnum::Apply_Mutation_To_DB );
			// Prepare the applyToDB requests
			std::vector<std::pair<UID, RestoreSimpleRequest>> requests;
			for (auto& applier : self->appliersInterf) {
				self->cmdID.nextCmd();
				requests.push_back( std::make_pair(applier.first, RestoreSimpleRequest(self->cmdID)) );
			}
			wait( sendBatchRequests(&RestoreApplierInterface::applyToDB, self->appliersInterf, requests) );

			break;
		} catch (Error &e) {
			fprintf(stdout, "[ERROR] Node:%s, Commands before cmdID:%s error. error code:%d, error message:%s\n", self->describeNode().c_str(),
					self->cmdID.toString().c_str(), e.code(), e.what());
		}
	}

	return Void();
}



ACTOR Future<Void> assignKeyRangeToAppliers(Reference<RestoreMasterData> self, Database cx)  { //, VectorRef<RestoreWorkerInterface> ret_agents
	//construct the key range for each applier
	std::vector<KeyRef> lowerBounds;
	std::vector<Standalone<KeyRangeRef>> keyRanges;
	std::vector<UID> applierIDs;

	// printf("[INFO] Node:%s, Assign key range to appliers. num_appliers:%ld\n", self->describeNode().c_str(), self->range2Applier.size());
	for (auto& applier : self->range2Applier) {
		lowerBounds.push_back(applier.first);
		applierIDs.push_back(applier.second);
		// printf("\t[INFO] ApplierID:%s lowerBound:%s\n",
		// 		applierIDs.back().toString().c_str(),
		// 		lowerBounds.back().toString().c_str());
	}
	for (int i  = 0; i < lowerBounds.size(); ++i) {
		KeyRef startKey = lowerBounds[i];
		KeyRef endKey;
		if ( i < lowerBounds.size() - 1) {
			endKey = lowerBounds[i+1];
		} else {
			endKey = normalKeys.end;
		}

		if (startKey > endKey) {
			fprintf(stderr, "ERROR at assignKeyRangeToAppliers, startKey:%s > endKey:%s\n", startKey.toString().c_str(), endKey.toString().c_str());
		}

		keyRanges.push_back(KeyRangeRef(startKey, endKey));
	}

	ASSERT( applierIDs.size() == keyRanges.size() );
	state std::map<UID, Standalone<KeyRangeRef>> appliers;
	appliers.clear(); // If this function is called more than once in multiple version batches, appliers may carry over the data from earlier version batch
	for (int i = 0; i < applierIDs.size(); ++i) {
		if (appliers.find(applierIDs[i]) != appliers.end()) {
			printf("[ERROR] ApplierID appear more than once. appliers size:%ld applierID: %s\n",
					appliers.size(), applierIDs[i].toString().c_str());
			printApplierKeyRangeInfo(appliers);
		}
		ASSERT( appliers.find(applierIDs[i]) == appliers.end() ); // we should not have a duplicate applierID respoinsbile for multiple key ranges
		appliers.insert(std::make_pair(applierIDs[i], keyRanges[i]));
	}

	state std::vector<Future<RestoreCommonReply>> cmdReplies;
	loop {
		try {
			cmdReplies.clear();
			self->cmdID.initPhase(RestoreCommandEnum::Assign_Applier_KeyRange);
			for (auto& applier : appliers) {
				KeyRangeRef keyRange = applier.second;
				UID applierID = applier.first;
				printf("[CMD] Node:%s, Assign KeyRange:%s [begin:%s end:%s] to applier ID:%s\n", self->describeNode().c_str(),
						keyRange.toString().c_str(),
						getHexString(keyRange.begin).c_str(), getHexString(keyRange.end).c_str(),
						applierID.toString().c_str());

				ASSERT( self->appliersInterf.find(applierID) != self->appliersInterf.end() );
				RestoreApplierInterface applierInterf = self->appliersInterf[applierID];
				self->cmdID.nextCmd();
				cmdReplies.push_back( applierInterf.setApplierKeyRangeRequest.getReply(RestoreSetApplierKeyRangeRequest(self->cmdID, applier.first, keyRange)) );

			}
			printf("[INFO] Wait for %ld applier to accept the cmd Assign_Applier_KeyRange\n", appliers.size());
			std::vector<RestoreCommonReply> reps = wait( timeoutError(getAll(cmdReplies), FastRestore_Failure_Timeout) );
			printf("All appliers have been assigned for ranges\n");
			
			break;
		} catch (Error &e) {
			fprintf(stdout, "[ERROR] Node:%s, Commands before cmdID:%s error. error code:%d, error message:%s\n", self->describeNode().c_str(),
					self->cmdID.toString().c_str(), e.code(), e.what());
		}
	}

	return Void();
}

// Restore Master: Notify loader about appliers' responsible key range
ACTOR Future<Void> notifyAppliersKeyRangeToLoader(Reference<RestoreMasterData> self, Database cx)  {
	state std::vector<UID> loaders = self->getLoaderIDs();
	state std::vector<Future<RestoreCommonReply>> cmdReplies;
	state Standalone<VectorRef<UID>> appliers;
	state Standalone<VectorRef<KeyRange>> ranges;

	state std::map<Standalone<KeyRef>, UID>::iterator applierRange;
	for (applierRange = self->range2Applier.begin(); applierRange != self->range2Applier.end(); applierRange++) {
		KeyRef beginRange = applierRange->first;
		KeyRange range(KeyRangeRef(beginRange, beginRange)); // TODO: Use the end of key range
		appliers.push_back(appliers.arena(), applierRange->second);
		ranges.push_back(ranges.arena(), range);
	}

	printf("Notify_Loader_ApplierKeyRange: number of appliers:%d\n", appliers.size());
	ASSERT( appliers.size() == ranges.size() && appliers.size() != 0 );

	self->cmdID.initPhase( RestoreCommandEnum::Notify_Loader_ApplierKeyRange );
	state std::map<UID, RestoreLoaderInterface>::iterator loader;
	for (loader = self->loadersInterf.begin(); loader != self->loadersInterf.end(); loader++) {
		self->cmdID.nextCmd();
		loop {
			try {
				cmdReplies.clear();
				printf("[CMD] Node:%s Notify node:%s about appliers key range\n", self->describeNode().c_str(), loader->first.toString().c_str());
				cmdReplies.push_back( loader->second.setApplierKeyRangeVectorRequest.getReply(RestoreSetApplierKeyRangeVectorRequest(self->cmdID, appliers, ranges)) );
				printf("[INFO] Wait for node:%s to accept the cmd Notify_Loader_ApplierKeyRange\n", loader->first.toString().c_str());
				std::vector<RestoreCommonReply> reps = wait( timeoutError( getAll(cmdReplies), FastRestore_Failure_Timeout ) );
				printf("Finished Notify_Loader_ApplierKeyRange: number of appliers:%d\n", appliers.size());
				cmdReplies.clear();
				break;
			} catch (Error &e) {
				fprintf(stdout, "[ERROR] Node:%s, Commands before cmdID:%s timeout\n", self->describeNode().c_str(), self->cmdID.toString().c_str());
			}
		}
	}

	return Void();
}


ACTOR static Future<Void> finishRestore(Reference<RestoreMasterData> self, Database cx, Standalone<VectorRef<RestoreRequest>> restoreRequests) {
	// Make restore workers quit
	state std::vector<Future<RestoreCommonReply>> cmdReplies;
	state std::map<UID, RestoreLoaderInterface>::iterator loader;
	state std::map<UID, RestoreApplierInterface>::iterator applier;
	loop {
		try {
			cmdReplies.clear();
			self->cmdID.initPhase(RestoreCommandEnum::Finish_Restore);

			for ( loader = self->loadersInterf.begin(); loader != self->loadersInterf.end(); loader++ ) {
				self->cmdID.nextCmd();
				cmdReplies.push_back(loader->second.finishRestore.getReply(RestoreSimpleRequest(self->cmdID)));
			}
			for ( applier = self->appliersInterf.begin(); applier != self->appliersInterf.end(); applier++ ) {
				self->cmdID.nextCmd();
				cmdReplies.push_back(applier->second.finishRestore.getReply(RestoreSimpleRequest(self->cmdID)));
			}

			if (!cmdReplies.empty()) {
				std::vector<RestoreCommonReply> reps =  wait( timeoutError( getAll(cmdReplies), FastRestore_Failure_Timeout / 100 ) );
				//std::vector<RestoreCommonReply> reps =  wait( getAll(cmdReplies) );
				cmdReplies.clear();
			}
			printf("All restore workers have quited\n");

			break;
		} catch(Error &e) {
			printf("[ERROR] At sending finishRestore request. error code:%d message:%s. Retry...\n", e.code(), e.what());
			self->loadersInterf.clear();
			self->appliersInterf.clear();
			cmdReplies.clear();
		}
	}

	// Notify tester that the restore has finished
	state ReadYourWritesTransaction tr3(cx);
	loop {
		try {
			//Standalone<StringRef> versionStamp = wait( tr3.getVersionstamp() );
			tr3.reset();
			tr3.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr3.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr3.clear(restoreRequestTriggerKey);
			tr3.clear(restoreRequestKeys);
			Version readVersion = wait(tr3.getReadVersion());
			tr3.set(restoreRequestDoneKey, restoreRequestDoneVersionValue(readVersion));
			wait(tr3.commit());
			TraceEvent("LeaderFinishRestoreRequest");
			printf("[INFO] RestoreLeader write restoreRequestDoneKey\n");

			break;
		}  catch( Error &e ) {
			TraceEvent("RestoreAgentLeaderErrorTr3").detail("ErrorCode", e.code()).detail("ErrorName", e.name());
			printf("[Error] RestoreLead operation on restoreRequestDoneKey, error:%s\n", e.what());
			wait( tr3.onError(e) );
		}
	};


 	// TODO:  Validate that the range version map has exactly the restored ranges in it.  This means that for any restore operation
 	// the ranges to restore must be within the backed up ranges, otherwise from the restore perspective it will appear that some
 	// key ranges were missing and so the backup set is incomplete and the restore has failed.
 	// This validation cannot be done currently because Restore only supports a single restore range but backups can have many ranges.

 	// Clear the applyMutations stuff, including any unapplied mutations from versions beyond the restored version.
 	//	restore.clearApplyMutationsKeys(tr);

	printf("[INFO] Notify the end of the restore\n");
	TraceEvent("NotifyRestoreFinished");

	return Void();
}



ACTOR static Future<Void> unlockDB(Database cx, UID uid) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			printf("CheckDBlock:%s START\n", uid.toString().c_str());
			wait(checkDatabaseLock(tr, uid));
			printf("CheckDBlock:%s DONE\n", uid.toString().c_str());

			printf("UnlockDB now. Start.\n");
			wait(unlockDatabase(tr, uid)); //NOTE: unlockDatabase didn't commit inside the function!

			printf("CheckDBlock:%s START\n", uid.toString().c_str());
			wait(checkDatabaseLock(tr, uid));
			printf("CheckDBlock:%s DONE\n", uid.toString().c_str());

			printf("UnlockDB now. Commit.\n");
			wait( tr->commit() );

			printf("UnlockDB now. Done.\n");
			break;
		} catch( Error &e ) {
			printf("Error when we unlockDB. Error:%s\n", e.what());
			wait(tr->onError(e));
		}
	};

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
			printf("Transaction Error when we registerStatus. Error:%s\n", e.what());
			wait(tr->onError(e));
		}
	 };

	return Void();
}