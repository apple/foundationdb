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

ACTOR Future<Void> askLoadersToCollectRestoreAppliersInterfaces(Reference<RestoreMasterData> self);
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

// The server of the restore master. It drives the restore progress with the following steps:
// 1) Collect interfaces of all RestoreLoader and RestoreApplier roles
// 2) Notify each loader to collect interfaces of all RestoreApplier roles
// 3) Wait on each RestoreRequest, which is sent by RestoreAgent operated by DBA
// 4) Process each restore request in actor processRestoreRequest;
// 5) After process all restore requests, finish restore by cleaning up the restore related system key
//    and ask all restore roles to quit.
ACTOR Future<Void> startRestoreMaster(Reference<RestoreMasterData> self, Database cx) {
	try {
		wait( delay(1.0) );
		wait( _collectRestoreRoleInterfaces(self, cx) );

		wait( delay(1.0) );
		wait( askLoadersToCollectRestoreAppliersInterfaces(self) );

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
			//wait( finishRestore(self) );
			wait( finishRestore(self, cx, restoreRequests) ); 

			printf("[INFO] MXRestoreEndHere RestoreID:%d\n", restoreId);
			TraceEvent("MXRestoreEndHere").detail("RestoreID", restoreId++);
			wait( delay(5.0) );
			//NOTE: we have to break the loop so that the tester.actor can receive the return of this test workload.
			//Otherwise, this special workload never returns and tester will think the test workload is stuck and the tester will timesout
			break; //TODO: this break will be removed later since we need the restore agent to run all the time!
		}

		return Void();

	} catch (Error &e) {
		fprintf(stdout, "[ERROR] Restoer Master encounters error. error code:%d, error message:%s\n",
				e.code(), e.what());
	}

	return Void();
}


ACTOR static Future<Version> processRestoreRequest(RestoreRequest request, Reference<RestoreMasterData> self, Database cx) {
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

	//MX: Lock DB if it is not locked
	printf("RestoreRequest lockDB:%d\n", lockDB);
	if ( lockDB == false ) {
		printf("[WARNING] RestoreRequest lockDB:%d; we will overwrite request.lockDB to true and forcely lock db\n", lockDB);
		lockDB = true;
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
	state Reference<RestoreConfig> restoreConfig(new RestoreConfig(randomUid));

	// lock DB for restore
	wait( _lockDB(cx, randomUid, lockDB) );
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

			wait( delay(1.0) );

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
	wait( unlockDB(cx, randomUid) );
	printf("Finish restore uid:%s \n", randomUid.toString().c_str());

	return targetVersion;
}

// Distribution workload per version batch
ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, Database cx, RestoreRequest request, Reference<RestoreConfig> restoreConfig) {
	state Key tagName = request.tagName;
	state Key url = request.url;
	state bool waitForComplete = request.waitForComplete;
	state Version targetVersion = request.targetVersion;
	state bool verbose = request.verbose;
	state KeyRange restoreRange = request.range;
	state Key addPrefix = request.addPrefix;
	state Key removePrefix = request.removePrefix;
	state bool lockDB = request.lockDB;
	state UID randomUid = request.randomUid;
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
	
	wait( sampleWorkload(self, request, restoreConfig, sampleSizeMB) );
	wait( delay(1.0) );

	printf("[Progress] distributeWorkloadPerVersionBatch sampling time:%.2f seconds\n", now() - startTime);
	state double startTimeAfterSampling = now();

	// Notify each applier about the key range it is responsible for, and notify appliers to be ready to receive data
	startTime = now();
	wait( assignKeyRangeToAppliers(self, cx) );
	wait( delay(1.0) );
	printf("[Progress] distributeWorkloadPerVersionBatch assignKeyRangeToAppliers time:%.2f seconds\n", now() - startTime);

	startTime = now();
	wait( notifyAppliersKeyRangeToLoader(self, cx) );
	wait( delay(1.0) );
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

	state int checkpointCurFileIndex = 0;
	state long checkpointCurOffset = 0; 

	startTime = now();
	// We should load log file before we do range file
	state RestoreCommandEnum phaseType = RestoreCommandEnum::Assign_Loader_Log_File;
	state std::vector<Future<RestoreCommonReply>> cmdReplies;
	loop {
		state int curFileIndex = 0; // The smallest index of the files that has not been FULLY loaded
		state long curOffset = 0;
		state bool allLoadReqsSent = false;
		loop {
			try {
				if ( allLoadReqsSent ) {
					break; // All load requests have been handled
				}
				wait(delay(1.0));

				cmdReplies.clear();
				printf("[INFO] Number of backup files:%ld\n", self->files.size());
				self->cmdID.initPhase(phaseType);
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
					//self->files[curFileIndex].cursor = 0; // This is a hacky way to make sure cursor is correct in current version when we load 1 file at a time
					param.url = request.url;
					param.version = self->files[curFileIndex].version;
					param.filename = self->files[curFileIndex].fileName;
					param.offset = curOffset; //self->files[curFileIndex].cursor;
					param.length = std::min(self->files[curFileIndex].fileSize - curOffset, self->files[curFileIndex].blockSize);
					//param.length = self->files[curFileIndex].fileSize;
					loadSizeB = param.length;
					param.blockSize = self->files[curFileIndex].blockSize;
					param.restoreRange = restoreRange;
					param.addPrefix = addPrefix;
					param.removePrefix = removePrefix;
					param.mutationLogPrefix = mutationLogPrefix;
					if ( !(param.length > 0  &&  param.offset >= 0 && param.offset < self->files[curFileIndex].fileSize) ) {
						printf("[ERROR] param: length:%ld offset:%ld fileSize:%ld for %ldth filename:%s\n",
								param.length, param.offset, self->files[curFileIndex].fileSize, curFileIndex,
								self->files[curFileIndex].fileName.c_str());
					}
					ASSERT( param.length > 0 );
					ASSERT( param.offset >= 0 );
					ASSERT( param.offset < self->files[curFileIndex].fileSize );
					self->files[curFileIndex].cursor = self->files[curFileIndex].cursor +  param.length;

					RestoreCommandEnum cmdType = RestoreCommandEnum::Assign_Loader_Range_File;
					if (self->files[curFileIndex].isRange) {
						cmdType = RestoreCommandEnum::Assign_Loader_Range_File;
						self->cmdID.setPhase(RestoreCommandEnum::Assign_Loader_Range_File);
					} else {
						cmdType = RestoreCommandEnum::Assign_Loader_Log_File;
						self->cmdID.setPhase(RestoreCommandEnum::Assign_Loader_Log_File);
					}

					if ( (phaseType == RestoreCommandEnum::Assign_Loader_Log_File && self->files[curFileIndex].isRange) 
						|| (phaseType == RestoreCommandEnum::Assign_Loader_Range_File && !self->files[curFileIndex].isRange) ) {
						self->files[curFileIndex].cursor = 0;
						curFileIndex++;
						curOffset = 0;
					} else { // load the type of file in the phaseType
						self->cmdID.nextCmd();
						printf("[CMD] Loading fileIndex:%ld fileInfo:%s loadingParam:%s on node %s\n",
							curFileIndex, self->files[curFileIndex].toString().c_str(), 
							param.toString().c_str(), loaderID.toString().c_str()); // VERY USEFUL INFO
						printf("[INFO] Node:%s CMDUID:%s cmdType:%d isRange:%d loaderNode:%s\n", self->describeNode().c_str(), self->cmdID.toString().c_str(),
								(int) cmdType, (int) self->files[curFileIndex].isRange, loaderID.toString().c_str());
						if (self->files[curFileIndex].isRange) {
							cmdReplies.push_back( loaderInterf.loadRangeFile.getReply(RestoreLoadFileRequest(self->cmdID, param)) );
						} else {
							cmdReplies.push_back( loaderInterf.loadLogFile.getReply(RestoreLoadFileRequest(self->cmdID, param)) );
						}
						curOffset += param.length;

						// Reach the end of the file
						if ( param.length + param.offset >= self->files[curFileIndex].fileSize ) {
							curFileIndex++;
							curOffset = 0;
						}
						
						// if (param.length <= loadSizeB) { // Reach the end of the file
						// 	ASSERT( self->files[curFileIndex].cursor == self->files[curFileIndex].fileSize );
						// 	curFileIndex++;
						// }
					}
					
					if ( curFileIndex >= self->files.size() ) {
						allLoadReqsSent = true;
						break;
					}
					//++loadingCmdIndex; // Replaced by cmdUID
				}

				printf("[INFO] Wait for %ld loaders to accept the cmd Assign_Loader_File\n", cmdReplies.size());

				// Question: How to set reps to different value based on cmdReplies.empty()?
				if ( !cmdReplies.empty() ) {
					std::vector<RestoreCommonReply> reps = wait( timeoutError( getAll(cmdReplies), FastRestore_Failure_Timeout ) ); //TODO: change to getAny. NOTE: need to keep the still-waiting replies
					//std::vector<RestoreCommonReply> reps = wait( getAll(cmdReplies) ); 

					cmdReplies.clear();
					for (int i = 0; i < reps.size(); ++i) {
						printf("[INFO] Get Ack reply:%s for Assign_Loader_File\n",
								reps[i].toString().c_str());
					}
					checkpointCurFileIndex = curFileIndex; // Save the previous success point
					checkpointCurOffset = curOffset;
				}

				// TODO: Let master print all nodes status. Note: We need a function to print out all nodes status

				if (allLoadReqsSent) {
					printf("[INFO] allLoadReqsSent has finished.\n");
					break; // NOTE: need to change when change to wait on any cmdReplies
				}

			} catch (Error &e) {
				fprintf(stdout, "[ERROR] Node:%s, Commands before cmdID:%s error. error code:%d, error message:%s\n", self->describeNode().c_str(),
						self->cmdID.toString().c_str(), e.code(), e.what());
				curFileIndex = checkpointCurFileIndex;
				curOffset = checkpointCurOffset;
			}
		}

		if (phaseType == RestoreCommandEnum::Assign_Loader_Log_File) {
			phaseType = RestoreCommandEnum::Assign_Loader_Range_File;
		} else if (phaseType == RestoreCommandEnum::Assign_Loader_Range_File) {
			break;
		}
	}

	wait( delay(1.0) );
	printf("[Progress] distributeWorkloadPerVersionBatch loadFiles time:%.2f seconds\n", now() - startTime);

	ASSERT( cmdReplies.empty() );
	
	wait( delay(5.0) );
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


// RestoreMaster: Ask loaders to sample data and send mutations to master applier. Ask master applier to calculate the range for each applier
ACTOR static Future<Void> sampleWorkload(Reference<RestoreMasterData> self, RestoreRequest request, Reference<RestoreConfig> restoreConfig, int64_t sampleMB_input) {
	state Key tagName = request.tagName;
	state Key url = request.url;
	state bool waitForComplete = request.waitForComplete;
	state Version targetVersion = request.targetVersion;
	state bool verbose = request.verbose;
	state KeyRange restoreRange = request.range;
	state Key addPrefix = request.addPrefix;
	state Key removePrefix = request.removePrefix;
	state bool lockDB = request.lockDB;
	state UID randomUid = request.randomUid;
	state Key mutationLogPrefix = restoreConfig->mutationLogPrefix();

	state bool allLoadReqsSent = false;
	state int64_t sampleMB = sampleMB_input; //100;
	state int64_t sampleB = sampleMB * 1024 * 1024; // Sample a block for every sampleB bytes. // Should adjust this value differently for simulation mode and real mode
	state int64_t curFileIndex = 0;
	state int64_t curFileOffset = 0;
	state int64_t loadSizeB = 0;
	state int64_t loadingCmdIndex = 0;
	state int64_t sampleIndex = 0;
	state double totalBackupSizeB = 0;
	state double samplePercent = 0.05; // sample 1 data block per samplePercent (0.01) of data. num_sample = 1 / samplePercent

	// We should sample 1% data
	for (int i = 0; i < self->files.size(); i++) {
		totalBackupSizeB += self->files[i].fileSize;
	}
	sampleB = std::max((int) (samplePercent * totalBackupSizeB), 10 * 1024 * 1024); // The minimal sample size is 10MB
	printf("Node:%s totalBackupSizeB:%.1fB (%.1fMB) samplePercent:%.2f, sampleB:%ld\n", self->describeNode().c_str(),
			totalBackupSizeB,  totalBackupSizeB / 1024 / 1024, samplePercent, sampleB);

	// Step: Distribute sampled file blocks to loaders to sample the mutations
	self->cmdID.initPhase(RestoreCommandEnum::Sample_Range_File);
	curFileIndex = 0;
	state CMDUID checkpointCMDUID = self->cmdID;
	state int checkpointCurFileIndex = curFileIndex;
	state int64_t checkpointCurFileOffset = 0;
	state std::vector<Future<RestoreCommonReply>> cmdReplies;
	state RestoreCommandEnum cmdType;
	loop { // For retry on timeout
		try {
			if ( allLoadReqsSent ) {
				break; // All load requests have been handled
			}
			wait(delay(1.0));

			cmdReplies.clear();

			printf("[Sampling] Node:%s We will sample the workload among %ld backup files.\n", self->describeNode().c_str(), self->files.size());
			printf("[Sampling] Node:%s totalBackupSizeB:%.1fB (%.1fMB) samplePercent:%.2f, sampleB:%ld, loadSize:%dB sampleIndex:%ld\n", self->describeNode().c_str(),
				totalBackupSizeB,  totalBackupSizeB / 1024 / 1024, samplePercent, sampleB, loadSizeB, sampleIndex);
			for (auto &loader : self->loadersInterf) {
				const UID &loaderID = loader.first;
				RestoreLoaderInterface &loaderInterf= loader.second;

				// Find the sample file
				while ( curFileIndex < self->files.size() && self->files[curFileIndex].fileSize == 0 ) {
					// NOTE: && self->files[curFileIndex].cursor >= self->files[curFileIndex].fileSize
					printf("[Sampling] File %ld:%s filesize:%ld skip the file\n", curFileIndex,
							self->files[curFileIndex].fileName.c_str(), self->files[curFileIndex].fileSize);
					curFileOffset = 0;
					curFileIndex++;
				}
				// Find the next sample point
				while ( loadSizeB / sampleB < sampleIndex && curFileIndex < self->files.size() ) {
					if (self->files[curFileIndex].fileSize == 0) {
						// NOTE: && self->files[curFileIndex].cursor >= self->files[curFileIndex].fileSize
						printf("[Sampling] File %ld:%s filesize:%ld skip the file\n", curFileIndex,
								self->files[curFileIndex].fileName.c_str(), self->files[curFileIndex].fileSize);
						curFileIndex++;
						curFileOffset = 0;
						continue;
					}
					if ( loadSizeB / sampleB >= sampleIndex ) {
						break;
					}
					if (curFileIndex >= self->files.size()) {
						break;
					}
					loadSizeB += std::min( self->files[curFileIndex].blockSize, std::max(self->files[curFileIndex].fileSize - curFileOffset * self->files[curFileIndex].blockSize, (int64_t) 0) );
					curFileOffset++;
					if ( self->files[curFileIndex].blockSize == 0 || curFileOffset >= self->files[curFileIndex].fileSize / self->files[curFileIndex].blockSize ) {
						curFileOffset = 0;
						curFileIndex++;
					}
				}
				if ( curFileIndex >= self->files.size() ) {
					allLoadReqsSent = true;
					break;
				}

				//sampleIndex++;

				// Notify loader to sample the file
				LoadingParam param;
				param.url = request.url;
				param.version = self->files[curFileIndex].version;
				param.filename = self->files[curFileIndex].fileName;
				param.offset = curFileOffset * self->files[curFileIndex].blockSize; // The file offset in bytes
				//param.length = std::min(self->files[curFileIndex].fileSize - self->files[curFileIndex].cursor, loadSizeB);
				param.length = std::min(self->files[curFileIndex].blockSize, std::max((int64_t)0, self->files[curFileIndex].fileSize - param.offset));
				loadSizeB += param.length;
				sampleIndex = std::ceil(loadSizeB / sampleB);
				curFileOffset++;

				//loadSizeB = param.length;
				param.blockSize = self->files[curFileIndex].blockSize;
				param.restoreRange = restoreRange;
				param.addPrefix = addPrefix;
				param.removePrefix = removePrefix;
				param.mutationLogPrefix = mutationLogPrefix;
				if ( !(param.length > 0  &&  param.offset >= 0 && param.offset < self->files[curFileIndex].fileSize) ) {
					printf("[ERROR] param: length:%ld offset:%ld fileSize:%ld for %ldth file:%s\n",
							param.length, param.offset, self->files[curFileIndex].fileSize, curFileIndex,
							self->files[curFileIndex].toString().c_str());
				}


				printf("[Sampling][File:%ld] filename:%s offset:%ld blockSize:%ld filesize:%ld loadSize:%ldB sampleIndex:%ld\n",
						curFileIndex, self->files[curFileIndex].fileName.c_str(), curFileOffset,
						self->files[curFileIndex].blockSize, self->files[curFileIndex].fileSize,
						loadSizeB, sampleIndex);


				ASSERT( param.length > 0 );
				ASSERT( param.offset >= 0 );
				ASSERT( param.offset <= self->files[curFileIndex].fileSize );

				printf("[Sampling][CMD] Node:%s Loading %s on node %s\n", 
						self->describeNode().c_str(), param.toString().c_str(), loaderID.toString().c_str());

				self->cmdID.nextCmd(); // The cmd index is the i^th file (range or log file) to be processed
				if (!self->files[curFileIndex].isRange) {
					cmdType = RestoreCommandEnum::Sample_Log_File;
					self->cmdID.setPhase(RestoreCommandEnum::Sample_Log_File);
					cmdReplies.push_back( loaderInterf.sampleLogFile.getReply(RestoreLoadFileRequest(self->cmdID, param)) );
				} else {
					cmdType = RestoreCommandEnum::Sample_Range_File;
					self->cmdID.setPhase(RestoreCommandEnum::Sample_Range_File);
					cmdReplies.push_back( loaderInterf.sampleRangeFile.getReply(RestoreLoadFileRequest(self->cmdID, param)) );
				}
				
				printf("[Sampling] Master cmdType:%d cmdUID:%s isRange:%d destinationNode:%s\n", 
						(int) cmdType, self->cmdID.toString().c_str(), (int) self->files[curFileIndex].isRange,
						loaderID.toString().c_str());
				
				if (param.offset + param.length >= self->files[curFileIndex].fileSize) { // Reach the end of the file
					curFileIndex++;
					curFileOffset = 0;
				}
				if ( curFileIndex >= self->files.size() ) {
					allLoadReqsSent = true;
					break;
				}
				++loadingCmdIndex;
			}

			printf("[Sampling] Wait for %ld loaders to accept the cmd Sample_Range_File or Sample_Log_File\n", cmdReplies.size());

			if ( !cmdReplies.empty() ) {
				//TODO: change to getAny. NOTE: need to keep the still-waiting replies
				std::vector<RestoreCommonReply> reps = wait( timeoutError( getAll(cmdReplies), FastRestore_Failure_Timeout ) ); 
				//std::vector<RestoreCommonReply> reps = wait( getAll(cmdReplies) ); 

				for (int i = 0; i < reps.size(); ++i) {
					printf("[Sampling][%d out of %d] Get reply:%s for  Sample_Range_File or Sample_Log_File\n",
							i, reps.size(), reps[i].toString().c_str());
				}
				checkpointCMDUID = self->cmdID;
				checkpointCurFileIndex = curFileIndex;
				checkpointCurFileOffset = curFileOffset;
			}

			if (allLoadReqsSent) {
				printf("[Sampling] allLoadReqsSent, sampling finished\n");
				break; // NOTE: need to change when change to wait on any cmdReplies
			}

		} catch (Error &e) {
			fprintf(stdout, "[ERROR] Node:%s, Commands before cmdID:%s error. error code:%d, error message:%s\n", self->describeNode().c_str(),
					self->cmdID.toString().c_str(), e.code(), e.what());
			self->cmdID = checkpointCMDUID;
			curFileIndex = checkpointCurFileIndex;
			curFileOffset = checkpointCurFileOffset;
			allLoadReqsSent = false;
			printf("[Sampling][Waring] Retry at CMDID:%s curFileIndex:%ld\n", self->cmdID.toString().c_str(), curFileIndex);
		}
	}

	wait(delay(1.0));

	// Ask master applier to calculate the key ranges for appliers
	state int numKeyRanges = 0;
	loop {
		try {
			printf("[Sampling][CMD] Ask master applier %s for the key ranges for appliers\n", self->masterApplierInterf.toString().c_str());

			ASSERT(self->appliersInterf.size() > 0);
			self->cmdID.initPhase(RestoreCommandEnum::Calculate_Applier_KeyRange);
			self->cmdID.nextCmd();
			GetKeyRangeNumberReply rep = wait( timeoutError( 
				self->masterApplierInterf.calculateApplierKeyRange.getReply(RestoreCalculateApplierKeyRangeRequest(self->cmdID, self->appliersInterf.size())),  FastRestore_Failure_Timeout) );
			printf("[Sampling][CMDRep] number of key ranges calculated by master applier:%d\n", rep.keyRangeNum);
			numKeyRanges = rep.keyRangeNum;

			if (numKeyRanges <= 0 || numKeyRanges > self->appliersInterf.size() ) {
				printf("[WARNING] Calculate_Applier_KeyRange receives wrong reply (numKeyRanges:%ld) from other phases. appliersInterf.size:%d Retry Calculate_Applier_KeyRange\n", numKeyRanges, self->appliersInterf.size());
				UNREACHABLE();
			}

			if ( numKeyRanges < self->appliersInterf.size() ) {
				printf("[WARNING][Sampling] numKeyRanges:%d < appliers number:%ld. %ld appliers will not be used!\n",
						numKeyRanges, self->appliersInterf.size(), self->appliersInterf.size() - numKeyRanges);
			}

			break;
		} catch (Error &e) {
			fprintf(stdout, "[ERROR] Node:%s, Commands before cmdID:%s error. error code:%d, error message:%s\n", self->describeNode().c_str(),
					self->cmdID.toString().c_str(), e.code(), e.what());
			printf("[Sampling] [Warning] Retry on Calculate_Applier_KeyRange\n");
		}
	}

	wait(delay(1.0));

	// Ask master applier to return the key range for appliers
	state std::vector<Future<GetKeyRangeReply>> keyRangeReplies;
	state std::map<UID, RestoreApplierInterface>::iterator applier;
	state int applierIndex = 0;
	loop {
		try {
			self->range2Applier.clear();
			keyRangeReplies.clear(); // In case error happens in try loop
			self->cmdID.initPhase(RestoreCommandEnum::Get_Applier_KeyRange);
			//self->cmdID.nextCmd();
			for ( applier = self->appliersInterf.begin(), applierIndex = 0;
				  applierIndex < numKeyRanges;
				  applier++, applierIndex++) {
				self->cmdID.nextCmd();
				printf("[Sampling][Master] Node:%s, CMDID:%s Ask masterApplierInterf:%s for the lower boundary of the key range for applier:%s\n",
						self->describeNode().c_str(), self->cmdID.toString().c_str(),
						self->masterApplierInterf.toString().c_str(), applier->first.toString().c_str());
				ASSERT( applier != self->appliersInterf.end() );
				keyRangeReplies.push_back( self->masterApplierInterf.getApplierKeyRangeRequest.getReply(
					RestoreGetApplierKeyRangeRequest(self->cmdID, applierIndex)) );
			}
			std::vector<GetKeyRangeReply> reps = wait( timeoutError( getAll(keyRangeReplies), FastRestore_Failure_Timeout) );

			ASSERT( reps.size() <= self->appliersInterf.size() );

			// TODO: Directly use the replied lowerBound and upperBound
			applier = self->appliersInterf.begin();
			for (int i = 0; i < reps.size() && i < numKeyRanges; ++i) {
				UID applierID = applier->first;
				Standalone<KeyRef> lowerBound = reps[i].lowerBound;
				// if (i < numKeyRanges) {
				// 	lowerBound = reps[i].lowerBound;
				// } else {
				// 	lowerBound = normalKeys.end;
				// }

				if (i == 0) {
					lowerBound = LiteralStringRef("\x00"); // The first interval must starts with the smallest possible key
				}
				printf("[INFO] Node:%s Assign key-to-applier map: Key:%s -> applierID:%s\n", self->describeNode().c_str(),
						getHexString(lowerBound).c_str(), applierID.toString().c_str());
				self->range2Applier.insert(std::make_pair(lowerBound, applierID));
				applier++;
			}

			break;
		} catch (Error &e) {
			fprintf(stdout, "[ERROR] Node:%s, Commands before cmdID:%s error. error code:%d, error message:%s\n", self->describeNode().c_str(),
					self->cmdID.toString().c_str(), e.code(), e.what());
			printf("[Sampling] [Warning] Retry on Get_Applier_KeyRange\n");
		}
	}
	printf("[Sampling] self->range2Applier has been set. Its size is:%d\n", self->range2Applier.size());
	self->printAppliersKeyRange();

	wait(delay(1.0));

	return Void();

}

// Restore Master: Ask each restore loader to collect all appliers' interfaces
ACTOR Future<Void> askLoadersToCollectRestoreAppliersInterfaces(Reference<RestoreMasterData> self) {
	state int index = 0;
	loop {
		try {
			wait(delay(1.0));
			index = 0;
			std::vector<Future<RestoreCommonReply>> cmdReplies;
			for(auto& loaderInterf : self->loadersInterf) {
				self->cmdID.nextCmd();
				printf("[CMD:%s] Node:%s askLoadersToCollectRestoreAppliersInterfaces for node (index=%d uid=%s)\n", 
						self->cmdID.toString().c_str(), self->describeNode().c_str(),
						index, loaderInterf.first.toString().c_str());
				cmdReplies.push_back( loaderInterf.second.collectRestoreRoleInterfaces.getReply(RestoreSimpleRequest(self->cmdID)) );
				index++;
			}
			std::vector<RestoreCommonReply> reps = wait( timeoutError(getAll(cmdReplies), FastRestore_Failure_Timeout) );
			printf("[setWorkerInterface] Finished\n");
			break;
		} catch (Error &e) {
			fprintf(stdout, "[ERROR] Node:%s, Commands before cmdID:%s error. error code:%d, error message:%s\n", self->describeNode().c_str(),
					self->cmdID.toString().c_str(), e.code(), e.what());
			printf("Node:%s waits on replies time out. Current phase: setWorkerInterface, Retry all commands.\n", self->describeNode().c_str());
		}
	}

	return Void();
}



// TODO: Revise the way to collect the restore request. We may make it into 1 transaction
ACTOR Future<Standalone<VectorRef<RestoreRequest>>> collectRestoreRequests(Database cx) {
	state int restoreId = 0;
	state int checkNum = 0;
	state Standalone<VectorRef<RestoreRequest>> restoreRequests;
	state Future<Void> watch4RestoreRequest;

	//wait for the restoreRequestTriggerKey to be set by the client/test workload
	state ReadYourWritesTransaction tr(cx);

	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			// Assumption: restoreRequestTriggerKey has not been set
			// Question: What if  restoreRequestTriggerKey has been set? we will stuck here?
			// Question: Can the following code handle the situation?
			// Note: restoreRequestTriggerKey may be set before the watch is set or may have a conflict when the client sets the same key
			// when it happens, will we  stuck at wait on the watch?

			watch4RestoreRequest = tr.watch(restoreRequestTriggerKey);
			wait(tr.commit());
			printf("[INFO][Master] Finish setting up watch for restoreRequestTriggerKey\n");
			break;
		} catch(Error &e) {
			printf("[WARNING] Transaction for restore request in watch restoreRequestTriggerKey. Error:%s\n", e.name());
			wait(tr.onError(e));
		}
	};


	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			// Assumption: restoreRequestTriggerKey has not been set
			// Before we wait on the watch, we must make sure the key is not there yet!
			//printf("[INFO][Master] Make sure restoreRequestTriggerKey does not exist before we wait on the key\n");
			Optional<Value> triggerKey = wait( tr.get(restoreRequestTriggerKey) );
			if ( triggerKey.present() ) {
				printf("!!! restoreRequestTriggerKey (and restore requests) is set before restore agent waits on the request. Restore agent can immediately proceed\n");
				break;
			}
			wait(watch4RestoreRequest);
			printf("[INFO][Master] restoreRequestTriggerKey watch is triggered\n");
			break;
		} catch(Error &e) {
			printf("[WARNING] Transaction for restore request at wait on watch restoreRequestTriggerKey. Error:%s\n", e.name());
			wait(tr.onError(e));
		}
	};

	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			state Optional<Value> numRequests = wait(tr.get(restoreRequestTriggerKey));
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
		} catch(Error &e) {
			printf("[WARNING] Transaction error: collect restore requests. Error:%s\n", e.name());
			wait(tr.onError(e));
		}
	};

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
	loop {
		try {
			wait(delay(1.0));
			std::vector<Future<RestoreCommonReply>> cmdReplies;
			self->cmdID.initPhase(RestoreCommandEnum::Reset_VersionBatch);
			for (auto &loader : self->loadersInterf) {
				cmdReplies.push_back( loader.second.initVersionBatch.getReply(RestoreVersionBatchRequest(self->cmdID, self->batchIndex)) );
			}
			for (auto &applier : self->appliersInterf) {
				cmdReplies.push_back( applier.second.initVersionBatch.getReply(RestoreVersionBatchRequest(self->cmdID, self->batchIndex)) );
			}

			std::vector<RestoreCommonReply> reps = wait( timeoutError(getAll(cmdReplies), FastRestore_Failure_Timeout) );
			printf("Initilaize Version Batch done\n");
			break;
		} catch (Error &e) {
			fprintf(stdout, "[ERROR] Node:%s, Current phase: initializeVersionBatch, Commands before cmdID:%s error. error code:%d, error message:%s\n", self->describeNode().c_str(),
					self->cmdID.toString().c_str(), e.code(), e.what());
		}
	}

	return Void();
}


ACTOR Future<Void> notifyApplierToApplyMutations(Reference<RestoreMasterData> self) {
	state std::vector<Future<RestoreCommonReply>> cmdReplies;
	loop {
		try {
			self->cmdID.initPhase( RestoreCommandEnum::Apply_Mutation_To_DB );
			state std::map<UID, RestoreApplierInterface>::iterator applier;
			for (applier = self->appliersInterf.begin(); applier != self->appliersInterf.end(); applier++) {
				RestoreApplierInterface &applierInterf = applier->second;
	
				printf("[CMD] Node:%s Notify node:%s to apply mutations to DB\n", self->describeNode().c_str(), applier->first.toString().c_str());
				cmdReplies.push_back( applier->second.applyToDB.getReply(RestoreSimpleRequest(self->cmdID)) );

				// Ask applier to apply to DB one by one
				printf("[INFO] Wait for %ld appliers to apply mutations to DB\n", self->appliersInterf.size());
				std::vector<RestoreCommonReply> reps = wait( timeoutError( getAll(cmdReplies), FastRestore_Failure_Timeout ) );
				//std::vector<RestoreCommonReply> reps = wait( getAll(cmdReplies) );
				printf("[INFO] %ld appliers finished applying mutations to DB\n", self->appliersInterf.size());

				cmdReplies.clear();

			}
			// Ask all appliers to apply to DB at once
			// printf("[INFO] Wait for %ld appliers to apply mutations to DB\n", self->appliersInterf.size());
			// std::vector<RestoreCommonReply> reps = wait( timeoutError( getAll(cmdReplies), FastRestore_Failure_Timeout ) );
			// //std::vector<RestoreCommonReply> reps = wait( getAll(cmdReplies) );
			// printf("[INFO] %ld appliers finished applying mutations to DB\n", self->appliersInterf.size());

			// cmdReplies.clear();

			wait(delay(5.0)); //TODO: Delete this wait and see if it can pass correctness

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
			wait( _collectRestoreRoleInterfaces(self, cx) );
		}
	}

	// Notify tester that the restore has finished
	state ReadYourWritesTransaction tr3(cx);
	loop {
		try {
			tr3.reset();
			tr3.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr3.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr3.clear(restoreRequestTriggerKey);
			tr3.clear(restoreRequestKeys);
			tr3.set(restoreRequestDoneKey, restoreRequestDoneValue(restoreRequests.size()));
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