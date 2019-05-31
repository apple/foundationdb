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
ACTOR static Future<Void> distributeWorkloadPerVersionBatchV2(Reference<RestoreMasterData> self, Database cx, RestoreRequest request, VersionBatch versionBatch);
ACTOR static Future<Void> _clearDB(Database cx);
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
			Version ver = wait( processRestoreRequest(it, self, cx) );
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