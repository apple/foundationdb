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

ACTOR static Future<Void> _clearDB(Database cx);
ACTOR static Future<Void> _collectBackupFiles(Reference<RestoreMasterData> self, Database cx, RestoreRequest request);
ACTOR static Future<Version> processRestoreRequest(RestoreRequest request, Reference<RestoreMasterData> self, Database cx);
ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, Database cx, RestoreRequest request, VersionBatch versionBatch);


ACTOR static Future<Standalone<VectorRef<RestoreRequest>>> collectRestoreRequests(Database cx);
ACTOR static Future<Void> initializeVersionBatch(Reference<RestoreMasterData> self);
ACTOR static Future<Void> notifyLoaderAppliersKeyRange(Reference<RestoreMasterData> self);
ACTOR static Future<Void> notifyApplierToApplyMutations(Reference<RestoreMasterData> self);
ACTOR static Future<Void> notifyRestoreCompleted(Reference<RestoreMasterData> self, Database cx);

void dummySampleWorkload(Reference<RestoreMasterData> self);

// The server of the restore master. It drives the restore progress with the following steps:
// 1) Lock database and clear the normal keyspace
// 2) Wait on each RestoreRequest, which is sent by RestoreAgent operated by DBA
// 3) Process each restore request in actor processRestoreRequest;
// 3.1) Sample workload to decide the key range for each applier, which is implemented as a dummy sampling;
// 3.2) Send each loader the map of key-range to applier interface;
// 3.3) Construct requests of which file should be loaded by which loader, and send requests to loaders;
// 4) After process all restore requests, finish restore by cleaning up the restore related system key
//    and ask all restore roles to quit.
ACTOR Future<Void> startRestoreMaster(Reference<RestoreMasterData> self, Database cx) {
	state int checkNum = 0;
	state UID randomUID = g_random->randomUniqueID();

	TraceEvent("FastRestore").detail("RestoreMaster", "WaitOnRestoreRequests");
	state Standalone<VectorRef<RestoreRequest>> restoreRequests = wait( collectRestoreRequests(cx) );

	// lock DB for restore
	wait( lockDatabase(cx,randomUID) );
	wait( _clearDB(cx) );

	// Step: Perform the restore requests
	for ( auto &it : restoreRequests ) {
		TraceEvent("FastRestore").detail("RestoreRequestInfo", it.toString());
		Version ver = wait( processRestoreRequest(it, self, cx) );
	}

	// Step: Notify all restore requests have been handled by cleaning up the restore keys
	wait( notifyRestoreCompleted(self, cx) );

	try {
		wait( unlockDatabase(cx,randomUID) );
	} catch(Error &e) {
		printf(" unlockDB fails. uid:%s\n", randomUID.toString().c_str());
	}
	

	TraceEvent("FastRestore").detail("RestoreMasterComplete", self->id());

	return Void();
}

ACTOR static Future<Version> processRestoreRequest(RestoreRequest request, Reference<RestoreMasterData> self, Database cx) {
	wait( _collectBackupFiles(self, cx, request) );
	self->constructFilesWithVersionRange();
	self->buildVersionBatches();
	state std::map<Version, VersionBatch>::iterator versionBatch;
	for (versionBatch = self->versionBatches.begin(); versionBatch != self->versionBatches.end(); versionBatch++) {
		wait( initializeVersionBatch(self) );
		wait( distributeWorkloadPerVersionBatch(self, cx, request, versionBatch->second) );
	}

	TraceEvent("FastRestore").detail("RestoreCompleted", request.randomUid);
	return request.targetVersion;
}

ACTOR static Future<Void> loadFilesOnLoaders(Reference<RestoreMasterData> self, Database cx, RestoreRequest request, VersionBatch versionBatch, bool isRangeFile) {
	TraceEvent("FastRestore").detail("FileTypeLoadedInVersionBatch", isRangeFile).detail("BeginVersion", versionBatch.beginVersion).detail("EndVersion", versionBatch.endVersion);

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

	Version prevVersion = versionBatch.beginVersion;

	for (auto &file : *files) {
		// NOTE: Cannot skip empty files because empty files, e.g., log file, still need to generate dummy mutation to drive applier's NotifiedVersion (e.g., logVersion and rangeVersion)
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
		param.offset = 0;
		param.length = file.fileSize; // We load file by file, instead of data block by data block for now
		param.blockSize = file.blockSize;
		param.restoreRange = request.range;
		param.addPrefix = request.addPrefix;
		param.removePrefix = request.removePrefix;
		param.mutationLogPrefix = mutationLogPrefix;
		ASSERT_WE_THINK( param.length >= 0 ); // we may load an empty file
		ASSERT_WE_THINK( param.offset >= 0 );
		ASSERT_WE_THINK( param.offset <= file.fileSize );
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

ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, Database cx, RestoreRequest request, VersionBatch versionBatch) {
	ASSERT( !versionBatch.isEmpty() );

	ASSERT( self->loadersInterf.size() > 0 );
	ASSERT( self->appliersInterf.size() > 0 );
	
	dummySampleWorkload(self);
	wait( notifyLoaderAppliersKeyRange(self) );

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
	self->logApplierKeyRange();
}

ACTOR static Future<Standalone<VectorRef<RestoreRequest>>> collectRestoreRequests(Database cx) {
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
				state Standalone<RangeResultRef> restoreRequestValues = wait(tr.getRange(restoreRequestKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!restoreRequestValues.more);
				if(restoreRequestValues.size()) {
					for ( auto &it : restoreRequestValues ) {
						restoreRequests.push_back(restoreRequests.arena(), decodeRestoreRequestValue(it.value));
						printf("Restore Request:%s\n", restoreRequests.back().toString().c_str());
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
	self->initBackupContainer(request.url);
	state BackupDescription desc = wait(self->bc->describeBackup());

	// TODO: Delete this and see if it works
	wait(desc.resolveVersionTimes(cx));

	printf("[INFO] Backup Description\n%s", desc.toString().c_str());
	if(request.targetVersion == invalidVersion && desc.maxRestorableVersion.present())
		request.targetVersion = desc.maxRestorableVersion.get();

	Optional<RestorableFileSet> restorable = wait(self->bc->getRestoreSet(request.targetVersion));

	if(!restorable.present()) {
		TraceEvent(SevWarn, "FastRestore").detail("NotRestorable", request.targetVersion);
		throw restore_missing_data();
	}

	if (!self->files.empty()) {
		TraceEvent(SevError, "FastRestore").detail("ClearOldFiles", self->files.size());
		self->files.clear();
	}

 	for(const RangeFile &f : restorable.get().ranges) {
 		TraceEvent("FastRestore").detail("RangeFile", f.toString());
		RestoreFileFR file(f.version, f.fileName, true, f.blockSize, f.fileSize, f.version, f.version);
 		self->files.push_back(file);
 	}
 	for(const LogFile &f : restorable.get().logs) {
 		TraceEvent("FastRestore").detail("LogFile", f.toString());
		RestoreFileFR file(f.beginVersion, f.fileName, false, f.blockSize, f.fileSize, f.endVersion, f.beginVersion);
		self->files.push_back(file);
 	}

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


ACTOR static Future<Void> initializeVersionBatch(Reference<RestoreMasterData> self) {

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

// Ask each applier to apply its received mutations to DB
ACTOR static Future<Void> notifyApplierToApplyMutations(Reference<RestoreMasterData> self) {
	// Prepare the applyToDB requests
	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;
	for (auto& applier : self->appliersInterf) {
		requests.push_back( std::make_pair(applier.first, RestoreVersionBatchRequest(self->batchIndex)) );
	}
	wait( sendBatchRequests(&RestoreApplierInterface::applyToDB, self->appliersInterf, requests) );

	TraceEvent("FastRestore").detail("Master", self->id()).detail("ApplyToDB", "Completed");
	return Void();
}

// Send the map of key-range to applier to each loader
ACTOR static Future<Void> notifyLoaderAppliersKeyRange(Reference<RestoreMasterData> self)  {
	std::vector<std::pair<UID, RestoreSetApplierKeyRangeVectorRequest>> requests;
	for (auto& loader : self->loadersInterf) {
		requests.push_back(std::make_pair(loader.first, RestoreSetApplierKeyRangeVectorRequest(self->range2Applier)) );
	}
	wait( sendBatchRequests(&RestoreLoaderInterface::setApplierKeyRangeVectorRequest, self->loadersInterf, requests) );

	return Void();
}

// Ask all loaders and appliers to perform housecleaning at the end of restore and
// Register the restoreRequestDoneKey to signal the end of restore
ACTOR static Future<Void> notifyRestoreCompleted(Reference<RestoreMasterData> self, Database cx) {
	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;
	for ( auto &loader : self->loadersInterf ) {
		requests.push_back( std::make_pair(loader.first, RestoreVersionBatchRequest(self->batchIndex)) );
	}
	wait( sendBatchRequests(&RestoreLoaderInterface::finishRestore, self->loadersInterf, requests) );

	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;
	for ( auto &applier : self->appliersInterf ) {
		requests.push_back( std::make_pair(applier.first, RestoreVersionBatchRequest(self->batchIndex)) );
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

	TraceEvent("FastRestore").detail("RestoreMaster", "RestoreCompleted");

	return Void();
}