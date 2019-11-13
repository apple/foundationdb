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

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR static Future<Void> clearDB(Database cx);
ACTOR static Future<Void> collectBackupFiles(Reference<IBackupContainer> bc, std::vector<RestoreFileFR>* files,
                                              Database cx, RestoreRequest request);

ACTOR static Future<Version> processRestoreRequest(Reference<RestoreMasterData> self, Database cx, RestoreRequest request);
ACTOR static Future<Void> startProcessRestoreRequests(Reference<RestoreMasterData> self, Database cx);
ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, Database cx,
                                                            RestoreRequest request, VersionBatch versionBatch);

ACTOR static Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> masterWorker,
                                              Reference<RestoreMasterData> masterData);
ACTOR static Future<Void> distributeRestoreSysInfo(Reference<RestoreWorkerData> masterWorker,
                                                   Reference<RestoreMasterData> masterData);

ACTOR static Future<Standalone<VectorRef<RestoreRequest>>> collectRestoreRequests(Database cx);
ACTOR static Future<Void> initializeVersionBatch(Reference<RestoreMasterData> self);
ACTOR static Future<Void> notifyLoaderAppliersKeyRange(Reference<RestoreMasterData> self);
ACTOR static Future<Void> notifyApplierToApplyMutations(Reference<RestoreMasterData> self);
ACTOR static Future<Void> notifyRestoreCompleted(Reference<RestoreMasterData> self, Database cx);

void dummySampleWorkload(Reference<RestoreMasterData> self);

ACTOR Future<Void> startRestoreMaster(Reference<RestoreWorkerData> masterWorker, Database cx) {
	state Reference<RestoreMasterData> self = Reference<RestoreMasterData>(new RestoreMasterData());

	try {
		// recruitRestoreRoles must come after masterWorker has finished collectWorkerInterface
		wait(recruitRestoreRoles(masterWorker, self));

		wait(distributeRestoreSysInfo(masterWorker, self));

		wait(startProcessRestoreRequests(self, cx));
	} catch (Error& e) {
		TraceEvent(SevError, "FastRestore")
		    .detail("StartRestoreMaster", "Unexpectedly unhandled error")
		    .detail("Error", e.what())
		    .detail("ErrorCode", e.code());
	}

	return Void();
}

// RestoreWorker that has restore master role: Recruite a role for each worker
ACTOR Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> masterWorker,
                                       Reference<RestoreMasterData> masterData) {
	TraceEvent("FastRestore")
	    .detail("RecruitRestoreRoles", masterWorker->workerInterfaces.size())
	    .detail("NumLoaders", opConfig.num_loaders)
	    .detail("NumAppliers", opConfig.num_appliers);
	ASSERT(masterData->loadersInterf.empty() && masterData->appliersInterf.empty());

	ASSERT(masterData.isValid());
	ASSERT(opConfig.num_loaders > 0 && opConfig.num_appliers > 0);
	// We assign 1 role per worker for now
	ASSERT(opConfig.num_loaders + opConfig.num_appliers <= masterWorker->workerInterfaces.size());

	// Assign a role to each worker
	state int nodeIndex = 0;
	state RestoreRole role;
	std::map<UID, RestoreRecruitRoleRequest> requests;
	for (auto& workerInterf : masterWorker->workerInterfaces) {
		if (nodeIndex >= 0 && nodeIndex < opConfig.num_appliers) {
			// [0, numApplier) are appliers
			role = RestoreRole::Applier;
		} else if (nodeIndex >= opConfig.num_appliers && nodeIndex < opConfig.num_loaders + opConfig.num_appliers) {
			// [numApplier, numApplier + numLoader) are loaders
			role = RestoreRole::Loader;
		} else {
			break;
		}

		TraceEvent("FastRestore")
		    .detail("Role", getRoleStr(role))
		    .detail("NodeIndex", nodeIndex)
		    .detail("WorkerNode", workerInterf.first);
		requests[workerInterf.first] = RestoreRecruitRoleRequest(role, nodeIndex);
		nodeIndex++;
	}

	state std::vector<RestoreRecruitRoleReply> replies;
	wait(getBatchReplies(&RestoreWorkerInterface::recruitRole, masterWorker->workerInterfaces, requests, &replies));
	for (auto& reply : replies) {
		if (reply.role == RestoreRole::Applier) {
			ASSERT_WE_THINK(reply.applier.present());
			masterData->appliersInterf[reply.applier.get().id()] = reply.applier.get();
		} else if (reply.role == RestoreRole::Loader) {
			ASSERT_WE_THINK(reply.loader.present());
			masterData->loadersInterf[reply.loader.get().id()] = reply.loader.get();
		} else {
			TraceEvent(SevError, "FastRestore").detail("RecruitRestoreRoles_InvalidRole", reply.role);
		}
	}
	TraceEvent("FastRestore").detail("RecruitRestoreRolesDone", masterWorker->workerInterfaces.size());

	return Void();
}

ACTOR Future<Void> distributeRestoreSysInfo(Reference<RestoreWorkerData> masterWorker,
                                            Reference<RestoreMasterData> masterData) {
	ASSERT(masterData.isValid());
	ASSERT(!masterData->loadersInterf.empty());
	RestoreSysInfo sysInfo(masterData->appliersInterf);
	std::vector<std::pair<UID, RestoreSysInfoRequest>> requests;
	for (auto& loader : masterData->loadersInterf) {
		requests.push_back(std::make_pair(loader.first, RestoreSysInfoRequest(sysInfo)));
	}

	TraceEvent("FastRestore").detail("DistributeRestoreSysInfoToLoaders", masterData->loadersInterf.size());
	wait(sendBatchRequests(&RestoreLoaderInterface::updateRestoreSysInfo, masterData->loadersInterf, requests));

	return Void();
}

// The server of the restore master. It drives the restore progress with the following steps:
// 1) Lock database and clear the normal keyspace
// 2) Wait on each RestoreRequest, which is sent by RestoreAgent operated by DBA
// 3) Process each restore request in actor processRestoreRequest;
// 3.1) Sample workload to decide the key range for each applier, which is implemented as a dummy sampling;
// 3.2) Send each loader the map of key-range to applier interface;
// 3.3) Construct requests of which file should be loaded by which loader, and send requests to loaders;
// 4) After process all restore requests, finish restore by cleaning up the restore related system key
//    and ask all restore roles to quit.
ACTOR Future<Void> startProcessRestoreRequests(Reference<RestoreMasterData> self, Database cx) {
	state UID randomUID = deterministicRandom()->randomUniqueID();
	TraceEvent("FastRestore").detail("RestoreMaster", "WaitOnRestoreRequests");
	state Standalone<VectorRef<RestoreRequest>> restoreRequests = wait(collectRestoreRequests(cx));

	// lock DB for restore
	state int numTries = 0;
	loop {
		try {
			wait(lockDatabase(cx, randomUID));
			state Reference<ReadYourWritesTransaction> tr =
			    Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkDatabaseLock(tr, randomUID));
			TraceEvent("FastRestore").detail("DBIsLocked", randomUID);
			break;
		} catch (Error& e) {
			TraceEvent("FastRestore").detail("CheckLockError", e.what());
			TraceEvent(numTries > 50 ? SevError : SevWarnAlways, "FastRestoreMayFail")
			    .detail("Reason", "DB is not properly locked")
			    .detail("ExpectedLockID", randomUID);
			numTries++;
			wait(delay(5.0));
		}
	}

	wait(clearDB(cx));

	// Step: Perform the restore requests
	state int restoreIndex = 0;
	try {
		for (restoreIndex = 0; restoreIndex < restoreRequests.size(); restoreIndex++) {
			RestoreRequest& request = restoreRequests[restoreIndex];
			TraceEvent("FastRestore").detail("RestoreRequestInfo", request.toString());
			wait(success(processRestoreRequest(self, cx, request)));
		}
	} catch (Error& e) {
		TraceEvent(SevError, "FastRestoreFailed").detail("RestoreRequest", restoreRequests[restoreIndex].toString());
	}

	// Step: Notify all restore requests have been handled by cleaning up the restore keys
	wait(notifyRestoreCompleted(self, cx));

	try {
		wait(unlockDatabase(cx, randomUID));
	} catch (Error& e) {
		TraceEvent(SevError, "UnlockDBFailed").detail("UID", randomUID.toString());
		ASSERT_WE_THINK(false); // This unlockDatabase should always succeed, we think.
	}


	TraceEvent("FastRestore").detail("RestoreMasterComplete", self->id());

	return Void();
}

ACTOR static Future<Version> processRestoreRequest(Reference<RestoreMasterData> self, Database cx,
                                                   RestoreRequest request) {
	state std::vector<RestoreFileFR> files;
	state std::vector<RestoreFileFR> allFiles;

	self->initBackupContainer(request.url);

	// Get all backup files' description and save them to files
	wait(collectBackupFiles(self->bc, &files, cx, request));
	self->buildVersionBatches(files, &self->versionBatches); // Divide files into version batches

	state std::map<Version, VersionBatch>::iterator versionBatch;
	for (versionBatch = self->versionBatches.begin(); versionBatch != self->versionBatches.end(); versionBatch++) {
		wait(initializeVersionBatch(self));
		wait(distributeWorkloadPerVersionBatch(self, cx, request, versionBatch->second));
		self->batchIndex++;
	}

	TraceEvent("FastRestore").detail("RestoreToVersion", request.targetVersion);
	return request.targetVersion;
}

ACTOR static Future<Void> loadFilesOnLoaders(Reference<RestoreMasterData> self, Database cx, RestoreRequest request,
                                             VersionBatch versionBatch, bool isRangeFile) {
	TraceEvent("FastRestore")
	    .detail("FileTypeLoadedInVersionBatch", isRangeFile)
	    .detail("BeginVersion", versionBatch.beginVersion)
	    .detail("EndVersion", versionBatch.endVersion);

	Key mutationLogPrefix;
	std::vector<RestoreFileFR>* files;
	if (isRangeFile) {
		files = &versionBatch.rangeFiles;
	} else {
		files = &versionBatch.logFiles;
		Reference<RestoreConfigFR> restoreConfig(new RestoreConfigFR(request.randomUid));
		mutationLogPrefix = restoreConfig->mutationLogPrefix();
	}

	// sort files in increasing order of beginVersion
	std::sort(files->begin(), files->end());

	std::vector<std::pair<UID, RestoreLoadFileRequest>> requests;
	std::map<UID, RestoreLoaderInterface>::iterator loader = self->loadersInterf.begin();

	// TODO: Remove files that are empty before proceed
	// ASSERT(files->size() > 0); // files should not be empty

	Version prevVersion = 0;
	for (auto& file : *files) {
		// NOTE: Cannot skip empty files because empty files, e.g., log file, still need to generate dummy mutation to
		// drive applier's NotifiedVersion.
		if (loader == self->loadersInterf.end()) {
			loader = self->loadersInterf.begin();
		}
		// Prepare loading
		LoadingParam param;

		param.prevVersion = 0; // Each file's NotifiedVersion starts from 0
		param.endVersion = file.isRange ? file.version : file.endVersion;
		param.fileIndex = file.fileIndex;

		param.url = request.url;
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

		prevVersion = param.endVersion;

		// Log file to be loaded
		TraceEvent("FastRestore").detail("LoadParam", param.toString()).detail("LoaderID", loader->first.toString());
		ASSERT_WE_THINK(param.length >= 0); // we may load an empty file
		ASSERT_WE_THINK(param.offset >= 0);
		ASSERT_WE_THINK(param.offset <= file.fileSize);
		ASSERT_WE_THINK(param.prevVersion <= param.endVersion);

		requests.push_back(std::make_pair(loader->first, RestoreLoadFileRequest(param)));
		loader++;
	}

	// Wait on the batch of load files or log files
	wait(sendBatchRequests(&RestoreLoaderInterface::loadFile, self->loadersInterf, requests));

	return Void();
}

ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, Database cx,
                                                            RestoreRequest request, VersionBatch versionBatch) {
	ASSERT(!versionBatch.isEmpty());

	ASSERT(self->loadersInterf.size() > 0);
	ASSERT(self->appliersInterf.size() > 0);

	dummySampleWorkload(self);
	wait(notifyLoaderAppliersKeyRange(self));

	// Parse log files and send mutations to appliers before we parse range files
	wait(loadFilesOnLoaders(self, cx, request, versionBatch, false));
	wait(loadFilesOnLoaders(self, cx, request, versionBatch, true));

	wait(notifyApplierToApplyMutations(self));

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
		keyrangeSplitter.push_back(deterministicRandom()->randomUniqueID());
	}
	std::sort(keyrangeSplitter.begin(), keyrangeSplitter.end());
	i = 0;
	for (auto& applier : self->appliersInterf) {
		if (i == 0) {
			self->rangeToApplier[normalKeys.begin] = applier.first;
		} else {
			self->rangeToApplier[StringRef(keyrangeSplitter[i].toString())] = applier.first;
		}
	}
	self->logApplierKeyRange();
}

ACTOR static Future<Standalone<VectorRef<RestoreRequest>>> collectRestoreRequests(Database cx) {
	state Standalone<VectorRef<RestoreRequest>> restoreRequests;
	state Future<Void> watch4RestoreRequest;

	// wait for the restoreRequestTriggerKey to be set by the client/test workload
	state ReadYourWritesTransaction tr(cx);
	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> numRequests = wait(tr.get(restoreRequestTriggerKey));
			if (!numRequests.present()) {
				watch4RestoreRequest = tr.watch(restoreRequestTriggerKey);
				wait(tr.commit());
				wait(watch4RestoreRequest);
			} else {
				Standalone<RangeResultRef> restoreRequestValues =
				    wait(tr.getRange(restoreRequestKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!restoreRequestValues.more);
				if (restoreRequestValues.size()) {
					for (auto& it : restoreRequestValues) {
						restoreRequests.push_back(restoreRequests.arena(), decodeRestoreRequestValue(it.value));
						TraceEvent("FastRestore").detail("RestoreRequest", restoreRequests.back().toString());
					}
				}
				break;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return restoreRequests;
}

// Collect the backup files' description into output_files by reading the backupContainer bc.
ACTOR static Future<Void> collectBackupFiles(Reference<IBackupContainer> bc, std::vector<RestoreFileFR>* files,
                                              Database cx, RestoreRequest request) {
	state BackupDescription desc = wait(bc->describeBackup());

	// Convert version to real time for operators to read the BackupDescription desc.
	wait(desc.resolveVersionTimes(cx));
	TraceEvent("FastRestore").detail("BackupDesc", desc.toString());

	if (request.targetVersion == invalidVersion && desc.maxRestorableVersion.present())
		request.targetVersion = desc.maxRestorableVersion.get();

	Optional<RestorableFileSet> restorable = wait(bc->getRestoreSet(request.targetVersion));

	if (!restorable.present()) {
		TraceEvent(SevWarn, "FastRestore").detail("NotRestorable", request.targetVersion);
		throw restore_missing_data();
	}

	if (!files->empty()) {
		TraceEvent(SevError, "FastRestore").detail("ClearOldFiles", files->size());
		files->clear();
	}

	for (const RangeFile& f : restorable.get().ranges) {
		TraceEvent("FastRestore").detail("RangeFile", f.toString());
		RestoreFileFR file(f.version, f.fileName, true, f.blockSize, f.fileSize, f.version, f.version);
		TraceEvent("FastRestore").detail("RangeFileFR", file.toString());
		files->push_back(file);
	}
	for (const LogFile& f : restorable.get().logs) {
		TraceEvent("FastRestore").detail("LogFile", f.toString());
		RestoreFileFR file(f.beginVersion, f.fileName, false, f.blockSize, f.fileSize, f.endVersion, f.beginVersion);
		TraceEvent("FastRestore").detail("LogFileFR", file.toString());
		files->push_back(file);
	}

	return Void();
}

ACTOR static Future<Void> clearDB(Database cx) {
	wait(runRYWTransaction(cx, [](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->clear(normalKeys);
		return Void();
	}));

	return Void();
}

ACTOR static Future<Void> initializeVersionBatch(Reference<RestoreMasterData> self) {

	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requestsToAppliers;
	for (auto& applier : self->appliersInterf) {
		requestsToAppliers.push_back(std::make_pair(applier.first, RestoreVersionBatchRequest(self->batchIndex)));
	}
	wait(sendBatchRequests(&RestoreApplierInterface::initVersionBatch, self->appliersInterf, requestsToAppliers));

	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requestsToLoaders;
	for (auto& loader : self->loadersInterf) {
		requestsToLoaders.push_back(std::make_pair(loader.first, RestoreVersionBatchRequest(self->batchIndex)));
	}
	wait(sendBatchRequests(&RestoreLoaderInterface::initVersionBatch, self->loadersInterf, requestsToLoaders));

	return Void();
}

// Ask each applier to apply its received mutations to DB
ACTOR static Future<Void> notifyApplierToApplyMutations(Reference<RestoreMasterData> self) {
	// Prepare the applyToDB requests
	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;
	for (auto& applier : self->appliersInterf) {
		requests.push_back(std::make_pair(applier.first, RestoreVersionBatchRequest(self->batchIndex)));
	}
	wait(sendBatchRequests(&RestoreApplierInterface::applyToDB, self->appliersInterf, requests));

	TraceEvent("FastRestore").detail("Master", self->id()).detail("ApplyToDB", "Completed");
	return Void();
}

// Send the map of key-range to applier to each loader
ACTOR static Future<Void> notifyLoaderAppliersKeyRange(Reference<RestoreMasterData> self) {
	std::vector<std::pair<UID, RestoreSetApplierKeyRangeVectorRequest>> requests;
	for (auto& loader : self->loadersInterf) {
		requests.push_back(std::make_pair(loader.first, RestoreSetApplierKeyRangeVectorRequest(self->rangeToApplier)));
	}
	wait(sendBatchRequests(&RestoreLoaderInterface::setApplierKeyRangeVectorRequest, self->loadersInterf, requests));

	return Void();
}

// Ask all loaders and appliers to perform housecleaning at the end of restore and
// Register the restoreRequestDoneKey to signal the end of restore
ACTOR static Future<Void> notifyRestoreCompleted(Reference<RestoreMasterData> self, Database cx) {
	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;
	for (auto& loader : self->loadersInterf) {
		requests.push_back(std::make_pair(loader.first, RestoreVersionBatchRequest(self->batchIndex)));
	}
	// A loader exits immediately after it receives the request. Master may not receive acks.
	Future<Void> endLoaders = sendBatchRequests(&RestoreLoaderInterface::finishRestore, self->loadersInterf, requests);

	requests.clear();
	for (auto& applier : self->appliersInterf) {
		requests.push_back(std::make_pair(applier.first, RestoreVersionBatchRequest(self->batchIndex)));
	}
	Future<Void> endApplier =
	    sendBatchRequests(&RestoreApplierInterface::finishRestore, self->appliersInterf, requests);

	wait(delay(5.0)); // Give some time for loaders and appliers to exit

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
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	TraceEvent("FastRestore").detail("RestoreMaster", "RestoreCompleted");

	return Void();
}
