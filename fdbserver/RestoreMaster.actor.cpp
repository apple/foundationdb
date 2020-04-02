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
ACTOR static Future<Version> collectBackupFiles(Reference<IBackupContainer> bc, std::vector<RestoreFileFR>* rangeFiles,
                                                std::vector<RestoreFileFR>* logFiles, Database cx,
                                                RestoreRequest request);

ACTOR static Future<Version> processRestoreRequest(Reference<RestoreMasterData> self, Database cx, RestoreRequest request);
ACTOR static Future<Void> startProcessRestoreRequests(Reference<RestoreMasterData> self, Database cx);
ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, int batchIndex,
                                                            Database cx, RestoreRequest request,
                                                            VersionBatch versionBatch);

ACTOR static Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> masterWorker,
                                              Reference<RestoreMasterData> masterData);
ACTOR static Future<Void> distributeRestoreSysInfo(Reference<RestoreWorkerData> masterWorker,
                                                   Reference<RestoreMasterData> masterData);

ACTOR static Future<Standalone<VectorRef<RestoreRequest>>> collectRestoreRequests(Database cx);
ACTOR static Future<Void> initializeVersionBatch(std::map<UID, RestoreApplierInterface> appliersInterf,
                                                 std::map<UID, RestoreLoaderInterface> loadersInterf, int batchIndex);
ACTOR static Future<Void> notifyApplierToApplyMutations(Reference<MasterBatchData> batchData,
                                                        Reference<MasterBatchStatus> batchStatus,
                                                        std::map<UID, RestoreApplierInterface> appliersInterf,
                                                        int batchIndex, NotifiedVersion* finishedBatch);
ACTOR static Future<Void> notifyLoadersVersionBatchFinished(std::map<UID, RestoreLoaderInterface> loadersInterf,
                                                            int batchIndex);
ACTOR static Future<Void> notifyRestoreCompleted(Reference<RestoreMasterData> self, bool terminate);
ACTOR static Future<Void> signalRestoreCompleted(Reference<RestoreMasterData> self, Database cx);
ACTOR static Future<Void> updateHeartbeatTime(Reference<RestoreMasterData> self);
ACTOR static Future<Void> checkRolesLiveness(Reference<RestoreMasterData> self);

void splitKeyRangeForAppliers(Reference<MasterBatchData> batchData,
                              std::map<UID, RestoreApplierInterface> appliersInterf, int batchIndex);

ACTOR Future<Void> startRestoreMaster(Reference<RestoreWorkerData> masterWorker, Database cx) {
	state Reference<RestoreMasterData> self = Reference<RestoreMasterData>(new RestoreMasterData());
	state ActorCollectionNoErrors actors;

	try {
		// recruitRestoreRoles must come after masterWorker has finished collectWorkerInterface
		wait(recruitRestoreRoles(masterWorker, self));

		actors.add(updateHeartbeatTime(self));
		actors.add(checkRolesLiveness(self));

		wait(distributeRestoreSysInfo(masterWorker, self));

		wait(startProcessRestoreRequests(self, cx));
	} catch (Error& e) {
		if (e.code() != error_code_operation_cancelled) {
			TraceEvent(SevError, "FastRestoreMasterStart").detail("Reason", "Unexpected unhandled error").error(e);
		}
	}

	return Void();
}

// RestoreWorker that has restore master role: Recruite a role for each worker
ACTOR Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> masterWorker,
                                       Reference<RestoreMasterData> masterData) {
	state int nodeIndex = 0;
	state RestoreRole role = RestoreRole::Invalid;

	TraceEvent("FastRestoreMaster", masterData->id())
	    .detail("RecruitRestoreRoles", masterWorker->workerInterfaces.size())
	    .detail("NumLoaders", SERVER_KNOBS->FASTRESTORE_NUM_LOADERS)
	    .detail("NumAppliers", SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS);
	ASSERT(masterData->loadersInterf.empty() && masterData->appliersInterf.empty());

	ASSERT(masterData.isValid());
	ASSERT(SERVER_KNOBS->FASTRESTORE_NUM_LOADERS > 0 && SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS > 0);
	// We assign 1 role per worker for now
	ASSERT(SERVER_KNOBS->FASTRESTORE_NUM_LOADERS + SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS <=
	       masterWorker->workerInterfaces.size());

	// Assign a role to each worker
	std::vector<std::pair<UID, RestoreRecruitRoleRequest>> requests;
	for (auto& workerInterf : masterWorker->workerInterfaces) {
		if (nodeIndex >= 0 && nodeIndex < SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS) {
			// [0, numApplier) are appliers
			role = RestoreRole::Applier;
		} else if (nodeIndex >= SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS &&
		           nodeIndex < SERVER_KNOBS->FASTRESTORE_NUM_LOADERS + SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS) {
			// [numApplier, numApplier + numLoader) are loaders
			role = RestoreRole::Loader;
		} else {
			break;
		}

		TraceEvent("FastRestoreMaster", masterData->id()).detail("WorkerNode", workerInterf.first);
		requests.emplace_back(workerInterf.first, RestoreRecruitRoleRequest(role, nodeIndex));
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
			TraceEvent(SevError, "FastRestoreMaster").detail("RecruitRestoreRolesInvalidRole", reply.role);
		}
	}
	TraceEvent("FastRestoreRecruitRestoreRolesDone", masterData->id())
	    .detail("Workers", masterWorker->workerInterfaces.size())
	    .detail("RecruitedRoles", replies.size());

	return Void();
}

ACTOR Future<Void> distributeRestoreSysInfo(Reference<RestoreWorkerData> masterWorker,
                                            Reference<RestoreMasterData> masterData) {
	ASSERT(masterData.isValid());
	ASSERT(!masterData->loadersInterf.empty());
	RestoreSysInfo sysInfo(masterData->appliersInterf);
	std::vector<std::pair<UID, RestoreSysInfoRequest>> requests;
	for (auto& loader : masterData->loadersInterf) {
		requests.emplace_back(loader.first, RestoreSysInfoRequest(sysInfo));
	}

	TraceEvent("FastRestoreDistributeRestoreSysInfoToLoaders", masterData->id())
	    .detail("Loaders", masterData->loadersInterf.size());
	wait(sendBatchRequests(&RestoreLoaderInterface::updateRestoreSysInfo, masterData->loadersInterf, requests));
	TraceEvent("FastRestoreDistributeRestoreSysInfoToLoadersDone", masterData->id())
	    .detail("Loaders", masterData->loadersInterf.size());

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
	state Standalone<VectorRef<RestoreRequest>> restoreRequests = wait(collectRestoreRequests(cx));
	state int numTries = 0;
	state int restoreIndex = 0;

	TraceEvent("FastRestoreMasterWaitOnRestoreRequests", self->id());

	// DB has been locked where restore request is submitted
	wait(clearDB(cx));

	// Step: Perform the restore requests
	try {
		for (restoreIndex = 0; restoreIndex < restoreRequests.size(); restoreIndex++) {
			RestoreRequest& request = restoreRequests[restoreIndex];
			TraceEvent("FastRestoreMasterProcessRestoreRequests", self->id())
			    .detail("RestoreRequestInfo", request.toString());
			// TODO: Initialize MasterData and all loaders and appliers' data for each restore request!
			self->resetPerRestoreRequest();
			wait(success(processRestoreRequest(self, cx, request)));
			wait(notifyRestoreCompleted(self, false));
		}
	} catch (Error& e) {
		if (restoreIndex < restoreRequests.size()) {
			TraceEvent(SevError, "FastRestoreMasterProcessRestoreRequestsFailed", self->id())
			    .detail("RestoreRequest", restoreRequests[restoreIndex].toString())
			    .error(e);
		} else {
			TraceEvent(SevError, "FastRestoreMasterProcessRestoreRequestsFailed", self->id())
			    .detail("RestoreRequests", restoreRequests.size())
			    .detail("RestoreIndex", restoreIndex)
			    .error(e);
		}
	}

	// Step: Notify all restore requests have been handled by cleaning up the restore keys
	wait(signalRestoreCompleted(self, cx));

	TraceEvent("FastRestoreMasterRestoreCompleted", self->id());

	return Void();
}

ACTOR static Future<Void> monitorFinishedVersion(Reference<RestoreMasterData> self, RestoreRequest request) {
	loop {
		TraceEvent("FastRestoreMonitorFinishedVersion", self->id())
		    .detail("RestoreRequest", request.toString())
		    .detail("BatchIndex", self->finishedBatch.get());
		wait(delay(SERVER_KNOBS->FASTRESTORE_VB_MONITOR_DELAY));
	}
}

ACTOR static Future<Version> processRestoreRequest(Reference<RestoreMasterData> self, Database cx,
                                                   RestoreRequest request) {
	state std::vector<RestoreFileFR> rangeFiles;
	state std::vector<RestoreFileFR> logFiles;
	state std::vector<RestoreFileFR> allFiles;
	state ActorCollection actors(false);

	self->initBackupContainer(request.url);

	// Get all backup files' description and save them to files
	Version targetVersion = wait(collectBackupFiles(self->bc, &rangeFiles, &logFiles, cx, request));
	ASSERT(targetVersion > 0);

	std::sort(rangeFiles.begin(), rangeFiles.end());
	std::sort(logFiles.begin(), logFiles.end(), [](RestoreFileFR const& f1, RestoreFileFR const& f2) -> bool {
		return std::tie(f1.endVersion, f1.beginVersion, f1.fileIndex, f1.fileName) <
		       std::tie(f2.endVersion, f2.beginVersion, f2.fileIndex, f2.fileName);
	});

	// Divide files into version batches.
	self->buildVersionBatches(rangeFiles, logFiles, &self->versionBatches, targetVersion);
	self->dumpVersionBatches(self->versionBatches);

	state std::vector<Future<Void>> fBatches;
	state std::vector<VersionBatch> versionBatches; // To randomize invoking order of version batchs
	for (auto& vb : self->versionBatches) {
		versionBatches.push_back(vb.second);
	}

	if (g_network->isSimulated() && deterministicRandom()->random01() < 0.5) {
		// Randomize invoking order of version batches
		int permTimes = deterministicRandom()->randomInt(0, 100);
		while (permTimes-- > 0) {
			std::next_permutation(versionBatches.begin(), versionBatches.end());
		}
	}

	actors.add(monitorFinishedVersion(self, request));
	state std::vector<VersionBatch>::iterator versionBatch = versionBatches.begin();
	for (; versionBatch != versionBatches.end(); versionBatch++) {
		while (self->runningVersionBatches.get() >= SERVER_KNOBS->FASTRESTORE_VB_PARALLELISM) {
			// Control how many batches can be processed in parallel. Avoid dead lock due to OOM on loaders
			TraceEvent("FastRestoreMasterDispatchVersionBatches")
			    .detail("WaitOnRunningVersionBatches", self->runningVersionBatches.get());
			wait(self->runningVersionBatches.onChange());
		}
		int batchIndex = versionBatch->batchIndex;
		TraceEvent("FastRestoreMasterDispatchVersionBatches")
		    .detail("BatchIndex", batchIndex)
		    .detail("BatchSize", versionBatch->size)
		    .detail("RunningVersionBatches", self->runningVersionBatches.get());
		self->batch[batchIndex] = Reference<MasterBatchData>(new MasterBatchData());
		self->batchStatus[batchIndex] = Reference<MasterBatchStatus>(new MasterBatchStatus());
		fBatches.push_back(distributeWorkloadPerVersionBatch(self, batchIndex, cx, request, *versionBatch));
		// Wait a bit to give the current version batch a head start from the next version batch
		wait(delay(SERVER_KNOBS->FASTRESTORE_VB_LAUNCH_DELAY));
	}

	wait(waitForAll(fBatches));

	TraceEvent("FastRestore").detail("RestoreToVersion", request.targetVersion);
	return request.targetVersion;
}

ACTOR static Future<Void> loadFilesOnLoaders(Reference<MasterBatchData> batchData,
                                             Reference<MasterBatchStatus> batchStatus,
                                             std::map<UID, RestoreLoaderInterface> loadersInterf, int batchIndex,
                                             Database cx, RestoreRequest request, VersionBatch versionBatch,
                                             bool isRangeFile) {
	// set is internally sorted
	std::set<RestoreFileFR>* files = isRangeFile ? &versionBatch.rangeFiles : &versionBatch.logFiles;

	TraceEvent("FastRestoreMasterPhaseLoadFilesStart")
	    .detail("BatchIndex", batchIndex)
	    .detail("FileTypeLoadedInVersionBatch", isRangeFile)
	    .detail("BeginVersion", versionBatch.beginVersion)
	    .detail("EndVersion", versionBatch.endVersion)
	    .detail("Files", (files != nullptr ? files->size() : -1));

	std::vector<std::pair<UID, RestoreLoadFileRequest>> requests;
	std::map<UID, RestoreLoaderInterface>::iterator loader = loadersInterf.begin();
	state std::vector<RestoreAsset> assets; // all assets loaded, used for sanity check restore progress

	// Balance workload on loaders for parsing range and log files across version batches
	int random = deterministicRandom()->randomInt(0, loadersInterf.size());
	while (random-- > 0) {
		loader++;
	}

	int paramIdx = 0;
	for (auto& file : *files) {
		if (loader == loadersInterf.end()) {
			loader = loadersInterf.begin();
		}
		// Prepare loading
		LoadingParam param;
		param.url = request.url;
		param.isRangeFile = file.isRange;
		param.rangeVersion = file.isRange ? file.version : -1;
		param.blockSize = file.blockSize;

		param.asset.uid = deterministicRandom()->randomUniqueID();
		param.asset.filename = file.fileName;
		param.asset.fileIndex = file.fileIndex;
		param.asset.partitionId = file.partitionId;
		param.asset.offset = 0;
		param.asset.len = file.fileSize;
		param.asset.range = request.range;
		param.asset.beginVersion = versionBatch.beginVersion;
		param.asset.endVersion = (isRangeFile || request.targetVersion == -1)
		                             ? versionBatch.endVersion
		                             : std::min(versionBatch.endVersion, request.targetVersion + 1);

		TraceEvent("FastRestoreMasterPhaseLoadFiles")
		    .detail("BatchIndex", batchIndex)
		    .detail("LoadParamIndex", paramIdx)
		    .detail("LoaderID", loader->first.toString())
		    .detail("LoadParam", param.toString());
		ASSERT_WE_THINK(param.asset.len > 0);
		ASSERT_WE_THINK(param.asset.offset >= 0);
		ASSERT_WE_THINK(param.asset.offset <= file.fileSize);
		ASSERT_WE_THINK(param.asset.beginVersion <= param.asset.endVersion);

		requests.emplace_back(loader->first, RestoreLoadFileRequest(batchIndex, param));
		// Restore asset should only be loaded exactly once.
		if (batchStatus->raStatus.find(param.asset) != batchStatus->raStatus.end()) {
			TraceEvent(SevError, "FastRestoreMasterPhaseLoadFiles")
			    .detail("LoadingParam", param.toString())
			    .detail("RestoreAssetAlreadyProcessed", batchStatus->raStatus[param.asset]);
		}
		batchStatus->raStatus[param.asset] = RestoreAssetStatus::Loading;
		assets.push_back(param.asset);
		++loader;
		++paramIdx;
	}
	TraceEvent(files->size() != paramIdx ? SevError : SevInfo, "FastRestoreMasterPhaseLoadFiles")
	    .detail("Files", files->size())
	    .detail("LoadParams", paramIdx);

	state std::vector<RestoreLoadFileReply> replies;
	// Wait on the batch of load files or log files
	wait(getBatchReplies(&RestoreLoaderInterface::loadFile, loadersInterf, requests, &replies,
	                     TaskPriority::RestoreLoaderLoadFiles));

	TraceEvent("FastRestoreMasterPhaseLoadFilesReply")
	    .detail("BatchIndex", batchIndex)
	    .detail("SamplingReplies", replies.size());
	for (auto& reply : replies) {
		// Update and sanity check restore asset's status
		RestoreAssetStatus status = batchStatus->raStatus[reply.param.asset];
		if (status == RestoreAssetStatus::Loading && !reply.isDuplicated) {
			batchStatus->raStatus[reply.param.asset] = RestoreAssetStatus::Loaded;
		} else if (status == RestoreAssetStatus::Loading && reply.isDuplicated) {
			// Duplicate request wait on the restore asset to be processed before it replies
			batchStatus->raStatus[reply.param.asset] = RestoreAssetStatus::Loaded;
			TraceEvent(SevWarn, "FastRestoreMasterPhaseLoadFilesReply")
			    .detail("RestoreAsset", reply.param.asset.toString())
			    .detail("DuplicateRequestArriveEarly", "RestoreAsset should have been processed");
		} else if (status == RestoreAssetStatus::Loaded && reply.isDuplicated) {
			TraceEvent(SevDebug, "FastRestoreMasterPhaseLoadFilesReply")
			    .detail("RestoreAsset", reply.param.asset.toString())
			    .detail("RequestIgnored", "Loading request was sent more than once");
		} else {
			TraceEvent(SevError, "FastRestoreMasterPhaseLoadFilesReply")
			    .detail("RestoreAsset", reply.param.asset.toString())
			    .detail("UnexpectedReply", reply.toString());
		}
		// Update sampled data
		for (int i = 0; i < reply.samples.size(); ++i) {
			MutationRef mutation = reply.samples[i];
			batchData->samples.addMetric(mutation.param1, mutation.weightedTotalSize());
			batchData->samplesSize += mutation.weightedTotalSize();
		}
	}

	// Sanity check: all restore assets status should be Loaded
	for (auto& asset : assets) {
		if (batchStatus->raStatus[asset] != RestoreAssetStatus::Loaded) {
			TraceEvent(SevError, "FastRestoreMasterPhaseLoadFilesReply")
			    .detail("RestoreAsset", asset.toString())
			    .detail("UnexpectedStatus", batchStatus->raStatus[asset]);
		}
	}

	TraceEvent("FastRestoreMasterPhaseLoadFilesDone")
	    .detail("BatchIndex", batchIndex)
	    .detail("FileTypeLoadedInVersionBatch", isRangeFile)
	    .detail("BeginVersion", versionBatch.beginVersion)
	    .detail("EndVersion", versionBatch.endVersion);
	return Void();
}

// Ask loaders to send its buffered mutations to appliers
ACTOR static Future<Void> sendMutationsFromLoaders(Reference<MasterBatchData> batchData,
                                                   Reference<MasterBatchStatus> batchStatus,
                                                   std::map<UID, RestoreLoaderInterface> loadersInterf, int batchIndex,
                                                   bool useRangeFile) {
	TraceEvent("FastRestoreMasterPhaseSendMutationsFromLoadersStart")
	    .detail("BatchIndex", batchIndex)
	    .detail("UseRangeFiles", useRangeFile)
	    .detail("Loaders", loadersInterf.size());

	std::vector<std::pair<UID, RestoreSendMutationsToAppliersRequest>> requests;
	for (auto& loader : loadersInterf) {
		requests.emplace_back(
		    loader.first, RestoreSendMutationsToAppliersRequest(batchIndex, batchData->rangeToApplier, useRangeFile));
		batchStatus->loadStatus[loader.first] =
		    useRangeFile ? RestoreSendStatus::SendingRanges : RestoreSendStatus::SendingLogs;
	}
	state std::vector<RestoreCommonReply> replies;
	wait(getBatchReplies(&RestoreLoaderInterface::sendMutations, loadersInterf, requests, &replies,
	                     TaskPriority::RestoreLoaderSendMutations));

	TraceEvent("FastRestoreMasterPhaseSendMutationsFromLoadersDone")
	    .detail("BatchIndex", batchIndex)
	    .detail("UseRangeFiles", useRangeFile)
	    .detail("Loaders", loadersInterf.size());

	return Void();
}

// Process a version batch. Phases (loading files, send mutations) should execute in order
ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, int batchIndex,
                                                            Database cx, RestoreRequest request,
                                                            VersionBatch versionBatch) {
	state Reference<MasterBatchData> batchData = self->batch[batchIndex];
	state Reference<MasterBatchStatus> batchStatus = self->batchStatus[batchIndex];

	self->runningVersionBatches.set(self->runningVersionBatches.get() + 1);

	// In case sampling data takes too much memory on master
	wait(isSchedulable(self, batchIndex, __FUNCTION__));

	wait(initializeVersionBatch(self->appliersInterf, self->loadersInterf, batchIndex));

	ASSERT(!versionBatch.isEmpty());
	ASSERT(self->loadersInterf.size() > 0);
	ASSERT(self->appliersInterf.size() > 0);

	// Parse log files and send mutations to appliers before we parse range files
	// TODO: Allow loading both range and log files in parallel
	ASSERT(batchData->samples.empty());
	ASSERT(batchData->samplesSize < 1 && batchData->samplesSize > -1); // samplesSize should be 0
	ASSERT(batchStatus->raStatus.empty());
	ASSERT(batchStatus->loadStatus.empty());
	ASSERT(batchStatus->applyStatus.empty());

	// New backup has subversion to order mutations at the same version. For mutations at the same version,
	// range file's mutations have the largest subversion and larger than log file's.
	// SOMEDAY: Extend subversion to old-style backup.
	wait(
	    loadFilesOnLoaders(batchData, batchStatus, self->loadersInterf, batchIndex, cx, request, versionBatch, false) &&
	    loadFilesOnLoaders(batchData, batchStatus, self->loadersInterf, batchIndex, cx, request, versionBatch, true));

	ASSERT(batchData->rangeToApplier.empty());
	splitKeyRangeForAppliers(batchData, self->appliersInterf, batchIndex);

	// Ask loaders to send parsed mutations to appliers;
	// log mutations should be applied before range mutations at the same version, which is ensured by LogMessageVersion
	wait(sendMutationsFromLoaders(batchData, batchStatus, self->loadersInterf, batchIndex, false) &&
	     sendMutationsFromLoaders(batchData, batchStatus, self->loadersInterf, batchIndex, true));

	// Synchronization point for version batch pipelining.
	// self->finishedBatch will continuously increase by 1 per version batch.
	wait(notifyApplierToApplyMutations(batchData, batchStatus, self->appliersInterf, batchIndex, &self->finishedBatch));

	wait(notifyLoadersVersionBatchFinished(self->loadersInterf, batchIndex));

	self->runningVersionBatches.set(self->runningVersionBatches.get() - 1);

	if (self->delayedActors > 0) {
		self->checkMemory.trigger();
	}
	return Void();
}

// Decide which key range should be taken by which applier
// Input: samples in batchData
// Output: rangeToApplier in batchData
void splitKeyRangeForAppliers(Reference<MasterBatchData> batchData,
                              std::map<UID, RestoreApplierInterface> appliersInterf, int batchIndex) {
	ASSERT(batchData->samplesSize >= 0);
	int numAppliers = appliersInterf.size();
	double slotSize = std::max(batchData->samplesSize / numAppliers, 1.0);
	std::set<Key> keyrangeSplitter; // unique key to split key range for appliers
	keyrangeSplitter.insert(normalKeys.begin); // First slot
	double cumulativeSize = slotSize;
	TraceEvent("FastRestoreMasterPhaseCalculateApplierKeyRangesStart")
	    .detail("BatchIndex", batchIndex)
	    .detail("SamplingSize", batchData->samplesSize)
	    .detail("SlotSize", slotSize);
	int slotIdx = 1;
	while (cumulativeSize < batchData->samplesSize) {
		IndexedSet<Key, int64_t>::iterator lowerBound = batchData->samples.index(cumulativeSize);
		if (lowerBound == batchData->samples.end()) {
			break;
		}
		keyrangeSplitter.insert(*lowerBound);
		TraceEvent("FastRestoreMasterPhaseCalculateApplierKeyRanges")
		    .detail("BatchIndex", batchIndex)
		    .detail("CumulativeSize", cumulativeSize)
		    .detail("Slot", slotIdx++)
		    .detail("LowerBoundKey", lowerBound->toString());
		cumulativeSize += slotSize;
	}
	if (keyrangeSplitter.size() < numAppliers) {
		TraceEvent(SevWarnAlways, "FastRestoreMasterPhaseCalculateApplierKeyRanges")
		    .detail("NotAllAppliersAreUsed", keyrangeSplitter.size())
		    .detail("NumAppliers", numAppliers);
	} else if (keyrangeSplitter.size() > numAppliers) {
		bool expected = (keyrangeSplitter.size() == numAppliers + 1);
		TraceEvent(expected ? SevWarn : SevError, "FastRestoreMasterPhaseCalculateApplierKeyRanges")
		    .detail("TooManySlotsThanAppliers", keyrangeSplitter.size())
		    .detail("NumAppliers", numAppliers)
		    .detail("SamplingSize", batchData->samplesSize)
		    .detail("PerformanceMayDegrade", "Last applier handles more data than others");
	}

	std::set<Key>::iterator splitter = keyrangeSplitter.begin();
	batchData->rangeToApplier.clear();
	for (auto& applier : appliersInterf) {
		if (splitter == keyrangeSplitter.end()) {
			break; // Not all appliers will be used
		}
		batchData->rangeToApplier[*splitter] = applier.first;
		splitter++;
	}
	ASSERT(batchData->rangeToApplier.size() > 0);
	ASSERT(batchData->sanityCheckApplierKeyRange());
	batchData->logApplierKeyRange(batchIndex);
	TraceEvent("FastRestoreMasterPhaseCalculateApplierKeyRangesDone")
	    .detail("BatchIndex", batchIndex)
	    .detail("SamplingSize", batchData->samplesSize)
	    .detail("SlotSize", slotSize);
}

ACTOR static Future<Standalone<VectorRef<RestoreRequest>>> collectRestoreRequests(Database cx) {
	state Standalone<VectorRef<RestoreRequest>> restoreRequests;
	state Future<Void> watch4RestoreRequest;
	state ReadYourWritesTransaction tr(cx);

	// wait for the restoreRequestTriggerKey to be set by the client/test workload
	loop {
		try {
			TraceEvent("FastRestoreMasterPhaseCollectRestoreRequestsWait");
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
						TraceEvent("FastRestoreMasterPhaseCollectRestoreRequests")
						    .detail("RestoreRequest", restoreRequests.back().toString());
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
// Returns the restore target version.
ACTOR static Future<Version> collectBackupFiles(Reference<IBackupContainer> bc, std::vector<RestoreFileFR>* rangeFiles,
                                                std::vector<RestoreFileFR>* logFiles, Database cx,
                                                RestoreRequest request) {
	state bool partitioned = wait(bc->isPartitionedBackup());
	state BackupDescription desc = wait(partitioned ? bc->describePartitionedBackup() : bc->describeBackup());

	// Convert version to real time for operators to read the BackupDescription desc.
	wait(desc.resolveVersionTimes(cx));

	if (request.targetVersion == invalidVersion && desc.maxRestorableVersion.present()) {
		request.targetVersion = desc.maxRestorableVersion.get();
	}

	TraceEvent("FastRestoreMasterPhaseCollectBackupFilesStart")
	    .detail("TargetVersion", request.targetVersion)
	    .detail("BackupDesc", desc.toString());
	if (g_network->isSimulated()) {
		std::cout << "Restore to version: " << request.targetVersion << "\nBackupDesc: \n" << desc.toString() << "\n\n";
	}

	Optional<RestorableFileSet> restorable = wait(partitioned ? bc->getPartitionedRestoreSet(request.targetVersion)
	                                                          : bc->getRestoreSet(request.targetVersion));

	if (!restorable.present()) {
		TraceEvent(SevWarn, "FastRestoreMasterPhaseCollectBackupFiles").detail("NotRestorable", request.targetVersion);
		throw restore_missing_data();
	}

	ASSERT(rangeFiles->empty());
	ASSERT(logFiles->empty());

	std::set<RestoreFileFR> uniqueRangeFiles;
	std::set<RestoreFileFR> uniqueLogFiles;
	for (const RangeFile& f : restorable.get().ranges) {
		TraceEvent("FastRestoreMasterPhaseCollectBackupFiles").detail("RangeFile", f.toString());
		if (f.fileSize <= 0) {
			continue;
		}
		RestoreFileFR file(f);
		TraceEvent("FastRestoreMasterPhaseCollectBackupFiles").detail("RangeFileFR", file.toString());
		uniqueRangeFiles.insert(file);
	}
	for (const LogFile& f : restorable.get().logs) {
		TraceEvent("FastRestoreMasterPhaseCollectBackupFiles").detail("LogFile", f.toString());
		if (f.fileSize <= 0) {
			continue;
		}
		RestoreFileFR file(f);
		TraceEvent("FastRestoreMasterPhaseCollectBackupFiles").detail("LogFileFR", file.toString());
		logFiles->push_back(file);
		uniqueLogFiles.insert(file);
	}
	// Assign unique range files and log files to output
	rangeFiles->assign(uniqueRangeFiles.begin(), uniqueRangeFiles.end());
	logFiles->assign(uniqueLogFiles.begin(), uniqueLogFiles.end());

	TraceEvent("FastRestoreMasterPhaseCollectBackupFilesDone")
	    .detail("BackupDesc", desc.toString())
	    .detail("RangeFiles", rangeFiles->size())
	    .detail("LogFiles", logFiles->size());
	return request.targetVersion;
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

ACTOR static Future<Void> initializeVersionBatch(std::map<UID, RestoreApplierInterface> appliersInterf,
                                                 std::map<UID, RestoreLoaderInterface> loadersInterf, int batchIndex) {
	TraceEvent("FastRestoreMasterPhaseInitVersionBatchForAppliersStart")
	    .detail("BatchIndex", batchIndex)
	    .detail("Appliers", appliersInterf.size());
	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requestsToAppliers;
	for (auto& applier : appliersInterf) {
		requestsToAppliers.emplace_back(applier.first, RestoreVersionBatchRequest(batchIndex));
	}
	wait(sendBatchRequests(&RestoreApplierInterface::initVersionBatch, appliersInterf, requestsToAppliers));

	TraceEvent("FastRestoreMasterPhaseInitVersionBatchForLoaders")
	    .detail("BatchIndex", batchIndex)
	    .detail("Loaders", loadersInterf.size());
	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requestsToLoaders;
	for (auto& loader : loadersInterf) {
		requestsToLoaders.emplace_back(loader.first, RestoreVersionBatchRequest(batchIndex));
	}
	wait(sendBatchRequests(&RestoreLoaderInterface::initVersionBatch, loadersInterf, requestsToLoaders));

	TraceEvent("FastRestoreMasterPhaseInitVersionBatchForLoadersDone").detail("BatchIndex", batchIndex);
	return Void();
}

// Ask each applier to apply its received mutations to DB
// NOTE: Master cannot start applying mutations at batchIndex until all appliers have applied for (batchIndex - 1)
//       because appliers at different batchIndex may have overlapped key ranges.
ACTOR static Future<Void> notifyApplierToApplyMutations(Reference<MasterBatchData> batchData,
                                                        Reference<MasterBatchStatus> batchStatus,
                                                        std::map<UID, RestoreApplierInterface> appliersInterf,
                                                        int batchIndex, NotifiedVersion* finishedBatch) {

	wait(finishedBatch->whenAtLeast(batchIndex - 1));
	TraceEvent("FastRestoreMasterPhaseApplyToDB")
	    .detail("BatchIndex", batchIndex)
	    .detail("FinishedBatch", finishedBatch->get());

	if (finishedBatch->get() == batchIndex - 1) {
		// Prepare the applyToDB requests
		std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;

		TraceEvent("FastRestoreMasterPhaseApplyToDB")
		    .detail("BatchIndex", batchIndex)
		    .detail("Appliers", appliersInterf.size());
		for (auto& applier : appliersInterf) {
			ASSERT(batchStatus->applyStatus.find(applier.first) == batchStatus->applyStatus.end());
			requests.emplace_back(applier.first, RestoreVersionBatchRequest(batchIndex));
			batchStatus->applyStatus[applier.first] = RestoreApplyStatus::Applying;
		}
		state std::vector<RestoreCommonReply> replies;
		// The actor at each batchIndex should only occur once.
		// Use batchData->applyToDB just incase the actor at a batchIndex is executed more than once.
		if (!batchData->applyToDB.present()) {
			batchData->applyToDB = Never();
			batchData->applyToDB = getBatchReplies(&RestoreApplierInterface::applyToDB, appliersInterf, requests,
			                                       &replies, TaskPriority::RestoreApplierWriteDB);
		} else {
			TraceEvent(SevError, "FastRestoreMasterPhaseApplyToDB")
			    .detail("BatchIndex", batchIndex)
			    .detail("Attention", "Actor should not be invoked twice for the same batch index");
		}

		ASSERT(batchData->applyToDB.present());
		wait(batchData->applyToDB.get());

		// Sanity check all appliers have applied data to destination DB
		for (auto& reply : replies) {
			if (batchStatus->applyStatus[reply.id] == RestoreApplyStatus::Applying) {
				batchStatus->applyStatus[reply.id] = RestoreApplyStatus::Applied;
				if (reply.isDuplicated) {
					TraceEvent(SevWarn, "FastRestoreMasterPhaseApplyToDB")
					    .detail("Applier", reply.id)
					    .detail("DuplicateRequestReturnEarlier", "Apply db request should have been processed");
				}
			}
		}
		for (auto& applier : appliersInterf) {
			if (batchStatus->applyStatus[applier.first] != RestoreApplyStatus::Applied) {
				TraceEvent(SevError, "FastRestoreMasterPhaseApplyToDB")
				    .detail("Applier", applier.first)
				    .detail("ApplyStatus", batchStatus->applyStatus[applier.first]);
			}
		}
		finishedBatch->set(batchIndex);
	}

	TraceEvent("FastRestoreMasterPhaseApplyToDBDone")
	    .detail("BatchIndex", batchIndex)
	    .detail("FinishedBatch", finishedBatch->get());

	return Void();
}

// Notify loaders that all data in the version batch has been applied to DB.
ACTOR static Future<Void> notifyLoadersVersionBatchFinished(std::map<UID, RestoreLoaderInterface> loadersInterf,
                                                            int batchIndex) {
	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requestsToLoaders;
	for (auto& loader : loadersInterf) {
		requestsToLoaders.emplace_back(loader.first, RestoreVersionBatchRequest(batchIndex));
	}
	wait(sendBatchRequests(&RestoreLoaderInterface::finishVersionBatch, loadersInterf, requestsToLoaders));

	return Void();
}

// Ask all loaders and appliers to perform housecleaning at the end of a restore request
// Terminate those roles if terminate = true
ACTOR static Future<Void> notifyRestoreCompleted(Reference<RestoreMasterData> self, bool terminate = false) {
	std::vector<std::pair<UID, RestoreFinishRequest>> requests;
	TraceEvent("FastRestoreMasterPhaseNotifyRestoreCompletedStart");
	for (auto& loader : self->loadersInterf) {
		requests.emplace_back(loader.first, RestoreFinishRequest(terminate));
	}

	Future<Void> endLoaders = sendBatchRequests(&RestoreLoaderInterface::finishRestore, self->loadersInterf, requests);

	requests.clear();
	for (auto& applier : self->appliersInterf) {
		requests.emplace_back(applier.first, RestoreFinishRequest(terminate));
	}
	Future<Void> endAppliers =
	    sendBatchRequests(&RestoreApplierInterface::finishRestore, self->appliersInterf, requests);

	// If terminate = true, loaders and appliers exits immediately after it receives the request. Master may not receive
	// acks.
	if (!terminate) {
		wait(endLoaders && endAppliers);
	}

	TraceEvent("FastRestoreMasterPhaseNotifyRestoreCompletedDone");

	return Void();
}

// Register the restoreRequestDoneKey to signal the end of restore
ACTOR static Future<Void> signalRestoreCompleted(Reference<RestoreMasterData> self, Database cx) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

	wait(notifyRestoreCompleted(self, true));

	wait(delay(5.0)); // Give some time for loaders and appliers to exit

	// Notify tester that the restore has finished
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

	TraceEvent("FastRestore").detail("RestoreMaster", "AllRestoreCompleted");

	return Void();
}

// Update the most recent time when master receives hearbeat from each loader and applier
ACTOR static Future<Void> updateHeartbeatTime(Reference<RestoreMasterData> self) {
	int numRoles = self->loadersInterf.size() + self->appliersInterf.size();
	state std::map<UID, RestoreLoaderInterface>::iterator loader = self->loadersInterf.begin();
	state std::map<UID, RestoreApplierInterface>::iterator applier = self->appliersInterf.begin();
	state std::vector<Future<RestoreCommonReply>> fReplies(numRoles, Never()); // TODO: Reserve memory for this vector
	state std::vector<UID> nodes;
	state int index = 0;
	state Future<Void> fTimeout = Void();

	// Initialize nodes only once
	std::transform(self->loadersInterf.begin(), self->loadersInterf.end(), std::back_inserter(nodes),
	               [](const std::pair<UID, RestoreLoaderInterface>& in) { return in.first; });
	std::transform(self->appliersInterf.begin(), self->appliersInterf.end(), std::back_inserter(nodes),
	               [](const std::pair<UID, RestoreApplierInterface>& in) { return in.first; });

	loop {
		loader = self->loadersInterf.begin();
		applier = self->appliersInterf.begin();
		index = 0;
		std::fill(fReplies.begin(), fReplies.end(), Never());
		// ping loaders and appliers
		while(loader != self->loadersInterf.end()) {
			fReplies[index] = loader->second.heartbeat.getReply(RestoreSimpleRequest());
			loader++;
			index++;
		}
		while(applier != self->appliersInterf.end()) {
			fReplies[index] = applier->second.heartbeat.getReply(RestoreSimpleRequest());
			applier++;
			index++;
		}

		fTimeout = delay(SERVER_KNOBS->FASTRESTORE_HEARTBEAT_DELAY);
		wait(waitForAll(fReplies) || fTimeout);
		// Update the most recent heart beat time for each role
		for (int i = 0; i < fReplies.size(); ++i) {
			if (fReplies[i].isReady()) {
				double currentTime = now();
				auto item = self->rolesHeartBeatTime.emplace(nodes[i], currentTime);
				item.first->second = currentTime;
			}
		}
		wait(fTimeout); // Ensure not updating heartbeat too quickly
	}
}

// Check if a restore role dies or disconnected
ACTOR static Future<Void> checkRolesLiveness(Reference<RestoreMasterData> self) {
	loop {
		wait(delay(SERVER_KNOBS->FASTRESTORE_HEARTBEAT_MAX_DELAY));
		for (auto& role : self->rolesHeartBeatTime) {
			if (now() - role.second > SERVER_KNOBS->FASTRESTORE_HEARTBEAT_MAX_DELAY) {
				TraceEvent(SevWarnAlways, "FastRestoreUnavailableRole", role.first).detail("Delta", now() - role.second).detail("LastAliveTime", role.second);
			}
		}
	}
}