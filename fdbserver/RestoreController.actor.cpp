/*
 * RestoreController.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

// This file implements the functions for RestoreController role

#include "fdbrpc/RangeMap.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreCommon.actor.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreController.actor.h"
#include "fdbserver/RestoreApplier.actor.h"
#include "fdbserver/RestoreLoader.actor.h"

#include "flow/Platform.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// TODO: Support [[maybe_unused]] attribute for actors
// ACTOR static Future<Void> clearDB(Database cx);
ACTOR static Future<Version> collectBackupFiles(Reference<IBackupContainer> bc,
                                                std::vector<RestoreFileFR>* rangeFiles,
                                                std::vector<RestoreFileFR>* logFiles,
                                                Version* minRangeVersion,
                                                Database cx,
                                                RestoreRequest request);
ACTOR static Future<Void> buildRangeVersions(KeyRangeMap<Version>* pRangeVersions,
                                             std::vector<RestoreFileFR>* pRangeFiles,
                                             Key url,
                                             Optional<std::string> proxy);

ACTOR static Future<Version> processRestoreRequest(Reference<RestoreControllerData> self,
                                                   Database cx,
                                                   RestoreRequest request);
ACTOR static Future<Void> startProcessRestoreRequests(Reference<RestoreControllerData> self, Database cx);
ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreControllerData> self,
                                                            int batchIndex,
                                                            Database cx,
                                                            RestoreRequest request,
                                                            VersionBatch versionBatch);

ACTOR static Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> controllerWorker,
                                              Reference<RestoreControllerData> controllerData);
ACTOR static Future<Void> distributeRestoreSysInfo(Reference<RestoreControllerData> controllerData,
                                                   KeyRangeMap<Version>* pRangeVersions);

ACTOR static Future<std::vector<RestoreRequest>> collectRestoreRequests(Database cx);
ACTOR static Future<Void> initializeVersionBatch(std::map<UID, RestoreApplierInterface> appliersInterf,
                                                 std::map<UID, RestoreLoaderInterface> loadersInterf,
                                                 int batchIndex);
ACTOR static Future<Void> notifyApplierToApplyMutations(Reference<ControllerBatchData> batchData,
                                                        Reference<ControllerBatchStatus> batchStatus,
                                                        std::map<UID, RestoreApplierInterface> appliersInterf,
                                                        int batchIndex,
                                                        NotifiedVersion* finishedBatch);
ACTOR static Future<Void> notifyLoadersVersionBatchFinished(std::map<UID, RestoreLoaderInterface> loadersInterf,
                                                            int batchIndex);
ACTOR static Future<Void> notifyRestoreCompleted(Reference<RestoreControllerData> self, bool terminate);
ACTOR static Future<Void> signalRestoreCompleted(Reference<RestoreControllerData> self, Database cx);
// TODO: Support [[maybe_unused]] attribute for actors
// ACTOR static Future<Void> updateHeartbeatTime(Reference<RestoreControllerData> self);
ACTOR static Future<Void> checkRolesLiveness(Reference<RestoreControllerData> self);

void splitKeyRangeForAppliers(Reference<ControllerBatchData> batchData,
                              std::map<UID, RestoreApplierInterface> appliersInterf,
                              int batchIndex);

ACTOR Future<Void> sampleBackups(Reference<RestoreControllerData> self, RestoreControllerInterface ci) {
	loop {
		try {
			RestoreSamplesRequest req = waitNext(ci.samples.getFuture());
			TraceEvent(SevDebug, "FastRestoreControllerSampleBackups")
			    .detail("SampleID", req.id)
			    .detail("BatchIndex", req.batchIndex)
			    .detail("Samples", req.samples.size());
			ASSERT(req.batchIndex <= self->batch.size()); // batchIndex starts from 1

			Reference<ControllerBatchData> batch = self->batch[req.batchIndex];
			ASSERT(batch.isValid());
			if (batch->sampleMsgs.find(req.id) != batch->sampleMsgs.end()) {
				req.reply.send(RestoreCommonReply(req.id));
				continue;
			}
			batch->sampleMsgs.insert(req.id);
			for (auto& m : req.samples) {
				batch->samples.addMetric(m.key, m.size);
				batch->samplesSize += m.size;
			}
			req.reply.send(RestoreCommonReply(req.id));
		} catch (Error& e) {
			TraceEvent(SevWarn, "FastRestoreControllerSampleBackupsError", self->id()).error(e);
			break;
		}
	}

	return Void();
}

ACTOR Future<Void> startRestoreController(Reference<RestoreWorkerData> controllerWorker, Database cx) {
	ASSERT(controllerWorker.isValid());
	ASSERT(controllerWorker->controllerInterf.present());
	state Reference<RestoreControllerData> self =
	    makeReference<RestoreControllerData>(controllerWorker->controllerInterf.get().id());
	state Future<Void> error = actorCollection(self->addActor.getFuture());

	try {
		// recruitRestoreRoles must come after controllerWorker has finished collectWorkerInterface
		wait(recruitRestoreRoles(controllerWorker, self));

		// self->addActor.send(updateHeartbeatTime(self));
		self->addActor.send(checkRolesLiveness(self));
		self->addActor.send(updateProcessMetrics(self));
		self->addActor.send(traceProcessMetrics(self, "RestoreController"));
		self->addActor.send(sampleBackups(self, controllerWorker->controllerInterf.get()));

		wait(startProcessRestoreRequests(self, cx) || error);
	} catch (Error& e) {
		if (e.code() != error_code_operation_cancelled) {
			TraceEvent(SevError, "FastRestoreControllerStart").error(e).detail("Reason", "Unexpected unhandled error");
		}
	}

	return Void();
}

// RestoreWorker that has restore controller role: Recruite a role for each worker
ACTOR Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> controllerWorker,
                                       Reference<RestoreControllerData> controllerData) {
	state int nodeIndex = 0;
	state RestoreRole role = RestoreRole::Invalid;

	TraceEvent("FastRestoreController", controllerData->id())
	    .detail("RecruitRestoreRoles", controllerWorker->workerInterfaces.size())
	    .detail("NumLoaders", SERVER_KNOBS->FASTRESTORE_NUM_LOADERS)
	    .detail("NumAppliers", SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS);
	ASSERT(controllerData->loadersInterf.empty() && controllerData->appliersInterf.empty());
	ASSERT(controllerWorker->controllerInterf.present());

	ASSERT(controllerData.isValid());
	ASSERT(SERVER_KNOBS->FASTRESTORE_NUM_LOADERS > 0 && SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS > 0);
	// We assign 1 role per worker for now
	ASSERT(SERVER_KNOBS->FASTRESTORE_NUM_LOADERS + SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS <=
	       controllerWorker->workerInterfaces.size());

	// Assign a role to each worker
	std::vector<std::pair<UID, RestoreRecruitRoleRequest>> requests;
	for (auto& workerInterf : controllerWorker->workerInterfaces) {
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

		TraceEvent("FastRestoreController", controllerData->id())
		    .detail("WorkerNode", workerInterf.first)
		    .detail("NodeRole", role)
		    .detail("NodeIndex", nodeIndex);
		requests.emplace_back(workerInterf.first,
		                      RestoreRecruitRoleRequest(controllerWorker->controllerInterf.get(), role, nodeIndex));
		nodeIndex++;
	}

	state std::vector<RestoreRecruitRoleReply> replies;
	wait(getBatchReplies(&RestoreWorkerInterface::recruitRole, controllerWorker->workerInterfaces, requests, &replies));
	for (auto& reply : replies) {
		if (reply.role == RestoreRole::Applier) {
			ASSERT_WE_THINK(reply.applier.present());
			controllerData->appliersInterf[reply.applier.get().id()] = reply.applier.get();
		} else if (reply.role == RestoreRole::Loader) {
			ASSERT_WE_THINK(reply.loader.present());
			controllerData->loadersInterf[reply.loader.get().id()] = reply.loader.get();
		} else {
			TraceEvent(SevError, "FastRestoreController").detail("RecruitRestoreRolesInvalidRole", reply.role);
		}
	}
	controllerData->recruitedRoles.send(Void());
	TraceEvent("FastRestoreRecruitRestoreRolesDone", controllerData->id())
	    .detail("Workers", controllerWorker->workerInterfaces.size())
	    .detail("RecruitedRoles", replies.size());

	return Void();
}

ACTOR Future<Void> distributeRestoreSysInfo(Reference<RestoreControllerData> controllerData,
                                            KeyRangeMap<Version>* pRangeVersions) {
	ASSERT(controllerData.isValid());
	ASSERT(!controllerData->loadersInterf.empty());
	RestoreSysInfo sysInfo(controllerData->appliersInterf);
	// Construct serializable KeyRange versions
	Standalone<VectorRef<std::pair<KeyRangeRef, Version>>> rangeVersionsVec;
	auto ranges = pRangeVersions->ranges();
	int i = 0;
	for (auto r = ranges.begin(); r != ranges.end(); ++r) {
		rangeVersionsVec.push_back(rangeVersionsVec.arena(),
		                           std::make_pair(KeyRangeRef(r->begin(), r->end()), r->value()));
		TraceEvent("DistributeRangeVersions")
		    .detail("RangeIndex", i++)
		    .detail("RangeBegin", r->begin())
		    .detail("RangeEnd", r->end())
		    .detail("RangeVersion", r->value());
	}
	std::vector<std::pair<UID, RestoreSysInfoRequest>> requests;
	for (auto& loader : controllerData->loadersInterf) {
		requests.emplace_back(loader.first, RestoreSysInfoRequest(sysInfo, rangeVersionsVec));
	}

	TraceEvent("FastRestoreDistributeRestoreSysInfoToLoaders", controllerData->id())
	    .detail("Loaders", controllerData->loadersInterf.size());
	wait(sendBatchRequests(&RestoreLoaderInterface::updateRestoreSysInfo, controllerData->loadersInterf, requests));
	TraceEvent("FastRestoreDistributeRestoreSysInfoToLoadersDone", controllerData->id())
	    .detail("Loaders", controllerData->loadersInterf.size());

	return Void();
}

// The server of the restore controller. It drives the restore progress with the following steps:
// 1) Lock database and clear the normal keyspace
// 2) Wait on each RestoreRequest, which is sent by RestoreTool operated by DBA
// 3) Process each restore request in actor processRestoreRequest;
// 3.1) Sample workload to decide the key range for each applier, which is implemented as a dummy sampling;
// 3.2) Send each loader the map of key-range to applier interface;
// 3.3) Construct requests of which file should be loaded by which loader, and send requests to loaders;
// 4) After process all restore requests, finish restore by cleaning up the restore related system key
//    and ask all restore roles to quit.
ACTOR Future<Void> startProcessRestoreRequests(Reference<RestoreControllerData> self, Database cx) {
	state UID randomUID = deterministicRandom()->randomUniqueID();
	state std::vector<RestoreRequest> restoreRequests = wait(collectRestoreRequests(cx));
	state int restoreIndex = 0;

	TraceEvent("FastRestoreControllerWaitOnRestoreRequests", self->id())
	    .detail("RestoreRequests", restoreRequests.size());

	// TODO: Sanity check restoreRequests' key ranges do not overlap

	// Step: Perform the restore requests
	try {
		for (restoreIndex = 0; restoreIndex < restoreRequests.size(); restoreIndex++) {
			state RestoreRequest request = restoreRequests[restoreIndex];
			state KeyRange range = request.range.removePrefix(request.removePrefix).withPrefix(request.addPrefix);
			TraceEvent("FastRestoreControllerProcessRestoreRequests", self->id())
			    .detail("RestoreRequestInfo", request.toString())
			    .detail("TransformedKeyRange", range);
			// TODO: Initialize controllerData and all loaders and appliers' data for each restore request!
			self->resetPerRestoreRequest();

			// clear the key range that will be restored
			wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				tr->clear(range);
				return Void();
			}));

			wait(success(processRestoreRequest(self, cx, request)));
			wait(notifyRestoreCompleted(self, false));
		}
	} catch (Error& e) {
		if (restoreIndex < restoreRequests.size()) {
			TraceEvent(SevError, "FastRestoreControllerProcessRestoreRequestsFailed", self->id())
			    .error(e)
			    .detail("RestoreRequest", restoreRequests[restoreIndex].toString());
		} else {
			TraceEvent(SevError, "FastRestoreControllerProcessRestoreRequestsFailed", self->id())
			    .error(e)
			    .detail("RestoreRequests", restoreRequests.size())
			    .detail("RestoreIndex", restoreIndex);
		}
	}

	// Step: Notify all restore requests have been handled by cleaning up the restore keys
	wait(signalRestoreCompleted(self, cx));

	TraceEvent("FastRestoreControllerRestoreCompleted", self->id());

	return Void();
}

ACTOR static Future<Void> monitorFinishedVersion(Reference<RestoreControllerData> self, RestoreRequest request) {
	loop {
		TraceEvent("FastRestoreMonitorFinishedVersion", self->id())
		    .detail("RestoreRequest", request.toString())
		    .detail("BatchIndex", self->finishedBatch.get());
		wait(delay(SERVER_KNOBS->FASTRESTORE_VB_MONITOR_DELAY));
	}
}

ACTOR static Future<Version> processRestoreRequest(Reference<RestoreControllerData> self,
                                                   Database cx,
                                                   RestoreRequest request) {
	state std::vector<RestoreFileFR> rangeFiles;
	state std::vector<RestoreFileFR> logFiles;
	state std::vector<RestoreFileFR> allFiles;
	state Version minRangeVersion = MAX_VERSION;

	self->initBackupContainer(request.url, request.proxy);

	// Get all backup files' description and save them to files
	state Version targetVersion =
	    wait(collectBackupFiles(self->bc, &rangeFiles, &logFiles, &minRangeVersion, cx, request));
	ASSERT(targetVersion > 0);
	ASSERT(minRangeVersion != MAX_VERSION); // otherwise, all mutations will be skipped

	std::sort(rangeFiles.begin(), rangeFiles.end());
	std::sort(logFiles.begin(), logFiles.end(), [](RestoreFileFR const& f1, RestoreFileFR const& f2) -> bool {
		return std::tie(f1.endVersion, f1.beginVersion, f1.fileIndex, f1.fileName) <
		       std::tie(f2.endVersion, f2.beginVersion, f2.fileIndex, f2.fileName);
	});

	// Build range versions: version of key ranges in range file
	state KeyRangeMap<Version> rangeVersions(minRangeVersion, allKeys.end);
	if (SERVER_KNOBS->FASTRESTORE_GET_RANGE_VERSIONS_EXPENSIVE) {
		wait(buildRangeVersions(&rangeVersions, &rangeFiles, request.url, request.proxy));
	} else {
		// Debug purpose, dump range versions
		auto ranges = rangeVersions.ranges();
		int i = 0;
		for (auto r = ranges.begin(); r != ranges.end(); ++r) {
			TraceEvent(SevDebug, "SingleRangeVersion")
			    .detail("RangeIndex", i++)
			    .detail("RangeBegin", r->begin())
			    .detail("RangeEnd", r->end())
			    .detail("RangeVersion", r->value());
		}
	}

	wait(distributeRestoreSysInfo(self, &rangeVersions));

	// Divide files into version batches.
	self->buildVersionBatches(rangeFiles, logFiles, &self->versionBatches, targetVersion);
	self->dumpVersionBatches(self->versionBatches);

	state std::vector<Future<Void>> fBatches;
	state std::vector<VersionBatch> versionBatches; // To randomize invoking order of version batchs
	for (auto& vb : self->versionBatches) {
		versionBatches.push_back(vb.second);
	}

	// releaseVBOutOfOrder can only be true in simulation
	state bool releaseVBOutOfOrder = g_network->isSimulated() ? deterministicRandom()->random01() < 0.5 : false;
	ASSERT(g_network->isSimulated() || !releaseVBOutOfOrder);
	if (releaseVBOutOfOrder) {
		// Randomize invoking order of version batches
		int permTimes = deterministicRandom()->randomInt(0, 100);
		while (permTimes-- > 0) {
			std::next_permutation(versionBatches.begin(), versionBatches.end());
		}
	}

	self->addActor.send(monitorFinishedVersion(self, request));
	state std::vector<VersionBatch>::iterator versionBatch = versionBatches.begin();
	for (; versionBatch != versionBatches.end(); versionBatch++) {
		while (self->runningVersionBatches.get() >= SERVER_KNOBS->FASTRESTORE_VB_PARALLELISM && !releaseVBOutOfOrder) {
			// Control how many batches can be processed in parallel. Avoid dead lock due to OOM on loaders
			TraceEvent("FastRestoreControllerDispatchVersionBatches")
			    .detail("WaitOnRunningVersionBatches", self->runningVersionBatches.get());
			wait(self->runningVersionBatches.onChange());
		}
		int batchIndex = versionBatch->batchIndex;
		TraceEvent("FastRestoreControllerDispatchVersionBatches")
		    .detail("BatchIndex", batchIndex)
		    .detail("BatchSize", versionBatch->size)
		    .detail("RunningVersionBatches", self->runningVersionBatches.get())
		    .detail("VersionBatches", versionBatches.size());
		self->batch[batchIndex] = makeReference<ControllerBatchData>();
		self->batchStatus[batchIndex] = makeReference<ControllerBatchStatus>();
		fBatches.push_back(distributeWorkloadPerVersionBatch(self, batchIndex, cx, request, *versionBatch));
		// Wait a bit to give the current version batch a head start from the next version batch
		wait(delay(SERVER_KNOBS->FASTRESTORE_VB_LAUNCH_DELAY));
	}

	try {
		wait(waitForAll(fBatches));
	} catch (Error& e) {
		TraceEvent(SevError, "FastRestoreControllerDispatchVersionBatchesUnexpectedError").error(e);
	}

	TraceEvent("FastRestoreController").detail("RestoreToVersion", request.targetVersion);
	return request.targetVersion;
}

ACTOR static Future<Void> loadFilesOnLoaders(Reference<ControllerBatchData> batchData,
                                             Reference<ControllerBatchStatus> batchStatus,
                                             std::map<UID, RestoreLoaderInterface> loadersInterf,
                                             int batchIndex,
                                             Database cx,
                                             RestoreRequest request,
                                             VersionBatch versionBatch,
                                             bool isRangeFile) {
	// set is internally sorted
	std::set<RestoreFileFR>* files = isRangeFile ? &versionBatch.rangeFiles : &versionBatch.logFiles;

	TraceEvent("FastRestoreControllerPhaseLoadFilesStart")
	    .detail("RestoreRequestID", request.randomUid)
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
		// TODO: Allow empty files in version batch; Filter out them here.
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
		param.asset.addPrefix = request.addPrefix;
		param.asset.removePrefix = request.removePrefix;
		param.asset.batchIndex = batchIndex;

		TraceEvent("FastRestoreControllerPhaseLoadFiles")
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
			TraceEvent(SevError, "FastRestoreControllerPhaseLoadFiles")
			    .detail("LoadingParam", param.toString())
			    .detail("RestoreAssetAlreadyProcessed", batchStatus->raStatus[param.asset]);
		}
		batchStatus->raStatus[param.asset] = RestoreAssetStatus::Loading;
		assets.push_back(param.asset);
		++loader;
		++paramIdx;
	}
	TraceEvent(files->size() != paramIdx ? SevError : SevInfo, "FastRestoreControllerPhaseLoadFiles")
	    .detail("BatchIndex", batchIndex)
	    .detail("Files", files->size())
	    .detail("LoadParams", paramIdx);

	state std::vector<RestoreLoadFileReply> replies;
	// Wait on the batch of load files or log files
	wait(getBatchReplies(
	    &RestoreLoaderInterface::loadFile, loadersInterf, requests, &replies, TaskPriority::RestoreLoaderLoadFiles));

	TraceEvent("FastRestoreControllerPhaseLoadFilesReply")
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
			TraceEvent(SevWarn, "FastRestoreControllerPhaseLoadFilesReply")
			    .detail("RestoreAsset", reply.param.asset.toString())
			    .detail("DuplicateRequestArriveEarly", "RestoreAsset should have been processed");
		} else if (status == RestoreAssetStatus::Loaded && reply.isDuplicated) {
			TraceEvent(SevDebug, "FastRestoreControllerPhaseLoadFilesReply")
			    .detail("RestoreAsset", reply.param.asset.toString())
			    .detail("RequestIgnored", "Loading request was sent more than once");
		} else {
			TraceEvent(SevError, "FastRestoreControllerPhaseLoadFilesReply")
			    .detail("RestoreAsset", reply.param.asset.toString())
			    .detail("UnexpectedReply", reply.toString());
		}
	}

	// Sanity check: all restore assets status should be Loaded
	for (auto& asset : assets) {
		if (batchStatus->raStatus[asset] != RestoreAssetStatus::Loaded) {
			TraceEvent(SevError, "FastRestoreControllerPhaseLoadFilesReply")
			    .detail("RestoreAsset", asset.toString())
			    .detail("UnexpectedStatus", batchStatus->raStatus[asset]);
		}
	}

	TraceEvent("FastRestoreControllerPhaseLoadFilesDone")
	    .detail("BatchIndex", batchIndex)
	    .detail("FileTypeLoadedInVersionBatch", isRangeFile)
	    .detail("BeginVersion", versionBatch.beginVersion)
	    .detail("EndVersion", versionBatch.endVersion);
	return Void();
}

// Ask loaders to send its buffered mutations to appliers
ACTOR static Future<Void> sendMutationsFromLoaders(Reference<ControllerBatchData> batchData,
                                                   Reference<ControllerBatchStatus> batchStatus,
                                                   std::map<UID, RestoreLoaderInterface> loadersInterf,
                                                   int batchIndex,
                                                   bool useRangeFile) {
	TraceEvent("FastRestoreControllerPhaseSendMutationsFromLoadersStart")
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
	wait(getBatchReplies(&RestoreLoaderInterface::sendMutations,
	                     loadersInterf,
	                     requests,
	                     &replies,
	                     TaskPriority::RestoreLoaderSendMutations));

	TraceEvent("FastRestoreControllerPhaseSendMutationsFromLoadersDone")
	    .detail("BatchIndex", batchIndex)
	    .detail("UseRangeFiles", useRangeFile)
	    .detail("Loaders", loadersInterf.size());

	return Void();
}

// Process a version batch. Phases (loading files, send mutations) should execute in order
ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreControllerData> self,
                                                            int batchIndex,
                                                            Database cx,
                                                            RestoreRequest request,
                                                            VersionBatch versionBatch) {
	state Reference<ControllerBatchData> batchData = self->batch[batchIndex];
	state Reference<ControllerBatchStatus> batchStatus = self->batchStatus[batchIndex];
	state double startTime = now();

	TraceEvent("FastRestoreControllerDispatchVersionBatchesStart", self->id())
	    .detail("BatchIndex", batchIndex)
	    .detail("BatchSize", versionBatch.size)
	    .detail("RunningVersionBatches", self->runningVersionBatches.get());

	self->runningVersionBatches.set(self->runningVersionBatches.get() + 1);

	// In case sampling data takes too much memory on controller
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

	TraceEvent("FastRestoreControllerDispatchVersionBatchesDone", self->id())
	    .detail("BatchIndex", batchIndex)
	    .detail("BatchSize", versionBatch.size)
	    .detail("RunningVersionBatches", self->runningVersionBatches.get())
	    .detail("Latency", now() - startTime);

	return Void();
}

// Decide which key range should be taken by which applier
// Input: samples in batchData
// Output: rangeToApplier in batchData
void splitKeyRangeForAppliers(Reference<ControllerBatchData> batchData,
                              std::map<UID, RestoreApplierInterface> appliersInterf,
                              int batchIndex) {
	ASSERT(batchData->samplesSize >= 0);
	// Sanity check: samples should not be used after freed
	ASSERT((batchData->samplesSize > 0 && !batchData->samples.empty()) ||
	       (batchData->samplesSize == 0 && batchData->samples.empty()));
	int numAppliers = appliersInterf.size();
	double slotSize = std::max(batchData->samplesSize / numAppliers, 1.0);
	double cumulativeSize = slotSize;
	TraceEvent("FastRestoreControllerPhaseCalculateApplierKeyRangesStart")
	    .detail("BatchIndex", batchIndex)
	    .detail("SamplingSize", batchData->samplesSize)
	    .detail("SlotSize", slotSize);

	std::set<Key> keyrangeSplitter; // unique key to split key range for appliers
	keyrangeSplitter.insert(normalKeys.begin); // First slot
	TraceEvent("FastRestoreControllerPhaseCalculateApplierKeyRanges")
	    .detail("BatchIndex", batchIndex)
	    .detail("CumulativeSize", cumulativeSize)
	    .detail("Slot", 0)
	    .detail("LowerBoundKey", normalKeys.begin);
	int slotIdx = 1;
	while (cumulativeSize < batchData->samplesSize) {
		IndexedSet<Key, int64_t>::iterator lowerBound = batchData->samples.index(cumulativeSize);
		if (lowerBound == batchData->samples.end()) {
			break;
		}
		keyrangeSplitter.insert(*lowerBound);
		TraceEvent("FastRestoreControllerPhaseCalculateApplierKeyRanges")
		    .detail("BatchIndex", batchIndex)
		    .detail("CumulativeSize", cumulativeSize)
		    .detail("Slot", slotIdx++)
		    .detail("LowerBoundKey", lowerBound->toString());
		cumulativeSize += slotSize;
	}
	if (keyrangeSplitter.size() < numAppliers) {
		TraceEvent(SevWarnAlways, "FastRestoreControllerPhaseCalculateApplierKeyRanges")
		    .detail("NotAllAppliersAreUsed", keyrangeSplitter.size())
		    .detail("NumAppliers", numAppliers);
	} else if (keyrangeSplitter.size() > numAppliers) {
		bool expected = (keyrangeSplitter.size() == numAppliers + 1);
		TraceEvent(expected ? SevWarn : SevError, "FastRestoreControllerPhaseCalculateApplierKeyRanges")
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
	TraceEvent("FastRestoreControllerPhaseCalculateApplierKeyRangesDone")
	    .detail("BatchIndex", batchIndex)
	    .detail("SamplingSize", batchData->samplesSize)
	    .detail("SlotSize", slotSize);
	batchData->samples.clear();
}

ACTOR static Future<std::vector<RestoreRequest>> collectRestoreRequests(Database cx) {
	state std::vector<RestoreRequest> restoreRequests;
	state Future<Void> watch4RestoreRequest;
	state ReadYourWritesTransaction tr(cx);

	// restoreRequestTriggerKey should already been set
	loop {
		try {
			TraceEvent("FastRestoreControllerPhaseCollectRestoreRequestsWait").log();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			// Sanity check
			Optional<Value> numRequests = wait(tr.get(restoreRequestTriggerKey));
			ASSERT(numRequests.present());

			RangeResult restoreRequestValues = wait(tr.getRange(restoreRequestKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!restoreRequestValues.more);
			if (restoreRequestValues.size()) {
				for (auto& it : restoreRequestValues) {
					restoreRequests.push_back(decodeRestoreRequestValue(it.value));
					TraceEvent("FastRestoreControllerPhaseCollectRestoreRequests")
					    .detail("RestoreRequest", restoreRequests.back().toString());
				}
				break;
			} else {
				TraceEvent(SevError, "FastRestoreControllerPhaseCollectRestoreRequestsEmptyRequests").log();
				wait(delay(5.0));
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return restoreRequests;
}

// Collect the backup files' description into output_files by reading the backupContainer bc.
// Returns the restore target version.
ACTOR static Future<Version> collectBackupFiles(Reference<IBackupContainer> bc,
                                                std::vector<RestoreFileFR>* rangeFiles,
                                                std::vector<RestoreFileFR>* logFiles,
                                                Version* minRangeVersion,
                                                Database cx,
                                                RestoreRequest request) {
	state BackupDescription desc = wait(bc->describeBackup());

	// Convert version to real time for operators to read the BackupDescription desc.
	wait(desc.resolveVersionTimes(cx));

	if (request.targetVersion == invalidVersion && desc.maxRestorableVersion.present()) {
		request.targetVersion = desc.maxRestorableVersion.get();
	}

	TraceEvent("FastRestoreControllerPhaseCollectBackupFilesStart")
	    .detail("TargetVersion", request.targetVersion)
	    .detail("BackupDesc", desc.toString())
	    .detail("UseRangeFile", SERVER_KNOBS->FASTRESTORE_USE_RANGE_FILE)
	    .detail("UseLogFile", SERVER_KNOBS->FASTRESTORE_USE_LOG_FILE);
	if (g_network->isSimulated()) {
		std::cout << "Restore to version: " << request.targetVersion << "\nBackupDesc: \n" << desc.toString() << "\n\n";
	}

	state VectorRef<KeyRangeRef> restoreRanges;
	restoreRanges.add(request.range);
	Optional<RestorableFileSet> restorable = wait(bc->getRestoreSet(request.targetVersion, restoreRanges));

	if (!restorable.present()) {
		TraceEvent(SevWarn, "FastRestoreControllerPhaseCollectBackupFiles")
		    .detail("NotRestorable", request.targetVersion);
		throw restore_missing_data();
	}

	ASSERT(rangeFiles->empty());
	ASSERT(logFiles->empty());

	std::set<RestoreFileFR> uniqueRangeFiles;
	std::set<RestoreFileFR> uniqueLogFiles;
	double rangeSize = 0;
	double logSize = 0;
	*minRangeVersion = MAX_VERSION;
	if (SERVER_KNOBS->FASTRESTORE_USE_RANGE_FILE) {
		for (const RangeFile& f : restorable.get().ranges) {
			TraceEvent(SevFRDebugInfo, "FastRestoreControllerPhaseCollectBackupFiles")
			    .detail("RangeFile", f.toString());
			if (f.fileSize <= 0) {
				continue;
			}
			RestoreFileFR file(f);
			TraceEvent(SevFRDebugInfo, "FastRestoreControllerPhaseCollectBackupFiles")
			    .detail("RangeFileFR", file.toString());
			uniqueRangeFiles.insert(file);
			rangeSize += file.fileSize;
			*minRangeVersion = std::min(*minRangeVersion, file.version);
		}
	}
	if (MAX_VERSION == *minRangeVersion) {
		*minRangeVersion = 0; // If no range file, range version must be 0 so that we apply all mutations
	}

	if (SERVER_KNOBS->FASTRESTORE_USE_LOG_FILE) {
		for (const LogFile& f : restorable.get().logs) {
			TraceEvent(SevFRDebugInfo, "FastRestoreControllerPhaseCollectBackupFiles").detail("LogFile", f.toString());
			if (f.fileSize <= 0) {
				continue;
			}
			RestoreFileFR file(f);
			TraceEvent(SevFRDebugInfo, "FastRestoreControllerPhaseCollectBackupFiles")
			    .detail("LogFileFR", file.toString());
			logFiles->push_back(file);
			uniqueLogFiles.insert(file);
			logSize += file.fileSize;
		}
	}

	// Assign unique range files and log files to output
	rangeFiles->assign(uniqueRangeFiles.begin(), uniqueRangeFiles.end());
	logFiles->assign(uniqueLogFiles.begin(), uniqueLogFiles.end());

	TraceEvent("FastRestoreControllerPhaseCollectBackupFilesDone")
	    .detail("BackupDesc", desc.toString())
	    .detail("RangeFiles", rangeFiles->size())
	    .detail("LogFiles", logFiles->size())
	    .detail("RangeFileBytes", rangeSize)
	    .detail("LogFileBytes", logSize)
	    .detail("UseRangeFile", SERVER_KNOBS->FASTRESTORE_USE_RANGE_FILE)
	    .detail("UseLogFile", SERVER_KNOBS->FASTRESTORE_USE_LOG_FILE);
	return request.targetVersion;
}

// By the first and last block of *file to get (beginKey, endKey);
// set (beginKey, endKey) and file->version to pRangeVersions
ACTOR static Future<Void> insertRangeVersion(KeyRangeMap<Version>* pRangeVersions,
                                             RestoreFileFR* file,
                                             Reference<IBackupContainer> bc) {
	TraceEvent("FastRestoreControllerDecodeRangeVersion").detail("File", file->toString());
	RangeFile rangeFile = { file->version, (uint32_t)file->blockSize, file->fileName, file->fileSize };

	// First and last key are the range for this file: endKey is exclusive
	KeyRange fileRange = wait(bc->getSnapshotFileKeyRange(rangeFile));
	TraceEvent("FastRestoreControllerInsertRangeVersion")
	    .detail("DecodedRangeFile", file->fileName)
	    .detail("KeyRange", fileRange)
	    .detail("Version", file->version);
	// Update version for pRangeVersions's ranges in fileRange
	auto ranges = pRangeVersions->modify(fileRange);
	for (auto r = ranges.begin(); r != ranges.end(); ++r) {
		r->value() = std::max(r->value(), file->version);
	}

	if (SERVER_KNOBS->FASTRESTORE_DUMP_INSERT_RANGE_VERSION) {
		// Dump the new key ranges for debugging purpose.
		ranges = pRangeVersions->ranges();
		int i = 0;
		for (auto r = ranges.begin(); r != ranges.end(); ++r) {
			TraceEvent(SevDebug, "RangeVersionsAfterUpdate")
			    .detail("File", file->toString())
			    .detail("FileRange", fileRange.toString())
			    .detail("FileVersion", file->version)
			    .detail("RangeIndex", i++)
			    .detail("RangeBegin", r->begin())
			    .detail("RangeEnd", r->end())
			    .detail("RangeVersion", r->value());
		}
	}

	return Void();
}

// Build the version skyline of snapshot ranges by parsing range files;
// Expensive and slow operation that should not run in real prod.
ACTOR static Future<Void> buildRangeVersions(KeyRangeMap<Version>* pRangeVersions,
                                             std::vector<RestoreFileFR>* pRangeFiles,
                                             Key url,
                                             Optional<std::string> proxy) {
	if (!g_network->isSimulated()) {
		TraceEvent(SevError, "ExpensiveBuildRangeVersions")
		    .detail("Reason", "Parsing all range files is slow and memory intensive");
		return Void();
	}
	Reference<IBackupContainer> bc = IBackupContainer::openContainer(url.toString(), proxy, {});

	// Key ranges not in range files are empty;
	// Assign highest version to avoid applying any mutation in these ranges
	state int fileIndex = 0;
	state std::vector<Future<Void>> fInsertRangeVersions;
	for (; fileIndex < pRangeFiles->size(); ++fileIndex) {
		fInsertRangeVersions.push_back(insertRangeVersion(pRangeVersions, &pRangeFiles->at(fileIndex), bc));
	}

	wait(waitForAll(fInsertRangeVersions));

	return Void();
}

/*
ACTOR static Future<Void> clearDB(Database cx) {
    wait(runRYWTransaction(cx, [](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
        tr->setOption(FDBTransactionOptions::LOCK_AWARE);
        tr->clear(normalKeys);
        return Void();
    }));

    return Void();
}
*/

ACTOR static Future<Void> initializeVersionBatch(std::map<UID, RestoreApplierInterface> appliersInterf,
                                                 std::map<UID, RestoreLoaderInterface> loadersInterf,
                                                 int batchIndex) {
	TraceEvent("FastRestoreControllerPhaseInitVersionBatchForAppliersStart")
	    .detail("BatchIndex", batchIndex)
	    .detail("Appliers", appliersInterf.size());
	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requestsToAppliers;
	requestsToAppliers.reserve(appliersInterf.size());
	for (auto& applier : appliersInterf) {
		requestsToAppliers.emplace_back(applier.first, RestoreVersionBatchRequest(batchIndex));
	}
	wait(sendBatchRequests(&RestoreApplierInterface::initVersionBatch, appliersInterf, requestsToAppliers));

	TraceEvent("FastRestoreControllerPhaseInitVersionBatchForLoaders")
	    .detail("BatchIndex", batchIndex)
	    .detail("Loaders", loadersInterf.size());
	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requestsToLoaders;
	requestsToLoaders.reserve(loadersInterf.size());
	for (auto& loader : loadersInterf) {
		requestsToLoaders.emplace_back(loader.first, RestoreVersionBatchRequest(batchIndex));
	}
	wait(sendBatchRequests(&RestoreLoaderInterface::initVersionBatch, loadersInterf, requestsToLoaders));

	TraceEvent("FastRestoreControllerPhaseInitVersionBatchForAppliersDone").detail("BatchIndex", batchIndex);
	return Void();
}

// Calculate the amount of data each applier should keep outstanding to DB;
// This is the amount of data that are in in-progress transactions.
ACTOR static Future<Void> updateApplierWriteBW(Reference<ControllerBatchData> batchData,
                                               std::map<UID, RestoreApplierInterface> appliersInterf,
                                               int batchIndex) {
	state std::unordered_map<UID, double> applierRemainMB;
	state double totalRemainMB = SERVER_KNOBS->FASTRESTORE_WRITE_BW_MB;
	state double standardAvgBW = SERVER_KNOBS->FASTRESTORE_WRITE_BW_MB / SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS;
	state int loopCount = 0;
	state std::vector<RestoreUpdateRateReply> replies;
	state std::vector<std::pair<UID, RestoreUpdateRateRequest>> requests;
	for (auto& applier : appliersInterf) {
		applierRemainMB[applier.first] = SERVER_KNOBS->FASTRESTORE_WRITE_BW_MB / SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS;
	}

	loop {
		requests.clear();
		for (auto& applier : appliersInterf) {
			double writeRate = totalRemainMB > 1 ? (applierRemainMB[applier.first] / totalRemainMB) *
			                                           SERVER_KNOBS->FASTRESTORE_WRITE_BW_MB
			                                     : standardAvgBW;
			requests.emplace_back(applier.first, RestoreUpdateRateRequest(batchIndex, writeRate));
		}
		replies.clear();
		wait(getBatchReplies(
		    &RestoreApplierInterface::updateRate,
		    appliersInterf,
		    requests,
		    &replies,
		    TaskPriority::DefaultEndpoint)); // DefaultEndpoint has higher priority than fast restore endpoints
		ASSERT(replies.size() == requests.size());
		totalRemainMB = 0;
		for (int i = 0; i < replies.size(); i++) {
			UID& applierID = requests[i].first;
			applierRemainMB[applierID] = replies[i].remainMB;
			totalRemainMB += replies[i].remainMB;
		}
		ASSERT(totalRemainMB >= 0);
		double delayTime = SERVER_KNOBS->FASTRESTORE_RATE_UPDATE_SECONDS;
		if (loopCount == 0) { // First loop: Need to update writeRate quicker
			delayTime = 0.2;
		}
		loopCount++;
		wait(delay(delayTime));
	}
}

// Ask each applier to apply its received mutations to DB
// NOTE: Controller cannot start applying mutations at batchIndex until all appliers have applied for (batchIndex - 1)
//       because appliers at different batchIndex may have overlapped key ranges.
ACTOR static Future<Void> notifyApplierToApplyMutations(Reference<ControllerBatchData> batchData,
                                                        Reference<ControllerBatchStatus> batchStatus,
                                                        std::map<UID, RestoreApplierInterface> appliersInterf,
                                                        int batchIndex,
                                                        NotifiedVersion* finishedBatch) {
	TraceEvent("FastRestoreControllerPhaseApplyToDBStart")
	    .detail("BatchIndex", batchIndex)
	    .detail("FinishedBatch", finishedBatch->get());

	wait(finishedBatch->whenAtLeast(batchIndex - 1));

	state Future<Void> updateRate;

	if (finishedBatch->get() == batchIndex - 1) {
		// Prepare the applyToDB requests
		std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;

		TraceEvent("FastRestoreControllerPhaseApplyToDBRunning")
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
			batchData->applyToDB = getBatchReplies(&RestoreApplierInterface::applyToDB,
			                                       appliersInterf,
			                                       requests,
			                                       &replies,
			                                       TaskPriority::RestoreApplierWriteDB);
			updateRate = updateApplierWriteBW(batchData, appliersInterf, batchIndex);
		} else {
			TraceEvent(SevError, "FastRestoreControllerPhaseApplyToDB")
			    .detail("BatchIndex", batchIndex)
			    .detail("Attention", "Actor should not be invoked twice for the same batch index");
		}

		ASSERT(batchData->applyToDB.present());
		ASSERT(!batchData->applyToDB.get().isError());
		wait(batchData->applyToDB.get());

		// Sanity check all appliers have applied data to destination DB
		for (auto& reply : replies) {
			if (batchStatus->applyStatus[reply.id] == RestoreApplyStatus::Applying) {
				batchStatus->applyStatus[reply.id] = RestoreApplyStatus::Applied;
				if (reply.isDuplicated) {
					TraceEvent(SevWarn, "FastRestoreControllerPhaseApplyToDB")
					    .detail("Applier", reply.id)
					    .detail("DuplicateRequestReturnEarlier", "Apply db request should have been processed");
				}
			}
		}
		for (auto& applier : appliersInterf) {
			if (batchStatus->applyStatus[applier.first] != RestoreApplyStatus::Applied) {
				TraceEvent(SevError, "FastRestoreControllerPhaseApplyToDB")
				    .detail("Applier", applier.first)
				    .detail("ApplyStatus", batchStatus->applyStatus[applier.first]);
			}
		}
		finishedBatch->set(batchIndex);
	}

	TraceEvent("FastRestoreControllerPhaseApplyToDBDone")
	    .detail("BatchIndex", batchIndex)
	    .detail("FinishedBatch", finishedBatch->get());

	return Void();
}

// Notify loaders that all data in the version batch has been applied to DB.
ACTOR static Future<Void> notifyLoadersVersionBatchFinished(std::map<UID, RestoreLoaderInterface> loadersInterf,
                                                            int batchIndex) {
	TraceEvent("FastRestoreControllerPhaseNotifyLoadersVersionBatchFinishedStart").detail("BatchIndex", batchIndex);
	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requestsToLoaders;
	requestsToLoaders.reserve(loadersInterf.size());
	for (auto& loader : loadersInterf) {
		requestsToLoaders.emplace_back(loader.first, RestoreVersionBatchRequest(batchIndex));
	}
	wait(sendBatchRequests(&RestoreLoaderInterface::finishVersionBatch, loadersInterf, requestsToLoaders));
	TraceEvent("FastRestoreControllerPhaseNotifyLoadersVersionBatchFinishedDone").detail("BatchIndex", batchIndex);

	return Void();
}

// Ask all loaders and appliers to perform housecleaning at the end of a restore request
// Terminate those roles if terminate = true
ACTOR static Future<Void> notifyRestoreCompleted(Reference<RestoreControllerData> self, bool terminate = false) {
	std::vector<std::pair<UID, RestoreFinishRequest>> requests;
	TraceEvent("FastRestoreControllerPhaseNotifyRestoreCompletedStart").log();
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

	// If terminate = true, loaders and appliers exits immediately after it receives the request. Controller may not
	// receive acks.
	if (!terminate) {
		wait(endLoaders && endAppliers);
	}

	TraceEvent("FastRestoreControllerPhaseNotifyRestoreCompletedDone").log();

	return Void();
}

// Register the restoreRequestDoneKey to signal the end of restore
ACTOR static Future<Void> signalRestoreCompleted(Reference<RestoreControllerData> self, Database cx) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

	wait(notifyRestoreCompleted(self, true)); // notify workers the restore has completed

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

	TraceEvent("FastRestoreControllerAllRestoreCompleted").log();

	return Void();
}

/*
// Update the most recent time when controller receives heartbeat from each loader and applier
// TODO: Replace the heartbeat mechanism with FDB failure monitoring mechanism
ACTOR static Future<Void> updateHeartbeatTime(Reference<RestoreControllerData> self) {
    wait(self->recruitedRoles.getFuture());

    int numRoles = self->loadersInterf.size() + self->appliersInterf.size();
    state std::map<UID, RestoreLoaderInterface>::iterator loader = self->loadersInterf.begin();
    state std::map<UID, RestoreApplierInterface>::iterator applier = self->appliersInterf.begin();
    state std::vector<Future<RestoreCommonReply>> fReplies(numRoles, Never()); // TODO: Reserve memory for this vector
    state std::vector<UID> nodes;
    state int index = 0;
    state Future<Void> fTimeout = Void();

    // Initialize nodes only once
    std::transform(self->loadersInterf.begin(),
                   self->loadersInterf.end(),
                   std::back_inserter(nodes),
                   [](const std::pair<UID, RestoreLoaderInterface>& in) { return in.first; });
    std::transform(self->appliersInterf.begin(),
                   self->appliersInterf.end(),
                   std::back_inserter(nodes),
                   [](const std::pair<UID, RestoreApplierInterface>& in) { return in.first; });

    loop {
        loader = self->loadersInterf.begin();
        applier = self->appliersInterf.begin();
        index = 0;
        std::fill(fReplies.begin(), fReplies.end(), Never());
        // ping loaders and appliers
        while (loader != self->loadersInterf.end()) {
            fReplies[index] = loader->second.heartbeat.getReply(RestoreSimpleRequest());
            loader++;
            index++;
        }
        while (applier != self->appliersInterf.end()) {
            fReplies[index] = applier->second.heartbeat.getReply(RestoreSimpleRequest());
            applier++;
            index++;
        }

        fTimeout = delay(SERVER_KNOBS->FASTRESTORE_HEARTBEAT_DELAY);

        // Here we have to handle error, otherwise controller worker will fail and exit.
        try {
            wait(waitForAll(fReplies) || fTimeout);
        } catch (Error& e) {
            // This should be an ignorable error.
            TraceEvent(g_network->isSimulated() ? SevWarnAlways : SevError, "FastRestoreUpdateHeartbeatError").error(e);
        }

        // Update the most recent heart beat time for each role
        for (int i = 0; i < fReplies.size(); ++i) {
            if (!fReplies[i].isError() && fReplies[i].isReady()) {
                double currentTime = now();
                auto item = self->rolesHeartBeatTime.emplace(nodes[i], currentTime);
                item.first->second = currentTime;
            }
        }
        wait(fTimeout); // Ensure not updating heartbeat too quickly
    }
}
*/

// Check if a restore role dies or disconnected
ACTOR static Future<Void> checkRolesLiveness(Reference<RestoreControllerData> self) {
	loop {
		wait(delay(SERVER_KNOBS->FASTRESTORE_HEARTBEAT_MAX_DELAY));
		for (auto& role : self->rolesHeartBeatTime) {
			if (now() - role.second > SERVER_KNOBS->FASTRESTORE_HEARTBEAT_MAX_DELAY) {
				TraceEvent(SevWarnAlways, "FastRestoreUnavailableRole", role.first)
				    .detail("Delta", now() - role.second)
				    .detail("LastAliveTime", role.second);
			}
		}
	}
}
