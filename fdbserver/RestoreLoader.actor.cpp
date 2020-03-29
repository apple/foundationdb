/*
 * RestoreLoader.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

// This file implements the functions and actors used by the RestoreLoader role.
// The RestoreLoader role starts with the restoreLoaderCore actor

#include "fdbclient/BackupContainer.h"
#include "fdbserver/RestoreLoader.actor.h"
#include "fdbserver/RestoreRoleCommon.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// SerializedMutationListMap:
// Key is the signature/version of the mutation list, Value is the mutation list (or part of the mutation list)
typedef std::map<Standalone<StringRef>, Standalone<StringRef>> SerializedMutationListMap;
// SerializedMutationPartMap:
// Key has the same semantics as SerializedMutationListMap; Value is the part number of the splitted mutation list
typedef std::map<Standalone<StringRef>, uint32_t> SerializedMutationPartMap;

std::vector<UID> getApplierIDs(std::map<Key, UID>& rangeToApplier);
void splitMutation(std::map<Key, UID>* pRangeToApplier, MutationRef m, Arena& mvector_arena,
                   VectorRef<MutationRef>& mvector, Arena& nodeIDs_arena, VectorRef<UID>& nodeIDs);
void _parseSerializedMutation(std::map<LoadingParam, VersionedMutationsMap>::iterator kvOpsIter,
                              SerializedMutationListMap* mutationMap,
                              std::map<LoadingParam, MutationsVec>::iterator samplesIter, LoaderCounters* cc,
                              const RestoreAsset& asset);

void handleRestoreSysInfoRequest(const RestoreSysInfoRequest& req, Reference<RestoreLoaderData> self);
ACTOR Future<Void> handleLoadFileRequest(RestoreLoadFileRequest req, Reference<RestoreLoaderData> self);
ACTOR Future<Void> handleSendMutationsRequest(RestoreSendMutationsToAppliersRequest req,
                                              Reference<RestoreLoaderData> self);
ACTOR Future<Void> sendMutationsToApplier(VersionedMutationsMap* pkvOps, int batchIndex, RestoreAsset asset,
                                          bool isRangeFile, std::map<Key, UID>* pRangeToApplier,
                                          std::map<UID, RestoreApplierInterface>* pApplierInterfaces);
ACTOR static Future<Void> _parseLogFileToMutationsOnLoader(NotifiedVersion* pProcessedFileOffset,
                                                           SerializedMutationListMap* mutationMap,
                                                           SerializedMutationPartMap* mutationPartMap,
                                                           Reference<IBackupContainer> bc, RestoreAsset asset);
ACTOR static Future<Void> _parseRangeFileToMutationsOnLoader(
    std::map<LoadingParam, VersionedMutationsMap>::iterator kvOpsIter,
    std::map<LoadingParam, MutationsVec>::iterator samplesIter, LoaderCounters* cc, Reference<IBackupContainer> bc,
    Version version, RestoreAsset asset);
ACTOR Future<Void> handleFinishVersionBatchRequest(RestoreVersionBatchRequest req, Reference<RestoreLoaderData> self);

ACTOR Future<Void> restoreLoaderCore(RestoreLoaderInterface loaderInterf, int nodeIndex, Database cx) {
	state Reference<RestoreLoaderData> self =
	    Reference<RestoreLoaderData>(new RestoreLoaderData(loaderInterf.id(), nodeIndex));
	state ActorCollection actors(false);
	state Future<Void> exitRole = Never();
	state Future<Void> updateProcessStatsTimer = delay(SERVER_KNOBS->FASTRESTORE_UPDATE_PROCESS_STATS_INTERVAL);

	actors.add(traceProcessMetrics(self, "Loader"));

	loop {
		state std::string requestTypeStr = "[Init]";

		try {
			choose {
				when(RestoreSimpleRequest req = waitNext(loaderInterf.heartbeat.getFuture())) {
					requestTypeStr = "heartbeat";
					actors.add(handleHeartbeat(req, loaderInterf.id()));
				}
				when(RestoreSysInfoRequest req = waitNext(loaderInterf.updateRestoreSysInfo.getFuture())) {
					requestTypeStr = "updateRestoreSysInfo";
					handleRestoreSysInfoRequest(req, self);
				}
				when(RestoreLoadFileRequest req = waitNext(loaderInterf.loadFile.getFuture())) {
					requestTypeStr = "loadFile";
					self->initBackupContainer(req.param.url);
					actors.add(handleLoadFileRequest(req, self));
				}
				when(RestoreSendMutationsToAppliersRequest req = waitNext(loaderInterf.sendMutations.getFuture())) {
					requestTypeStr = "sendMutations";
					actors.add(handleSendMutationsRequest(req, self));
				}
				when(RestoreVersionBatchRequest req = waitNext(loaderInterf.initVersionBatch.getFuture())) {
					requestTypeStr = "initVersionBatch";
					actors.add(handleInitVersionBatchRequest(req, self));
				}
				when(RestoreVersionBatchRequest req = waitNext(loaderInterf.finishVersionBatch.getFuture())) {
					requestTypeStr = "finishVersionBatch";
					actors.add(handleFinishVersionBatchRequest(req, self));
				}
				when(RestoreFinishRequest req = waitNext(loaderInterf.finishRestore.getFuture())) {
					requestTypeStr = "finishRestore";
					handleFinishRestoreRequest(req, self);
					if (req.terminate) {
						exitRole = Void();
					}
				}
				when(wait(updateProcessStatsTimer)) {
					updateProcessStats(self);
					updateProcessStatsTimer = delay(SERVER_KNOBS->FASTRESTORE_UPDATE_PROCESS_STATS_INTERVAL);
				}
				when(wait(exitRole)) {
					TraceEvent("FastRestore").detail("RestoreLoaderCore", "ExitRole").detail("NodeID", self->id());
					break;
				}
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "FastRestore")
			    .detail("RestoreLoaderError", e.what())
			    .detail("RequestType", requestTypeStr);
			break;
		}
	}

	return Void();
}

// Assume: Only update the local data if it (applierInterf) has not been set
void handleRestoreSysInfoRequest(const RestoreSysInfoRequest& req, Reference<RestoreLoaderData> self) {
	TraceEvent("FastRestoreLoader", self->id()).detail("HandleRestoreSysInfoRequest", self->id());
	ASSERT(self.isValid());

	// The loader has received the appliers interfaces
	if (!self->appliersInterf.empty()) {
		req.reply.send(RestoreCommonReply(self->id()));
		return;
	}

	self->appliersInterf = req.sysInfo.appliers;

	req.reply.send(RestoreCommonReply(self->id()));
}

// Parse a data block in a partitioned mutation log file and store mutations
// into "kvOpsIter" and samples into "samplesIter".
ACTOR static Future<Void> _parsePartitionedLogFileOnLoader(
    NotifiedVersion* processedFileOffset, std::map<LoadingParam, VersionedMutationsMap>::iterator kvOpsIter,
    std::map<LoadingParam, MutationsVec>::iterator samplesIter, Reference<IBackupContainer> bc, RestoreAsset asset) {
	state Standalone<StringRef> buf = makeString(asset.len);
	state Reference<IAsyncFile> file = wait(bc->readFile(asset.filename));
	int rLen = wait(file->read(mutateString(buf), asset.len, asset.offset));
	if (rLen != asset.len) throw restore_bad_read();

	TraceEvent("FastRestore")
	    .detail("DecodingLogFile", asset.filename)
	    .detail("Offset", asset.offset)
	    .detail("Length", asset.len);

	// Ensure data blocks in the same file are processed in order
	wait(processedFileOffset->whenAtLeast(asset.offset));
	ASSERT(processedFileOffset->get() == asset.offset);

	StringRefReader reader(buf, restore_corrupted_data());
	try {
		// Read block header
		if (reader.consume<int32_t>() != PARTITIONED_MLOG_VERSION) throw restore_unsupported_file_version();

		VersionedMutationsMap& kvOps = kvOpsIter->second;
		while (1) {
			// If eof reached or first key len bytes is 0xFF then end of block was reached.
			if (reader.eof() || *reader.rptr == 0xFF) break;

			// Deserialize messages written in saveMutationsToFile().
			LogMessageVersion msgVersion;
			msgVersion.version = reader.consumeNetworkUInt64();
			msgVersion.sub = reader.consumeNetworkUInt32();
			int msgSize = reader.consumeNetworkInt32();
			const uint8_t* message = reader.consume(msgSize);

			// Skip mutations out of the version range
			if (!asset.isInVersionRange(msgVersion.version)) continue;

			VersionedMutationsMap::iterator it;
			bool inserted;
			std::tie(it, inserted) = kvOps.emplace(msgVersion, MutationsVec());
			ASSERT(inserted);

			ArenaReader rd(buf.arena(), StringRef(message, msgSize), AssumeVersion(currentProtocolVersion));
			MutationRef mutation;
			rd >> mutation;

			// Should this mutation be skipped?
			if (mutation.param1 >= asset.range.end ||
			    (isRangeMutation(mutation) && mutation.param2 < asset.range.begin) ||
			    (!isRangeMutation(mutation) && mutation.param1 < asset.range.begin)) {
				continue;
			}
			// Only apply mutation within the asset.range
			if (isRangeMutation(mutation)) {
				mutation.param1 = mutation.param1 >= asset.range.begin ? mutation.param1 : asset.range.begin;
				mutation.param2 = mutation.param2 < asset.range.end ? mutation.param2 : asset.range.end;
			}

			TraceEvent(SevFRMutationInfo, "FastRestore_VerboseDebug")
			    .detail("CommitVersion", msgVersion.toString())
			    .detail("ParsedMutation", mutation.toString());
			it->second.push_back_deep(it->second.arena(), mutation);
			// Sampling (FASTRESTORE_SAMPLING_PERCENT%) data
			if (deterministicRandom()->random01() * 100 < SERVER_KNOBS->FASTRESTORE_SAMPLING_PERCENT) {
				samplesIter->second.push_back_deep(samplesIter->second.arena(), mutation);
			}
		}

		// Make sure any remaining bytes in the block are 0xFF
		for (auto b : reader.remainder()) {
			if (b != 0xFF) throw restore_corrupted_data_padding();
		}
	} catch (Error& e) {
		TraceEvent(SevWarn, "FileRestoreCorruptLogFileBlock")
		    .error(e)
		    .detail("Filename", file->getFilename())
		    .detail("BlockOffset", asset.offset)
		    .detail("BlockLen", asset.len);
		throw;
	}
	processedFileOffset->set(asset.offset + asset.len);
	return Void();
}

ACTOR Future<Void> _processLoadingParam(LoadingParam param, Reference<LoaderBatchData> batchData, UID loaderID,
                                        Reference<IBackupContainer> bc) {
	// Temporary data structure for parsing log files into (version, <K, V, mutationType>)
	// Must use StandAlone to save mutations, otherwise, the mutationref memory will be corrupted
	// mutationMap: Key is the unique identifier for a batch of mutation logs at the same version
	state SerializedMutationListMap mutationMap;
	state std::map<Standalone<StringRef>, uint32_t> mutationPartMap; // Sanity check the data parsing is correct
	state NotifiedVersion processedFileOffset(0);
	state std::vector<Future<Void>> fileParserFutures;
	state std::map<LoadingParam, VersionedMutationsMap>::iterator kvOpsPerLPIter = batchData->kvOpsPerLP.end();
	state std::map<LoadingParam, MutationsVec>::iterator samplesIter = batchData->sampleMutations.end();

	// Q: How to record the  param's fields inside LoadingParam Refer to storageMetrics
	TraceEvent("FastRestoreLoaderProcessLoadingParam", loaderID).detail("LoadingParam", param.toString());
	ASSERT(param.blockSize > 0);
	ASSERT(param.asset.offset % param.blockSize == 0); // Parse file must be at block boundary.
	ASSERT(batchData->kvOpsPerLP.find(param) == batchData->kvOpsPerLP.end());

	// NOTE: map's iterator is guaranteed to be stable, but pointer may not.
	bool inserted;
	std::tie(kvOpsPerLPIter, inserted) = batchData->kvOpsPerLP.emplace(param, VersionedMutationsMap());
	ASSERT(inserted);
	std::tie(samplesIter, inserted) = batchData->sampleMutations.emplace(param, MutationsVec());
	ASSERT(inserted);

	for (int64_t j = param.asset.offset; j < param.asset.len; j += param.blockSize) {
		RestoreAsset subAsset = param.asset;
		subAsset.offset = j;
		subAsset.len = std::min<int64_t>(param.blockSize, param.asset.len - j);
		if (param.isRangeFile) {
			fileParserFutures.push_back(_parseRangeFileToMutationsOnLoader(
			    kvOpsPerLPIter, samplesIter, &batchData->counters, bc, param.rangeVersion.get(), subAsset));
		} else {
			// TODO: Sanity check the log file's range is overlapped with the restored version range
			if (param.isPartitionedLog()) {
				fileParserFutures.push_back(_parsePartitionedLogFileOnLoader(&processedFileOffset, kvOpsPerLPIter,
				                                                             samplesIter, bc, subAsset));
			} else {
				fileParserFutures.push_back(_parseLogFileToMutationsOnLoader(&processedFileOffset, &mutationMap,
				                                                             &mutationPartMap, bc, subAsset));
			}
		}
	}
	wait(waitForAll(fileParserFutures));

	if (!param.isRangeFile && !param.isPartitionedLog()) {
		_parseSerializedMutation(kvOpsPerLPIter, &mutationMap, samplesIter, &batchData->counters, param.asset);
	}

	TraceEvent("FastRestoreLoaderProcessLoadingParamDone", loaderID).detail("LoadingParam", param.toString());

	return Void();
}

// A loader can process multiple RestoreLoadFileRequest in parallel.
ACTOR Future<Void> handleLoadFileRequest(RestoreLoadFileRequest req, Reference<RestoreLoaderData> self) {
	state Reference<LoaderBatchData> batchData = self->batch[req.batchIndex];
	state bool isDuplicated = true;
	ASSERT(batchData.isValid());
	bool paramExist = batchData->processedFileParams.find(req.param) != batchData->processedFileParams.end();
	bool isReady = paramExist ? batchData->processedFileParams[req.param].isReady() : false;

	TraceEvent("FastRestoreLoaderPhaseLoadFile", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("ProcessLoadParam", req.param.toString())
	    .detail("NotProcessed", !paramExist)
	    .detail("Processed", isReady)
	    .detail("CurrentMemory", getSystemStatistics().processMemory);

	wait(isSchedulable(self, req.batchIndex, __FUNCTION__));

	if (batchData->processedFileParams.find(req.param) == batchData->processedFileParams.end()) {
		TraceEvent("FastRestoreLoadFile", self->id())
		    .detail("BatchIndex", req.batchIndex)
		    .detail("ProcessLoadParam", req.param.toString());
		ASSERT(batchData->sampleMutations.find(req.param) == batchData->sampleMutations.end());
		batchData->processedFileParams[req.param] = _processLoadingParam(req.param, batchData, self->id(), self->bc);
		isDuplicated = false;
	} else {
		TraceEvent("FastRestoreLoadFile", self->id())
		    .detail("BatchIndex", req.batchIndex)
		    .detail("WaitOnProcessLoadParam", req.param.toString());
	}
	auto it = batchData->processedFileParams.find(req.param);
	ASSERT(it != batchData->processedFileParams.end());
	wait(it->second); // wait on the processing of the req.param.

	req.reply.send(RestoreLoadFileReply(req.param, batchData->sampleMutations[req.param], isDuplicated));
	TraceEvent("FastRestoreLoaderPhaseLoadFileDone", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("ProcessLoadParam", req.param.toString());
	// TODO: clear self->sampleMutations[req.param] memory to save memory on loader
	return Void();
}

// Send buffered mutations to appliers.
// Do not need to block on low memory usage because this actor should not increase memory usage.
ACTOR Future<Void> handleSendMutationsRequest(RestoreSendMutationsToAppliersRequest req,
                                              Reference<RestoreLoaderData> self) {
	state Reference<LoaderBatchData> batchData = self->batch[req.batchIndex];
	state Reference<LoaderBatchStatus> batchStatus = self->status[req.batchIndex];
	state bool isDuplicated = true;

	TraceEvent("FastRestoreLoaderPhaseSendMutations", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("UseRangeFile", req.useRangeFile)
	    .detail("LoaderSendStatus", batchStatus->toString());

	// Ensure each file is sent exactly once by using batchStatus->sendAllLogs and batchStatus->sendAllRanges
	if (!req.useRangeFile) {
		if (!batchStatus->sendAllLogs.present()) { // Has not sent
			batchStatus->sendAllLogs = Never();
			isDuplicated = false;
			TraceEvent(SevInfo, "FastRestoreSendMutationsProcessLogRequest", self->id())
			    .detail("BatchIndex", req.batchIndex)
			    .detail("UseRangeFile", req.useRangeFile);
		} else if (!batchStatus->sendAllLogs.get().isReady()) { // In the process of sending
			TraceEvent(SevDebug, "FastRestoreSendMutationsWaitDuplicateLogRequest", self->id())
			    .detail("BatchIndex", req.batchIndex)
			    .detail("UseRangeFile", req.useRangeFile);
			wait(batchStatus->sendAllLogs.get());
		} else { // Already sent
			TraceEvent(SevDebug, "FastRestoreSendMutationsSkipDuplicateLogRequest", self->id())
			    .detail("BatchIndex", req.batchIndex)
			    .detail("UseRangeFile", req.useRangeFile);
		}
	} else {
		if (!batchStatus->sendAllRanges.present()) {
			batchStatus->sendAllRanges = Never();
			isDuplicated = false;
			TraceEvent(SevInfo, "FastRestoreSendMutationsProcessRangeRequest", self->id())
			    .detail("BatchIndex", req.batchIndex)
			    .detail("UseRangeFile", req.useRangeFile);
		} else if (!batchStatus->sendAllRanges.get().isReady()) {
			TraceEvent(SevDebug, "FastRestoreSendMutationsWaitDuplicateRangeRequest", self->id())
			    .detail("BatchIndex", req.batchIndex)
			    .detail("UseRangeFile", req.useRangeFile);
			wait(batchStatus->sendAllRanges.get());
		} else {
			TraceEvent(SevDebug, "FastRestoreSendMutationsSkipDuplicateRangeRequest", self->id())
			    .detail("BatchIndex", req.batchIndex)
			    .detail("UseRangeFile", req.useRangeFile);
		}
	}

	if (!isDuplicated) {
		vector<Future<Void>> fSendMutations;
		batchData->rangeToApplier = req.rangeToApplier;
		for (auto& [loadParam, kvOps] : batchData->kvOpsPerLP) {
			if (loadParam.isRangeFile == req.useRangeFile) {
				// Send the parsed mutation to applier who will apply the mutation to DB
				fSendMutations.push_back(sendMutationsToApplier(&kvOps, req.batchIndex, loadParam.asset,
				                                                loadParam.isRangeFile, &batchData->rangeToApplier,
				                                                &self->appliersInterf));
			}
		}
		wait(waitForAll(fSendMutations));
		if (req.useRangeFile) {
			batchStatus->sendAllRanges = Void(); // Finish sending kvs parsed from range files
		} else {
			batchStatus->sendAllLogs = Void();
		}
	}

	TraceEvent("FastRestoreLoaderPhaseSendMutationsDone", self->id())
	    .detail("BatchIndex", req.batchIndex)
	    .detail("UseRangeFile", req.useRangeFile)
	    .detail("LoaderSendStatus", batchStatus->toString());
	req.reply.send(RestoreCommonReply(self->id(), isDuplicated));
	return Void();
}

// Assume: kvOps data are from the same RestoreAsset.
// Input: pkvOps: versioned kv mutation for the asset in the version batch (batchIndex)
//   isRangeFile: is pkvOps from range file? Let receiver (applier) know if the mutation is log mutation;
//   pRangeToApplier: range to applierID mapping, deciding which applier is responsible for which range
//   pApplierInterfaces: applier interfaces to send the mutations to
ACTOR Future<Void> sendMutationsToApplier(VersionedMutationsMap* pkvOps, int batchIndex, RestoreAsset asset,
                                          bool isRangeFile, std::map<Key, UID>* pRangeToApplier,
                                          std::map<UID, RestoreApplierInterface>* pApplierInterfaces) {
	state VersionedMutationsMap& kvOps = *pkvOps;
	state VersionedMutationsMap::iterator kvOp = kvOps.begin();
	state int kvCount = 0;
	state int splitMutationIndex = 0;
	state std::vector<std::pair<UID, RestoreSendVersionedMutationsRequest>> requests;
	state Version prevVersion = 0; // startVersion
	state std::vector<UID> applierIDs = getApplierIDs(*pRangeToApplier);

	TraceEvent("FastRestoreLoaderSendMutationToApplier")
	    .detail("IsRangeFile", isRangeFile)
	    .detail("EndVersion", asset.endVersion)
	    .detail("RestoreAsset", asset.toString());

	// There should be no mutation at asset.endVersion version because it is exclusive
	if (kvOps.lower_bound(LogMessageVersion(asset.endVersion)) != kvOps.end()) {
		TraceEvent(SevError, "FastRestoreLoaderSendMutationToApplier")
		    .detail("BatchIndex", batchIndex)
		    .detail("RestoreAsset", asset.toString())
		    .detail("IsRangeFile", isRangeFile)
		    .detail("Data loss at version", asset.endVersion);
	} else {
		// Ensure there is a mutation request sent at endVersion, so that applier can advance its notifiedVersion
		kvOps[LogMessageVersion(asset.endVersion)] = MutationsVec(); // Empty mutation vector will be handled by applier
	}

	splitMutationIndex = 0;
	kvCount = 0;

	// applierMutationsBuffer is the mutation vector to be sent to each applier
	// applierMutationsSize is buffered mutation vector size for each applier
	state std::map<UID, MutationsVec> applierMutationsBuffer;
	state std::map<UID, SubSequenceVec> applierSubsBuffer;
	state std::map<UID, double> applierMutationsSize;
	for (auto& applierID : applierIDs) {
		applierMutationsBuffer[applierID] = MutationsVec();
		applierSubsBuffer[applierID] = SubSequenceVec();
		applierMutationsSize[applierID] = 0.0;
	}
	for (kvOp = kvOps.begin(); kvOp != kvOps.end(); kvOp++) {
		const LogMessageVersion& commitVersion = kvOp->first;
		ASSERT(commitVersion.version >= asset.beginVersion);
		ASSERT(commitVersion.version <= asset.endVersion); // endVersion is an empty commit to ensure progress

		for (const MutationRef& kvm : kvOp->second) {
			// Send the mutation to applier
			if (isRangeMutation(kvm)) {
				MutationsVec mvector;
				Standalone<VectorRef<UID>> nodeIDs;
				// Because using a vector of mutations causes overhead, and the range mutation should happen rarely;
				// We handle the range mutation and key mutation differently for the benefit of avoiding memory copy
				// WARNING: The splitMutation() may have bugs
				splitMutation(pRangeToApplier, kvm, mvector.arena(), mvector.contents(), nodeIDs.arena(),
				              nodeIDs.contents());
				ASSERT(mvector.size() == nodeIDs.size());

				if (debugMutation("RestoreLoader", commitVersion.version, kvm)) {
					TraceEvent e("DebugSplit");
					int i = 0;
					for (auto& [key, uid] : *pRangeToApplier) {
						e.detail(format("Range%d", i).c_str(), printable(key))
						    .detail(format("UID%d", i).c_str(), uid.toString());
						i++;
					}
				}
				for (splitMutationIndex = 0; splitMutationIndex < mvector.size(); splitMutationIndex++) {
					MutationRef mutation = mvector[splitMutationIndex];
					UID applierID = nodeIDs[splitMutationIndex];
					// printf("SPLITTED MUTATION: %d: mutation:%s applierID:%s\n", splitMutationIndex,
					// mutation.toString().c_str(), applierID.toString().c_str());
					if (debugMutation("RestoreLoader", commitVersion.version, mutation)) {
						TraceEvent("SplittedMutation")
						    .detail("Version", commitVersion.toString())
						    .detail("Mutation", mutation.toString());
					}
					applierMutationsBuffer[applierID].push_back_deep(applierMutationsBuffer[applierID].arena(), mutation);
					applierSubsBuffer[applierID].push_back(applierSubsBuffer[applierID].arena(), commitVersion.sub);
					applierMutationsSize[applierID] += mutation.expectedSize();

					kvCount++;
				}
			} else { // mutation operates on a particular key
				std::map<Key, UID>::iterator itlow = pRangeToApplier->upper_bound(kvm.param1);
				--itlow; // make sure itlow->first <= m.param1
				ASSERT(itlow->first <= kvm.param1);
				UID applierID = itlow->second;
				kvCount++;

				if (debugMutation("RestoreLoader", commitVersion.version, kvm)) {
					TraceEvent("SendMutation")
					    .detail("Applier", applierID)
					    .detail("Version", commitVersion.toString())
					    .detail("Mutation", kvm.toString());
				}
				applierMutationsBuffer[applierID].push_back_deep(applierMutationsBuffer[applierID].arena(), kvm);
				applierSubsBuffer[applierID].push_back(applierSubsBuffer[applierID].arena(), commitVersion.sub);
				applierMutationsSize[applierID] += kvm.expectedSize();
			}
		} // Mutations at the same LogMessageVersion

		// Batch same Version's mutations in one request. We could batch more by
		// changing the version comparison below.
		auto next = std::next(kvOp, 1);
		if (next == kvOps.end() || commitVersion.version < next->first.version) {
			// TODO: Sanity check each asset has been received exactly once!
			// Send the mutations to appliers for each version
			for (const UID& applierID : applierIDs) {
				requests.emplace_back(applierID, RestoreSendVersionedMutationsRequest(
				                                     batchIndex, asset, prevVersion, commitVersion.version, isRangeFile,
				                                     applierMutationsBuffer[applierID], applierSubsBuffer[applierID]));
			}
			TraceEvent(SevDebug, "FastRestore_SendMutationToApplier")
			    .detail("PrevVersion", prevVersion)
			    .detail("CommitVersion", commitVersion.toString())
			    .detail("RestoreAsset", asset.toString());
			ASSERT(prevVersion < commitVersion.version);
			prevVersion = commitVersion.version;
			wait(sendBatchRequests(&RestoreApplierInterface::sendMutationVector, *pApplierInterfaces, requests,
			                       TaskPriority::RestoreLoaderSendMutations));
			requests.clear();
			for (auto& applierID : applierIDs) {
				applierMutationsBuffer[applierID] = MutationsVec();
				applierSubsBuffer[applierID] = SubSequenceVec();
				applierMutationsSize[applierID] = 0.0;
			}
		}
	} // all versions of mutations in the same file

	TraceEvent("FastRestore").detail("LoaderSendMutationOnAppliers", kvCount);
	return Void();
}

// TODO: Add a unit test for this function
void splitMutation(std::map<Key, UID>* pRangeToApplier, MutationRef m, Arena& mvector_arena,
                   VectorRef<MutationRef>& mvector, Arena& nodeIDs_arena, VectorRef<UID>& nodeIDs) {
	TraceEvent(SevWarn, "FastRestoreSplitMutation").detail("Mutation", m.toString());
	// mvector[i] should be mapped to nodeID[i]
	ASSERT(mvector.empty());
	ASSERT(nodeIDs.empty());
	// key range [m->param1, m->param2)
	std::map<Key, UID>::iterator itlow, itup; // we will return [itlow, itup)
	itlow = pRangeToApplier->lower_bound(m.param1); // lower_bound returns the iterator that is >= m.param1
	if (itlow == pRangeToApplier->end()) {
		--itlow;
		mvector.push_back_deep(mvector_arena, m);
		nodeIDs.push_back(nodeIDs_arena, itlow->second);
		return;
	}
	if (itlow->first > m.param1) {
		if (itlow != pRangeToApplier->begin()) {
			--itlow;
		}
	}

	itup = pRangeToApplier->upper_bound(m.param2); // return rmap::end if no key is after m.param2.
	ASSERT(itup == pRangeToApplier->end() || itup->first > m.param2);

	std::map<Key, UID>::iterator itApplier;
	while (itlow != itup) {
		Standalone<MutationRef> curm; // current mutation
		curm.type = m.type;
		// The first split mutation should starts with m.first.
		// The later ones should start with the rangeToApplier boundary.
		if (m.param1 > itlow->first) {
			curm.param1 = m.param1;
		} else {
			curm.param1 = itlow->first;
		}
		itApplier = itlow;
		itlow++;
		if (itlow == itup) {
			ASSERT(m.param2 <= normalKeys.end);
			curm.param2 = m.param2;
		} else if (m.param2 < itlow->first) {
			UNREACHABLE();
			curm.param2 = m.param2;
		} else {
			curm.param2 = itlow->first;
		}
		ASSERT(curm.param1 <= curm.param2);
		mvector.push_back_deep(mvector_arena, curm);
		nodeIDs.push_back(nodeIDs_arena, itApplier->second);
	}
}

// key_input format:
// [logRangeMutation.first][hash_value_of_commit_version:1B][bigEndian64(commitVersion)][bigEndian32(part)]
// value_input: serialized binary of mutations at the same version
bool concatenateBackupMutationForLogFile(std::map<Standalone<StringRef>, Standalone<StringRef>>* pMutationMap,
                                         std::map<Standalone<StringRef>, uint32_t>* pMutationPartMap,
                                         Standalone<StringRef> key_input, Standalone<StringRef> val_input,
                                         const RestoreAsset& asset) {
	SerializedMutationListMap& mutationMap = *pMutationMap;
	std::map<Standalone<StringRef>, uint32_t>& mutationPartMap = *pMutationPartMap;
	const int key_prefix_len = sizeof(uint8_t) + sizeof(Version) + sizeof(uint32_t);

	StringRefReader readerKey(key_input, restore_corrupted_data()); // read key_input!
	int logRangeMutationFirstLength = key_input.size() - key_prefix_len;
	bool concatenated = false;

	ASSERT_WE_THINK(key_input.size() >= key_prefix_len);

	if (logRangeMutationFirstLength > 0) {
		// Strip out the [logRangeMutation.first]; otherwise, the following readerKey.consume will produce wrong value
		readerKey.consume(logRangeMutationFirstLength);
	}

	readerKey.consume<uint8_t>(); // uint8_t hashValue = readerKey.consume<uint8_t>()
	Version commitVersion = readerKey.consumeNetworkUInt64();
	// Skip mutations not in [asset.beginVersion, asset.endVersion), which is what we are only processing right now
	if (!asset.isInVersionRange(commitVersion))  {
		return false;
	}

	uint32_t part = readerKey.consumeNetworkUInt32();
	// Use commitVersion as id
	Standalone<StringRef> id = StringRef((uint8_t*)&commitVersion, sizeof(Version));

	auto it = mutationMap.find(id);
	if (it == mutationMap.end()) {
		mutationMap.insert(std::make_pair(id, val_input));
		if (part != 0) {
			TraceEvent(SevError, "FastRestore")
			    .detail("FirstPartNotZero", part)
			    .detail("KeyInput", getHexString(key_input));
		}
		mutationPartMap.insert(std::make_pair(id, part));
	} else { // Concatenate the val string with the same commitVersion
		it->second = it->second.contents().withSuffix(val_input.contents()); // Assign the new Areana to the map's value
		auto& currentPart = mutationPartMap[id];
		if (part != (currentPart + 1)) {
			// Check if the same range or log file has been processed more than once!
			TraceEvent(SevError, "FastRestore")
			    .detail("CurrentPart1", currentPart)
			    .detail("CurrentPart2", part)
			    .detail("KeyInput", getHexString(key_input))
			    .detail("Hint", "Check if the same range or log file has been processed more than once");
		}
		currentPart = part;
		concatenated = true;
	}

	return concatenated;
}

// Parse the kv pair (version, serialized_mutation), which are the results parsed from log file, into
// (version, <K, V, mutationType>) pair;
// Put the parsed versioned mutations into *pkvOps.
//
// Input key: [commitVersion_of_the_mutation_batch:uint64_t];
// Input value: [includeVersion:uint64_t][val_length:uint32_t][encoded_list_of_mutations], where
// includeVersion is the serialized version in the batch commit. It is not the commitVersion in Input key.
//
// val_length is always equal to (val.size() - 12); otherwise,
// we may not get the entire mutation list for the version encoded_list_of_mutations:
// [mutation1][mutation2]...[mutationk], where
//	a mutation is encoded as [type:uint32_t][keyLength:uint32_t][valueLength:uint32_t][keyContent][valueContent]
void _parseSerializedMutation(std::map<LoadingParam, VersionedMutationsMap>::iterator kvOpsIter,
                              SerializedMutationListMap* pmutationMap,
                              std::map<LoadingParam, MutationsVec>::iterator samplesIter, LoaderCounters* cc,
                              const RestoreAsset& asset) {
	VersionedMutationsMap& kvOps = kvOpsIter->second;
	MutationsVec& samples = samplesIter->second;
	SerializedMutationListMap& mutationMap = *pmutationMap;

	for (auto& m : mutationMap) {
		StringRef k = m.first.contents();
		StringRef val = m.second.contents();

		StringRefReader kReader(k, restore_corrupted_data());
		uint64_t commitVersion = kReader.consume<uint64_t>(); // Consume little Endian data
		// We have already filter the commit not in [beginVersion, endVersion) when we concatenate kv pair in log file
		ASSERT_WE_THINK(asset.isInVersionRange(commitVersion));

		StringRefReader vReader(val, restore_corrupted_data());
		vReader.consume<uint64_t>(); // Consume the includeVersion
		// TODO(xumengpanda): verify the protocol version is compatible and raise error if needed

		// Parse little endian value, confirmed it is correct!
		uint32_t val_length_decoded = vReader.consume<uint32_t>();
		ASSERT(val_length_decoded == val.size() - sizeof(uint64_t) - sizeof(uint32_t));

		int sub = 0;
		while (1) {
			// stop when reach the end of the string
			if (vReader.eof()) { //|| *reader.rptr == 0xFF
				break;
			}

			uint32_t type = vReader.consume<uint32_t>();
			uint32_t kLen = vReader.consume<uint32_t>();
			uint32_t vLen = vReader.consume<uint32_t>();
			const uint8_t* k = vReader.consume(kLen);
			const uint8_t* v = vReader.consume(vLen);

			MutationRef mutation((MutationRef::Type)type, KeyRef(k, kLen), KeyRef(v, vLen));
			// Should this mutation be skipped?
			if (mutation.param1 >= asset.range.end ||
			    (isRangeMutation(mutation) && mutation.param2 < asset.range.begin) ||
			    (!isRangeMutation(mutation) && mutation.param1 < asset.range.begin)) {
				continue;
			}
			// Only apply mutation within the asset.range
			if (isRangeMutation(mutation)) {
				mutation.param1 = mutation.param1 >= asset.range.begin ? mutation.param1 : asset.range.begin;
				mutation.param2 = mutation.param2 < asset.range.end ? mutation.param2 : asset.range.end;
			}

			cc->sampledLogBytes += mutation.totalSize();

			TraceEvent(SevFRMutationInfo, "FastRestore_VerboseDebug")
			    .detail("CommitVersion", commitVersion)
			    .detail("ParsedMutation", mutation.toString());

			auto it = kvOps.insert(std::make_pair(LogMessageVersion(commitVersion, sub++), MutationsVec()));
			ASSERT(it.second); // inserted is true
			ASSERT(sub < std::numeric_limits<int32_t>::max()); // range file mutation uses int32_max as subversion
			it.first->second.push_back_deep(it.first->second.arena(), mutation);

			// Sampling (FASTRESTORE_SAMPLING_PERCENT%) data
			if (deterministicRandom()->random01() * 100 < SERVER_KNOBS->FASTRESTORE_SAMPLING_PERCENT) {
				samples.push_back_deep(samples.arena(), mutation);
			}
			ASSERT_WE_THINK(kLen >= 0 && kLen < val.size());
			ASSERT_WE_THINK(vLen >= 0 && vLen < val.size());
		}
	}
}

// Parsing the data blocks in a range file
// kvOpsIter: saves the parsed versioned-mutations for the sepcific LoadingParam;
// samplesIter: saves the sampled mutations from the parsed versioned-mutations;
// bc: backup container to read the backup file
// version: the version the parsed mutations should be at
// asset: RestoreAsset about which backup data should be parsed
ACTOR static Future<Void> _parseRangeFileToMutationsOnLoader(
    std::map<LoadingParam, VersionedMutationsMap>::iterator kvOpsIter,
    std::map<LoadingParam, MutationsVec>::iterator samplesIter, LoaderCounters* cc, Reference<IBackupContainer> bc,
    Version version, RestoreAsset asset) {
	state VersionedMutationsMap& kvOps = kvOpsIter->second;
	state MutationsVec& sampleMutations = samplesIter->second;

	TraceEvent("FastRestoreDecodedRangeFile")
	    .detail("Filename", asset.filename)
	    .detail("Version", version)
	    .detail("BeginVersion", asset.beginVersion)
	    .detail("EndVersion", asset.endVersion);
	// Sanity check the range file is within the restored version range
	ASSERT_WE_THINK(asset.isInVersionRange(version));

	// The set of key value version is rangeFile.version. the key-value set in the same range file has the same version
	Reference<IAsyncFile> inFile = wait(bc->readFile(asset.filename));
	Standalone<VectorRef<KeyValueRef>> blockData =
	    wait(parallelFileRestore::decodeRangeFileBlock(inFile, asset.offset, asset.len));
	TraceEvent("FastRestore")
	    .detail("DecodedRangeFile", asset.filename)
	    .detail("DataSize", blockData.contents().size());

	// First and last key are the range for this file
	KeyRange fileRange = KeyRangeRef(blockData.front().key, blockData.back().key);

	// If fileRange doesn't intersect restore range then we're done.
	if (!fileRange.intersects(asset.range)) {
		return Void();
	}

	// We know the file range intersects the restore range but there could still be keys outside the restore range.
	// Find the subvector of kv pairs that intersect the restore range.
	// Note that the first and last keys are just the range endpoints for this file.
	// They are metadata, not the real data.
	int rangeStart = 1;
	int rangeEnd = blockData.size() - 1; // The rangeStart and rangeEnd is [,)

	// Slide start from begining, stop if something in range is found
	// Move rangeStart and rangeEnd until they is within restoreRange
	while (rangeStart < rangeEnd && !asset.range.contains(blockData[rangeStart].key)) {
		++rangeStart;
	}
	// Side end from back, stop if something at (rangeEnd-1) is found in range
	while (rangeEnd > rangeStart && !asset.range.contains(blockData[rangeEnd - 1].key)) {
		--rangeEnd;
	}

	// Now data only contains the kv mutation within restoreRange
	VectorRef<KeyValueRef> data = blockData.slice(rangeStart, rangeEnd);

	// Note we give INT_MAX as the sub sequence number to override any log mutations.
	const LogMessageVersion msgVersion(version, std::numeric_limits<int32_t>::max());

	// Convert KV in data into SET mutations of different keys in kvOps
	for (const KeyValueRef& kv : data) {
		// NOTE: The KV pairs in range files are the real KV pairs in original DB.
		// Should NOT add prefix or remove surfix for the backup data!
		MutationRef m(MutationRef::Type::SetValue, kv.key,
		              kv.value); // ASSUME: all operation in range file is set.
		cc->loadedRangeBytes += m.totalSize();

		// We cache all kv operations into kvOps, and apply all kv operations later in one place
		auto it = kvOps.insert(std::make_pair(msgVersion, MutationsVec()));
		TraceEvent(SevFRMutationInfo, "FastRestore_VerboseDebug")
		    .detail("CommitVersion", version)
		    .detail("ParsedMutationKV", m.toString());

		it.first->second.push_back_deep(it.first->second.arena(), m);
		// Sampling (FASTRESTORE_SAMPLING_PERCENT%) data
		if (deterministicRandom()->random01() * 100 < SERVER_KNOBS->FASTRESTORE_SAMPLING_PERCENT) {
			cc->sampledRangeBytes += m.totalSize();
			sampleMutations.push_back_deep(sampleMutations.arena(), m);
		}
	}

	return Void();
}

// Parse data blocks in a log file into a vector of <string, string> pairs.
// Each pair.second contains the mutations at a version encoded in pair.first;
// Step 1: decodeLogFileBlock into <string, string> pairs;
// Step 2: Concatenate the second of pairs with the same pair.first.
// pProcessedFileOffset: ensure each data block is processed in order exactly once;
// pMutationMap: concatenated mutation list string at the mutation's commit version
ACTOR static Future<Void> _parseLogFileToMutationsOnLoader(NotifiedVersion* pProcessedFileOffset,
                                                           SerializedMutationListMap* pMutationMap,
                                                           SerializedMutationPartMap* pMutationPartMap,
                                                           Reference<IBackupContainer> bc, RestoreAsset asset) {
	Reference<IAsyncFile> inFile = wait(bc->readFile(asset.filename));
	// decodeLogFileBlock() must read block by block!
	state Standalone<VectorRef<KeyValueRef>> data =
	    wait(parallelFileRestore::decodeLogFileBlock(inFile, asset.offset, asset.len));
	TraceEvent("FastRestore")
	    .detail("DecodedLogFile", asset.filename)
	    .detail("Offset", asset.offset)
	    .detail("Length", asset.len)
	    .detail("DataSize", data.contents().size());

	// Ensure data blocks in the same file are processed in order
	wait(pProcessedFileOffset->whenAtLeast(asset.offset));

	if (pProcessedFileOffset->get() == asset.offset) {
		for (const KeyValueRef& kv : data) {
			// Concatenate the backuped param1 and param2 (KV) at the same version.
			concatenateBackupMutationForLogFile(pMutationMap, pMutationPartMap, kv.key, kv.value, asset);
		}
		pProcessedFileOffset->set(asset.offset + asset.len);
	}

	return Void();
}

// Return applier IDs that are used to apply key-values
std::vector<UID> getApplierIDs(std::map<Key, UID>& rangeToApplier) {
	std::vector<UID> applierIDs;
	for (auto& applier : rangeToApplier) {
		applierIDs.push_back(applier.second);
	}

	ASSERT(!applierIDs.empty());
	return applierIDs;
}

// Notify loaders that the version batch (index) has been applied.
// This affects which version batch each loader can release actors even when the worker has low memory
ACTOR Future<Void> handleFinishVersionBatchRequest(RestoreVersionBatchRequest req, Reference<RestoreLoaderData> self) {
	// Ensure batch (i-1) is applied before batch i
	TraceEvent("FastRestoreLoaderHandleFinishVersionBatch", self->id())
	    .detail("FinishedBatchIndex", self->finishedBatch.get())
	    .detail("RequestedBatchIndex", req.batchIndex);
	wait(self->finishedBatch.whenAtLeast(req.batchIndex - 1));
	if (self->finishedBatch.get() == req.batchIndex - 1) {
		self->finishedBatch.set(req.batchIndex);
	}
	if (self->delayedActors > 0) {
		self->checkMemory.trigger();
	}
	req.reply.send(RestoreCommonReply(self->id(), false));
	return Void();
}
