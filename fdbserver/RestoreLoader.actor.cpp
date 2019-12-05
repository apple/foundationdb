/*
 * RestoreLoader.actor.cpp
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

// This file implements the functions and actors used by the RestoreLoader role.
// The RestoreLoader role starts with the restoreLoaderCore actor

#include "fdbclient/BackupContainer.h"
#include "fdbserver/RestoreLoader.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// SerializedMutationListMap:
// Key is the signature/version of the mutation list, Value is the mutation list (or part of the mutation list)
typedef std::map<Standalone<StringRef>, Standalone<StringRef>> SerializedMutationListMap;
// SerializedMutationPartMap:
// Key has the same semantics as SerializedMutationListMap; Value is the part number of the splitted mutation list
typedef std::map<Standalone<StringRef>, uint32_t> SerializedMutationPartMap;

bool isRangeMutation(MutationRef m);
void splitMutation(Reference<RestoreLoaderData> self, MutationRef m, Arena& mvector_arena,
                   VectorRef<MutationRef>& mvector, Arena& nodeIDs_arena, VectorRef<UID>& nodeIDs);
void _parseSerializedMutation(std::map<LoadingParam, VersionedMutationsMap>::iterator kvOpsIter,
                              SerializedMutationListMap* mutationMap,
                              std::map<LoadingParam, MutationsVec>::iterator samplesIter, bool isSampling = false);

void handleRestoreSysInfoRequest(const RestoreSysInfoRequest& req, Reference<RestoreLoaderData> self);
ACTOR Future<Void> handleLoadFileRequest(RestoreLoadFileRequest req, Reference<RestoreLoaderData> self,
                                         bool isSampling = false);
ACTOR Future<Void> handleSendMutationsRequest(RestoreSendMutationsToAppliersRequest req,
                                              Reference<RestoreLoaderData> self);
ACTOR Future<Void> sendMutationsToApplier(Reference<RestoreLoaderData> self, VersionedMutationsMap* kvOps,
                                          bool isRangeFile, Version startVersion, Version endVersion, int fileIndex);
ACTOR static Future<Void> _parseLogFileToMutationsOnLoader(
    NotifiedVersion* pProcessedFileOffset, SerializedMutationListMap* mutationMap,
    SerializedMutationPartMap* mutationPartMap, Reference<IBackupContainer> bc, Version version, std::string fileName,
    int64_t readOffset, int64_t readLen, KeyRange restoreRange, Key addPrefix, Key removePrefix, Key mutationLogPrefix);
ACTOR static Future<Void> _parseRangeFileToMutationsOnLoader(
    std::map<LoadingParam, VersionedMutationsMap>::iterator kvOpsIter,
    std::map<LoadingParam, MutationsVec>::iterator samplesIter, Reference<IBackupContainer> bc, Version version,
    std::string fileName, int64_t readOffset_input, int64_t readLen_input, KeyRange restoreRange);

ACTOR Future<Void> restoreLoaderCore(RestoreLoaderInterface loaderInterf, int nodeIndex, Database cx) {
	state Reference<RestoreLoaderData> self =
	    Reference<RestoreLoaderData>(new RestoreLoaderData(loaderInterf.id(), nodeIndex));

	state ActorCollection actors(false);
	state Future<Void> exitRole = Never();
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
					actors.add(handleLoadFileRequest(req, self, false));
				}
				when(RestoreSendMutationsToAppliersRequest req = waitNext(loaderInterf.sendMutations.getFuture())) {
					requestTypeStr = "sendMutations";
					actors.add(handleSendMutationsRequest(req, self));
				}
				when(RestoreVersionBatchRequest req = waitNext(loaderInterf.initVersionBatch.getFuture())) {
					requestTypeStr = "initVersionBatch";
					wait(handleInitVersionBatchRequest(req, self));
				}
				when(RestoreVersionBatchRequest req = waitNext(loaderInterf.finishRestore.getFuture())) {
					requestTypeStr = "finishRestore";
					handleFinishRestoreRequest(req, self);
					exitRole = Void();
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
	TraceEvent("FastRestore").detail("HandleRestoreSysInfoRequest", self->id());
	ASSERT(self.isValid());

	// The loader has received the appliers interfaces
	if (!self->appliersInterf.empty()) {
		req.reply.send(RestoreCommonReply(self->id()));
		return;
	}

	self->appliersInterf = req.sysInfo.appliers;

	req.reply.send(RestoreCommonReply(self->id()));
}

ACTOR Future<Void> _processLoadingParam(LoadingParam param, Reference<RestoreLoaderData> self) {
	// Q: How to record the  param's fields inside LoadingParam Refer to storageMetrics
	TraceEvent("FastRestore").detail("Loader", self->id()).detail("StartProcessLoadParam", param.toString());
	ASSERT(param.blockSize > 0);
	ASSERT(param.offset % param.blockSize == 0); // Parse file must be at block bondary.
	ASSERT(self->kvOpsPerLP.find(param) == self->kvOpsPerLP.end());
	// NOTE: map's iterator is guaranteed to be stable, but pointer may not.
	// state VersionedMutationsMap* kvOps = &self->kvOpsPerLP[param];
	self->kvOpsPerLP.emplace(param, VersionedMutationsMap());
	self->sampleMutations.emplace(param, MutationsVec());
	state std::map<LoadingParam, VersionedMutationsMap>::iterator kvOpsPerLPIter = self->kvOpsPerLP.find(param);
	state std::map<LoadingParam, MutationsVec>::iterator samplesIter = self->sampleMutations.find(param);

	// Temporary data structure for parsing log files into (version, <K, V, mutationType>)
	// Must use StandAlone to save mutations, otherwise, the mutationref memory will be corrupted
	// mutationMap: Key is the unique identifier for a batch of mutation logs at the same version
	state SerializedMutationListMap mutationMap;
	state std::map<Standalone<StringRef>, uint32_t> mutationPartMap; // Sanity check the data parsing is correct
	state NotifiedVersion processedFileOffset(0);
	state std::vector<Future<Void>> fileParserFutures;

	int64_t j;
	int64_t readOffset;
	int64_t readLen;
	for (j = param.offset; j < param.length; j += param.blockSize) {
		readOffset = j;
		readLen = std::min<int64_t>(param.blockSize, param.length - j);
		if (param.isRangeFile) {
			fileParserFutures.push_back(_parseRangeFileToMutationsOnLoader(kvOpsPerLPIter, samplesIter, self->bc,
			                                                               param.version, param.filename, readOffset,
			                                                               readLen, param.restoreRange));
		} else {
			fileParserFutures.push_back(_parseLogFileToMutationsOnLoader(
			    &processedFileOffset, &mutationMap, &mutationPartMap, self->bc, param.version, param.filename,
			    readOffset, readLen, param.restoreRange, param.addPrefix, param.removePrefix, param.mutationLogPrefix));
		}
	}
	wait(waitForAll(fileParserFutures));

	if (!param.isRangeFile) {
		_parseSerializedMutation(kvOpsPerLPIter, &mutationMap, samplesIter);
	}

	TraceEvent("FastRestore").detail("Loader", self->id()).detail("FinishLoadingFile", param.filename);

	return Void();
}

// A loader can process multiple RestoreLoadFileRequest in parallel.
ACTOR Future<Void> handleLoadFileRequest(RestoreLoadFileRequest req, Reference<RestoreLoaderData> self,
                                         bool isSampling) {
	if (self->processedFileParams.find(req.param) == self->processedFileParams.end()) {
		TraceEvent("FastRestore").detail("Loader", self->id()).detail("ProcessLoadParam", req.param.toString());
		ASSERT(self->sampleMutations.find(req.param) == self->sampleMutations.end());
		self->processedFileParams[req.param] = Never();
		self->processedFileParams[req.param] = _processLoadingParam(req.param, self);
	} else {
		TraceEvent("FastRestore").detail("Loader", self->id()).detail("WaitOnProcessLoadParam", req.param.toString());
	}
	ASSERT(self->processedFileParams.find(req.param) != self->processedFileParams.end());
	wait(self->processedFileParams[req.param]); // wait on the processing of the req.param.

	// TODO: Send sampled mutations back to master
	req.reply.send(RestoreLoadFileReply(req.param, self->sampleMutations[req.param]));
	// TODO: clear self->sampleMutations[req.param] memory to save memory on loader
	return Void();
}

ACTOR Future<Void> handleSendMutationsRequest(RestoreSendMutationsToAppliersRequest req,
                                              Reference<RestoreLoaderData> self) {
	self->rangeToApplier = req.rangeToApplier;

	state std::map<LoadingParam, VersionedMutationsMap>::iterator item = self->kvOpsPerLP.begin();
	for (; item != self->kvOpsPerLP.end(); item++) {
		if (item->first.isRangeFile == req.useRangeFile) {
			// Send the parsed mutation to applier who will apply the mutation to DB
			wait(sendMutationsToApplier(self, &item->second, item->first.isRangeFile, item->first.prevVersion,
			                            item->first.endVersion, item->first.fileIndex));
		}
	}

	req.reply.send(RestoreCommonReply(self->id()));
	return Void();
}

// TODO: This function can be revised better
// Assume: kvOps data are from the same file.
ACTOR Future<Void> sendMutationsToApplier(Reference<RestoreLoaderData> self, VersionedMutationsMap* pkvOps,
                                          bool isRangeFile, Version startVersion, Version endVersion, int fileIndex) {
	state VersionedMutationsMap& kvOps = *pkvOps;
	state int kvCount = 0;
	state int splitMutationIndex = 0;
	state std::vector<UID> applierIDs = self->getWorkingApplierIDs();
	state std::vector<std::pair<UID, RestoreSendMutationVectorVersionedRequest>> requests;
	state Version prevVersion = startVersion;

	TraceEvent("FastRestore_SendMutationToApplier")
	    .detail("Loader", self->id())
	    .detail("IsRangeFile", isRangeFile)
	    .detail("StartVersion", startVersion)
	    .detail("EndVersion", endVersion)
	    .detail("FileIndex", fileIndex);

	// Ensure there is a mutation request sent at endVersion, so that applier can advance its notifiedVersion
	if (kvOps.find(endVersion) == kvOps.end()) {
		kvOps[endVersion] = VectorRef<MutationRef>(); // Empty mutation vector will be handled by applier
	}

	// applierMutationsBuffer is the mutation vector to be sent to each applier
	// applierMutationsSize is buffered mutation vector size for each applier
	state std::map<UID, MutationsVec> applierMutationsBuffer;
	state std::map<UID, double> applierMutationsSize;
	state MutationsVec mvector;
	state Standalone<VectorRef<UID>> nodeIDs;

	splitMutationIndex = 0;
	kvCount = 0;
	state VersionedMutationsMap::iterator kvOp = kvOps.begin();

	for (kvOp = kvOps.begin(); kvOp != kvOps.end(); kvOp++) {
		applierMutationsBuffer.clear();
		applierMutationsSize.clear();
		for (auto& applierID : applierIDs) {
			applierMutationsBuffer[applierID] = MutationsVec(VectorRef<MutationRef>());
			applierMutationsSize[applierID] = 0.0;
		}
		state Version commitVersion = kvOp->first;

		for (int mIndex = 0; mIndex < kvOp->second.size(); mIndex++) {
			MutationRef kvm = kvOp->second[mIndex];
			// Send the mutation to applier
			if (isRangeMutation(kvm)) {
				// Because using a vector of mutations causes overhead, and the range mutation should happen rarely;
				// We handle the range mutation and key mutation differently for the benefit of avoiding memory copy
				mvector.pop_front(mvector.size());
				nodeIDs.pop_front(nodeIDs.size());
				// WARNING: The splitMutation() may have bugs
				splitMutation(self, kvm, mvector.arena(), mvector.contents(), nodeIDs.arena(), nodeIDs.contents());
				ASSERT(mvector.size() == nodeIDs.size());

				for (splitMutationIndex = 0; splitMutationIndex < mvector.size(); splitMutationIndex++) {
					MutationRef mutation = mvector[splitMutationIndex];
					UID applierID = nodeIDs[splitMutationIndex];
					// printf("SPLITTED MUTATION: %d: mutation:%s applierID:%s\n", splitMutationIndex,
					// mutation.toString().c_str(), applierID.toString().c_str());
					applierMutationsBuffer[applierID].push_back_deep(applierMutationsBuffer[applierID].arena(), mutation);
					applierMutationsSize[applierID] += mutation.expectedSize();

					kvCount++;
				}
			} else { // mutation operates on a particular key
				std::map<Key, UID>::iterator itlow = self->rangeToApplier.upper_bound(kvm.param1);
				--itlow; // make sure itlow->first <= m.param1
				ASSERT(itlow->first <= kvm.param1);
				MutationRef mutation = kvm;
				UID applierID = itlow->second;
				// printf("KV--Applier: K:%s ApplierID:%s\n", kvm.param1.toString().c_str(),
				// applierID.toString().c_str());
				kvCount++;

				applierMutationsBuffer[applierID].push_back_deep(applierMutationsBuffer[applierID].arena(), mutation);
				applierMutationsSize[applierID] += mutation.expectedSize();
			}
		} // Mutations at the same version

		// Send the mutations to appliers for each version
		for (auto& applierID : applierIDs) {
			requests.push_back(std::make_pair(
			    applierID, RestoreSendMutationVectorVersionedRequest(fileIndex, prevVersion, commitVersion, isRangeFile,
			                                                         applierMutationsBuffer[applierID])));
			applierMutationsBuffer[applierID].pop_front(applierMutationsBuffer[applierID].size());
			applierMutationsSize[applierID] = 0;
		}
		TraceEvent(SevDebug, "FastRestore_Debug")
		    .detail("Loader", self->id())
		    .detail("PrevVersion", prevVersion)
		    .detail("CommitVersion", commitVersion)
		    .detail("FileIndex", fileIndex);
		ASSERT(prevVersion < commitVersion);
		wait(sendBatchRequests(&RestoreApplierInterface::sendMutationVector, self->appliersInterf, requests));
		requests.clear();
		ASSERT(prevVersion < commitVersion);
		prevVersion = commitVersion;
	} // all versions of mutations in the same file

	TraceEvent("FastRestore").detail("LoaderSendMutationOnAppliers", kvCount);
	return Void();
}

// TODO: Add a unit test for this function
void splitMutation(Reference<RestoreLoaderData> self, MutationRef m, Arena& mvector_arena,
                   VectorRef<MutationRef>& mvector, Arena& nodeIDs_arena, VectorRef<UID>& nodeIDs) {
	// mvector[i] should be mapped to nodeID[i]
	ASSERT(mvector.empty());
	ASSERT(nodeIDs.empty());
	// key range [m->param1, m->param2)
	std::map<Standalone<KeyRef>, UID>::iterator itlow, itup; // we will return [itlow, itup)
	itlow = self->rangeToApplier.lower_bound(m.param1); // lower_bound returns the iterator that is >= m.param1
	if (itlow->first > m.param1) {
		if (itlow != self->rangeToApplier.begin()) {
			--itlow;
		}
	}

	itup = self->rangeToApplier.upper_bound(m.param2); // return rmap::end if no key is after m.param2.
	ASSERT(itup == self->rangeToApplier.end() || itup->first > m.param2);

	std::map<Standalone<KeyRef>, UID>::iterator itApplier;
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
                                         Standalone<StringRef> key_input, Standalone<StringRef> val_input) {
	SerializedMutationListMap& mutationMap = *pMutationMap;
	std::map<Standalone<StringRef>, uint32_t>& mutationPartMap = *pMutationPartMap;
	std::string prefix = "||\t";
	std::stringstream ss;
	StringRef val = val_input.contents();
	const int key_prefix_len = sizeof(uint8_t) + sizeof(Version) + sizeof(uint32_t);

	StringRefReaderMX reader(val, restore_corrupted_data());
	StringRefReaderMX readerKey(key_input, restore_corrupted_data()); // read key_input!
	int logRangeMutationFirstLength = key_input.size() - key_prefix_len;
	bool concatenated = false;

	ASSERT_WE_THINK(key_input.size() >= key_prefix_len);

	if (logRangeMutationFirstLength > 0) {
		// Strip out the [logRangeMutation.first]; otherwise, the following readerKey.consume will produce wrong value
		readerKey.consume(logRangeMutationFirstLength);
	}

	readerKey.consume<uint8_t>(); // uint8_t hashValue = readerKey.consume<uint8_t>()
	Version commitVersion = readerKey.consumeNetworkUInt64();
	uint32_t part = readerKey.consumeNetworkUInt32();
	// Use commitVersion as id
	Standalone<StringRef> id = StringRef((uint8_t*)&commitVersion, sizeof(Version));

	if (mutationMap.find(id) == mutationMap.end()) {
		mutationMap.insert(std::make_pair(id, val_input));
		if (part != 0) {
			TraceEvent(SevError, "FastRestore").detail("FirstPartNotZero", part).detail("KeyInput", getHexString(key_input));
		}
		mutationPartMap.insert(std::make_pair(id, part));
	} else { // Concatenate the val string with the same commitVersion
		mutationMap[id] =
		    mutationMap[id].contents().withSuffix(val_input.contents()); // Assign the new Areana to the map's value
		if (part != (mutationPartMap[id] + 1)) {
			// Check if the same range or log file has been processed more than once!
			TraceEvent(SevError, "FastRestore")
				.detail("CurrentPart1", mutationPartMap[id])
				.detail("CurrentPart2", part)
				.detail("KeyInput", getHexString(key_input))
				.detail("Hint", "Check if the same range or log file has been processed more than once");
		}
		mutationPartMap[id] = part;
		concatenated = true;
	}

	return concatenated;
}

bool isRangeMutation(MutationRef m) {
	if (m.type == MutationRef::Type::ClearRange) {
		ASSERT(m.type != MutationRef::Type::DebugKeyRange);
		return true;
	} else {
		ASSERT(m.type == MutationRef::Type::SetValue || isAtomicOp((MutationRef::Type)m.type));
		return false;
	}
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
                              std::map<LoadingParam, MutationsVec>::iterator samplesIter, bool isSampling) {
	VersionedMutationsMap& kvOps = kvOpsIter->second;
	MutationsVec& samples = samplesIter->second;
	SerializedMutationListMap& mutationMap = *pmutationMap;

	for (auto& m : mutationMap) {
		StringRef k = m.first.contents();
		StringRef val = m.second.contents();

		StringRefReaderMX kReader(k, restore_corrupted_data());
		uint64_t commitVersion = kReader.consume<uint64_t>(); // Consume little Endian data
		kvOps.insert(std::make_pair(commitVersion, VectorRef<MutationRef>()));

		StringRefReaderMX vReader(val, restore_corrupted_data());
		vReader.consume<uint64_t>(); // Consume the includeVersion
		// TODO(xumengpanda): verify the protocol version is compatible and raise error if needed

		// Parse little endian value, confirmed it is correct!
		uint32_t val_length_decoded = vReader.consume<uint32_t>();
		ASSERT(val_length_decoded == val.size() - sizeof(uint64_t) - sizeof(uint32_t));

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
			TraceEvent(SevFRMutationInfo, "FastRestore_VerboseDebug")
			    .detail("CommitVersion", commitVersion)
			    .detail("ParsedMutation", mutation.toString());
			kvOps[commitVersion].push_back_deep(kvOps[commitVersion].arena(), mutation);
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
ACTOR static Future<Void> _parseRangeFileToMutationsOnLoader(
    std::map<LoadingParam, VersionedMutationsMap>::iterator kvOpsIter,
    std::map<LoadingParam, MutationsVec>::iterator samplesIter, Reference<IBackupContainer> bc, Version version,
    std::string fileName, int64_t readOffset, int64_t readLen, KeyRange restoreRange) {
	state VersionedMutationsMap& kvOps = kvOpsIter->second;
	state MutationsVec& sampleMutations = samplesIter->second;

	// The set of key value version is rangeFile.version. the key-value set in the same range file has the same version
	Reference<IAsyncFile> inFile = wait(bc->readFile(fileName));
	Standalone<VectorRef<KeyValueRef>> blockData =
	    wait(parallelFileRestore::decodeRangeFileBlock(inFile, readOffset, readLen));
	TraceEvent("FastRestore").detail("DecodedRangeFile", fileName).detail("DataSize", blockData.contents().size());

	// First and last key are the range for this file
	KeyRange fileRange = KeyRangeRef(blockData.front().key, blockData.back().key);

	// If fileRange doesn't intersect restore range then we're done.
	if (!fileRange.intersects(restoreRange)) {
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
	while (rangeStart < rangeEnd && !restoreRange.contains(blockData[rangeStart].key)) {
		++rangeStart;
	}
	// Side end from back, stop if something at (rangeEnd-1) is found in range
	while (rangeEnd > rangeStart && !restoreRange.contains(blockData[rangeEnd - 1].key)) {
		--rangeEnd;
	}

	// Now data only contains the kv mutation within restoreRange
	VectorRef<KeyValueRef> data = blockData.slice(rangeStart, rangeEnd);
	int start = 0;
	int end = data.size();

	// Convert KV in data into mutations in kvOps
	for (int i = start; i < end; ++i) {
		// NOTE: The KV pairs in range files are the real KV pairs in original DB.
		// Should NOT removePrefix and addPrefix for the backup data!
		// In other words, the following operation is wrong:
		// data[i].key.removePrefix(removePrefix).withPrefix(addPrefix)
		MutationRef m(MutationRef::Type::SetValue, data[i].key,
		              data[i].value); // ASSUME: all operation in range file is set.

		// We cache all kv operations into kvOps, and apply all kv operations later in one place
		kvOps.insert(std::make_pair(version, VectorRef<MutationRef>()));
		TraceEvent(SevFRMutationInfo, "FastRestore_VerboseDebug")
		    .detail("CommitVersion", version)
		    .detail("ParsedMutationKV", m.toString());

		ASSERT_WE_THINK(kvOps.find(version) != kvOps.end());
		kvOps[version].push_back_deep(kvOps[version].arena(), m);
		// Sampling (FASTRESTORE_SAMPLING_PERCENT%) data
		if (deterministicRandom()->random01() * 100 < SERVER_KNOBS->FASTRESTORE_SAMPLING_PERCENT) {
			sampleMutations.push_back_deep(sampleMutations.arena(), m);
		}
	}

	return Void();
}

// Parse data blocks in a log file into a vector of <string, string> pairs. Each pair.second contains the mutations at a
// version encoded in pair.first Step 1: decodeLogFileBlock into <string, string> pairs Step 2: Concatenate the
// pair.second of pairs with the same pair.first.
ACTOR static Future<Void> _parseLogFileToMutationsOnLoader(NotifiedVersion* pProcessedFileOffset,
                                                           SerializedMutationListMap* pMutationMap,
                                                           SerializedMutationPartMap* pMutationPartMap,
                                                           Reference<IBackupContainer> bc, Version version,
                                                           std::string fileName, int64_t readOffset, int64_t readLen,
                                                           KeyRange restoreRange, Key addPrefix, Key removePrefix,
                                                           Key mutationLogPrefix) {
	Reference<IAsyncFile> inFile = wait(bc->readFile(fileName));
	// decodeLogFileBlock() must read block by block!
	state Standalone<VectorRef<KeyValueRef>> data =
	    wait(parallelFileRestore::decodeLogFileBlock(inFile, readOffset, readLen));
	TraceEvent("FastRestore")
	    .detail("DecodedLogFile", fileName)
	    .detail("Offset", readOffset)
	    .detail("Length", readLen)
	    .detail("DataSize", data.contents().size());

	// Ensure data blocks in the same file are processed in order
	wait(pProcessedFileOffset->whenAtLeast(readOffset));

	if (pProcessedFileOffset->get() == readOffset) {
		int start = 0;
		int end = data.size();
		int numConcatenated = 0;
		for (int i = start; i < end; ++i) {
			// Key k = data[i].key.withPrefix(mutationLogPrefix);
			// ValueRef v = data[i].value;
			// Concatenate the backuped param1 and param2 (KV) at the same version.
			bool concatenated =
			    concatenateBackupMutationForLogFile(pMutationMap, pMutationPartMap, data[i].key, data[i].value);
			numConcatenated += (concatenated ? 1 : 0);
		}
		pProcessedFileOffset->set(readOffset + readLen);
	}

	return Void();
}
