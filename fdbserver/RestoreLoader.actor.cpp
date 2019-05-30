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

#include "flow/actorcompiler.h"  // This must be the last #include.

typedef std::map<Version, Standalone<VectorRef<MutationRef>>> VersionedMutationsMap;

ACTOR Future<Void> handleSetApplierKeyRangeVectorRequest(RestoreSetApplierKeyRangeVectorRequest req, Reference<RestoreLoaderData> self);
ACTOR Future<Void> handleLoadFileRequest(RestoreLoadFileRequest req, Reference<RestoreLoaderData> self, bool isSampling = false);
ACTOR static Future<Void> _parseLogFileToMutationsOnLoader(std::map<Standalone<StringRef>, Standalone<StringRef>> *mutationMap,
									std::map<Standalone<StringRef>, uint32_t> *mutationPartMap,
 									Reference<IBackupContainer> bc, Version version,
 									std::string fileName, int64_t readOffset, int64_t readLen,
 									KeyRange restoreRange, Key addPrefix, Key removePrefix,
 									Key mutationLogPrefix);									 
ACTOR static Future<Void> _parseRangeFileToMutationsOnLoader(std::map<Version, Standalone<VectorRef<MutationRef>>> *kvOps,
 									Reference<IBackupContainer> bc, Version version,
									std::string fileName, int64_t readOffset_input, int64_t readLen_input,KeyRange restoreRange);	
ACTOR Future<Void> registerMutationsToApplier(Reference<RestoreLoaderData> self,
									std::map<Version, Standalone<VectorRef<MutationRef>>> *kvOps,
									bool isRangeFile, Version startVersion, Version endVersion);
 void _parseSerializedMutation(std::map<Version, Standalone<VectorRef<MutationRef>>> *kvOps,
	 						 std::map<Standalone<StringRef>, Standalone<StringRef>> *mutationMap,
							 bool isSampling = false);
bool isRangeMutation(MutationRef m);
void splitMutation(Reference<RestoreLoaderData> self,  MutationRef m, Arena& mvector_arena, VectorRef<MutationRef>& mvector, Arena& nodeIDs_arena, VectorRef<UID>& nodeIDs) ;


ACTOR Future<Void> restoreLoaderCore(Reference<RestoreLoaderData> self, RestoreLoaderInterface loaderInterf, Database cx) {
	state ActorCollection actors(false);
	state Future<Void> exitRole = Never();
	state double lastLoopTopTime;
	loop {
		
		double loopTopTime = now();
		double elapsedTime = loopTopTime - lastLoopTopTime;
		if( elapsedTime > 0.050 ) {
			if (g_random->random01() < 0.01)
				TraceEvent(SevWarn, "SlowRestoreLoaderLoopx100").detail("NodeDesc", self->describeNode()).detail("Elapsed", elapsedTime);
		}
		lastLoopTopTime = loopTopTime;
		state std::string requestTypeStr = "[Init]";

		try {
			choose {
				when ( RestoreSimpleRequest req = waitNext(loaderInterf.heartbeat.getFuture()) ) {
					requestTypeStr = "heartbeat";
					actors.add(handleHeartbeat(req, loaderInterf.id()));
				}
				when ( RestoreSetApplierKeyRangeVectorRequest req = waitNext(loaderInterf.setApplierKeyRangeVectorRequest.getFuture()) ) {
					requestTypeStr = "setApplierKeyRangeVectorRequest";
					actors.add(handleSetApplierKeyRangeVectorRequest(req, self));
				}
				when ( RestoreLoadFileRequest req = waitNext(loaderInterf.loadFile.getFuture()) ) {
					requestTypeStr = "loadFile";
					self->initBackupContainer(req.param.url);
					actors.add( handleLoadFileRequest(req, self, false) );
				}
				when ( RestoreVersionBatchRequest req = waitNext(loaderInterf.initVersionBatch.getFuture()) ) {
					requestTypeStr = "initVersionBatch";
					actors.add( handleInitVersionBatchRequest(req, self) );
				}
				when ( RestoreSimpleRequest req = waitNext(loaderInterf.finishRestore.getFuture()) ) {
					requestTypeStr = "finishRestore";
					exitRole = handlerFinishRestoreRequest(req, self, cx);
				}
				when ( wait(exitRole) ) {
					TraceEvent("FastRestore").detail("RestoreApplierCore", "ExitRole");
					break;
				}
			}
		} catch (Error &e) {
            fprintf(stdout, "[ERROR] Restore Loader handle received request:%s error. error code:%d, error message:%s\n",
                    requestTypeStr.c_str(), e.code(), e.what());

			if ( requestTypeStr.find("[Init]") != std::string::npos ) {
				printf("Exit due to error at requestType:%s", requestTypeStr.c_str());
				break;
			}
		}
	}
	TraceEvent("FastRestore").detail("RestoreApplierCore", "Exit");
	return Void();
}

// Restore Loader
ACTOR Future<Void> handleSetApplierKeyRangeVectorRequest(RestoreSetApplierKeyRangeVectorRequest req, Reference<RestoreLoaderData> self) {
	// Idempodent operation. OK to re-execute the duplicate cmd
	if ( self->range2Applier.empty() ) {
		self->range2Applier = req.range2Applier;
	}
	
	req.reply.send(RestoreCommonReply(self->id()));

	return Void();
}

ACTOR Future<Void> _processLoadingParam(LoadingParam param, Reference<RestoreLoaderData> self) {
	// Temporary data structure for parsing range and log files into (version, <K, V, mutationType>)
	state std::map<Version, Standalone<VectorRef<MutationRef>>> kvOps;
	// Must use StandAlone to save mutations, otherwise, the mutationref memory will be corrupted
	state std::map<Standalone<StringRef>, Standalone<StringRef>> mutationMap; // Key is the unique identifier for a batch of mutation logs at the same version
	state std::map<Standalone<StringRef>, uint32_t> mutationPartMap; // Sanity check the data parsing is correct

	// Q: How to record the  param's fields. Refer to storageMetrics
	//TraceEvent("FastRestore").detail("LoaderID", self->id()).detail("LoadingParam", param.);
	printf("[INFO][Loader] Node:%s, Execute: handleLoadFileRequest, loading param:%s\n",
			self->describeNode().c_str(), param.toString().c_str());

	ASSERT( param.blockSize > 0 );
	//state std::vector<Future<Void>> fileParserFutures;
	if (param.offset % param.blockSize != 0) {
		fprintf(stderr, "[WARNING] Parse file not at block boundary! param.offset:%ld param.blocksize:%ld, remainder:%ld\n",
				param.offset, param.blockSize, param.offset % param.blockSize);
	}
	state int64_t j;
	state int64_t readOffset;
	state int64_t readLen;
	for (j = param.offset; j < param.length; j += param.blockSize) {
		readOffset = j;
		readLen = std::min<int64_t>(param.blockSize, param.length - j);
		printf("[DEBUG_TMP] _parseRangeFileToMutationsOnLoader starts\n");
		if ( param.isRangeFile ) {
			wait( _parseRangeFileToMutationsOnLoader(&kvOps, self->bc, param.version, param.filename, readOffset, readLen, param.restoreRange) );
		} else {
			wait( _parseLogFileToMutationsOnLoader(&mutationMap, &mutationPartMap, self->bc, param.version, param.filename, readOffset, readLen, param.restoreRange, param.addPrefix, param.removePrefix, param.mutationLogPrefix) );
		}
		printf("[DEBUG_TMP] _parseRangeFileToMutationsOnLoader ends\n");
	}

	printf("[INFO][Loader] Finishes process Range file:%s\n", param.filename.c_str());
	
	if ( !param.isRangeFile ) {
		_parseSerializedMutation(&kvOps, &mutationMap);
	}
	
	wait( registerMutationsToApplier(self, &kvOps, true, param.prevVersion, param.endVersion) ); // Send the parsed mutation to applier who will apply the mutation to DB
	
	return Void();
}

ACTOR Future<Void> handleLoadFileRequest(RestoreLoadFileRequest req, Reference<RestoreLoaderData> self, bool isSampling) {
	try {
		if (self->processedFileParams.find(req.param) ==  self->processedFileParams.end()) {
			// Deduplicate the same requests
			printf("self->processedFileParams.size:%d Process param:%s\n", self->processedFileParams.size(), req.param.toString().c_str());
			self->processedFileParams[req.param] = Never();
			self->processedFileParams[req.param] = _processLoadingParam(req.param,  self);
			printf("processedFileParam.size:%d\n", self->processedFileParams.size());
			printf("processedFileParam[req.param].ready:%d\n", self->processedFileParams[req.param].isReady());
			ASSERT(self->processedFileParams.find(req.param) !=  self->processedFileParams.end());
			wait(self->processedFileParams[req.param]);
		} else {
			ASSERT(self->processedFileParams.find(req.param) !=  self->processedFileParams.end());
			printf("Process param that is being processed:%s\n", req.param.toString().c_str());
			wait(self->processedFileParams[req.param]);	
		}
	} catch (Error &e) {
		fprintf(stdout, "[ERROR] handleLoadFileRequest Node:%s, error. error code:%d, error message:%s\n", self->describeNode().c_str(),
				 e.code(), e.what());
	}

	req.reply.send(RestoreCommonReply(self->id()));
	return Void();
}

ACTOR Future<Void> registerMutationsToApplier(Reference<RestoreLoaderData> self,
									VersionedMutationsMap *pkvOps,
									bool isRangeFile, Version startVersion, Version endVersion) {
    state VersionedMutationsMap &kvOps = *pkvOps;
	printf("[INFO][Loader] Node:%s self->masterApplierInterf:%s, registerMutationsToApplier\n",
			self->describeNode().c_str(), self->masterApplierInterf.toString().c_str());

	state int packMutationNum = 0;
	state int packMutationThreshold = 10;
	state int kvCount = 0;
	state std::vector<Future<RestoreCommonReply>> cmdReplies;

	state int splitMutationIndex = 0;

	// Ensure there is a mutation request sent at endVersion, so that applier can advance its notifiedVersion
	if ( kvOps.find(endVersion)  == kvOps.end() ) {
		kvOps[endVersion] = VectorRef<MutationRef>();
	}

	self->printAppliersKeyRange();

	//state double mutationVectorThreshold = 1;//1024 * 10; // Bytes.
	state std::map<UID, Standalone<VectorRef<MutationRef>>> applierMutationsBuffer; // The mutation vector to be sent to each applier
	state std::map<UID, double> applierMutationsSize; // buffered mutation vector size for each applier
	state Standalone<VectorRef<MutationRef>> mvector;
	state Standalone<VectorRef<UID>> nodeIDs;
	// Initialize the above two maps
	state std::vector<UID> applierIDs = self->getWorkingApplierIDs();
	state std::vector<std::pair<UID, RestoreSendMutationVectorVersionedRequest>> requests;
	state Version prevVersion = startVersion;
	loop {
		try {
			packMutationNum = 0;
			splitMutationIndex = 0;
			kvCount = 0;
			state std::map<Version, Standalone<VectorRef<MutationRef>>>::iterator kvOp;
			
			for ( kvOp = kvOps.begin(); kvOp != kvOps.end(); kvOp++) {
				// In case try-catch has error and loop back
				applierMutationsBuffer.clear();
				applierMutationsSize.clear();
				for (auto &applierID : applierIDs) {
					applierMutationsBuffer[applierID] = Standalone<VectorRef<MutationRef>>(VectorRef<MutationRef>());
					applierMutationsSize[applierID] = 0.0;
				}
				state Version commitVersion = kvOp->first;
				state int mIndex;
				state MutationRef kvm;
				for (mIndex = 0; mIndex < kvOp->second.size(); mIndex++) {
					kvm = kvOp->second[mIndex];
					if ( debug_verbose ) {
						printf("[VERBOSE_DEBUG] mutation to sent to applier, mutation:%s\n", kvm.toString().c_str());
					}
					// Send the mutation to applier
					if ( isRangeMutation(kvm) ) { // MX: Use false to skip the range mutation handling
						// Because using a vector of mutations causes overhead, and the range mutation should happen rarely;
						// We handle the range mutation and key mutation differently for the benefit of avoiding memory copy
						mvector.pop_front(mvector.size());
						nodeIDs.pop_front(nodeIDs.size());
						//state std::map<Standalone<MutationRef>, UID> m2appliers;
						// '' Bug may be here! The splitMutation() may be wrong!
						splitMutation(self, kvm, mvector.arena(), mvector.contents(), nodeIDs.arena(), nodeIDs.contents());
						// m2appliers = splitMutationv2(self, kvm);
						// // convert m2appliers to mvector and nodeIDs
						// for (auto& m2applier : m2appliers) {
						// 	mvector.push_back(m2applier.first);
						// 	nodeIDs.push_back(m2applier.second);
						// }
					
						printf("SPLITMUTATION: mvector.size:%d\n", mvector.size());
						ASSERT(mvector.size() == nodeIDs.size());

						for (splitMutationIndex = 0; splitMutationIndex < mvector.size(); splitMutationIndex++ ) {
							MutationRef mutation = mvector[splitMutationIndex];
							UID applierID = nodeIDs[splitMutationIndex];
							printf("SPLITTED MUTATION: %d: mutation:%s applierID:%s\n", splitMutationIndex, mutation.toString().c_str(), applierID.toString().c_str());
							applierMutationsBuffer[applierID].push_back_deep(applierMutationsBuffer[applierID].arena(), mutation); // Q: Maybe push_back_deep()?
							applierMutationsSize[applierID] += mutation.expectedSize();

							kvCount++;
						}
					} else { // mutation operates on a particular key
						std::map<Standalone<KeyRef>, UID>::iterator itlow = self->range2Applier.lower_bound(kvm.param1); // lower_bound returns the iterator that is >= m.param1
						// make sure itlow->first <= m.param1
						if ( itlow == self->range2Applier.end() || itlow->first > kvm.param1 ) {
							if ( itlow == self->range2Applier.begin() ) {
								printf("KV-Applier: SHOULD NOT HAPPEN. kvm.param1:%s\n", kvm.param1.toString().c_str());
							}
							--itlow;
						}
						ASSERT( itlow->first <= kvm.param1 );
						MutationRef mutation = kvm;
						UID applierID = itlow->second;
						printf("KV--Applier: K:%s ApplierID:%s\n", kvm.param1.toString().c_str(), applierID.toString().c_str());
						kvCount++;

						applierMutationsBuffer[applierID].push_back_deep(applierMutationsBuffer[applierID].arena(), mutation); // Q: Maybe push_back_deep()?
						applierMutationsSize[applierID] += mutation.expectedSize();
					}
				} // Mutations at the same version

				// In case the mutation vector is not larger than mutationVectorThreshold
				// We must send out the leftover mutations any way; otherwise, the mutations at different versions will be mixed together
				printf("[DEBUG][Loader] sendMutationVector send mutations at Version:%ld to appliers, applierIDs.size:%d\n", commitVersion, applierIDs.size());
				for (auto &applierID : applierIDs) {
					printf("[DEBUG][Loader] sendMutationVector size:%d for applierID:%s\n", applierMutationsBuffer[applierID].size(), applierID.toString().c_str());
					requests.push_back( std::make_pair(applierID, RestoreSendMutationVectorVersionedRequest(prevVersion, commitVersion, isRangeFile, applierMutationsBuffer[applierID])) );
					applierMutationsBuffer[applierID].pop_front(applierMutationsBuffer[applierID].size());
					applierMutationsSize[applierID] = 0;
					//std::vector<RestoreCommonReply> reps = wait( timeoutError( getAll(cmdReplies), FastRestore_Failure_Timeout ) ); // Q: We need to wait for each reply, otherwise, correctness has error. Why?
					//cmdReplies.clear();
				}
				wait( sendBatchRequests(&RestoreApplierInterface::sendMutationVector, self->appliersInterf, requests) );
				requests.clear();
				ASSERT( prevVersion < commitVersion );
				prevVersion = commitVersion;
			} // all versions of mutations

			printf("[Summary][Loader] Node:%s produces %d mutation operations\n",
					self->describeNode().c_str(), kvCount);

			//kvOps.clear();
			break;

		} catch (Error &e) {
			fprintf(stdout, "[ERROR] registerMutationsToApplier Node:%s, error. error code:%d, error message:%s\n", self->describeNode().c_str(),
					e.code(), e.what());
		}
	};

	return Void();
}


// TODO: Add a unit test for this function
void splitMutation(Reference<RestoreLoaderData> self,  MutationRef m, Arena& mvector_arena, VectorRef<MutationRef>& mvector, Arena& nodeIDs_arena, VectorRef<UID>& nodeIDs) {
	// mvector[i] should be mapped to nodeID[i]
	ASSERT(mvector.empty());
	ASSERT(nodeIDs.empty());
	// key range [m->param1, m->param2)
	//std::map<Standalone<KeyRef>, UID>;
	printf("SPLITMUTATION: orignal mutation:%s\n", m.toString().c_str());
	std::map<Standalone<KeyRef>, UID>::iterator itlow, itup; //we will return [itlow, itup)
	itlow = self->range2Applier.lower_bound(m.param1); // lower_bound returns the iterator that is >= m.param1
	if ( itlow->first > m.param1 ) {
		if ( itlow != self->range2Applier.begin() ) {
			--itlow;
		}
	}

	// if ( itlow != self->range2Applier.begin() && itlow->first > m.param1 ) { // m.param1 is not the smallest key \00
	// 	// (itlow-1) is the node whose key range includes m.param1
	// 	--itlow;
	// } else {
	// 	if ( m.param1 != LiteralStringRef("\00") || itlow->first != m.param1 ) { // MX: This is useless
	// 		printf("[ERROR] splitMutation has bug on range mutation:%s\n", m.toString().c_str());
	// 	}
	// }

	itup = self->range2Applier.upper_bound(m.param2); // upper_bound returns the iterator that is > m.param2; return rmap::end if no keys are considered to go after m.param2.
	printf("SPLITMUTATION: itlow_key:%s itup_key:%s\n", itlow->first.toString().c_str(), itup == self->range2Applier.end() ? "[end]" : itup->first.toString().c_str());
	ASSERT( itup == self->range2Applier.end() || itup->first >= m.param2 );
	// Now adjust for the case: example: mutation range is [a, d); we have applier's ranges' inclusive lower bound values are: a, b, c, d, e; upper_bound(d) returns itup to e, but we want itup to d.
	//--itup;
	//ASSERT( itup->first <= m.param2 );
	// if ( itup->first < m.param2 ) {
	// 	++itup; //make sure itup is >= m.param2, that is, itup is the next key range >= m.param2
	// }

	std::map<Standalone<KeyRef>, UID>::iterator itApplier;
	while (itlow != itup) {
		Standalone<MutationRef> curm; //current mutation
		curm.type = m.type;
		// the first split mutation should starts with m.first. The later onces should start with the range2Applier boundary
		if ( m.param1 > itlow->first ) {
			curm.param1 = m.param1;
		} else {
			curm.param1 = itlow->first;
		}
		itApplier = itlow;
		//curm.param1 = ((m.param1 > itlow->first) ? m.param1 : itlow->first); 
		itlow++;
		if (itlow == itup) {
			ASSERT( m.param2 <= normalKeys.end );
			curm.param2 = m.param2;
		} else if ( m.param2 < itlow->first ) {
			curm.param2 = m.param2;
		} else {
			curm.param2 = itlow->first;
		}
		printf("SPLITMUTATION: mvector.push_back:%s\n", curm.toString().c_str());
		ASSERT( curm.param1 <= curm.param2 );
		mvector.push_back_deep(mvector_arena, curm);
		nodeIDs.push_back(nodeIDs_arena, itApplier->second);
	}

	printf("SPLITMUTATION: mvector.size:%d\n", mvector.size());

	return;
}


// key_input format: [logRangeMutation.first][hash_value_of_commit_version:1B][bigEndian64(commitVersion)][bigEndian32(part)]
// value_input: serialized binary of mutations at the same version
bool concatenateBackupMutationForLogFile(std::map<Standalone<StringRef>, Standalone<StringRef>> *pMutationMap,
									std::map<Standalone<StringRef>, uint32_t> *pMutationPartMap,
									Standalone<StringRef> key_input, Standalone<StringRef> val_input) {
    std::map<Standalone<StringRef>, Standalone<StringRef>> &mutationMap = *pMutationMap;
	std::map<Standalone<StringRef>, uint32_t> &mutationPartMap = *pMutationPartMap;
	std::string prefix = "||\t";
	std::stringstream ss;
	StringRef val = val_input.contents();


	StringRefReaderMX reader(val, restore_corrupted_data());
	StringRefReaderMX readerKey(key_input, restore_corrupted_data()); //read key_input!
	int logRangeMutationFirstLength = key_input.size() - 1 - 8 - 4;
	bool concatenated = false;

	ASSERT_WE_THINK( key_input.size() >= 1 + 8 + 4 );

	if ( logRangeMutationFirstLength > 0 ) {
		printf("readerKey consumes %dB\n", logRangeMutationFirstLength);
		readerKey.consume(logRangeMutationFirstLength); // Strip out the [logRangeMutation.first]; otherwise, the following readerKey.consume will produce wrong value
	}

	uint8_t hashValue = readerKey.consume<uint8_t>();
	uint64_t commitVersion = readerKey.consumeNetworkUInt64(); // Convert big Endian value encoded in log file into a littleEndian uint64_t value, i.e., commitVersion
	uint32_t part = readerKey.consumeNetworkUInt32(); //Consume big Endian value encoded in log file
	//Use commitVersion as id
	Standalone<StringRef> id = StringRef((uint8_t*) &commitVersion, 8);

	if ( mutationMap.find(id) == mutationMap.end() ) {
		mutationMap.insert(std::make_pair(id, val_input));
		if ( part != 0 ) {
			fprintf(stderr, "[ERROR]!!! part:%d != 0 for key_input:%s\n", part, getHexString(key_input).c_str());
		}
		mutationPartMap.insert(std::make_pair(id, part));
	} else { // concatenate the val string with the same commitVersion
		mutationMap[id] = mutationMap[id].contents().withSuffix(val_input.contents()); //Assign the new Areana to the map's value
		if ( part != (mutationPartMap[id] + 1) ) {
			// Check if the same range or log file has been processed more than once!
			fprintf(stderr, "[ERROR]!!! current part id:%d new part_direct:%d is not the next integer of key_input:%s\n", mutationPartMap[id], part, getHexString(key_input).c_str());
			printf("[HINT] Check if the same range or log file has been processed more than once!\n");
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
		ASSERT( m.type == MutationRef::Type::SetValue || isAtomicOp((MutationRef::Type) m.type) );
		return false;
	}
}


 // Parse the kv pair (version, serialized_mutation), which are the results parsed from log file, into (version, <K, V, mutationType>) pair
 // Put the parsed versioned mutations into *pkvOps
 // Input key: [commitVersion_of_the_mutation_batch:uint64_t]
 // Input value: [includeVersion:uint64_t][val_length:uint32_t][encoded_list_of_mutations], where
 // includeVersion is the serialized version in the batch commit. It is not the commitVersion in Input key.
 // val_length is always equal to (val.size() - 12); otherwise, we may not get the entire mutation list for the version
 // encoded_list_of_mutations: [mutation1][mutation2]...[mutationk], where
 //	a mutation is encoded as [type:uint32_t][keyLength:uint32_t][valueLength:uint32_t][keyContent][valueContent]
 void _parseSerializedMutation(VersionedMutationsMap *pkvOps,
	 						 std::map<Standalone<StringRef>, Standalone<StringRef>> *pmutationMap,
							 bool isSampling) {
	VersionedMutationsMap &kvOps = *pkvOps;
	std::map<Standalone<StringRef>, Standalone<StringRef>> &mutationMap = *pmutationMap;

	for ( auto& m : mutationMap ) {
		StringRef k = m.first.contents();
		StringRef val = m.second.contents();

		StringRefReaderMX kReader(k, restore_corrupted_data());
		uint64_t commitVersion = kReader.consume<uint64_t>(); // Consume little Endian data
		kvOps.insert(std::make_pair(commitVersion, VectorRef<MutationRef>()));

		StringRefReaderMX vReader(val, restore_corrupted_data());
		vReader.consume<uint64_t>(); // Consume the includeVersion
		uint32_t val_length_decoded = vReader.consume<uint32_t>(); // Parse little endian value, confirmed it is correct!
		ASSERT( val_length_decoded == val.size() - 12 ); // 12 is the length of [includeVersion:uint64_t][val_length:uint32_t]

		while (1) {
			// stop when reach the end of the string
			if(vReader.eof() ) { //|| *reader.rptr == 0xFF
				break;
			}

			uint32_t type = vReader.consume<uint32_t>();
			uint32_t kLen = vReader.consume<uint32_t>();
			uint32_t vLen = vReader.consume<uint32_t>();
			const uint8_t *k = vReader.consume(kLen);
			const uint8_t *v = vReader.consume(vLen);

			MutationRef mutation((MutationRef::Type) type, KeyRef(k, kLen), KeyRef(v, vLen));
			kvOps[commitVersion].push_back_deep(kvOps[commitVersion].arena(), mutation);
			ASSERT_WE_THINK( kLen >= 0 && kLen < val.size() );
			ASSERT_WE_THINK( vLen >= 0 && vLen < val.size() );
		}
	}
}

// Parsing the data blocks in a range file
ACTOR static Future<Void> _parseRangeFileToMutationsOnLoader(VersionedMutationsMap *pkvOps,
 									Reference<IBackupContainer> bc, Version version,
 									std::string fileName, int64_t readOffset, int64_t readLen,
 									KeyRange restoreRange) {
    state VersionedMutationsMap &kvOps = *pkvOps;

 	// The set of key value version is rangeFile.version. the key-value set in the same range file has the same version
 	Reference<IAsyncFile> inFile = wait(bc->readFile(fileName));
 	state Standalone<VectorRef<KeyValueRef>> blockData = wait(parallelFileRestore::decodeRangeFileBlock(inFile, readOffset, readLen));

 	// First and last key are the range for this file
 	state KeyRange fileRange = KeyRangeRef(blockData.front().key, blockData.back().key);

 	// If fileRange doesn't intersect restore range then we're done.
 	if(!fileRange.intersects(restoreRange)) {
 		return Void();
 	}

 	// We know the file range intersects the restore range but there could still be keys outside the restore range.
 	// Find the subvector of kv pairs that intersect the restore range. 
	// Note that the first and last keys are just the range endpoints for this file. They are metadata, not the real data
 	int rangeStart = 1; 
 	int rangeEnd = blockData.size() -1; // The rangeStart and rangeEnd is [,)

 	// Slide start from begining, stop if something in range is found
	// Move rangeStart and rangeEnd until they is within restoreRange
 	while(rangeStart < rangeEnd && !restoreRange.contains(blockData[rangeStart].key)) {
		++rangeStart;
	}
 	// Side end backwaself, stop if something at (rangeEnd-1) is found in range
 	while(rangeEnd > rangeStart && !restoreRange.contains(blockData[rangeEnd - 1].key)) {
		--rangeEnd;
	}

 	// Now data only contains the kv mutation within restoreRange
 	state VectorRef<KeyValueRef> data = blockData.slice(rangeStart, rangeEnd);
 	printf("[INFO] RangeFile:%s blockData entry size:%d recovered data size:%d\n", fileName.c_str(), blockData.size(), data.size()); // TO_DELETE

 	state int start = 0;
 	state int end = data.size();
 	state int kvCount = 0;

	// Convert KV in data into mutations in kvOps
	for(int i = start; i < end; ++i) {
		// NOTE: The KV pairs in range files are the real KV pairs in original DB. 
		// Should NOT removePrefix and addPrefix for the backup data!
		// In other words, the following operation is wrong:  data[i].key.removePrefix(removePrefix).withPrefix(addPrefix)
		MutationRef m(MutationRef::Type::SetValue, data[i].key, data[i].value); //ASSUME: all operation in range file is set.
		++kvCount;

		// We cache all kv operations into kvOps, and apply all kv operations later in one place
		kvOps.insert(std::make_pair(version, VectorRef<MutationRef>()));

		ASSERT_WE_THINK(kvOps.find(version) != kvOps.end());
		kvOps[version].push_back_deep(kvOps[version].arena(), m);
	}

	return Void();
 }

 // Parse data blocks in a log file into a vector of <string, string> pairs. Each pair.second contains the mutations at a version encoded in pair.first
 // Step 1: decodeLogFileBlock into <string, string> pairs
 // Step 2: Concatenate the pair.second of pairs with the same pair.first.
 ACTOR static Future<Void> _parseLogFileToMutationsOnLoader(std::map<Standalone<StringRef>, Standalone<StringRef>> *pMutationMap,
									std::map<Standalone<StringRef>, uint32_t> *pMutationPartMap,
 									Reference<IBackupContainer> bc, Version version,
 									std::string fileName, int64_t readOffset, int64_t readLen,
 									KeyRange restoreRange, Key addPrefix, Key removePrefix,
 									Key mutationLogPrefix) {

	
 	state Reference<IAsyncFile> inFile = wait(bc->readFile(fileName));

 	printf("Parse log file:%s readOffset:%d readLen:%ld\n", fileName.c_str(), readOffset, readLen);
 	// decodeLogFileBlock() must read block by block!
 	state Standalone<VectorRef<KeyValueRef>> data = wait(parallelFileRestore::decodeLogFileBlock(inFile, readOffset, readLen));
 	TraceEvent("FastRestore").detail("DecodedLogFileName", fileName).detail("DataSize", data.contents().size());

 	state int start = 0;
 	state int end = data.size();
	state int numConcatenated = 0;
	for(int i = start; i < end; ++i) {
		Key k = data[i].key.withPrefix(mutationLogPrefix);
		ValueRef v = data[i].value;
		// Concatenate the backuped param1 and param2 (KV) at the same version.
		bool concatenated = concatenateBackupMutationForLogFile(pMutationMap, pMutationPartMap, data[i].key, data[i].value);
		numConcatenated += ( concatenated ? 1 : 0);
	}

	return Void();
 }
