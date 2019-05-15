/*
 * RestoreApplier.actor.cpp
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

// This file defines the functions used by the RestoreApplier role.
// RestoreApplier role starts at restoreApplierCore actor

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/BackupContainer.h"

#include "fdbserver/RestoreCommon.actor.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreApplier.actor.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

ACTOR Future<Void> handleGetApplierKeyRangeRequest(RestoreGetApplierKeyRangeRequest req, Reference<RestoreApplierData> self);
ACTOR Future<Void> handleSetApplierKeyRangeRequest(RestoreSetApplierKeyRangeRequest req, Reference<RestoreApplierData> self);
ACTOR Future<Void> handleCalculateApplierKeyRangeRequest(RestoreCalculateApplierKeyRangeRequest req, Reference<RestoreApplierData> self);
ACTOR Future<Void> handleSendSampleMutationVectorRequest(RestoreSendMutationVectorRequest req, Reference<RestoreApplierData> self);
ACTOR Future<Void> handleSendMutationVectorRequest(RestoreSendMutationVectorRequest req, Reference<RestoreApplierData> self);
ACTOR Future<Void> handleApplyToDBRequest(RestoreSimpleRequest req, Reference<RestoreApplierData> self, Database cx);


ACTOR Future<Void> restoreApplierCore(Reference<RestoreApplierData> self, RestoreApplierInterface applierInterf, Database cx) {
	state ActorCollection actors(false);
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
				when ( RestoreSimpleRequest req = waitNext(applierInterf.heartbeat.getFuture()) ) {
					requestTypeStr = "heartbeat";
					wait(handleHeartbeat(req, applierInterf.id()));
				}
				when ( RestoreGetApplierKeyRangeRequest req = waitNext(applierInterf.getApplierKeyRangeRequest.getFuture()) ) {
					requestTypeStr = "getApplierKeyRangeRequest";
					wait(handleGetApplierKeyRangeRequest(req, self));
				}
				when ( RestoreSetApplierKeyRangeRequest req = waitNext(applierInterf.setApplierKeyRangeRequest.getFuture()) ) {
					requestTypeStr = "setApplierKeyRangeRequest";
					wait(handleSetApplierKeyRangeRequest(req, self));
				}

				when ( RestoreCalculateApplierKeyRangeRequest req = waitNext(applierInterf.calculateApplierKeyRange.getFuture()) ) {
					requestTypeStr = "calculateApplierKeyRange";
					wait(handleCalculateApplierKeyRangeRequest(req, self));
				}
				when ( RestoreSendMutationVectorRequest req = waitNext(applierInterf.sendSampleMutationVector.getFuture()) ) {
					requestTypeStr = "sendSampleMutationVector";
					actors.add( handleSendSampleMutationVectorRequest(req, self));
				} 
				when ( RestoreSendMutationVectorRequest req = waitNext(applierInterf.sendMutationVector.getFuture()) ) {
					requestTypeStr = "sendMutationVector";
					actors.add( handleSendMutationVectorRequest(req, self) );
				}
				when ( RestoreSimpleRequest req = waitNext(applierInterf.applyToDB.getFuture()) ) {
					requestTypeStr = "applyToDB";
					actors.add( handleApplyToDBRequest(req, self, cx) );
				}
				when ( RestoreVersionBatchRequest req = waitNext(applierInterf.initVersionBatch.getFuture()) ) {
					requestTypeStr = "initVersionBatch";
					wait(handleInitVersionBatchRequest(req, self));
				}
				when ( RestoreSimpleRequest req = waitNext(applierInterf.finishRestore.getFuture()) ) {
					requestTypeStr = "finishRestore";
					wait( handlerFinishRestoreRequest(req, self, cx) );
					break;
				}
				when ( RestoreSimpleRequest req = waitNext(applierInterf.collectRestoreRoleInterfaces.getFuture()) ) {
					// NOTE: This must be after wait(configureRolesHandler()) because we must ensure all workers have registered their workerInterfaces into DB before we can read the workerInterface.
					// TODO: Wait until all workers have registered their workerInterface.
					wait( handleCollectRestoreRoleInterfaceRequest(req, self, cx) );
				}
			}
		} catch (Error &e) {
			fprintf(stdout, "[ERROR] Loader handle received request:%s error. error code:%d, error message:%s\n",
					requestTypeStr.c_str(), e.code(), e.what());

			if ( requestTypeStr.find("[Init]") != std::string::npos ) {
				printf("Exit due to error at requestType:%s", requestTypeStr.c_str());
				break;
			}
		}
	}

	return Void();
}

// Based on the number of sampled mutations operated in the key space, split the key space evenly to k appliers
// If the number of splitted key spaces is smaller than k, some appliers will not be used
ACTOR Future<Void> handleCalculateApplierKeyRangeRequest(RestoreCalculateApplierKeyRangeRequest req, Reference<RestoreApplierData> self) {
	state int numMutations = 0;
	state std::vector<Standalone<KeyRef>> keyRangeLowerBounds;

	while (self->isInProgress(RestoreCommandEnum::Calculate_Applier_KeyRange)) {
		printf("[DEBUG] NODE:%s Calculate_Applier_KeyRange wait for 5s\n",  self->describeNode().c_str());
		wait(delay(5.0));
	}

	wait( delay(1.0) );
	// Handle duplicate message
	// We need to recalculate the value for duplicate message! Because the reply to duplicate message may arrive earlier!
	if (self->isCmdProcessed(req.cmdID) && !keyRangeLowerBounds.empty() ) {
		printf("[DEBUG] Node:%s skip duplicate cmd:%s\n", self->describeNode().c_str(), req.cmdID.toString().c_str());
		req.reply.send(GetKeyRangeNumberReply(keyRangeLowerBounds.size()));
		return Void();
	}
	self->setInProgressFlag(RestoreCommandEnum::Calculate_Applier_KeyRange);

	// Applier will calculate applier key range
	printf("[INFO][Applier] CMD:%s, Node:%s Calculate key ranges for %d appliers\n",
			req.cmdID.toString().c_str(), self->describeNode().c_str(), req.numAppliers);

	if ( keyRangeLowerBounds.empty() ) {
		keyRangeLowerBounds = self->calculateAppliersKeyRanges(req.numAppliers); // keyRangeIndex is the number of key ranges requested
		self->keyRangeLowerBounds = keyRangeLowerBounds;
	}
	
	printf("[INFO][Applier] CMD:%s, NodeID:%s: num of key ranges:%ld\n",
			req.cmdID.toString().c_str(), self->describeNode().c_str(), keyRangeLowerBounds.size());
	req.reply.send(GetKeyRangeNumberReply(keyRangeLowerBounds.size()));
	self->processedCmd[req.cmdID] = 1; // We should not skip this command in the following phase. Otherwise, the handler in other phases may return a wrong number of appliers
	self->clearInProgressFlag(RestoreCommandEnum::Calculate_Applier_KeyRange);

	return Void();
}

// Reply with the key range for the aplier req.applierIndex.
// This actor cannot return until the applier has calculated the key ranges for appliers
ACTOR Future<Void> handleGetApplierKeyRangeRequest(RestoreGetApplierKeyRangeRequest req, Reference<RestoreApplierData> self) {
	state int numMutations = 0;
	//state std::vector<Standalone<KeyRef>> keyRangeLowerBounds = self->keyRangeLowerBounds;

	while (self->isInProgress(RestoreCommandEnum::Get_Applier_KeyRange)) {
		printf("[DEBUG] NODE:%s Calculate_Applier_KeyRange wait for 5s\n",  self->describeNode().c_str());
		wait(delay(5.0));
	}

	wait( delay(1.0) );
	//NOTE: Must reply a valid lowerBound and upperBound! Otherwise, the master will receive an invalid value!
	// if (self->isCmdProcessed(req.cmdID) ) {
	// 	printf("[DEBUG] Node:%s skip duplicate cmd:%s\n", self->describeNode().c_str(), req.cmdID.toString().c_str());
	// 	req.reply.send(GetKeyRangeReply(workerInterf.id(), req.cmdID)); // Must wait until the previous command returns
	// 	return Void();
	// }
	self->setInProgressFlag(RestoreCommandEnum::Get_Applier_KeyRange);
	
	if ( req.applierIndex < 0 || req.applierIndex >= self->keyRangeLowerBounds.size() ) {
		printf("[INFO][Applier] NodeID:%s Get_Applier_KeyRange keyRangeIndex is out of range. keyIndex:%d keyRagneSize:%ld\n",
				self->describeNode().c_str(), req.applierIndex,  self->keyRangeLowerBounds.size());
	}

	printf("[INFO][Applier] NodeID:%s replies Get_Applier_KeyRange. keyRangeIndex:%d lower_bound_of_keyRange:%s\n",
			self->describeNode().c_str(), req.applierIndex, getHexString(self->keyRangeLowerBounds[req.applierIndex]).c_str());

	KeyRef lowerBound = self->keyRangeLowerBounds[req.applierIndex];
	KeyRef upperBound = (req.applierIndex + 1) < self->keyRangeLowerBounds.size() ? self->keyRangeLowerBounds[req.applierIndex+1] : normalKeys.end;

	req.reply.send(GetKeyRangeReply(self->id(), req.cmdID, req.applierIndex, lowerBound, upperBound));
	self->clearInProgressFlag(RestoreCommandEnum::Get_Applier_KeyRange);

	return Void();

}

// Assign key range to applier req.applierID
// Idempodent operation. OK to re-execute the duplicate cmd
// The applier should remember the key range it is responsible for
ACTOR Future<Void> handleSetApplierKeyRangeRequest(RestoreSetApplierKeyRangeRequest req, Reference<RestoreApplierData> self) {
	while (self->isInProgress(RestoreCommandEnum::Assign_Applier_KeyRange)) {
		printf("[DEBUG] NODE:%s handleSetApplierKeyRangeRequest wait for 1s\n",  self->describeNode().c_str());
		wait(delay(1.0));
	}
	if ( self->isCmdProcessed(req.cmdID) ) {
		req.reply.send(RestoreCommonReply(self->id(),req.cmdID));
		return Void();
	}
	self->setInProgressFlag(RestoreCommandEnum::Assign_Applier_KeyRange);

	self->range2Applier[req.range.begin] = req.applierID;

	self->processedCmd.clear(); // The Loader_Register_Mutation_to_Applier command can be sent in both sampling and actual loading phases
	self->processedCmd[req.cmdID] = 1;
	self->clearInProgressFlag(RestoreCommandEnum::Assign_Applier_KeyRange);

	req.reply.send(RestoreCommonReply(self->id(), req.cmdID));

	return Void();
}



// Applier receive mutation from loader
ACTOR Future<Void> handleSendMutationVectorRequest(RestoreSendMutationVectorRequest req, Reference<RestoreApplierData> self) {
	state int numMutations = 0;

	if ( debug_verbose ) {
		printf("[VERBOSE_DEBUG] Node:%s receive mutation number:%d\n", self->describeNode().c_str(), req.mutations.size());
	}

	// NOTE: We have insert operation to self->kvOps. For the same worker, we should only allow one actor of this kind to run at any time!
	// Otherwise, race condition may happen!
	while (self->isInProgress(RestoreCommandEnum::Loader_Send_Mutations_To_Applier)) {
		printf("[DEBUG] NODE:%s sendMutation wait for 1s\n",  self->describeNode().c_str());
		wait(delay(1.0));
	}

	// Handle duplicat cmd
	if ( self->isCmdProcessed(req.cmdID) ) {
		printf("[DEBUG] NODE:%s skip duplicate cmd:%s\n", self->describeNode().c_str(), req.cmdID.toString().c_str());
		//printf("[DEBUG] Skipped duplicate cmd:%s\n", req.cmdID.toString().c_str());
		req.reply.send(RestoreCommonReply(self->id(), req.cmdID));	
		return Void();
	}
	self->setInProgressFlag(RestoreCommandEnum::Loader_Send_Mutations_To_Applier);

	// Applier will cache the mutations at each version. Once receive all mutations, applier will apply them to DB
	state uint64_t commitVersion = req.commitVersion;
	VectorRef<MutationRef> mutations(req.mutations);
	printf("[DEBUG] Node:%s receive %d mutations at version:%ld\n", self->describeNode().c_str(), mutations.size(), commitVersion);
	if ( self->kvOps.find(commitVersion) == self->kvOps.end() ) {
		self->kvOps.insert(std::make_pair(commitVersion, VectorRef<MutationRef>()));
	}
	state int mIndex = 0;
	for (mIndex = 0; mIndex < mutations.size(); mIndex++) {
		MutationRef mutation = mutations[mIndex];
		self->kvOps[commitVersion].push_back_deep(self->kvOps[commitVersion].arena(), mutation);
		numMutations++;
		//if ( numMutations % 100000 == 1 ) { // Should be different value in simulation and in real mode
			printf("[INFO][Applier] Node:%s Receives %d mutations. cur_mutation:%s\n",
					self->describeNode().c_str(), numMutations, mutation.toString().c_str());
		//}
	}
	
	req.reply.send(RestoreCommonReply(self->id(), req.cmdID));
	// Avoid race condition when this actor is called twice on the same command
	self->processedCmd[req.cmdID] = 1;
	self->clearInProgressFlag(RestoreCommandEnum::Loader_Send_Mutations_To_Applier);

	return Void();
}

ACTOR Future<Void> handleSendSampleMutationVectorRequest(RestoreSendMutationVectorRequest req, Reference<RestoreApplierData> self) {
	state int numMutations = 0;
	self->numSampledMutations = 0;

	// NOTE: We have insert operation to self->kvOps. For the same worker, we should only allow one actor of this kind to run at any time!
	// Otherwise, race condition may happen!
	while (self->isInProgress(RestoreCommandEnum::Loader_Send_Sample_Mutation_To_Applier)) {
		printf("[DEBUG] NODE:%s handleSendSampleMutationVectorRequest wait for 1s\n",  self->describeNode().c_str());
		wait(delay(1.0));
	}

	// Handle duplicate message
	if (self->isCmdProcessed(req.cmdID)) {
		printf("[DEBUG] NODE:%s skip duplicate cmd:%s\n", self->describeNode().c_str(), req.cmdID.toString().c_str());
		req.reply.send(RestoreCommonReply(self->id(), req.cmdID));
		return Void();
	}
	self->setInProgressFlag(RestoreCommandEnum::Loader_Send_Sample_Mutation_To_Applier);

	// Applier will cache the mutations at each version. Once receive all mutations, applier will apply them to DB
	state uint64_t commitVersion = req.commitVersion;
	// TODO: Change the req.mutation to a vector of mutations
	VectorRef<MutationRef> mutations(req.mutations);

	state int mIndex = 0;
	for (mIndex = 0; mIndex < mutations.size(); mIndex++) {
		MutationRef mutation = mutations[mIndex];
		if ( self->keyOpsCount.find(mutation.param1) == self->keyOpsCount.end() ) {
			self->keyOpsCount.insert(std::make_pair(mutation.param1, 0));
		}
		// NOTE: We may receive the same mutation more than once due to network package lost.
		// Since sampling is just an estimation and the network should be stable enough, we do NOT handle the duplication for now
		// In a very unreliable network, we may get many duplicate messages and get a bad key-range splits for appliers. But the restore should still work except for running slower.
		self->keyOpsCount[mutation.param1]++;
		self->numSampledMutations++;

		if ( debug_verbose && self->numSampledMutations % 1000 == 1 ) {
			printf("[Sampling][Applier] Node:%s Receives %d sampled mutations. cur_mutation:%s\n",
					self->describeNode().c_str(), self->numSampledMutations, mutation.toString().c_str());
		}
	}
	
	req.reply.send(RestoreCommonReply(self->id(), req.cmdID));
	self->processedCmd[req.cmdID] = 1;

	self->clearInProgressFlag(RestoreCommandEnum::Loader_Send_Sample_Mutation_To_Applier);

	return Void();
}

 ACTOR Future<Void> handleApplyToDBRequest(RestoreSimpleRequest req, Reference<RestoreApplierData> self, Database cx) {
 	state bool isPrint = false; //Debug message
 	state std::string typeStr = "";

	// Wait in case the  applyToDB request was delivered twice;
	while (self->inProgressApplyToDB) {
		printf("[DEBUG] NODE:%s inProgressApplyToDB wait for 5s\n",  self->describeNode().c_str());
		wait(delay(5.0));
	}
	
	if ( self->isCmdProcessed(req.cmdID) ) {
		printf("[DEBUG] NODE:%s skip duplicate cmd:%s\n", self->describeNode().c_str(), req.cmdID.toString().c_str());
		req.reply.send(RestoreCommonReply(self->id(), req.cmdID));
		return Void();
	}

	self->inProgressApplyToDB = true;

	// Assume the process will not crash when it apply mutations to DB. The reply message can be lost though
	if (self->kvOps.empty()) {
		printf("Node:%s kvOps is empty. No-op for apply to DB\n", self->describeNode().c_str());
		req.reply.send(RestoreCommonReply(self->id(), req.cmdID));
		self->processedCmd[req.cmdID] = 1;
		self->inProgressApplyToDB = false;
		return Void();
	}
	
	self->sanityCheckMutationOps();

 	if ( debug_verbose ) {
		TraceEvent("ApplyKVOPsToDB").detail("MapSize", self->kvOps.size());
		printf("ApplyKVOPsToDB num_of_version:%ld\n", self->kvOps.size());
 	}
 	state std::map<Version, Standalone<VectorRef<MutationRef>>>::iterator it = self->kvOps.begin();
	state std::map<Version, Standalone<VectorRef<MutationRef>>>::iterator prevIt = it;
	state int index = 0;
	state int prevIndex = index;
 	state int count = 0;
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state int numVersion = 0;
	state double transactionSize = 0;
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			transactionSize = 0;

			for ( ; it != self->kvOps.end(); ++it ) {
				numVersion++;
				if ( debug_verbose ) {
					TraceEvent("ApplyKVOPsToDB\t").detail("Version", it->first).detail("OpNum", it->second.size());
				}
				//printf("ApplyKVOPsToDB numVersion:%d Version:%08lx num_of_ops:%d, \n", numVersion, it->first, it->second.size());

				state MutationRef m;
				for ( ; index < it->second.size(); ++index ) {
					m = it->second[index];
					if (  m.type >= MutationRef::Type::SetValue && m.type <= MutationRef::Type::MAX_ATOMIC_OP )
						typeStr = typeString[m.type];
					else {
						printf("ApplyKVOPsToDB MutationType:%d is out of range\n", m.type);
					}

					if ( debug_verbose && count % 1000 == 0 ) {
						printf("ApplyKVOPsToDB Node:%s num_mutation:%d Version:%08lx num_of_ops to apply:%d\n",
								self->describeNode().c_str(), count, it->first, it->second.size());
					}

					if ( debug_verbose ) {
						printf("[VERBOSE_DEBUG] Node:%s apply mutation:%s\n", self->describeNode().c_str(), m.toString().c_str());
					}

					if ( m.type == MutationRef::SetValue ) {
						tr->set(m.param1, m.param2);
					} else if ( m.type == MutationRef::ClearRange ) {
						KeyRangeRef mutationRange(m.param1, m.param2);
						tr->clear(mutationRange);
					} else if ( isAtomicOp((MutationRef::Type) m.type) ) {
						//// Now handle atomic operation from this if statement
						// TODO: Have not de-duplicated the mutations for multiple network delivery
						// ATOMIC_MASK = (1 << AddValue) | (1 << And) | (1 << Or) | (1 << Xor) | (1 << AppendIfFits) | (1 << Max) | (1 << Min) | (1 << SetVersionstampedKey) | (1 << SetVersionstampedValue) | (1 << ByteMin) | (1 << ByteMax) | (1 << MinV2) | (1 << AndV2),
						//atomicOp( const KeyRef& key, const ValueRef& operand, uint32_t operationType )
						tr->atomicOp(m.param1, m.param2, m.type);
					} else {
						printf("[WARNING] mtype:%d (%s) unhandled\n", m.type, typeStr.c_str());
					}
					++count;
					transactionSize += m.expectedSize();
					
					if ( transactionSize >= transactionBatchSizeThreshold ) { // commit per 1000 mutations
						wait(tr->commit());
						tr->reset();
						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);
						prevIt = it;
						prevIndex = index;
						transactionSize = 0;
					}

					if ( isPrint ) {
						printf("\tApplyKVOPsToDB Version:%016lx MType:%s K:%s, V:%s K_size:%d V_size:%d\n", it->first, typeStr.c_str(),
							getHexString(m.param1).c_str(), getHexString(m.param2).c_str(), m.param1.size(), m.param2.size());

						TraceEvent("ApplyKVOPsToDB\t\t").detail("Version", it->first)
								.detail("MType", m.type).detail("MTypeStr", typeStr)
								.detail("MKey", getHexString(m.param1))
								.detail("MValueSize", m.param2.size())
								.detail("MValue", getHexString(m.param2));
					}
				}

				if ( transactionSize > 0 ) { // the commit batch should NOT across versions
					wait(tr->commit());
					tr->reset();
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					prevIt = it;
					prevIndex = index;
					transactionSize = 0;
				}
				index = 0;
			}
			// Last transaction
			if (transactionSize > 0) {
				wait(tr->commit());
			}
			break;
		} catch(Error &e) {
			printf("ApplyKVOPsToDB transaction error:%s.\n", e.what());
			wait(tr->onError(e));
			it = prevIt;
			index = prevIndex;
			transactionSize = 0;
		}
	}

 	self->kvOps.clear();
 	printf("Node:%s ApplyKVOPsToDB number of kv mutations:%d\n", self->describeNode().c_str(), count);

	req.reply.send(RestoreCommonReply(self->id(), req.cmdID));
	printf("self->processedCmd size:%d req.cmdID:%s\n", self->processedCmd.size(), req.cmdID.toString().c_str());
	self->processedCmd[req.cmdID] = 1;
	self->inProgressApplyToDB = false;

 	return Void();
}



