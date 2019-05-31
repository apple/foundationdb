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

ACTOR Future<Void> handleSendMutationVectorRequest(RestoreSendMutationVectorVersionedRequest req, Reference<RestoreApplierData> self);
ACTOR Future<Void> handleApplyToDBRequest(RestoreSimpleRequest req, Reference<RestoreApplierData> self, Database cx);

ACTOR Future<Void> restoreApplierCore(Reference<RestoreApplierData> self, RestoreApplierInterface applierInterf, Database cx) {
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
				when ( RestoreSimpleRequest req = waitNext(applierInterf.heartbeat.getFuture()) ) {
					requestTypeStr = "heartbeat";
					actors.add(handleHeartbeat(req, applierInterf.id()));
				}
				when ( RestoreSendMutationVectorVersionedRequest req = waitNext(applierInterf.sendMutationVector.getFuture()) ) {
					requestTypeStr = "sendMutationVector";
					actors.add( handleSendMutationVectorRequest(req, self) );
				}
				when ( RestoreSimpleRequest req = waitNext(applierInterf.applyToDB.getFuture()) ) {
					requestTypeStr = "applyToDB";
					actors.add( handleApplyToDBRequest(req, self, cx) );
				}
				when ( RestoreVersionBatchRequest req = waitNext(applierInterf.initVersionBatch.getFuture()) ) {
					requestTypeStr = "initVersionBatch";
					actors.add(handleInitVersionBatchRequest(req, self));
				}
				when ( RestoreSimpleRequest req = waitNext(applierInterf.finishRestore.getFuture()) ) {
					requestTypeStr = "finishRestore";
					exitRole =  handlerFinishRestoreRequest(req, self, cx);
				}
				when ( wait(exitRole) ) {
					TraceEvent("FastRestore").detail("RestoreApplierCore", "ExitRole");
					break;
				}
			}
		} catch (Error &e) {
			TraceEvent(SevWarn, "FastRestore").detail("RestoreLoaderError", e.what()).detail("RequestType", requestTypeStr);
			break;
		}
	}

	return Void();
}

// The actor may be invovked multiple times and executed async.
// No race condition as long as we do not wait or yield when operate the shared data, it should be fine,
// because all actors run on 1 thread.
ACTOR Future<Void> handleSendMutationVectorRequest(RestoreSendMutationVectorVersionedRequest req, Reference<RestoreApplierData> self) {
	state int numMutations = 0;

	TraceEvent("FastRestore").detail("ApplierNode", self->id())
			.detail("LogVersion", self->logVersion.get()).detail("RangeVersion", self->rangeVersion.get())
			.detail("Request", req.toString());
	if ( debug_verbose ) {
		// NOTE: Print out the current version and received req is helpful in debugging
		printf("[VERBOSE_DEBUG] handleSendMutationVectorRequest Node:%s at rangeVersion:%ld logVersion:%ld receive mutation number:%d, req:%s\n",
			self->describeNode().c_str(), self->rangeVersion.get(), self->logVersion.get(), req.mutations.size(), req.toString().c_str());
	}

	if ( req.isRangeFile ) {
		wait( self->rangeVersion.whenAtLeast(req.prevVersion) );
	} else {
		wait( self->logVersion.whenAtLeast(req.prevVersion) );
	}

	if ( (req.isRangeFile &&  self->rangeVersion.get() == req.prevVersion) ||
	     (!req.isRangeFile && self->logVersion.get() == req.prevVersion) )  {  // Not a duplicate (check relies on no waiting between here and self->version.set() below!)
		// Applier will cache the mutations at each version. Once receive all mutations, applier will apply them to DB
		state Version commitVersion = req.version;
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

		// Notify the same actor and unblock the request at the next version
		if ( req.isRangeFile ) {
			self->rangeVersion.set(req.version);
		} else {
			self->logVersion.set(req.version);
		}
	}

	req.reply.send(RestoreCommonReply(self->id()));
	return Void();
}

 ACTOR Future<Void> applyToDB(Reference<RestoreApplierData> self, Database cx) {
 	state bool isPrint = false; //Debug message
 	state std::string typeStr = "";

	// Assume the process will not crash when it apply mutations to DB. The reply message can be lost though
	if (self->kvOps.empty()) {
		printf("Node:%s kvOps is empty. No-op for apply to DB\n", self->describeNode().c_str());
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
						tr->atomicOp(m.param1, m.param2, m.type);
					} else {
						printf("[WARNING] mtype:%d (%s) unhandled\n", m.type, typeStr.c_str());
					}
					++count;
					transactionSize += m.expectedSize();
					
					if ( transactionSize >= opConfig.transactionBatchSizeThreshold ) { // commit per 1000 mutations
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

 	return Void();
 }

 ACTOR Future<Void> handleApplyToDBRequest(RestoreSimpleRequest req, Reference<RestoreApplierData> self, Database cx) {
	if ( !self->dbApplier.present() ) {
		self->dbApplier = Never();
		self->dbApplier = applyToDB(self, cx);
		wait( self->dbApplier.get() );
	} else {
		ASSERT( self->dbApplier.present() );
		wait( self->dbApplier.get() );
	}
	
	req.reply.send(RestoreCommonReply(self->id()));

	return Void();
}



