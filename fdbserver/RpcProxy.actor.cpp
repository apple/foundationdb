/*
 * RpcProxy.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#include "flow/ActorCollection.h"
#include "flow/Platform.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/ApplyMetadataMutation.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/IRpcInterface.h"
#include "fdbserver/GrpcService.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/TDMetric.actor.h"
#include "fdbrpc/Stats.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct RpcProxyData {
	UID dbgid;

	CounterCollection cc;
	Future<Void> logger;

	Database cx;

	std::map<UID, Reference<ReadYourWritesTransaction>> openTransactions; // TODO: need expiration mechanism

	RpcProxyData(UID dbgid, const InitializeRpcProxyRequest& req, Reference<AsyncVar<ServerDBInfo>> db) : dbgid(dbgid), cc("RpcProxy", dbgid.toString()) {
		logger = traceCounters("RpcProxyMetrics", dbgid, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "RpcProxyMetrics");
		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, false, true);
	}
};

void processCreateTransaction(RpcProxyData *rpcProxyData, RpcCreateTransactionRequest request) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(rpcProxyData->cx));
	if(request.readVersion > 0) {
		tr->setVersion(request.readVersion);
	}
	else {
		tr->getReadVersion(); // start the read version fetch
	}
	UID id = deterministicRandom()->randomUniqueID();
	rpcProxyData->openTransactions[id] = tr;
	request.reply.send(id);
}

Reference<ReadYourWritesTransaction> getTransaction(RpcProxyData *rpcProxyData, UID transactionId) {
	// TODO: error for missing transaction
	return rpcProxyData->openTransactions[transactionId];
}

ACTOR Future<Void> processGetValue(RpcProxyData *rpcProxyData, RpcGetValueRequest request) {
	state Reference<ReadYourWritesTransaction> tr = getTransaction(rpcProxyData, request.transactionId);

	try {
		Optional<Value> value = wait(tr->get(request.key, request.snapshot));
		request.reply.send(value);
	} 
	catch(Error &e) {
		request.reply.sendError(e);
	}

	return Void();
}

void processApplyMutations(RpcProxyData *rpcProxyData, RpcApplyMutationsRequest const& request) {
	Reference<ReadYourWritesTransaction> tr = getTransaction(rpcProxyData, request.transactionId);
	for(auto mutation : request.mutations) {
		if(mutation.type == MutationRef::SetValue) {
			tr->set(mutation.param1, mutation.param2);
		}
		else if(mutation.type == MutationRef::ClearRange) {
			tr->clear(KeyRangeRef(mutation.param1, mutation.param2));
		}
		else {
			tr->atomicOp(mutation.param1, mutation.param2, mutation.type);
		}
	}
} 

ACTOR Future<Void> processCommit(RpcProxyData *rpcProxyData, RpcCommitRequest request) {
	state Reference<ReadYourWritesTransaction> tr = getTransaction(rpcProxyData, request.transactionId);

	try {
		state Future<Standalone<StringRef>> versionstamp = tr->getVersionstamp();
		wait(tr->commit());
		ASSERT(versionstamp.isReady() && (!versionstamp.isError() || versionstamp.getError().code() == error_code_no_commit_version));
		request.reply.send(std::make_pair(tr->getCommittedVersion(), versionstamp.isError() ? Standalone<StringRef>() : versionstamp.get()));
	}
	catch(Error &e) {
		request.reply.sendError(e);
	}

	return Void();
}

void processTransactionDestroy(RpcProxyData *rpcProxyData, RpcTransactionDestroyRequest const& request) {
	fprintf(stderr, "Destroying transaction %s\n", request.transactionId.toString().c_str());
	rpcProxyData->openTransactions.erase(request.transactionId);
} 

ACTOR Future<Void> rpcProxyCore(
	RpcProxyInterface interf,
	InitializeRpcProxyRequest req,
	Reference<AsyncVar<ServerDBInfo>> db)
{
	state RpcProxyData rpcProxyData(interf.id(), req, db);
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection( addActor.getFuture() );
	state Future<Void> dbInfoChange = Void();

	state Reference<IRpcService> rpcService(dynamic_cast<IRpcService*>(new GrpcService()));
	state RpcInterface rpcInterface;

	addActor.send(rpcService->run(NetworkAddress(), rpcInterface));

	try {
		loop choose {
		when(RpcCreateTransactionRequest request = waitNext(rpcInterface.createTransaction.getFuture())) {
				processCreateTransaction(&rpcProxyData, request);
			}
			when(RpcGetValueRequest request = waitNext(rpcInterface.getValue.getFuture())) {
				addActor.send(processGetValue(&rpcProxyData, request));
			}
			when(RpcApplyMutationsRequest request = waitNext(rpcInterface.applyMutations.getFuture())) {
				processApplyMutations(&rpcProxyData, request);
			}
			when(RpcCommitRequest request = waitNext(rpcInterface.commit.getFuture())) {
				addActor.send(processCommit(&rpcProxyData, request));
			}
			when(RpcTransactionDestroyRequest request = waitNext(rpcInterface.transactionDestroy.getFuture())) {
				processTransactionDestroy(&rpcProxyData, request);
			}
			when(wait( dbInfoChange ) ) {
				dbInfoChange = db->onChange();
			}
			when(wait(error)) {}
		}
	}
	catch(...) {
		rpcService->shutdown();
		throw;
	}
}

ACTOR Future<Void> rpcProxy(
	RpcProxyInterface interf,
	InitializeRpcProxyRequest req,
	Reference<AsyncVar<ServerDBInfo>> db)
{
	try {
		TraceEvent("RpcProxyStart", interf.id());
		state Future<Void> core = rpcProxyCore(interf, req, db);
		loop choose{
			when(wait(core)) { return Void(); }
		}
	}
	catch (Error& e) {
		if (e.code() == error_code_actor_cancelled || e.code() == error_code_worker_removed)
		{
			TraceEvent("RpcProxyTerminated", interf.id()).error(e, true);
			return Void();
		}
		throw;
	}
}
