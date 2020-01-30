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
#include "flow/ThreadHelper.actor.h"
#include "flow/TDMetric.actor.h"
#include "flow/Stats.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

#include "grpc_service/kv_service.grpc.pb.h"
#include "grpc_service/kv_service.pb.h"

#include <grpcpp/grpcpp.h>

struct RpcProxyData {
	UID dbgid;

	CounterCollection cc;
	Future<Void> logger;

	Database cx;

	RpcProxyData(UID dbgid, const InitializeRpcProxyRequest& req, Reference<AsyncVar<ServerDBInfo>> db) : dbgid(dbgid), cc("RpcProxy", dbgid.toString()) {
		logger = traceCounters("RpcProxyMetrics", dbgid, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "RpcProxyMetrics");
		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, false, true);
	}
};

struct RpcDatabase {
	RpcDatabase(Database cx) : cx(cx) {}
	Future<Optional<Value>> getValue(Key key);//, RpcCallData<foundationdb::GetValueRequest, foundationdb::GetValueReply> *callData);

	Database cx;
};

ACTOR Future<Optional<Value>> getValue(RpcDatabase *self, Key key) {//, grpc::Responder responder, void *tag) {
	loop {
		state Transaction tr(self->cx);
		try {
			Optional<Value> value = wait(tr.get(key));
			return value;
			//callData->reply.set_value(value.present() ? value.get().toString() : "<not present>");
			//callData->responder.Finish(reply, Status::OK, callData);
		} catch(Error &e) {
			wait(tr.onError(e));
		}
	}	
}

Future<Optional<Value>> RpcDatabase::getValue(Key key) {
	return ::getValue(this, key);
}

class FdbKvClientServiceImpl : public foundationdb::FdbKvClient::Service {
public:
	FdbKvClientServiceImpl(RpcDatabase *db) : db(db) {}

private:
	grpc::Status GetValue(grpc::ServerContext* context, const foundationdb::GetValueRequest* request, foundationdb::GetValueReply* reply) override {
		Standalone<StringRef> key(request->key());
		ThreadFuture<Optional<Value>> f = onMainThread([this, key]() {
			return db->getValue(key);
		});

		Optional<Value> value = f.getBlocking();

		reply->set_value(value.present() ? value.get().toString() : "<not present>");
		return grpc::Status::OK;
	}

	RpcDatabase *db;
};

/*class RpcCallDataBase {
public:
	RpcCallData(foundationdb::FdbKvClient::AsyncService *service, ServerCompletionQueue *completionQueue, RpcDatabase *db) : service(service), completionQueue(completionQueue), db(db), status(Status::PROCESS) {}

	virtual ~RpcCallData() {}

	void HandleCall() {
		if(status == Status::PROCESS) {
			status = Status::FINISH;
			processRequest();
		} else if(status == Status::FINISH) {
			finishRequest();
		} else {
			ASSERT(false);
		}
	}

protected:
	foundationdb::FdbKvClient::AsyncService *service;
	ServerCompletionQueue *completionQueue;
	RpcDatabase *db;

	ServerContext ctx;

	enum class Status {
		PROCESS
		FINISH
	};

	Status status;

	virtual void getRequest() = 0;
	virtual void processRequest() = 0;

	virtual void finishRequest() {
		delete this;
	}
};

template<class Request, class Reply>
class RpcCallData : public RpcCallDataBase {
public:
	RpcCallData(foundationdb::FdbKvClient::AsyncService *service, ServerCompletionQueue *completionQueue, RpcDatabase *db) : RpcCallDataBase(service, completionQueue, db), responder(ctx) {
		getRequest();
	}

private:
	Request request;
	Reply reply;

	SeverAsyncResponseWriter<Reply> responder;

	void getRequest() {
		service->RequestGetValue(&ctx, &request, &responder, cq, cq, this);
	}

	void processRequest() {
		new RpcCallData<Request, Reply>(service, completionQueue, db);

		Standalone<StringRef> key(request->key());
		ThreadFuture<Optional<Value>> f = onMainThread([this, key]() {
			return db->getValue(key, this);
		});
	}
};*/

void* runServer(void* dbPtr) {
	std::string server_address("0.0.0.0:50051");
	RpcDatabase *db = (RpcDatabase*)dbPtr;
	//foundationdb::FdbKvClient::AsyncService service;
	FdbKvClientServiceImpl service(db);

	grpc::ServerBuilder builder;

	// Listen on the given address without any authentication mechanism.
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

	// Register "service" as the instance through which we'll communicate with clients.
	builder.RegisterService(&service);
	//builder.RegisterAsyncService(&service);

	auto cq = builder.AddCompletionQueue();
	auto server = builder.BuildAndStart();

	fprintf(stderr, "Running service\n");

	server->Wait();

	/*new RpcCallData<GetValueRequest, GetValueReply>(&service, cq.get(), db);

	void *tag;
	bool ok;

	while(true) {
		cq->next(&tag, &ok);
		static_cast<RpcCallData<GetValueRequest, GetValueReply>*>(tag)->proceed();
	}*/

	return nullptr;
}

ACTOR Future<Void> processRpcRequests() {
	loop {
		wait(Future<Void>(Never()));
	}
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
	state RpcDatabase rpcDb(rpcProxyData.cx);

	/*THREAD_HANDLE rpcThread = */startThread(runServer, &rpcDb);

	addActor.send(processRpcRequests());

	loop choose {
		when( wait( dbInfoChange ) ) {
			dbInfoChange = db->onChange();
		}
		when (wait(error)) {}
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
