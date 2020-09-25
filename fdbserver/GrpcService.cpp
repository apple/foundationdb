/*
 * GrpcService.cpp 
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

#include "fdbserver/GrpcService.h"
#include "fdbserver/IRpcInterface.h"

#include "flow/flow.h"
#include "flow/ThreadHelper.actor.h"
#include "fdbclient/FDBTypes.h"

#include "fdbserver/kv_service.grpc.pb.h"
#include "fdbserver/kv_service.pb.h"

#include <grpcpp/grpcpp.h>

void* handleRequests(void *serverPtr);

template <class Request, class Reply>
using GrpcRequestFunc = void (foundationdb::FdbKvClient::AsyncService::*)(grpc::ServerContext *context, 
                                                                          Request *request, 
                                                                          grpc::ServerAsyncResponseWriter<Reply> *response, 
                                                                          grpc::CompletionQueue *new_call_cq,
                                                                          grpc::ServerCompletionQueue *notification_cq, 
                                                                          void *tag);

class RpcCallback;

class CallDataBase : public ThreadSafeReferenceCounted<CallDataBase> {
public:
	void init(RpcInterface *rpcInterface, foundationdb::FdbKvClient::AsyncService *asyncGrpcService, grpc::ServerCompletionQueue *cq) {
		this->rpcInterface = rpcInterface; 
		this->asyncGrpcService = asyncGrpcService;
		this->cq = cq;

		enqueue();
	}

	static void proceed(Reference<CallDataBase> callData) {
		callData->process();
	}

	static std::vector<Reference<CallDataBase>> initialCallData;

	virtual const char* name() = 0;

protected:
	virtual void process() = 0;
	virtual void finishRequest() = 0;
	virtual void enqueue() = 0;

	RpcInterface* rpcInterface;
	foundationdb::FdbKvClient::AsyncService* asyncGrpcService;
	grpc::ServerCompletionQueue* cq;

	friend class RpcCallback;
};

std::vector<Reference<CallDataBase>> CallDataBase::initialCallData;

class RpcCallback : public ThreadCallback {
public:
	RpcCallback(Reference<CallDataBase> callData) : callData(callData) {}

	virtual bool canFire(int notMadeActive) {
		return true;
	};

	virtual void fire(const Void &unused, int& userParam) {
		callData->finishRequest();	
		delete this;
	};

	virtual void error(const Error&, int& userParam) {
		callData->finishRequest();	
		delete(this);
	};

private:
	Reference<CallDataBase> callData;
};

template<class CallDataSub>
class CallDataRegister {
public:
	CallDataRegister() {
		CallDataBase::initialCallData.push_back(Reference<CallDataBase>(dynamic_cast<CallDataBase*>(new CallDataSub())));
	}
};
#define REGISTER_RPC_CALL(Type) namespace CallDataRegisterNamespace { CallDataRegister<Type> callDataRegister##Type; }

template <class Request, class Reply, class Sub, class FutureType>
class CallData : public CallDataBase {
public:
	CallData() : responder(&ctx), callState(PROCESS) {}

	virtual const char *name() {
		return "CallData";
	}

protected:
	virtual void enqueue() {
		(asyncGrpcService->*getRequestFunc())(&ctx, &request, &responder, cq, cq, this);
	}

	virtual void process() {
		if(callState == PROCESS) {
			//fprintf(stderr, "GotRequest: %s\n", name());
			(new Sub())->init(rpcInterface, asyncGrpcService, cq);

			rpcResult = onMainThread([this]() {
				return makeRpcRequest();
			});

			int ignore;
			rpcResult.callOrSetAsCallback(new RpcCallback(Reference<CallDataBase>::addRef(this)), ignore, 0);
		} 
		else {
			ASSERT(callState == FINISH);
		}
	}

	virtual void finishRequest() {
		grpc::Status status;
		if(rpcResult.isError()) {
			status = fdbErrorToGrpcStatus(rpcResult.getError());
		}
		else {
			status = getResult();
		}

		//fprintf(stderr, "FinishedRequest: %s\n", name());
		responder.Finish(reply, status, this);
		addref();
		callState = FINISH;
	}

	virtual Future<FutureType> makeRpcRequest() = 0;
	virtual grpc::Status getResult() = 0;
	virtual GrpcRequestFunc<Request, Reply> getRequestFunc() = 0;

	grpc::ServerContext ctx;

	Request request;
	Reply reply;

	grpc::ServerAsyncResponseWriter<Reply> responder;

	enum CallState { PROCESS, FINISH };
	std::atomic<CallState> callState;

	ThreadFuture<FutureType> rpcResult;
};

grpc::Status fdbErrorToGrpcStatus(Error const& e) {
	// TODO: use Any type
	foundationdb::FdbError error;
	error.set_code(e.code());
	error.set_name(e.name());
	error.set_message(e.what());
	std::string errorOutput;
	error.SerializeToString(&errorOutput);
	return grpc::Status(grpc::StatusCode::ABORTED, "FoundationDB Error", errorOutput);
}

//
// RPC endpoint implementations
//

class CreateTransactionCallData : public CallData<foundationdb::CreateTransactionRequest, 
                                                  foundationdb::CreateTransactionReply, 
                                                  CreateTransactionCallData,
                                                  UID> {
protected:
	virtual GrpcRequestFunc<foundationdb::CreateTransactionRequest, foundationdb::CreateTransactionReply> getRequestFunc() {
		return &foundationdb::FdbKvClient::AsyncService::RequestCreateTransaction;
	}

	virtual Future<UID> makeRpcRequest() {
		RpcCreateTransactionRequest req(request.readversion());
		rpcInterface->createTransaction.send(req);
		return req.reply.getFuture();
	}

	virtual grpc::Status getResult() {
		foundationdb::UID *transactionId = new foundationdb::UID();
		transactionId->set_uid(rpcResult.get().toString()); // TODO: avoid string conversions
		reply.set_allocated_transactionid(transactionId);

		return grpc::Status::OK;
	}

public:
	virtual const char* name() {
		return "CreateTransaction";
	}
};
REGISTER_RPC_CALL(CreateTransactionCallData);

void applyMutations(RpcInterface *rpcInterface, UID transactionId, foundationdb::OrderedMutationSet serializedMutations) {
	Standalone<VectorRef<MutationRef>> mutations;
	for(auto mutation : serializedMutations.mutations()) {
		mutations.push_back_deep(mutations.arena(), MutationRef((MutationRef::Type)mutation.mutationtype(), mutation.param1(), mutation.param2()));
	}

	if(mutations.size()) {
		RpcApplyMutationsRequest applyMutationReq(transactionId, mutations);
		rpcInterface->applyMutations.send(applyMutationReq);
	}
}

class GetValueCallData : public CallData<foundationdb::GetValueRequest, 
                                         foundationdb::GetValueReply, 
                                         GetValueCallData,
                                         Optional<Value>> {
protected:
	virtual GrpcRequestFunc<foundationdb::GetValueRequest, foundationdb::GetValueReply> getRequestFunc() {
		return &foundationdb::FdbKvClient::AsyncService::RequestTransactionGetValue;
	}

	virtual Future<Optional<Value>> makeRpcRequest() {
		UID transactionId = UID::fromString(request.transactionid().uid());

		if(request.has_mutations()) {
			applyMutations(rpcInterface, transactionId, request.mutations());
		}

		RpcGetValueRequest req(transactionId, KeyRef(request.key()), request.snapshot());
		rpcInterface->getValue.send(req);
		return req.reply.getFuture();
	}

	virtual grpc::Status getResult() {
		reply.set_present(rpcResult.get().present());
		if(rpcResult.get().present()) {
			reply.set_value(rpcResult.get().get().begin(), rpcResult.get().get().size());
		}
		return grpc::Status::OK;
	}

public:
	virtual const char* name() {
		return "GetValue";
	}
};
REGISTER_RPC_CALL(GetValueCallData);

class MutateCallData : public CallData<foundationdb::MutateRequest, 
                                       foundationdb::MutateReply, 
                                       MutateCallData,
                                       Void> {
protected:
	virtual GrpcRequestFunc<foundationdb::MutateRequest, foundationdb::MutateReply> getRequestFunc() {
		return &foundationdb::FdbKvClient::AsyncService::RequestTransactionMutate;
	}

	virtual Future<Void> makeRpcRequest() {
		UID transactionId = UID::fromString(request.transactionid().uid());
		applyMutations(rpcInterface, transactionId, request.mutations());
		return Future<Void>(Void());
	}

	virtual grpc::Status getResult() {
		return grpc::Status::OK;
	}

public:
	virtual const char* name() {
		return "Mutate";
	}
};
REGISTER_RPC_CALL(MutateCallData);

class CommitCallData : public CallData<foundationdb::CommitRequest, 
                                       foundationdb::CommitReply, 
                                       CommitCallData,
                                       std::pair<Version, Standalone<StringRef>>> {
protected:
	virtual GrpcRequestFunc<foundationdb::CommitRequest, foundationdb::CommitReply> getRequestFunc() {
		return &foundationdb::FdbKvClient::AsyncService::RequestTransactionCommit;
	}

	virtual Future<std::pair<Version, Standalone<StringRef>>> makeRpcRequest() {
		UID transactionId = UID::fromString(request.transactionid().uid());

		if(request.has_mutations()) {
			applyMutations(rpcInterface, transactionId, request.mutations());
		}

		RpcCommitRequest req(transactionId);
		rpcInterface->commit.send(req);
		return req.reply.getFuture();
	}

	virtual grpc::Status getResult() {
		reply.set_committedversion(rpcResult.get().first);
		reply.set_versionstamp(rpcResult.get().second.begin(), rpcResult.get().second.size());
		return grpc::Status::OK;
	}

public:
	virtual const char* name() {
		return "Commit";
	}
};
REGISTER_RPC_CALL(CommitCallData);

class TransactionDestroyCallData : public CallData<foundationdb::TransactionDestroyRequest, 
                                          foundationdb::TransactionDestroyReply, 
                                          TransactionDestroyCallData,
                                          Void> {
protected:
	virtual GrpcRequestFunc<foundationdb::TransactionDestroyRequest, foundationdb::TransactionDestroyReply> getRequestFunc() {
		return &foundationdb::FdbKvClient::AsyncService::RequestTransactionDestroy;
	}

	virtual Future<Void> makeRpcRequest() {
		UID transactionId = UID::fromString(request.transactionid().uid());

		RpcTransactionDestroyRequest req(transactionId);
		rpcInterface->transactionDestroy.send(req);
		return Future<Void>(Void());
	}

	virtual grpc::Status getResult() {
		return grpc::Status::OK;
	}

public:
	virtual const char* name() {
		return "TransactionDestroy";
	}
};
REGISTER_RPC_CALL(TransactionDestroyCallData);

//
// RPC Service
//

Future<Void> GrpcService::run(NetworkAddress address, RpcInterface const& rpcInterface) {
	this->rpcInterface = rpcInterface;

	std::string serverAddress("0.0.0.0:50051"); // TODO: use address
	grpc::ServerBuilder builder;

	// Listen on the given address without any authentication mechanism.
	builder.AddListeningPort(serverAddress, grpc::InsecureServerCredentials());
	builder.RegisterService(&asyncGrpcService);

	cq = builder.AddCompletionQueue();
	server = builder.BuildAndStart();

	TraceEvent("GrpcServiceStarted").detail("Address", serverAddress);
	startThread(::handleRequests, this);

	return shutdownPromise.getFuture();
}

void GrpcService::shutdown() {
	isShutdown = true;
}

void GrpcService::doShutdown(Optional<Error> e) {
	TraceEvent("GrpcServiceShuttingDown");

	server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(1)); // TODO: different for errors? Knobify?
	cq->Shutdown();

	void *tag;
	bool ok;
	while(cq->Next(&tag, &ok)) {
		static_cast<CallDataBase*>(tag)->delref();
	}

	onMainThread([this, e](){
		TraceEvent("GrpcServiceShutDown");
		if(e.present()) {
			shutdownPromise.sendError(e.get());
		}
		else {
			shutdownPromise.send(Void());
		}
		return Future<Void>(Void());
	});
}

void GrpcService::handleRequests() {
	try {
		while(CallDataBase::initialCallData.size() > 0) {
			Reference<CallDataBase> callData = CallDataBase::initialCallData.back();
			CallDataBase::initialCallData.pop_back();
			callData->init(&rpcInterface, &asyncGrpcService, cq.get());
			callData.extractPtr();
		}

		void* tag;
		bool ok;
		while(!isShutdown) {
			ASSERT(cq->Next(&tag, &ok));
			Reference<CallDataBase> callData(static_cast<CallDataBase*>(tag));
			if(ok) {
				CallDataBase::proceed(callData);
			}
		}
		doShutdown(Optional<Error>());
	}
	catch(Error &e) {
		TraceEvent(SevError, "UnexpectedGrpcError").error(e);
		doShutdown(e);
	}
	catch(...) {
		TraceEvent(SevError, "UnknownGrpcError");
		doShutdown(unknown_error());
	}
}

void* handleRequests(void *serverPtr) {
	Reference<GrpcService> service = Reference<GrpcService>::addRef(static_cast<GrpcService*>(serverPtr));
	service->handleRequests();
	return nullptr;
}
