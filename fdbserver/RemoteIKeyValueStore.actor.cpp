/*
 * RemoteIKeyValueStore.actor.cpp
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

#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "flow/ActorCollection.h"
#include "flow/Error.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "fdbrpc/FlowProcess.actor.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RemoteIKeyValueStore.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

StringRef KeyValueStoreProcess::_name = "KeyValueStoreProcess"_sr;

// A guard for guaranteed killing of machine after runIKVS returns
struct AfterReturn {
	IKeyValueStore* kvStore;
	UID id;
	AfterReturn() : kvStore(nullptr) {}
	AfterReturn(IKeyValueStore* store, UID& uid) : kvStore(store), id(uid) {}
	~AfterReturn() {
		TraceEvent(SevDebug, "RemoteKVStoreAfterReturn")
		    .detail("Valid", kvStore != nullptr ? "True" : "False")
		    .detail("UID", id)
		    .log();
		if (kvStore != nullptr) {
			kvStore->close();
		}
	}
	// called when we already explicitly closed the kv store
	void invalidate() { kvStore = nullptr; }
};

ACTOR void sendCommitReply(IKVSCommitRequest commitReq, IKeyValueStore* kvStore, Future<Void> onClosed) {
	try {
		choose {
			when(wait(onClosed)) {
				commitReq.reply.sendError(remote_kvs_cancelled());
			}
			when(wait(kvStore->commit(commitReq.sequential))) {
				StorageBytes storageBytes = kvStore->getStorageBytes();
				commitReq.reply.send(IKVSCommitReply(storageBytes));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevDebug, "RemoteKVSCommitReplyError").errorUnsuppressed(e);
		commitReq.reply.sendError(e.code() == error_code_actor_cancelled ? remote_kvs_cancelled() : e);
	}
}

ACTOR template <class T>
Future<Void> cancellableForwardPromise(ReplyPromise<T> output, Future<T> input) {
	try {
		T value = wait(input);
		output.send(value);
	} catch (Error& e) {
		TraceEvent(SevDebug, "CancellableForwardPromiseError").errorUnsuppressed(e).backtrace();
		output.sendError(e.code() == error_code_actor_cancelled ? remote_kvs_cancelled() : e);
	}
	return Void();
}

ACTOR Future<Void> runIKVS(OpenKVStoreRequest openReq, IKVSInterface ikvsInterface) {
	state IKeyValueStore* kvStore = openKVStore(openReq.storeType,
	                                            openReq.filename,
	                                            openReq.logID,
	                                            openReq.memoryLimit,
	                                            openReq.checkChecksums,
	                                            openReq.checkIntegrity);
	state UID kvsId(ikvsInterface.id());
	state ActorCollection actors(false);
	state AfterReturn guard(kvStore, kvsId);
	state Promise<Void> onClosed;
	TraceEvent(SevDebug, "RemoteKVStoreInitializing").detail("UID", kvsId);
	wait(kvStore->init());
	openReq.reply.send(ikvsInterface);
	TraceEvent(SevInfo, "RemoteKVStoreInitialized").detail("IKVSInterfaceUID", kvsId);

	loop {
		try {
			choose {
				when(IKVSGetValueRequest getReq = waitNext(ikvsInterface.getValue.getFuture())) {
					actors.add(cancellableForwardPromise(getReq.reply, kvStore->readValue(getReq.key, getReq.options)));
				}
				when(IKVSSetRequest req = waitNext(ikvsInterface.set.getFuture())) {
					kvStore->set(req.keyValue);
				}
				when(IKVSClearRequest req = waitNext(ikvsInterface.clear.getFuture())) {
					kvStore->clear(req.range);
				}
				when(IKVSCommitRequest commitReq = waitNext(ikvsInterface.commit.getFuture())) {
					sendCommitReply(commitReq, kvStore, onClosed.getFuture());
				}
				when(IKVSReadValuePrefixRequest readPrefixReq = waitNext(ikvsInterface.readValuePrefix.getFuture())) {
					actors.add(cancellableForwardPromise(
					    readPrefixReq.reply,
					    kvStore->readValuePrefix(readPrefixReq.key, readPrefixReq.maxLength, readPrefixReq.options)));
				}
				when(IKVSReadRangeRequest readRangeReq = waitNext(ikvsInterface.readRange.getFuture())) {
					actors.add(cancellableForwardPromise(
					    readRangeReq.reply,
					    fmap([](const RangeResult& result) { return IKVSReadRangeReply(result); },
					         kvStore->readRange(readRangeReq.keys,
					                            readRangeReq.rowLimit,
					                            readRangeReq.byteLimit,
					                            readRangeReq.options))));
				}
				when(IKVSGetStorageByteRequest req = waitNext(ikvsInterface.getStorageBytes.getFuture())) {
					StorageBytes storageBytes = kvStore->getStorageBytes();
					req.reply.send(storageBytes);
				}
				when(IKVSGetErrorRequest getFutureReq = waitNext(ikvsInterface.getError.getFuture())) {
					actors.add(cancellableForwardPromise(getFutureReq.reply, kvStore->getError()));
				}
				when(IKVSOnClosedRequest onClosedReq = waitNext(ikvsInterface.onClosed.getFuture())) {
					// onClosed request is not cancelled even this actor is cancelled
					forwardPromise(onClosedReq.reply, kvStore->onClosed());
				}
				when(IKVSDisposeRequest disposeReq = waitNext(ikvsInterface.dispose.getFuture())) {
					TraceEvent(SevDebug, "RemoteIKVSDisposeReceivedRequest").detail("UID", kvsId);
					kvStore->dispose();
					guard.invalidate();
					onClosed.send(Void());
					return Void();
				}
				when(IKVSCloseRequest closeReq = waitNext(ikvsInterface.close.getFuture())) {
					TraceEvent(SevDebug, "RemoteIKVSCloseReceivedRequest").detail("UID", kvsId);
					kvStore->close();
					guard.invalidate();
					onClosed.send(Void());
					return Void();
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				TraceEvent(SevInfo, "RemoteKVStoreCancelled").detail("UID", kvsId).backtrace();
				onClosed.send(Void());
				return Void();
			} else {
				TraceEvent(SevError, "RemoteKVStoreError").error(e).detail("UID", kvsId).backtrace();
				throw;
			}
		}
	}
}

ACTOR static Future<int> flowProcessRunner(RemoteIKeyValueStore* self, Promise<Void> ready) {
	state FlowProcessInterface processInterface;
	state Future<int> process;

	auto path = abspath(getExecPath());
	auto endpoint = processInterface.registerProcess.getEndpoint();
	auto address = endpoint.addresses.address.toString();
	auto token = endpoint.token;

	// port 0 means we will find a random available port number for it
	std::string flowProcessAddr = g_network->getLocalAddress().ip.toString().append(":0");
	std::vector<std::string> args = { "bin/fdbserver",
		                              "-r",
		                              "flowprocess",
		                              "-C",
		                              SERVER_KNOBS->CONN_FILE,
		                              "--logdir",
		                              SERVER_KNOBS->LOG_DIRECTORY,
		                              "--trace-format",
		                              getTraceFormatExtension(),
		                              "-p",
		                              flowProcessAddr,
		                              "--process-name",
		                              KeyValueStoreProcess::_name.toString(),
		                              "--process-endpoint",
		                              format("%s,%lu,%lu", address.c_str(), token.first(), token.second()) };
	// For remote IKV store, we need to make sure the shutdown signal is sent back until we can destroy it in the
	// simulation
	process = spawnProcess(path, args, -1.0, false, 0.01 /*not used*/, self);
	choose {
		when(FlowProcessRegistrationRequest req = waitNext(processInterface.registerProcess.getFuture())) {
			self->consumeInterface(req.flowProcessInterface);
			ready.send(Void());
		}
		when(int res = wait(process)) {
			// 0 means process normally shut down; non-zero means errors
			// process should not shut down normally before not ready
			ASSERT(res);
			return res;
		}
	}
	int res = wait(process);
	return res;
}

ACTOR static Future<Void> initializeRemoteKVStore(RemoteIKeyValueStore* self, OpenKVStoreRequest openKVSReq) {
	TraceEvent(SevInfo, "WaitingOnFlowProcess").detail("StoreType", openKVSReq.storeType).log();
	Promise<Void> ready;
	self->returnCode = flowProcessRunner(self, ready);
	wait(ready.getFuture());
	IKVSInterface ikvsInterface = wait(self->kvsProcess.openKVStore.getReply(openKVSReq));
	TraceEvent(SevInfo, "IKVSInterfaceReceived").detail("UID", ikvsInterface.id());
	self->interf = ikvsInterface;
	self->interf.storeType = openKVSReq.storeType;
	return Void();
}

IKeyValueStore* openRemoteKVStore(KeyValueStoreType storeType,
                                  std::string const& filename,
                                  UID logID,
                                  int64_t memoryLimit,
                                  bool checkChecksums,
                                  bool checkIntegrity) {
	RemoteIKeyValueStore* self = new RemoteIKeyValueStore();
	self->initialized = initializeRemoteKVStore(
	    self, OpenKVStoreRequest(storeType, filename, logID, memoryLimit, checkChecksums, checkIntegrity));
	return self;
}

Future<Void> runFlowProcess(std::string const& name, Endpoint endpoint) {
	TraceEvent(SevInfo, "RunFlowProcessStart").log();
	FlowProcess* self = IProcessFactory::create(name.c_str());
	self->registerEndpoint(endpoint);
	RequestStream<FlowProcessRegistrationRequest> registerProcess(endpoint);
	FlowProcessRegistrationRequest req;
	req.flowProcessInterface = self->serializedInterface();
	registerProcess.send(req);
	TraceEvent(SevDebug, "FlowProcessInitFinished").log();
	return self->run();
}
