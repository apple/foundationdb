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

#include "fdbserver/RemoteIKeyValueStore.actor.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/FlowProcess.actor.h"

#include "fdbserver/Knobs.h"
#include "flow/ActorCollection.h"
#include "flow/Error.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

StringRef KeyValueStoreProcess::_name = "KeyValueStoreProcess"_sr;

// test adding a guard for guaranteed killing of machine after runIKVS returns
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
	void invalidate() { kvStore = nullptr; }
};

ACTOR void sendCommitReply(IKVSCommitRequest commitReq, IKeyValueStore* kvStore, Future<Void> onClosed) {
	try {
		choose {
			when(wait(onClosed)) { commitReq.reply.sendError(remote_kvs_cancelled()); }
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
void cancellableForwardPromise(ReplyPromise<T> output, Future<T> input, Future<Void> stop) {
	try {
		choose {
			when(T value = wait(input)) { output.send(value); }
			when(wait(stop)) { return; }
		}
	} catch (Error& e) {
		output.sendError(e);
	}
}

ACTOR template <class T>
void forwardPromiseVariant(ReplyPromise<T> output, Future<T> input, std::string trace = "") {
	try {
		T value = wait(input);
		output.send(value);
		if (trace.size()) {
			TraceEvent(SevDebug, "ForwardPromiseSend").detail("Name", trace);
		}
	} catch (Error& e) {
		TraceEvent(SevDebug, "ForwardPromiseVariantError").errorUnsuppressed(e).backtrace();
		output.sendError(e.code() == error_code_actor_cancelled ? remote_kvs_cancelled() : e);
	}
}

ACTOR Future<Void> runIKVS(OpenKVStoreRequest openReq, IKVSInterface ikvsInterface) {
	state IKeyValueStore* kvStore = openKVStore(openReq.storeType,
	                                            openReq.filename,
	                                            openReq.logID,
	                                            openReq.memoryLimit,
	                                            openReq.checkChecksums,
	                                            openReq.checkIntegrity);
	state UID uid_kvs(deterministicRandom()->randomUniqueID());
	state AfterReturn guard(kvStore, uid_kvs);
	state Promise<Void> onClosed;
	TraceEvent(SevDebug, "RemoteKVStoreInitializing")
	    .detail("Action", "initializing local store")
	    .detail("UID", uid_kvs);
	wait(kvStore->init());
	openReq.reply.send(ikvsInterface);
	TraceEvent("RemoteKVStoreInitializaed").detail("IKVSInterfaceUID", ikvsInterface.id());

	loop {
		try {
			choose {
				when(IKVSGetValueRequest getReq = waitNext(ikvsInterface.getValue.getFuture())) {
					// forwardPromise(getReq.reply, kvStore->readValue(getReq.key, getReq.type, getReq.debugID));
					cancellableForwardPromise(getReq.reply,
					                          kvStore->readValue(getReq.key, getReq.type, getReq.debugID),
					                          onClosed.getFuture());
				}
				when(IKVSSetRequest req = waitNext(ikvsInterface.set.getFuture())) { kvStore->set(req.keyValue); }
				when(IKVSClearRequest req = waitNext(ikvsInterface.clear.getFuture())) { kvStore->clear(req.range); }
				when(IKVSCommitRequest commitReq = waitNext(ikvsInterface.commit.getFuture())) {
					sendCommitReply(commitReq, kvStore, onClosed.getFuture());
				}
				when(IKVSReadValuePrefixRequest readPrefixReq = waitNext(ikvsInterface.readValuePrefix.getFuture())) {
					// forwardPromise(
					//     readPrefixReq.reply,
					//     kvStore->readValuePrefix(
					//         readPrefixReq.key, readPrefixReq.maxLength, readPrefixReq.type, readPrefixReq.debugID));
					cancellableForwardPromise(
					    readPrefixReq.reply,
					    kvStore->readValuePrefix(
					        readPrefixReq.key, readPrefixReq.maxLength, readPrefixReq.type, readPrefixReq.debugID),
					    onClosed.getFuture());
				}
				when(IKVSReadRangeRequest readRangeReq = waitNext(ikvsInterface.readRange.getFuture())) {
					cancellableForwardPromise(
					    readRangeReq.reply,
					    fmap([](const RangeResult& result) { return IKVSReadRangeReply(result); },
					         kvStore->readRange(
					             readRangeReq.keys, readRangeReq.rowLimit, readRangeReq.byteLimit, readRangeReq.type)),
					    onClosed.getFuture());
				}
				when(IKVSGetStorageByteRequest req = waitNext(ikvsInterface.getStorageBytes.getFuture())) {
					StorageBytes storageBytes = kvStore->getStorageBytes();
					req.reply.send(storageBytes);
				}
				when(IKVSGetErrorRequest getFutureReq = waitNext(ikvsInterface.getError.getFuture())) {
					forwardPromiseVariant(getFutureReq.reply, kvStore->getError());
				}
				when(IKVSOnClosedRequest onClosedReq = waitNext(ikvsInterface.onClosed.getFuture())) {
					forwardPromiseVariant(onClosedReq.reply, kvStore->onClosed(), "OnClosed");
				}
				when(IKVSDisposeRequest req = waitNext(ikvsInterface.dispose.getFuture())) {
					TraceEvent(SevDebug, "RemoteIKVSDisposeReceivedRequest").detail("UID", uid_kvs);
					Future<Void> f = kvStore->onClosed();
					kvStore->dispose();
					guard.invalidate();
					onClosed.send(Void());
					wait(f);
					TraceEvent(SevDebug, "RemoteIKVSDispose").detail("UID", uid_kvs);
					return Void();
				}
				when(IKVSCloseRequest req = waitNext(ikvsInterface.close.getFuture())) {
					TraceEvent(SevDebug, "RemoteIKVSCloseReceivedRequest").detail("UID", uid_kvs);
					Future<Void> f = kvStore->onClosed();
					kvStore->close();
					guard.invalidate();
					onClosed.send(Void());
					wait(f);
					TraceEvent(SevDebug, "RemoteIKVSClose").detail("UID", uid_kvs);
					return Void();
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				// fprintf(stderr, "RemoteIKVS cancelling\n");
				TraceEvent("RemoteKVStoreCancelled").detail("UID", uid_kvs).backtrace();
				// kvStore->close();
				// guard.invalidate();
				onClosed.send(Void());
				// fprintf(stderr, "RemoteIKVS cancelled\n");
				return Void();
			} else {
				TraceEvent(SevError, "RemoteKVStoreError").error(e).detail("UID", uid_kvs).backtrace();
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

	std::string flowProcessAddr = g_network->getLocalAddress().ip.toString().append(":0");
	std::vector<std::string> args = { "bin/fdbserver",
		                              "-r",
		                              "flowprocess",
		                              "-C",
		                              SERVER_KNOBS->CONN_FILE,
		                              "--logdir",
		                              SERVER_KNOBS->LOG_DIRECTORY,
		                              "-p",
		                              flowProcessAddr,
		                              "--process-name",
		                              KeyValueStoreProcess::_name.toString(),
		                              "--process-endpoint",
		                              format("%s,%lu,%lu", address.c_str(), token.first(), token.second()) };
	// For remote IKV store, we need to make sure the shutdown signal is sent back until we can destroy it in the
	// simulation
	process = spawnProcess(path, args, -1.0, false, 0.01, self);
	choose {
		when(FlowProcessRegistrationRequest req = waitNext(processInterface.registerProcess.getFuture())) {
			self->consumeInterface(req.flowProcessInterface);
			ready.send(Void());
		}
		when(int res = wait(process)) {
			TraceEvent(SevDebug, "FlowProcessRunnerFinishedInChoose").detail("Result", res);
			// 0 means process killed; non-zero means errors
			if (res) {
				ready.sendError(operation_failed());
			} else {
				ready.sendError(shutdown_in_progress());
			}
			return res;
		}
	}
	int res = wait(process);
	return res;
}

ACTOR static Future<Void> initializeRemoteKVStore(RemoteIKeyValueStore* self, OpenKVStoreRequest openKVSReq) {
	TraceEvent("WaitingOnFlowProcess").detail("StoreType", openKVSReq.storeType).log();
	Promise<Void> ready;
	self->returnCode = flowProcessRunner(self, ready);
	wait(ready.getFuture());
	TraceEvent("FlowProcessReady").log();
	IKVSInterface ikvsInterface = wait(self->kvsProcess.openKVStore.getReply(openKVSReq));
	TraceEvent("IKVSInterfaceReceived").detail("UID", ikvsInterface.id());
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
