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

// test adding a guard for guaranteed killing of machine after runIKVS returns
struct AfterReturn {
	IKeyValueStore* kvStore;
	AfterReturn() {}
	AfterReturn(IKeyValueStore* store) : kvStore(store) {}
	~AfterReturn() {
		TraceEvent(SevDebug, "RemoteKVStoreAfterReturn").log();
		if (kvStore != nullptr) {
			kvStore->close();
		}
	}
};

ACTOR void sendCommitReply(IKVSCommitRequest commitReq, IKeyValueStore* kvStore) {
	wait(kvStore->commit(commitReq.sequential));
	StorageBytes storageBytes = kvStore->getStorageBytes();
	commitReq.reply.send(IKVSCommitReply{ storageBytes });
}

ACTOR Future<Void> runIKVS(OpenKVStoreRequest openReq, IKVSInterface ikvsInterface) {
	state IKeyValueStore* kvStore = openKVStore(openReq.storeType,
	                                            openReq.filename,
	                                            openReq.logID,
	                                            openReq.memoryLimit,
	                                            openReq.checkChecksums,
	                                            openReq.checkIntegrity);
	state AfterReturn guard(kvStore);
	TraceEvent(SevDebug, "RemoteKVStore").detail("Action", "initializing local store");
	wait(kvStore->init());
	openReq.reply.send(ikvsInterface);
	TraceEvent(SevDebug, "RemoteKVStore").detail("IKVSInterfaceUID", ikvsInterface.id());

	loop {
		try {
			choose {
				when(IKVSGetValueRequest getReq = waitNext(ikvsInterface.getValue.getFuture())) {
					forwardPromise(getReq.reply, kvStore->readValue(getReq.key, getReq.type, getReq.debugID));
				}
				when(IKVSSetRequest req = waitNext(ikvsInterface.set.getFuture())) { kvStore->set(req.keyValue); }
				when(IKVSClearRequest req = waitNext(ikvsInterface.clear.getFuture())) { kvStore->clear(req.range); }
				when(IKVSCommitRequest commitReq = waitNext(ikvsInterface.commit.getFuture())) {
					sendCommitReply(commitReq, kvStore);
				}
				when(IKVSReadValuePrefixRequest readPrefixReq = waitNext(ikvsInterface.readValuePrefix.getFuture())) {
					forwardPromise(
					    readPrefixReq.reply,
					    kvStore->readValuePrefix(
					        readPrefixReq.key, readPrefixReq.maxLength, readPrefixReq.type, readPrefixReq.debugID));
				}
				when(IKVSReadRangeRequest readRangeReq = waitNext(ikvsInterface.readRange.getFuture())) {
					forwardPromise(
					    readRangeReq.reply,
					    kvStore->readRange(
					        readRangeReq.keys, readRangeReq.rowLimit, readRangeReq.byteLimit, readRangeReq.type));
				}
				when(IKVSGetStorageByteRequest req = waitNext(ikvsInterface.getStorageBytes.getFuture())) {
					StorageBytes storageBytes = kvStore->getStorageBytes();
					req.reply.send(storageBytes);
				}
				when(IKVSGetErrorRequest getFutureReq = waitNext(ikvsInterface.getError.getFuture())) {
					forwardPromise(getFutureReq.reply, kvStore->getError());
				}
				when(IKVSOnClosedRequest onClosedReq = waitNext(ikvsInterface.onClosed.getFuture())) {
					forwardPromise(onClosedReq.reply, kvStore->onClosed());
				}
				when(IKVSDisposeRequest req = waitNext(ikvsInterface.dispose.getFuture())) { kvStore->dispose(); }
				when(IKVSCloseRequest req = waitNext(ikvsInterface.close.getFuture())) { kvStore->close(); }
			}
		} catch (Error& e) {
			TraceEvent(SevDebug, "RemoteKVStoreError").detail("Error", e.code());
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			throw actor_cancelled();
			// TODO: Error handling
		}
	}
	// return Void();
}

ACTOR Future<Void> spawnRemoteIKVS(RemoteIKeyValueStore* self, OpenKVStoreRequest openKVSReq) {
	state KeyValueStoreProcess process;
	process.start();
	TraceEvent(SevDebug, "WaitingOnFlowProcess").log();
	wait(process.onReady());
	TraceEvent(SevDebug, "FlowProcessReady").log();
	IKVSInterface ikvsInterface = wait(process.kvsIf.openKVStore.getReply(openKVSReq));
	TraceEvent(SevDebug, "IKVSInterfaceReceived").detail("UID", ikvsInterface.id());
	self->ikvsProcess = process;
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
	OpenKVStoreRequest request(storeType, filename, logID, memoryLimit, checkChecksums, checkIntegrity);
	self->initialized = spawnRemoteIKVS(self, request);
	return self;
}

ACTOR Future<Void> runRemoteServer() {

	state IKVSProcessInterface processInterface;
	state ActorCollection actors(false);
	processInterface.getProcessInterface.makeWellKnownEndpoint(WLTOKEN_IKVS_PROCESS_SERVER,
	                                                           TaskPriority::DefaultEndpoint);

	loop {
		try {
			choose {
				when(GetIKVSProcessInterfaceRequest req = waitNext(processInterface.getProcessInterface.getFuture())) {
					TraceEvent(SevDebug, "RequestIKVSProcessInterface").detail("Status", "received request");
					req.reply.send(processInterface);
				}
				when(OpenKVStoreRequest req = waitNext(processInterface.openKVStore.getFuture())) {
					IKVSInterface ikvsInterf;
					ikvsInterf.storeType = req.storeType;
					TraceEvent(SevDebug, "CreatingIKVSInterface").detail("UID", ikvsInterf.id());
					actors.add(runIKVS(req, ikvsInterf));
				}
				when(wait(actors.getResult())) {
					TraceEvent(SevDebug, "RemoteIKVStoreServerError").detail("Action", "actors got result");
					// add futures, if any throw exception, throw on the wait get result
					UNSTOPPABLE_ASSERT(false);
				}
			}
		} catch (Error& e) {
			TraceEvent(SevDebug, "RemoteIKVStoreServerError").detail("Error", e.code());
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			throw;
			// TODO: Error handling
		}
	}
}