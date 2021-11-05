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

ACTOR Future<Void> runIKVS(OpenKVStoreRequest openReq, IKVSInterface ikvsInterface);

struct KeyValueStoreProcess : FlowProcess {
	IKVSProcessInterface kvsIf;
	Standalone<StringRef> serializedIf;

	KeyValueStoreProcess() {
		ObjectWriter writer(IncludeVersion());
		writer.serialize(kvsIf);
		serializedIf = writer.toString();
	}

	StringRef name() const override {
		return "KeyValueStoreProcess"_sr;
	}
  StringRef serializedInterface() const override {
		return serializedIf;
	}

	void consumeInterface(StringRef intf) override {
		kvsIf = ObjectReader::fromStringRef<IKVSProcessInterface>(intf, IncludeVersion());
	}

	ACTOR static Future<Void> _run(KeyValueStoreProcess* self) {
		state ActorCollection actors(true);
		loop {
			choose {
				when (OpenKVStoreRequest req = waitNext(self->kvsIf.openKVStore.getFuture())) {
					IKVSInterface reply;
					actors.add(runIKVS(req, reply));
				}
				when (wait(actors.getResult())) { return Void(); }
			}
		}
	}

  Future<Void> run() override {
		return _run(this);
	}
};

ACTOR Future<Void> runIKVS(OpenKVStoreRequest openReq, IKVSInterface ikvsInterface) {
	state IKeyValueStore* kvStore = openKVStore(openReq.storeType,
	                                            openReq.filename,
	                                            openReq.logID,
	                                            openReq.memoryLimit,
	                                            openReq.checkChecksums,
	                                            openReq.checkIntegrity);
	state AfterReturn guard(kvStore);
	std::cout << "open KV store request received\n";
	TraceEvent(SevDebug, "RemoteKVStore").detail("Action", "initializing local store");
	wait(kvStore->init());
	openReq.reply.send(ikvsInterface);
	TraceEvent(SevDebug, "RemoteKVStore").detail("IKVSInterfaceUID", ikvsInterface.id());

	loop {
		try {
			choose {
				when(IKVSGetValueRequest getReq = waitNext(ikvsInterface.getValue.getFuture())) {
					// TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "get");
					forwardPromise(getReq.reply, kvStore->readValue(getReq.key, getReq.debugID));
				}
				when(IKVSSetRequest req = waitNext(ikvsInterface.set.getFuture())) {
					// TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "set");
					kvStore->set(req.keyValue);
				}
				when(IKVSClearRequest req = waitNext(ikvsInterface.clear.getFuture())) {
					// TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "clear");
					kvStore->clear(req.range);
				}
				when(IKVSCommitRequest commitReq = waitNext(ikvsInterface.commit.getFuture())) {
					// TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "commit");
					// forwardPromise(commitReq.reply, kvStore->commit(commitReq.sequential));
					sendCommitReply(commitReq, kvStore);
				}
				when(IKVSReadValuePrefixRequest readPrefixReq = waitNext(ikvsInterface.readValuePrefix.getFuture())) {
					// TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "readPrefix");
					forwardPromise(
					    readPrefixReq.reply,
					    kvStore->readValuePrefix(readPrefixReq.key, readPrefixReq.maxLength, readPrefixReq.debugID));
				}
				when(IKVSReadRangeRequest readRangeReq = waitNext(ikvsInterface.readRange.getFuture())) {
					// TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "readRange");
					forwardPromise(
					    readRangeReq.reply,
					    kvStore->readRange(readRangeReq.keys, readRangeReq.rowLimit, readRangeReq.byteLimit));
				}
				when(IKVSGetStorageByteRequest req = waitNext(ikvsInterface.getStorageBytes.getFuture())) {
					// TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "getStorageBytes");
					StorageBytes storageBytes = kvStore->getStorageBytes();
					req.reply.send(storageBytes);
				}
				when(IKVSGetErrorRequest getFutureReq = waitNext(ikvsInterface.getError.getFuture())) {
					// TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "getError");
					forwardPromise(getFutureReq.reply, kvStore->getError());
				}
				when(IKVSOnClosedRequest onClosedReq = waitNext(ikvsInterface.onClosed.getFuture())) {
					// TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "onClosed");
					forwardPromise(onClosedReq.reply, kvStore->onClosed());
				}
				when(IKVSDisposeRequest req = waitNext(ikvsInterface.dispose.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "dispose");
					kvStore->dispose();
				}
				when(IKVSCloseRequest req = waitNext(ikvsInterface.close.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "close");
					kvStore->close();
				}
			}
		} catch (Error& e) {
			TraceEvent(SevDebug, "RemoteKVStoreError").detail("Error", e.code());
			// kvStore->close();
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			throw;
			// TODO: Error handling
		}
	}
	// return Void();
}

ACTOR Future<Void> spawnRemoteIKVS(RemoteIKeyValueStore* self, OpenKVStoreRequest openKVSReq) {
	state IKVSProcess* process = static_cast<IKVSProcess*>(g_network->global(INetwork::enIKVSProcess));
	std::cout << "initializing connection to server...\n";
	TraceEvent(SevDebug, "InitProcess").log();
	// IKVSProcess* process = g_network->global(INetwork::enIKVSProcess);
	if (process == nullptr) {
		process = new IKVSProcess();
		g_network->setGlobal(INetwork::enIKVSProcess, process);
	}
	wait(process->init());
	std::cout << "Connection established\n";
	TraceEvent(SevDebug, "RemoteKVStore")
	    .detail("Action", "sending open kv store request")
	    .detail("Filename", openKVSReq.filename)
	    .detail("StoreType", openKVSReq.storeType);
	IKVSInterface ikvsInterface = wait(process->processInterface.openKVStore.getReply(openKVSReq));
	TraceEvent(SevDebug, "IKVSInterfaceReceived").detail("UID", ikvsInterface.id());
	std::cout << "kv store opened\n";
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
	TraceEvent(SevDebug, "RemoteKVStore").detail("Action", "remote kv store opened");
	return self;
}

ACTOR Future<Void> runRemoteServer() {

	state IKVSProcessInterface processInterface;
	state ActorCollection actors(false);
	TraceEvent(SevDebug, "RemoteIKVStoreServerStarting").detail("IKVSInterfaceUID", processInterface.id());
	processInterface.getProcessInterface.makeWellKnownEndpoint(WLTOKEN_IKVS_PROCESS_SERVER,
	                                                           TaskPriority::DefaultEndpoint);
	TraceEvent(SevDebug, "RemoteIKVStoreServerStarted").log();
	std::cout << "Remote ikvs server receiving connections\n";

	loop {
		try {
			choose {
				when(GetIKVSProcessInterfaceRequest req = waitNext(processInterface.getProcessInterface.getFuture())) {
					TraceEvent(SevDebug, "RequestIKVSProcessInterface").detail("Status", "received request");
					std::cout << "received response for ikvs process interface\n";
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