#include "fdbserver/RemoteIKeyValueStore.actor.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

#include "fdbserver/Knobs.h"
#include "flow/ActorCollection.h"
#include "flow/Error.h"
#include "flow/Platform.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/network.h"
#include "flow/Platform.h"
#include <string>

ACTOR Future<Void> runIKVS(OpenKVStoreRequest openReq, IKVSInterface ikvsInterface) {
	state ActorCollection actors(false);
	std::cout << "open KV store request received\n";
	TraceEvent(SevDebug, "RemoteKVStore").detail("Action", "initializing local store");
	state IKeyValueStore* kvStore = openKVStore(openReq.storeType,
	                                            openReq.filename,
	                                            openReq.logID,
	                                            openReq.memoryLimit,
	                                            openReq.checkChecksums,
	                                            openReq.checkIntegrity);
	wait(kvStore->init());
	openReq.reply.send(ikvsInterface);
	TraceEvent(SevDebug, "RemoteKVStore").detail("Action", "local store initialized");

	loop {
		try {
			choose {
				when(IKVSGetValueRequest getReq = waitNext(ikvsInterface.getValue.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "get");
					forwardPromise(getReq.reply, kvStore->readValue(getReq.key, getReq.debugID));
				}
				when(IKVSSetRequest req = waitNext(ikvsInterface.set.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "set");
					kvStore->set(req.keyValue);
				}
				when(IKVSClearRequest req = waitNext(ikvsInterface.clear.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "clear");
					kvStore->clear(req.range);
				}
				when(IKVSCommitRequest commitReq = waitNext(ikvsInterface.commit.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "commit");
					forwardPromise(commitReq.reply, kvStore->commit(commitReq.sequential));
				}
				when(IKVSReadValuePrefixRequest readPrefixReq = waitNext(ikvsInterface.readValuePrefix.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "readPrefix");
					forwardPromise(
					    readPrefixReq.reply,
					    kvStore->readValuePrefix(readPrefixReq.key, readPrefixReq.maxLength, readPrefixReq.debugID));
				}
				when(IKVSReadRangeRequest readRangeReq = waitNext(ikvsInterface.readRange.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "readRange");
					forwardPromise(
					    readRangeReq.reply,
					    kvStore->readRange(readRangeReq.keys, readRangeReq.rowLimit, readRangeReq.byteLimit));
				}
				when(IKVSGetStorageByteRequest req = waitNext(ikvsInterface.getStorageBytes.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "getStorageBytes");
					StorageBytes storageBytes = kvStore->getStorageBytes();
					req.reply.send(storageBytes);
				}
				when(IKVSGetErrorRequest getFutureReq = waitNext(ikvsInterface.getError.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "getError");
					forwardPromise(getFutureReq.reply, kvStore->getError());
				}
				when(IKVSOnClosedRequest onClosedReq = waitNext(ikvsInterface.onClosed.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "onClosed");
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
				when(wait(actors.getResult())) { UNSTOPPABLE_ASSERT(false); }
			}
		} catch (Error& e) {
			TraceEvent(SevDebug, "RemoteKVStoreError").detail("error", e.code());
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			throw;
			// TODO: Error handling
		}
	}
}

namespace {

struct IKVSProcess {
	IKVSProcessInterface processInterface; // do this in the caller
	Future<int> ikvsProcess = 0; // do this in the caller

	Future<Void> init() { return init(this); }

private:
	// TODO: rename to initializeIKVSProcess
	ACTOR static Future<Void> init(IKVSProcess* self) {
		std::string ikvsAddrStr = "127.0.0.1:";
		std::string ikvsPortStr = std::to_string(SERVER_KNOBS->IKVS_PORT);
		ikvsAddrStr.append(ikvsPortStr);
		state NetworkAddress addr = NetworkAddress::parse(ikvsAddrStr);
		state IKVSProcessInterface ikvsProcessServer;
		ikvsProcessServer.getProcessInterface =
		    RequestStream<GetIKVSProcessInterfaceRequest>(Endpoint({ addr }, UID(-1, 2)));

		if (self->ikvsProcess.isReady()) {
			// no ikvs process is running
			state std::string absExecPath = abspath(getExecPath());
			state std::vector<std::string> paramList = {
				absExecPath,        "-r",       "remoteIKVS", "-p", ikvsAddrStr, "--knob_min_trace_severity", "1",
				"--knob_ikvs_port", ikvsPortStr
			};
			self->ikvsProcess = spawnProcess(absExecPath, paramList, 20.0, false, 0.01);
			std::cout << "creating new endpoint\n";
			IKVSProcessInterface processInterface =
			    wait(ikvsProcessServer.getProcessInterface.getReply(GetIKVSProcessInterfaceRequest()));
			std::cout << "new endpoint created\n";
			self->processInterface = processInterface; // because flow is stupid
		}
		return Void();
	}
};

IKVSProcess process;

} // namespace

ACTOR Future<Void> spawnRemoteIKVS(RemoteIKeyValueStore* self, OpenKVStoreRequest openKVSReq) {

	std::cout << "initializing connection to server...\n";
	wait(process.init());
	std::cout << "Connection established\n";
	TraceEvent(SevDebug, "RemoteKVStore")
	    .detail("Action", "sending open kv store request")
	    .detail("filename", openKVSReq.filename)
	    .detail("storeType", openKVSReq.storeType);
	IKVSInterface ikvsInterface = wait(process.processInterface.openKVStore.getReply(openKVSReq));
	TraceEvent(SevDebug, "IKVSInterface").detail("UID", ikvsInterface.id());
	std::cout << "kv store opened\n";
	self->interf = ikvsInterface;
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
	TraceEvent(SevDebug, "RemoteIKVStoreServerStarting").log();
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
					IKVSInterface ikvsInterf(req.storeType);
					TraceEvent(SevDebug, "IKVSInterface").detail("UID", ikvsInterf.id());
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