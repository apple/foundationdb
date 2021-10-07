#include "fdbserver/RemoteIKeyValueStore.actor.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

#include "flow/ActorCollection.h"
#include "flow/Error.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/network.h"

ACTOR Future<Void> remoteIKVSCommit(IKeyValueStore* kvStore, IKVSCommitRequest commitReq) {
	wait(kvStore->commit(commitReq.sequential));
	commitReq.reply.send(Void());
	return Void();
}

ACTOR Future<Void> runIKVS(OpenKVStoreRequest openReq, IKVSInterface ikvsInterface) {
	std::cout << "open KV store request received\n";

	// state ActorCollection actors(false); // actor causes some compilation issue, will get to it after connection
	// issue is fixed
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
				when(state IKVSGetValueRequest getReq = waitNext(ikvsInterface.getValue.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "get");
					Optional<Value> value = wait(kvStore->readValue(getReq.key, getReq.debugID));
					getReq.reply.send(value);
				}
				when(IKVSSetRequest req = waitNext(ikvsInterface.set.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "set");
					kvStore->set(req.keyValue);
				}
				when(IKVSClearRequest req = waitNext(ikvsInterface.clear.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "clear");
					kvStore->clear(req.range);
				}
				when(state IKVSCommitRequest commitReq = waitNext(ikvsInterface.commit.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "commit");
					wait(kvStore->commit(commitReq.sequential)); // temporary, will change to actors
					commitReq.reply.send(Void());
					// requestActors.add(remoteIKVSCommit(kvStore, commitReq));
				}
				when(state IKVSReadValuePrefixRequest readPrefixReq =
				         waitNext(ikvsInterface.readValuePrefix.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "readPrefix");
					Optional<Value> resultPrefix = wait(
					    kvStore->readValuePrefix(readPrefixReq.key, readPrefixReq.maxLength, readPrefixReq.debugID));
					readPrefixReq.reply.send(resultPrefix);
				}
				when(state IKVSReadRangeRequest readRangeReq = waitNext(ikvsInterface.readRange.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "readRange");
					RangeResult rangeResult =
					    wait(kvStore->readRange(readRangeReq.keys, readRangeReq.rowLimit, readRangeReq.byteLimit));
					readRangeReq.reply.send(rangeResult);
				}
				when(IKVSGetStorageByteRequest req = waitNext(ikvsInterface.getStorageBytes.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "getStorageBytes");
					StorageBytes storageBytes = kvStore->getStorageBytes();
					req.reply.send(storageBytes);
				}
				when(IKVSGetErrorRequest getFutureReq = waitNext(ikvsInterface.getError.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "getError");
					kvStore->getError();
					getFutureReq.reply.send(Void());
					// actors.add((kvStore->getError()));
					// getFutureReq.reply.send(actors.getResult());
				}
				when(IKVSOnClosedRequest onClosedReq = waitNext(ikvsInterface.onClosed.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "onClosed");
					kvStore->onClosed();
					onClosedReq.reply.send(Void());
					// actors.add(wait(kvStore->onClosed()));
					// onClosedReq.reply.send(actors.getResult());
				}
				when(IKVSDisposeRequest req = waitNext(ikvsInterface.dispose.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "dispose");
					kvStore->dispose();
					req.reply.send(Void());
				}
				when(IKVSCloseRequest req = waitNext(ikvsInterface.close.getFuture())) {
					TraceEvent(SevDebug, "RemoteKVStore").detail("Request", "close");
					kvStore->close();
					req.reply.send(Void());
				}
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

		state NetworkAddress addr = NetworkAddress::parse("127.0.0.1:28374");
		state IKVSProcessInterface ikvsProcessServer;
		ikvsProcessServer.getProcessInterface =
		    RequestStream<GetIKVSProcessInterfaceRequest>(Endpoint({ addr }, UID(-1, 2)));

		if (self->ikvsProcess.isReady()) {
			// no ikvs process is running
			state std::vector<std::string> paramList = { "bin/fdbserver", "-r", "remoteIKVS", "-p", "127.0.0.1:28374" };
			self->ikvsProcess = spawnProcess("bin/fdbserver", paramList, 20.0, false, 0.01);
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
	Optional<Value> val = wait(self->readValue(KeyRef("foo")));
	TraceEvent(SevDebug, "IKVSInterface")
	    .detail("UID", self->interf.id())
	    .detail("event", "after read")
	    .detail("value", val);

	return Void();
}

IKeyValueStore* openRemoteKVStore(KeyValueStoreType storeType,
                                  std::string const& filename,
                                  UID logID,
                                  int64_t memoryLimit,
                                  bool checkChecksums,
                                  bool checkIntegrity) {
	RemoteIKeyValueStore* self = new RemoteIKeyValueStore();
	// pass storetype, filename, etc and pass it through the OpenKVStoreRequest
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