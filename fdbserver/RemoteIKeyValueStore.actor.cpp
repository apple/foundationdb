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

	TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "initializing local store");
	state IKeyValueStore* kvStore = openKVStore(openReq.storeType,
	                                            openReq.filename,
	                                            openReq.logID,
	                                            openReq.memoryLimit,
	                                            openReq.checkChecksums,
	                                            openReq.checkIntegrity);
	TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "local store initialized");
	// ikvsInterface.initializeEndpoints();

	loop {
		try {
			choose {
				when(state IKVSGetValueRequest getReq = waitNext(ikvsInterface.getValue.getFuture())) {
					// std::cout << "get value request received\n";

					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Request", "get");
					Optional<Value> value = wait(kvStore->readValue(getReq.key, getReq.debugID));
					getReq.reply.send(value);
				}
				when(IKVSSetRequest req = waitNext(ikvsInterface.set.getFuture())) {
					// std::cout << "set value request received\n";
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Request", "set");
					kvStore->set(req.keyValue);
				}
				when(IKVSClearRequest req = waitNext(ikvsInterface.clear.getFuture())) {
					// std::cout << "clear request received\n";
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Request", "clear");
					kvStore->clear(req.range);
				}
				when(state IKVSCommitRequest commitReq = waitNext(ikvsInterface.commit.getFuture())) {
					// std::cout << "commit request received\n";
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Request", "commit");
					// dont do this: wait(kvStore->commit(commitReq.sequential));
					wait(kvStore->commit(commitReq.sequential));
					commitReq.reply.send(Void());
					// requestActors.add(remoteIKVSCommit(kvStore, commitReq));
					// commitReq.reply.send(actors.getResult());
				}
				when(state IKVSReadValuePrefixRequest readPrefixReq =
				         waitNext(ikvsInterface.readValuePrefix.getFuture())) {
					// std::cout << "get prefix request received\n";
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Request", "readPrefix");
					Optional<Value> resultPrefix = wait(
					    kvStore->readValuePrefix(readPrefixReq.key, readPrefixReq.maxLength, readPrefixReq.debugID));
					readPrefixReq.reply.send(resultPrefix);
				}
				when(state IKVSReadRangeRequest readRangeReq = waitNext(ikvsInterface.readRange.getFuture())) {
					// std::cout << "read range request received\n";
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Request", "readRange");
					RangeResult rangeResult =
					    wait(kvStore->readRange(readRangeReq.keys, readRangeReq.rowLimit, readRangeReq.byteLimit));
					readRangeReq.reply.send(rangeResult);
				}
				when(IKVSGetStorageByteRequest req = waitNext(ikvsInterface.getStorageBytes.getFuture())) {
					// std::cout << "get storage byte request received\n";
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Request", "getStorageBytes");
					StorageBytes storageBytes = kvStore->getStorageBytes();
					req.reply.send(storageBytes);
				}
				when(IKVSGetErrorRequest getFutureReq = waitNext(ikvsInterface.getError.getFuture())) {
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Request", "getError");
					kvStore->getError();
					getFutureReq.reply.send(Void());
					// actors.add((kvStore->getError()));
					// getFutureReq.reply.send(actors.getResult());
				}
				when(IKVSOnClosedRequest onClosedReq = waitNext(ikvsInterface.onClosed.getFuture())) {
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Request", "onClosed");
					kvStore->onClosed();
					onClosedReq.reply.send(Void());
					// actors.add(wait(kvStore->onClosed()));
					// onClosedReq.reply.send(actors.getResult());
				}
				when(IKVSDisposeRequest req = waitNext(ikvsInterface.dispose.getFuture())) {
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Request", "dispose");
					kvStore->dispose();
					req.reply.send(Void());
				}
				when(IKVSCloseRequest req = waitNext(ikvsInterface.close.getFuture())) {
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Request", "close");
					kvStore->close();
					req.reply.send(Void());
				}
				// when(wait(requestActors.getResult())) {}
			}
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "RemoteKVStoreError").detail("error", e.code());
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
		// IKVSProcessInterface processInterface =
		//     wait(ikvsProcessServer.getProcessInterface.getReply(GetIKVSProcessInterfaceRequest()));
		// std::cout << "new endpoint created\n";
		// self->processInterface = processInterface; // because flow is stupid
		return Void();
	}
};

IKVSProcess process;

} // namespace

ACTOR Future<Void> spawnRemoteIKVS(RemoteIKeyValueStore* self, OpenKVStoreRequest openKVSReq) {

	std::cout << "initializing connection to server...\n";
	wait(process.init());
	std::cout << "Connection established\n";
	TraceEvent(SevWarnAlways, "RemoteKVStore")
	    .detail("Action", "sending open kv store request")
	    .detail("filename", openKVSReq.filename)
	    .detail("storeType", openKVSReq.storeType);
	IKVSInterface ikvsInterface = wait(process.processInterface.openKVStore.getReply(openKVSReq));
	TraceEvent(SevWarnAlways, "IKVSInterface").detail("UID", ikvsInterface.id()).detail("tempID", ikvsInterface.tempId);
	std::cout << "kv store opened\n";
	self->interf = ikvsInterface;
	// wait(self->commit());

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
	TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote kv store opened");
	return self;
}

IKeyValueStore* openRemoteKVStoreTemp() {
	RemoteIKeyValueStore* self = new RemoteIKeyValueStore();
	// pass storetype, filename, etc and pass it through the OpenKVStoreRequest
	self->initialized = spawnRemoteIKVS(self, OpenKVStoreRequest{});
	return self;
}

ACTOR Future<Void> runRemoteServer() {

	state IKVSProcessInterface processInterface;
	state ActorCollection actors(false);
	TraceEvent(SevWarnAlways, "RemoteIKVStoreServerStarting").log();
	processInterface.getProcessInterface.makeWellKnownEndpoint(WLTOKEN_IKVS_PROCESS_SERVER,
	                                                           TaskPriority::DefaultEndpoint);
	TraceEvent(SevWarnAlways, "RemoteIKVStoreServerStarted").log();
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
					ikvsInterf.tempId = "28374";
					TraceEvent(SevWarnAlways, "IKVSInterface").detail("UID", ikvsInterf.id());
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "opening local store");
					// actors.add(runIKVS(req, ikvsInterf));
					TraceEvent(SevWarnAlways, "RemoteKVStore")
					    .detail("Action", "local store opened, sending interface");
					req.reply.send(ikvsInterf);
					TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "interface is sent");
					wait(runIKVS(req, ikvsInterf));
					TraceEvent(SevWarnAlways, "RemoteIKVStoreServerError").detail("Action", "run ikvs finished");
				}
				when(wait(actors.getResult())) {
					TraceEvent(SevWarnAlways, "RemoteIKVStoreServerError").detail("Action", "actors got result");
					// add futures, if any throw exception, throw on the wait get result
					UNSTOPPABLE_ASSERT(false);
				}
			}
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "RemoteIKVStoreServerError").detail("Error", e.code());
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			throw;
			// TODO: Error handling
		}
	}
}