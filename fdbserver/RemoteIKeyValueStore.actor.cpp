#include "fdbserver/RemoteIKeyValueStore.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

#include "flow/ActorCollection.h"
#include "flow/Error.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Void> runRemoteServer() {

	state IKVSProcessInterface processInterface;
	state ActorCollection actors(false);
	processInterface.getProcessInterface.makeWellKnownEndpoint(WLTOKEN_FIRST_AVAILABLE, TaskPriority::DefaultEndpoint);
	std::cout << "Remote ikvs server receiving connections\n";

	loop {
		try {
			choose {
				when(GetIKVSProcessInterfaceRequest req = waitNext(processInterface.getProcessInterface.getFuture())) {
					std::cout << "received response for running remote IKVS server\n";
					req.reply.send(processInterface);
				}
				when(OpenKVStoreRequest req = waitNext(processInterface.openKVStore.getFuture())) {
					// call runIKVS()
					std::cout << "received remote ikvs get value request\n";
					// actors.add(runIKVS(req));
				}
				when(wait(actors.getResult())) {
					// add futures, if any throw exception, throw on the wait get result
					UNSTOPPABLE_ASSERT(false);
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			throw;
			// TODO: Error handling
		}
	}
}