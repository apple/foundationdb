#include "fdbcli/fdbcli.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace fdb_cli;

ACTOR static Future<bool> consistencycheckCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	state Reference<ITransaction> tr = db->createTransaction();
	tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	if (tokens.size() == 1) {
		Optional<Value> suspended = wait(safeThreadFutureToFuture(tr->get(consistencyCheckSpeicalKey)));
		printf("ConsistencyCheck is %s\n", suspended.present() ? "off" : "on");
	} else if (tokens.size() == 2 && tokencmp(tokens[1], "off")) {
		tr->set(consistencyCheckSpeicalKey, Value());
		wait(safeThreadFutureToFuture(tr->commit()));
	} else if (tokens.size() == 2 && tokencmp(tokens[1], "on")) {
		tr->clear(consistencyCheckSpeicalKey);
		wait(safeThreadFutureToFuture(tr->commit()));
	} else {
		printUsage(tokens[0]);
		return false;
	}
	return true;
}

namespace fdb_cli {

const KeyRef consistencyCheckSpeicalKey = LiteralStringRef("\xff\xff/management/consistency_check_suspended");

Future<bool> consistencycheckCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	return consistencycheckCommandActor(db, tokens);
}

CommandFactory consistencycheckFactory(
    "consistencycheck",
    CommandHelp(
        "consistencycheck [on|off]",
        "permits or prevents consistency checking",
        "Calling this command with `on' permits consistency check processes to run and `off' will halt their checking. "
        "Calling this command with no arguments will display if consistency checking is currently allowed.\n"));

} // namespace fdb_cli
