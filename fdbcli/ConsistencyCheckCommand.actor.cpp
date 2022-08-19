/*
 * ConsistencyCheckCommand.actor.cpp
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

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

const KeyRef consistencyCheckSpecialKey = LiteralStringRef("\xff\xff/management/consistency_check_suspended");

ACTOR Future<bool> consistencyCheckCommandActor(Reference<ITransaction> tr,
                                                std::vector<StringRef> tokens,
                                                bool intrans) {
	// Here we do not proceed in a try-catch loop since the transaction is always supposed to succeed.
	// If not, the outer loop catch block(fdbcli.actor.cpp) will handle the error and print out the error message
	tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	if (tokens.size() == 1) {
		// hold the returned standalone object's memory
		state ThreadFuture<Optional<Value>> suspendedF = tr->get(consistencyCheckSpecialKey);
		Optional<Value> suspended = wait(safeThreadFutureToFuture(suspendedF));
		printf("ConsistencyCheck is %s\n", suspended.present() ? "off" : "on");
	} else if (tokens.size() == 2 && tokencmp(tokens[1], "off")) {
		tr->set(consistencyCheckSpecialKey, Value());
		if (!intrans)
			wait(safeThreadFutureToFuture(tr->commit()));
	} else if (tokens.size() == 2 && tokencmp(tokens[1], "on")) {
		tr->clear(consistencyCheckSpecialKey);
		if (!intrans)
			wait(safeThreadFutureToFuture(tr->commit()));
	} else {
		printUsage(tokens[0]);
		return false;
	}
	return true;
}

CommandFactory consistencyCheckFactory(
    "consistencycheck",
    CommandHelp(
        "consistencycheck [on|off]",
        "permits or prevents consistency checking",
        "Calling this command with `on' permits consistency check processes to run and `off' will halt their checking. "
        "Calling this command with no arguments will display if consistency checking is currently allowed.\n"));

} // namespace fdb_cli
