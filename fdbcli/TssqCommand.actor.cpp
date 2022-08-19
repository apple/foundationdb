/*
 * TssqCommand.actor.cpp
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
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/SystemData.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ACTOR Future<Void> tssQuarantineList(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			// Hold the reference to the standalone's memory
			state ThreadFuture<RangeResult> resultFuture = tr->getRange(tssQuarantineKeys, CLIENT_KNOBS->TOO_MANY);
			RangeResult result = wait(safeThreadFutureToFuture(resultFuture));
			// shouldn't have many quarantined TSSes
			ASSERT(!result.more);
			printf("Found %d quarantined TSS processes%s\n", result.size(), result.size() == 0 ? "." : ":");
			for (auto& it : result) {
				printf("  %s\n", decodeTssQuarantineKey(it.key).toString().c_str());
			}
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<bool> tssQuarantine(Reference<IDatabase> db, bool enable, UID tssId) {
	state Reference<ITransaction> tr = db->createTransaction();
	state KeyBackedMap<UID, UID> tssMapDB = KeyBackedMap<UID, UID>(tssMappingKeys.begin);

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			// Do some validation first to make sure the command is valid
			// hold the returned standalone object's memory
			state ThreadFuture<Optional<Value>> serverListValueF = tr->get(serverListKeyFor(tssId));
			Optional<Value> serverListValue = wait(safeThreadFutureToFuture(serverListValueF));
			if (!serverListValue.present()) {
				printf("No TSS %s found in cluster!\n", tssId.toString().c_str());
				return false;
			}
			state StorageServerInterface ssi = decodeServerListValue(serverListValue.get());
			if (!ssi.isTss()) {
				printf("Cannot quarantine Non-TSS storage ID %s!\n", tssId.toString().c_str());
				return false;
			}

			// hold the returned standalone object's memory
			state ThreadFuture<Optional<Value>> currentQuarantineValueF = tr->get(tssQuarantineKeyFor(tssId));
			Optional<Value> currentQuarantineValue = wait(safeThreadFutureToFuture(currentQuarantineValueF));
			if (enable && currentQuarantineValue.present()) {
				printf("TSS %s already in quarantine, doing nothing.\n", tssId.toString().c_str());
				return false;
			} else if (!enable && !currentQuarantineValue.present()) {
				printf("TSS %s is not in quarantine, cannot remove from quarantine!.\n", tssId.toString().c_str());
				return false;
			}

			if (enable) {
				tr->set(tssQuarantineKeyFor(tssId), LiteralStringRef(""));
				// remove server from TSS mapping when quarantine is enabled
				tssMapDB.erase(tr, ssi.tssPairID.get());
			} else {
				tr->clear(tssQuarantineKeyFor(tssId));
			}

			wait(safeThreadFutureToFuture(tr->commit()));
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
	printf("Successfully %s TSS %s\n", enable ? "quarantined" : "removed", tssId.toString().c_str());
	return true;
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> tssqCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() == 2) {
		if (tokens[1] != LiteralStringRef("list")) {
			printUsage(tokens[0]);
			return false;
		} else {
			wait(tssQuarantineList(db));
		}
	} else if (tokens.size() == 3) {
		if ((tokens[1] != LiteralStringRef("start") && tokens[1] != LiteralStringRef("stop")) ||
		    (tokens[2].size() != 32) || !std::all_of(tokens[2].begin(), tokens[2].end(), &isxdigit)) {
			printUsage(tokens[0]);
			return false;
		} else {
			bool enable = tokens[1] == LiteralStringRef("start");
			UID tssId = UID::fromString(tokens[2].toString());
			bool success = wait(tssQuarantine(db, enable, tssId));
			return success;
		}
	} else {
		printUsage(tokens[0]);
		return false;
	}
	return true;
}

CommandFactory tssqFactory(
    "tssq",
    CommandHelp("tssq start|stop <StorageUID>",
                "start/stop tss quarantine",
                "Toggles Quarantine mode for a Testing Storage Server. Quarantine will happen automatically if the "
                "TSS is detected to have incorrect data, but can also be initiated manually. You can also remove a "
                "TSS from quarantine once your investigation is finished, which will destroy the TSS process."));

} // namespace fdb_cli
