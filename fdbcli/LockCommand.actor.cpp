/*
 * LockCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/Knobs.h"
#include "fdbclient/Schemas.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ACTOR Future<Void> lockDatabase(Reference<IDatabase> db, UID id) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			tr->set(fdb_cli::lockSpecialKey, id.toString());
			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked)
				throw e;
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(fdb_cli::getSpecialKeysFailureErrorMessage(tr));
				if (errorMsgStr == "Database has already been locked")
					throw database_locked();
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}
}

} // namespace

namespace fdb_cli {

const KeyRef lockSpecialKey = LiteralStringRef("\xff\xff/management/db_locked");

ACTOR Future<bool> lockCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 1) {
		printUsage(tokens[0]);
		return false;
	} else {
		state UID lockUID = deterministicRandom()->randomUniqueID();
		printf("Locking database with lockUID: %s\n", lockUID.toString().c_str());
		wait((lockDatabase(db, lockUID)));
		printf("Database locked.\n");
		return true;
	}
}

ACTOR Future<Void> unlockDatabaseActor(Reference<IDatabase> db, UID uid) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			state ThreadFuture<Optional<Value>> valF = tr->get(fdb_cli::lockSpecialKey);
			Optional<Value> val = wait(safeThreadFutureToFuture(valF));

			if (!val.present())
				return Void();

			if (val.present() && UID::fromString(val.get().toString()) != uid) {
				throw database_locked();
			}

			tr->clear(fdb_cli::lockSpecialKey);
			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked)
				throw e;
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(fdb_cli::getSpecialKeysFailureErrorMessage(tr));
				if (errorMsgStr == "Database has already been locked")
					throw database_locked();
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}
}

CommandFactory lockFactory(
    "lock",
    CommandHelp("lock",
                "lock the database with a randomly generated lockUID",
                "Randomly generates a lockUID, prints this lockUID, and then uses the lockUID to lock the database."));

CommandFactory unlockFactory(
    "unlock",
    CommandHelp("unlock <UID>",
                "unlock the database with the provided lockUID",
                "Unlocks the database with the provided lockUID. This is a potentially dangerous operation, so the "
                "user will be asked to enter a passphrase to confirm their intent."));
} // namespace fdb_cli
