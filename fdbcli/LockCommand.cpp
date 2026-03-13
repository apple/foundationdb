/*
 * LockCommand.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbcli/fdbcli.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/Schemas.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
namespace {

Future<bool> lockDatabase(Reference<IDatabase> db, UID id) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		Error err;
		try {
			tr->set(fdb_cli::lockSpecialKey, id.toString());
			co_await safeThreadFutureToFuture(tr->commit());
			printf("Database locked.\n");
			co_return true;
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_database_locked)
			throw err;
		if (err.code() == error_code_special_keys_api_failure) {
			std::string errorMsgStr = co_await fdb_cli::getSpecialKeysFailureErrorMessage(tr);
			fprintf(stderr, "%s\n", errorMsgStr.c_str());
			co_return false;
		}
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

} // namespace

namespace fdb_cli {

const KeyRef lockSpecialKey = "\xff\xff/management/db_locked"_sr;

Future<bool> lockCommandActor(Reference<IDatabase> db, std::vector<StringRef> const& tokens) {
	if (tokens.size() != 1) {
		printUsage(tokens[0]);
		co_return false;
	} else {
		UID lockUID = deterministicRandom()->randomUniqueID();
		printf("Locking database with lockUID: %s\n", lockUID.toString().c_str());
		bool result = co_await lockDatabase(db, lockUID);
		co_return result;
	}
}

Future<bool> unlockDatabaseActor(Reference<IDatabase> db, UID uid) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		Error err;
		try {
			ThreadFuture<Optional<Value>> valF = tr->get(fdb_cli::lockSpecialKey);
			Optional<Value> val = co_await safeThreadFutureToFuture(valF);

			if (!val.present())
				co_return true;

			if (val.present() && UID::fromString(val.get().toString()) != uid) {
				printf("Unable to unlock database. Make sure to unlock with the correct lock UID.\n");
				co_return false;
			}

			tr->clear(fdb_cli::lockSpecialKey);
			co_await safeThreadFutureToFuture(tr->commit());
			printf("Database unlocked.\n");
			co_return true;
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_special_keys_api_failure) {
			std::string errorMsgStr = co_await fdb_cli::getSpecialKeysFailureErrorMessage(tr);
			fprintf(stderr, "%s\n", errorMsgStr.c_str());
			co_return false;
		}
		co_await safeThreadFutureToFuture(tr->onError(err));
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
