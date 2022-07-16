/*
 * BlobRangeCommand.actor.cpp
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
#include "fdbclient/ManagementAPI.actor.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

// copy to standalones for krm
ACTOR Future<Void> setBlobRange(Database db, Key startKey, Key endKey, Value value) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			// FIXME: check that the set range is currently inactive, and that a revoked range is currently its own
			// range in the map and fully set.

			tr->set(blobRangeChangeKey, deterministicRandom()->randomUniqueID().toString());
			// This is not coalescing because we want to keep each range logically separate.
			wait(krmSetRange(tr, blobRangeKeys.begin, KeyRange(KeyRangeRef(startKey, endKey)), value));
			wait(tr->commit());
			printf("Successfully updated blob range [%s - %s) to %s\n",
			       startKey.printable().c_str(),
			       endKey.printable().c_str(),
			       value.printable().c_str());
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> blobRangeCommandActor(Database localDb,
                                         Optional<TenantMapEntry> tenantEntry,
                                         std::vector<StringRef> tokens) {
	// enables blob writing for the given range
	if (tokens.size() != 4) {
		printUsage(tokens[0]);
		return false;
	}

	Key begin;
	Key end;

	if (tenantEntry.present()) {
		begin = tokens[2].withPrefix(tenantEntry.get().prefix);
		end = tokens[3].withPrefix(tenantEntry.get().prefix);
	} else {
		begin = tokens[2];
		end = tokens[3];
	}

	if (end > LiteralStringRef("\xff")) {
		// TODO is this something we want?
		printf("Cannot blobbify system keyspace! Problematic End Key: %s\n", tokens[3].printable().c_str());
		return false;
	} else if (tokens[2] >= tokens[3]) {
		printf("Invalid blob range [%s - %s)\n", tokens[2].printable().c_str(), tokens[3].printable().c_str());
	} else {
		if (tokencmp(tokens[1], "start")) {
			printf("Starting blobbify range for [%s - %s)\n",
			       tokens[2].printable().c_str(),
			       tokens[3].printable().c_str());
			wait(setBlobRange(localDb, begin, end, LiteralStringRef("1")));
		} else if (tokencmp(tokens[1], "stop")) {
			printf("Stopping blobbify range for [%s - %s)\n",
			       tokens[2].printable().c_str(),
			       tokens[3].printable().c_str());
			wait(setBlobRange(localDb, begin, end, StringRef()));
		} else {
			printUsage(tokens[0]);
			printf("Usage: blobrange <start|stop> <startkey> <endkey>");
			return false;
		}
	}
	return true;
}

CommandFactory blobRangeFactory("blobrange", CommandHelp("blobrange <start|stop> <startkey> <endkey>", "", ""));
} // namespace fdb_cli
