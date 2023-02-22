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
#include "fdbclient/NativeAPI.actor.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ACTOR Future<Version> getLatestReadVersion(Database db) {
	state Transaction tr(db);
	loop {
		try {
			Version rv = wait(tr.getReadVersion());
			fmt::print("Resolved latest read version as {0}\n", rv);
			return rv;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// print after delay if not cancelled
ACTOR Future<Void> printAfterDelay(double delaySeconds, std::string message) {
	wait(delay(delaySeconds));
	fmt::print("{}\n", message);
	return Void();
}

ACTOR Future<Void> doBlobPurge(Database db, Key startKey, Key endKey, Optional<Version> version, bool force) {
	state Version purgeVersion;
	if (version.present()) {
		purgeVersion = version.get();
	} else {
		wait(store(purgeVersion, getLatestReadVersion(db)));
	}

	state Key purgeKey = wait(db->purgeBlobGranules(KeyRange(KeyRangeRef(startKey, endKey)), purgeVersion, {}, force));

	fmt::print("Blob purge registered for [{0} - {1}) @ {2}\n", startKey.printable(), endKey.printable(), purgeVersion);

	state Future<Void> printWarningActor = printAfterDelay(
	    5.0, "Waiting for purge to complete. (interrupting this wait with CTRL+C will not cancel the purge)");
	wait(db->waitPurgeGranulesComplete(purgeKey));

	fmt::print("Blob purge complete for [{0} - {1}) @ {2}\n", startKey.printable(), endKey.printable(), purgeVersion);

	return Void();
}

ACTOR Future<Void> doBlobCheck(Database db, Key startKey, Key endKey, Optional<Version> version) {
	state double elapsed = -timer_monotonic();

	state Version readVersionOut = wait(db->verifyBlobRange(KeyRangeRef(startKey, endKey), version));

	elapsed += timer_monotonic();

	fmt::print("Blob check complete for [{0} - {1}) @ {2} in {3:.6f} seconds\n",
	           startKey.printable(),
	           endKey.printable(),
	           readVersionOut,
	           elapsed);
	return Void();
}

ACTOR Future<Void> doBlobFlush(Database db, Key startKey, Key endKey, Optional<Version> version, bool compact) {
	state double elapsed = -timer_monotonic();
	state KeyRange keyRange(KeyRangeRef(startKey, endKey));
	bool result = wait(db->flushBlobRange(keyRange, compact, version));
	elapsed += timer_monotonic();

	fmt::print("Blob Flush [{0} - {1}) {2} in {3:.6f} seconds\n",
	           startKey.printable(),
	           endKey.printable(),
	           result ? "succeeded" : "failed",
	           elapsed);

	return Void();
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> blobRangeCommandActor(Database localDb,
                                         Optional<TenantMapEntry> tenantEntry,
                                         std::vector<StringRef> tokens) {
	// enables blob writing for the given range
	if (tokens.size() != 4 && tokens.size() != 5) {
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

	if (end > "\xff"_sr) {
		// TODO is this something we want?
		fmt::print("Cannot blobbify system keyspace! Problematic End Key: {0}\n", tokens[3].printable());
		return false;
	} else if (tokens[2] >= tokens[3]) {
		fmt::print("Invalid blob range [{0} - {1})\n", tokens[2].printable(), tokens[3].printable());
	} else {
		if (tokencmp(tokens[1], "start") || tokencmp(tokens[1], "stop")) {
			state bool starting = tokencmp(tokens[1], "start");
			if (tokens.size() > 4) {
				printUsage(tokens[0]);
				return false;
			}
			fmt::print("{0} blobbify range for [{1} - {2})\n",
			           starting ? "Starting" : "Stopping",
			           tokens[2].printable(),
			           tokens[3].printable());
			state bool success = false;
			if (starting) {
				wait(store(success, localDb->blobbifyRange(KeyRangeRef(begin, end))));
			} else {
				wait(store(success, localDb->unblobbifyRange(KeyRangeRef(begin, end))));
			}
			if (success) {
				fmt::print("{0} updated blob range [{1} - {2}) succeeded\n",
				           starting ? "Starting" : "Stopping",
				           tokens[2].printable(),
				           tokens[3].printable());
			} else {
				fmt::print("{0} blobbify range for [{1} - {2}) failed\n",
				           starting ? "Starting" : "Stopping",
				           tokens[2].printable(),
				           tokens[3].printable());
			}
			return success;
		} else if (tokencmp(tokens[1], "purge") || tokencmp(tokens[1], "forcepurge") || tokencmp(tokens[1], "check") ||
		           tokencmp(tokens[1], "flush") || tokencmp(tokens[1], "compact")) {
			bool purge = tokencmp(tokens[1], "purge") || tokencmp(tokens[1], "forcepurge");
			bool forcePurge = tokencmp(tokens[1], "forcepurge");

			Optional<Version> version;
			if (tokens.size() > 4) {
				Version v;
				int n = 0;
				if (sscanf(tokens[4].toString().c_str(), "%" PRId64 "%n", &v, &n) != 1 || n != tokens[4].size()) {
					printUsage(tokens[0]);
					return false;
				}
				version = v;
			}

			fmt::print("{0} blob range [{1} - {2}){3}",
			           purge ? "Purging" : "Checking",
			           tokens[2].printable(),
			           tokens[3].printable(),
			           forcePurge ? " (force)" : "");
			if (version.present()) {
				fmt::print(" @ {0}", version.get());
			}
			fmt::print("\n");

			if (purge) {
				wait(doBlobPurge(localDb, begin, end, version, forcePurge));
			} else {
				if (tokencmp(tokens[1], "check")) {
					wait(doBlobCheck(localDb, begin, end, version));
				} else if (tokencmp(tokens[1], "flush")) {
					wait(doBlobFlush(localDb, begin, end, version, false));
				} else if (tokencmp(tokens[1], "compact")) {
					wait(doBlobFlush(localDb, begin, end, version, true));
				} else {
					ASSERT(false);
				}
			}
		} else {
			printUsage(tokens[0]);
			return false;
		}
	}
	return true;
}

CommandFactory blobRangeFactory(
    "blobrange",
    CommandHelp("blobrange <start|stop|check|purge|forcepurge|flush|compact> <startkey> <endkey> [version]", "", ""));
} // namespace fdb_cli
