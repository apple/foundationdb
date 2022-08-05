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

ACTOR Future<Void> doBlobPurge(Database db, Key startKey, Key endKey, Optional<Version> version) {
	state Version purgeVersion;
	if (version.present()) {
		purgeVersion = version.get();
	} else {
		wait(store(purgeVersion, getLatestReadVersion(db)));
	}

	state Key purgeKey = wait(db->purgeBlobGranules(KeyRange(KeyRangeRef(startKey, endKey)), purgeVersion, {}));

	fmt::print("Blob purge registered for [{0} - {1}) @ {2}\n", startKey.printable(), endKey.printable(), purgeVersion);

	state Future<Void> printWarningActor = printAfterDelay(
	    5.0, "Waiting for purge to complete. (interrupting this wait with CTRL+C will not cancel the purge)");
	wait(db->waitPurgeGranulesComplete(purgeKey));

	fmt::print("Blob purge complete for [{0} - {1}) @ {2}\n", startKey.printable(), endKey.printable(), purgeVersion);

	return Void();
}

ACTOR Future<Version> checkBlobSubrange(Database db, KeyRange keyRange, Optional<Version> version) {
	state Transaction tr(db);
	state Version readVersionOut = invalidVersion;
	loop {
		try {
			wait(success(tr.readBlobGranules(keyRange, 0, version, &readVersionOut)));
			return readVersionOut;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> doBlobCheck(Database db, Key startKey, Key endKey, Optional<Version> version) {
	state Transaction tr(db);
	state Version readVersionOut = invalidVersion;
	state double elapsed = -timer_monotonic();
	state KeyRange range = KeyRange(KeyRangeRef(startKey, endKey));
	state Standalone<VectorRef<KeyRangeRef>> allRanges;
	loop {
		try {
			wait(store(allRanges, tr.getBlobGranuleRanges(range)));
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	if (allRanges.empty()) {
		fmt::print("ERROR: No blob ranges for [{0} - {1})\n", startKey.printable(), endKey.printable());
		return Void();
	}
	fmt::print("Loaded {0} blob ranges to check\n", allRanges.size());
	state std::vector<Future<Version>> checkParts;
	// Chunk up to smaller ranges than this limit. Must be smaller than BG_TOO_MANY_GRANULES to not hit the limit
	int maxChunkSize = CLIENT_KNOBS->BG_TOO_MANY_GRANULES / 2;
	KeyRange currentChunk;
	int currentChunkSize = 0;
	for (auto& it : allRanges) {
		if (currentChunkSize == maxChunkSize) {
			checkParts.push_back(checkBlobSubrange(db, currentChunk, version));
			currentChunkSize = 0;
		}
		if (currentChunkSize == 0) {
			currentChunk = it;
		} else if (it.begin != currentChunk.end) {
			fmt::print("ERROR: Blobrange check failed, gap in blob ranges from [{0} - {1})\n",
			           currentChunk.end.printable(),
			           it.begin.printable());
			return Void();
		} else {
			currentChunk = KeyRangeRef(currentChunk.begin, it.end);
		}
		currentChunkSize++;
	}
	checkParts.push_back(checkBlobSubrange(db, currentChunk, version));

	wait(waitForAll(checkParts));
	readVersionOut = checkParts.back().get();

	elapsed += timer_monotonic();

	fmt::print("Blob check complete for [{0} - {1}) @ {2} in {3:.6f} seconds\n",
	           startKey.printable(),
	           endKey.printable(),
	           readVersionOut,
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

	if (end > LiteralStringRef("\xff")) {
		// TODO is this something we want?
		fmt::print("Cannot blobbify system keyspace! Problematic End Key: {0}\n", tokens[3].printable());
		return false;
	} else if (tokens[2] >= tokens[3]) {
		fmt::print("Invalid blob range [{0} - {1})\n", tokens[2].printable(), tokens[3].printable());
	} else {
		if (tokencmp(tokens[1], "start") || tokencmp(tokens[1], "stop")) {
			bool starting = tokencmp(tokens[1], "start");
			if (tokens.size() > 4) {
				printUsage(tokens[0]);
				return false;
			}
			fmt::print("{0} blobbify range for [{1} - {2})\n",
			           starting ? "Starting" : "Stopping",
			           tokens[2].printable().c_str(),
			           tokens[3].printable().c_str());
			wait(setBlobRange(localDb, begin, end, starting ? LiteralStringRef("1") : StringRef()));
		} else if (tokencmp(tokens[1], "purge") || tokencmp(tokens[1], "check")) {
			bool purge = tokencmp(tokens[1], "purge");

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

			fmt::print("{0} blob range [{1} - {2})",
			           purge ? "Purging" : "Checking",
			           tokens[2].printable(),
			           tokens[3].printable());
			if (version.present()) {
				fmt::print(" @ {0}", version.get());
			}
			fmt::print("\n");

			if (purge) {
				wait(doBlobPurge(localDb, begin, end, version));
			} else {
				wait(doBlobCheck(localDb, begin, end, version));
			}
		} else {
			printUsage(tokens[0]);
			return false;
		}
	}
	return true;
}

CommandFactory blobRangeFactory("blobrange",
                                CommandHelp("blobrange <start|stop|purge|check> <startkey> <endkey> [version]",
                                            "",
                                            ""));
} // namespace fdb_cli
