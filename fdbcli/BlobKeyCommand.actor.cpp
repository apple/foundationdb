/*
 * BlobKeyCommand.actor.cpp
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

ACTOR Future<bool> printBlobHistory(Database db, Key key, Optional<Version> version) {
	fmt::print("Printing blob history for {0}", key.printable());
	if (version.present()) {
		fmt::print(" @ {0}", version.get());
	}
	fmt::print("\n");

	state Transaction tr(db);
	state KeyRange activeGranule;
	state KeyRange queryRange(KeyRangeRef(key, keyAfter(key)));
	loop {
		try {
			Standalone<VectorRef<KeyRangeRef>> granules = wait(tr.getBlobGranuleRanges(queryRange, 2));
			if (granules.empty()) {
				fmt::print("No active granule for {0}\n", key.printable());
				return false;
			}
			ASSERT(granules.size() == 1);
			activeGranule = granules[0];
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	fmt::print("Active granule: [{0} - {1})\n", activeGranule.begin.printable(), activeGranule.end.printable());

	// get latest history entry for range
	state GranuleHistory history;
	loop {
		try {
			RangeResult result =
			    wait(tr.getRange(blobGranuleHistoryKeyRangeFor(activeGranule), 1, Snapshot::False, Reverse::True));
			ASSERT(result.size() <= 1);

			if (result.empty()) {
				fmt::print("No history entry found\n");
				return true;
			}

			std::pair<KeyRange, Version> decodedKey = decodeBlobGranuleHistoryKey(result[0].key);
			ASSERT(activeGranule == decodedKey.first);
			history = GranuleHistory(activeGranule, decodedKey.second, decodeBlobGranuleHistoryValue(result[0].value));

			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	fmt::print("History:\n\n");
	loop {
		// print history
		std::string boundaryChangeAction;
		if (history.value.parentVersions.empty()) {
			boundaryChangeAction = "root";
		} else if (history.value.parentVersions.size() == 1) {
			boundaryChangeAction = "split";
		} else {
			boundaryChangeAction = "merge";
		}
		fmt::print("{0}) {1}\n\t{2}\n\t{3}\n({4})\n\n",
		           history.version,
		           history.value.granuleID.toString(),
		           history.range.begin.printable(),
		           history.range.end.printable(),
		           boundaryChangeAction);
		// traverse back

		if (history.value.parentVersions.empty() || (version.present() && history.version <= version.get())) {
			break;
		}

		int i;
		for (i = 0; i < history.value.parentBoundaries.size(); i++) {
			if (history.value.parentBoundaries[i] <= key) {
				break;
			}
		}
		// key should fall between boundaries
		ASSERT(i < history.value.parentBoundaries.size());
		KeyRangeRef parentRange(history.value.parentBoundaries[i], history.value.parentBoundaries[i + 1]);
		Version parentVersion = history.value.parentVersions[i];
		state Key parentHistoryKey = blobGranuleHistoryKeyFor(parentRange, parentVersion);
		state bool foundParent;

		loop {
			try {
				Optional<Value> parentHistoryValue = wait(tr.get(parentHistoryKey));
				foundParent = parentHistoryValue.present();
				if (foundParent) {
					std::pair<KeyRange, Version> decodedKey = decodeBlobGranuleHistoryKey(parentHistoryKey);
					history = GranuleHistory(
					    decodedKey.first, decodedKey.second, decodeBlobGranuleHistoryValue(parentHistoryValue.get()));
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		if (!foundParent) {
			break;
		}
	}

	fmt::print("Done\n");
	return true;
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> blobKeyCommandActor(Database localDb,
                                       Optional<TenantMapEntry> tenantEntry,
                                       std::vector<StringRef> tokens) {
	// enables blob writing for the given range
	if (tokens.size() != 3 && tokens.size() != 4) {
		printUsage(tokens[0]);
		return false;
	}

	ASSERT(tokens[1] == "history"_sr);

	Key key;
	Optional<Version> version;

	if (tenantEntry.present()) {
		key = tokens[2].withPrefix(tenantEntry.get().prefix);
	} else {
		key = tokens[2];
	}

	if (tokens.size() > 3) {
		Version v;
		int n = 0;
		if (sscanf(tokens[3].toString().c_str(), "%" PRId64 "%n", &v, &n) != 1 || n != tokens[3].size()) {
			printUsage(tokens[0]);
			return false;
		}
		version = v;
	}

	if (key >= "\xff"_sr) {
		fmt::print("No blob history for system keyspace\n", key.printable());
		return false;
	} else {
		bool result = wait(printBlobHistory(localDb, key, version));
		return result;
	}
}

// can extend to other blobkey commands later
CommandFactory blobKeyFactory("blobkey", CommandHelp("blobkey history <key> [version]", "", ""));
} // namespace fdb_cli
