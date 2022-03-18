/*
 * TargetVersionCommand.actor.cpp
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

#include "boost/lexical_cast.hpp"

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/IClientApi.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

const KeyRef versionEpochSpecialKey = LiteralStringRef("\xff\xff/management/version_epoch");

struct VersionInfo {
	int64_t version;
	int64_t expectedVersion;
};

ACTOR static Future<Optional<VersionInfo>> getVersionInfo(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state Version rv = wait(safeThreadFutureToFuture(tr->getReadVersion()));
			Optional<Standalone<StringRef>> versionEpochValue =
			    wait(safeThreadFutureToFuture(tr->get(versionEpochKey)));
			if (!versionEpochValue.present()) {
				return Optional<VersionInfo>();
			}
			int64_t versionEpoch = BinaryReader::fromStringRef<Version>(versionEpochValue.get(), Unversioned());
			int64_t expected = g_network->timer() * CLIENT_KNOBS->CORE_VERSIONSPERSECOND - versionEpoch;
			return VersionInfo{ rv, expected };
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR static Future<Optional<int64_t>> getVersionEpoch(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			Optional<Value> versionEpochVal = wait(safeThreadFutureToFuture(tr->get(versionEpochSpecialKey)));
			return versionEpochVal.present() ? boost::lexical_cast<int64_t>(versionEpochVal.get().toString())
			                                 : Optional<int64_t>();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<bool> targetVersionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() <= 3) {
		if (tokens.size() == 1) {
			Optional<VersionInfo> versionInfo = wait(getVersionInfo(db));
			if (versionInfo.present()) {
				printf("Version:    %" PRId64 "\n", versionInfo.get().version);
				printf("Expected:   %" PRId64 "\n", versionInfo.get().expectedVersion);
				printf("Difference: %" PRId64 "\n", versionInfo.get().expectedVersion - versionInfo.get().version);
			} else {
				printf("Version epoch is unset\n");
			}
			return true;
		} else if (tokens.size() == 2 && tokencmp(tokens[1], "getepoch")) {
			Optional<int64_t> versionEpoch = wait(getVersionEpoch(db));
			if (versionEpoch.present()) {
				printf("Current version epoch is %" PRId64 "\n", versionEpoch.get());
			} else {
				printf("Version epoch is unset\n");
			}
			return true;
		} else if (tokens.size() == 2 && tokencmp(tokens[1], "clearepoch")) {
			// Clearing the version epoch means versions will no longer attempt
			// to advance at the same rate as the clock. The current version
			// will remain unchanged.
			state Reference<ITransaction> clearTr = db->createTransaction();
			loop {
				clearTr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				try {
					Optional<int64_t> versionEpoch = wait(getVersionEpoch(db));
					if (!versionEpoch.present()) {
						return true;
					} else {
						clearTr->clear(versionEpochSpecialKey);
						wait(safeThreadFutureToFuture(clearTr->commit()));
					}
				} catch (Error& e) {
					wait(safeThreadFutureToFuture(clearTr->onError(e)));
				}
			}
		} else if (tokens.size() == 3) {
			state int64_t v;
			int n = 0;
			if (sscanf(tokens[2].toString().c_str(), "%" SCNd64 "%n", &v, &n) != 1 || n != tokens[2].size()) {
				printUsage(tokens[0]);
				return false;
			}

			state int64_t newVersionEpoch = -1;
			if (tokencmp(tokens[1], "setepoch")) {
				newVersionEpoch = v;
			} else if (tokencmp(tokens[1], "add")) {
				Optional<int64_t> versionEpoch = wait(getVersionEpoch(db));
				newVersionEpoch = versionEpoch.orDefault(CLIENT_KNOBS->DEFAULT_VERSION_EPOCH) + v;
			} else {
				printUsage(tokens[0]);
				return false;
			}

			state Reference<ITransaction> tr = db->createTransaction();
			loop {
				tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				try {
					Optional<int64_t> versionEpoch = wait(getVersionEpoch(db));
					if (!versionEpoch.present() || newVersionEpoch != versionEpoch.get()) {
						// Since this transaction causes a recovery, it will
						// almost certainly receive commit_unknown_result.
						// Re-read the version epoch on each loop to check
						// whether the change has been committed successfully.
						tr->set(versionEpochSpecialKey, boost::lexical_cast<std::string>(newVersionEpoch));
						wait(safeThreadFutureToFuture(tr->commit()));
					} else {
						printf("Current version epoch is %" PRId64 "\n", versionEpoch.get());
						return true;
					}
				} catch (Error& e) {
					wait(safeThreadFutureToFuture(tr->onError(e)));
				}
			}
		}
	}

	printUsage(tokens[0]);
	return false;
}

CommandFactory versionEpochFactory(
    "targetversion",
    CommandHelp("targetversion [<getepoch|clearepoch|setepoch|add> [EPOCH]]",
                "Read or write the version epoch",
                "If no arguments are specified, reports the offset between the expected version "
                "and the actual version. Otherwise, reads, clears, or sets the version epoch. "
                "The add command adds the number of versions specified to the current version "
                "(value can be negative). If the version of the cluster will increase as a result "
                "of this command, a recovery will occur and a one time jump to the larger version "
                "made. Otherwise, the rate at which versions are given out will be decreased until "
                "the cluster version is synchronized with the new target."));
} // namespace fdb_cli
