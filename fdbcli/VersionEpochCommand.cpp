/*
 * VersionEpochCommand.cpp
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

#include "boost/lexical_cast.hpp"

#include "fdbcli/fdbcli.h"

#include "fdbclient/IClientApi.h"
#include "fdbclient/ManagementAPI.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"

namespace fdb_cli {

const KeyRef versionEpochSpecialKey = "\xff\xff/management/version_epoch"_sr;

struct VersionInfo {
	int64_t version;
	int64_t expectedVersion;
};

static Future<Optional<VersionInfo>> getVersionInfo(Reference<IDatabase> db) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Version rv = co_await safeThreadFutureToFuture(tr->getReadVersion());
			ThreadFuture<Optional<Value>> versionEpochValFuture = tr->get(versionEpochKey);
			Optional<Value> versionEpochVal = co_await safeThreadFutureToFuture(versionEpochValFuture);
			if (!versionEpochVal.present()) {
				co_return Optional<VersionInfo>();
			}
			int64_t versionEpoch = BinaryReader::fromStringRef<int64_t>(versionEpochVal.get(), Unversioned());
			int64_t expected = g_network->timer() * CLIENT_KNOBS->CORE_VERSIONSPERSECOND - versionEpoch;
			co_return VersionInfo{ rv, expected };
		} catch (Error& e) {
			err = e;
		}
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

static Future<Optional<int64_t>> getVersionEpoch(Reference<ITransaction> tr) {
	while (true) {
		Error err;
		try {
			ThreadFuture<Optional<Value>> versionEpochValFuture = tr->get(versionEpochSpecialKey);
			Optional<Value> versionEpochVal = co_await safeThreadFutureToFuture(versionEpochValFuture);
			co_return versionEpochVal.present() ? boost::lexical_cast<int64_t>(versionEpochVal.get().toString())
			                                    : Optional<int64_t>();
		} catch (Error& e) {
			err = e;
		}
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<bool> versionEpochCommandActor(Reference<IDatabase> db, Database cx, std::vector<StringRef> tokens) {
	if (tokens.size() <= 3) {
		Reference<ITransaction> tr = db->createTransaction();
		if (tokens.size() == 1) {
			Optional<VersionInfo> versionInfo = co_await getVersionInfo(db);
			if (versionInfo.present()) {
				int64_t diff = versionInfo.get().expectedVersion - versionInfo.get().version;
				printf("Version:    %" PRId64 "\n", versionInfo.get().version);
				printf("Expected:   %" PRId64 "\n", versionInfo.get().expectedVersion);
				printf("Difference: %" PRId64 " (%.2fs)\n", diff, 1.0 * diff / CLIENT_KNOBS->VERSIONS_PER_SECOND);
			} else {
				printf("Version epoch is unset\n");
			}
			co_return true;
		} else if (tokens.size() == 2 && tokencmp(tokens[1], "get")) {
			Optional<int64_t> versionEpoch = co_await getVersionEpoch(db->createTransaction());
			if (versionEpoch.present()) {
				printf("Current version epoch is %" PRId64 "\n", versionEpoch.get());
			} else {
				printf("Version epoch is unset\n");
			}
			co_return true;
		} else if (tokens.size() == 2 && tokencmp(tokens[1], "disable")) {
			// Clearing the version epoch means versions will no longer attempt
			// to advance at the same rate as the clock. The current version
			// will remain unchanged.
			while (true) {
				Error err;
				try {
					tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					Optional<int64_t> versionEpoch = co_await getVersionEpoch(db->createTransaction());
					if (!versionEpoch.present()) {
						co_return true;
					} else {
						tr->clear(versionEpochSpecialKey);
						co_await safeThreadFutureToFuture(tr->commit());
					}
				} catch (Error& e) {
					err = e;
				}
				co_await safeThreadFutureToFuture(tr->onError(err));
			}
		} else if ((tokens.size() == 2 && tokencmp(tokens[1], "enable")) ||
		           (tokens.size() == 3 && tokencmp(tokens[1], "set"))) {
			int64_t v;
			if (tokens.size() == 3) {
				int n = 0;
				if (sscanf(tokens[2].toString().c_str(), "%" SCNd64 "%n", &v, &n) != 1 || n != tokens[2].size()) {
					printUsage(tokens[0]);
					co_return false;
				}
			} else {
				v = 0; // default version epoch
			}

			while (true) {
				Error err;
				try {
					tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					Optional<int64_t> versionEpoch = co_await getVersionEpoch(tr);
					if (!versionEpoch.present() || (versionEpoch.get() != v && tokens.size() == 3)) {
						tr->set(versionEpochSpecialKey, BinaryWriter::toValue(v, Unversioned()));
						co_await safeThreadFutureToFuture(tr->commit());
					} else {
						printf("Version epoch enabled. Run `versionepoch commit` to irreversibly jump to the target "
						       "version\n");
						co_return true;
					}
				} catch (Error& e) {
					err = e;
				}
				co_await safeThreadFutureToFuture(tr->onError(err));
			}
		} else if (tokens.size() == 2 && tokencmp(tokens[1], "commit")) {
			Optional<VersionInfo> versionInfo = co_await getVersionInfo(db);
			if (versionInfo.present()) {
				co_await advanceVersion(cx, versionInfo.get().expectedVersion);
			} else {
				printf("Must set the version epoch before committing it (see `versionepoch enable`)\n");
			}
			co_return true;
		}
	}

	printUsage(tokens[0]);
	co_return false;
}

CommandFactory versionEpochFactory(
    "versionepoch",
    CommandHelp("versionepoch [<enable|commit|get|set|disable> [EPOCH]]",
                "Read or write the version epoch",
                "If no arguments are specified, reports the offset between the expected version "
                "and the actual version. Otherwise, enables, disables, or commits the version epoch. "
                "Setting the version epoch can be irreversible since it can cause a large version jump. "
                "Thus, the version epoch must first by enabled with the enable or set command. This "
                "causes a recovery. Once the version epoch has been set, versions may be given out at "
                "a faster or slower rate to attempt to match the actual version to the expected version, "
                "based on the version epoch. After setting the version, run the commit command to perform "
                "a one time jump to the expected version. This is useful when there is a very large gap "
                "between the current version and the expected version. Note that once a version jump has "
                "occurred, it cannot be undone. Run this command without any arguments to see the current "
                "and expected version."));
} // namespace fdb_cli
