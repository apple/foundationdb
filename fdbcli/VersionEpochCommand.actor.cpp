/*
 * VersionEpochCommand.actor.cpp
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

ACTOR Future<bool> versionEpochCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() == 1 || tokens.size() == 3) {
		state Reference<ITransaction> tr = db->createTransaction();
		state int64_t versionEpoch = -1;
		loop {
			try {
				Optional<Value> versionEpochVal = wait(safeThreadFutureToFuture(tr->get(versionEpochSpecialKey)));
				ASSERT(versionEpochVal.present()); // should always return a default value
				versionEpoch = boost::lexical_cast<int64_t>(versionEpochVal.get().toString());
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		if (tokens.size() == 1) {
			printf("Current version epoch: %" PRId64 "\n", versionEpoch);
			return true;
		} else if (tokens.size() == 3) {
			int64_t v;
			int n = 0;
			if (sscanf(tokens[2].toString().c_str(), "%" SCNd64 "%n", &v, &n) != 1 || n != tokens[2].size()) {
				printUsage(tokens[0]);
				return false;
			}

			state int64_t newVersionEpoch = versionEpoch;
			if (tokencmp(tokens[1], "set")) {
				newVersionEpoch = v;
			} else if (tokencmp(tokens[1], "add")) {
				newVersionEpoch += v;
			} else {
				printUsage(tokens[0]);
				return false;
			}

			loop {
				tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				try {
					tr->set(versionEpochSpecialKey, boost::lexical_cast<std::string>(newVersionEpoch));
					wait(safeThreadFutureToFuture(tr->commit()));
					return true;
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
    "versionepoch",
    CommandHelp("versionepoch [{set|add} EPOCH]",
                "Read or write the version epoch",
                "Reads the version epoch of the cluster if no arguments are specified. Otherwise, sets the absolute "
                "version epoch or updates the existing epoch. If the new version epoch is lower than the "
                "current version epoch, the cluster version will be advanced. Otherwise, versions will be "
                "given out slower until the cluster version is synchronized with wall clock time."));
} // namespace fdb_cli
