/*
 * AdvanceVersionCommand.actor.cpp
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
#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/IClientApi.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

const KeyRef advanceVersionSpecialKey = LiteralStringRef("\xff\xff/management/min_required_commit_version");

ACTOR Future<bool> advanceVersionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 2) {
		printUsage(tokens[0]);
		return false;
	} else {
		state Version v;
		int n = 0;
		if (sscanf(tokens[1].toString().c_str(), "%" PRId64 "%n", &v, &n) != 1 || n != tokens[1].size()) {
			printUsage(tokens[0]);
			return false;
		} else {
			state Reference<ITransaction> tr = db->createTransaction();
			loop {
				tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				try {
					Version rv = wait(safeThreadFutureToFuture(tr->getReadVersion()));
					if (rv <= v) {
						tr->set(advanceVersionSpecialKey, boost::lexical_cast<std::string>(v));
						wait(safeThreadFutureToFuture(tr->commit()));
					} else {
						fmt::print("Current read version is {}\n", rv);
						return true;
					}
				} catch (Error& e) {
					wait(safeThreadFutureToFuture(tr->onError(e)));
				}
			}
		}
	}
}

CommandFactory advanceVersionFactory(
    "advanceversion",
    CommandHelp(
        "advanceversion <VERSION>",
        "Force the cluster to recover at the specified version",
        "Forces the cluster to recover at the specified version. If the specified version is larger than the current "
        "version of the cluster, the cluster version is advanced "
        "to the specified version via a forced recovery."));
} // namespace fdb_cli
