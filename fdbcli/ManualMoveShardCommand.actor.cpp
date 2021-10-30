/*
 * ForceRecoveryWithDataLossCommand.actor.cpp
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

#include "fdbclient/IClientApi.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> moveShardCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	KeyRangeRef keys(tokens[1], tokens[2]);
	std::vector<NetworkAddress> addresses;
	for (int i = 3; i < tokens.size(); ++i) {
		addresses.push_back(NetworkAddress::parse(tokens[i].toString()));
	}
	wait(safeThreadFutureToFuture(db->moveShard(keys, addresses)));
	return true;
}

CommandFactory moveShardFactory(
    "move_shard",
    CommandHelp("move_shard <BEGIN_KEY> <END_KEY> <ADDRESS...>",
                "Move data range [BEGIN_KEY, END_KEY) to servers identified by their addresses",
                "Manually move the data to a set of storage servers, this tool can be used in tests to manually distribute data."
                "WARNING, the command will also DISABLE DD, to avoid DD moving data in parallel.\n"));
} // namespace fdb_cli
