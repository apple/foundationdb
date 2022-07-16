/*
 * CacheRangeCommand.actor.cpp
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
#include "fdbclient/SystemData.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> cacheRangeCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 4) {
		printUsage(tokens[0]);
		return false;
	} else {
		state KeyRangeRef cacheRange(tokens[2], tokens[3]);
		if (tokencmp(tokens[1], "set")) {
			wait(ManagementAPI::addCachedRange(db, cacheRange));
		} else if (tokencmp(tokens[1], "clear")) {
			wait(ManagementAPI::removeCachedRange(db, cacheRange));
		} else {
			printUsage(tokens[0]);
			return false;
		}
	}
	return true;
}

CommandFactory cacheRangeFactory(
    "cache_range",
    CommandHelp(
        "cache_range <set|clear> <BEGINKEY> <ENDKEY>",
        "Mark a key range to add to or remove from storage caches.",
        "Use the storage caches to assist in balancing hot read shards. Set the appropriate ranges when experiencing "
        "heavy load, and clear them when they are no longer necessary."));

} // namespace fdb_cli
