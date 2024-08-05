/*
 * LocationMetadataCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.actor.h"
#include "fdbclient/IClientApi.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
ACTOR Future<std::string> describeServers(Reference<ReadYourWritesTransaction> tr, std::vector<UID> ids) {
	std::vector<Future<Optional<Value>>> serverListEntries;
	for (const UID& id : ids) {
		serverListEntries.push_back(tr->get(serverListKeyFor(id)));
	}
	std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));
	std::string res;
	for (auto& v : serverListValues) {
		StorageServerInterface ssi = decodeServerListValue(v.get());
		if (!res.empty()) {
			res += ", ";
		}
		res += fmt::format("ServerID: {}, Addr: {}", ssi.uniqueID.toString(), ssi.stableAddress().toString());
	}

	return res;
}

ACTOR Future<Void> printKeyServersEntry(Reference<ReadYourWritesTransaction> tr,
                                        RangeResultRef UIDtoTagMap,
                                        Value entry,
                                        KeyRangeRef range) {
	state std::vector<UID> src;
	state std::vector<UID> dest;
	state UID srcId;
	state UID destId;
	decodeKeyServersValue(UIDtoTagMap, entry, src, dest, srcId, destId);
	state std::string srcDesc = wait(describeServers(tr, src));
	std::string destDesc = wait(describeServers(tr, dest));
	printf("Range: %s, ShardID: %s, Src Servers: %s, Dest Servers: %s\n",
	       Traceable<KeyRangeRef>::toString(range).c_str(),
	       srcId.toString().c_str(),
	       srcDesc.c_str(),
	       destDesc.c_str());
	return Void();
}

ACTOR Future<Void> printRandomShards(Database cx, int n, bool physicalShard) {
	state Key begin = allKeys.begin;
	state int numShards = 0;

	while (begin < allKeys.end && numShards < n) {
		// RYW to optimize re-reading the same key ranges
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		loop {
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state RangeResult UIDtoTagMap = wait(tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				state KeyRangeRef currentKeys(begin, allKeys.end);
				state RangeResult shards =
				    wait(krmGetRanges(tr, keyServersPrefix, currentKeys, n, CLIENT_KNOBS->TOO_MANY));

				state int i = 0;
				for (; i < shards.size() - 1 && numShards < n; ++i) {
					KeyRangeRef currentRange(shards[i].key, shards[i + 1].key);
					std::vector<UID> src;
					std::vector<UID> dest;
					UID srcId;
					UID destId;
					decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest, srcId, destId);
					if (physicalShard == (srcId != anonymousShardId)) {
						wait(printKeyServersEntry(tr, UIDtoTagMap, shards[i].value, currentRange));
						++numShards;
					}
				}

				begin = shards.back().key;
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	printf("Found %d %s shards\n", numShards, physicalShard ? "Physical" : "Non-physical");

	return Void();
}

ACTOR Future<Void> printPhysicalShardCount(Database cx) {
	state Key begin = allKeys.begin;
	state int numShards = 0;
	state int numPhysicalShards = 0;

	while (begin < allKeys.end) {
		// RYW to optimize re-reading the same key ranges
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		loop {
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state RangeResult UIDtoTagMap = wait(tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				KeyRangeRef currentKeys(begin, allKeys.end);
				RangeResult shards = wait(
				    krmGetRanges(tr, keyServersPrefix, currentKeys, CLIENT_KNOBS->TOO_MANY, CLIENT_KNOBS->TOO_MANY));

				for (int i = 0; i < shards.size() - 1; ++i) {
					std::vector<UID> src;
					std::vector<UID> dest;
					UID srcId;
					UID destId;
					decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest, srcId, destId);
					if (srcId != anonymousShardId) {
						++numPhysicalShards;
					}
				}

				begin = shards.back().key;
				numShards += shards.size() - 1;
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	printf("Total number of shards: %d, number of physical shards: %d\n", numShards, numPhysicalShards);

	return Void();
}

ACTOR Future<Void> printServerShards(Database cx, UID serverId) {
	state Key begin = allKeys.begin;

	while (begin < allKeys.end) {
		// RYW to optimize re-reading the same key ranges
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		loop {
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				RangeResult serverShards =
				    wait(krmGetRanges(tr, serverKeysPrefixFor(serverId), KeyRangeRef(begin, allKeys.end)));

				for (int i = 0; i < serverShards.size() - 1; ++i) {
					KeyRangeRef currentRange(serverShards[i].key, serverShards[i + 1].key);
					UID shardId;
					bool assigned, emptyRange;
					DataMoveType dataMoveType = DataMoveType::LOGICAL;
					DataMovementReason dataMoveReason = DataMovementReason::INVALID;
					decodeServerKeysValue(
					    serverShards[i].value, assigned, emptyRange, dataMoveType, shardId, dataMoveReason);
					printf("Range: %s, ShardID: %s, Assigned: %s\n",
					       Traceable<KeyRangeRef>::toString(currentRange).c_str(),
					       shardId.toString().c_str(),
					       assigned ? "true" : "false");
				}

				begin = serverShards.back().key;
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	return Void();
}

ACTOR Future<Void> resolveRange(Database cx, KeyRange range) {
	state Key begin = range.begin;

	while (begin < range.end) {
		// RYW to optimize re-reading the same key ranges
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		loop {
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state RangeResult UIDtoTagMap = wait(tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				state KeyRangeRef currentKeys(begin, range.end);
				state RangeResult shards = wait(
				    krmGetRanges(tr, keyServersPrefix, currentKeys, CLIENT_KNOBS->TOO_MANY, CLIENT_KNOBS->TOO_MANY));

				state int i = 0;
				for (; i < shards.size() - 1; ++i) {
					state KeyRangeRef currentRange(shards[i].key, shards[i + 1].key);
					wait(printKeyServersEntry(tr, UIDtoTagMap, shards[i].value, currentRange));
				}

				begin = shards.back().key;
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	return Void();
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> locationMetadataCommandActor(Database cx, std::vector<StringRef> tokens) {
	if (tokens.size() < 2 || tokens.size() > 4) {
		printUsage(tokens[0]);
		return false;
	}

	if (tokens.size() == 2) {
		if (!tokencmp(tokens[1], "physicalshards")) {
			printUsage(tokens[0]);
			return false;
		}
		wait(printPhysicalShardCount(cx));
	} else if (tokencmp(tokens[1], "resolve")) {
		if (tokens.size() == 3) {
			wait(resolveRange(cx, singleKeyRange(tokens[2])));
		} else {
			wait(resolveRange(cx, KeyRangeRef(tokens[2], tokens[3])));
		}
	} else if (tokencmp(tokens[1], "servershards")) {
		if (tokens.size() != 3) {
			printUsage(tokens[0]);
			return false;
		}
		wait(printServerShards(cx, UID::fromString(tokens[2].toString())));
	} else if (tokencmp(tokens[1], "listshards")) {
		if (tokens.size() == 4 && !tokencmp(tokens[3], "physical")) {
			printUsage(tokens[0]);
			return false;
		}
		const bool physical = tokens.size() == 4;
		wait(printRandomShards(cx, std::stoi(tokens[2].toString()), physical));
	} else {
		printUsage(tokens[0]);
		return false;
	}

	return true;
}

CommandFactory locationMetadataFactory(
    "location_metadata",
    CommandHelp(
        "location_metadata [physicalshards|resolve|servershards|listshards] [<id>|<begin>|<n>] [<end>|physical]",
        "Check location metadata",
        "To check number of physical shards: `location_metadata physicalshards'\n"
        "To check the location of a key: `location_metadata resolve <key>'\n"
        "To check the location of a keyrange: `location_metadata resolve <begin> <end>'\n"
        "To check shard assignments of a storage server: `location_metadata servershards <ssID>'\n"
        "To list <n> random physical shards: `location_metadata listshards <n> physical'\n"
        "To list <n> random non-physical shards: `location_metadata listshards <n>'\n"));
} // namespace fdb_cli
