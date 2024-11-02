/*
 * DebugCommands.actor.cpp
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

#include <fmt/core.h>

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

std::string toHex(StringRef v) {
	std::string result;
	result.reserve(v.size() * 4);
	for (int i = 0; i < v.size(); i++) {
		result.append(format("\\x%02x", v[i]));
	}
	return result;
}

// Gets a version at which to read from the storage servers
ACTOR Future<Version> getVersion(Database cx) {
	loop {
		state Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			Version version = wait(tr.getReadVersion());
			return version;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Get a list of storage servers that persist keys within range "kr" from the
// first commit proxy. Returns false if there is a failure (in this case,
// keyServersPromise will never be set).
ACTOR Future<bool> getKeyServers(
    Database cx,
    Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServersPromise,
    KeyRangeRef kr) {
	state std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> keyServers;

	// Try getting key server locations from the first commit proxy
	state Future<ErrorOr<GetKeyServerLocationsReply>> keyServerLocationFuture;
	state Key begin = kr.begin;
	state Key end = kr.end;
	state int limitKeyServers = 100;

	while (begin < end) {
		state Reference<CommitProxyInfo> commitProxyInfo =
		    wait(cx->getCommitProxiesFuture(UseProvisionalProxies::False));
		keyServerLocationFuture =
		    commitProxyInfo->get(0, &CommitProxyInterface::getKeyServersLocations)
		        .getReplyUnlessFailedFor(
		            GetKeyServerLocationsRequest({}, {}, begin, end, limitKeyServers, false, latestVersion, Arena()),
		            2,
		            0);

		state bool keyServersInsertedForThisIteration = false;
		choose {
			when(ErrorOr<GetKeyServerLocationsReply> shards = wait(keyServerLocationFuture)) {
				// Get the list of shards if one was returned.
				if (shards.present() && !keyServersInsertedForThisIteration) {
					keyServers.insert(keyServers.end(), shards.get().results.begin(), shards.get().results.end());
					keyServersInsertedForThisIteration = true;
					begin = shards.get().results.back().first.end;
					break;
				}
			}
			when(wait(cx->onProxiesChanged())) {}
		}

		if (!keyServersInsertedForThisIteration) // Retry the entire workflow
			wait(delay(1.0));
	}

	keyServersPromise.send(keyServers);
	return true;
}

// The command is used to get all storage server addresses for a given key.
ACTOR Future<bool> getLocationCommandActor(Database cx, std::vector<StringRef> tokens) {
	if (tokens.size() != 2 && tokens.size() != 3) {
		fmt::println("getlocation <KEY> [<KEY2>]\n"
		             "fetch the storage server address for a given key or range.\n"
		             "Displays the addresses of storage servers, or `not found' if location is not found.");
		return false;
	}

	state KeyRange kr = KeyRangeRef(tokens[1], tokens.size() == 3 ? tokens[2] : keyAfter(tokens[1]));
	// find key range locations without GRV
	state Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServersPromise;
	bool found = wait(getKeyServers(cx, keyServersPromise, kr));
	if (!found) {
		fmt::println("{} locations not found", printable(kr));
		return false;
	}
	std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> keyServers =
	    keyServersPromise.getFuture().get();
	for (const auto& [range, servers] : keyServers) {
		fmt::println("Key range: {}", printable(range));
		for (const auto& server : servers) {
			fmt::println("  {}", server.address().toString());
		}
	}
	return true;
}
// hidden commands, no help text for now
CommandFactory getLocationCommandFactory("getlocation");

// The command is used to get values from all storage servers that have the given key.
ACTOR Future<bool> getallCommandActor(Database cx, std::vector<StringRef> tokens, Version version) {
	if (tokens.size() != 2) {
		fmt::println("getall <KEY>\n"
		             "fetch values from all storage servers that have the given key.\n"
		             "Displays the value and the addresses of storage servers, or `not found' if key is not found.");
		return false;
	}

	KeyRangeLocationInfo loc = wait(getKeyLocation_internal(
	    cx, {}, tokens[1], SpanContext(), Optional<UID>(), UseProvisionalProxies::False, Reverse::False, version));

	if (loc.locations) {
		fmt::println("version is {}", version);
		fmt::println("`{}' is at:", printable(tokens[1]));
		state Reference<LocationInfo::Locations> locations = loc.locations->locations();
		state std::vector<Future<GetValueReply>> replies;
		for (int i = 0; locations && i < locations->size(); i++) {
			GetValueRequest req({}, {}, tokens[1], version, {}, {}, {});
			replies.push_back(locations->get(i, &StorageServerInterface::getValue).getReply(req));
		}
		wait(waitForAll(replies));
		for (int i = 0; i < replies.size(); i++) {
			std::string ssi = locations->getInterface(i).address().toString();
			if (replies[i].isError()) {
				fmt::println(stderr, "ERROR: {} {}", ssi, replies[i].getError().what());
			} else {
				Optional<Value> v = replies[i].get().value;
				fmt::println(" {} {}", ssi, v.present() ? printable(v.get()) : "(not found)");
			}
		}
	} else {
		fmt::println("`{}': location not found", printable(tokens[1]));
	}
	return true;
}
// hidden commands, no help text for now
CommandFactory getallCommandFactory("getall");

std::string printStorageServerMachineInfo(const StorageServerInterface& server) {
	std::string serverIp = server.address().toString();
	std::string serverLocality = server.locality.toString();
	return serverLocality + " " + serverIp;
}

std::string printAllStorageServerMachineInfo(const std::vector<StorageServerInterface>& servers) {
	std::string res;
	for (int i = 0; i < servers.size(); i++) {
		if (i == 0) {
			res = printStorageServerMachineInfo(servers[i]);
		} else {
			res = res + "; " + printStorageServerMachineInfo(servers[i]);
		}
	}
	return res;
}

// check that all replies are the same. Update begin to the next key to check
// checkResults keeps invariants:
// (1) hasMore = true if any server has more data not read yet
// (2) nextBeginKey is the minimal key returned from all servers
// (3) checkResults reports inconsistency of keys only before the nextBeginKey if hasMore=true
// Therefore, whether to proceed to the next round depends on hasMore
// If there is a next round, it starts from the minimal key returned from all servers
bool checkResults(Version version,
                  bool hasMore,
                  Key claimEndKey,
                  const std::vector<StorageServerInterface>& servers,
                  const std::vector<GetKeyValuesReply>& replies) {
	// Compare servers
	bool allSame = true;
	int firstValidServer = -1;
	for (int j = 0; j < replies.size(); j++) {
		if (firstValidServer == -1) {
			firstValidServer = j;
			// Print full list of comparing servers and the reference server
			// Used to check server info which does not produce an inconsistency log
			fmt::println("CheckResult: servers: {}, reference server: {}",
			             printAllStorageServerMachineInfo(servers),
			             printStorageServerMachineInfo(servers[firstValidServer]));
			continue; // always select the first server as reference
		}
		// compare reference and current
		GetKeyValuesReply current = replies[j];
		GetKeyValuesReply reference = replies[firstValidServer];
		if (current.data == reference.data && current.more == reference.more) {
			continue;
		}
		// Detecting corrupted keys for any mismatching replies between current and reference servers
		allSame = false;
		size_t currentI = 0, referenceI = 0;
		while (currentI < current.data.size() || referenceI < reference.data.size()) {
			if (hasMore && ((referenceI < reference.data.size() && reference.data[referenceI].key >= claimEndKey) ||
			                (currentI < current.data.size() && current.data[currentI].key >= claimEndKey))) {
				// If there will be a next round and the key is out of claimEndKey
				// We will delay the detection to the next round
				break;
			}
			if (currentI >= current.data.size()) {
				// ServerA(1), ServerB(0): 1 indicates that ServerA has the key while 0 indicates that ServerB does not
				// have the key
				fmt::println(
				    "Inconsistency: UniqueKey, {}(1), {}(0), CurrentIndex {}, ReferenceIndex {}, Version {}, Key {}",
				    printStorageServerMachineInfo(servers[firstValidServer]),
				    printStorageServerMachineInfo(servers[j]),
				    currentI,
				    referenceI,
				    version,
				    toHex(reference.data[referenceI].key));
				referenceI++;
			} else if (referenceI >= reference.data.size()) {
				fmt::println(
				    "Inconsistency: UniqueKey, {}(1), {}(0), CurrentIndex {}, ReferenceIndex {}, Version {}, Key {}",
				    printStorageServerMachineInfo(servers[j]),
				    printStorageServerMachineInfo(servers[firstValidServer]),
				    currentI,
				    referenceI,
				    version,
				    toHex(current.data[currentI].key));
				currentI++;
			} else {
				KeyValueRef currentKV = current.data[currentI];
				KeyValueRef referenceKV = reference.data[referenceI];
				if (currentKV.key == referenceKV.key) {
					if (currentKV.value != referenceKV.value) {
						fmt::println("Inconsistency: MismatchValue, {}(1), {}(1), CurrentIndex {}, ReferenceIndex {}, "
						             "Version {}, Key {}",
						             printStorageServerMachineInfo(servers[firstValidServer]),
						             printStorageServerMachineInfo(servers[j]),
						             currentI,
						             referenceI,
						             version,
						             toHex(currentKV.key));
					}
					currentI++;
					referenceI++;
				} else if (currentKV.key < referenceKV.key) {
					fmt::println(
					    "Inconsistency: UniqueKey, {}(1), {}(0), CurrentIndex {}, ReferenceIndex {}, Version {}, "
					    "Key {}",
					    printStorageServerMachineInfo(servers[j]),
					    printStorageServerMachineInfo(servers[firstValidServer]),
					    currentI,
					    referenceI,
					    version,
					    toHex(currentKV.key));
					currentI++;
				} else {
					fmt::println(
					    "Inconsistency: UniqueKey, {}(1), {}(0), CurrentIndex {}, ReferenceIndex {}, Version {}, "
					    "Key {}",
					    printStorageServerMachineInfo(servers[firstValidServer]),
					    printStorageServerMachineInfo(servers[j]),
					    currentI,
					    referenceI,
					    version,
					    toHex(referenceKV.key));
					referenceI++;
				}
			}
		}
	}

	return allSame;
}

ACTOR Future<bool> doCheckAll(Database cx, KeyRange inputRange, bool checkAll);
// Return whether inconsistency is detected in the inputRange
ACTOR Future<bool> doCheckAll(Database cx, KeyRange inputRange, bool checkAll) {
	state Transaction onErrorTr(cx); // This transaction exists only to access onError and its backoff behavior
	state bool consistent = true;
	loop {
		try {
			fmt::println("Start checking for range: {}", printable(inputRange));
			// Get SS interface for each shard of the inputRange
			state Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServerPromise;
			bool foundKeyServers = wait(getKeyServers(cx, keyServerPromise, inputRange));
			if (!foundKeyServers) {
				fmt::println("key server locations for {} not found, retrying in 1s...", printable(inputRange));
				wait(delay(1.0));
				continue;
			}
			state std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> keyServers =
			    keyServerPromise.getFuture().get();
			// We partition the entire input range into shards
			// and we conduct comparison shard by shard
			state int i = 0;
			for (; i < keyServers.size(); i++) { // for each shard
				state KeyRange rangeToCheck = keyServers[i].first;
				rangeToCheck = rangeToCheck & inputRange; // Only check the shard part within the inputRange
				if (rangeToCheck.empty()) {
					continue; // Skip the shard if it is outside of the inputRange
				}
				const auto& servers = keyServers[i].second;
				state Key beginKeyToCheck = rangeToCheck.begin;
				fmt::println("Key range to check: {}", printable(rangeToCheck));
				for (const auto& server : servers) {
					fmt::println("\t{}", server.address().toString());
				}
				state std::vector<Future<ErrorOr<GetKeyValuesReply>>> replies;
				state bool hasMore = true;
				state int round = 0;
				state Version version;
				while (hasMore) {
					wait(store(version, getVersion(cx)));
					replies.clear();
					fmt::println("Round {}: {} - {}", round, toHex(beginKeyToCheck), toHex(rangeToCheck.end));
					for (const auto& s : keyServers[i].second) { // for each storage server
						GetKeyValuesRequest req;
						req.begin = firstGreaterOrEqual(beginKeyToCheck);
						req.end = firstGreaterOrEqual(rangeToCheck.end);
						req.limit = CLIENT_KNOBS->KRM_GET_RANGE_LIMIT;
						req.limitBytes = CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES;
						req.version = version; // all replica should read at the same version
						req.tags = TagSet();
						replies.push_back(s.getKeyValues.getReplyUnlessFailedFor(req, 2, 0));
					}
					wait(waitForAll(replies));

					// Decide comparison scope
					Key claimEndKey; // used for the next round if hasMore == true
					Key maxEndKey;
					hasMore = false; // re-calculate hasMore according to replies
					for (int j = 0; j < replies.size(); j++) {
						auto reply = replies[j].get();
						if (reply.isError()) {
							fmt::println("checkResults error: {}", reply.getError().what());
							throw reply.getError();
						} else if (reply.get().error.present()) {
							fmt::println("checkResults error: {}", reply.get().error.get().what());
							throw reply.get().error.get();
						}
						GetKeyValuesReply current = reply.get();
						if (current.data.size() == 0) {
							continue; // Ignore if no data has replied
						}
						if (claimEndKey.empty() || current.data[current.data.size() - 1].key < claimEndKey) {
							claimEndKey = current.data[current.data.size() - 1].key;
						}
						if (maxEndKey.empty() || current.data[current.data.size() - 1].key > maxEndKey) {
							maxEndKey = current.data[current.data.size() - 1].key;
						}
						hasMore = hasMore || current.more;
					}
					fmt::println("Compare scope has been decided\n\tBeginKey: {}\n\tEndKey: {}\n\tHasMore: {}",
					             toHex(beginKeyToCheck),
					             toHex(claimEndKey),
					             hasMore);
					if (claimEndKey.empty()) {
						// It is possible that there is clear operation between the prev round and the current round
						// which result in empty claimEndKey --- nothing to compare
						// In this case, we simply skip the current shard
						ASSERT(hasMore == false);
						continue;
					} else if ((beginKeyToCheck == claimEndKey) && hasMore) {
						// This is a special case: rangeBegin == claimEndKey == next beginKeyToCheck
						// We separate this case and the third case to solve a corner issue led by the
						// third code path: the progress will get stuck on repeatedly checking beginKeyToCheck.
						// In the third code path, if hasMore == true and beginKeyToCheck == claimEndKey,
						// The next round of beginKeyToCheck (aka claimEndKey) will always be beginKeyToCheck of the
						// current round. To avoid this issue, we spawn a child checkall on a smaller range
						// (beginKeyToCheck ~ maxEndKey). This smaller range guarantees that the hasMore is always false
						// and the child checkall will complete and the global progress will move forward. Once the
						// child checkall is done, we move to the range: maxEndKey ~ rangeToCheck.end
						state KeyRange spawnedRangeToCheck = Standalone(KeyRangeRef(beginKeyToCheck, maxEndKey));
						fmt::println("Spawn new checkall for range {}", printable(spawnedRangeToCheck));
						bool allSame = wait(doCheckAll(cx, spawnedRangeToCheck, checkAll));
						beginKeyToCheck = spawnedRangeToCheck.end;
						consistent = consistent && allSame; // !allSame of any subrange results in !consistent
					} else {
						std::vector<GetKeyValuesReply> keyValueReplies;
						for (int j = 0; j < replies.size(); j++) {
							auto reply = replies[j].get();
							ASSERT(reply.present() && !reply.get().error.present()); // has thrown eariler of error
							keyValueReplies.push_back(reply.get());
						}
						// keyServers and keyValueReplies must follow the same order
						bool allSame =
						    checkResults(version, hasMore, claimEndKey, keyServers[i].second, keyValueReplies);
						// Using claimEndKey of the current round as the nextBeginKey for the next round
						// Note that claimEndKey is not compared in the current round
						// This key will be compared in the next round
						fmt::println("Result: compared {} - {}", toHex(beginKeyToCheck), toHex(claimEndKey));
						beginKeyToCheck = claimEndKey;
						fmt::println("allSame {}, hasMore {}, checkAll {}", allSame, hasMore, checkAll);
						consistent = consistent && allSame; // !allSame of any subrange results in !consistent
					}
					if (!consistent && !checkAll) {
						return false;
					}
					round++;
				}
			}
			break;

		} catch (Error& e) {
			fmt::print("Error: {}", e.what());
			wait(onErrorTr.onError(e));
			fmt::println(", retrying in 1s...");
		}
		wait(delay(1.0));
	}
	return consistent;
}

// The command is used to check the data inconsistency of the user input range
ACTOR Future<bool> checkallCommandActor(Database cx, std::vector<StringRef> tokens) {
	state bool checkAll = false; // If set, do not return on first error, continue checking all keys
	state KeyRange inputRange;
	if (tokens.size() == 3) {
		inputRange = KeyRangeRef(tokens[1], tokens[2]);
	} else if (tokens.size() == 4 && tokens[3] == "all"_sr) {
		inputRange = KeyRangeRef(tokens[1], tokens[2]);
		checkAll = true;
	} else {
		fmt::println(
		    "checkall [<KEY> <KEY2>] (all)\n"
		    "Check inconsistency of the input range by comparing all replicas and print any corruptions.\n"
		    "The default behavior is to stop on the first subrange where corruption is found\n"
		    "`all` is optional. When `all` is appended, the checker does not stop until all subranges have checked.\n"
		    "Note this is intended to check a small range of keys, not the entire database (consider consistencycheck "
		    "for that purpose).");
		return false;
	}
	if (inputRange.empty()) {
		fmt::println("Input empty range: {}.\nImmediately exit.", printable(inputRange));
		return false;
	}
	// At this point, we have a non-empty inputRange to check
	bool res = wait(doCheckAll(cx, inputRange, checkAll));
	fmt::println("Checking complete. AllSame: {}", res);
	return true;
}

CommandFactory checkallCommandFactory("checkall");
} // namespace fdb_cli
