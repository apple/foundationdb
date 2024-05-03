/*
 * DebugCommands.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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
		printf("getlocation <KEY> [<KEY2>]\n"
		       "fetch the storage server address for a given key or range.\n"
		       "Displays the addresses of storage servers, or `not found' if location is not found.");
		return false;
	}

	state KeyRange kr = KeyRangeRef(tokens[1], tokens.size() == 3 ? tokens[2] : keyAfter(tokens[1]));
	// find key range locations without GRV
	state Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServersPromise;
	bool found = wait(getKeyServers(cx, keyServersPromise, kr));
	if (!found) {
		printf("%s locations not found\n", printable(kr).c_str());
		return false;
	}
	std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> keyServers =
	    keyServersPromise.getFuture().get();
	for (const auto& [range, servers] : keyServers) {
		printf("Key range: %s\n", printable(range).c_str());
		for (const auto& server : servers) {
			printf("  %s\n", server.address().toString().c_str());
		}
	}
	return true;
}
// hidden commands, no help text for now
CommandFactory getLocationCommandFactory("getlocation");

// The command is used to get values from all storage servers that have the given key.
ACTOR Future<bool> getallCommandActor(Database cx, std::vector<StringRef> tokens, Version version) {
	if (tokens.size() != 2) {
		printf("getall <KEY>\n"
		       "fetch values from all storage servers that have the given key.\n"
		       "Displays the value and the addresses of storage servers, or `not found' if key is not found.");
		return false;
	}

	KeyRangeLocationInfo loc = wait(getKeyLocation_internal(
	    cx, {}, tokens[1], SpanContext(), Optional<UID>(), UseProvisionalProxies::False, Reverse::False, version));

	if (loc.locations) {
		printf("version is %ld\n", version);
		printf("`%s' is at:\n", printable(tokens[1]).c_str());
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
				fprintf(stderr, "ERROR: %s %s\n", ssi.c_str(), replies[i].getError().what());
			} else {
				Optional<Value> v = replies[i].get().value;
				printf(" %s %s\n", ssi.c_str(), v.present() ? printable(v.get()).c_str() : "(not found)");
			}
		}
	} else {
		printf("`%s': location not found\n", printable(tokens[1]).c_str());
	}
	return true;
}
// hidden commands, no help text for now
CommandFactory getallCommandFactory("getall");

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
				printf("Inconsistency: UniqueKey, %s(1), %s(0), CurrentIndex %lu, ReferenceIndex %lu, Version %ld, Key "
				       "%s\n",
				       servers[firstValidServer].address().toString().c_str(),
				       servers[j].address().toString().c_str(),
				       currentI,
				       referenceI,
				       version,
				       toHex(reference.data[referenceI].key).c_str());
				referenceI++;
			} else if (referenceI >= reference.data.size()) {
				printf("Inconsistency: UniqueKey, %s(1), %s(0), CurrentIndex %lu, ReferenceIndex %lu, Version %ld, Key "
				       "%s\n",
				       servers[j].address().toString().c_str(),
				       servers[firstValidServer].address().toString().c_str(),
				       currentI,
				       referenceI,
				       version,
				       toHex(current.data[currentI].key).c_str());
				currentI++;
			} else {
				KeyValueRef currentKV = current.data[currentI];
				KeyValueRef referenceKV = reference.data[referenceI];
				if (currentKV.key == referenceKV.key) {
					if (currentKV.value != referenceKV.value) {
						printf("Inconsistency: MismatchValue, %s(1), %s(1), CurrentIndex %lu, ReferenceIndex %lu, "
						       "Version %ld, "
						       "Key %s\n",
						       servers[firstValidServer].address().toString().c_str(),
						       servers[j].address().toString().c_str(),
						       currentI,
						       referenceI,
						       version,
						       toHex(currentKV.key).c_str());
					}
					currentI++;
					referenceI++;
				} else if (currentKV.key < referenceKV.key) {
					printf("Inconsistency: UniqueKey, %s(1), %s(0), CurrentIndex %lu, ReferenceIndex %lu, Version %ld, "
					       "Key %s\n",
					       servers[j].address().toString().c_str(),
					       servers[firstValidServer].address().toString().c_str(),
					       currentI,
					       referenceI,
					       version,
					       toHex(currentKV.key).c_str());
					currentI++;
				} else {
					printf("Inconsistency: UniqueKey, %s(1), %s(0), CurrentIndex %lu, ReferenceIndex %lu, Version %ld, "
					       "Key %s\n",
					       servers[firstValidServer].address().toString().c_str(),
					       servers[j].address().toString().c_str(),
					       currentI,
					       referenceI,
					       version,
					       toHex(referenceKV.key).c_str());
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
			printf("Start checking for range: %s\n", printable(inputRange).c_str());
			// Get SS interface for each shard of the inputRange
			state Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServerPromise;
			bool foundKeyServers = wait(getKeyServers(cx, keyServerPromise, inputRange));
			if (!foundKeyServers) {
				printf("key server locations for %s not found, retrying in 1s...\n", printable(inputRange).c_str());
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
				printf("Key range to check: %s\n", printable(rangeToCheck).c_str());
				for (const auto& server : servers) {
					printf("\t%s\n", server.address().toString().c_str());
				}
				state std::vector<Future<ErrorOr<GetKeyValuesReply>>> replies;
				state bool hasMore = true;
				state int round = 0;
				state Version version;
				while (hasMore) {
					wait(store(version, getVersion(cx)));
					replies.clear();
					printf(
					    "Round %d: %s - %s\n", round, toHex(beginKeyToCheck).c_str(), toHex(rangeToCheck.end).c_str());
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
							printf("checkResults error: %s\n", reply.getError().what());
							throw reply.getError();
						} else if (reply.get().error.present()) {
							printf("checkResults error: %s\n", reply.get().error.get().what());
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
					printf("Compare scope has been decided\n\tBeginKey: %s\n\tEndKey: %s\n\tHasMore: %d\n",
					       toHex(beginKeyToCheck).c_str(),
					       toHex(claimEndKey).c_str(),
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
						printf("Spawn new checkall for range %s\n", printable(spawnedRangeToCheck).c_str());
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
						printf(
						    "Result: compared %s - %s\n", toHex(beginKeyToCheck).c_str(), toHex(claimEndKey).c_str());
						beginKeyToCheck = claimEndKey;
						printf("allSame %d, hasMore %d, checkAll %d\n", allSame, hasMore, checkAll);
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
			printf("Error: %s", e.what());
			wait(onErrorTr.onError(e));
			printf(", retrying in 1s...\n");
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
		printf(
		    "checkall [<KEY> <KEY2>] (all)\n"
		    "Check inconsistency of the input range by comparing all replicas and print any corruptions.\n"
		    "The default behavior is to stop on the first subrange where corruption is found\n"
		    "`all` is optional. When `all` is appended, the checker does not stop until all subranges have checked.\n"
		    "Note this is intended to check a small range of keys, not the entire database (consider consistencycheck "
		    "for that purpose).\n");
		return false;
	}
	if (inputRange.empty()) {
		printf("Input empty range: %s.\nImmediately exit.\n", printable(inputRange).c_str());
		return false;
	}
	// At this point, we have a non-empty inputRange to check
	bool res = wait(doCheckAll(cx, inputRange, checkAll));
	printf("Checking complete. AllSame: %d\n", res);
	return true;
}

CommandFactory checkallCommandFactory("checkall");
} // namespace fdb_cli