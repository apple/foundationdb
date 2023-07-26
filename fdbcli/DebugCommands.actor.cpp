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
bool checkResults(Version version,
                  const std::vector<StorageServerInterface>& servers,
                  const std::vector<Future<ErrorOr<GetKeyValuesReply>>>& replies,
                  KeySelectorRef& begin,
                  const KeySelectorRef& end) {
	bool allSame = true;
	int firstValidServer = -1;
	for (int j = 0; j < replies.size(); j++) {
		auto reply = replies[j].get();
		if (!reply.present() || reply.get().error.present()) {
			printf("Error from %s: %s\n",
			       servers[j].address().toString().c_str(),
			       reply.present() ? reply.get().error.get().what() : "no reply");
			continue;
		}
		GetKeyValuesReply current = reply.get();
		if (firstValidServer == -1) {
			firstValidServer = j;
			continue;
		}
		GetKeyValuesReply reference = replies[firstValidServer].get().get();

		// compare the two replicas
		if (current.data == reference.data && current.more == reference.more) {
			continue;
		}

		allSame = false;
		printf("#%d  %s\n", firstValidServer, servers[firstValidServer].address().toString().c_str());
		printf("#%d  %s\n", j, servers[j].address().toString().c_str());
		int currentI = 0, referenceI = 0;
		while (currentI < current.data.size() || referenceI < reference.data.size()) {
			if (currentI >= current.data.size()) {
				printf(" #%d Unique key: %s\n", firstValidServer, printable(reference.data[referenceI].key).c_str());
				referenceI++;
			} else if (referenceI >= reference.data.size()) {
				printf(" #%d Unique key: %s\n", j, printable(current.data[currentI].key).c_str());
				currentI++;
			} else {
				KeyValueRef currentKV = current.data[currentI];
				KeyValueRef referenceKV = reference.data[referenceI];

				if (currentKV.key == referenceKV.key) {
					if (currentKV.value != referenceKV.value) {
						printf(" Value mismatch key: %s\n", printable(currentKV.key).c_str());
						currentI++;
						referenceI++;
					}
				} else if (currentKV.key < referenceKV.key) {
					printf(" #%d Unique key: %s\n", j, printable(currentKV.key).c_str());
					currentI++;
				} else {
					printf(" #%d Unique key: %s\n", firstValidServer, printable(referenceKV.key).c_str());
					referenceI++;
				}
			}
		}
	}

	if (!allSame)
		return false;

	if (firstValidServer >= 0 && replies[firstValidServer].get().get().more) {
		const VectorRef<KeyValueRef>& result = replies[firstValidServer].get().get().data;
		begin = firstGreaterThan(result[result.size() - 1].key);
	} else {
		printf("Same at version %ld\n", version);
		begin = end; // signal that we're done
	}
	return true;
}

// The command is used to check the inconsistency in a keyspace, default is \xff\x02/blog/ keyspace.
ACTOR Future<bool> checkallCommandActor(Database cx, std::vector<StringRef> tokens) {
	// ignore tokens for now
	state Transaction onErrorTr(cx); // This transaction exists only to access onError and its backoff behavior
	state int i = 0;
	state Version version;
	state KeySelectorRef begin, end;
	state bool checkAll = false; // do not return on first error, continue checking all keys
	state KeyRange toCheck = backupLogKeys;

	if (tokens.size() == 2 && tokens[1] == "ALL"_sr) {
		printf("Checking all keys for corruption...\n");
		checkAll = true;
	}
	if (tokens.size() == 2 && tokens[1] == "help"_sr) {
		printf("checkall [ALL]|[<KEY> <KEY2>]\n"
		       "Check inconsistency in a keyspace by comparing all replicas and print any corruptions.\n"
		       "The default behavior is to stop on the first shard where corruption is found and the\n"
		       "default keyspace is \\xff\\x02/blog/.\n"
		       "The default keyspace can be changed by specifying a range. Note this is intended to check\n"
		       "a small range of keys, not the entire database (consider consistencycheck for that purpose).\n");
		return false;
	}
	if (tokens.size() == 3) {
		toCheck = KeyRangeRef(tokens[1], tokens[2]);
	}

	loop {
		printf("Start checking range: %s\n", printable(toCheck).c_str());
		try {
			state Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServerPromise;
			bool foundKeyServers = wait(getKeyServers(cx, keyServerPromise, toCheck));

			if (!foundKeyServers) {
				printf("key server locations for %s not found, retrying in 1s...\n", printable(toCheck).c_str());
				wait(delay(1.0));
				continue;
			}

			state std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> keyServers =
			    keyServerPromise.getFuture().get();
			for (i = 0; i < keyServers.size(); i++) { // for each key range
				state KeyRange range = keyServers[i].first;
				range = range & toCheck;
				if (range.empty()) {
					continue;
				}
				const auto& servers = keyServers[i].second;
				begin = firstGreaterOrEqual(range.begin);
				end = firstGreaterOrEqual(range.end);
				printf("Key range: %s\n", printable(range).c_str());
				for (const auto& server : servers) {
					printf("  %s\n", server.address().toString().c_str());
				}
				wait(store(version, getVersion(cx)));
				state std::vector<Future<ErrorOr<GetKeyValuesReply>>> replies;
				for (const auto& s : keyServers[i].second) { // for each storage server
					GetKeyValuesRequest req;
					req.begin = begin;
					req.end = end;
					req.limit = 1e4;
					req.limitBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
					req.version = version;
					req.tags = TagSet();

					replies.push_back(s.getKeyValues.getReplyUnlessFailedFor(req, 2, 0));
				}
				// printf("waiting for %lu replies at version: %ld\n", keyServers[i].second.size(), version);
				wait(waitForAll(replies));
				if (!checkResults(version, keyServers[i].second, replies, begin, end) && !checkAll) {
					return false;
				}
				if (begin == end) {
					toCheck = KeyRangeRef(range.end, toCheck.end);
				}
				// TODO: if there are more results, continue checking in the same shard
			}
		} catch (Error& e) {
			printf("Retrying for error: %s\n", e.what());
			wait(onErrorTr.onError(e));
		}
		if (toCheck.begin == toCheck.end) {
			return true;
		}
		wait(delay(1.0));
	}
}

CommandFactory checkallCommandFactory("checkall");
} // namespace fdb_cli