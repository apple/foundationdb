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

#include "fdbclient/NativeAPI.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

// The command is used to get all storage server addresses for a given key.
ACTOR Future<bool> getLocationCommandActor(Database cx, std::vector<StringRef> tokens, Version version) {
	if (tokens.size() != 2) {
		printf("getlocation <KEY>\n"
		       "fetch the storage server address for a given key.\n"
		       "Displays the addresses of storage servers, or `not found' if location is not found.");
		return false;
	}

	KeyRangeLocationInfo loc = wait(getKeyLocation_internal(
	    cx, {}, tokens[1], SpanContext(), Optional<UID>(), UseProvisionalProxies::False, Reverse::False, version));

	if (loc.locations) {
		printf("version is %ld\n", version);
		printf("`%s' is at:\n", printable(tokens[1]).c_str());
		auto locations = loc.locations->locations();
		for (int i = 0; locations && i < locations->size(); i++) {
			printf("  %s\n", locations->getInterface(i).address().toString().c_str());
		}
	} else {
		printf("`%s': location not found\n", printable(tokens[1]).c_str());
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

} // namespace fdb_cli