/*
 * SnapshotCommand.actor.cpp
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

#include "fdbclient/IClientApi.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> snapshotCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	state bool result = true;
	if (tokens.size() < 2) {
		printUsage(tokens[0]);
		result = false;
	} else {
		Standalone<StringRef> snap_cmd;
		state Key uid(deterministicRandom()->randomUniqueID().toString());
		for (int i = 1; i < tokens.size(); i++) {
			snap_cmd = snap_cmd.withSuffix(tokens[i]);
			if (i != tokens.size() - 1) {
				snap_cmd = snap_cmd.withSuffix(LiteralStringRef(" "));
			}
		}
		try {
			wait(safeThreadFutureToFuture(db->createSnapshot(uid, snap_cmd)));
			printf("Snapshot command succeeded with UID %s\n", uid.toString().c_str());
		} catch (Error& e) {
			fprintf(stderr,
			        "Snapshot command failed %d (%s)."
			        " Please cleanup any instance level snapshots created with UID %s.\n",
			        e.code(),
			        e.what(),
			        uid.toString().c_str());
			result = false;
		}
	}
	return result;
}

// hidden commands, no help text for now
CommandFactory snapshotFactory("snapshot");
} // namespace fdb_cli
