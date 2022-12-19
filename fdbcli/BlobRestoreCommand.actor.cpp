/*
 * BlobRestoreCommand.actor.cpp
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
#include "fdbclient/FDBOptions.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> blobRestoreCommandActor(Database localDb, std::vector<StringRef> tokens) {
	if (tokens.size() != 1 && tokens.size() != 2) {
		printUsage(tokens[0]);
		return false;
	}

	state bool success = false;
	wait(store(success, localDb->blobRestore(normalKeys)));
	if (success) {
		fmt::print(
		    "Started blob restore for the full cluster. Please use 'status details' command to check progress.\n");
	} else {
		fmt::print("Fail to start a new blob restore while there is a pending one.\n");
	}
	return success;
}

CommandFactory blobRestoreFactory("blobrestore", CommandHelp("blobrestore", "", ""));
} // namespace fdb_cli
