/*
 * BulkLoadCommand.actor.cpp
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

#include "fdbclient/IClientApi.h"

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/BulkLoading.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<UID> bulkLoadCommandActor(Reference<IClusterConnectionRecord> clusterFile,
                                       Database localDb,
                                       std::vector<StringRef> tokens) {
	if (tokencmp(tokens[1], "mode")) {
		if (tokens.size() != 3) { // TODO(Zhe): check by ID
			printUsage(tokens[0]);
			return UID();
		}
		if (tokencmp(tokens[2], "on")) {
			int old_ = wait(setBulkLoadMode(localDb, 1));
			return UID();
		} else if (tokencmp(tokens[2], "off")) {
			int old_ = wait(setBulkLoadMode(localDb, 0));
			return UID();
		} else {
			printUsage(tokens[0]);
			return UID();
		}

	} else if (tokencmp(tokens[1], "localtask")) {
		if (tokens.size() < 7) {
			printUsage(tokens[0]);
			return UID();
		}
		Key rangeBegin = tokens[2];
		Key rangeEnd = tokens[3];
		if (rangeBegin > normalKeys.end || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		std::string folder = tokens[4].toString();
		std::string dataFile = tokens[5].toString();
		std::string byteSampleFile = tokens[6].toString();
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		state BulkLoadState bulkLoadTask = newBulkLoadTaskLocalSST(range, folder, dataFile, byteSampleFile);
		wait(submitBulkLoadTask(clusterFile, bulkLoadTask, /*timeoutSeconds=*/60));
		return bulkLoadTask.taskId;

	} else {
		printUsage(tokens[0]);
		return UID();
	}
}

CommandFactory bulkLoadFactory(
    "bulkload",
    CommandHelp("bulkload <mode|task> <Type> <Method> [BeginKey EndKey] <Folder> <DataFile> <ByteSampleFile>",
                "Start a bulk load task",
                "Specify `Type' (only `sst' is supported currently), and\n"
                "Specify `Method' (only `cp' is supported currently).\n"));
} // namespace fdb_cli
