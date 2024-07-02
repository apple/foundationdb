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
			int old = wait(setBulkLoadMode(localDb, 1));
			TraceEvent("SetBulkLoadModeCommand").detail("OldValue", old).detail("NewValue", 1);
			return UID();
		} else if (tokencmp(tokens[2], "off")) {
			int old = wait(setBulkLoadMode(localDb, 0));
			TraceEvent("SetBulkLoadModeCommand").detail("OldValue", old).detail("NewValue", 0);
			return UID();
		} else {
			printUsage(tokens[0]);
			return UID();
		}

	} else if (tokencmp(tokens[1], "acknowledge")) {
		if (tokens.size() != 5) {
			printUsage(tokens[0]);
			return UID();
		}
		state UID taskId = UID::fromString(tokens[2].toString());
		Key rangeBegin = tokens[3];
		Key rangeEnd = tokens[4];
		if (rangeBegin > normalKeys.end || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		wait(submitBulkLoadTask(
		    clusterFile, BulkLoadState(taskId, range), TriggerBulkLoadRequestType::Acknowledge, /*timeoutSeconds=*/60));
		return taskId;

	} else if (tokencmp(tokens[1], "local")) {
		// Generate spec of bulk loading local files
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
		wait(submitBulkLoadTask(clusterFile, bulkLoadTask, TriggerBulkLoadRequestType::New, /*timeoutSeconds=*/60));
		return bulkLoadTask.getTaskId();

	} else {
		printUsage(tokens[0]);
		return UID();
	}
}

CommandFactory bulkLoadFactory(
    "bulkload",
    CommandHelp("bulkload <mode|acknowledge|local> [BeginKey EndKey] <Folder> <DataFile> <ByteSampleFile>",
                "Start a bulk load task",
                "Start a bulk load task"));
} // namespace fdb_cli
