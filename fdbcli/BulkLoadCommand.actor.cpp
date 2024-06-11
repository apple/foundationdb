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

ACTOR Future<UID> bulkLoadCommandActor(Reference<IClusterConnectionRecord> clusterFile, std::vector<StringRef> tokens) {
	if (tokens.size() < 2) {
		printUsage(tokens[0]);
		return UID();
	}
	BulkLoadType type = BulkLoadType::Invalid;
	if (tokencmp(tokens[1], "sst")) {
		type = BulkLoadType::SST;
	} else {
		printUsage(tokens[0]);
		return UID();
	}
	BulkLoadTransportMethod method = BulkLoadTransportMethod::Invalid;
	if (tokencmp(tokens[2], "cp")) {
		method = BulkLoadTransportMethod::CP;
	} else {
		printUsage(tokens[0]);
		return UID();
	}
	Key rangeBegin = tokens[3];
	Key rangeEnd = tokens[4];
	if (rangeBegin > normalKeys.end || rangeEnd > normalKeys.end) {
		printUsage(tokens[0]);
		return UID();
	}
	std::string folder = tokens[5].toString();
	std::string dataFile = tokens[6].toString();
	std::string byteSampleFile = tokens[7].toString();
	KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
	BulkLoadState bulkLoadTask(range, type, folder);
	bulkLoadTask.setTransportMethod(method);
	bulkLoadTask.addDataFile(dataFile);
	bulkLoadTask.setByteSampleFile(byteSampleFile);
	UID taskId = wait(triggerBulkLoad(clusterFile, bulkLoadTask, /*timeoutSeconds=*/60));
	if (!taskId.isValid()) {
		printUsage(tokens[0]);
		return UID();
	}
	return taskId;
}

CommandFactory bulkLoadFactory(
    "bulkload",
    CommandHelp("bulkload <Type> <Method> [BeginKey EndKey] <Folder> <DataFile> <ByteSampleFile>",
                "Start a bulk load task",
                "Specify `Type' (only `sst' is supported currently), and\n"
                "Specify `Method' (only `cp' is supported currently).\n"));
} // namespace fdb_cli
