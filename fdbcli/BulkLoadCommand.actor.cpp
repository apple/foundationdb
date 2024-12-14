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

#include <fmt/core.h>

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/IClientApi.h"

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/BulkLoading.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<Void> getBulkLoadTaskStateByRange(Database cx,
                                               KeyRange rangeToRead,
                                               size_t countLimit,
                                               Optional<BulkLoadTaskPhase> phase) {
	try {
		std::vector<BulkLoadTaskState> res = wait(getValidBulkLoadTasksWithinRange(cx, rangeToRead, countLimit, phase));
		int64_t finishCount = 0;
		int64_t unfinishedCount = 0;
		for (const auto& bulkLoadTaskState : res) {
			if (bulkLoadTaskState.phase == BulkLoadTaskPhase::Complete) {
				fmt::println("[Complete]: {}", bulkLoadTaskState.toString());
				++finishCount;
			} else if (bulkLoadTaskState.phase == BulkLoadTaskPhase::Running) {
				fmt::println("[Running]: {}", bulkLoadTaskState.toString());
				++unfinishedCount;
			} else if (bulkLoadTaskState.phase == BulkLoadTaskPhase::Triggered) {
				fmt::println("[Triggered]: {}", bulkLoadTaskState.toString());
				++unfinishedCount;
			} else if (bulkLoadTaskState.phase == BulkLoadTaskPhase::Submitted) {
				fmt::println("[Submitted] {}", bulkLoadTaskState.toString());
				++unfinishedCount;
			} else if (bulkLoadTaskState.phase == BulkLoadTaskPhase::Acknowledged) {
				fmt::println("[Acknowledge] {}", bulkLoadTaskState.toString());
				++finishCount;
			} else {
				UNREACHABLE();
			}
		}
		fmt::println("Finished task count {} of total {} tasks", finishCount, finishCount + unfinishedCount);
	} catch (Error& e) {
		if (e.code() == error_code_timed_out) {
			fmt::println("timed out");
		}
	}
	return Void();
}

ACTOR Future<UID> bulkLoadCommandActor(Reference<IClusterConnectionRecord> clusterFile,
                                       Database cx,
                                       std::vector<StringRef> tokens) {
	if (tokencmp(tokens[1], "mode")) {
		// Set bulk loading mode
		if (tokens.size() != 3) {
			printUsage(tokens[0]);
			return UID();
		}
		if (tokencmp(tokens[2], "on")) {
			int old = wait(setBulkLoadMode(cx, 1));
			TraceEvent("SetBulkLoadModeCommand").detail("OldValue", old).detail("NewValue", 1);
			return UID();
		} else if (tokencmp(tokens[2], "off")) {
			int old = wait(setBulkLoadMode(cx, 0));
			TraceEvent("SetBulkLoadModeCommand").detail("OldValue", old).detail("NewValue", 0);
			return UID();
		} else {
			printUsage(tokens[0]);
			return UID();
		}

	} else if (tokencmp(tokens[1], "acknowledge")) {
		// Acknowledge any completed bulk loading task and clear the corresponding metadata
		if (tokens.size() != 5) {
			printUsage(tokens[0]);
			return UID();
		}
		state UID taskId = UID::fromString(tokens[2].toString());
		Key rangeBegin = tokens[3];
		Key rangeEnd = tokens[4];
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		wait(acknowledgeBulkLoadTask(cx, range, taskId));
		return taskId;

	} else if (tokencmp(tokens[1], "local")) {
		// Generate spec of bulk loading local files and submit the bulk loading task
		if (tokens.size() < 7) {
			printUsage(tokens[0]);
			return UID();
		}
		Key rangeBegin = tokens[2];
		Key rangeEnd = tokens[3];
		// Bulk load can only inject data to normal key space, aka "" ~ \xff
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		std::string folder = tokens[4].toString();
		std::string dataFile = tokens[5].toString();
		std::string byteSampleFile = tokens[6].toString(); // TODO(BulkLoad): reject if the input bytes sampling file is
		                                                   // not same as the configuration as FDB cluster
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		state BulkLoadTaskState bulkLoadTask = newBulkLoadTaskLocalSST(UID(), range, folder, dataFile, byteSampleFile);
		wait(submitBulkLoadTask(cx, bulkLoadTask));
		return bulkLoadTask.getTaskId();

	} else if (tokencmp(tokens[1], "status")) {
		// Get progress of existing bulk loading tasks intersecting the input range
		// TODO(BulkLoad): check status by ID
		if (tokens.size() < 6) {
			printUsage(tokens[0]);
			return UID();
		}
		Key rangeBegin = tokens[2];
		Key rangeEnd = tokens[3];
		// Bulk load can only inject data to normal key space, aka "" ~ \xff
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		std::string inputPhase = tokens[4].toString();
		Optional<BulkLoadTaskPhase> phase;
		if (inputPhase == "all") {
			phase = Optional<BulkLoadTaskPhase>();
		} else if (inputPhase == "submitted") {
			phase = BulkLoadTaskPhase::Submitted;
		} else if (inputPhase == "triggered") {
			phase = BulkLoadTaskPhase::Triggered;
		} else if (inputPhase == "running") {
			phase = BulkLoadTaskPhase::Running;
		} else if (inputPhase == "complete") {
			phase = BulkLoadTaskPhase::Complete;
		} else if (inputPhase == "acknowledged") {
			phase = BulkLoadTaskPhase::Acknowledged;
		} else {
			printUsage(tokens[0]);
			return UID();
		}
		int countLimit = std::stoi(tokens[5].toString());
		wait(getBulkLoadTaskStateByRange(cx, range, countLimit, phase));
		return UID();

	} else {
		printUsage(tokens[0]);
		return UID();
	}
}

CommandFactory bulkLoadFactory(
    "bulkload",
    CommandHelp("bulkload [mode|acknowledge|local|status] [ARGs]",
                "bulkload commands",
                "To set bulkLoad mode: `bulkload mode [on|off]'\n"
                "To acknowledge completed tasks within a range: `bulkload acknowledge <TaskID> <BeginKey> <EndKey>'\n"
                "To trigger a task injecting a SST file from local file system: `bulkload local <BeginKey> <EndKey> "
                "<Folder> <DataFile> <ByteSampleFile>'\n"
                "To get progress of tasks within a range: `bulkload status <BeginKey> <EndKey> "
                "[all|submitted|triggered|running|complete] <limit>'\n"));
} // namespace fdb_cli
