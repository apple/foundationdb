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
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<Void> getBulkLoadStateByRange(Database cx,
                                           KeyRange rangeToRead,
                                           size_t countLimit,
                                           Optional<BulkLoadPhase> phase) {
	try {
		std::vector<BulkLoadState> res = wait(getValidBulkLoadTasksWithinRange(cx, rangeToRead, countLimit, phase));
		int64_t finishCount = 0;
		int64_t unfinishedCount = 0;
		for (const auto& bulkLoadState : res) {
			if (bulkLoadState.phase == BulkLoadPhase::Complete) {
				fmt::println("[Complete]: {}", bulkLoadState.toString());
				++finishCount;
			} else if (bulkLoadState.phase == BulkLoadPhase::Running) {
				fmt::println("[Running]: {}", bulkLoadState.toString());
				++unfinishedCount;
			} else if (bulkLoadState.phase == BulkLoadPhase::Triggered) {
				fmt::println("[Triggered]: {}", bulkLoadState.toString());
				++unfinishedCount;
			} else if (bulkLoadState.phase == BulkLoadPhase::Submitted) {
				fmt::println("[Submitted] {}", bulkLoadState.toString());
				++unfinishedCount;
			} else if (bulkLoadState.phase == BulkLoadPhase::Acknowledged) {
				fmt::println("[Acknowledge] {}", bulkLoadState.toString());
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
		state BulkLoadState bulkLoadTask = newBulkLoadTaskLocalSST(range, folder, dataFile, byteSampleFile);
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
		Optional<BulkLoadPhase> phase;
		if (inputPhase == "all") {
			phase = Optional<BulkLoadPhase>();
		} else if (inputPhase == "submitted") {
			phase = BulkLoadPhase::Submitted;
		} else if (inputPhase == "triggered") {
			phase = BulkLoadPhase::Triggered;
		} else if (inputPhase == "running") {
			phase = BulkLoadPhase::Running;
		} else if (inputPhase == "complete") {
			phase = BulkLoadPhase::Complete;
		} else if (inputPhase == "acknowledged") {
			phase = BulkLoadPhase::Acknowledged;
		} else {
			printUsage(tokens[0]);
			return UID();
		}
		int countLimit = std::stoi(tokens[5].toString());
		wait(getBulkLoadStateByRange(cx, range, countLimit, phase));
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
