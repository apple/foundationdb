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

ACTOR Future<Void> getBulkLoadTaskStateByRange(Database cx,
                                               KeyRange rangeToRead,
                                               size_t countLimit,
                                               Optional<BulkLoadPhase> phase) {
	try {
		std::vector<BulkLoadTaskState> res = wait(getBulkLoadTasksWithinRange(cx, rangeToRead, countLimit, phase));
		int64_t finishCount = 0;
		int64_t unfinishedCount = 0;
		for (const auto& bulkLoadTaskState : res) {
			if (bulkLoadTaskState.phase == BulkLoadPhase::Complete) {
				fmt::println("[Complete]: {}", bulkLoadTaskState.toString());
				++finishCount;
			} else if (bulkLoadTaskState.phase == BulkLoadPhase::Running) {
				fmt::println("[Running]: {}", bulkLoadTaskState.toString());
				++unfinishedCount;
			} else if (bulkLoadTaskState.phase == BulkLoadPhase::Triggered) {
				fmt::println("[Triggered]: {}", bulkLoadTaskState.toString());
				++unfinishedCount;
			} else if (bulkLoadTaskState.phase == BulkLoadPhase::Submitted) {
				fmt::println("[Submitted] {}", bulkLoadTaskState.toString());
				++unfinishedCount;
			} else if (bulkLoadTaskState.phase == BulkLoadPhase::Acknowledged) {
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

ACTOR Future<bool> getOngoingBulkLoadJob(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			Optional<BulkLoadJobState> job = wait(getAliveBulkLoadJob(&tr));
			if (job.present()) {
				fmt::println("Running bulk loading job: {}", job.get().getJobId().toString());
				return true;
			} else {
				fmt::println("No bulk loading job is running");
				return false;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> getBulkLoadCompleteRanges(Database cx, KeyRange rangeToRead) {
	try {
		size_t finishCount = wait(getBulkLoadCompleteTaskCount(cx, rangeToRead));
		fmt::println("Finished {} tasks", finishCount);
	} catch (Error& e) {
		if (e.code() == error_code_timed_out) {
			fmt::println("timed out");
		}
	}
	return Void();
}

ACTOR Future<UID> bulkLoadUnitTestCommandActor(Database cx, std::vector<StringRef> tokens) {
	if (tokencmp(tokens[2], "acknowledge")) {
		// Acknowledge any completed bulk loading task and clear the corresponding metadata
		if (tokens.size() != 6) {
			printUsage(tokens[0]);
			return UID();
		}
		state UID taskId = UID::fromString(tokens[2].toString());
		Key rangeBegin = tokens[4];
		Key rangeEnd = tokens[5];
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		wait(finalizeBulkLoadTask(cx, range, taskId));
		return taskId;

	} else if (tokencmp(tokens[2], "local")) {
		// Generate spec of bulk loading local files and submit the bulk loading task.
		// This is used for testing of bulkload task engine.
		// Therefore, some information of manifest is ignored.
		if (tokens.size() < 8) {
			printUsage(tokens[0]);
			return UID();
		}
		Key rangeBegin = tokens[3];
		Key rangeEnd = tokens[4];
		// Bulk load can only inject data to normal key space, aka "" ~ \xff
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		std::string folder = tokens[5].toString();
		std::string dataFile = tokens[6].toString();
		std::string byteSampleFile = tokens[7].toString();
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		BulkLoadFileSet fileSet =
		    BulkLoadFileSet(folder, "", generateEmptyManifestFileName(), dataFile, byteSampleFile, BulkLoadChecksum());
		state BulkLoadTaskState bulkLoadTask =
		    createBulkLoadTask(deterministicRandom()->randomUniqueID(),
		                       range,
		                       fileSet,
		                       BulkLoadByteSampleSetting(0, "hashlittle2", 0, 0, 0), // We fake it here
		                       /*snapshotVersion=*/invalidVersion,
		                       /*bytes=*/-1,
		                       /*keyCount=*/-1,
		                       BulkLoadType::SST,
		                       BulkLoadTransportMethod::CP);
		wait(submitBulkLoadTask(cx, bulkLoadTask));
		return bulkLoadTask.getTaskId();

	} else if (tokencmp(tokens[2], "status")) {
		// Get progress of existing bulk loading tasks intersecting the input range
		if (tokens.size() < 7) {
			printUsage(tokens[0]);
			return UID();
		}
		Key rangeBegin = tokens[3];
		Key rangeEnd = tokens[4];
		// Bulk load can only inject data to normal key space, aka "" ~ \xff
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		std::string inputPhase = tokens[5].toString();
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
		int countLimit = std::stoi(tokens[6].toString());
		wait(getBulkLoadTaskStateByRange(cx, range, countLimit, phase));
		return UID();

	} else {
		printUsage(tokens[0]);
		return UID();
	}
}

ACTOR Future<UID> bulkLoadCommandActor(Database cx, std::vector<StringRef> tokens) {
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
	} else if (tokencmp(tokens[1], "unittest")) {
		// For internal unit test only
		UID taskId = wait(bulkLoadUnitTestCommandActor(cx, tokens));
		return taskId;

	} else if (tokencmp(tokens[1], "local")) {
		if (tokens.size() != 6) {
			printUsage(tokens[0]);
			return UID();
		}
		UID jobId = UID::fromString(tokens[2].toString());
		Key rangeBegin = tokens[3];
		Key rangeEnd = tokens[4];
		// Bulk load can only inject data to normal key space, aka "" ~ \xff
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		std::string jobRoot = tokens[5].toString();
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		state BulkLoadJobState bulkLoadJob = createBulkLoadJob(jobId, range, jobRoot, BulkLoadTransportMethod::CP);
		wait(submitBulkLoadJob(cx, bulkLoadJob));
		return bulkLoadJob.getJobId();

	} else if (tokencmp(tokens[1], "blobstore")) {
		if (tokens.size() != 6) {
			printUsage(tokens[0]);
			return UID();
		}
		UID jobId = UID::fromString(tokens[2].toString());
		Key rangeBegin = tokens[3];
		Key rangeEnd = tokens[4];
		// Bulk load can only inject data to normal key space, aka "" ~ \xff
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		std::string jobRoot = tokens[5].toString();
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		BulkLoadJobState bulkLoadJob = createBulkLoadJob(jobId, range, jobRoot, BulkLoadTransportMethod::BLOBSTORE);
		wait(submitBulkLoadJob(cx, bulkLoadJob));
		return bulkLoadJob.getJobId();

	} else if (tokencmp(tokens[1], "clear")) {
		if (tokens.size() != 3) {
			printUsage(tokens[0]);
			return UID();
		}
		state UID jobId = UID::fromString(tokens[2].toString());
		wait(clearBulkLoadJob(cx, jobId));
		fmt::println("Job {} has been cleared. No task will be spawned.", jobId.toString());
		return UID();

	} else if (tokencmp(tokens[1], "status")) {
		if (tokens.size() != 4) {
			printUsage(tokens[0]);
			return UID();
		}
		bool anyJob = wait(getOngoingBulkLoadJob(cx));
		if (!anyJob) {
			return UID();
		}
		Key rangeBegin = tokens[2];
		Key rangeEnd = tokens[3];
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		wait(getBulkLoadCompleteRanges(cx, range));
		return UID();

	} else {
		printUsage(tokens[0]);
		return UID();
	}
}

CommandFactory bulkLoadFactory(
    "bulkload",
    CommandHelp(
        "bulkload [mode|local|blobstore|status|unittest] [ARGs]",
        "bulkload commands",
        "To set bulkload mode: `bulkload mode [on|off]'\n"
        "To load a range from SST files: `bulkload [local|blobstore] <BeginKey> <EndKey> dumpFolder`\n"
        "To clear current bulkload job: `bulkload clear <JobID>`\n"
        "To get completed bulkload ranges: `bulkload status <BeginKey> <EndKey>`\n"
        "BulkLoad tasks are operated when setting unittest.\n"
        "To acknowledge completed tasks within a range: `bulkload unittest acknowledge <TaskID> <BeginKey> <EndKey>'\n"
        "To trigger a task injecting a SST file from local file system: `bulkload unittest local <BeginKey> <EndKey> "
        "<Folder> <DataFile> <ByteSampleFile>'\n"
        "To get progress of tasks within a range: `bulkload unittest status <BeginKey> <EndKey> "
        "[all|submitted|triggered|running|complete] <limit>'\n"));
} // namespace fdb_cli
