/*
 * BulkDumpCommand.actor.cpp
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

#include <cstddef>
#include <fmt/core.h>
#include "fdbcli/fdbcli.actor.h"
#include "fdbclient/BulkDumping.h"
#include "fdbclient/BulkLoading.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> getOngoingBulkDumpJob(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			Optional<UID> jobId = wait(getAliveBulkDumpJob(&tr));
			if (jobId.present()) {
				fmt::println("Running bulk dumping job: {}", jobId.get().toString());
				return true;
			} else {
				fmt::println("No bulk dumping job is running");
				return false;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> getBulkDumpCompleteRanges(Database cx, KeyRange rangeToRead) {
	try {
		size_t finishCount = wait(getBulkDumpCompleteTaskCount(cx, rangeToRead));
		fmt::println("Finished {} tasks", finishCount);
	} catch (Error& e) {
		if (e.code() == error_code_timed_out) {
			fmt::println("timed out");
		}
	}
	return Void();
}

ACTOR Future<UID> bulkDumpCommandActor(Reference<IClusterConnectionRecord> clusterFile,
                                       Database cx,
                                       std::vector<StringRef> tokens) {
	state BulkDumpState bulkDumpJob;
	if (tokencmp(tokens[1], "mode")) {
		if (tokens.size() != 2 && tokens.size() != 3) {
			printUsage(tokens[0]);
			return UID();
		}
		if (tokens.size() == 2) {
			int old = wait(getBulkDumpMode(cx));
			if (old == 0) {
				fmt::println("Bulk dump is disabled");
			} else if (old == 1) {
				fmt::println("Bulk dump is enabled");
			} else {
				fmt::println("Invalid mode value {}", old);
			}
			return UID();
		}
		ASSERT(tokens.size() == 3);
		if (tokencmp(tokens[2], "on")) {
			int old = wait(setBulkDumpMode(cx, 1));
			TraceEvent("SetBulkDumpModeCommand").detail("OldValue", old).detail("NewValue", 1);
			return UID();
		} else if (tokencmp(tokens[2], "off")) {
			int old = wait(setBulkDumpMode(cx, 0));
			TraceEvent("SetBulkDumpModeCommand").detail("OldValue", old).detail("NewValue", 0);
			return UID();
		} else {
			printUsage(tokens[0]);
			return UID();
		}

	} else if (tokencmp(tokens[1], "local")) {
		if (tokens.size() != 5) {
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
		std::string remoteRoot = tokens[4].toString();
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		bulkDumpJob = createBulkDumpJob(range, remoteRoot, BulkLoadType::SST, BulkLoadTransportMethod::CP);
		wait(submitBulkDumpJob(cx, bulkDumpJob));
		return bulkDumpJob.getJobId();

	} else if (tokencmp(tokens[1], "blobstore")) {
		if (tokens.size() != 5) {
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
		std::string remoteRoot = tokens[4].toString();
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		bulkDumpJob = createBulkDumpJob(range, remoteRoot, BulkLoadType::SST, BulkLoadTransportMethod::BLOBSTORE);
		wait(submitBulkDumpJob(cx, bulkDumpJob));
		return bulkDumpJob.getJobId();

	} else if (tokencmp(tokens[1], "clear")) {
		if (tokens.size() != 3) {
			printUsage(tokens[0]);
			return UID();
		}
		state UID jobId = UID::fromString(tokens[2].toString());
		wait(clearBulkDumpJob(cx, jobId));
		fmt::println("Job {} has been cleared. No task will be spawned.", jobId.toString());
		return UID();

	} else if (tokencmp(tokens[1], "status")) {
		if (tokens.size() != 4) {
			printUsage(tokens[0]);
			return UID();
		}
		bool anyJob = wait(getOngoingBulkDumpJob(cx));
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
		wait(getBulkDumpCompleteRanges(cx, range));
		return UID();

	} else {
		printUsage(tokens[0]);
		return UID();
	}
}

CommandFactory bulkDumpFactory(
    "bulkdump",
    CommandHelp("bulkdump [mode|local|blobstore|clear|status] [ARGs]",
                "bulkdump commands",
                "To set bulkdump mode: `bulkdump mode [on|off]'\n"
                "To dump a range to SST files: `bulkdump [local|blobstore] <BeginKey> <EndKey> dumpFolder`\n"
                "To clear current bulkdump job: `bulkdump clear <JobID>`\n"
                "To get completed bulkdump ranges: `bulkdump status <BeginKey> <EndKey>`\n"));
} // namespace fdb_cli
