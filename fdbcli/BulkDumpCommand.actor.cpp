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
			Optional<UID> jobId = wait(existAnyBulkDumpTask(&tr));
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
	if (tokencmp(tokens[1], "mode")) {
		// Set bulk dumping mode
		if (tokens.size() != 3) {
			printUsage(tokens[0]);
			return UID();
		}
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
		if (rangeBegin > normalKeys.end || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		std::string remoteRoot = tokens[4].toString();
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		state BulkDumpState bulkDumpJob = newBulkDumpTaskLocalSST(range, remoteRoot);
		wait(submitBulkDumpJob(cx, bulkDumpJob));
		return bulkDumpJob.getJobId();

	} else if (tokencmp(tokens[1], "clear")) {
		if (tokens.size() != 2) {
			printUsage(tokens[0]);
			return UID();
		}
		wait(clearBulkDumpJob(cx));
		fmt::println("Metadata cleared. No task will be spawned.");
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
		if (rangeBegin > normalKeys.end || rangeEnd > normalKeys.end) {
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
    CommandHelp("bulkdump [mode|local|clear|status] [ARGs]",
                "bulkdump commands",
                "To set bulkdump mode: `bulkdump mode [on|off]'\n"
                "To dump a range to local path in SST files: `bulkdump local <BeginKey> <EndKey> dumpFolder\n"
                "To clear current bulkdump job: `bulkdump clear\n"
                "To get completed bulkdump ranges: `bulkdump status <BeginKey> <EndKey>\n"));
} // namespace fdb_cli
