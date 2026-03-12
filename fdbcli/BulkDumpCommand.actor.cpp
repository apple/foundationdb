/*
 * BulkDumpCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/Arena.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

static const char* BULK_DUMP_MODE_USAGE = "To set bulkdump mode: bulkdump mode [on|off]\n";
static const char* BULK_DUMP_DUMP_USAGE = "To dump a range of key/values: bulkdump dump <BEGINKEY> <ENDKEY> <DIR>\n"
                                          " where <BEGINKEY> to <ENDKEY> denotes the key/value range and <DIR> is\n"
                                          " a local directory OR blobstore url to dump SST files to.\n";
static const char* BULK_DUMP_STATUS_USAGE = "To get status: bulkdump status\n";
static const char* BULK_DUMP_CANCEL_USAGE = "To cancel current bulkdump job: bulkdump cancel <JOBID>\n";

static const std::string BULK_DUMP_HELP_MESSAGE =
    std::string(BULK_DUMP_MODE_USAGE) + std::string(BULK_DUMP_DUMP_USAGE) + std::string(BULK_DUMP_STATUS_USAGE) +
    std::string(BULK_DUMP_CANCEL_USAGE);

Future<bool> getOngoingBulkDumpJob(Database cx) {
	Transaction tr(cx);
	loop {
		Error err;
		try {
			Optional<BulkDumpState> job = co_await getSubmittedBulkDumpJob(&tr);
			if (job.present()) {
				fmt::println("Running bulk dumping job: {}", job.get().getJobId().toString());
				co_return true;
			} else {
				fmt::println("No bulk dumping job is running");
				co_return false;
			}
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<Void> getBulkDumpCompleteRanges(Database cx, KeyRange rangeToRead) {
	try {
		size_t finishCount = co_await getBulkDumpCompleteTaskCount(cx, rangeToRead);
		fmt::println("Finished {} tasks", finishCount);
	} catch (Error& e) {
		if (e.code() == error_code_timed_out) {
			fmt::println("timed out");
		}
	}
	co_return;
}

Future<UID> bulkDumpCommandActor(Database cx, std::vector<StringRef> tokens) {
	BulkDumpState bulkDumpJob;
	if (tokencmp(tokens[1], "mode")) {
		if (tokens.size() != 2 && tokens.size() != 3) {
			fmt::println("{}", BULK_DUMP_MODE_USAGE);
			co_return UID();
		}
		if (tokens.size() == 2) {
			int mode = co_await getBulkDumpMode(cx);
			if (mode == 0) {
				fmt::println("Bulkdump mode is disabled");
			} else if (mode == 1) {
				fmt::println("Bulkdump mode is enabled");
			} else {
				fmt::println("Invalid bulkload mode value {}", mode);
			}
			co_return UID();
		}
		ASSERT(tokens.size() == 3);
		if (tokencmp(tokens[2], "on")) {
			int old = co_await setBulkDumpMode(cx, 1);
			TraceEvent("SetBulkDumpModeCommand").detail("OldValue", old).detail("NewValue", 1);
			co_return UID();
		} else if (tokencmp(tokens[2], "off")) {
			int old = co_await setBulkDumpMode(cx, 0);
			TraceEvent("SetBulkDumpModeCommand").detail("OldValue", old).detail("NewValue", 0);
			co_return UID();
		} else {
			fmt::println("ERROR: Invalid bulkdump mode value {}", tokens[2].toString());
			fmt::println("{}", BULK_DUMP_MODE_USAGE);
			co_return UID();
		}

	} else if (tokencmp(tokens[1], "dump")) {
		int mode = co_await getBulkDumpMode(cx);
		if (mode == 0) {
			fmt::println("ERROR: Bulkdump mode must be enabled to dump data");
			co_return UID();
		}
		if (tokens.size() != 5) {
			fmt::println("{}", BULK_DUMP_DUMP_USAGE);
			co_return UID();
		}
		Key rangeBegin = tokens[2];
		Key rangeEnd = tokens[3];
		// Bulk load can only inject data to normal key space, aka "" ~ \xff
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			fmt::println(
			    "ERROR: Invalid range: {} to {}, normal key space only", rangeBegin.toString(), rangeEnd.toString());
			fmt::println("{}", BULK_DUMP_DUMP_USAGE);
			co_return UID();
		}
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		std::string jobRoot = tokens[4].toString();
		bulkDumpJob = createBulkDumpJob(range,
		                                jobRoot,
		                                BulkLoadType::SST,
		                                jobRoot.find("blobstore://") == 0 ? BulkLoadTransportMethod::BLOBSTORE
		                                                                  : BulkLoadTransportMethod::CP);
		co_await submitBulkDumpJob(cx, bulkDumpJob);
		co_return bulkDumpJob.getJobId();
	} else if (tokencmp(tokens[1], "cancel")) {
		if (tokens.size() != 3) {
			fmt::println("{}", BULK_DUMP_CANCEL_USAGE);
			co_return UID();
		}
		UID jobId = UID::fromString(tokens[2].toString());
		co_await cancelBulkDumpJob(cx, jobId);
		fmt::println("Job {} has been cancelled. No new tasks will be spawned.", jobId.toString());
		co_return UID();

	} else if (tokencmp(tokens[1], "status")) {
		if (tokens.size() != 2) {
			fmt::println("{}", BULK_DUMP_STATUS_USAGE);
			co_return UID();
		}
		bool anyJob = co_await getOngoingBulkDumpJob(cx);
		if (!anyJob) {
			co_return UID();
		}
		KeyRange range = Standalone(KeyRangeRef(normalKeys.begin, normalKeys.end));
		co_await getBulkDumpCompleteRanges(cx, range);
		co_return UID();

	} else {
		printUsage(tokens[0]);
		printLongDesc(tokens[0]);
		co_return UID();
	}
}

CommandFactory bulkDumpFactory("bulkdump",
                               CommandHelp("bulkdump [mode|dump|status|cancel] [ARGs]",
                                           "bulkdump commands",
                                           BULK_DUMP_HELP_MESSAGE.c_str()));
} // namespace fdb_cli
