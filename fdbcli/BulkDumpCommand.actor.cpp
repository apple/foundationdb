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
#include "flow/Util.h"
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

ACTOR Future<bool> getOngoingBulkDumpJob(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			Optional<BulkDumpState> job = wait(getSubmittedBulkDumpJob(&tr));
			if (job.present()) {
				fmt::println("Running bulk dumping job: {}", job.get().getJobId().toString());
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

ACTOR Future<UID> bulkDumpCommandActor(Database cx, std::vector<StringRef> tokens) {
	state BulkDumpState bulkDumpJob;
	if (tokencmp(tokens[1], "mode")) {
		if (tokens.size() != 2 && tokens.size() != 3) {
			fmt::println("{}", BULK_DUMP_MODE_USAGE);
			return UID();
		}
		if (tokens.size() == 2) {
			int mode = wait(getBulkDumpMode(cx));
			if (mode == 0) {
				fmt::println("Bulkdump mode is disabled");
			} else if (mode == 1) {
				fmt::println("Bulkdump mode is enabled");
			} else {
				fmt::println("Invalid bulkload mode value {}", mode);
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
			fmt::println("ERROR: Invalid bulkdump mode value {}", tokens[2].toString());
			fmt::println("{}", BULK_DUMP_MODE_USAGE);
			return UID();
		}

	} else if (tokencmp(tokens[1], "dump")) {
		int mode = wait(getBulkDumpMode(cx));
		if (mode == 0) {
			fmt::println("ERROR: Bulkdump mode must be enabled to dump data");
			return UID();
		}
		if (tokens.size() != 5) {
			fmt::println("{}", BULK_DUMP_DUMP_USAGE);
			return UID();
		}
		Key rangeBegin = tokens[2];
		Key rangeEnd = tokens[3];
		// Bulk load can only inject data to normal key space, aka "" ~ \xff
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			fmt::println(
			    "ERROR: Invalid range: {} to {}, normal key space only", rangeBegin.toString(), rangeEnd.toString());
			fmt::println("{}", BULK_DUMP_DUMP_USAGE);
			return UID();
		}
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		std::string jobRoot = tokens[4].toString();
		bulkDumpJob = createBulkDumpJob(range,
		                                jobRoot,
		                                BulkLoadType::SST,
		                                jobRoot.find("blobstore://") == 0 ? BulkLoadTransportMethod::BLOBSTORE
		                                                                  : BulkLoadTransportMethod::CP);
		wait(submitBulkDumpJob(cx, bulkDumpJob));
		return bulkDumpJob.getJobId();
	} else if (tokencmp(tokens[1], "cancel")) {
		if (tokens.size() != 3) {
			fmt::println("{}", BULK_DUMP_CANCEL_USAGE);
			return UID();
		}
		state UID jobId = UID::fromString(tokens[2].toString());
		wait(cancelBulkDumpJob(cx, jobId));
		fmt::println("Job {} has been cancelled. No new tasks will be spawned.", jobId.toString());
		return UID();

	} else if (tokencmp(tokens[1], "status")) {
		if (tokens.size() != 2) {
			fmt::println("{}", BULK_DUMP_STATUS_USAGE);
			return UID();
		}

		// Get aggregated progress
		Optional<BulkDumpProgress> progressOpt = wait(getBulkDumpProgress(cx));
		if (!progressOpt.present()) {
			fmt::println("No bulk dumping job is running");
			return UID();
		}

		state BulkDumpProgress progress = progressOpt.get();

		// Check if this bulkdump is owned by a backup
		state Transaction tr(cx);
		state Optional<BulkDumpState> jobState;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<BulkDumpState> job = wait(getSubmittedBulkDumpJob(&tr));
				jobState = job;
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		if (jobState.present() && jobState.get().hasOwner()) {
			std::string ownerType = jobState.get().getOwnerType().orDefault("unknown");
			std::string ownerName = jobState.get().getOwnerName().orDefault("");
			fmt::println("BulkDump job {} is owned by {} '{}'.", progress.jobId.toString(), ownerType, ownerName);
			if (ownerType == "backup") {
				fmt::println("For full status, use: fdbbackup status -t {}", ownerName);
			}
			return UID();
		}

		// Format standalone bulkdump progress
		fmt::println("BulkDump job {} is in progress.", progress.jobId.toString());
		fmt::println(" Range: {}", progress.jobRange.toString());
		fmt::println("");
		fmt::println("Progress:");
		fmt::println(" Tasks completed - {} / {} ({:.1f}%)",
		             progress.completeTasks,
		             progress.totalTasks,
		             progress.progressPercent());

		fmt::println(" Bytes completed - {} ({}) / {} ({})",
		             progress.completedBytes,
		             formatBytesHumanReadable(progress.completedBytes),
		             progress.totalBytes,
		             formatBytesHumanReadable(progress.totalBytes));

		double throughput = progress.avgBytesPerSecond();
		if (throughput > 0) {
			fmt::println(" Throughput - {:.1f} MB/s", throughput / 1048576.0);
		}

		if (progress.etaSeconds().present()) {
			fmt::println(" Estimated time remaining - {}",
			             formatDurationHumanReadable((int)progress.etaSeconds().get()));
		}

		if (progress.elapsedSeconds > 0) {
			fmt::println(" Elapsed time - {}", formatDurationHumanReadable((int)progress.elapsedSeconds));
		}

		// Show stalled tasks as warnings
		if (!progress.stalledTasks.empty()) {
			fmt::println("");
			fmt::println("WARNING: {} stalled tasks (no progress > 60s):", progress.stalledTasks.size());
			for (const auto& stalled : progress.stalledTasks) {
				fmt::println(" Task {}: {}, stalled {:.0f}s, {} restarts",
				             stalled.taskId.shortString(),
				             stalled.range.toString(),
				             stalled.stalledSeconds,
				             stalled.restartCount);
				if (!stalled.lastError.empty()) {
					fmt::println("           Last error: {}", stalled.lastError);
				}
			}
		}

		if (progress.errorTasks > 0) {
			fmt::println("");
			fmt::println("WARNING: {} tasks in error state", progress.errorTasks);
		}

		return UID();

	} else {
		printUsage(tokens[0]);
		printLongDesc(tokens[0]);
		return UID();
	}
}

CommandFactory bulkDumpFactory("bulkdump",
                               CommandHelp("bulkdump [mode|dump|status|cancel] [ARGs]",
                                           "bulkdump commands",
                                           BULK_DUMP_HELP_MESSAGE.c_str()));
} // namespace fdb_cli
