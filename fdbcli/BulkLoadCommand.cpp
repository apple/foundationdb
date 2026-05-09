/*
 * BulkLoadCommand.cpp
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

#include "fdbcli/fdbcli.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/BulkLoading.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/Util.h"

namespace fdb_cli {

static const std::string BULK_LOAD_MODE_USAGE = "To set bulkload mode: bulkload mode [on|off]\n";
static const std::string BULK_LOAD_LOAD_USAGE =
    "To load a range of key/values: bulkload load <JOBID> <BEGINKEY> <ENDKEY> <DIR>\n"
    " where <JOBID> is the id of the bulkdumped job to load, <BEGINKEY> to <ENDKEY>\n"
    " denotes the key/value range to load, and <DIR> is a local directory OR \n"
    " blobstore url to load SST files from.\n";
static const std::string BULK_LOAD_STATUS_USAGE = "To get status: bulkload status\n";
static const std::string BULK_LOAD_CANCEL_USAGE = "To cancel current bulkload job: bulkload cancel <JOBID>\n";
static const std::string BULK_LOAD_HISTORY_USAGE = "To print bulkload job history: bulkload history\n";
static const std::string BULK_LOAD_HISTORY_CLEAR_USAGE = "To clear history: bulkload history clear [all|id]\n";

static const std::string BULKLOAD_ADD_LOCK_OWNER_USAGE =
    "To add a range lock owner: bulkload addlockowner <OWNER_UNIQUE_ID>\n";
static const std::string BULKLOAD_PRINT_LOCK_USAGE = "To print locked ranges: bulkload printlock\n";
static const std::string BULKLOAD_PRINT_LOCK_OWNER_USAGE = "To print range lock owners: bulkload printlockowner\n";
static const std::string BULKLOAD_CLEAR_LOCK_USAGE = "To clear a range lock: bulkload clearlock <OWNER_UNIQUE_ID>\n";

static const std::string BULK_LOAD_HELP_MESSAGE =
    BULK_LOAD_MODE_USAGE + BULK_LOAD_LOAD_USAGE + BULK_LOAD_STATUS_USAGE + BULK_LOAD_CANCEL_USAGE +
    BULK_LOAD_HISTORY_USAGE + BULK_LOAD_HISTORY_CLEAR_USAGE + BULKLOAD_ADD_LOCK_OWNER_USAGE +
    BULKLOAD_PRINT_LOCK_USAGE + BULKLOAD_PRINT_LOCK_OWNER_USAGE + BULKLOAD_CLEAR_LOCK_USAGE;

Future<Void> printPastBulkLoadJob(Database cx) {
	std::vector<BulkLoadJobState> jobs = co_await getBulkLoadJobFromHistory(cx);
	if (jobs.empty()) {
		fmt::println("No bulk loading job in the history");
		co_return;
	}
	for (const auto& job : jobs) {
		ASSERT(job.getPhase() == BulkLoadJobPhase::Complete || job.getPhase() == BulkLoadJobPhase::Error ||
		       job.getPhase() == BulkLoadJobPhase::Cancelled);
		if (!job.getTaskCount().present()) {
			fmt::println("Job {} submitted at {} for range {}. The job has not initialized for {:.1f} mins and exited "
			             "with status {}.",
			             job.getJobId().toString(),
			             formatTimeISO8601(job.getSubmitTime()),
			             job.getJobRange().toString(),
			             (job.getEndTime() - job.getSubmitTime()) / 60.0,
			             convertBulkLoadJobPhaseToString(job.getPhase()));
		} else {
			fmt::println(
			    "Job {} submitted at {} for range {}. The job has {} tasks. The job ran for {:.1f} mins and exited "
			    "with status {}.",
			    job.getJobId().toString(),
			    formatTimeISO8601(job.getSubmitTime()),
			    job.getJobRange().toString(),
			    job.getTaskCount().get(),
			    (job.getEndTime() - job.getSubmitTime()) / 60.0,
			    convertBulkLoadJobPhaseToString(job.getPhase()));
		}
		if (job.getPhase() == BulkLoadJobPhase::Error) {
			Optional<std::string> errorMessage = job.getErrorMessage();
			fmt::println("Error message: {}", errorMessage.present() ? errorMessage.get() : "Not provided.");
		}
	}
}

void printBulkLoadJobTotalTaskCount(Optional<uint64_t> count) {
	if (count.present()) {
		fmt::println("Total {} tasks", count.get());
	} else {
		fmt::println("Total task count is unknown");
	}
	return;
}

Future<Void> printBulkLoadJobProgress(Database cx, BulkLoadJobState job) {
	Transaction tr(cx);
	Key readBegin = job.getJobRange().begin;
	Key readEnd = job.getJobRange().end;
	UID jobId = job.getJobId();
	RangeResult rangeResult;
	size_t completeTaskCount = 0;
	size_t submitTaskCount = 0;
	size_t errorTaskCount = 0;
	Optional<uint64_t> totalTaskCount = job.getTaskCount();
	while (readBegin < readEnd) {
		Error err;
		bool hasErr = false;
		try {
			rangeResult.clear();
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			rangeResult = co_await krmGetRanges(&tr, bulkLoadTaskPrefix, KeyRangeRef(readBegin, readEnd));
			for (int i = 0; i < rangeResult.size() - 1; ++i) {
				if (rangeResult[i].value.empty()) {
					continue;
				}
				BulkLoadTaskState bulkLoadTask = decodeBulkLoadTaskState(rangeResult[i].value);
				if (bulkLoadTask.getJobId() != jobId) {
					fmt::println("Submitted {} tasks", submitTaskCount);
					fmt::println("Finished {} tasks", completeTaskCount);
					fmt::println("Error {} tasks", errorTaskCount);
					printBulkLoadJobTotalTaskCount(totalTaskCount);
					if (bulkLoadTask.phase == BulkLoadPhase::Submitted && bulkLoadTask.getJobId().isValid()) {
						fmt::println("Job {} has been cancelled or has completed", jobId.toString());
					}
					co_return;
				}
				if (bulkLoadTask.phase == BulkLoadPhase::Complete) {
					completeTaskCount = completeTaskCount + bulkLoadTask.getManifests().size();
				} else if (bulkLoadTask.phase == BulkLoadPhase::Error) {
					errorTaskCount = errorTaskCount + bulkLoadTask.getManifests().size();
				}
				submitTaskCount = submitTaskCount + bulkLoadTask.getManifests().size();
			}
			readBegin = rangeResult.back().key;
		} catch (Error& e) {
			err = e;
			hasErr = true;
		}
		if (hasErr) {
			co_await tr.onError(err);
		}
	}
	fmt::println("Submitted {} tasks", submitTaskCount);
	fmt::println("Finished {} tasks", completeTaskCount);
	fmt::println("Error {} tasks", errorTaskCount);
	printBulkLoadJobTotalTaskCount(totalTaskCount);
}

Future<UID> bulkLoadCommandActor(Database cx, std::vector<StringRef> tokens) {
	if (tokencmp(tokens[1], "mode")) {
		if (tokens.size() == 2) {
			int mode = co_await getBulkLoadMode(cx);
			if (mode == 0) {
				fmt::println("Bulkload mode is disabled");
			} else if (mode == 1) {
				fmt::println("Bulkload mode is enabled");
			} else {
				fmt::println("Invalid bulkload mode value {}", mode);
			}
			co_return UID();
		}
		// Set bulk loading mode
		if (tokens.size() != 3) {
			fmt::println("{}", BULK_LOAD_MODE_USAGE);
			co_return UID();
		}
		if (tokencmp(tokens[2], "on")) {
			int old = co_await setBulkLoadMode(cx, 1);
			TraceEvent("SetBulkLoadModeCommand").detail("OldValue", old).detail("NewValue", 1);
			co_return UID();
		} else if (tokencmp(tokens[2], "off")) {
			int old = co_await setBulkLoadMode(cx, 0);
			TraceEvent("SetBulkLoadModeCommand").detail("OldValue", old).detail("NewValue", 0);
			co_return UID();
		} else {
			fmt::println("ERROR: Invalid bulkload mode value {}", tokens[2].toString());
			fmt::println("{}", BULK_LOAD_MODE_USAGE);
			co_return UID();
		}
	} else if (tokencmp(tokens[1], "load")) {
		int mode = co_await getBulkLoadMode(cx);
		if (mode == 0) {
			fmt::println("ERROR: Bulkload mode must be enabled to load data");
			co_return UID();
		}
		if (tokens.size() != 6) {
			fmt::println("{}", BULK_LOAD_LOAD_USAGE);
			co_return UID();
		}
		UID jobId = validateBulkJobId(tokens[2], BULK_LOAD_LOAD_USAGE.c_str());
		Key rangeBegin = tokens[3];
		Key rangeEnd = tokens[4];
		// Bulk load can only inject data to normal key space, aka "" ~ \xff
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			fmt::println(
			    "ERROR: Invalid range: {} to {}, normal key space only", rangeBegin.toString(), rangeEnd.toString());
			fmt::println("{}", BULK_LOAD_LOAD_USAGE);
			co_return UID();
		}
		std::string jobRoot = tokens[5].toString();
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		BulkLoadJobState bulkLoadJob = createBulkLoadJob(
		    jobId,
		    range,
		    jobRoot,
		    jobRoot.find("blobstore://") == 0 ? BulkLoadTransportMethod::BLOBSTORE : BulkLoadTransportMethod::CP);
		co_await submitBulkLoadJob(cx, bulkLoadJob);
		co_return bulkLoadJob.getJobId();
	} else if (tokencmp(tokens[1], "cancel")) {
		if (tokens.size() != 3) {
			fmt::println("{}", BULK_LOAD_CANCEL_USAGE);
			co_return UID();
		}
		UID cancelJobId = validateBulkJobId(tokens[2], BULK_LOAD_CANCEL_USAGE.c_str());
		co_await cancelBulkLoadJob(cx, cancelJobId);
		fmt::println("Job {} has been cancelled. The job range lock has been cleared", cancelJobId.toString());
		co_return UID();

	} else if (tokencmp(tokens[1], "status")) {
		if (tokens.size() != 2) {
			fmt::println("{}", BULK_LOAD_STATUS_USAGE);
			co_return UID();
		}

		// Get aggregated progress
		Optional<BulkLoadProgress> progressOpt = co_await getBulkLoadProgress(cx);
		if (!progressOpt.present()) {
			fmt::println("No bulk loading job is running");
			co_return UID();
		}

		BulkLoadProgress progress = progressOpt.get();

		fmt::println("BulkLoad job {} is in progress.", progress.jobId.toString());

		std::string ownerSuffix = co_await getBulkOwnerSuffix(cx, progress.jobId, false);

		fmt::println(" Range: {}{}", progress.jobRange.toString(), ownerSuffix);
		fmt::println("");
		fmt::println("Progress:");

		int totalTasks = progress.submittedTasks + progress.triggeredTasks + progress.runningTasks +
		                 progress.completeTasks + progress.errorTasks;
		if (totalTasks > 0) {
			fmt::println(" Tasks: {}/{} complete ({:.1f}%)  |  {} submitted, {} triggered, {} running{}",
			             progress.completeTasks,
			             totalTasks,
			             100.0 * progress.completeTasks / totalTasks,
			             progress.submittedTasks,
			             progress.triggeredTasks,
			             progress.runningTasks,
			             progress.errorTasks > 0 ? fmt::format(", {} error", progress.errorTasks) : "");
		}

		fmt::println(" Bytes: {}", formatBytesProgress(progress.completedBytes, progress.totalBytes));

		printProgressMetrics(progress.avgBytesPerSecond(), progress.etaSeconds(), progress.elapsedSeconds);

		// Only show warnings for actual problems (stalled tasks, errors)
		printStalledTasks(progress.stalledTasks);
		if (progress.errorTasks > 0) {
			fmt::println("");
			fmt::println("WARNING: {} tasks in error state", progress.errorTasks);
		}

		co_return UID();

	} else if (tokencmp(tokens[1], "history")) {
		if (tokens.size() == 2) {
			co_await printPastBulkLoadJob(cx);
			co_return UID();
		}
		if (tokens.size() == 3) {
			if (tokencmp(tokens[2], "clear")) {
				fmt::println("{}", BULK_LOAD_HISTORY_CLEAR_USAGE);
				co_return UID();
			} else {
				fmt::println("ERROR: Invalid history option {}", tokens[2].toString());
				fmt::println("{}", BULK_LOAD_HISTORY_CLEAR_USAGE);
				co_return UID();
			}
		}
		if (tokens.size() == 4) {
			if (tokencmp(tokens[2], "clear")) {
				if (tokencmp(tokens[3], "all")) {
					co_await clearBulkLoadJobHistory(cx);
					fmt::println("All bulkload job history has been cleared");
					co_return UID();
				} else {
					fmt::println("ERROR: Invalid history clear option {}", tokens[3].toString());
					fmt::println("{}", BULK_LOAD_HISTORY_CLEAR_USAGE);
					co_return UID();
				}
			} else {
				fmt::println("ERROR: Invalid history clear option {}", tokens[2].toString());
				fmt::println("{}", BULK_LOAD_HISTORY_CLEAR_USAGE);
				co_return UID();
			}
		}
		if (tokens.size() == 5) {
			if (tokencmp(tokens[2], "clear") && tokencmp(tokens[3], "id")) {
				UID jobId = validateBulkJobId(tokens[4], BULK_LOAD_HISTORY_CLEAR_USAGE.c_str());
				co_await clearBulkLoadJobHistory(cx, jobId);
				fmt::println("Bulkload job {} has been cleared from history", jobId.toString());
				co_return jobId;
			}
		}
		printLongDesc(tokens[0]);
		co_return UID();

	} else if (tokencmp(tokens[1], "addlockowner")) {
		// For debugging purposes and invisible to users.
		if (tokens.size() != 3) {
			fmt::println("{}", BULK_LOAD_STATUS_USAGE);
			co_return UID();
		}
		std::string ownerUniqueID = tokens[2].toString();
		if (ownerUniqueID.empty()) {
			fmt::println("ERROR: Owner unique id cannot be empty");
			fmt::println("{}", BULKLOAD_ADD_LOCK_OWNER_USAGE);
			co_return UID();
		}
		co_await registerRangeLockOwner(cx, ownerUniqueID, ownerUniqueID);
		co_return UID();

	} else if (tokencmp(tokens[1], "printlock")) {
		// For debugging purposes and invisible to users.
		if (tokens.size() != 2) {
			fmt::println("{}", BULKLOAD_PRINT_LOCK_USAGE);
			co_return UID();
		}
		std::vector<std::pair<KeyRange, RangeLockState>> lockedRanges =
		    co_await findExclusiveReadLockOnRange(cx, normalKeys);
		fmt::println("Total {} locked ranges", lockedRanges.size());
		if (lockedRanges.size() > 10) {
			fmt::println("First 10 locks are:");
		}
		int count = 1;
		for (const auto& lock : lockedRanges) {
			if (count > 10) {
				break;
			}
			fmt::println("Lock {} on {} for {}", count, lock.first.toString(), lock.second.toString());
			count++;
		}
		co_return UID();

	} else if (tokencmp(tokens[1], "printlockowner")) {
		// For debugging purposes and invisible to users.
		if (tokens.size() != 2) {
			fmt::println("{}", BULKLOAD_PRINT_LOCK_OWNER_USAGE);
			co_return UID();
		}
		std::vector<RangeLockOwner> owners = co_await getAllRangeLockOwners(cx);
		for (const auto& owner : owners) {
			fmt::println("{}", owner.toString());
		}
		co_return UID();

	} else if (tokencmp(tokens[1], "clearlock")) {
		// For debugging purposes and invisible to users.
		if (tokens.size() != 3) {
			fmt::println("{}", BULKLOAD_CLEAR_LOCK_USAGE);
			co_return UID();
		}
		std::string ownerUniqueID = tokens[2].toString();
		co_await releaseExclusiveReadLockByUser(cx, ownerUniqueID);
		co_return UID();

	} else {
		printUsage(tokens[0]);
		printLongDesc(tokens[0]);
		co_return UID();
	}
}

CommandFactory bulkLoadFactory("bulkload",
                               CommandHelp("bulkload [mode|load|status|cancel|history] [ARGs]",
                                           "bulkload commands",
                                           BULK_LOAD_HELP_MESSAGE.c_str()));
} // namespace fdb_cli
