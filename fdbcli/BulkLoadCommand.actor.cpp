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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/BulkLoading.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

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

ACTOR Future<Void> printPastBulkLoadJob(Database cx) {
	std::vector<BulkLoadJobState> jobs = wait(getBulkLoadJobFromHistory(cx));
	if (jobs.empty()) {
		fmt::println("No bulk loading job in the history");
		return Void();
	}
	for (const auto& job : jobs) {
		ASSERT(job.getPhase() == BulkLoadJobPhase::Complete || job.getPhase() == BulkLoadJobPhase::Error ||
		       job.getPhase() == BulkLoadJobPhase::Cancelled);
		if (!job.getTaskCount().present()) {
			fmt::println("Job {} submitted at {} for range {}. The job has not initialized for {} mins and exited "
			             "with status {}.",
			             job.getJobId().toString(),
			             std::to_string(job.getSubmitTime()),
			             job.getJobRange().toString(),
			             std::to_string((job.getEndTime() - job.getSubmitTime()) / 60.0),
			             convertBulkLoadJobPhaseToString(job.getPhase()));
		} else {
			fmt::println(
			    "Job {} submitted at {} for range {}. The job has {} tasks. The job ran for {} mins and exited "
			    "with status {}.",
			    job.getJobId().toString(),
			    std::to_string(job.getSubmitTime()),
			    job.getJobRange().toString(),
			    job.getTaskCount().get(),
			    std::to_string((job.getEndTime() - job.getSubmitTime()) / 60.0),
			    convertBulkLoadJobPhaseToString(job.getPhase()));
		}
		if (job.getPhase() == BulkLoadJobPhase::Error) {
			Optional<std::string> errorMessage = job.getErrorMessage();
			fmt::println("Error message: {}", errorMessage.present() ? errorMessage.get() : "Not provided.");
		}
	}
	return Void();
}

void printBulkLoadJobTotalTaskCount(Optional<uint64_t> count) {
	if (count.present()) {
		fmt::println("Total {} tasks", count.get());
	} else {
		fmt::println("Total task count is unknown");
	}
	return;
}

ACTOR Future<Void> printBulkLoadJobProgress(Database cx, BulkLoadJobState job) {
	state Transaction tr(cx);
	state Key readBegin = job.getJobRange().begin;
	state Key readEnd = job.getJobRange().end;
	state UID jobId = job.getJobId();
	state RangeResult rangeResult;
	state size_t completeTaskCount = 0;
	state size_t submitTaskCount = 0;
	state size_t errorTaskCount = 0;
	state Optional<uint64_t> totalTaskCount = job.getTaskCount();
	while (readBegin < readEnd) {
		try {
			rangeResult.clear();
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(store(rangeResult, krmGetRanges(&tr, bulkLoadTaskPrefix, KeyRangeRef(readBegin, readEnd))));
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
					if (bulkLoadTask.phase == BulkLoadPhase::Submitted &&
					    bulkLoadTask.getJobId() != UID::fromString("00000000-0000-0000-0000-000000000000")) {
						fmt::println("Job {} has been cancelled or has completed", jobId.toString());
					}
					return Void();
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
			wait(tr.onError(e));
		}
	}
	fmt::println("Submitted {} tasks", submitTaskCount);
	fmt::println("Finished {} tasks", completeTaskCount);
	fmt::println("Error {} tasks", errorTaskCount);
	printBulkLoadJobTotalTaskCount(totalTaskCount);
	return Void();
}

ACTOR Future<int> getBulkLoadMode(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			state int oldMode = 0;
			Optional<Value> oldModeValue = wait(tr.get(bulkLoadModeKey));
			if (oldModeValue.present()) {
				BinaryReader rd(oldModeValue.get(), Unversioned());
				rd >> oldMode;
			}
			return oldMode;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<UID> bulkLoadCommandActor(Database cx, std::vector<StringRef> tokens) {
	if (tokencmp(tokens[1], "mode")) {
		if (tokens.size() == 2) {
			int mode = wait(getBulkLoadMode(cx));
			if (mode == 0) {
				fmt::println("Bulkload mode is disabled");
			} else if (mode == 1) {
				fmt::println("Bulkload mode is enabled");
			} else {
				fmt::println("Invalid bulkload mode value {}", mode);
			}
			return UID();
		}
		// Set bulk loading mode
		if (tokens.size() != 3) {
			fmt::println("{}", BULK_LOAD_MODE_USAGE);
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
			fmt::println("ERROR: Invalid bulkload mode value {}", tokens[2].toString());
			fmt::println("{}", BULK_LOAD_MODE_USAGE);
			return UID();
		}
	} else if (tokencmp(tokens[1], "load")) {
		int mode = wait(getBulkLoadMode(cx));
		if (mode == 0) {
			fmt::println("ERROR: Bulkload mode must be enabled to load data");
			return UID();
		}
		if (tokens.size() != 6) {
			fmt::println("{}", BULK_LOAD_LOAD_USAGE);
			return UID();
		}
		UID jobId = UID::fromString(tokens[2].toString());
		if (!jobId.isValid()) {
			fmt::println("ERROR: Invalid job id {}", tokens[2].toString());
			fmt::println("{}", BULK_LOAD_LOAD_USAGE);
			return UID();
		}
		Key rangeBegin = tokens[3];
		Key rangeEnd = tokens[4];
		// Bulk load can only inject data to normal key space, aka "" ~ \xff
		if (rangeBegin >= rangeEnd || rangeEnd > normalKeys.end) {
			fmt::println(
			    "ERROR: Invalid range: {} to {}, normal key space only", rangeBegin.toString(), rangeEnd.toString());
			fmt::println("{}", BULK_LOAD_LOAD_USAGE);
			return UID();
		}
		std::string jobRoot = tokens[5].toString();
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		state BulkLoadJobState bulkLoadJob = createBulkLoadJob(
		    jobId,
		    range,
		    jobRoot,
		    jobRoot.find("blobstore://") == 0 ? BulkLoadTransportMethod::BLOBSTORE : BulkLoadTransportMethod::CP);
		wait(submitBulkLoadJob(cx, bulkLoadJob));
		return bulkLoadJob.getJobId();
	} else if (tokencmp(tokens[1], "cancel")) {
		if (tokens.size() != 3) {
			fmt::println("{}", BULK_LOAD_CANCEL_USAGE);
			return UID();
		}
		state UID jobId = UID::fromString(tokens[2].toString());
		if (!jobId.isValid()) {
			fmt::println("ERROR: Invalid job id {}", tokens[2].toString());
			fmt::println("{}", BULK_LOAD_CANCEL_USAGE);
			return UID();
		}
		wait(cancelBulkLoadJob(cx, jobId));
		fmt::println("Job {} has been cancelled. The job range lock has been cleared", jobId.toString());
		return UID();

	} else if (tokencmp(tokens[1], "status")) {
		if (tokens.size() != 2) {
			fmt::println("{}", BULK_LOAD_STATUS_USAGE);
			return UID();
		}
		Optional<BulkLoadJobState> job = wait(getRunningBulkLoadJob(cx));
		if (!job.present()) {
			fmt::println("No bulk loading job is running");
			return UID();
		}
		fmt::println("Running bulk loading job: {}", job.get().getJobId().toString());
		fmt::println("Job information: {}", job.get().toString());
		wait(printBulkLoadJobProgress(cx, job.get()));
		return UID();

	} else if (tokencmp(tokens[1], "history")) {
		if (tokens.size() == 2) {
			wait(printPastBulkLoadJob(cx));
			return UID();
		}
		if (tokens.size() == 3) {
			if (tokencmp(tokens[2], "clear")) {
				fmt::println("{}", BULK_LOAD_HISTORY_CLEAR_USAGE);
				return UID();
			} else {
				fmt::println("ERROR: Invalid history option {}", tokens[2].toString());
				fmt::println("{}", BULK_LOAD_HISTORY_CLEAR_USAGE);
				return UID();
			}
		}
		if (tokens.size() == 4) {
			if (tokencmp(tokens[2], "clear")) {
				if (tokencmp(tokens[3], "all")) {
					wait(clearBulkLoadJobHistory(cx));
					fmt::println("All bulkload job history has been cleared");
					return UID();
				} else {
					fmt::println("ERROR: Invalid history clear option {}", tokens[3].toString());
					fmt::println("{}", BULK_LOAD_HISTORY_CLEAR_USAGE);
					return UID();
				}
			} else {
				fmt::println("ERROR: Invalid history clear option {}", tokens[2].toString());
				fmt::println("{}", BULK_LOAD_HISTORY_CLEAR_USAGE);
				return UID();
			}
		}
		if (tokens.size() == 5) {
			if (tokencmp(tokens[2], "clear") && tokencmp(tokens[3], "id")) {
				UID jobId = UID::fromString(tokens[4].toString());
				if (!jobId.isValid()) {
					fmt::println("ERROR: Invalid job id {}", tokens[4].toString());
					fmt::println("{}", BULK_LOAD_HISTORY_CLEAR_USAGE);
					return UID();
				}
				wait(clearBulkLoadJobHistory(cx, jobId));
				fmt::println("Bulkload job {} has been cleared from history", jobId.toString());
				return jobId;
			}
		}
		printLongDesc(tokens[0]);
		return UID();

	} else if (tokencmp(tokens[1], "addlockowner")) {
		// For debugging purposes and invisible to users.
		if (tokens.size() != 3) {
			fmt::println("{}", BULK_LOAD_STATUS_USAGE);
			return UID();
		}
		std::string ownerUniqueID = tokens[2].toString();
		if (ownerUniqueID.empty()) {
			fmt::println("ERROR: Owner unique id cannot be empty");
			fmt::println("{}", BULKLOAD_ADD_LOCK_OWNER_USAGE);
			return UID();
		}
		wait(registerRangeLockOwner(cx, ownerUniqueID, ownerUniqueID));
		return UID();

	} else if (tokencmp(tokens[1], "printlock")) {
		// For debugging purposes and invisible to users.
		if (tokens.size() != 2) {
			fmt::println("{}", BULKLOAD_PRINT_LOCK_USAGE);
			return UID();
		}
		std::vector<std::pair<KeyRange, RangeLockState>> lockedRanges =
		    wait(findExclusiveReadLockOnRange(cx, normalKeys));
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
		return UID();

	} else if (tokencmp(tokens[1], "printlockowner")) {
		// For debugging purposes and invisible to users.
		if (tokens.size() != 2) {
			fmt::println("{}", BULKLOAD_PRINT_LOCK_OWNER_USAGE);
			return UID();
		}
		std::vector<RangeLockOwner> owners = wait(getAllRangeLockOwners(cx));
		for (const auto owner : owners) {
			fmt::println("{}", owner.toString());
		}
		return UID();

	} else if (tokencmp(tokens[1], "clearlock")) {
		// For debugging purposes and invisible to users.
		if (tokens.size() != 3) {
			fmt::println("{}", BULKLOAD_CLEAR_LOCK_USAGE);
			return UID();
		}
		std::string ownerUniqueID = tokens[2].toString();
		wait(releaseExclusiveReadLockByUser(cx, ownerUniqueID));
		return UID();

	} else {
		printUsage(tokens[0]);
		printLongDesc(tokens[0]);
		return UID();
	}
}

CommandFactory bulkLoadFactory("bulkload",
                               CommandHelp("bulkload [mode|load|status|cancel|history] [ARGs]",
                                           "bulkload commands",
                                           BULK_LOAD_HELP_MESSAGE.c_str()));
} // namespace fdb_cli
