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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/BulkLoading.h"
#include "flow/Arena.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<Void> printPastBulkLoadJob(Database cx) {
	std::vector<BulkLoadJobState> jobs = wait(getBulkLoadJobFromHistory(cx));
	if (jobs.empty()) {
		fmt::println("No bulk loading job in the history");
		return Void();
	}
	for (const auto& job : jobs) {
		ASSERT(job.getPhase() == BulkLoadJobPhase::Complete || job.getPhase() == BulkLoadJobPhase::Error ||
		       job.getPhase() == BulkLoadJobPhase::Cancelled);
		fmt::println("Job {} submitted at {} for range {}. The job ends by {} mins for {} status",
		             job.getJobId().toString(),
		             std::to_string(job.getSubmitTime()),
		             job.getJobRange().toString(),
		             std::to_string((job.getEndTime() - job.getSubmitTime()) / 60.0),
		             convertBulkLoadJobPhaseToString(job.getPhase()));
	}
	return Void();
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
					fmt::println("Job {} has been cancelled or completed", jobId.toString());
					return Void();
				}
				if (bulkLoadTask.phase == BulkLoadPhase::Complete) {
					completeTaskCount++;
				} else if (bulkLoadTask.phase == BulkLoadPhase::Error) {
					errorTaskCount++;
				}
				submitTaskCount++;
			}
			readBegin = rangeResult.back().key;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	fmt::println("Submitted {} tasks", submitTaskCount);
	fmt::println("Finished {} tasks", completeTaskCount);
	fmt::println("Error {} tasks", errorTaskCount);
	return Void();
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
	} else if (tokencmp(tokens[1], "local")) {
		if (tokens.size() != 6) {
			printUsage(tokens[0]);
			return UID();
		}
		UID jobId = UID::fromString(tokens[2].toString());
		if (!jobId.isValid()) {
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
		if (!jobId.isValid()) {
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
		std::string jobRoot = tokens[5].toString();
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		BulkLoadJobState bulkLoadJob = createBulkLoadJob(jobId, range, jobRoot, BulkLoadTransportMethod::BLOBSTORE);
		wait(submitBulkLoadJob(cx, bulkLoadJob));
		return bulkLoadJob.getJobId();

	} else if (tokencmp(tokens[1], "cancel")) {
		if (tokens.size() != 3) {
			printUsage(tokens[0]);
			return UID();
		}
		state UID jobId = UID::fromString(tokens[2].toString());
		if (!jobId.isValid()) {
			printUsage(tokens[0]);
			return UID();
		}
		wait(cancelBulkLoadJob(cx, jobId));
		fmt::println("Job {} has been cancelled.", jobId.toString());
		return UID();

	} else if (tokencmp(tokens[1], "status")) {
		if (tokens.size() != 4) {
			printUsage(tokens[0]);
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
		if (tokens.size() != 2) {
			printUsage(tokens[0]);
			return UID();
		}
		wait(printPastBulkLoadJob(cx));
		return UID();

	} else if (tokencmp(tokens[1], "clearhistory")) {
		if (tokens.size() > 4) {
			printUsage(tokens[0]);
			return UID();
		}
		if (tokens.size() == 3) {
			if (!tokencmp(tokens[2], "all")) {
				printUsage(tokens[0]);
				return UID();
			}
			wait(clearBulkLoadJobHistory(cx));
			fmt::println("All bulkload job history has been cleared");
			return UID();
		}
		ASSERT(tokens.size() == 4);
		if (!tokencmp(tokens[2], "id")) {
			printUsage(tokens[0]);
			return UID();
		}
		UID jobId = UID::fromString(tokens[3].toString());
		if (!jobId.isValid()) {
			printUsage(tokens[0]);
			return UID();
		}
		wait(clearBulkLoadJobHistory(cx, jobId));
		fmt::println("Bulkload job {} has been cleared from history", jobId.toString());
		return jobId;

	} else {
		printUsage(tokens[0]);
		return UID();
	}
}

CommandFactory bulkLoadFactory(
    "bulkload",
    CommandHelp("bulkload [mode|local|blobstore|status|cancel|history|historyclear] [ARGs]",
                "bulkload commands",
                "To set bulkload mode: `bulkload mode [on|off]'\n"
                "To load a range from SST files: `bulkload [local|blobstore] <BeginKey> <EndKey> dumpFolder`\n"
                "To cancel current bulkload job: `bulkload cancel <Non-Zero JobID>`\n"
                "To get completed bulkload ranges: `bulkload status <BeginKey> <EndKey>`\n"
                "To print bulkload job history: `bulkload history`\n"
                "To clear bulkload job history: `bulkload clearhistory [all|id]`\n"));
} // namespace fdb_cli
