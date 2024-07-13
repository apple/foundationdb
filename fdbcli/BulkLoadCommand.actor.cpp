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

#include "fdbclient/IClientApi.h"

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/BulkLoading.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<Void> getBulkLoadStateByRange(Database cx, KeyRange rangeToRead) {
	state Transaction tr(cx);
	state Key readBegin = rangeToRead.begin;
	state Key readEnd = rangeToRead.end;
	state int64_t finishCount = 0;
	state int64_t unfinishedCount = 0;
	state RangeResult res;
	while (readBegin < readEnd) {
		state int retryCount = 0;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				RangeResult res_ = wait(krmGetRanges(&tr,
				                                     bulkLoadPrefix,
				                                     KeyRangeRef(readBegin, readEnd),
				                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
				                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
				res = res_;
				break;
			} catch (Error& e) {
				if (retryCount > 30) {
					printf("Incomplete check\n");
					return Void();
				}
				wait(tr.onError(e));
				retryCount++;
			}
		}
		for (int i = 0; i < res.size() - 1; ++i) {
			if (res[i].value.empty()) {
				continue;
			}
			BulkLoadState bulkLoadState = decodeBulkLoadState(res[i].value);
			KeyRange range = Standalone(KeyRangeRef(res[i].key, res[i + 1].key));
			if (range != bulkLoadState.getRange()) {
				ASSERT(bulkLoadState.getRange().contains(range));
				continue;
			}
			if (bulkLoadState.phase == BulkLoadPhase::Complete) {
				printf("[Complete]: %s\n", bulkLoadState.toString().c_str());
				++finishCount;
			} else if (bulkLoadState.phase == BulkLoadPhase::Running) {
				printf("[Running]: %s\n", bulkLoadState.toString().c_str());
				++unfinishedCount;
			} else if (bulkLoadState.phase == BulkLoadPhase::Triggered) {
				printf("[Triggered]: %s\n", bulkLoadState.toString().c_str());
				++unfinishedCount;
			} else if (bulkLoadState.phase == BulkLoadPhase::Invalid) {
				printf("[NotStarted] %s\n", bulkLoadState.toString().c_str());
				++unfinishedCount;
			} else {
				UNREACHABLE();
			}
		}
		readBegin = res.back().key;
	}

	printf("Finished task count %ld of total %ld tasks\n", finishCount, finishCount + unfinishedCount);
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
		if (rangeBegin > normalKeys.end || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		wait(submitBulkLoadTask(
		    clusterFile, BulkLoadState(taskId, range), TriggerBulkLoadRequestType::Acknowledge, /*timeoutSeconds=*/60));
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
		if (rangeBegin > normalKeys.end || rangeEnd > normalKeys.end) {
			printUsage(tokens[0]);
			return UID();
		}
		std::string folder = tokens[4].toString();
		std::string dataFile = tokens[5].toString();
		std::string byteSampleFile = tokens[6].toString();
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		state BulkLoadState bulkLoadTask = newBulkLoadTaskLocalSST(range, folder, dataFile, byteSampleFile);
		wait(submitBulkLoadTask(clusterFile, bulkLoadTask, TriggerBulkLoadRequestType::New, /*timeoutSeconds=*/60));
		return bulkLoadTask.getTaskId();

	} else if (tokencmp(tokens[1], "status")) {
		// Get progress of existing bulk loading tasks intersecting the input range
		// TODO(Zhe): check status by ID
		if (tokens.size() < 4) {
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
		KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		wait(getBulkLoadStateByRange(cx, range));
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
                "To acknowledge completed tasks within a range: `bulkload acknowledge <BeginKey> <EndKey> <Folder> "
                "<DataFile> <ByteSampleFile>'\n"
                "To trigger a task injecting a SST file from local file system: `bulkload local <BeginKey> <EndKey> "
                "<Folder> <DataFile> <ByteSampleFile>'\n"
                "To get progress of tasks within a range: `bulkload status <BeginKey> <EndKey>'\n"));
} // namespace fdb_cli
