/*
 * GetBulkLoadStatusCommand.actor.cpp
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
#include "fdbclient/BulkLoading.h"
#include "fdbclient/IClientApi.h"
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
			KeyRange range = Standalone(KeyRangeRef(res[i].key, res[i + 1].key));
			ASSERT(range == bulkLoadState.getRange());
		}
		readBegin = res.back().key;
	}

	printf("Finished task count %ld of total %ld tasks\n", finishCount, finishCount + unfinishedCount);
	return Void();
}

ACTOR Future<bool> getBulkLoadStatusCommandActor(Database cx, std::vector<StringRef> tokens) {
	if (tokens.size() < 3) {
		printUsage(tokens[0]);
		return false;
	}
	Key rangeBegin = tokens[1];
	Key rangeEnd = tokens[2];
	if (rangeBegin > normalKeys.end || rangeEnd > normalKeys.end) {
		printUsage(tokens[0]);
		return true;
	}
	KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
	wait(getBulkLoadStateByRange(cx, range));

	return true;
}

CommandFactory getBulkLoadStatusFactory("get_bulkload_status",
                                        CommandHelp("get_bulkload_status <Begin, End>",
                                                    "Specify range to check bulk load task progress",
                                                    "Specify range to check bulk load task progress"));

} // namespace fdb_cli
