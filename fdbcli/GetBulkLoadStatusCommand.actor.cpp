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

ACTOR Future<std::vector<BulkLoadState>> getBulkLoadStateByRange(Database cx, KeyRange rangeToRead) {
	state Transaction tr(cx);
	state std::vector<BulkLoadState> bulkLoadStates;
	state Key readBegin;
	state Key readEnd;
	loop {
		try {
			readBegin = rangeToRead.begin;
			readEnd = rangeToRead.end;
			bulkLoadStates.clear();
			loop {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				state RangeResult res = wait(krmGetRanges(&tr,
				                                          bulkLoadPrefix,
				                                          KeyRangeRef(readBegin, readEnd),
				                                          CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
				                                          CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
				for (int i = 0; i < res.size() - 1; ++i) {
					const BulkLoadState bulkLoadState = decodeBulkLoadState(res[i].value);
					KeyRange range = Standalone(KeyRangeRef(res[i].key, res[i + 1].key));
					ASSERT(range == bulkLoadState.range);
					bulkLoadStates.push_back(bulkLoadState);
				}
				if (!res.more) {
					break;
				}
				tr.reset();
			}
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return bulkLoadStates;
}

ACTOR Future<Void> getBulkLoadByRange(Database cx, KeyRange range) {
	state KeyRange rangeToRead = range;
	state Key rangeToReadBegin = rangeToRead.begin;
	state int retryCount = 0;
	state int64_t finishCount = 0;
	state int64_t unfinishedCount = 0;
	while (rangeToReadBegin < range.end) {
		loop {
			try {
				rangeToRead = KeyRangeRef(rangeToReadBegin, range.end);
				state std::vector<BulkLoadState> bulkLoadStates = wait(getBulkLoadStateByRange(cx, rangeToRead));
				for (int i = 0; i < bulkLoadStates.size(); i++) {
					BulkLoadPhase phase = bulkLoadStates[i].phase;
					if (phase == BulkLoadPhase::Complete) {
						printf("[Complete]: %s\n", bulkLoadStates[i].toString().c_str());
						++finishCount;
					} else if (phase == BulkLoadPhase::Running) {
						printf("[Running]: %s\n", bulkLoadStates[i].toString().c_str());
						++unfinishedCount;
					} else if (phase == BulkLoadPhase::Triggered) {
						printf("[Triggered]: %s\n", bulkLoadStates[i].toString().c_str());
						++unfinishedCount;
					} else if (phase == BulkLoadPhase::Invalid) {
						printf("[NotStarted] %s\n", bulkLoadStates[i].toString().c_str());
						++unfinishedCount;
					} else {
						UNREACHABLE();
					}
				}
				rangeToReadBegin = bulkLoadStates.back().range.end;
				break;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw e;
				}
				if (retryCount > 30) {
					printf("Incomplete check\n");
					return Void();
				}
				wait(delay(0.5));
				retryCount++;
			}
		}
	}
	printf("Finished range count: %ld\n", finishCount);
	printf("Unfinished range count: %ld\n", unfinishedCount);
	return Void();
}

ACTOR Future<bool> getBulkLoadStatusCommandActor(Database cx, std::vector<StringRef> tokens) {
	Key rangeBegin = tokens[1];
	Key rangeEnd = tokens[2];
	if (rangeBegin > normalKeys.end || rangeEnd > normalKeys.end) {
		printUsage(tokens[0]);
		return true;
	}
	KeyRange range = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
	wait(getBulkLoadByRange(cx, range));

	return true;
}

CommandFactory getBulkLoadStatusFactory("get_bulkload_status",
                                        CommandHelp("get_bulkload_status <Begin, End>",
                                                    "Specify range to check bulk load task progress",
                                                    "Specify range to check bulk load task progress"));

} // namespace fdb_cli
