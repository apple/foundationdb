/*
 * CheckMetadataEncodingCommand.cpp
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
#include "fdbclient/IClientApi.h"
#include "fdbclient/SystemData.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"

namespace fdb_cli {

Future<bool> checkMetadataEncodingCommandActor(Database cx, std::vector<StringRef> tokens) {
	int64_t keyServersOld = 0, keyServersNew = 0;
	int64_t serverKeysOld = 0, serverKeysNew = 0;
	int64_t dataMovesCount = 0;

	// Scan keyServers.
	// Empty-value entries are KRM boundary sentinels — they mark the
	// edges between adjacent same-valued ranges and do not carry any
	// keyServers assignment. Skip them; counting them as either format
	// leaves a permanent residual that makes the FORWARD COMPLETE
	// terminal state unreachable on any real cluster.
	{
		Key begin = keyServersPrefix;
		Key end = keyServersEnd;
		while (begin < end) {
			Transaction tr(cx);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Error err;
			try {
				RangeResult result = co_await tr.getRange(KeyRangeRef(begin, end), 1000);
				for (const auto& kv : result) {
					if (kv.value.empty()) {
						continue;
					}
					BinaryReader rd(kv.value, IncludeVersion());
					if (rd.protocolVersion().hasShardEncodeLocationMetaData()) {
						keyServersNew++;
					} else {
						keyServersOld++;
					}
				}
				if (!result.more) {
					break;
				}
				begin = keyAfter(result.back().key);
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	// Scan serverKeys.
	// Uses classifiers from SystemData.h to distinguish format-neutral
	// entries (empty sentinels + serverKeysFalse "not assigned") from
	// old-format assignments (serverKeysTrue / serverKeysTrueEmptyRange)
	// from new-format assignments (any other non-empty value: a
	// UID-encoded dataMoveId). Previously all three category buckets
	// were merged into "old", making the FORWARD COMPLETE terminal
	// state unreachable — any running cluster always has serverKeysFalse
	// entries as range-boundary markers around assigned ranges.
	{
		Key begin = serverKeysPrefix;
		Key end = strinc(serverKeysPrefix);
		while (begin < end) {
			Transaction tr(cx);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Error err;
			try {
				RangeResult result = co_await tr.getRange(KeyRangeRef(begin, end), 1000);
				for (const auto& kv : result) {
					if (isServerKeysUnassigned(kv.value)) {
						continue;
					}
					if (isServerKeysOldFormatAssigned(kv.value)) {
						serverKeysOld++;
					} else {
						serverKeysNew++;
					}
				}
				if (!result.more) {
					break;
				}
				begin = keyAfter(result.back().key);
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	// Count dataMoves
	loop {
		Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		Error err;
		try {
			RangeResult result = co_await tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY);
			dataMovesCount = result.size();
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}

	// Report
	int64_t keyServersTotal = keyServersOld + keyServersNew;
	int64_t serverKeysTotal = serverKeysOld + serverKeysNew;

	fmt::println("keyServers: {} entries", keyServersTotal);
	fmt::println("  Old format (tag-based): {}", keyServersOld);
	fmt::println("  New format (UID-based): {}", keyServersNew);
	fmt::println("serverKeys: {} entries", serverKeysTotal);
	fmt::println("  Old format (constants): {}", serverKeysOld);
	fmt::println("  New format (UID-encoded): {}", serverKeysNew);
	fmt::println("dataMoves: {} entries", dataMovesCount);
	fmt::println("");

	if (keyServersNew == 0 && serverKeysNew == 0 && dataMovesCount == 0) {
		fmt::println("Migration status: ROLLBACK COMPLETE — safe to downgrade binary");
	} else if (keyServersOld == 0 && serverKeysOld == 0) {
		fmt::println("Migration status: FORWARD COMPLETE");
	} else if (keyServersNew > 0 || serverKeysNew > 0) {
		fmt::println("Migration status: MIGRATION IN PROGRESS (mixed format: {} new keyServers, {} new serverKeys)",
		             keyServersNew,
		             serverKeysNew);
	} else {
		fmt::println("Migration status: NOT STARTED (all old format)");
	}

	co_return true;
}

} // namespace fdb_cli
