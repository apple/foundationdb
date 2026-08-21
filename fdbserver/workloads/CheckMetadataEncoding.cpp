/*
 * CheckMetadataEncoding.cpp
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

// Workload that verifies keyServers and serverKeys encoding format matches the
// SHARD_ENCODE_LOCATION_METADATA knob. Used to validate forward migration and
// rollback of the shard-encoded metadata feature.

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"
#include "flow/Error.h"
#include "flow/flow.h"

struct CheckMetadataEncodingWorkload : TestWorkload {
	static constexpr auto NAME = "CheckMetadataEncoding";

	bool shardEncodeExpected;
	bool allowMixedFormats; // True in rollback scenarios where old entries remain
	bool requireKnobFalse; // If true, assert that SHARD_ENCODE is actually false
	// If true (in a knob=true phase), require the FORWARD COMPLETE condition:
	// zero old-format assigned entries in both keyServers and serverKeys.
	// Uses the same counting logic as fdbcli's `audit_storage
	// metadata_encoding` command, so this option gates on the same
	// terminal state that command reports.
	bool requireForwardComplete;
	// If true (in a knob=false phase), require the ROLLBACK COMPLETE
	// condition: zero new-format entries in both keyServers and
	// serverKeys, AND no dataMoves entries. Same condition the fdbcli
	// audit tool uses to report "safe to downgrade binary".
	bool requireRollbackComplete;

	explicit CheckMetadataEncodingWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		shardEncodeExpected = SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA;
		allowMixedFormats = getOption(options, "allowMixedFormats"_sr, false);
		requireKnobFalse = getOption(options, "requireKnobFalse"_sr, false);
		requireForwardComplete = getOption(options, "requireForwardComplete"_sr, false);
		requireRollbackComplete = getOption(options, "requireRollbackComplete"_sr, false);
	}

	Future<Void> setup(Database const& cx) override {
		if (requireKnobFalse && SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
			TraceEvent(SevError, "CheckMetadataEncodingKnobNotFalse")
			    .detail("Expected", false)
			    .detail("Actual", SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA)
			    .detail("Hint",
			            "TOML [[knobs]] override for shard_encode_location_metadata "
			            "did not take effect. The knob infrastructure fix may not be working.");
		}
		return Void();
	}
	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(this, cx);
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> _start(CheckMetadataEncodingWorkload* self, Database cx) {
		int64_t keyServersOld = 0, keyServersNew = 0;
		int64_t serverKeysOld = 0, serverKeysNew = 0;
		int64_t dataMovesCount = 0;

		// Scan keyServers.
		// Empty-value entries are KRM boundary sentinels — format-neutral,
		// skipped for the same reason the fdbcli audit tool skips them
		// (see fdbcli/CheckMetadataEncodingCommand.cpp).
		{
			Key begin = keyServersPrefix;
			Key end = keyServersEnd;
			while (begin < end) {
				Transaction tr(cx);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
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
				} catch (Error& e) {
					err = e;
				}
				if (err.isValid()) {
					co_await tr.onError(err);
				}
			}
		}

		// Scan serverKeys.
		// Uses the same classifiers as the fdbcli audit tool: format-
		// neutral entries (empty sentinels + serverKeysFalse) are
		// skipped, old-format assignments (serverKeysTrue /
		// serverKeysTrueEmptyRange) and new-format assignments
		// (UID-encoded) are counted separately.
		{
			Key begin = serverKeysPrefix;
			Key end = strinc(serverKeysPrefix);
			while (begin < end) {
				Transaction tr(cx);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
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
				} catch (Error& e) {
					err = e;
				}
				if (err.isValid()) {
					co_await tr.onError(err);
				}
			}
		}

		// Count in-flight data moves. Needed for the ROLLBACK COMPLETE
		// terminal condition (same as the fdbcli audit tool). Only scan
		// when the assertion actually needs the count — the scan is
		// otherwise wasted I/O and the trace event below distinguishes
		// "not scanned" from "0 entries" by omitting the DataMoves detail.
		bool dataMovesScanned = false;
		if (self->requireRollbackComplete) {
			while (true) {
				Transaction tr(cx);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				Error err;
				try {
					RangeResult result = co_await tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY);
					ASSERT(!result.more && result.size() < CLIENT_KNOBS->TOO_MANY);
					dataMovesCount = result.size();
					dataMovesScanned = true;
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
		}

		auto resultEvent = TraceEvent("CheckMetadataEncodingResult");
		resultEvent.detail("ShardEncodeExpected", self->shardEncodeExpected)
		    .detail("KeyServersOld", keyServersOld)
		    .detail("KeyServersNew", keyServersNew)
		    .detail("ServerKeysOld", serverKeysOld)
		    .detail("ServerKeysNew", serverKeysNew)
		    .detail("RequireForwardComplete", self->requireForwardComplete)
		    .detail("RequireRollbackComplete", self->requireRollbackComplete);
		if (dataMovesScanned) {
			resultEvent.detail("DataMoves", dataMovesCount);
		}

		if (self->shardEncodeExpected) {
			// With SHARD_ENCODE=true and moves having occurred, we expect new-format entries.
			// There may still be old-format entries if not all shards have been moved yet.
			if (keyServersNew == 0) {
				TraceEvent(SevError, "CheckMetadataEncodingFailed")
				    .detail("Reason", "Expected new-format keyServers entries but found none")
				    .detail("KeyServersOld", keyServersOld)
				    .detail("KeyServersNew", keyServersNew);
			}
			if (serverKeysNew == 0) {
				TraceEvent(SevError, "CheckMetadataEncodingFailed")
				    .detail("Reason", "Expected new-format serverKeys entries but found none")
				    .detail("ServerKeysOld", serverKeysOld)
				    .detail("ServerKeysNew", serverKeysNew);
			}
		} else {
			// With SHARD_ENCODE=false on a fresh cluster, all entries should be old format.
			// In a rollback scenario (allowMixedFormats), leftover new-format entries from
			// the prior phase are expected and not an error.
			if (keyServersNew > 0 && !self->allowMixedFormats) {
				TraceEvent(SevError, "CheckMetadataEncodingFailed")
				    .detail("Reason", "Expected all old-format keyServers but found new-format entries")
				    .detail("KeyServersOld", keyServersOld)
				    .detail("KeyServersNew", keyServersNew);
			}
			if (serverKeysNew > 0 && !self->allowMixedFormats) {
				TraceEvent(SevError, "CheckMetadataEncodingFailed")
				    .detail("Reason", "Expected all old-format serverKeys but found new-format entries")
				    .detail("ServerKeysOld", serverKeysOld)
				    .detail("ServerKeysNew", serverKeysNew);
			}
		}

		// Terminal-state assertions are independent of shardEncodeExpected:
		// if the caller requested one, the corresponding assertion must
		// always run — otherwise a knob-override failure would silently
		// bypass the check. Mismatched-mode combinations (e.g.
		// requireForwardComplete=true while knob is false) are treated as
		// misconfiguration and fail loudly.
		if (self->requireForwardComplete) {
			if (!self->shardEncodeExpected) {
				TraceEvent(SevError, "CheckMetadataEncodingFailed")
				    .detail("Reason", "requireForwardComplete set but SHARD_ENCODE_LOCATION_METADATA is false")
				    .detail("ShardEncodeExpected", self->shardEncodeExpected);
			} else if (keyServersOld != 0 || serverKeysOld != 0) {
				// FORWARD COMPLETE: no old-format assigned entries remain.
				// Same condition fdbcli's audit tool reports as
				// "FORWARD COMPLETE".
				TraceEvent(SevError, "CheckMetadataEncodingFailed")
				    .detail("Reason", "FORWARD COMPLETE required but old-format entries remain")
				    .detail("KeyServersOld", keyServersOld)
				    .detail("KeyServersNew", keyServersNew)
				    .detail("ServerKeysOld", serverKeysOld)
				    .detail("ServerKeysNew", serverKeysNew);
			}
		}
		if (self->requireRollbackComplete) {
			if (self->shardEncodeExpected) {
				TraceEvent(SevError, "CheckMetadataEncodingFailed")
				    .detail("Reason", "requireRollbackComplete set but SHARD_ENCODE_LOCATION_METADATA is true")
				    .detail("ShardEncodeExpected", self->shardEncodeExpected);
			} else if (keyServersNew != 0 || serverKeysNew != 0 || dataMovesCount != 0) {
				// ROLLBACK COMPLETE: no new-format entries and no data
				// moves. Same condition fdbcli's audit tool reports as
				// "ROLLBACK COMPLETE — safe to downgrade binary".
				TraceEvent(SevError, "CheckMetadataEncodingFailed")
				    .detail("Reason", "ROLLBACK COMPLETE required but state not yet drained")
				    .detail("KeyServersOld", keyServersOld)
				    .detail("KeyServersNew", keyServersNew)
				    .detail("ServerKeysOld", serverKeysOld)
				    .detail("ServerKeysNew", serverKeysNew)
				    .detail("DataMoves", dataMovesCount);
			}
		}

		co_return;
	}
};

WorkloadFactory<CheckMetadataEncodingWorkload> CheckMetadataEncodingWorkloadFactory;
