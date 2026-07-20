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
// cluster's effective shard-location-metadata target. The effective target is
// DatabaseConfiguration's shard_metadata_format when set, otherwise the
// SHARD_ENCODE_LOCATION_METADATA knob (same resolution DD uses). Used to
// validate forward migration and rollback of the shard-encoded metadata
// feature, driven either by the knob or by `configure shard_metadata_format`.

#include "fdbclient/DatabaseConfiguration.h"
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
	// If true, require the cluster's effective target to be original (i.e.
	// rolled back). Works whether the rollback was driven by the knob
	// (knob=false) or by configure (shard_metadata_format=original with the
	// knob left as-is). Named for historical reasons; it now checks the
	// resolved effective target, not the raw knob.
	bool requireKnobFalse;
	// If true (effective target encoded), require the FORWARD COMPLETE condition:
	// zero old-format assigned entries in both keyServers and serverKeys.
	// Uses the same counting logic as fdbcli's `audit_storage
	// metadata_encoding` command, so this option gates on the same
	// terminal state that command reports.
	bool requireForwardComplete;
	// If true (effective target original), require the ROLLBACK COMPLETE
	// condition: zero new-format entries in both keyServers and
	// serverKeys, AND no dataMoves entries. Same condition the fdbcli
	// audit tool uses to report "safe to downgrade binary".
	bool requireRollbackComplete;

	explicit CheckMetadataEncodingWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		// Default before the effective target is resolved from config in _start;
		// overwritten there. The knob is only the fallback when
		// shard_metadata_format is UNSET.
		shardEncodeExpected = SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA;
		allowMixedFormats = getOption(options, "allowMixedFormats"_sr, false);
		requireKnobFalse = getOption(options, "requireKnobFalse"_sr, false);
		requireForwardComplete = getOption(options, "requireForwardComplete"_sr, false);
		requireRollbackComplete = getOption(options, "requireRollbackComplete"_sr, false);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(this, cx);
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Resolve the cluster's effective shard-location-metadata target the same
	// way DD does: DatabaseConfiguration.shard_metadata_format when set,
	// otherwise the SHARD_ENCODE_LOCATION_METADATA knob.
	static Future<bool> resolveEffectiveTarget(Database cx) {
		loop {
			Transaction tr(cx);
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				Optional<Value> v = co_await tr.get(
				    StringRef(DatabaseConfiguration::SHARD_METADATA_FORMAT_KEY).withPrefix(configKeysPrefix));
				if (v.present()) {
					co_return v.get() == StringRef(DatabaseConfiguration::SHARD_METADATA_FORMAT_ENCODED);
				}
				co_return SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> _start(CheckMetadataEncodingWorkload* self, Database cx) {
		// Resolve the effective target (config-or-knob) and drive all
		// expectations off it, so the workload validates both knob-driven and
		// configure-driven rollback/forward.
		self->shardEncodeExpected = co_await resolveEffectiveTarget(cx);
		if (self->requireKnobFalse && self->shardEncodeExpected) {
			TraceEvent(SevError, "CheckMetadataEncodingEffectiveNotOldFormat")
			    .detail("Expected", "original")
			    .detail("EffectiveIsNewFormat", self->shardEncodeExpected)
			    .detail("Knob", SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA)
			    .detail("Hint",
			            "requireKnobFalse set but the effective shard_metadata target is still "
			            "encoded. The knob override or `configure shard_metadata_format=original` "
			            "did not take effect.");
		}

		// When a terminal-state condition is required, the DD-side rewrite
		// (forward migration or rollback) runs asynchronously and may not be
		// finished the instant this workload starts scanning. Poll until the
		// required terminal condition holds or a deadline elapses, then
		// assert on the final observation. This removes the false failure
		// where the audit scanned a few seconds before the DD rewrite sealed
		// (observed at T=249 while the rewrite completed at T=253 — see
		// journal 2026-07-18). Only poll when the requested terminal mode
		// matches the knob direction; a mismatched request is a test
		// misconfiguration and should fail loudly and immediately below.
		const bool pollForConvergence = (self->requireRollbackComplete && !self->shardEncodeExpected) ||
		                                 (self->requireForwardComplete && self->shardEncodeExpected);
		// Deadline is generous: under BUGGIFY the DD rollback rewrite can be
		// throttled hard (tiny SHARD_ENCODE_REWRITE_KS_BATCH_SIZE and
		// KRM_GET_RANGE_LIMIT), making Phase 2/3 legitimately take a few
		// minutes of sim time to drain a large shard set. Too short a
		// deadline produces a false timeout while DD is still correctly
		// converging (observed at ~182s with both knobs pinned small).
		const double pollDeadline = now() + 400.0;
		const double pollInterval = 2.0;

		int64_t keyServersOld = 0, keyServersNew = 0;
		int64_t serverKeysOld = 0, serverKeysNew = 0;
		int64_t dataMovesCount = 0;
		bool dataMovesScanned = false;
		int scanAttempts = 0;

		loop {
			scanAttempts++;
			keyServersOld = keyServersNew = 0;
			serverKeysOld = serverKeysNew = 0;
			dataMovesCount = 0;
			dataMovesScanned = false;

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
			if (self->requireRollbackComplete) {
				loop {
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

			if (!pollForConvergence) {
				break; // single-shot: preserve legacy one-scan behavior
			}
			// Terminal condition matching the requested mode. Both may be
			// requested in principle; require whichever are set.
			bool terminalReached = true;
			if (self->requireRollbackComplete) {
				terminalReached =
				    terminalReached && (keyServersNew == 0 && serverKeysNew == 0 && dataMovesCount == 0);
			}
			if (self->requireForwardComplete) {
				terminalReached = terminalReached && (keyServersOld == 0 && serverKeysOld == 0);
			}
			if (terminalReached) {
				TraceEvent(SevInfo, "CheckMetadataEncodingConverged")
				    .detail("ScanAttempts", scanAttempts)
				    .detail("KeyServersOld", keyServersOld)
				    .detail("KeyServersNew", keyServersNew)
				    .detail("ServerKeysOld", serverKeysOld)
				    .detail("ServerKeysNew", serverKeysNew)
				    .detail("DataMoves", dataMovesCount);
				break;
			}
			if (now() >= pollDeadline) {
				// Fall through to the assertions below, which fail loudly
				// with the final counts.
				TraceEvent(SevWarnAlways, "CheckMetadataEncodingConvergeTimeout")
				    .detail("ScanAttempts", scanAttempts)
				    .detail("KeyServersOld", keyServersOld)
				    .detail("KeyServersNew", keyServersNew)
				    .detail("ServerKeysOld", serverKeysOld)
				    .detail("ServerKeysNew", serverKeysNew)
				    .detail("DataMoves", dataMovesCount);
				break;
			}
			co_await delay(pollInterval);
		}

		auto resultEvent = TraceEvent("CheckMetadataEncodingResult");
		resultEvent.detail("ShardEncodeExpected", self->shardEncodeExpected)
		    .detail("KeyServersOld", keyServersOld)
		    .detail("KeyServersNew", keyServersNew)
		    .detail("ServerKeysOld", serverKeysOld)
		    .detail("ServerKeysNew", serverKeysNew)
		    .detail("ScanAttempts", scanAttempts)
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
				    .detail("Reason", "requireForwardComplete set but effective shard_metadata target is original")
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
				    .detail("Reason", "requireRollbackComplete set but effective shard_metadata target is encoded")
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
