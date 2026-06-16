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

	explicit CheckMetadataEncodingWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		shardEncodeExpected = SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA;
		allowMixedFormats = getOption(options, "allowMixedFormats"_sr, false);
		requireKnobFalse = getOption(options, "requireKnobFalse"_sr, false);
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

		// Scan keyServers
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
							keyServersOld++;
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

		// Scan serverKeys
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
						if (kv.value == serverKeysTrue || kv.value == serverKeysFalse ||
						    kv.value == serverKeysTrueEmptyRange) {
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

		TraceEvent("CheckMetadataEncodingResult")
		    .detail("ShardEncodeExpected", self->shardEncodeExpected)
		    .detail("KeyServersOld", keyServersOld)
		    .detail("KeyServersNew", keyServersNew)
		    .detail("ServerKeysOld", serverKeysOld)
		    .detail("ServerKeysNew", serverKeysNew);

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

		co_return;
	}
};

WorkloadFactory<CheckMetadataEncodingWorkload> CheckMetadataEncodingWorkloadFactory;
