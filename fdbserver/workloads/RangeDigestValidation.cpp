/*
 * RangeDigestValidation.cpp
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

// RangeDigestValidationWorkload exercises the RangeDigest audit end to end in simulation:
//   1. Client 0 writes a set of known key-values into the user keyspace.
//   2. It triggers a RangeDigest audit over normalKeys and waits for completion.
//   3. It reads the cluster root DD combined from the per-range digests and cross-checks it
//      against an INDEPENDENT client-side computation of the same additive digest (scanning
//      every key-value and applying the canonical leaf encoding). This validates the SS fold
//      and the DD combine.
//   4. It triggers a SECOND RangeDigest audit and asserts the root is identical. DD may have
//      moved shards in between, but nothing here forces it to, so this is a determinism check
//      that additionally covers partition-independence on the seeds where movement occurred.

#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/RangeDigest.h"
#include "fdbserver/tester/workloads.h"

struct RangeDigestValidationWorkload : TestWorkload {
	static constexpr auto NAME = "RangeDigestValidation";

	// Independent expected-digest computation result.
	struct ExpectedDigest {
		RangeDigest digest;
		int64_t kvCount = 0;
		int64_t byteCount = 0;
	};

	int nodeCount;
	int valueBytes;
	double validateAfter;
	double checkInterval;
	double maxWaitTime;
	int expectMinShards;
	double shardSettleTimeout;
	bool strictShardCheck;
	// A run is only meaningful if the content comparison actually executed. Absent this, every error
	// path is a silent skip and a permanently broken digest still yields a green test.
	bool validated = false;

	explicit RangeDigestValidationWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		// Defaults are modest so the dataset fits the simulation `memory` storage engine and the test
		// stays cheap in the Joshua matrix. For a dedicated multi-shard/throughput run, override
		// nodeCount/valueBytes/expectMinShards/strictShardCheck via the TOML.
		nodeCount = getOption(options, "nodeCount"_sr, 10000);
		valueBytes = getOption(options, "valueBytes"_sr, 100);
		validateAfter = getOption(options, "validateAfter"_sr, 5.0);
		checkInterval = getOption(options, "checkInterval"_sr, 2.0);
		maxWaitTime = getOption(options, "maxWaitTime"_sr, 120.0);
		// Splitting into several shards exercises the cross-shard/cross-server combine. In the
		// randomized Joshua matrix a seed may keep everything in one shard (shard-size knob is forced
		// to 400KB/2MB/10MB, not the min_shard_bytes knob), so by default this is best-effort (a warning,
		// not a failure). strictShardCheck=true makes it a hard requirement for dedicated runs.
		expectMinShards = getOption(options, "expectMinShards"_sr, 2);
		shardSettleTimeout = getOption(options, "shardSettleTimeout"_sr, 60.0);
		strictShardCheck = getOption(options, "strictShardCheck"_sr, false);
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return loadData(cx);
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(cx);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return clientId != 0 || validated; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	static Key testKey(int i) { return StringRef(format("rdv/%08d", i)); }

	// Deterministic, per-key-unique value of length valueBytes.
	Value testValue(int i) {
		std::string base = format("value-%08d-", i);
		std::string v = base;
		v.reserve(valueBytes);
		// Pad with a repeating pattern seeded by the index so content is unique per key.
		char fill = (char)('A' + (i % 26));
		while ((int)v.size() < valueBytes) {
			v.push_back(fill);
		}
		v.resize(std::max<int>(valueBytes, (int)base.size()));
		return Value(StringRef(v));
	}

	Future<Void> loadData(Database cx) {
		int i = 0;
		int batchSize = 100;
		while (i < nodeCount) {
			Transaction tr(cx);
			while (true) {
				Error err;
				try {
					int end = std::min(i + batchSize, nodeCount);
					for (int j = i; j < end; ++j) {
						tr.set(testKey(j), testValue(j));
					}
					co_await tr.commit();
					i = end;
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
		}
		TraceEvent("RangeDigestValidationDataLoaded")
		    .detail("NodeCount", nodeCount)
		    .detail("ValueBytes", valueBytes)
		    .detail("ApproxTotalBytes", (int64_t)nodeCount * (valueBytes + 12));
	}

	// Independently compute the expected additive digest by scanning every key-value in normalKeys.
	Future<ExpectedDigest> computeExpectedDigest(Database cx) {
		ExpectedDigest out;
		Key begin = normalKeys.begin;
		while (true) {
			Transaction tr(cx);
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				RangeResult res = co_await tr.getRange(KeyRangeRef(begin, normalKeys.end), 1000);
				for (const auto& kv : res) {
					out.digest.addKeyValue(kv.key, kv.value);
					++out.kvCount;
					out.byteCount += kv.key.size() + kv.value.size();
				}
				if (!res.more || res.empty()) {
					co_return out;
				}
				begin = keyAfter(res.back().key);
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	// Count the shards spanning normalKeys. getRangeSplitPoints with a large chunk size returns only
	// the shard boundaries (plus begin/end), so shardCount == splitPoints.size() - 1.
	Future<int> countShards(Database cx) {
		while (true) {
			Transaction tr(cx);
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Standalone<VectorRef<KeyRef>> splitPoints =
				    co_await tr.getRangeSplitPoints(normalKeys, /*chunkSize=*/10000000);
				co_return std::max(0, (int)splitPoints.size() - 1);
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}
	// Retries the whole audit on transient cluster errors (e.g. movekeys_conflict during DD failover),
	// mirroring RestoreValidation's tolerance for buggify-induced instability.
	Future<RangeDigestSummary> runOneDigest(Database cx) {
		Reference<IClusterConnectionRecord> clusterFile = cx->getConnectionRecord();
		int auditRetryCount = 0;
		int maxAuditRetries = 10;
		while (true) {
			Error retryErr;
			try {
				UID auditId = co_await auditStorage(
				    clusterFile, normalKeys, AuditType::RangeDigest, KeyValueStoreType::END, maxWaitTime);
				TraceEvent("RangeDigestValidationAuditScheduled")
				    .detail("AuditID", auditId)
				    .detail("RetryCount", auditRetryCount);

				double startTime = now();
				bool done = false;
				AuditStorageState finalState;
				while (!done) {
					co_await delay(checkInterval);
					AuditStorageState st = co_await getAuditState(cx, AuditType::RangeDigest, auditId);
					if (st.getPhase() == AuditPhase::Complete) {
						finalState = st;
						done = true;
						break;
					}
					if (st.getPhase() == AuditPhase::Error || st.getPhase() == AuditPhase::Failed) {
						TraceEvent(SevWarn, "RangeDigestValidationAuditPhaseFailed")
						    .detail("AuditID", auditId)
						    .detail("Phase", (int)st.getPhase())
						    .detail("Error", st.error);
						throw audit_storage_failed();
					}
					if (now() - startTime > maxWaitTime) {
						TraceEvent(SevWarn, "RangeDigestValidationTimeout").detail("AuditID", auditId);
						throw timed_out();
					}
				}

				// The combined root is stored in the top-level audit record on completion (the
				// per-range progress is cleared by then).
				RangeDigestSummary summary;
				summary.root = RangeDigest::fromBytes(finalState.digest);
				summary.kvCount = finalState.kvCount;
				summary.byteCount = finalState.byteCount;
				summary.complete = true;
				TraceEvent("RangeDigestValidationAuditRoot")
				    .detail("AuditID", auditId)
				    .detail("Root", summary.root.toHex())
				    .detail("KVCount", summary.kvCount)
				    .detail("Bytes", summary.byteCount)
				    .detail("Complete", summary.complete);
				co_return summary;
			} catch (Error& e) {
				retryErr = e;
			}
			if (retryErr.code() == error_code_actor_cancelled) {
				throw retryErr;
			}
			// audit_storage_failed and related transient conditions are retryable under buggify.
			if ((retryErr.code() == error_code_audit_storage_failed ||
			     retryErr.code() == error_code_persist_new_audit_metadata_error ||
			     retryErr.code() == error_code_audit_storage_cancelled ||
			     retryErr.code() == error_code_audit_storage_task_outdated) &&
			    auditRetryCount < maxAuditRetries) {
				++auditRetryCount;
				TraceEvent(SevWarn, "RangeDigestValidationAuditRetry")
				    .error(retryErr)
				    .detail("RetryCount", auditRetryCount)
				    .detail("MaxRetries", maxAuditRetries);
				co_await delay(std::min(10.0, 2.0 * auditRetryCount));
			} else {
				throw retryErr;
			}
		}
	}

	Future<Void> _start(Database cx) {
		co_await delay(validateAfter);

		// Best-effort wait for DD to split the data into multiple shards so the additive combine is
		// exercised across shards/servers. NOT reaching expectMinShards is not a correctness failure in
		// the randomized Joshua matrix (some seeds keep one shard), so only warn -- unless
		// strictShardCheck is set for a dedicated multi-shard run.
		double settleStart = now();
		int shards = 0;
		while (true) {
			shards = co_await countShards(cx);
			if (shards >= expectMinShards) {
				break;
			}
			if (now() - settleStart > shardSettleTimeout) {
				if (strictShardCheck) {
					TraceEvent(SevError, "RangeDigestValidationTooFewShards")
					    .detail("Shards", shards)
					    .detail("ExpectMinShards", expectMinShards);
					ASSERT(false);
				}
				TraceEvent(SevWarn, "RangeDigestValidationFewShards")
				    .detail("Shards", shards)
				    .detail("ExpectMinShards", expectMinShards);
				break;
			}
			co_await delay(2.0);
		}
		TraceEvent("RangeDigestValidationShardsReady").detail("Shards", shards).detail("ExpectMin", expectMinShards);

		// Run the independent computation and the two SS-side audits. runOneDigest already retries the
		// transient audit errors, so an error escaping to here means the digest never completed and the
		// comparison never ran -- check() fails on !validated rather than reporting a green skip. The
		// strict content asserts below run OUTSIDE this try so a genuine mismatch (which throws
		// internal_error via ASSERT) is never swallowed.
		ExpectedDigest expected;
		RangeDigestSummary first;
		RangeDigestSummary second;
		try {
			expected = co_await computeExpectedDigest(cx);
			TraceEvent("RangeDigestValidationExpected")
			    .detail("Root", expected.digest.toHex())
			    .detail("KVCount", expected.kvCount)
			    .detail("Bytes", expected.byteCount);
			first = co_await runOneDigest(cx);
			second = co_await runOneDigest(cx);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevWarnAlways, "RangeDigestValidationDidNotComplete")
			    .error(e)
			    .detail("Reason", "Audit never reached Complete; content comparison did not run");
			co_return;
		}

		// Both audits completed, and DD publishes a root only over fully-tiled coverage, so any
		// disagreement here is a genuine correctness violation -> hard fail.
		if (first.root != expected.digest || first.kvCount != expected.kvCount ||
		    first.byteCount != expected.byteCount) {
			TraceEvent(SevError, "RangeDigestValidationMismatchVsExpected")
			    .detail("AuditRoot", first.root.toHex())
			    .detail("ExpectedRoot", expected.digest.toHex())
			    .detail("AuditKV", first.kvCount)
			    .detail("ExpectedKV", expected.kvCount)
			    .detail("AuditBytes", first.byteCount)
			    .detail("ExpectedBytes", expected.byteCount);
			ASSERT(false);
		}
		if (second.root != first.root) {
			TraceEvent(SevError, "RangeDigestValidationNondeterministicRoot")
			    .detail("FirstRoot", first.root.toHex())
			    .detail("SecondRoot", second.root.toHex());
			ASSERT(false);
		}

		TraceEvent("RangeDigestValidationSuccess")
		    .detail("Root", first.root.toHex())
		    .detail("KVCount", first.kvCount)
		    .detail("Bytes", first.byteCount)
		    .detail("Shards", shards);
		validated = true;
	}
};

WorkloadFactory<RangeDigestValidationWorkload> RangeDigestValidationWorkloadFactory;
