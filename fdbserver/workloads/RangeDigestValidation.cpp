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
//   4. It forces a repartition (excluding a storage server that owns part of the range), then
//      triggers a SECOND RangeDigest audit and asserts the root is identical. Because the layout
//      really changed, this asserts partition-independence and not merely that a deterministic fold
//      is deterministic. On a cluster too small to spare a server the repartition is skipped and the
//      run degrades to a determinism check, recorded as PartitionChanged=0.
//
// Every digest is preceded by a wait for quiescence (data in flight and the DD queue both drained),
// which is the precondition the digest requires; a timeout there is reported as inconclusive rather
// than asserted on.

#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/RangeDigest.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/QuietDatabase.h"
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
	double quiesceTimeout;
	double exclusionTimeout;
	bool forceMovementBetweenDigests;
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
		quiesceTimeout = getOption(options, "quiesceTimeout"_sr, 120.0);
		exclusionTimeout = getOption(options, "exclusionTimeout"_sr, 180.0);
		forceMovementBetweenDigests = getOption(options, "forceMovementBetweenDigests"_sr, true);
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

	// Canonical description of how normalKeys is currently partitioned AND who owns each piece, so a
	// repartition can be demonstrated rather than assumed. Ownership matters independently of
	// boundaries: relocating a shard to a different team changes which server folds those keys while
	// leaving the boundary list identical.
	//
	// `owners`, when non-null, collects the servers currently holding any of normalKeys. Excluding a
	// server outside that set relocates nothing, which at this dataset size is the common case: ~1MB
	// spread over a triple-replicated 18-machine cluster leaves most servers holding no rdv/ keys at all.
	Future<std::string> capturePartition(Database cx, std::set<UID>* owners = nullptr) {
		Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		while (true) {
			Error err;
			try {
				RangeResult shards = co_await krmGetRanges(
				    &tr, keyServersPrefix, normalKeys, CLIENT_KNOBS->TOO_MANY, CLIENT_KNOBS->TOO_MANY);
				RangeResult UIDtoTagMap = co_await tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY);
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
				std::string out;
				if (owners != nullptr) {
					owners->clear();
				}
				for (int i = 0; i + 1 < shards.size(); ++i) {
					std::vector<UID> src, dest;
					UID srcId, destId;
					decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest, srcId, destId);
					std::sort(src.begin(), src.end());
					out += shards[i].key.toString() + "=";
					for (const UID& id : src) {
						out += id.shortString() + ",";
						if (owners != nullptr) {
							owners->insert(id);
						}
					}
					out += ";";
				}
				co_return out;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	// Wait for data distribution to go idle. The digest requires a quiescent cluster: every server
	// folds at its own read version, and the additive combine assumes each key-value is folded exactly
	// once, so a shard in flight can be counted by both the losing and the gaining server or by
	// neither. Digesting mid-movement is not a theoretical hazard -- at 100M scale it produced a root
	// over-counting ~104.8M against a true ~100M.
	//
	// Returns false on timeout so the caller can decline to assert rather than digest a moving cluster.
	Future<bool> waitForQuiescence(Database cx, double timeoutSeconds) {
		double start = now();
		while (now() - start < timeoutSeconds) {
			Error err;
			try {
				int64_t inFlight = co_await getDataInFlight(cx, dbInfo);
				int64_t queueSize = co_await getDataDistributionQueueSize(cx, dbInfo, /*reportInFlight=*/true);
				if (inFlight == 0 && queueSize == 0) {
					TraceEvent("RangeDigestValidationQuiesced").detail("Elapsed", now() - start);
					co_return true;
				}
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_actor_cancelled) {
				throw err;
			}
			if (err.code() != invalid_error_code) {
				// These metrics come from per-worker event logs, so they throw (attribute_not_found when
				// a server's worker cannot be found, timed_out when it does not answer) exactly while the
				// cluster is churning -- which is when this is called. Not being able to observe
				// quiescence is not evidence of it, so keep polling and let the timeout be the only exit.
				TraceEvent(SevInfo, "RangeDigestValidationQuiesceProbeFailed")
				    .error(err)
				    .detail("Elapsed", now() - start);
			}
			co_await delay(1.0);
		}
		TraceEvent(SevWarn, "RangeDigestValidationQuiesceTimeout").detail("Timeout", timeoutSeconds);
		co_return false;
	}

	// Force a repartition between the two digests by excluding one storage server, so DD relocates its
	// shards onto other teams. Without this the second digest re-runs over the same layout and asserts
	// only that a deterministic fold is deterministic -- it would pass even if the digest were
	// partition-DEPENDENT, which is the one property the whole before/after comparison rests on.
	//
	// Uses exclusion rather than a direct moveKeys: this audit is driven by data distribution, so
	// setDDMode(0) (which RandomMoveKeys needs to take the moveKeys lock) risks stalling the very
	// mechanism under test.
	//
	// Returns false when no repartition was performed. That is never a failure: a simulated cluster
	// that cannot spare a server must not turn this test red.
	Future<bool> forceRepartition(Database cx) {
		// Read the configured replication factor rather than assuming triple: excluding down to exactly
		// the replica count leaves DD no destination team and the exclusion never completes.
		DatabaseConfiguration configuration;
		{
			Transaction tr(cx);
			while (true) {
				Error err;
				try {
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
					RangeResult res = co_await tr.getRange(configKeys, 1000);
					ASSERT(!res.more);
					for (const auto& kv : res) {
						configuration.set(kv.key, kv.value);
					}
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
		}

		// Only a server that actually holds part of normalKeys is worth excluding; draining a server
		// with no rdv/ keys relocates nothing and leaves the layout identical, which is what happens on
		// most seeds because the dataset is far smaller than the fleet.
		std::set<UID> owners;
		co_await capturePartition(cx, &owners);

		std::vector<StorageServerInterface> servers = co_await getStorageServers(cx);
		std::vector<StorageServerInterface> candidates;
		int nonTssCount = 0;
		for (const auto& ss : servers) {
			if (ss.isTss()) {
				continue;
			}
			++nonTssCount;
			if (owners.contains(ss.id())) {
				candidates.push_back(ss);
			}
		}
		if (nonTssCount <= configuration.storageTeamSize + 1 || candidates.empty()) {
			TraceEvent(SevWarn, "RangeDigestValidationSkipRepartition")
			    .detail("Reason", candidates.empty() ? "NoServerOwnsAuditRange" : "TooFewStorageServers")
			    .detail("NonTssServers", nonTssCount)
			    .detail("OwningCandidates", candidates.size())
			    .detail("StorageTeamSize", configuration.storageTeamSize);
			co_return false;
		}

		const StorageServerInterface victim = candidates[deterministicRandom()->randomInt(0, candidates.size())];
		AddressExclusion exclusion(victim.address().ip, victim.address().port);
		TraceEvent("RangeDigestValidationExcluding").detail("Server", victim.id()).detail("Address", victim.address());
		try {
			co_await excludeServers(cx, std::vector<AddressExclusion>{ exclusion });
			// waitForAllExcluded: return only once the server holds no data, i.e. the relocation the
			// repartition depends on has actually happened.
			Optional<std::set<NetworkAddress>> excluded = co_await timeout(
			    checkForExcludingServers(cx, std::vector<AddressExclusion>{ exclusion }, /*waitForAllExcluded=*/true),
			    exclusionTimeout);
			if (!excluded.present()) {
				TraceEvent(SevWarn, "RangeDigestValidationSkipRepartition")
				    .detail("Reason", "ExclusionTimedOut")
				    .detail("Server", victim.id());
				co_await includeServers(cx, std::vector<AddressExclusion>(1));
				co_return false;
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevWarn, "RangeDigestValidationSkipRepartition")
			    .error(e)
			    .detail("Reason", "ExclusionFailed")
			    .detail("Server", victim.id());
			co_return false;
		}
		TraceEvent("RangeDigestValidationExcluded").detail("Server", victim.id());
		co_return true;
	}

	// Retries the whole audit on transient cluster errors (e.g. movekeys_conflict during DD failover),
	// mirroring RestoreValidation's tolerance for buggify-induced instability.
	Future<RangeDigestSummary> runOneDigest(Database cx) {
		Reference<IClusterConnectionRecord> clusterFile = cx->getConnectionRecord();
		int auditRetryCount = 0;
		int maxAuditRetries = 10;
		while (true) {
			Error retryErr;
			// Distinguishes a launch timeout from a completion timeout: both surface as timed_out, but
			// only the former is unambiguously transient (see the retry decision below).
			bool auditScheduled = false;
			try {
				UID auditId = co_await auditStorage(
				    clusterFile, normalKeys, AuditType::RangeDigest, KeyValueStoreType::END, maxWaitTime);
				auditScheduled = true;
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
			// audit_storage_failed and related transient conditions are retryable under buggify. A
			// timed_out from auditStorage() means the CC never issued an audit ID -- no audit exists to
			// be stuck, so it is equally transient. A timed_out from the completion wait is NOT retried:
			// that audit was scheduled and stopped making progress, which is a finding, and re-arming it
			// maxAuditRetries times would spend maxWaitTime on each before failing anyway.
			if ((retryErr.code() == error_code_audit_storage_failed ||
			     retryErr.code() == error_code_persist_new_audit_metadata_error ||
			     retryErr.code() == error_code_audit_storage_cancelled ||
			     retryErr.code() == error_code_audit_storage_task_outdated ||
			     (retryErr.code() == error_code_timed_out && !auditScheduled)) &&
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
		bool repartitioned = false;
		bool partitionActuallyChanged = false;
		try {
			// The audit requires a quiescent cluster. The load above is small enough that DD normally
			// settles long before this, but relying on that is relying on the dataset staying small.
			if (!co_await waitForQuiescence(cx, quiesceTimeout)) {
				TraceEvent(SevWarnAlways, "RangeDigestValidationInconclusive")
				    .detail("Reason", "Cluster never quiesced before the first digest");
				co_return;
			}

			expected = co_await computeExpectedDigest(cx);
			TraceEvent("RangeDigestValidationExpected")
			    .detail("Root", expected.digest.toHex())
			    .detail("KVCount", expected.kvCount)
			    .detail("Bytes", expected.byteCount);
			first = co_await runOneDigest(cx);

			// Repartition between the digests so the second one is taken over a different physical
			// layout. Without this the second digest only re-confirms determinism.
			std::string partitionBefore = co_await capturePartition(cx);
			if (forceMovementBetweenDigests) {
				repartitioned = co_await forceRepartition(cx);
			}
			if (repartitioned) {
				// The exclusion guarantees the excluded server is drained, not that the cluster as a
				// whole is idle; DD keeps rebalancing after. Digesting now would re-create the 100M
				// over-count, so settle before the second digest rather than after re-including.
				if (!co_await waitForQuiescence(cx, quiesceTimeout)) {
					TraceEvent(SevWarnAlways, "RangeDigestValidationInconclusive")
					    .detail("Reason", "Cluster never re-quiesced after the forced repartition");
					co_await includeServers(cx, std::vector<AddressExclusion>(1));
					co_return;
				}
				std::string partitionAfter = co_await capturePartition(cx);
				partitionActuallyChanged = partitionAfter != partitionBefore;
				TraceEvent("RangeDigestValidationRepartitioned")
				    .detail("PartitionChanged", partitionActuallyChanged)
				    .detail("ShardsBefore", std::count(partitionBefore.begin(), partitionBefore.end(), ';'))
				    .detail("ShardsAfter", std::count(partitionAfter.begin(), partitionAfter.end(), ';'));
			}

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
		// When the layout provably changed, this is a partition-independence assertion; otherwise it
		// degrades to the determinism check it has always been. PartitionChanged distinguishes the two
		// in the trace, so a green run cannot be mistaken for the stronger claim.
		if (second.root != first.root) {
			TraceEvent(SevError, "RangeDigestValidationNondeterministicRoot")
			    .detail("FirstRoot", first.root.toHex())
			    .detail("SecondRoot", second.root.toHex())
			    .detail("Repartitioned", repartitioned)
			    .detail("PartitionChanged", partitionActuallyChanged);
			ASSERT(false);
		}

		TraceEvent("RangeDigestValidationSuccess")
		    .detail("Root", first.root.toHex())
		    .detail("KVCount", first.kvCount)
		    .detail("Bytes", first.byteCount)
		    .detail("Shards", shards)
		    .detail("Repartitioned", repartitioned)
		    .detail("PartitionChanged", partitionActuallyChanged);
		validated = true;
		if (repartitioned) {
			// Leave the fleet as we found it for any workload that follows and for the end-of-test
			// quiescence check.
			co_await includeServers(cx, std::vector<AddressExclusion>(1));
		}
	}
};

WorkloadFactory<RangeDigestValidationWorkload> RangeDigestValidationWorkloadFactory;
