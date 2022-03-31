/*
 * ProxyCommitData.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_PROXYCOMMITDATA_ACTOR_G_H)
#define FDBSERVER_PROXYCOMMITDATA_ACTOR_G_H
#include "fdbserver/ProxyCommitData.actor.g.h"
#elif !defined(FDBSERVER_PROXYCOMMITDATA_ACTOR_H)
#define FDBSERVER_PROXYCOMMITDATA_ACTOR_H

#include "fdbclient/FDBTypes.h"
#include "fdbclient/Tenant.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogSystemDiskQueueAdapter.h"
#include "flow/IRandom.h"

#include "flow/actorcompiler.h" // This must be the last #include.

DESCR struct SingleKeyMutation {
	Standalone<StringRef> shardBegin;
	Standalone<StringRef> shardEnd;
	int64_t tag1;
	int64_t tag2;
	int64_t tag3;
};

struct ApplyMutationsData {
	Future<Void> worker;
	Version endVersion;
	Reference<KeyRangeMap<Version>> keyVersion;
};

struct ProxyStats {
	CounterCollection cc;
	Counter txnCommitIn, txnCommitVersionAssigned, txnCommitResolving, txnCommitResolved, txnCommitOut,
	    txnCommitOutSuccess, txnCommitErrors;
	Counter txnConflicts;
	Counter txnRejectedForQueuedTooLong;
	Counter commitBatchIn, commitBatchOut;
	Counter mutationBytes;
	Counter mutations;
	Counter conflictRanges;
	Counter keyServerLocationIn, keyServerLocationOut, keyServerLocationErrors;
	Counter txnExpensiveClearCostEstCount;
	Version lastCommitVersionAssigned;

	LatencySample commitLatencySample;
	LatencyBands commitLatencyBands;

	// Ratio of tlogs receiving empty commit messages.
	LatencySample commitBatchingEmptyMessageRatio;

	LatencySample commitBatchingWindowSize;

	Future<Void> logger;

	int64_t maxComputeNS;
	int64_t minComputeNS;

	Reference<Histogram> commitBatchQueuingDist;
	Reference<Histogram> getCommitVersionDist;
	std::vector<Reference<Histogram>> resolverDist;
	Reference<Histogram> resolutionDist;
	Reference<Histogram> postResolutionDist;
	Reference<Histogram> processingMutationDist;
	Reference<Histogram> tlogLoggingDist;
	Reference<Histogram> replyCommitDist;

	int64_t getAndResetMaxCompute() {
		int64_t r = maxComputeNS;
		maxComputeNS = 0;
		return r;
	}

	int64_t getAndResetMinCompute() {
		int64_t r = minComputeNS;
		minComputeNS = 1e12;
		return r;
	}

	explicit ProxyStats(UID id,
	                    NotifiedVersion* pVersion,
	                    NotifiedVersion* pCommittedVersion,
	                    int64_t* commitBatchesMemBytesCountPtr)
	  : cc("ProxyStats", id.toString()), txnCommitIn("TxnCommitIn", cc),
	    txnCommitVersionAssigned("TxnCommitVersionAssigned", cc), txnCommitResolving("TxnCommitResolving", cc),
	    txnCommitResolved("TxnCommitResolved", cc), txnCommitOut("TxnCommitOut", cc),
	    txnCommitOutSuccess("TxnCommitOutSuccess", cc), txnCommitErrors("TxnCommitErrors", cc),
	    txnConflicts("TxnConflicts", cc), txnRejectedForQueuedTooLong("TxnRejectedForQueuedTooLong", cc),
	    commitBatchIn("CommitBatchIn", cc), commitBatchOut("CommitBatchOut", cc), mutationBytes("MutationBytes", cc),
	    mutations("Mutations", cc), conflictRanges("ConflictRanges", cc),
	    keyServerLocationIn("KeyServerLocationIn", cc), keyServerLocationOut("KeyServerLocationOut", cc),
	    keyServerLocationErrors("KeyServerLocationErrors", cc),
	    txnExpensiveClearCostEstCount("ExpensiveClearCostEstCount", cc), lastCommitVersionAssigned(0),
	    commitLatencySample("CommitLatencyMetrics",
	                        id,
	                        SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                        SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    commitLatencyBands("CommitLatencyBands", id, SERVER_KNOBS->STORAGE_LOGGING_DELAY),
	    commitBatchingEmptyMessageRatio("CommitBatchingEmptyMessageRatio",
	                                    id,
	                                    SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                                    SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    commitBatchingWindowSize("CommitBatchingWindowSize",
	                             id,
	                             SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                             SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    maxComputeNS(0), minComputeNS(1e12),
	    commitBatchQueuingDist(Histogram::getHistogram(LiteralStringRef("CommitProxy"),
	                                                   LiteralStringRef("CommitBatchQueuing"),
	                                                   Histogram::Unit::microseconds)),
	    getCommitVersionDist(Histogram::getHistogram(LiteralStringRef("CommitProxy"),
	                                                 LiteralStringRef("GetCommitVersion"),
	                                                 Histogram::Unit::microseconds)),
	    resolutionDist(Histogram::getHistogram(LiteralStringRef("CommitProxy"),
	                                           LiteralStringRef("Resolution"),
	                                           Histogram::Unit::microseconds)),
	    postResolutionDist(Histogram::getHistogram(LiteralStringRef("CommitProxy"),
	                                               LiteralStringRef("PostResolutionQueuing"),
	                                               Histogram::Unit::microseconds)),
	    processingMutationDist(Histogram::getHistogram(LiteralStringRef("CommitProxy"),
	                                                   LiteralStringRef("ProcessingMutation"),
	                                                   Histogram::Unit::microseconds)),
	    tlogLoggingDist(Histogram::getHistogram(LiteralStringRef("CommitProxy"),
	                                            LiteralStringRef("TlogLogging"),
	                                            Histogram::Unit::microseconds)),
	    replyCommitDist(Histogram::getHistogram(LiteralStringRef("CommitProxy"),
	                                            LiteralStringRef("ReplyCommit"),
	                                            Histogram::Unit::microseconds)) {
		specialCounter(cc, "LastAssignedCommitVersion", [this]() { return this->lastCommitVersionAssigned; });
		specialCounter(cc, "Version", [pVersion]() { return pVersion->get(); });
		specialCounter(cc, "CommittedVersion", [pCommittedVersion]() { return pCommittedVersion->get(); });
		specialCounter(cc, "CommitBatchesMemBytesCount", [commitBatchesMemBytesCountPtr]() {
			return *commitBatchesMemBytesCountPtr;
		});
		specialCounter(cc, "MaxCompute", [this]() { return this->getAndResetMaxCompute(); });
		specialCounter(cc, "MinCompute", [this]() { return this->getAndResetMinCompute(); });
		logger = traceCounters("ProxyMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "ProxyMetrics");
	}
};

struct ProxyCommitData {
	UID dbgid;
	int64_t commitBatchesMemBytesCount;
	ProxyStats stats;
	MasterInterface master;
	std::vector<ResolverInterface> resolvers;
	LogSystemDiskQueueAdapter* logAdapter;
	Reference<ILogSystem> logSystem;
	IKeyValueStore* txnStateStore;
	NotifiedVersion committedVersion; // Provided that this recovery has succeeded or will succeed, this version is
	                                  // fully committed (durable)
	Version minKnownCommittedVersion; // No version smaller than this one will be used as the known committed version
	                                  // during recovery
	NotifiedVersion version; // The version at which txnStateStore is up to date
	Promise<Void> validState; // Set once txnStateStore and version are valid
	double lastVersionTime;
	KeyRangeMap<std::set<Key>> vecBackupKeys;
	uint64_t commitVersionRequestNumber;
	uint64_t mostRecentProcessedRequestNumber;
	KeyRangeMap<Deque<std::pair<Version, int>>> keyResolvers;
	// When all resolvers process system keys (for private mutations), the "keyResolvers"
	// only tracks normalKeys. This is used for tracking versions for systemKeys.
	Deque<Version> systemKeyVersions;
	KeyRangeMap<ServerCacheInfo> keyInfo; // keyrange -> all storage servers in all DCs for the keyrange
	KeyRangeMap<bool> cacheInfo;
	std::map<Key, ApplyMutationsData> uid_applyMutationsData;
	bool firstProxy;
	double lastCoalesceTime;
	bool locked;
	Optional<Value> metadataVersion;
	double commitBatchInterval;

	int64_t localCommitBatchesStarted;
	NotifiedVersion latestLocalCommitBatchResolving;
	NotifiedVersion latestLocalCommitBatchLogging;

	RequestStream<GetReadVersionRequest> getConsistentReadVersion;
	RequestStream<CommitTransactionRequest> commit;
	Database cx;
	Reference<AsyncVar<ServerDBInfo> const> db;
	EventMetricHandle<SingleKeyMutation> singleKeyMutationEvent;

	std::map<UID, Reference<StorageInfo>> storageCache;
	std::unordered_map<UID, StorageServerInterface> tssMapping;
	std::map<Tag, Version> tag_popped;
	Deque<std::pair<Version, Version>> txsPopVersions;
	Version lastTxsPop;
	bool popRemoteTxs;
	std::vector<Standalone<StringRef>> whitelistedBinPathVec;

	Optional<LatencyBandConfig> latencyBandConfig;
	double lastStartCommit;
	double lastCommitLatency;
	int updateCommitRequests = 0;
	NotifiedDouble lastCommitTime;

	std::vector<double> commitComputePerOperation;
	UIDTransactionTagMap<TransactionCommitCostEstimation> ssTrTagCommitCost;
	double lastMasterReset;
	double lastResolverReset;

	std::map<TenantName, TenantMapEntry> tenantMap;

	// The tag related to a storage server rarely change, so we keep a vector of tags for each key range to be slightly
	// more CPU efficient. When a tag related to a storage server does change, we empty out all of these vectors to
	// signify they must be repopulated. We do not repopulate them immediately to avoid a slow task.
	const std::vector<Tag>& tagsForKey(StringRef key) {
		auto& tags = keyInfo[key].tags;
		if (!tags.size()) {
			auto& r = keyInfo.rangeContaining(key).value();
			r.populateTags();
			return r.tags;
		}
		return tags;
	}

	bool needsCacheTag(KeyRangeRef range) {
		auto ranges = cacheInfo.intersectingRanges(range);
		for (auto r : ranges) {
			if (r.value()) {
				return true;
			}
		}
		return false;
	}

	void updateLatencyBandConfig(Optional<LatencyBandConfig> newLatencyBandConfig) {
		if (newLatencyBandConfig.present() != latencyBandConfig.present() ||
		    (newLatencyBandConfig.present() &&
		     newLatencyBandConfig.get().commitConfig != latencyBandConfig.get().commitConfig)) {
			TraceEvent("LatencyBandCommitUpdatingConfig").detail("Present", newLatencyBandConfig.present());
			stats.commitLatencyBands.clearBands();
			if (newLatencyBandConfig.present()) {
				for (auto band : newLatencyBandConfig.get().commitConfig.bands) {
					stats.commitLatencyBands.addThreshold(band);
				}
			}
		}

		latencyBandConfig = newLatencyBandConfig;
	}

	void updateSSTagCost(const UID& id, const TagSet& tagSet, MutationRef m, int cost) {
		auto [it, _] = ssTrTagCommitCost.try_emplace(id, TransactionTagMap<TransactionCommitCostEstimation>());

		for (auto& tag : tagSet) {
			auto& costItem = it->second[tag];
			if (m.isAtomicOp() || m.type == MutationRef::Type::SetValue || m.type == MutationRef::Type::ClearRange) {
				costItem.opsSum++;
				costItem.costSum += cost;
			}
		}
	}

	ProxyCommitData(UID dbgid,
	                MasterInterface master,
	                RequestStream<GetReadVersionRequest> getConsistentReadVersion,
	                Version recoveryTransactionVersion,
	                RequestStream<CommitTransactionRequest> commit,
	                Reference<AsyncVar<ServerDBInfo> const> db,
	                bool firstProxy)
	  : dbgid(dbgid), commitBatchesMemBytesCount(0),
	    stats(dbgid, &version, &committedVersion, &commitBatchesMemBytesCount), master(master), logAdapter(nullptr),
	    txnStateStore(nullptr), committedVersion(recoveryTransactionVersion), minKnownCommittedVersion(0), version(0),
	    lastVersionTime(0), commitVersionRequestNumber(1), mostRecentProcessedRequestNumber(0), firstProxy(firstProxy),
	    lastCoalesceTime(0), locked(false), commitBatchInterval(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_MIN),
	    localCommitBatchesStarted(0), getConsistentReadVersion(getConsistentReadVersion), commit(commit),
	    cx(openDBOnServer(db, TaskPriority::DefaultEndpoint, LockAware::True)), db(db),
	    singleKeyMutationEvent(LiteralStringRef("SingleKeyMutation")), lastTxsPop(0), popRemoteTxs(false),
	    lastStartCommit(0), lastCommitLatency(SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION), lastCommitTime(0),
	    lastMasterReset(now()), lastResolverReset(now()) {
		commitComputePerOperation.resize(SERVER_KNOBS->PROXY_COMPUTE_BUCKETS, 0.0);
	}
};

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_PROXYCOMMITDATA_H
