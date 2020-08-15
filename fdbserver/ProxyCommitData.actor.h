/*
 * ProxyCommitData.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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
	Counter txnRequestIn, txnRequestOut, txnRequestErrors;
	Counter txnStartIn, txnStartOut, txnStartBatch;
	Counter txnSystemPriorityStartIn, txnSystemPriorityStartOut;
	Counter txnBatchPriorityStartIn, txnBatchPriorityStartOut;
	Counter txnDefaultPriorityStartIn, txnDefaultPriorityStartOut;
	Counter txnCommitIn, txnCommitVersionAssigned, txnCommitResolving, txnCommitResolved, txnCommitOut,
	    txnCommitOutSuccess, txnCommitErrors;
	Counter txnConflicts;
	Counter txnThrottled;
	Counter commitBatchIn, commitBatchOut;
	Counter mutationBytes;
	Counter mutations;
	Counter conflictRanges;
	Counter keyServerLocationIn, keyServerLocationOut, keyServerLocationErrors;
	Version lastCommitVersionAssigned;

	LatencySample commitLatencySample;
	LatencySample grvLatencySample;

	LatencyBands commitLatencyBands;
	LatencyBands grvLatencyBands;

	Future<Void> logger;

	int recentRequests;
	Deque<int> requestBuckets;
	double lastBucketBegin;
	double bucketInterval;

	void updateRequestBuckets() {
		while (now() - lastBucketBegin > bucketInterval) {
			lastBucketBegin += bucketInterval;
			recentRequests -= requestBuckets.front();
			requestBuckets.pop_front();
			requestBuckets.push_back(0);
		}
	}

	void addRequest() {
		updateRequestBuckets();
		++recentRequests;
		++requestBuckets.back();
	}

	int getRecentRequests() {
		updateRequestBuckets();
		return recentRequests * FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE /
		       (FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE - (lastBucketBegin + bucketInterval - now()));
	}

	explicit ProxyStats(UID id, Version* pVersion, NotifiedVersion* pCommittedVersion,
	                    int64_t* commitBatchesMemBytesCountPtr)
	  : cc("ProxyStats", id.toString()), recentRequests(0), lastBucketBegin(now()),
	    bucketInterval(FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE / FLOW_KNOBS->BASIC_LOAD_BALANCE_BUCKETS),
	    txnRequestIn("TxnRequestIn", cc), txnRequestOut("TxnRequestOut", cc), txnRequestErrors("TxnRequestErrors", cc),
	    txnStartIn("TxnStartIn", cc), txnStartOut("TxnStartOut", cc), txnStartBatch("TxnStartBatch", cc),
	    txnSystemPriorityStartIn("TxnSystemPriorityStartIn", cc),
	    txnSystemPriorityStartOut("TxnSystemPriorityStartOut", cc),
	    txnBatchPriorityStartIn("TxnBatchPriorityStartIn", cc),
	    txnBatchPriorityStartOut("TxnBatchPriorityStartOut", cc),
	    txnDefaultPriorityStartIn("TxnDefaultPriorityStartIn", cc),
	    txnDefaultPriorityStartOut("TxnDefaultPriorityStartOut", cc), txnCommitIn("TxnCommitIn", cc),
	    txnCommitVersionAssigned("TxnCommitVersionAssigned", cc), txnCommitResolving("TxnCommitResolving", cc),
	    txnCommitResolved("TxnCommitResolved", cc), txnCommitOut("TxnCommitOut", cc),
	    txnCommitOutSuccess("TxnCommitOutSuccess", cc), txnCommitErrors("TxnCommitErrors", cc),
	    txnConflicts("TxnConflicts", cc), txnThrottled("TxnThrottled", cc), commitBatchIn("CommitBatchIn", cc),
	    commitBatchOut("CommitBatchOut", cc), mutationBytes("MutationBytes", cc), mutations("Mutations", cc),
	    conflictRanges("ConflictRanges", cc), keyServerLocationIn("KeyServerLocationIn", cc),
	    keyServerLocationOut("KeyServerLocationOut", cc), keyServerLocationErrors("KeyServerLocationErrors", cc),
	    lastCommitVersionAssigned(0),
	    commitLatencySample("CommitLatencyMetrics", id, SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                        SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    grvLatencySample("GRVLatencyMetrics", id, SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                     SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    commitLatencyBands("CommitLatencyMetrics", id, SERVER_KNOBS->STORAGE_LOGGING_DELAY),
	    grvLatencyBands("GRVLatencyMetrics", id, SERVER_KNOBS->STORAGE_LOGGING_DELAY) {
		specialCounter(cc, "LastAssignedCommitVersion", [this]() { return this->lastCommitVersionAssigned; });
		specialCounter(cc, "Version", [pVersion]() { return *pVersion; });
		specialCounter(cc, "CommittedVersion", [pCommittedVersion]() { return pCommittedVersion->get(); });
		specialCounter(cc, "CommitBatchesMemBytesCount",
		               [commitBatchesMemBytesCountPtr]() { return *commitBatchesMemBytesCountPtr; });
		logger = traceCounters("ProxyMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "ProxyMetrics");
		for (int i = 0; i < FLOW_KNOBS->BASIC_LOAD_BALANCE_BUCKETS; i++) {
			requestBuckets.push_back(0);
		}
	}
};

struct TxnStateStoreWrapper : IKeyValueStore {
	/* begin IKeyValueStore interface */
	KeyValueStoreType getType() const override { return txnStateStore->getType(); }
	void set(KeyValueRef keyValue, const Arena* arena = nullptr) override {
		TraceEvent(SevDebug, "TxnStateStoreMutation", debugID)
		    .detail("Key", keyValue.key)
		    .detail("Value", keyValue.value)
		    .detail("Mutation", "Set");
		++mutationsApplied;
		txnStateStore->set(keyValue, arena);
	}
	void clear(KeyRangeRef range, const Arena* arena = nullptr) override {
		TraceEvent(SevDebug, "TxnStateStoreMutation", debugID)
		    .detail("Begin", range.begin)
		    .detail("End", range.end)
		    .detail("Mutation", "Clear");
		++mutationsApplied;
		txnStateStore->clear(range, arena);
	}
	Future<Void> commit(bool sequential = false) override { return txnStateStore->commit(sequential); }
	Future<Optional<Value>> readValue(KeyRef key, Optional<UID> debugID = Optional<UID>()) override {
		return txnStateStore->readValue(key, debugID);
	}
	Future<Optional<Value>> readValuePrefix(KeyRef key, int maxLength,
	                                        Optional<UID> debugID = Optional<UID>()) override {
		return txnStateStore->readValuePrefix(key, maxLength, debugID);
	}
	Future<Standalone<RangeResultRef>> readRange(KeyRangeRef keys, int rowLimit = 1 << 30,
	                                             int byteLimit = 1 << 30) override {
		return txnStateStore->readRange(keys, rowLimit, byteLimit);
	}
	std::tuple<size_t, size_t, size_t> getSize() const override { return txnStateStore->getSize(); }
	StorageBytes getStorageBytes() const override { return txnStateStore->getStorageBytes(); }
	void resyncLog() override { txnStateStore->resyncLog(); }
	void enableSnapshot() override { txnStateStore->enableSnapshot(); }
	Future<Void> init() override { return txnStateStore->init(); }
	/* end IKeyValueStore interface */

	/* begin ICloseable interface */
	Future<Void> getError() override { return txnStateStore->getError(); }
	Future<Void> onClosed() override { return txnStateStore->onClosed(); }
	void dispose() override { txnStateStore->dispose(); }
	void close() override { txnStateStore->close(); }
	/* end ICloseable interface */

	int getMutationsAppliedCount() const { return mutationsApplied; }
	void setTxnStateStore(IKeyValueStore* txnStateStore) { this->txnStateStore = txnStateStore; }
	void setDebugID(UID debugID) { this->debugID = debugID; }

private:
	UID debugID;
	int64_t mutationsApplied = 0;
	IKeyValueStore* txnStateStore = nullptr;
};

struct ProxyCommitData {
	UID dbgid;
	int64_t commitBatchesMemBytesCount;
	ProxyStats stats;
	MasterInterface master;
	vector<ResolverInterface> resolvers;
	LogSystemDiskQueueAdapter* logAdapter;
	Reference<ILogSystem> logSystem;
	TxnStateStoreWrapper txnStateStoreWrapper;
	IKeyValueStore* txnStateStore;
	NotifiedVersion committedVersion; // Provided that this recovery has succeeded or will succeed, this version is
	                                  // fully committed (durable)
	Version minKnownCommittedVersion; // No version smaller than this one will be used as the known committed version
	                                  // during recovery
	Version version; // The version at which txnStateStore is up to date
	Promise<Void> validState; // Set once txnStateStore and version are valid
	double lastVersionTime;
	KeyRangeMap<std::set<Key>> vecBackupKeys;
	uint64_t commitVersionRequestNumber;
	uint64_t mostRecentProcessedRequestNumber;
	KeyRangeMap<Deque<std::pair<Version, int>>> keyResolvers;
	KeyRangeMap<ServerCacheInfo> keyInfo;
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
	Reference<AsyncVar<ServerDBInfo>> db;
	EventMetricHandle<SingleKeyMutation> singleKeyMutationEvent;

	std::map<UID, Reference<StorageInfo>> storageCache;
	std::map<Tag, Version> tag_popped;
	Deque<std::pair<Version, Version>> txsPopVersions;
	Version lastTxsPop;
	bool popRemoteTxs;
	vector<Standalone<StringRef>> whitelistedBinPathVec;

	Optional<LatencyBandConfig> latencyBandConfig;
	double lastStartCommit;
	double lastCommitLatency;
	int updateCommitRequests = 0;
	NotifiedDouble lastCommitTime;

	vector<double> commitComputePerOperation;
	TransactionTagMap<TransactionCommitCostEstimation> transactionTagCommitCostEst;

	// The tag related to a storage server rarely change, so we keep a vector of tags for each key range to be slightly
	// more CPU efficient. When a tag related to a storage server does change, we empty out all of these vectors to
	// signify they must be repopulated. We do not repopulate them immediately to avoid a slow task.
	const vector<Tag>& tagsForKey(StringRef key) {
		auto& tags = keyInfo[key].tags;
		if (!tags.size()) {
			auto& r = keyInfo.rangeContaining(key).value();
			for (auto info : r.src_info) {
				r.tags.push_back(info->tag);
			}
			for (auto info : r.dest_info) {
				r.tags.push_back(info->tag);
			}
			uniquify(r.tags);
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
		     newLatencyBandConfig.get().grvConfig != latencyBandConfig.get().grvConfig)) {
			TraceEvent("LatencyBandGrvUpdatingConfig").detail("Present", newLatencyBandConfig.present());
			stats.grvLatencyBands.clearBands();
			if (newLatencyBandConfig.present()) {
				for (auto band : newLatencyBandConfig.get().grvConfig.bands) {
					stats.grvLatencyBands.addThreshold(band);
				}
			}
		}

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

	ProxyCommitData(UID dbgid, MasterInterface master, RequestStream<GetReadVersionRequest> getConsistentReadVersion,
	                Version recoveryTransactionVersion, RequestStream<CommitTransactionRequest> commit,
	                Reference<AsyncVar<ServerDBInfo>> db, bool firstProxy)
	  : dbgid(dbgid), stats(dbgid, &version, &committedVersion, &commitBatchesMemBytesCount), master(master),
	    logAdapter(NULL), txnStateStore(&txnStateStoreWrapper), popRemoteTxs(false),
	    committedVersion(recoveryTransactionVersion), version(0), minKnownCommittedVersion(0), lastVersionTime(0),
	    commitVersionRequestNumber(1), mostRecentProcessedRequestNumber(0),
	    getConsistentReadVersion(getConsistentReadVersion), commit(commit), lastCoalesceTime(0),
	    localCommitBatchesStarted(0), locked(false),
	    commitBatchInterval(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_MIN), firstProxy(firstProxy),
	    cx(openDBOnServer(db, TaskPriority::DefaultEndpoint, true, true)), db(db),
	    singleKeyMutationEvent(LiteralStringRef("SingleKeyMutation")), commitBatchesMemBytesCount(0), lastTxsPop(0),
	    lastStartCommit(0), lastCommitLatency(SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION), lastCommitTime(0) {
		commitComputePerOperation.resize(SERVER_KNOBS->PROXY_COMPUTE_BUCKETS, 0.0);
	}
};

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_PROXYCOMMITDATA_H