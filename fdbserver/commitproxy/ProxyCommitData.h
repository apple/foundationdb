/*
 * ProxyCommitData.h
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

#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/RangeLock.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/core/AccumulativeChecksumUtil.h"
#include "fdbserver/logsystem/ApplyMetadataMutation.h"
#include "fdbserver/logsystem/CDCRoutingTable.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/logsystem/LogSystem.h"
#include "fdbserver/logsystem/LogSystemConsumer.h"
#include "fdbserver/core/MasterInterface.h"
#include "fdbserver/core/ResolverInterface.h"
#include "../logsystem/include/fdbserver/logsystem/LogSystem.h"
#include "flow/IRandom.h"

struct SingleKeyMutationDescriptor {
	Standalone<StringRef> shardBegin;
	Standalone<StringRef> shardEnd;
	int64_t tag1;
	int64_t tag2;
	int64_t tag3;
};

template <>
struct Descriptor<SingleKeyMutationDescriptor>
  : DescribeType<SingleKeyMutationDescriptor,
                 "SingleKeyMutation",
                 DescribeField<&SingleKeyMutationDescriptor::shardBegin, "shardBegin">,
                 DescribeField<&SingleKeyMutationDescriptor::shardEnd, "shardEnd">,
                 DescribeField<&SingleKeyMutationDescriptor::tag1, "tag1">,
                 DescribeField<&SingleKeyMutationDescriptor::tag2, "tag2">,
                 DescribeField<&SingleKeyMutationDescriptor::tag3, "tag3">> {};

class LogSystemDiskQueueAdapter;

struct ProxyStats {
	enum class CommitBatchFlushReason {
		BYTE_LIMIT,
		COUNT_LIMIT,
		TIMEOUT,
		FIRST_IN_BATCH,
		TRANSACTION_SIZE_LIMIT,
	};

	CounterCollection cc;
	Counter txnCommitIn, txnCommitVersionAssigned, txnCommitResolving, txnCommitResolved, txnCommitOut,
	    txnCommitOutSuccess, txnCommitErrors;
	Counter txnConflicts;
	Counter txnRejectedForQueuedTooLong;
	Counter commitBatchIn, commitBatchOut;
	Counter commitBatchFlushByteLimit, commitBatchFlushCountLimit, commitBatchFlushTimeout,
	    commitBatchFlushFirstInBatch, commitBatchFlushTransactionSizeLimit;
	Counter mutationBytes;
	Counter mutations;
	Counter conflictRanges;
	Counter keyServerLocationIn, keyServerLocationOut, keyServerLocationErrors;
	Counter txnExpensiveClearCostEstCount;
	// rejectMutationsForReadLockOnRange takes the fast path (no locks held -> early return)
	// vs the slow path (per-mutation lock check). Observable in production via ProxyMetrics
	// to confirm the no-locks-active optimization fires when expected.
	Counter rangeLockFastPath, rangeLockSlowPath;
	Version lastCommitVersionAssigned;

	LatencySample commitLatencySample;
	LatencyBands commitLatencyBands;

	// Ratio of tlogs receiving empty commit messages.
	LatencySample commitBatchingEmptyMessageRatio;

	LatencySample commitBatchingWindowSize;

	LatencySample computeLatency;

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
	Reference<Histogram> transactionSizeDist;

	// These metrics are only logged as part of `ProxyDetailedMetrics`. Since
	// the detailed proxy metrics combine data from different sources, we can't
	// use a `Counter` along with a `CounterCollection` here, and instead have
	// to reimplement the basic functionality.
	std::unordered_set<NetworkAddress> uniqueClients;

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

	int64_t getSizeAndResetUniqueClients() {
		int64_t r = uniqueClients.size();
		uniqueClients.clear();
		return r;
	}

	void recordCommitBatchFlush(CommitBatchFlushReason reason) {
		switch (reason) {
		case CommitBatchFlushReason::BYTE_LIMIT:
			++commitBatchFlushByteLimit;
			break;
		case CommitBatchFlushReason::COUNT_LIMIT:
			++commitBatchFlushCountLimit;
			break;
		case CommitBatchFlushReason::TIMEOUT:
			++commitBatchFlushTimeout;
			break;
		case CommitBatchFlushReason::FIRST_IN_BATCH:
			++commitBatchFlushFirstInBatch;
			break;
		case CommitBatchFlushReason::TRANSACTION_SIZE_LIMIT:
			++commitBatchFlushTransactionSizeLimit;
			break;
		}
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
	    commitBatchIn("CommitBatchIn", cc), commitBatchOut("CommitBatchOut", cc),
	    commitBatchFlushByteLimit("CommitBatchFlushByteLimit", cc),
	    commitBatchFlushCountLimit("CommitBatchFlushCountLimit", cc),
	    commitBatchFlushTimeout("CommitBatchFlushTimeout", cc),
	    commitBatchFlushFirstInBatch("CommitBatchFlushFirstInBatch", cc),
	    commitBatchFlushTransactionSizeLimit("CommitBatchFlushTransactionSizeLimit", cc),
	    mutationBytes("MutationBytes", cc), mutations("Mutations", cc), conflictRanges("ConflictRanges", cc),
	    keyServerLocationIn("KeyServerLocationIn", cc), keyServerLocationOut("KeyServerLocationOut", cc),
	    keyServerLocationErrors("KeyServerLocationErrors", cc),
	    txnExpensiveClearCostEstCount("ExpensiveClearCostEstCount", cc), rangeLockFastPath("RangeLockFastPath", cc),
	    rangeLockSlowPath("RangeLockSlowPath", cc), lastCommitVersionAssigned(0),
	    commitLatencySample("CommitLatencyMetrics",
	                        id,
	                        SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                        SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
	    commitLatencyBands("CommitLatencyBands", id, SERVER_KNOBS->STORAGE_LOGGING_DELAY),
	    commitBatchingEmptyMessageRatio("CommitBatchingEmptyMessageRatio",
	                                    id,
	                                    SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                                    SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
	    commitBatchingWindowSize("CommitBatchingWindowSize",
	                             id,
	                             SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                             SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
	    computeLatency("ComputeLatency",
	                   id,
	                   SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                   SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
	    maxComputeNS(0), minComputeNS(1e12),
	    commitBatchQueuingDist(
	        Histogram::getHistogram("CommitProxy"_sr, "CommitBatchQueuing"_sr, Histogram::Unit::milliseconds)),
	    getCommitVersionDist(
	        Histogram::getHistogram("CommitProxy"_sr, "GetCommitVersion"_sr, Histogram::Unit::milliseconds)),
	    resolutionDist(Histogram::getHistogram("CommitProxy"_sr, "Resolution"_sr, Histogram::Unit::milliseconds)),
	    postResolutionDist(
	        Histogram::getHistogram("CommitProxy"_sr, "PostResolutionQueuing"_sr, Histogram::Unit::milliseconds)),
	    processingMutationDist(
	        Histogram::getHistogram("CommitProxy"_sr, "ProcessingMutation"_sr, Histogram::Unit::milliseconds)),
	    tlogLoggingDist(Histogram::getHistogram("CommitProxy"_sr, "TlogLogging"_sr, Histogram::Unit::milliseconds)),
	    replyCommitDist(Histogram::getHistogram("CommitProxy"_sr, "ReplyCommit"_sr, Histogram::Unit::milliseconds)),
	    transactionSizeDist(Histogram::getHistogram("CommitProxy"_sr, "TransactionSize"_sr, Histogram::Unit::bytes)) {
		specialCounter(cc, "LastAssignedCommitVersion", [this]() { return this->lastCommitVersionAssigned; });
		specialCounter(cc, "Version", [pVersion]() { return pVersion->get(); });
		specialCounter(cc, "CommittedVersion", [pCommittedVersion]() { return pCommittedVersion->get(); });
		specialCounter(cc, "CommitBatchesMemBytesCount", [commitBatchesMemBytesCountPtr]() {
			return *commitBatchesMemBytesCountPtr;
		});
		specialCounter(cc, "MaxCompute", [this]() { return this->getAndResetMaxCompute(); });
		specialCounter(cc, "MinCompute", [this]() { return this->getAndResetMinCompute(); });
		logger = cc.traceCounters("ProxyMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, "ProxyMetrics");
	}
};

struct ExpectedIdempotencyIdCountForKey {
	Version commitVersion = invalidVersion;
	int16_t idempotencyIdCount = 0;
	uint8_t batchIndexHighByte = 0;

	ExpectedIdempotencyIdCountForKey() = default;
	ExpectedIdempotencyIdCountForKey(Version commitVersion, int16_t idempotencyIdCount, uint8_t batchIndexHighByte)
	  : commitVersion(commitVersion), idempotencyIdCount(idempotencyIdCount), batchIndexHighByte(batchIndexHighByte) {}
};

class RangeLock;
struct ProxyCommitData {
	UID dbgid;
	int64_t commitBatchesMemBytesCount;
	ProxyStats stats;
	MasterInterface master;
	std::vector<ResolverInterface> resolvers;
	LogSystemDiskQueueAdapter* logAdapter;
	Reference<LogSystem> logSystem;
	Reference<LogSystemConsumer> logSystemConsumer;
	IKeyValueStore* txnStateStore;
	NotifiedVersion committedVersion; // Provided that this recovery has succeeded or will succeed, this version is
	                                  // fully committed (durable)
	Version minKnownCommittedVersion; // No version smaller than this one will be used as the known committed version
	                                  // during recovery
	NotifiedVersion version; // The version at which txnStateStore is up to date
	Promise<Void> validState; // Set once txnStateStore and version are valid
	double lastVersionTime;
	KeyRangeMap<std::set<Key>> vecBackupKeys;
	CDCRoutingTable cdcRouting;
	uint64_t commitVersionRequestNumber;
	uint64_t mostRecentProcessedRequestNumber;
	KeyRangeMap<Deque<std::pair<Version, int>>> keyResolvers;
	// When all resolvers process system keys (for private mutations), the "keyResolvers"
	// only tracks normalKeys. This is used for tracking versions for systemKeys.
	Deque<Version> systemKeyVersions;
	KeyRangeMap<ServerCacheInfo> keyInfo; // keyrange -> all storage servers in all DCs for the keyrange
	std::map<Key, ApplyMutationsData> uid_applyMutationsData;
	bool firstProxy;
	double lastCoalesceTime;
	bool locked;
	Optional<Value> metadataVersion;
	double commitBatchInterval;
	bool provisional;

	int64_t localCommitBatchesStarted;
	NotifiedVersion latestLocalCommitBatchResolving;
	NotifiedVersion latestLocalCommitBatchLogging;

	PublicRequestStream<CommitTransactionRequest> commit;
	Database cx;
	Reference<AsyncVar<ServerDBInfo> const> db;
	EventMetricHandle<SingleKeyMutationDescriptor> singleKeyMutationEvent;
	std::map<UID, Reference<StorageInfo>> storageCache;
	std::unordered_map<UID, StorageServerInterface> tssMapping;
	std::map<Tag, Version> tag_popped;
	Deque<std::pair<Version, Version>> txsPopVersions;
	Version lastTxsPop;
	bool popRemoteTxs;
	std::vector<Standalone<StringRef>> whitelistedBinPathVec;
	std::vector<std::pair<KeyRange, double>> hotShards;

	Optional<LatencyBandConfig> latencyBandConfig;
	double lastStartCommit;
	double lastCommitLatency;
	int updateCommitRequests = 0;
	NotifiedDouble lastCommitTime;

	std::vector<double> commitComputePerOperation;
	UIDTransactionTagMap<TransactionCommitCostEstimation> ssTrTagCommitCost;
	double lastMasterReset;
	double lastResolverReset;
	int localTLogCount = -1;

	PromiseStream<ExpectedIdempotencyIdCountForKey> expectedIdempotencyIdCountForKey;
	Standalone<VectorRef<MutationRef>> idempotencyClears;

	AsyncVar<bool> triggerCommit;

	uint16_t commitProxyIndex; // decided when the cluster controller recruits commit proxies
	std::shared_ptr<AccumulativeChecksumBuilder> acsBuilder = nullptr;
	LogEpoch epoch;

	Version lastShardMove;

	std::shared_ptr<RangeLock> rangeLock = nullptr;

	// The tag related to a storage server rarely change, so we keep a vector of tags for each key range to be slightly
	// more CPU efficient. When a tag related to a storage server does change, we empty out all of these vectors to
	// signify they must be repopulated. We do not repopulate them immediately to avoid a slow task.
	const std::vector<Tag>& tagsForKey(StringRef key) {
		auto& tags = keyInfo[key].tags;
		if (tags.empty()) {
			auto& r = keyInfo.rangeContaining(key).value();
			r.populateTags();
			return r.tags;
		}
		return tags;
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

	void updateSSTagCost(const UID& id, const TagSet& tagSet, MutationRef m, uint64_t cost) {
		auto [it, _] = ssTrTagCommitCost.try_emplace(id, TransactionTagMap<TransactionCommitCostEstimation>());

		for (auto& tag : tagSet) {
			auto& costItem = it->second[tag];
			if (m.isAtomicOp() || m.type == MutationRef::Type::SetValue || m.type == MutationRef::Type::ClearRange) {
				costItem.opsSum++;
				costItem.costSum += cost;
			}
		}
	}

	// Admission is separate from enforcement. Existing durable locks remain
	// active even when an operator disables acquisition or changes log modes.
	bool rangeLockAdmissionEnabled() const {
		return SERVER_KNOBS->ENABLE_READ_LOCK_ON_RANGE && !SERVER_KNOBS->ENABLE_VERSION_VECTOR &&
		       !SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST;
	}

	ProxyCommitData(UID dbgid,
	                MasterInterface master,
	                Version recoveryTransactionVersion,
	                PublicRequestStream<CommitTransactionRequest> commit,
	                Reference<AsyncVar<ServerDBInfo> const> db,
	                bool firstProxy,
	                bool provisional,
	                uint16_t commitProxyIndex,
	                LogEpoch epoch)
	  : dbgid(dbgid), commitBatchesMemBytesCount(0),
	    stats(dbgid, &version, &committedVersion, &commitBatchesMemBytesCount), master(master), logAdapter(nullptr),
	    txnStateStore(nullptr), committedVersion(recoveryTransactionVersion), minKnownCommittedVersion(0), version(0),
	    lastVersionTime(0), commitVersionRequestNumber(1), mostRecentProcessedRequestNumber(0), firstProxy(firstProxy),
	    provisional(provisional), lastCoalesceTime(0), locked(false),
	    commitBatchInterval(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_MIN), localCommitBatchesStarted(0),
	    commit(commit), cx(openDBOnServer(db, TaskPriority::DefaultEndpoint, LockAware::True)), db(db),
	    singleKeyMutationEvent("SingleKeyMutation"_sr), lastTxsPop(0), popRemoteTxs(false), lastStartCommit(0),
	    lastCommitLatency(SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION), lastCommitTime(0), lastMasterReset(now()),
	    lastResolverReset(now()), commitProxyIndex(commitProxyIndex),
	    acsBuilder(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM &&
	                       !SERVER_KNOBS->ENABLE_VERSION_VECTOR && !SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST
	                   ? std::make_shared<AccumulativeChecksumBuilder>(
	                         getCommitProxyAccumulativeChecksumIndex(commitProxyIndex))
	                   : nullptr),
	    lastShardMove(invalidVersion), epoch(epoch) {
		commitComputePerOperation.resize(SERVER_KNOBS->PROXY_COMPUTE_BUCKETS, 0.0);
	}

	// Build the core-owned view consumed by applyMetadataMutations() without
	// exposing the full ProxyCommitData type outside commitproxy.
	ApplyMetadataProxyContext getApplyMetadataProxyContext();
};
class RangeLock final : public ApplyMetadataRangeLock {
public:
	RangeLock() = default;
	~RangeLock() override = default;

	// The raw KRM boundaries must not be coalesced: a subsequent raw clear
	// can remove one of two equal-valued boundaries and expose its predecessor.
	bool anyExclusiveLockHeld() const { return exclusiveBoundaryCount_ != 0; }
	const RangeLockConfiguration& configuration() const { return configuration_; }
	bool enforcementStateValid() const { return !invalidRecoveredState_; }
	RangeLockReadiness effectiveReadiness() const {
		return invalidRecoveredState_ ? RangeLockReadiness::Unknown : configuration_.readiness();
	}
	bool ordinaryWritesAllowed(bool acquisitionKnobEnabled) const {
		return !invalidRecoveredState_ && !configuration_.isMigrating() &&
		       (configuration_.isReady() || !acquisitionKnobEnabled);
	}

	void setBoundary(const KeyRef& key, const ValueRef& value) override {
		RangeLockStateSet state = decodeBoundary(key, value);
		auto previous = boundaries_.find(key);
		if (previous != boundaries_.end() && countsAsExclusive(previous->first, previous->second)) {
			--exclusiveBoundaryCount_;
		}
		exclusiveBoundaryCount_ += countsAsExclusive(key, state);
		boundaries_.insert_or_assign(Key(key), std::move(state));
	}

	void recoverBoundary(const KeyRef& key, const ValueRef& value) override {
		try {
			setBoundary(key, value);
		} catch (Error& e) {
			markInvalidRecoveredState(e);
		}
	}

	void finishRecovery() {
		if (configuration_.isReady()) {
			try {
				validateBoundaryMap(boundaries_);
			} catch (Error& e) {
				markInvalidRecoveredState(e);
			}
		}
	}

	// range is in the system-key KRM namespace; its end can be the prefix
	// successor, which does not itself start with rangeLockPrefix.
	void clearBoundaries(const KeyRangeRef& range) override {
		exclusiveBoundaryCount_ -= clearBoundaryMap(boundaries_, range);
	}

	void resetBoundaries() override {
		boundaries_.clear();
		exclusiveBoundaryCount_ = 0;
		invalidRecoveredState_ = false;
	}

	void setConfiguration(const RangeLockConfiguration& configuration) override { configuration_ = configuration; }

	static void validateBoundary(const KeyRef& key, const ValueRef& value) { decodeBoundary(key, value); }
	void validateCompleteMap() const {
		if (invalidRecoveredState_) {
			throw range_lock_not_ready();
		}
		validateBoundaryMap(boundaries_);
	}

	bool isLocked(const KeyRangeRef& range) const {
		const KeyRangeRef normalRange = range & normalKeys;
		if (normalRange.empty()) {
			return false;
		}
		auto it = boundaries_.upper_bound(normalRange.begin);
		if (it != boundaries_.begin()) {
			--it;
		}
		for (; it != boundaries_.end() && it->first < normalRange.end; ++it) {
			if (it->second.isLockedFor(RangeLockType::ExclusiveReadLock)) {
				return true;
			}
		}
		return false;
	}

	// Validate actual metadata mutations and classify their semantic effect.
	// A release's end boundary can preserve a neighboring lock; that is not a
	// new acquisition. A changed acquisition ID is a new acquisition.
	bool wouldAddLocks(const VectorRef<MutationRef>& mutations) const {
		if (invalidRecoveredState_) {
			throw range_lock_not_ready();
		}
		BoundaryMap after = boundaries_;
		for (const auto& mutation : mutations) {
			if (mutation.type == MutationRef::ClearRange) {
				clearBoundaryMap(after, KeyRangeRef(mutation.param1, mutation.param2));
			} else if (rangeLockKeys.contains(mutation.param1)) {
				if (mutation.type != MutationRef::SetValue) {
					throw range_lock_not_ready();
				}
				const KeyRef key = mutation.param1.removePrefix(rangeLockPrefix);
				after.insert_or_assign(Key(key), decodeBoundary(key, mutation.param2));
			}
		}
		validateBoundaryMap(after);
		return containsNewLocks(boundaries_, after);
	}

private:
	using BoundaryMap = std::map<Key, RangeLockStateSet, std::less<>>;

	void markInvalidRecoveredState(const Error& error) {
		if (error.code() == error_code_actor_cancelled) {
			throw error;
		}
		// Preserve the durable bytes for an owner-checked reconciliation.
		// Crashing recruitment here would make that repair impossible.
		invalidRecoveredState_ = true;
		TraceEvent(SevWarnAlways, "RangeLockInvalidRecoveredState").suppressFor(5.0).detail("ErrorCode", error.code());
	}

	static bool countsAsExclusive(const KeyRef& key, const RangeLockStateSet& state) {
		return key < normalKeys.end && state.isLockedFor(RangeLockType::ExclusiveReadLock);
	}

	static RangeLockStateSet decodeBoundary(const KeyRef& key, const ValueRef& value) {
		if (key > normalKeys.end) {
			throw range_lock_not_ready();
		}
		RangeLockStateSet state = decodeRangeLockStateSetSafe(value);
		if (!state.isValid() || state.getLocks().size() > 1) {
			// ExclusiveReadLock is currently the only supported lock type.
			// Raw metadata must preserve the same one-acquisition-per-interval
			// invariant as RangeLockStateSet::insertIfNotExist.
			throw range_lock_not_ready();
		}
		for (const auto& [name, lock] : state.getLocks()) {
			if (lock.getRange().empty() || !normalKeys.contains(lock.getRange()) || !lock.getRange().contains(key) ||
			    !lock.isLockedFor(RangeLockType::ExclusiveReadLock)) {
				throw range_lock_not_ready();
			}
		}
		return state;
	}

	static size_t clearBoundaryMap(BoundaryMap& boundaries, const KeyRangeRef& range) {
		const KeyRangeRef intersection = range & rangeLockKeys;
		if (intersection.empty()) {
			return 0;
		}
		auto begin = boundaries.lower_bound(intersection.begin.removePrefix(rangeLockPrefix));
		auto end = intersection.end.startsWith(rangeLockPrefix)
		               ? boundaries.lower_bound(intersection.end.removePrefix(rangeLockPrefix))
		               : boundaries.end();
		size_t removedExclusive = 0;
		for (auto it = begin; it != end; ++it) {
			removedExclusive += countsAsExclusive(it->first, it->second);
		}
		boundaries.erase(begin, end);
		return removedExclusive;
	}

	static void validateBoundaryMap(const BoundaryMap& boundaries) {
		for (auto it = boundaries.begin(); it != boundaries.end() && it->first < normalKeys.end; ++it) {
			const auto next = std::next(it);
			const KeyRef end = next == boundaries.end() ? normalKeys.end : KeyRef(next->first);
			const KeyRangeRef interval(it->first, end);
			for (const auto& [name, lock] : it->second.getLocks()) {
				if (!lock.getRange().contains(interval)) {
					throw range_lock_not_ready();
				}
			}
		}
	}

	static bool containsNewLocks(const BoundaryMap& before, const BoundaryMap& after) {
		const RangeLockStateSet empty;
		const RangeLockStateSet* beforeState = &empty;
		const RangeLockStateSet* afterState = &empty;
		auto beforeIt = before.begin();
		auto afterIt = after.begin();
		KeyRef cursor = normalKeys.begin;
		while (cursor < normalKeys.end) {
			while (beforeIt != before.end() && beforeIt->first <= cursor) {
				beforeState = &beforeIt++->second;
			}
			while (afterIt != after.end() && afterIt->first <= cursor) {
				afterState = &afterIt++->second;
			}
			for (const auto& [name, lock] : afterState->getLocks()) {
				if (!beforeState->containsExactLock(lock)) {
					return true;
				}
			}
			cursor = normalKeys.end;
			if (beforeIt != before.end()) {
				cursor = std::min(cursor, KeyRef(beforeIt->first));
			}
			if (afterIt != after.end()) {
				cursor = std::min(cursor, KeyRef(afterIt->first));
			}
		}
		return false;
	}

	BoundaryMap boundaries_;
	RangeLockConfiguration configuration_;
	size_t exclusiveBoundaryCount_ = 0;
	bool invalidRecoveredState_ = false;
};

inline ApplyMetadataProxyContext ProxyCommitData::getApplyMetadataProxyContext() {
	return { .dbgid = dbgid,
		     .txnStateStore = txnStateStore,
		     .vecBackupKeys = &vecBackupKeys,
		     .cdcRouting = &cdcRouting,
		     .keyInfo = &keyInfo,
		     .uid_applyMutationsData = firstProxy ? &uid_applyMutationsData : nullptr,
		     .commit = commit,
		     .cx = cx,
		     .committedVersion = &committedVersion,
		     .storageCache = &storageCache,
		     .tag_popped = &tag_popped,
		     .tssMapping = &tssMapping,
		     .commitProxyIndex = commitProxyIndex,
		     .acsBuilder = acsBuilder,
		     .epoch = epoch,
		     // applyMetadataMutations() only borrows the interface during the
		     // synchronous call, so there is no need to propagate shared
		     // ownership through the context object.
		     .rangeLock = rangeLock.get() };
}
