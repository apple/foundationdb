/*
 * MasterProxyServer.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/Atomic.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/MasterProxyInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Notified.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/sim_validation.h"
#include "fdbserver/ApplyMetadataMutation.h"
#include "fdbserver/ConflictSet.h"
#include "fdbserver/DataDistributorInterface.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LatencyBandConfig.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/LogSystemDiskQueueAdapter.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/ActorCollection.h"
#include "flow/Knobs.h"
#include "flow/Stats.h"
#include "flow/TDMetric.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

ACTOR Future<Void> broadcastTxnRequest(TxnStateRequest req, int sendAmount, bool sendReply) {
	state ReplyPromise<Void> reply = req.reply;
	resetReply( req );
	std::vector<Future<Void>> replies;
	int currentStream = 0;
	std::vector<Endpoint> broadcastEndpoints = req.broadcastInfo;
	for(int i = 0; i < sendAmount && currentStream < broadcastEndpoints.size(); i++) {
		std::vector<Endpoint> endpoints;
		RequestStream<TxnStateRequest> cur(broadcastEndpoints[currentStream++]);
		while(currentStream < broadcastEndpoints.size()*(i+1)/sendAmount) {
			endpoints.push_back(broadcastEndpoints[currentStream++]);
		}
		req.broadcastInfo = endpoints;
		replies.push_back(brokenPromiseToNever( cur.getReply( req ) ));
		resetReply( req );
	}
	wait( waitForAll(replies) );
	if(sendReply) {
		reply.send(Void());
	}
	return Void();
}

struct ProxyStats {
	CounterCollection cc;
	Counter txnRequestIn, txnRequestOut, txnRequestErrors;
	Counter txnStartIn, txnStartOut, txnStartBatch;
	Counter txnSystemPriorityStartIn, txnSystemPriorityStartOut;
	Counter txnBatchPriorityStartIn, txnBatchPriorityStartOut;
	Counter txnDefaultPriorityStartIn, txnDefaultPriorityStartOut;
	Counter txnCommitIn, txnCommitVersionAssigned, txnCommitResolving, txnCommitResolved, txnCommitOut, txnCommitOutSuccess, txnCommitErrors;
	Counter txnConflicts;
	Counter txnThrottled;
	Counter commitBatchIn, commitBatchOut;
	Counter mutationBytes;
	Counter mutations;
	Counter conflictRanges;
	Counter keyServerLocationIn, keyServerLocationOut, keyServerLocationErrors;
	Version lastCommitVersionAssigned;

	LatencyBands commitLatencyBands;
	LatencyBands grvLatencyBands;

	Future<Void> logger;

	int recentRequests;
	Deque<int> requestBuckets;
	double lastBucketBegin;
	double bucketInterval;

	void updateRequestBuckets() {
		while(now() - lastBucketBegin > bucketInterval) {
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
		return recentRequests*FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE/(FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE-(lastBucketBegin+bucketInterval-now()));
	}

	explicit ProxyStats(UID id, Version* pVersion, NotifiedVersion* pCommittedVersion, int64_t *commitBatchesMemBytesCountPtr)
	  : cc("ProxyStats", id.toString()), recentRequests(0), lastBucketBegin(now()), bucketInterval(FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE/FLOW_KNOBS->BASIC_LOAD_BALANCE_BUCKETS),
	    txnRequestIn("TxnRequestIn", cc), txnRequestOut("TxnRequestOut", cc),
	    txnRequestErrors("TxnRequestErrors", cc), txnStartIn("TxnStartIn", cc), txnStartOut("TxnStartOut", cc),
		txnStartBatch("TxnStartBatch", cc), txnSystemPriorityStartIn("TxnSystemPriorityStartIn", cc),
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
		commitLatencyBands("CommitLatencyMetrics", id, SERVER_KNOBS->STORAGE_LOGGING_DELAY),
		grvLatencyBands("GRVLatencyMetrics", id, SERVER_KNOBS->STORAGE_LOGGING_DELAY) {
		specialCounter(cc, "LastAssignedCommitVersion", [this](){return this->lastCommitVersionAssigned;});
		specialCounter(cc, "Version", [pVersion](){return *pVersion; });
		specialCounter(cc, "CommittedVersion", [pCommittedVersion](){ return pCommittedVersion->get(); });
		specialCounter(cc, "CommitBatchesMemBytesCount", [commitBatchesMemBytesCountPtr]() { return *commitBatchesMemBytesCountPtr; });
		logger = traceCounters("ProxyMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "ProxyMetrics");
		for(int i = 0; i < FLOW_KNOBS->BASIC_LOAD_BALANCE_BUCKETS; i++) {
			requestBuckets.push_back(0);
		}
	}
};

struct TransactionRateInfo {
	double rate;
	double limit;
	double budget;

	bool disabled;

	Smoother smoothRate;
	Smoother smoothReleased;

	TransactionRateInfo(double rate) : rate(rate), limit(0), budget(0), disabled(true), smoothRate(SERVER_KNOBS->START_TRANSACTION_RATE_WINDOW), 
	                                   smoothReleased(SERVER_KNOBS->START_TRANSACTION_RATE_WINDOW) {}

	void reset() {
		// Determine the number of transactions that this proxy is allowed to release
		// Roughly speaking, this is done by computing the number of transactions over some historical window that we could
		// have started but didn't, and making that our limit. More precisely, we track a smoothed rate limit and release rate,
		// the difference of which is the rate of additional transactions that we could have released based on that window.
		// Then we multiply by the window size to get a number of transactions.
		// 
		// Limit can be negative in the event that we are releasing more transactions than we are allowed (due to the use of
		// our budget or because of higher priority transactions).
		double releaseRate = smoothRate.smoothTotal() - smoothReleased.smoothRate();
		limit = SERVER_KNOBS->START_TRANSACTION_RATE_WINDOW * releaseRate;
	}

	bool canStart(int64_t numAlreadyStarted, int64_t count) {
		return numAlreadyStarted + count <= std::min(limit + budget, SERVER_KNOBS->START_TRANSACTION_MAX_TRANSACTIONS_TO_START);
	}

	void updateBudget(int64_t numStartedAtPriority, bool queueEmptyAtPriority, double elapsed) {
		// Update the budget to accumulate any extra capacity available or remove any excess that was used.
		// The actual delta is the portion of the limit we didn't use multiplied by the fraction of the window that elapsed.
		// 
		// We may have exceeded our limit due to the budget or because of higher priority transactions, in which case this 
		// delta will be negative. The delta can also be negative in the event that our limit was negative, which can happen 
		// if we had already started more transactions in our window than our rate would have allowed.
		//
		// This budget has the property that when the budget is required to start transactions (because batches are big),
		// the sum limit+budget will increase linearly from 0 to the batch size over time and decrease by the batch size
		// upon starting a batch. In other words, this works equivalently to a model where we linearly accumulate budget over 
		// time in the case that our batches are too big to take advantage of the window based limits.
		budget = std::max(0.0, budget + elapsed * (limit - numStartedAtPriority) / SERVER_KNOBS->START_TRANSACTION_RATE_WINDOW);

		// If we are emptying out the queue of requests, then we don't need to carry much budget forward
		// If we did keep accumulating budget, then our responsiveness to changes in workflow could be compromised
		if(queueEmptyAtPriority) {
			budget = std::min(budget, SERVER_KNOBS->START_TRANSACTION_MAX_EMPTY_QUEUE_BUDGET);
		}

		smoothReleased.addDelta(numStartedAtPriority);
	}

	void disable() {
		disabled = true;
		rate = 0;
		smoothRate.reset(0);
	}

	void setRate(double rate) {
		ASSERT(rate >= 0 && rate != std::numeric_limits<double>::infinity() && !std::isnan(rate));

		this->rate = rate;
		if(disabled) {
			smoothRate.reset(rate);
			disabled = false;
		}
		else {
			smoothRate.setTotal(rate);
		}
	}
};


ACTOR Future<Void> getRate(UID myID, Reference<AsyncVar<ServerDBInfo>> db, int64_t* inTransactionCount, int64_t* inBatchTransactionCount, TransactionRateInfo *transactionRateInfo,
						   TransactionRateInfo *batchTransactionRateInfo, GetHealthMetricsReply* healthMetricsReply, GetHealthMetricsReply* detailedHealthMetricsReply,
						   TransactionTagMap<uint64_t>* transactionTagCounter, PrioritizedTransactionTagMap<ClientTagThrottleLimits>* throttledTags) {
	state Future<Void> nextRequestTimer = Never();
	state Future<Void> leaseTimeout = Never();
	state Future<GetRateInfoReply> reply = Never();
	state double lastDetailedReply = 0.0; // request detailed metrics immediately
	state bool expectingDetailedReply = false;
	state int64_t lastTC = 0;

	if (db->get().ratekeeper.present()) nextRequestTimer = Void();
	loop choose {
		when ( wait( db->onChange() ) ) {
			if ( db->get().ratekeeper.present() ) {
				TraceEvent("ProxyRatekeeperChanged", myID)
				.detail("RKID", db->get().ratekeeper.get().id());
				nextRequestTimer = Void();  // trigger GetRate request
			} else {
				TraceEvent("ProxyRatekeeperDied", myID);
				nextRequestTimer = Never();
				reply = Never();
			}
		}
		when ( wait( nextRequestTimer ) ) {
			nextRequestTimer = Never();
			bool detailed = now() - lastDetailedReply > SERVER_KNOBS->DETAILED_METRIC_UPDATE_RATE;
			reply = brokenPromiseToNever(db->get().ratekeeper.get().getRateInfo.getReply(GetRateInfoRequest(myID, *inTransactionCount, *inBatchTransactionCount, *transactionTagCounter, detailed)));
			transactionTagCounter->clear();
			expectingDetailedReply = detailed;
		}
		when ( GetRateInfoReply rep = wait(reply) ) {
			reply = Never();

			transactionRateInfo->setRate(rep.transactionRate);
			batchTransactionRateInfo->setRate(rep.batchTransactionRate);
			//TraceEvent("MasterProxyRate", myID).detail("Rate", rep.transactionRate).detail("BatchRate", rep.batchTransactionRate).detail("Lease", rep.leaseDuration).detail("ReleasedTransactions", *inTransactionCount - lastTC);
			lastTC = *inTransactionCount;
			leaseTimeout = delay(rep.leaseDuration);
			nextRequestTimer = delayJittered(rep.leaseDuration / 2);
			healthMetricsReply->update(rep.healthMetrics, expectingDetailedReply, true);
			if (expectingDetailedReply) {
				detailedHealthMetricsReply->update(rep.healthMetrics, true, true);
				lastDetailedReply = now();
			}

			// Replace our throttles with what was sent by ratekeeper. Because we do this,
			// we are not required to expire tags out of the map
			if(rep.throttledTags.present()) {
				*throttledTags = std::move(rep.throttledTags.get());
			}
		}
		when ( wait( leaseTimeout ) ) {
			transactionRateInfo->disable();
			batchTransactionRateInfo->disable();
			TraceEvent(SevWarn, "MasterProxyRateLeaseExpired", myID).suppressFor(5.0);
			//TraceEvent("MasterProxyRate", myID).detail("Rate", 0.0).detail("BatchRate", 0.0).detail("Lease", 0);
			leaseTimeout = Never();
		}
	}
}

ACTOR Future<Void> queueTransactionStartRequests(
	Reference<AsyncVar<ServerDBInfo>> db,
	Deque<GetReadVersionRequest> *systemQueue,
	Deque<GetReadVersionRequest> *defaultQueue,
	Deque<GetReadVersionRequest> *batchQueue,
	FutureStream<GetReadVersionRequest> readVersionRequests,
	PromiseStream<Void> GRVTimer, double *lastGRVTime,
	double *GRVBatchTime, FutureStream<double> replyTimes,
	ProxyStats* stats, TransactionRateInfo* batchRateInfo,
	TransactionTagMap<uint64_t>* transactionTagCounter) 
{
	loop choose{
		when(GetReadVersionRequest req = waitNext(readVersionRequests)) {
			//WARNING: this code is run at a high priority, so it needs to do as little work as possible
			stats->addRequest();
			if( stats->txnRequestIn.getValue() - stats->txnRequestOut.getValue() > SERVER_KNOBS->START_TRANSACTION_MAX_QUEUE_SIZE ) {
				++stats->txnRequestErrors;
				//FIXME: send an error instead of giving an unreadable version when the client can support the error: req.reply.sendError(proxy_memory_limit_exceeded());
				GetReadVersionReply rep;
				rep.version = 1;
				rep.locked = true;
				req.reply.send(rep);
				TraceEvent(SevWarnAlways, "ProxyGRVThresholdExceeded").suppressFor(60);
			} else {
				// TODO: check whether this is reasonable to do in the fast path
				for(auto tag : req.tags) {
					(*transactionTagCounter)[tag.first] += tag.second;
				}

				if (req.debugID.present())
					g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "MasterProxyServer.queueTransactionStartRequests.Before");

				if (systemQueue->empty() && defaultQueue->empty() && batchQueue->empty()) {
					forwardPromise(GRVTimer, delayJittered(std::max(0.0, *GRVBatchTime - (now() - *lastGRVTime)), TaskPriority::ProxyGRVTimer));
				}

				++stats->txnRequestIn;
				stats->txnStartIn += req.transactionCount;
				if (req.priority >= TransactionPriority::IMMEDIATE) {
					stats->txnSystemPriorityStartIn += req.transactionCount;
					systemQueue->push_back(req);
				} else if (req.priority >= TransactionPriority::DEFAULT) {
					stats->txnDefaultPriorityStartIn += req.transactionCount;
					defaultQueue->push_back(req);
				} else {
					// Return error for batch_priority GRV requests
					int64_t proxiesCount = std::max((int)db->get().client.proxies.size(), 1);
					if (batchRateInfo->rate <= (1.0 / proxiesCount)) {
						req.reply.sendError(batch_transaction_throttled());
						stats->txnThrottled += req.transactionCount;
						continue;
					}

					stats->txnBatchPriorityStartIn += req.transactionCount;
					batchQueue->push_back(req);
				}
			}
		}
		// dynamic batching monitors reply latencies
		when(double reply_latency = waitNext(replyTimes)) {
			double target_latency = reply_latency * SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION;
			*GRVBatchTime = std::max(
			    SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MIN,
			    std::min(SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MAX,
			             target_latency * SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA +
			                 *GRVBatchTime * (1 - SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA)));
		}
	}
}

ACTOR void discardCommit(UID id, Future<LogSystemDiskQueueAdapter::CommitMessage> fcm, Future<Void> dummyCommitState) {
	ASSERT(!dummyCommitState.isReady());
	LogSystemDiskQueueAdapter::CommitMessage cm = wait(fcm);
	TraceEvent("Discarding", id).detail("Count", cm.messages.size());
	cm.acknowledge.send(Void());
	ASSERT(dummyCommitState.isReady());
}

DESCR struct SingleKeyMutation {
	Standalone<StringRef> shardBegin;
	Standalone<StringRef> shardEnd;
	int64_t tag1;
	int64_t tag2;
	int64_t tag3;
};

struct ProxyCommitData {
	UID dbgid;
	int64_t commitBatchesMemBytesCount;
	ProxyStats stats;
	MasterInterface master;
	vector<ResolverInterface> resolvers;
	LogSystemDiskQueueAdapter* logAdapter;
	Reference<ILogSystem> logSystem;
	IKeyValueStore* txnStateStore;
	NotifiedVersion committedVersion; // Provided that this recovery has succeeded or will succeed, this version is fully committed (durable)
	Version minKnownCommittedVersion; // No version smaller than this one will be used as the known committed version
	                                  // during recovery
	Version version;  // The version at which txnStateStore is up to date
	Promise<Void> validState; // Set once txnStateStore and version are valid //TODO: Set txnLifetime before it is set
	double lastVersionTime;
	KeyRangeMap<std::set<Key>> vecBackupKeys;
	uint64_t commitVersionRequestNumber;
	uint64_t mostRecentProcessedRequestNumber;
	KeyRangeMap<Deque<std::pair<Version,int>>> keyResolvers;
	KeyRangeMap<ServerCacheInfo> keyInfo;
	KeyRangeMap<bool> cacheInfo;
	std::map<Key, applyMutationsData> uid_applyMutationsData;
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

	Version readTxnLifetime;

	//The tag related to a storage server rarely change, so we keep a vector of tags for each key range to be slightly more CPU efficient.
	//When a tag related to a storage server does change, we empty out all of these vectors to signify they must be repopulated.
	//We do not repopulate them immediately to avoid a slow task.
	const vector<Tag>& tagsForKey(StringRef key) {
		auto& tags = keyInfo[key].tags;
		if(!tags.size()) {
			auto& r = keyInfo.rangeContaining(key).value();
			for(auto info : r.src_info) {
				r.tags.push_back(info->tag);
			}
			for(auto info : r.dest_info) {
				r.tags.push_back(info->tag);
			}
			uniquify(r.tags);
			return r.tags;
		}
		return tags;
	}

	const bool needsCacheTag(KeyRangeRef range) {
		auto ranges = cacheInfo.intersectingRanges(range);
		for(auto r : ranges) {
			if(r.value()) {
				return true;
			}
		}
		return false;
	}
	
	void updateLatencyBandConfig(Optional<LatencyBandConfig> newLatencyBandConfig) {
		if(newLatencyBandConfig.present() != latencyBandConfig.present()
			|| (newLatencyBandConfig.present() && newLatencyBandConfig.get().grvConfig != latencyBandConfig.get().grvConfig))
		{
			TraceEvent("LatencyBandGrvUpdatingConfig").detail("Present", newLatencyBandConfig.present());
			stats.grvLatencyBands.clearBands();
			if(newLatencyBandConfig.present()) {
				for(auto band : newLatencyBandConfig.get().grvConfig.bands) {
					stats.grvLatencyBands.addThreshold(band);
				}
			}
		}

		if(newLatencyBandConfig.present() != latencyBandConfig.present()
			|| (newLatencyBandConfig.present() && newLatencyBandConfig.get().commitConfig != latencyBandConfig.get().commitConfig))
		{
			TraceEvent("LatencyBandCommitUpdatingConfig").detail("Present", newLatencyBandConfig.present());
			stats.commitLatencyBands.clearBands();
			if(newLatencyBandConfig.present()) {
				for(auto band : newLatencyBandConfig.get().commitConfig.bands) {
					stats.commitLatencyBands.addThreshold(band);
				}
			}
		}

		latencyBandConfig = newLatencyBandConfig;
	}

	ProxyCommitData(UID dbgid, MasterInterface master, RequestStream<GetReadVersionRequest> getConsistentReadVersion,
	                Version recoveryTransactionVersion, RequestStream<CommitTransactionRequest> commit,
	                Reference<AsyncVar<ServerDBInfo>> db, bool firstProxy, Version readTxnLifetime)
	  : dbgid(dbgid), stats(dbgid, &version, &committedVersion, &commitBatchesMemBytesCount), master(master),
	    logAdapter(NULL), txnStateStore(NULL), popRemoteTxs(false), committedVersion(recoveryTransactionVersion),
	    version(0), minKnownCommittedVersion(0), lastVersionTime(0), commitVersionRequestNumber(1),
	    mostRecentProcessedRequestNumber(0), getConsistentReadVersion(getConsistentReadVersion), commit(commit),
	    lastCoalesceTime(0), localCommitBatchesStarted(0), locked(false),
	    commitBatchInterval(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_MIN), firstProxy(firstProxy),
	    readTxnLifetime(readTxnLifetime), cx(openDBOnServer(db, TaskPriority::DefaultEndpoint, true, true)), db(db),
	    singleKeyMutationEvent(LiteralStringRef("SingleKeyMutation")), commitBatchesMemBytesCount(0), lastTxsPop(0),
	    lastStartCommit(0), lastCommitLatency(SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION), lastCommitTime(0) {
		commitComputePerOperation.resize(SERVER_KNOBS->PROXY_COMPUTE_BUCKETS,0.0);
		ASSERT_WE_THINK(readTxnLifetime > 0);
	}
};

struct ResolutionRequestBuilder {
	ProxyCommitData* self;
	vector<ResolveTransactionBatchRequest> requests;
	vector<vector<int>> transactionResolverMap;
	vector<CommitTransactionRef*> outTr;
	std::vector<std::vector<std::vector<int>>>
	    txReadConflictRangeIndexMap; // Used to report conflicting keys, the format is
	                                 // [CommitTransactionRef_Index][Resolver_Index][Read_Conflict_Range_Index_on_Resolver]
	                                 // -> read_conflict_range's original index in the commitTransactionRef

	ResolutionRequestBuilder( ProxyCommitData* self, Version version, Version prevVersion, Version lastReceivedVersion) : self(self), requests(self->resolvers.size()) {
		for(auto& req : requests) {
			req.prevVersion = prevVersion;
			req.version = version;
			req.lastReceivedVersion = lastReceivedVersion;
		}
	}

	CommitTransactionRef& getOutTransaction(int resolver, Version read_snapshot) {
		CommitTransactionRef *& out = outTr[resolver];
		if (!out) {
			ResolveTransactionBatchRequest& request = requests[resolver];
			request.transactions.resize(request.arena, request.transactions.size() + 1);
			out = &request.transactions.back();
			out->read_snapshot = read_snapshot;
		}
		return *out;
	}

	void addTransaction(CommitTransactionRef& trIn, int transactionNumberInBatch) {
		// SOMEDAY: There are a couple of unnecessary O( # resolvers ) steps here
		outTr.assign(requests.size(), NULL);
		ASSERT( transactionNumberInBatch >= 0 && transactionNumberInBatch < 32768 );

		bool isTXNStateTransaction = false;
		for (auto & m : trIn.mutations) {
			if (m.type == MutationRef::SetVersionstampedKey) {
				transformVersionstampMutation( m, &MutationRef::param1, requests[0].version, transactionNumberInBatch );
				trIn.write_conflict_ranges.push_back( requests[0].arena, singleKeyRange( m.param1, requests[0].arena ) );
			} else if (m.type == MutationRef::SetVersionstampedValue) {
				transformVersionstampMutation( m, &MutationRef::param2, requests[0].version, transactionNumberInBatch );
			}
			if (isMetadataMutation(m)) {
				isTXNStateTransaction = true;
				getOutTransaction(0, trIn.read_snapshot).mutations.push_back(requests[0].arena, m);
			}
		}
		std::vector<std::vector<int>> rCRIndexMap(
		    requests.size()); // [resolver_index][read_conflict_range_index_on_the_resolver]
		                      // -> read_conflict_range's original index
		for (int idx = 0; idx < trIn.read_conflict_ranges.size(); ++idx) {
			const auto& r = trIn.read_conflict_ranges[idx];
			auto ranges = self->keyResolvers.intersectingRanges( r );
			std::set<int> resolvers;
			for(auto &ir : ranges) {
				auto& version_resolver = ir.value();
				for(int i = version_resolver.size()-1; i >= 0; i--) {
					resolvers.insert(version_resolver[i].second);
					if( version_resolver[i].first < trIn.read_snapshot )
						break;
				}
			}
			ASSERT(resolvers.size());
			for (int resolver : resolvers) {
				getOutTransaction( resolver, trIn.read_snapshot ).read_conflict_ranges.push_back( requests[resolver].arena, r );
				rCRIndexMap[resolver].push_back(idx);
			}
		}
		txReadConflictRangeIndexMap.push_back(std::move(rCRIndexMap));
		for(auto& r : trIn.write_conflict_ranges) {
			auto ranges = self->keyResolvers.intersectingRanges( r );
			std::set<int> resolvers;
			for(auto &ir : ranges)
				resolvers.insert(ir.value().back().second);
			ASSERT(resolvers.size());
			for(int resolver : resolvers)
				getOutTransaction( resolver, trIn.read_snapshot ).write_conflict_ranges.push_back( requests[resolver].arena, r );
		}
		if (isTXNStateTransaction)
			for (int r = 0; r<requests.size(); r++) {
				int transactionNumberInRequest = &getOutTransaction(r, trIn.read_snapshot) - requests[r].transactions.begin();
				requests[r].txnStateTransactions.push_back(requests[r].arena, transactionNumberInRequest);
			}

		vector<int> resolversUsed;
		for (int r = 0; r<outTr.size(); r++)
			if (outTr[r]) {
				resolversUsed.push_back(r);
				outTr[r]->report_conflicting_keys = trIn.report_conflicting_keys;
			}
		transactionResolverMap.push_back(std::move(resolversUsed));
	}
};

ACTOR Future<Void> commitBatcher(ProxyCommitData *commitData, PromiseStream<std::pair<std::vector<CommitTransactionRequest>, int> > out, FutureStream<CommitTransactionRequest> in, int desiredBytes, int64_t memBytesLimit) {
	wait(delayJittered(commitData->commitBatchInterval, TaskPriority::ProxyCommitBatcher));

	state double lastBatch = 0;

	loop{
		state Future<Void> timeout;
		state std::vector<CommitTransactionRequest> batch;
		state int batchBytes = 0;

		if(SERVER_KNOBS->MAX_COMMIT_BATCH_INTERVAL <= 0) {
			timeout = Never();
		}
		else {
			timeout = delayJittered(SERVER_KNOBS->MAX_COMMIT_BATCH_INTERVAL, TaskPriority::ProxyCommitBatcher);
		}

		while(!timeout.isReady() && !(batch.size() == SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_COUNT_MAX || batchBytes >= desiredBytes)) {
			choose{
				when(CommitTransactionRequest req = waitNext(in)) {
					//WARNING: this code is run at a high priority, so it needs to do as little work as possible
					commitData->stats.addRequest();
					int bytes = getBytes(req);

					// Drop requests if memory is under severe pressure
					if(commitData->commitBatchesMemBytesCount + bytes > memBytesLimit) {
						++commitData->stats.txnCommitErrors;
						req.reply.sendError(proxy_memory_limit_exceeded());
						TraceEvent(SevWarnAlways, "ProxyCommitBatchMemoryThresholdExceeded").suppressFor(60).detail("MemBytesCount", commitData->commitBatchesMemBytesCount).detail("MemLimit", memBytesLimit);
						continue;
					}

					if (bytes > FLOW_KNOBS->PACKET_WARNING) {
						TraceEvent(!g_network->isSimulated() ? SevWarnAlways : SevWarn, "LargeTransaction")
						    .suppressFor(1.0)
						    .detail("Size", bytes)
						    .detail("Client", req.reply.getEndpoint().getPrimaryAddress());
					}
					++commitData->stats.txnCommitIn;

					if(req.debugID.present()) {
						g_traceBatch.addEvent("CommitDebug", req.debugID.get().first(), "MasterProxyServer.batcher");
					}

					if(!batch.size()) {
						if(now() - lastBatch > commitData->commitBatchInterval) {
							timeout = delayJittered(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_FROM_IDLE, TaskPriority::ProxyCommitBatcher);
						}
						else {
							timeout = delayJittered(commitData->commitBatchInterval - (now() - lastBatch), TaskPriority::ProxyCommitBatcher);
						}
					}

					if((batchBytes + bytes > CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT || req.firstInBatch()) && batch.size()) {
						out.send({ std::move(batch), batchBytes });
						lastBatch = now();
						timeout = delayJittered(commitData->commitBatchInterval, TaskPriority::ProxyCommitBatcher);
						batch = std::vector<CommitTransactionRequest>();
						batchBytes = 0;
					}

					batch.push_back(req);
					batchBytes += bytes;
					commitData->commitBatchesMemBytesCount += bytes;
				}
				when(wait(timeout)) {}
			}
		}
		out.send({ std::move(batch), batchBytes });
		lastBatch = now();
	}
}

void createWhitelistBinPathVec(const std::string& binPath, vector<Standalone<StringRef>>& binPathVec) {
	TraceEvent(SevDebug, "BinPathConverter").detail("Input", binPath);
	StringRef input(binPath);
	while (input != StringRef()) {
		StringRef token = input.eat(LiteralStringRef(","));
		if (token != StringRef()) {
			const uint8_t* ptr = token.begin();
			while (ptr != token.end() && *ptr == ' ') {
				ptr++;
			}
			if (ptr != token.end()) {
				Standalone<StringRef> newElement(token.substr(ptr - token.begin()));
				TraceEvent(SevDebug, "BinPathItem").detail("Element", newElement);
				binPathVec.push_back(newElement);
			}
		}
	}
	return;
}

bool isWhitelisted(const vector<Standalone<StringRef>>& binPathVec, StringRef binPath) {
	TraceEvent("BinPath").detail("Value", binPath);
	for (const auto& item : binPathVec) {
		TraceEvent("Element").detail("Value", item);
	}
	return std::find(binPathVec.begin(), binPathVec.end(), binPath) != binPathVec.end();
}

ACTOR Future<Void> addBackupMutations(ProxyCommitData* self, std::map<Key, MutationListRef>* logRangeMutations,
                                      LogPushData* toCommit, Version commitVersion, double* computeDuration, double* computeStart) {
	state std::map<Key, MutationListRef>::iterator logRangeMutation = logRangeMutations->begin();
	state int32_t version = commitVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
	state int yieldBytes = 0;
	state BinaryWriter valueWriter(Unversioned());

	// Serialize the log range mutations within the map
	for (; logRangeMutation != logRangeMutations->end(); ++logRangeMutation)
	{
		//FIXME: this is re-implementing the serialize function of MutationListRef in order to have a yield
		valueWriter = BinaryWriter(IncludeVersion(ProtocolVersion::withBackupMutations()));
		valueWriter << logRangeMutation->second.totalSize();

		state MutationListRef::Blob* blobIter = logRangeMutation->second.blob_begin;
		while(blobIter) {
			if(yieldBytes > SERVER_KNOBS->DESIRED_TOTAL_BYTES) {
				yieldBytes = 0;
				if(g_network->check_yield(TaskPriority::ProxyCommitYield1)) {
					*computeDuration += g_network->timer() - *computeStart;
					wait(delay(0, TaskPriority::ProxyCommitYield1));
					*computeStart = g_network->timer();
				}
			}
			valueWriter.serializeBytes(blobIter->data);
			yieldBytes += blobIter->data.size();
			blobIter = blobIter->next;
		}

		Key val = valueWriter.toValue();
		
		BinaryWriter wr(Unversioned());

		// Serialize the log destination
		wr.serializeBytes( logRangeMutation->first );

		// Write the log keys and version information
		wr << (uint8_t)hashlittle(&version, sizeof(version), 0);
		wr << bigEndian64(commitVersion);

		MutationRef backupMutation;
		backupMutation.type = MutationRef::SetValue;
		uint32_t* partBuffer = NULL;

		for (int part = 0; part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE < val.size(); part++) {

			// Assign the second parameter as the part
			backupMutation.param2 = val.substr(part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE,
				std::min(val.size() - part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE, CLIENT_KNOBS->MUTATION_BLOCK_SIZE));

			// Write the last part of the mutation to the serialization, if the buffer is not defined
			if (!partBuffer) {
				// Serialize the part to the writer
				wr << bigEndian32(part);

				// Define the last buffer part
				partBuffer = (uint32_t*) ((char*) wr.getData() + wr.getLength() - sizeof(uint32_t));
			}
			else {
				*partBuffer = bigEndian32(part);
			}

			// Define the mutation type and and location
			backupMutation.param1 = wr.toValue();
			ASSERT( backupMutation.param1.startsWith(logRangeMutation->first) );  // We are writing into the configured destination
				
			auto& tags = self->tagsForKey(backupMutation.param1);
			toCommit->addTags(tags);
			toCommit->addTypedMessage(backupMutation);

//			if (DEBUG_MUTATION("BackupProxyCommit", commitVersion, backupMutation)) {
//				TraceEvent("BackupProxyCommitTo", self->dbgid).detail("To", describe(tags)).detail("BackupMutation", backupMutation.toString())
//					.detail("BackupMutationSize", val.size()).detail("Version", commitVersion).detail("DestPath", logRangeMutation.first)
//					.detail("PartIndex", part).detail("PartIndexEndian", bigEndian32(part)).detail("PartData", backupMutation.param1);
//			}
		}
	}
	return Void();
}

ACTOR Future<Void> releaseResolvingAfter(ProxyCommitData* self, Future<Void> releaseDelay, int64_t localBatchNumber) {
	wait(releaseDelay);
	ASSERT(self->latestLocalCommitBatchResolving.get() == localBatchNumber-1);
	self->latestLocalCommitBatchResolving.set(localBatchNumber);
	return Void();
}

// Commit one batch of transactions trs
ACTOR Future<Void> commitBatch(
	ProxyCommitData* self,
	vector<CommitTransactionRequest> trs,
	int currentBatchMemBytesCount)
{
	//WARNING: this code is run at a high priority (until the first delay(0)), so it needs to do as little work as possible
	state int64_t localBatchNumber = ++self->localCommitBatchesStarted;
	state LogPushData toCommit(self->logSystem);
	state double t1 = now();
	state Optional<UID> debugID;
	state bool forceRecovery = false;
	state int batchOperations = 0;
	int64_t batchBytes = 0;
	for (int t = 0; t<trs.size(); t++) {
		batchOperations += trs[t].transaction.mutations.size();
		batchBytes += trs[t].transaction.mutations.expectedSize();
	}
	state int latencyBucket = batchOperations == 0 ? 0 : std::min<int>(SERVER_KNOBS->PROXY_COMPUTE_BUCKETS-1,SERVER_KNOBS->PROXY_COMPUTE_BUCKETS*batchBytes/(batchOperations*(CLIENT_KNOBS->VALUE_SIZE_LIMIT+CLIENT_KNOBS->KEY_SIZE_LIMIT)));

	ASSERT(self->readTxnLifetime <=
	       SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT); // since we are using just the former to limit the number of versions
	                                              // actually in flight!

	// Active load balancing runs at a very high priority (to obtain accurate estimate of memory used by commit batches) so we need to downgrade here
	wait(delay(0, TaskPriority::ProxyCommit));

	self->lastVersionTime = t1;

	++self->stats.commitBatchIn;

	for (int t = 0; t<trs.size(); t++) {
		if (trs[t].debugID.present()) {
			if (!debugID.present())
				debugID = nondeterministicRandom()->randomUniqueID();
			g_traceBatch.addAttach("CommitAttachID", trs[t].debugID.get().first(), debugID.get().first());
		}
	}

	if(localBatchNumber == 2 && !debugID.present() && self->firstProxy && !g_network->isSimulated()) {
		debugID = deterministicRandom()->randomUniqueID();
		TraceEvent("SecondCommitBatch", self->dbgid).detail("DebugID", debugID.get());
	}

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.Before");

	/////// Phase 1: Pre-resolution processing (CPU bound except waiting for a version # which is separately pipelined and *should* be available by now (unless empty commit); ordered; currently atomic but could yield)

	// Queuing pre-resolution commit processing
	TEST(self->latestLocalCommitBatchResolving.get() < localBatchNumber - 1);
	wait(self->latestLocalCommitBatchResolving.whenAtLeast(localBatchNumber-1));
	state Future<Void> releaseDelay = delay(std::min(SERVER_KNOBS->MAX_PROXY_COMPUTE, batchOperations*self->commitComputePerOperation[latencyBucket]), TaskPriority::ProxyMasterVersionReply);

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.GettingCommitVersion");

	GetCommitVersionRequest req(self->commitVersionRequestNumber++, self->mostRecentProcessedRequestNumber, self->dbgid);
	GetCommitVersionReply versionReply = wait( brokenPromiseToNever(self->master.getCommitVersion.getReply(req, TaskPriority::ProxyMasterVersionReply)) );
	self->mostRecentProcessedRequestNumber = versionReply.requestNum;

	self->stats.txnCommitVersionAssigned += trs.size();
	self->stats.lastCommitVersionAssigned = versionReply.version;

	state Version commitVersion = versionReply.version;
	state Version prevVersion = versionReply.prevVersion;

	for(auto it : versionReply.resolverChanges) {
		auto rs = self->keyResolvers.modify(it.range);
		for(auto r = rs.begin(); r != rs.end(); ++r)
			r->value().emplace_back(versionReply.resolverChangesVersion,it.dest);
	}

	//TraceEvent("ProxyGotVer", self->dbgid).detail("Commit", commitVersion).detail("Prev", prevVersion);

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.GotCommitVersion");

	ResolutionRequestBuilder requests( self, commitVersion, prevVersion, self->version );
	int conflictRangeCount = 0;
	state int64_t maxTransactionBytes = 0;
	for (int t = 0; t<trs.size(); t++) {
		requests.addTransaction(trs[t].transaction, t);
		conflictRangeCount += trs[t].transaction.read_conflict_ranges.size() + trs[t].transaction.write_conflict_ranges.size();
		//TraceEvent("MPTransactionDump", self->dbgid).detail("Snapshot", trs[t].transaction.read_snapshot);
		//for(auto& m : trs[t].transaction.mutations)
		maxTransactionBytes = std::max<int64_t>(maxTransactionBytes, trs[t].transaction.expectedSize());
		//	TraceEvent("MPTransactionsDump", self->dbgid).detail("Mutation", m.toString());
	}
	self->stats.conflictRanges += conflictRangeCount;

	for (int r = 1; r<self->resolvers.size(); r++)
		ASSERT(requests.requests[r].txnStateTransactions.size() == requests.requests[0].txnStateTransactions.size());

	// Sending these requests is the fuzzy border between phase 1 and phase 2; it could conceivably overlap with resolution processing but is still using CPU
	self->stats.txnCommitResolving += trs.size();
	vector< Future<ResolveTransactionBatchReply> > replies;
	for (int r = 0; r<self->resolvers.size(); r++) {
		requests.requests[r].debugID = debugID;
		replies.push_back(brokenPromiseToNever(self->resolvers[r].resolve.getReply(requests.requests[r], TaskPriority::ProxyResolverReply)));
	}

	state vector<vector<int>> transactionResolverMap = std::move( requests.transactionResolverMap );
	state std::vector<std::vector<std::vector<int>>> txReadConflictRangeIndexMap =
	    std::move(requests.txReadConflictRangeIndexMap); // used to report conflicting keys
	state Future<Void> releaseFuture = releaseResolvingAfter(self, releaseDelay, localBatchNumber);

	/////// Phase 2: Resolution (waiting on the network; pipelined)
	state vector<ResolveTransactionBatchReply> resolution = wait( getAll(replies) );

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.AfterResolution");

	////// Phase 3: Post-resolution processing (CPU bound except for very rare situations; ordered; currently atomic but doesn't need to be)
	TEST(self->latestLocalCommitBatchLogging.get() < localBatchNumber - 1); // Queuing post-resolution commit processing
	wait(self->latestLocalCommitBatchLogging.whenAtLeast(localBatchNumber-1));
	wait(yield(TaskPriority::ProxyCommitYield1));

	state double computeStart = g_network->timer();
	state double computeDuration = 0; 
	self->stats.txnCommitResolved += trs.size();

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.ProcessingMutations");

	state Arena arena;
	state bool isMyFirstBatch = !self->version;
	state Optional<Value> oldCoordinators = self->txnStateStore->readValue(coordinatorsKey).get();

	//TraceEvent("ResolutionResult", self->dbgid).detail("Sequence", sequence).detail("Version", commitVersion).detail("StateMutationProxies", resolution[0].stateMutations.size()).detail("WaitForResolution", now()-t1).detail("R0Committed", resolution[0].committed.size())
	//	.detail("Transactions", trs.size());

	for(int r=1; r<resolution.size(); r++) {
		ASSERT( resolution[r].stateMutations.size() == resolution[0].stateMutations.size() );
		for(int s=0; s<resolution[r].stateMutations.size(); s++)
			ASSERT( resolution[r].stateMutations[s].size() == resolution[0].stateMutations[s].size() );
	}

	// Compute and apply "metadata" effects of each other proxy's most recent batch
	bool initialState = isMyFirstBatch;
	state bool firstStateMutations = isMyFirstBatch;
	state vector< std::pair<Future<LogSystemDiskQueueAdapter::CommitMessage>, Future<Void>> > storeCommits;
	for (int versionIndex = 0; versionIndex < resolution[0].stateMutations.size(); versionIndex++) {
		// self->logAdapter->setNextVersion( ??? );  << Ideally we would be telling the log adapter that the pushes in this commit will be in the version at which these state mutations were committed by another proxy, but at present we don't have that information here.  So the disk queue may be unnecessarily conservative about popping.

		for (int transactionIndex = 0; transactionIndex < resolution[0].stateMutations[versionIndex].size() && !forceRecovery; transactionIndex++) {
			bool committed = true;
			for (int resolver = 0; resolver < resolution.size(); resolver++)
				committed = committed && resolution[resolver].stateMutations[versionIndex][transactionIndex].committed;
			if (committed)
				applyMetadataMutations( self->dbgid, arena, resolution[0].stateMutations[versionIndex][transactionIndex].mutations, self->txnStateStore, nullptr, &forceRecovery, self->logSystem, 0, &self->vecBackupKeys, &self->keyInfo, &self->cacheInfo, self->firstProxy ? &self->uid_applyMutationsData : nullptr, self->commit, self->cx, &self->committedVersion, &self->storageCache, &self->tag_popped);

			if( resolution[0].stateMutations[versionIndex][transactionIndex].mutations.size() && firstStateMutations ) {
				ASSERT(committed);
				firstStateMutations = false;
				forceRecovery = false;
			}
			//TraceEvent("MetadataTransaction", self->dbgid).detail("Committed", committed).detail("Mutations", resolution[0].stateMutations[versionIndex][transactionIndex].second.size()).detail("R1Mutations", resolution.back().stateMutations[versionIndex][transactionIndex].second.size());
		}
		//TraceEvent("MetadataBatch", self->dbgid).detail("Transactions", resolution[0].stateMutations[versionIndex].size());

		// These changes to txnStateStore will be committed by the other proxy, so we simply discard the commit message
		auto fcm = self->logAdapter->getCommitMessage();
		storeCommits.emplace_back(fcm, self->txnStateStore->commit());
		//discardCommit( dbgid, fcm, txnStateStore->commit() );

		if (initialState) {
			//TraceEvent("ResyncLog", dbgid);
			initialState = false;
			forceRecovery = false;
			self->txnStateStore->resyncLog();

			for (auto &p : storeCommits) {
				ASSERT(!p.second.isReady());
				p.first.get().acknowledge.send(Void());
				ASSERT(p.second.isReady());
			}
			storeCommits.clear();
		}
	}

	// Determine which transactions actually committed (conservatively) by combining results from the resolvers
	state vector<uint8_t> committed(trs.size());
	ASSERT(transactionResolverMap.size() == committed.size());
	// For each commitTransactionRef, it is only sent to resolvers specified in transactionResolverMap
	// Thus, we use this nextTr to track the correct transaction index on each resolver.
	state vector<int> nextTr(resolution.size());
	for (int t = 0; t<trs.size(); t++) {
		uint8_t commit = ConflictBatch::TransactionCommitted;
		for (int r : transactionResolverMap[t])
		{
			commit = std::min(resolution[r].committed[nextTr[r]++], commit);
		}
		committed[t] = commit;
	}
	for (int r = 0; r<resolution.size(); r++)
		ASSERT(nextTr[r] == resolution[r].committed.size());

	self->logAdapter->setNextVersion(commitVersion);

	state Optional<Key> lockedKey = self->txnStateStore->readValue(databaseLockedKey).get();
	state bool locked = lockedKey.present() && lockedKey.get().size();

	state Optional<Key> mustContainSystemKey = self->txnStateStore->readValue(mustContainSystemMutationsKey).get();
	if(mustContainSystemKey.present() && mustContainSystemKey.get().size()) {
		for (int t = 0; t<trs.size(); t++) {
			if( committed[t] == ConflictBatch::TransactionCommitted ) {
				bool foundSystem = false;
				for(auto& m : trs[t].transaction.mutations) {
					if( ( m.type == MutationRef::ClearRange ? m.param2 : m.param1 ) >= nonMetadataSystemKeys.end) {
						foundSystem = true;
						break;
					}
				}
				if(!foundSystem) {
					committed[t] = ConflictBatch::TransactionConflict;
				}
			}
		}
	}

	if(forceRecovery) {
		wait( Future<Void>(Never()) );
	}

	// This first pass through committed transactions deals with "metadata" effects (modifications of txnStateStore, changes to storage servers' responsibilities)
	int t;
	state int commitCount = 0;
	for (t = 0; t < trs.size() && !forceRecovery; t++)
	{
		if (committed[t] == ConflictBatch::TransactionCommitted && (!locked || trs[t].isLockAware())) {
			commitCount++;
			applyMetadataMutations(self->dbgid, arena, trs[t].transaction.mutations, self->txnStateStore, &toCommit, &forceRecovery, self->logSystem, commitVersion+1, &self->vecBackupKeys, &self->keyInfo, &self->cacheInfo, self->firstProxy ? &self->uid_applyMutationsData : NULL, self->commit, self->cx, &self->committedVersion, &self->storageCache, &self->tag_popped);
		}
		if(firstStateMutations) {
			ASSERT(committed[t] == ConflictBatch::TransactionCommitted);
			firstStateMutations = false;
			forceRecovery = false;
		}
	}
	if (forceRecovery) {
		for (; t<trs.size(); t++)
			committed[t] = ConflictBatch::TransactionConflict;
		TraceEvent(SevWarn, "RestartingTxnSubsystem", self->dbgid).detail("Stage", "AwaitCommit");
	}

	lockedKey = self->txnStateStore->readValue(databaseLockedKey).get();
	state bool lockedAfter = lockedKey.present() && lockedKey.get().size();

	state Optional<Value> metadataVersionAfter = self->txnStateStore->readValue(metadataVersionKey).get();

	auto fcm = self->logAdapter->getCommitMessage();
	storeCommits.emplace_back(fcm, self->txnStateStore->commit());
	self->version = commitVersion;
	if (!self->validState.isSet()) self->validState.send(Void());
	ASSERT(commitVersion);

	if (!isMyFirstBatch && self->txnStateStore->readValue( coordinatorsKey ).get().get() != oldCoordinators.get()) {
		wait( brokenPromiseToNever( self->master.changeCoordinators.getReply( ChangeCoordinatorsRequest( self->txnStateStore->readValue( coordinatorsKey ).get().get() ) ) ) );
		ASSERT(false);   // ChangeCoordinatorsRequest should always throw
	}

	// This second pass through committed transactions assigns the actual mutations to the appropriate storage servers' tags
	state int mutationCount = 0;
	state int mutationBytes = 0;

	state std::map<Key, MutationListRef> logRangeMutations;
	state Arena logRangeMutationsArena;
	state int transactionNum = 0;
	state int yieldBytes = 0;

	for (; transactionNum<trs.size(); transactionNum++) {
		if (committed[transactionNum] == ConflictBatch::TransactionCommitted && (!locked || trs[transactionNum].isLockAware())) {
			state int mutationNum = 0;
			state VectorRef<MutationRef>* pMutations = &trs[transactionNum].transaction.mutations;
			for (; mutationNum < pMutations->size(); mutationNum++) {
				if(yieldBytes > SERVER_KNOBS->DESIRED_TOTAL_BYTES) {
					yieldBytes = 0;
					if(g_network->check_yield(TaskPriority::ProxyCommitYield1)) {
						computeDuration += g_network->timer() - computeStart;
						wait(delay(0, TaskPriority::ProxyCommitYield1));
						computeStart = g_network->timer();
					}
				}

				auto& m = (*pMutations)[mutationNum];
				mutationCount++;
				mutationBytes += m.expectedSize();
				yieldBytes += m.expectedSize();
				// Determine the set of tags (responsible storage servers) for the mutation, splitting it
				// if necessary.  Serialize (splits of) the mutation into the message buffer and add the tags.

				if (isSingleKeyMutation((MutationRef::Type) m.type)) {
					auto& tags = self->tagsForKey(m.param1);

					if(self->singleKeyMutationEvent->enabled) {
						KeyRangeRef shard = self->keyInfo.rangeContaining(m.param1).range();
						self->singleKeyMutationEvent->tag1 = (int64_t)tags[0].id;
						self->singleKeyMutationEvent->tag2 = (int64_t)tags[1].id;
						self->singleKeyMutationEvent->tag3 = (int64_t)tags[2].id;
						self->singleKeyMutationEvent->shardBegin = shard.begin;
						self->singleKeyMutationEvent->shardEnd = shard.end;
						self->singleKeyMutationEvent->log();
					}

					DEBUG_MUTATION("ProxyCommit", commitVersion, m).detail("Dbgid", self->dbgid).detail("To", tags).detail("Mutation", m);
					
					toCommit.addTags(tags);
					if(self->cacheInfo[m.param1]) {
						toCommit.addTag(cacheTag);
					}
					toCommit.addTypedMessage(m);
				}
				else if (m.type == MutationRef::ClearRange) {
					KeyRangeRef clearRange(KeyRangeRef(m.param1, m.param2));
					auto ranges = self->keyInfo.intersectingRanges(clearRange);
					auto firstRange = ranges.begin();
					++firstRange;
					if (firstRange == ranges.end()) {
						// Fast path
						DEBUG_MUTATION("ProxyCommit", commitVersion, m).detail("Dbgid", self->dbgid).detail("To", ranges.begin().value().tags).detail("Mutation", m);

						ranges.begin().value().populateTags();
						toCommit.addTags(ranges.begin().value().tags);
					}
					else {
						TEST(true); //A clear range extends past a shard boundary
						std::set<Tag> allSources;
						for (auto r : ranges) {
							r.value().populateTags();
							allSources.insert(r.value().tags.begin(), r.value().tags.end());
						}
						DEBUG_MUTATION("ProxyCommit", commitVersion, m).detail("Dbgid", self->dbgid).detail("To", allSources).detail("Mutation", m);

						toCommit.addTags(allSources);
					}
					if(self->needsCacheTag(clearRange)) {
						toCommit.addTag(cacheTag);
					}
					toCommit.addTypedMessage(m);
				} else
					UNREACHABLE();


				// Check on backing up key, if backup ranges are defined and a normal key
				if (self->vecBackupKeys.size() > 1 && (normalKeys.contains(m.param1) || m.param1 == metadataVersionKey)) {
					if (m.type != MutationRef::Type::ClearRange) {
						// Add the mutation to the relevant backup tag
						for (auto backupName : self->vecBackupKeys[m.param1]) {
							logRangeMutations[backupName].push_back_deep(logRangeMutationsArena, m);
						}
					}
					else {
						KeyRangeRef mutationRange(m.param1, m.param2);
						KeyRangeRef intersectionRange;

						// Identify and add the intersecting ranges of the mutation to the array of mutations to serialize
						for (auto backupRange : self->vecBackupKeys.intersectingRanges(mutationRange))
						{
							// Get the backup sub range
							const auto&		backupSubrange = backupRange.range();

							// Determine the intersecting range
							intersectionRange = mutationRange & backupSubrange;

							// Create the custom mutation for the specific backup tag
							MutationRef		backupMutation(MutationRef::Type::ClearRange, intersectionRange.begin, intersectionRange.end);

							// Add the mutation to the relevant backup tag
							for (auto backupName : backupRange.value()) {
								logRangeMutations[backupName].push_back_deep(logRangeMutationsArena, backupMutation);
							}
						}
					}
				}
			}
		}
	}

	// Serialize and backup the mutations as a single mutation
	if ((self->vecBackupKeys.size() > 1) && logRangeMutations.size()) {
		wait( addBackupMutations(self, &logRangeMutations, &toCommit, commitVersion, &computeDuration, &computeStart) );
	}

	self->stats.mutations += mutationCount;
	self->stats.mutationBytes += mutationBytes;

	// Storage servers mustn't make durable versions which are not fully committed (because then they are impossible to roll back)
	// We prevent this by limiting the number of versions which are semi-committed but not fully committed to be less than the MVCC window
	if (self->committedVersion.get() < commitVersion - self->readTxnLifetime) {
		computeDuration += g_network->timer() - computeStart;
		while (self->committedVersion.get() < commitVersion - self->readTxnLifetime) {
			// This should be *extremely* rare in the real world, but knob buggification should make it happen in
			// simulation
			TEST(true);  // Semi-committed pipeline limited by MVCC window
			//TraceEvent("ProxyWaitingForCommitted", self->dbgid).detail("CommittedVersion", self->committedVersion.get()).detail("NeedToCommit", commitVersion);
			choose{
				when(wait(self->committedVersion.whenAtLeast(commitVersion - self->readTxnLifetime))) {
					wait(yield());
					break;
				}
				when(GetReadVersionReply v = wait(self->getConsistentReadVersion.getReply(GetReadVersionRequest(0, TransactionPriority::IMMEDIATE, GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY)))) {
					if(v.version > self->committedVersion.get()) {
						self->locked = v.locked;
						self->metadataVersion = v.metadataVersion;
						self->committedVersion.set(v.version);
					}

					if (self->committedVersion.get() < commitVersion - self->readTxnLifetime)
						wait(delay(SERVER_KNOBS->PROXY_SPIN_DELAY));
				}
			}
		}
		computeStart = g_network->timer();
	}

	state LogSystemDiskQueueAdapter::CommitMessage msg = storeCommits.back().first.get();

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.AfterStoreCommits");

	// txnState (transaction subsystem state) tag: message extracted from log adapter
	bool firstMessage = true;
	for(auto m : msg.messages) {
		if(firstMessage) {
			toCommit.addTxsTag();
		}
		toCommit.addMessage(StringRef(m.begin(), m.size()), !firstMessage);
		firstMessage = false;
	}

	if ( prevVersion && commitVersion - prevVersion < SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT/2 )
		debug_advanceMaxCommittedVersion( UID(), commitVersion );  //< Is this valid?

	//TraceEvent("ProxyPush", self->dbgid).detail("PrevVersion", prevVersion).detail("Version", commitVersion)
	//	.detail("TransactionsSubmitted", trs.size()).detail("TransactionsCommitted", commitCount).detail("TxsPopTo", msg.popTo);

	if ( prevVersion && commitVersion - prevVersion < SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT/2 )
		debug_advanceMaxCommittedVersion(UID(), commitVersion);

	state double commitStartTime = now();
	self->lastStartCommit = commitStartTime;
	Future<Version> loggingComplete = self->logSystem->push( prevVersion, commitVersion, self->committedVersion.get(), self->minKnownCommittedVersion, toCommit, debugID );

	if (!forceRecovery) {
		ASSERT(self->latestLocalCommitBatchLogging.get() == localBatchNumber-1);
		self->latestLocalCommitBatchLogging.set(localBatchNumber);
	}

	computeDuration += g_network->timer() - computeStart;
	if(computeDuration > SERVER_KNOBS->MIN_PROXY_COMPUTE && batchOperations > 0) {
		double computePerOperation = computeDuration/batchOperations;
		if(computePerOperation <= self->commitComputePerOperation[latencyBucket]) {
			self->commitComputePerOperation[latencyBucket] = computePerOperation;
		} else {
			self->commitComputePerOperation[latencyBucket] = SERVER_KNOBS->PROXY_COMPUTE_GROWTH_RATE*computePerOperation + ((1.0-SERVER_KNOBS->PROXY_COMPUTE_GROWTH_RATE)*self->commitComputePerOperation[latencyBucket]);
		}
	}

	/////// Phase 4: Logging (network bound; pipelined up to MAX_READ_TRANSACTION_LIFE_VERSIONS (limited by loop above))

	try {
		choose {
			when(Version ver = wait(loggingComplete)) {
				self->minKnownCommittedVersion = std::max(self->minKnownCommittedVersion, ver);
			}
			when(wait(self->committedVersion.whenAtLeast( commitVersion+1 ))) {}
		}
	} catch(Error &e) {
		if(e.code() == error_code_broken_promise) {
			throw master_tlog_failed();
		}
		throw;
	}
	self->lastCommitLatency = now()-commitStartTime;
	self->lastCommitTime = std::max(self->lastCommitTime.get(), commitStartTime);
	wait(yield(TaskPriority::ProxyCommitYield2));

	if( self->popRemoteTxs && msg.popTo > ( self->txsPopVersions.size() ? self->txsPopVersions.back().second : self->lastTxsPop ) ) {
		if(self->txsPopVersions.size() >= SERVER_KNOBS->MAX_TXS_POP_VERSION_HISTORY) {
			TraceEvent(SevWarnAlways, "DiscardingTxsPopHistory").suppressFor(1.0);
			self->txsPopVersions.pop_front();
		}

		self->txsPopVersions.emplace_back(commitVersion, msg.popTo);
	}
	self->logSystem->popTxs(msg.popTo);

	/////// Phase 5: Replies (CPU bound; no particular order required, though ordered execution would be best for latency)
	if ( prevVersion && commitVersion - prevVersion < SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT/2 )
		debug_advanceMinCommittedVersion(UID(), commitVersion);

	//TraceEvent("ProxyPushed", self->dbgid).detail("PrevVersion", prevVersion).detail("Version", commitVersion);
	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.AfterLogPush");

	for (auto &p : storeCommits) {
		ASSERT(!p.second.isReady());
		p.first.get().acknowledge.send(Void());
		ASSERT(p.second.isReady());
	}

	TEST(self->committedVersion.get() > commitVersion);   // A later version was reported committed first
	if( commitVersion > self->committedVersion.get() ) {
		if (SERVER_KNOBS->ASK_READ_VERSION_FROM_MASTER) {
			// Let master know this commit version so that every other proxy can know.
			wait(self->master.reportLiveCommittedVersion.getReply(ReportRawCommittedVersionRequest(commitVersion, lockedAfter, metadataVersionAfter), TaskPriority::ProxyMasterVersionReply));
		}

		// After we report the commit version above, other batch commitBatch executions may have updated 'self->committedVersion'
		// to be a larger commitVersion.
		if (commitVersion > self->committedVersion.get()) {
			self->committedVersion.set(commitVersion);
			self->locked = lockedAfter;
			self->metadataVersion = metadataVersionAfter;
		}
	}

	if (forceRecovery) {
		TraceEvent(SevWarn, "RestartingTxnSubsystem", self->dbgid).detail("Stage", "ProxyShutdown");
		throw worker_removed();
	}

	// Send replies to clients
	double endTime = g_network->timer();
	// Reset all to zero, used to track the correct index of each commitTransacitonRef on each resolver
	std::fill(nextTr.begin(), nextTr.end(), 0);
	for (int t = 0; t < trs.size(); t++) {
		if (committed[t] == ConflictBatch::TransactionCommitted && (!locked || trs[t].isLockAware())) {
			ASSERT_WE_THINK(commitVersion != invalidVersion);
			trs[t].reply.send(CommitID(commitVersion, t, metadataVersionAfter));
		}
		else if (committed[t] == ConflictBatch::TransactionTooOld) {
			trs[t].reply.sendError(transaction_too_old());
		}
		else {
			// If enable the option to report conflicting keys from resolvers, we send back all keyranges' indices
			// through CommitID
			if (trs[t].transaction.report_conflicting_keys) {
				Standalone<VectorRef<int>> conflictingKRIndices;
				for (int resolverInd : transactionResolverMap[t]) {
					auto const& cKRs =
					    resolution[resolverInd]
					        .conflictingKeyRangeMap[nextTr[resolverInd]]; // nextTr[resolverInd] -> index of this trs[t]
					                                                      // on the resolver
					for (auto const& rCRIndex : cKRs)
						// read_conflict_range can change when sent to resolvers, mapping the index from resolver-side
						// to original index in commitTransactionRef
						conflictingKRIndices.push_back(conflictingKRIndices.arena(),
						                               txReadConflictRangeIndexMap[t][resolverInd][rCRIndex]);
				}
				// At least one keyRange index should be returned
				ASSERT(conflictingKRIndices.size());
				trs[t].reply.send(CommitID(invalidVersion, t, Optional<Value>(),
				                           Optional<Standalone<VectorRef<int>>>(conflictingKRIndices)));
			} else {
				trs[t].reply.sendError(not_committed());
			}
		}

		// Update corresponding transaction indices on each resolver
		for (int resolverInd : transactionResolverMap[t]) nextTr[resolverInd]++;

		// TODO: filter if pipelined with large commit
		if(self->latencyBandConfig.present()) {
			bool filter = maxTransactionBytes > self->latencyBandConfig.get().commitConfig.maxCommitBytes.orDefault(std::numeric_limits<int>::max());
			self->stats.commitLatencyBands.addMeasurement(endTime - trs[t].requestTime(), filter);
		}
	}

	++self->stats.commitBatchOut;
	self->stats.txnCommitOut += trs.size();
	self->stats.txnConflicts += trs.size() - commitCount;
	self->stats.txnCommitOutSuccess += commitCount;

	if(now() - self->lastCoalesceTime > SERVER_KNOBS->RESOLVER_COALESCE_TIME) {
		self->lastCoalesceTime = now();
		int lastSize = self->keyResolvers.size();
		auto rs = self->keyResolvers.ranges();
		Version oldestVersion = prevVersion - SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS;
		for(auto r = rs.begin(); r != rs.end(); ++r) {
			while(r->value().size() > 1 && r->value()[1].first < oldestVersion)
				r->value().pop_front();
			if(r->value().size() && r->value().front().first < oldestVersion)
				r->value().front().first = 0;
		}
		self->keyResolvers.coalesce(allKeys);
		if(self->keyResolvers.size() != lastSize)
			TraceEvent("KeyResolverSize", self->dbgid).detail("Size", self->keyResolvers.size());
	}

	// Dynamic batching for commits
	double target_latency = (now() - t1) * SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION;
	self->commitBatchInterval = std::max(
	    SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_MIN,
	    std::min(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_MAX,
	             target_latency * SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA +
	                 self->commitBatchInterval * (1 - SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA)));

	self->commitBatchesMemBytesCount -= currentBatchMemBytesCount;
	ASSERT_ABORT(self->commitBatchesMemBytesCount >= 0);
	wait(releaseFuture);
	return Void();
}

ACTOR Future<Void> updateLastCommit(ProxyCommitData* self, Optional<UID> debugID = Optional<UID>()) {
	state double confirmStart = now();
	self->lastStartCommit = confirmStart;
	self->updateCommitRequests++;
	wait(self->logSystem->confirmEpochLive(debugID));
	self->updateCommitRequests--;
	self->lastCommitLatency = now()-confirmStart;
	self->lastCommitTime = std::max(self->lastCommitTime.get(), confirmStart);
	return Void();
}

ACTOR Future<GetReadVersionReply> getLiveCommittedVersion(ProxyCommitData* commitData, uint32_t flags, vector<MasterProxyInterface> *otherProxies, Optional<UID> debugID,
                                                          int transactionCount, int systemTransactionCount, int defaultPriTransactionCount, int batchPriTransactionCount)
{
	// Returns a version which (1) is committed, and (2) is >= the latest version reported committed (by a commit response) when this request was sent
	// (1) The version returned is the committedVersion of some proxy at some point before the request returns, so it is committed.
	// (2) No proxy on our list reported committed a higher version before this request was received, because then its committedVersion would have been higher,
	//     and no other proxy could have already committed anything without first ending the epoch
	++commitData->stats.txnStartBatch;
	state vector<Future<GetReadVersionReply>> proxyVersions;
	state Future<GetReadVersionReply> replyFromMasterFuture;
	if (SERVER_KNOBS->ASK_READ_VERSION_FROM_MASTER) {
		replyFromMasterFuture = commitData->master.getLiveCommittedVersion.getReply(GetRawCommittedVersionRequest(debugID), TaskPriority::GetLiveCommittedVersionReply);
	} else {
		for (auto const& p : *otherProxies)
			proxyVersions.push_back(brokenPromiseToNever(p.getRawCommittedVersion.getReply(GetRawCommittedVersionRequest(debugID), TaskPriority::TLogConfirmRunningReply)));
	}

	if (!SERVER_KNOBS->ALWAYS_CAUSAL_READ_RISKY && !(flags&GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY)) {
		wait(updateLastCommit(commitData, debugID));
	} else if (SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION > 0 && now() - SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION > commitData->lastCommitTime.get()) {
		wait(commitData->lastCommitTime.whenAtLeast(now() - SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION));
	}

	if (debugID.present()) {
		g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "MasterProxyServer.getLiveCommittedVersion.confirmEpochLive");
	}

	state GetReadVersionReply rep;
	rep.locked = commitData->locked;
	rep.metadataVersion = commitData->metadataVersion;
	rep.version = commitData->committedVersion.get();

	if (SERVER_KNOBS->ASK_READ_VERSION_FROM_MASTER) {
		GetReadVersionReply replyFromMaster = wait(replyFromMasterFuture);
		if (replyFromMaster.version > rep.version) {
			rep = replyFromMaster;
		}
	} else {
		vector<GetReadVersionReply> versions = wait(getAll(proxyVersions));
		for (auto v : versions) {
			if (v.version > rep.version) {
				rep = v;
			}
		}
	}
	rep.recentRequests = commitData->stats.getRecentRequests();

	if (debugID.present()) {
		g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "MasterProxyServer.getLiveCommittedVersion.After");
	}

	commitData->stats.txnStartOut += transactionCount;
	commitData->stats.txnSystemPriorityStartOut += systemTransactionCount;
	commitData->stats.txnDefaultPriorityStartOut += defaultPriTransactionCount;
	commitData->stats.txnBatchPriorityStartOut += batchPriTransactionCount;

	return rep;
}

ACTOR Future<Void> sendGrvReplies(Future<GetReadVersionReply> replyFuture, std::vector<GetReadVersionRequest> requests,
                                  ProxyStats* stats, Version minKnownCommittedVersion, PrioritizedTransactionTagMap<ClientTagThrottleLimits> throttledTags) {
	GetReadVersionReply _reply = wait(replyFuture);
	GetReadVersionReply reply = _reply;
	Version replyVersion = reply.version;

	double end = g_network->timer();
	for(GetReadVersionRequest const& request : requests) {
		if(request.priority >= TransactionPriority::DEFAULT) {
			stats->grvLatencyBands.addMeasurement(end - request.requestTime());
		}

		if (request.flags & GetReadVersionRequest::FLAG_USE_MIN_KNOWN_COMMITTED_VERSION) {
			// Only backup worker may infrequently use this flag.
			reply.version = minKnownCommittedVersion;
		} 
		else {
			reply.version = replyVersion;
		}

		reply.tagThrottleInfo.clear();

		if(!request.tags.empty()) {
			auto& priorityThrottledTags = throttledTags[request.priority];
			for(auto tag : request.tags) {
				auto tagItr = priorityThrottledTags.find(tag.first);
				if(tagItr != priorityThrottledTags.end()) {
					if(tagItr->second.expiration > now()) {
						TEST(true); // Proxy returning tag throttle
						reply.tagThrottleInfo[tag.first] = tagItr->second;
					}
					else {
						// This isn't required, but we might as well
						TEST(true); // Proxy expiring tag throttle
						priorityThrottledTags.erase(tagItr);
					}
				}
			}
		}

		request.reply.send(reply);
		++stats->txnRequestOut;
	}

	return Void();
}

ACTOR static Future<Void> transactionStarter(
	MasterProxyInterface proxy,
	Reference<AsyncVar<ServerDBInfo>> db,
	PromiseStream<Future<Void>> addActor,
	ProxyCommitData* commitData, GetHealthMetricsReply* healthMetricsReply,
	GetHealthMetricsReply* detailedHealthMetricsReply)
{
	state double lastGRVTime = 0;
	state PromiseStream<Void> GRVTimer;
	state double GRVBatchTime = SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MIN;

	state int64_t transactionCount = 0;
	state int64_t batchTransactionCount = 0;
	state TransactionRateInfo normalRateInfo(10);
	state TransactionRateInfo batchRateInfo(0);

	state Deque<GetReadVersionRequest> systemQueue;
	state Deque<GetReadVersionRequest> defaultQueue;
	state Deque<GetReadVersionRequest> batchQueue;
	state vector<MasterProxyInterface> otherProxies;

	state TransactionTagMap<uint64_t> transactionTagCounter;
	state PrioritizedTransactionTagMap<ClientTagThrottleLimits> throttledTags;

	state PromiseStream<double> replyTimes;

	addActor.send(getRate(proxy.id(), db, &transactionCount, &batchTransactionCount, &normalRateInfo, &batchRateInfo, healthMetricsReply, detailedHealthMetricsReply, &transactionTagCounter, &throttledTags));
	addActor.send(queueTransactionStartRequests(db, &systemQueue, &defaultQueue, &batchQueue, proxy.getConsistentReadVersion.getFuture(),
	                                            GRVTimer, &lastGRVTime, &GRVBatchTime, replyTimes.getFuture(), &commitData->stats, &batchRateInfo,
	                                            &transactionTagCounter));

	// Get a list of the other proxies that go together with us
	while (std::find(db->get().client.proxies.begin(), db->get().client.proxies.end(), proxy) == db->get().client.proxies.end())
		wait(db->onChange());
	for (MasterProxyInterface mp : db->get().client.proxies) {
		if (mp != proxy)
			otherProxies.push_back(mp);
	}

	ASSERT(db->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS);  // else potentially we could return uncommitted read versions (since self->committedVersion is only a committed version if this recovery succeeds)

	TraceEvent("ProxyReadyForTxnStarts", proxy.id());

	loop{
		waitNext(GRVTimer.getFuture());
		// Select zero or more transactions to start
		double t = now();
		double elapsed = now() - lastGRVTime;
		lastGRVTime = t;

		if(elapsed == 0) elapsed = 1e-15; // resolve a possible indeterminant multiplication with infinite transaction rate

		normalRateInfo.reset();
		batchRateInfo.reset();

		int transactionsStarted[2] = {0,0};
		int systemTransactionsStarted[2] = {0,0};
		int defaultPriTransactionsStarted[2] = { 0, 0 };
		int batchPriTransactionsStarted[2] = { 0, 0 };

		vector<vector<GetReadVersionRequest>> start(2);  // start[0] is transactions starting with !(flags&CAUSAL_READ_RISKY), start[1] is transactions starting with flags&CAUSAL_READ_RISKY
		Optional<UID> debugID;

		int requestsToStart = 0;

		while (requestsToStart < SERVER_KNOBS->START_TRANSACTION_MAX_REQUESTS_TO_START) {
			Deque<GetReadVersionRequest>* transactionQueue;
			if(!systemQueue.empty()) {
				transactionQueue = &systemQueue;
			} else if(!defaultQueue.empty()) {
				transactionQueue = &defaultQueue;
			} else if(!batchQueue.empty()) {
				transactionQueue = &batchQueue;
			} else {
				break;
			}

			auto& req = transactionQueue->front();
			int tc = req.transactionCount;

			if(req.priority < TransactionPriority::DEFAULT && !batchRateInfo.canStart(transactionsStarted[0] + transactionsStarted[1], tc)) {
				break;
			}
			else if(req.priority < TransactionPriority::IMMEDIATE && !normalRateInfo.canStart(transactionsStarted[0] + transactionsStarted[1], tc)) {
				break;	
			}

			if (req.debugID.present()) {
				if (!debugID.present()) debugID = nondeterministicRandom()->randomUniqueID();
				g_traceBatch.addAttach("TransactionAttachID", req.debugID.get().first(), debugID.get().first());
			}

			transactionsStarted[req.flags&1] += tc;
			if (req.priority >= TransactionPriority::IMMEDIATE)
				systemTransactionsStarted[req.flags & 1] += tc;
			else if (req.priority >= TransactionPriority::DEFAULT)
				defaultPriTransactionsStarted[req.flags & 1] += tc;
			else
				batchPriTransactionsStarted[req.flags & 1] += tc;

			start[req.flags & 1].push_back(std::move(req));  static_assert(GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY == 1, "Implementation dependent on flag value");
			transactionQueue->pop_front();
			requestsToStart++;
		}

		if (!systemQueue.empty() || !defaultQueue.empty() || !batchQueue.empty()) {
			forwardPromise(GRVTimer, delayJittered(SERVER_KNOBS->START_TRANSACTION_BATCH_QUEUE_CHECK_INTERVAL, TaskPriority::ProxyGRVTimer));
		}

		/*TraceEvent("GRVBatch", proxy.id())
		.detail("Elapsed", elapsed)
		.detail("NTransactionToStart", nTransactionsToStart)
		.detail("TransactionRate", transactionRate)
		.detail("TransactionQueueSize", transactionQueue.size())
		.detail("NumTransactionsStarted", transactionsStarted[0] + transactionsStarted[1])
		.detail("NumSystemTransactionsStarted", systemTransactionsStarted[0] + systemTransactionsStarted[1])
		.detail("NumNonSystemTransactionsStarted", transactionsStarted[0] + transactionsStarted[1] -
		systemTransactionsStarted[0] - systemTransactionsStarted[1])
		.detail("TransactionBudget", transactionBudget)
		.detail("BatchTransactionBudget", batchTransactionBudget);*/

		int systemTotalStarted = systemTransactionsStarted[0] + systemTransactionsStarted[1];
		int normalTotalStarted = defaultPriTransactionsStarted[0] + defaultPriTransactionsStarted[1];
		int batchTotalStarted = batchPriTransactionsStarted[0] + batchPriTransactionsStarted[1];

		transactionCount += transactionsStarted[0] + transactionsStarted[1];
		batchTransactionCount += batchTotalStarted;

		normalRateInfo.updateBudget(systemTotalStarted + normalTotalStarted, systemQueue.empty() && defaultQueue.empty(), elapsed);
		batchRateInfo.updateBudget(systemTotalStarted + normalTotalStarted + batchTotalStarted, systemQueue.empty() && defaultQueue.empty() && batchQueue.empty(), elapsed);

		if (debugID.present()) {
			g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "MasterProxyServer.masterProxyServerCore.Broadcast");
		}

		for (int i = 0; i < start.size(); i++) {
			if (start[i].size()) {
				Future<GetReadVersionReply> readVersionReply = getLiveCommittedVersion(commitData, i, &otherProxies, debugID, transactionsStarted[i], systemTransactionsStarted[i], defaultPriTransactionsStarted[i], batchPriTransactionsStarted[i]);
				addActor.send(sendGrvReplies(readVersionReply, start[i], &commitData->stats,
				                             commitData->minKnownCommittedVersion, throttledTags));

				// for now, base dynamic batching on the time for normal requests (not read_risky)
				if (i == 0) {
					addActor.send(timeReply(readVersionReply, replyTimes));
				}
			}
		}
	}
}

ACTOR static Future<Void> doKeyServerLocationRequest( GetKeyServerLocationsRequest req, ProxyCommitData* commitData ) {
	// We can't respond to these requests until we have valid txnStateStore
	wait(commitData->validState.getFuture());
	wait(delay(0, TaskPriority::DefaultEndpoint));

	GetKeyServerLocationsReply rep;
	if(!req.end.present()) {
		auto r = req.reverse ? commitData->keyInfo.rangeContainingKeyBefore(req.begin) : commitData->keyInfo.rangeContaining(req.begin);
		vector<StorageServerInterface> ssis;
		ssis.reserve(r.value().src_info.size());
		for(auto& it : r.value().src_info) {
			ssis.push_back(it->interf);
		}
		rep.results.push_back(std::make_pair(r.range(), ssis));
	} else if(!req.reverse) {
		int count = 0;
		for(auto r = commitData->keyInfo.rangeContaining(req.begin); r != commitData->keyInfo.ranges().end() && count < req.limit && r.begin() < req.end.get(); ++r) {
			vector<StorageServerInterface> ssis;
			ssis.reserve(r.value().src_info.size());
			for(auto& it : r.value().src_info) {
				ssis.push_back(it->interf);
			}
			rep.results.push_back(std::make_pair(r.range(), ssis));
			count++;
		}
	} else {
		int count = 0;
		auto r = commitData->keyInfo.rangeContainingKeyBefore(req.end.get());
		while( count < req.limit && req.begin < r.end() ) {
			vector<StorageServerInterface> ssis;
			ssis.reserve(r.value().src_info.size());
			for(auto& it : r.value().src_info) {
				ssis.push_back(it->interf);
			}
			rep.results.push_back(std::make_pair(r.range(), ssis));
			if(r == commitData->keyInfo.ranges().begin()) {
				break;
			}
			count++;
			--r;
		}
	}
	req.reply.send(rep);
	++commitData->stats.keyServerLocationOut;
	return Void();
}

ACTOR static Future<Void> readRequestServer( MasterProxyInterface proxy, PromiseStream<Future<Void>> addActor, ProxyCommitData* commitData ) {
	loop {
		GetKeyServerLocationsRequest req = waitNext(proxy.getKeyServersLocations.getFuture());
		//WARNING: this code is run at a high priority, so it needs to do as little work as possible
		commitData->stats.addRequest();
		if(req.limit != CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT && //Always do data distribution requests
		   commitData->stats.keyServerLocationIn.getValue() - commitData->stats.keyServerLocationOut.getValue() > SERVER_KNOBS->KEY_LOCATION_MAX_QUEUE_SIZE) {
			++commitData->stats.keyServerLocationErrors;
			req.reply.sendError(proxy_memory_limit_exceeded());
			TraceEvent(SevWarnAlways, "ProxyLocationRequestThresholdExceeded").suppressFor(60);
		} else {
			++commitData->stats.keyServerLocationIn;
			addActor.send(doKeyServerLocationRequest(req, commitData));
		}
	}
}

ACTOR static Future<Void> rejoinServer( MasterProxyInterface proxy, ProxyCommitData* commitData ) {
	// We can't respond to these requests until we have valid txnStateStore
	wait(commitData->validState.getFuture());

	TraceEvent("ProxyReadyForReads", proxy.id());

	loop {
		GetStorageServerRejoinInfoRequest req = waitNext(proxy.getStorageServerRejoinInfo.getFuture());
		if (commitData->txnStateStore->readValue(serverListKeyFor(req.id)).get().present()) {
			GetStorageServerRejoinInfoReply rep;
			rep.version = commitData->version;
			rep.tag = decodeServerTagValue( commitData->txnStateStore->readValue(serverTagKeyFor(req.id)).get().get() );
			Standalone<RangeResultRef> history = commitData->txnStateStore->readRange(serverTagHistoryRangeFor(req.id)).get();
			for(int i = history.size()-1; i >= 0; i-- ) {
				rep.history.push_back(std::make_pair(decodeServerTagHistoryKey(history[i].key), decodeServerTagValue(history[i].value)));
			}
			auto localityKey = commitData->txnStateStore->readValue(tagLocalityListKeyFor(req.dcId)).get();
			rep.newLocality = false;
			if( localityKey.present() ) {
				int8_t locality = decodeTagLocalityListValue(localityKey.get());
				if(rep.tag.locality != tagLocalityUpgraded && locality != rep.tag.locality) {
					TraceEvent(SevWarnAlways, "SSRejoinedWithChangedLocality").detail("Tag", rep.tag.toString()).detail("DcId", req.dcId).detail("NewLocality", locality);
				} else if(locality != rep.tag.locality) {
					uint16_t tagId = 0;
					std::vector<uint16_t> usedTags;
					auto tagKeys = commitData->txnStateStore->readRange(serverTagKeys).get();
					for( auto& kv : tagKeys ) {
						Tag t = decodeServerTagValue( kv.value );
						if(t.locality == locality) {
							usedTags.push_back(t.id);
						}
					}
					auto historyKeys = commitData->txnStateStore->readRange(serverTagHistoryKeys).get();
					for( auto& kv : historyKeys ) {
						Tag t = decodeServerTagValue( kv.value );
						if(t.locality == locality) {
							usedTags.push_back(t.id);
						}
					}
					std::sort(usedTags.begin(), usedTags.end());

					int usedIdx = 0;
					for(; usedTags.size() > 0 && tagId <= usedTags.end()[-1]; tagId++) {
						if(tagId < usedTags[usedIdx]) {
							break;
						} else {
							usedIdx++;
						}
					}
					rep.newTag = Tag(locality, tagId);
				}
			} else if(rep.tag.locality != tagLocalityUpgraded) {
				TraceEvent(SevWarnAlways, "SSRejoinedWithUnknownLocality").detail("Tag", rep.tag.toString()).detail("DcId", req.dcId);
			} else {
				rep.newLocality = true;
				int8_t maxTagLocality = -1;
				auto localityKeys = commitData->txnStateStore->readRange(tagLocalityListKeys).get();
				for( auto& kv : localityKeys ) {
					maxTagLocality = std::max(maxTagLocality, decodeTagLocalityListValue( kv.value ));
				}
				rep.newTag = Tag(maxTagLocality+1,0);
			}
			req.reply.send(rep);
		} else {
			req.reply.sendError(worker_removed());
		}
	}
}

ACTOR Future<Void> healthMetricsRequestServer(MasterProxyInterface proxy, GetHealthMetricsReply* healthMetricsReply, GetHealthMetricsReply* detailedHealthMetricsReply)
{
	loop {
		choose {
			when(GetHealthMetricsRequest req =
				 waitNext(proxy.getHealthMetrics.getFuture()))
			{
				if (req.detailed)
					req.reply.send(*detailedHealthMetricsReply);
				else
					req.reply.send(*healthMetricsReply);
			}
		}
	}
}

ACTOR Future<Void> ddMetricsRequestServer(MasterProxyInterface proxy, Reference<AsyncVar<ServerDBInfo>> db)
{
	loop {
		choose {
			when(state GetDDMetricsRequest req = waitNext(proxy.getDDMetrics.getFuture()))
			{
				ErrorOr<GetDataDistributorMetricsReply> reply = wait(errorOr(db->get().distributor.get().dataDistributorMetrics.getReply(GetDataDistributorMetricsRequest(req.keys, req.shardLimit))));
				if ( reply.isError() ) {
					req.reply.sendError(reply.getError());
				} else {
					GetDDMetricsReply newReply;
					newReply.storageMetricsList = reply.get().storageMetricsList;
					req.reply.send(newReply);
				}
			}
		}
	}
}

ACTOR Future<Void> monitorRemoteCommitted(ProxyCommitData* self) {
	loop {
		wait(delay(0)); //allow this actor to be cancelled if we are removed after db changes.
		state Optional<std::vector<OptionalInterface<TLogInterface>>> remoteLogs;
		if(self->db->get().recoveryState >= RecoveryState::ALL_LOGS_RECRUITED) {
			for(auto& logSet : self->db->get().logSystemConfig.tLogs) {
				if(!logSet.isLocal) {
					remoteLogs = logSet.tLogs;
					for(auto& tLog : logSet.tLogs) {
						if(!tLog.present()) {
							remoteLogs = Optional<std::vector<OptionalInterface<TLogInterface>>>();
							break;
						}
					}
					break;
				}
			}
		}

		if(!remoteLogs.present()) {
			wait(self->db->onChange());
			continue;
		}
		self->popRemoteTxs = true;

		state Future<Void> onChange = self->db->onChange();
		loop {
			state std::vector<Future<TLogQueuingMetricsReply>> replies;
			for(auto &it : remoteLogs.get()) {
				replies.push_back(brokenPromiseToNever( it.interf().getQueuingMetrics.getReply( TLogQueuingMetricsRequest() ) ));
			}
			wait( waitForAll(replies) || onChange );

			if(onChange.isReady()) {
				break;
			}

			//FIXME: use the configuration to calculate a more precise minimum recovery version.
			Version minVersion = std::numeric_limits<Version>::max();
			for(auto& it : replies) {
				minVersion = std::min(minVersion, it.get().v);
			}

			while(self->txsPopVersions.size() && self->txsPopVersions.front().first <= minVersion) {
				self->lastTxsPop = self->txsPopVersions.front().second;
				self->logSystem->popTxs(self->txsPopVersions.front().second, tagLocalityRemoteLog);
				self->txsPopVersions.pop_front();
			}

			wait( delay(SERVER_KNOBS->UPDATE_REMOTE_LOG_VERSION_INTERVAL) || onChange );
			if(onChange.isReady()) {
				break;
			}
		}
	}
}

ACTOR Future<Void> lastCommitUpdater(ProxyCommitData* self, PromiseStream<Future<Void>> addActor) {
	loop {
		double interval = std::max(SERVER_KNOBS->MIN_CONFIRM_INTERVAL, (SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION - self->lastCommitLatency)/2.0);
		double elapsed = now()-self->lastStartCommit;
		if(elapsed < interval) {
			wait( delay(interval + 0.0001 - elapsed) );
		} else {
			if(self->updateCommitRequests < SERVER_KNOBS->MAX_COMMIT_UPDATES) {
				addActor.send(updateLastCommit(self));
			} else {
				TraceEvent(g_network->isSimulated() ? SevInfo : SevWarnAlways, "TooManyLastCommitUpdates").suppressFor(1.0);
				self->lastStartCommit = now();
			}
		}
	}
}

ACTOR Future<Void> proxySnapCreate(ProxySnapRequest snapReq, ProxyCommitData* commitData) {
	TraceEvent("SnapMasterProxy_SnapReqEnter")
		.detail("SnapPayload", snapReq.snapPayload)
		.detail("SnapUID", snapReq.snapUID);
	try {
		// whitelist check
		ExecCmdValueString execArg(snapReq.snapPayload);
		StringRef binPath = execArg.getBinaryPath();
		if (!isWhitelisted(commitData->whitelistedBinPathVec, binPath)) {
			TraceEvent("SnapMasterProxy_WhiteListCheckFailed")
				.detail("SnapPayload", snapReq.snapPayload)
				.detail("SnapUID", snapReq.snapUID);
			throw snap_path_not_whitelisted();
		}
		// db fully recovered check
		if (commitData->db->get().recoveryState != RecoveryState::FULLY_RECOVERED)  {
			// Cluster is not fully recovered and needs TLogs
			// from previous generation for full recovery.
			// Currently, snapshot of old tlog generation is not
			// supported and hence failing the snapshot request until
			// cluster is fully_recovered.
			TraceEvent("SnapMasterProxy_ClusterNotFullyRecovered")
				.detail("SnapPayload", snapReq.snapPayload)
				.detail("SnapUID", snapReq.snapUID);
			throw snap_not_fully_recovered_unsupported();
		}

		auto result =
			commitData->txnStateStore->readValue(LiteralStringRef("log_anti_quorum").withPrefix(configKeysPrefix)).get();
		int logAntiQuorum = 0;
		if (result.present()) {
			logAntiQuorum = atoi(result.get().toString().c_str());
		}
		// FIXME: logAntiQuorum not supported, remove it later,
		// In version2, we probably don't need this limtiation, but this needs to be tested.
		if (logAntiQuorum > 0) {
			TraceEvent("SnapMasterProxy_LogAnitQuorumNotSupported")
				.detail("SnapPayload", snapReq.snapPayload)
				.detail("SnapUID", snapReq.snapUID);
			throw snap_log_anti_quorum_unsupported();
		}

		// send a snap request to DD
		if (!commitData->db->get().distributor.present()) {
			TraceEvent(SevWarnAlways, "DataDistributorNotPresent").detail("Operation", "SnapRequest");
			throw operation_failed();
		}
		state Future<ErrorOr<Void>> ddSnapReq =
			commitData->db->get().distributor.get().distributorSnapReq.tryGetReply(DistributorSnapRequest(snapReq.snapPayload, snapReq.snapUID));
		try {
			wait(throwErrorOr(ddSnapReq));
		} catch (Error& e) {
			TraceEvent("SnapMasterProxy_DDSnapResponseError")
				.detail("SnapPayload", snapReq.snapPayload)
				.detail("SnapUID", snapReq.snapUID)
				.error(e, true /*includeCancelled*/ );
			throw e;
		}
		snapReq.reply.send(Void());
	} catch (Error& e) {
		TraceEvent("SnapMasterProxy_SnapReqError")
			.detail("SnapPayload", snapReq.snapPayload)
			.detail("SnapUID", snapReq.snapUID)
			.error(e, true /*includeCancelled*/);
		if (e.code() != error_code_operation_cancelled) {
			snapReq.reply.sendError(e);
		} else {
			throw e;
		}
	}
	TraceEvent("SnapMasterProxy_SnapReqExit")
		.detail("SnapPayload", snapReq.snapPayload)
		.detail("SnapUID", snapReq.snapUID);
	return Void();
}

ACTOR Future<Void> proxyCheckSafeExclusion(Reference<AsyncVar<ServerDBInfo>> db, ExclusionSafetyCheckRequest req) {
	TraceEvent("SafetyCheckMasterProxyBegin");
	state ExclusionSafetyCheckReply reply(false);
	if (!db->get().distributor.present()) {
		TraceEvent(SevWarnAlways, "DataDistributorNotPresent").detail("Operation", "ExclusionSafetyCheck");
		req.reply.send(reply);
		return Void();
	}
	try {
		state Future<ErrorOr<DistributorExclusionSafetyCheckReply>> safeFuture =
		    db->get().distributor.get().distributorExclCheckReq.tryGetReply(
		        DistributorExclusionSafetyCheckRequest(req.exclusions));
		DistributorExclusionSafetyCheckReply _reply = wait(throwErrorOr(safeFuture));
		reply.safe = _reply.safe;
	} catch (Error& e) {
		TraceEvent("SafetyCheckMasterProxyResponseError").error(e);
		if (e.code() != error_code_operation_cancelled) {
			req.reply.sendError(e);
			return Void();
		} else {
			throw e;
		}
	}
	TraceEvent("SafetyCheckMasterProxyFinish");
	req.reply.send(reply);
	return Void();
}

ACTOR Future<Void> masterProxyServerCore(MasterProxyInterface proxy, MasterInterface master,
                                         Reference<AsyncVar<ServerDBInfo>> db, LogEpoch epoch,
                                         Version recoveryTransactionVersion, bool firstProxy,
                                         std::string whitelistBinPaths, Version readTxnLifetime) {
	state ProxyCommitData commitData(proxy.id(), master, proxy.getConsistentReadVersion, recoveryTransactionVersion,
	                                 proxy.commit, db, firstProxy, readTxnLifetime);

	state Future<Sequence> sequenceFuture = (Sequence)0;
	state PromiseStream< std::pair<vector<CommitTransactionRequest>, int> > batchedCommits;
	state Future<Void> commitBatcherActor;
	state Future<Void> lastCommitComplete = Void();

	state PromiseStream<Future<Void>> addActor;
	state Future<Void> onError = transformError( actorCollection(addActor.getFuture()), broken_promise(), master_tlog_failed() );
	state double lastCommit = 0;
	state std::set<Sequence> txnSequences;
	state Sequence maxSequence = std::numeric_limits<Sequence>::max();

	state GetHealthMetricsReply healthMetricsReply;
	state GetHealthMetricsReply detailedHealthMetricsReply;

	addActor.send( waitFailureServer(proxy.waitFailure.getFuture()) );
	addActor.send( traceRole(Role::MASTER_PROXY, proxy.id()) );

	//TraceEvent("ProxyInit1", proxy.id());

	// Wait until we can load the "real" logsystem, since we don't support switching them currently
	while (!(commitData.db->get().master.id() == master.id() && commitData.db->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION)) {
		//TraceEvent("ProxyInit2", proxy.id()).detail("LSEpoch", db->get().logSystemConfig.epoch).detail("Need", epoch);
		wait(commitData.db->onChange());
	}
	state Future<Void> dbInfoChange = commitData.db->onChange();
	//TraceEvent("ProxyInit3", proxy.id());

	commitData.resolvers = commitData.db->get().resolvers;
	ASSERT(commitData.resolvers.size() != 0);

	auto rs = commitData.keyResolvers.modify(allKeys);
	for(auto r = rs.begin(); r != rs.end(); ++r)
		r->value().emplace_back(0,0);

	commitData.logSystem = ILogSystem::fromServerDBInfo(proxy.id(), commitData.db->get(), false, addActor);
	commitData.logAdapter = new LogSystemDiskQueueAdapter(commitData.logSystem, Reference<AsyncVar<PeekTxsInfo>>(), 1, false);
	commitData.txnStateStore = keyValueStoreLogSystem(commitData.logAdapter, proxy.id(), 2e9, true, true, true);
	createWhitelistBinPathVec(whitelistBinPaths, commitData.whitelistedBinPathVec);

	commitData.updateLatencyBandConfig(commitData.db->get().latencyBandConfig);

	// ((SERVER_MEM_LIMIT * COMMIT_BATCHES_MEM_FRACTION_OF_TOTAL) / COMMIT_BATCHES_MEM_TO_TOTAL_MEM_SCALE_FACTOR) is only a approximate formula for limiting the memory used.
	// COMMIT_BATCHES_MEM_TO_TOTAL_MEM_SCALE_FACTOR is an estimate based on experiments and not an accurate one.
	state int64_t commitBatchesMemoryLimit = std::min(SERVER_KNOBS->COMMIT_BATCHES_MEM_BYTES_HARD_LIMIT, static_cast<int64_t>((SERVER_KNOBS->SERVER_MEM_LIMIT * SERVER_KNOBS->COMMIT_BATCHES_MEM_FRACTION_OF_TOTAL) / SERVER_KNOBS->COMMIT_BATCHES_MEM_TO_TOTAL_MEM_SCALE_FACTOR));
	TraceEvent(SevInfo, "CommitBatchesMemoryLimit").detail("BytesLimit", commitBatchesMemoryLimit);

	addActor.send(monitorRemoteCommitted(&commitData));
	addActor.send(transactionStarter(proxy, commitData.db, addActor, &commitData, &healthMetricsReply, &detailedHealthMetricsReply));
	addActor.send(readRequestServer(proxy, addActor, &commitData));
	addActor.send(rejoinServer(proxy, &commitData));
	addActor.send(healthMetricsRequestServer(proxy, &healthMetricsReply, &detailedHealthMetricsReply));
	addActor.send(ddMetricsRequestServer(proxy, db));

	// wait for txnStateStore recovery
	wait(success(commitData.txnStateStore->readValue(StringRef())));

	if(SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION > 0) {
		addActor.send(lastCommitUpdater(&commitData, addActor));
	}

	int commitBatchByteLimit =
	    (int)std::min<double>(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_MAX,
	                          std::max<double>(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_MIN,
	                                           SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_SCALE_BASE *
	                                               pow(commitData.db->get().client.proxies.size(),
	                                                   SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_SCALE_POWER)));

	commitBatcherActor = commitBatcher(&commitData, batchedCommits, proxy.commit.getFuture(), commitBatchByteLimit, commitBatchesMemoryLimit);
	loop choose{
		when( wait( dbInfoChange ) ) {
			dbInfoChange = commitData.db->onChange();
			if(commitData.db->get().master.id() == master.id() && commitData.db->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION) {
				commitData.logSystem = ILogSystem::fromServerDBInfo(proxy.id(), commitData.db->get(), false, addActor);
				for(auto it : commitData.tag_popped) {
					commitData.logSystem->pop(it.second, it.first);
				}
				commitData.logSystem->popTxs(commitData.lastTxsPop, tagLocalityRemoteLog);
			}

			commitData.updateLatencyBandConfig(commitData.db->get().latencyBandConfig);
		}
		when(wait(onError)) {}
		when(std::pair<vector<CommitTransactionRequest>, int> batchedRequests = waitNext(batchedCommits.getFuture())) {
			//WARNING: this code is run at a high priority, so it needs to do as little work as possible
			const vector<CommitTransactionRequest> &trs = batchedRequests.first;
			int batchBytes = batchedRequests.second;
			//TraceEvent("MasterProxyCTR", proxy.id()).detail("CommitTransactions", trs.size()).detail("TransactionRate", transactionRate).detail("TransactionQueue", transactionQueue.size()).detail("ReleasedTransactionCount", transactionCount);
			if (trs.size() || (commitData.db->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS && now() - lastCommit >= SERVER_KNOBS->MAX_COMMIT_BATCH_INTERVAL)) {
				lastCommit = now();

				if (trs.size() || lastCommitComplete.isReady()) {
					lastCommitComplete = commitBatch(&commitData, trs, batchBytes);
					addActor.send(lastCommitComplete);
				}
			}
		}
		when(GetRawCommittedVersionRequest req = waitNext(proxy.getRawCommittedVersion.getFuture())) {
			//TraceEvent("ProxyGetRCV", proxy.id());
			if (req.debugID.present())
				g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "MasterProxyServer.masterProxyServerCore.GetRawCommittedVersion");
			GetReadVersionReply rep;
			rep.locked = commitData.locked;
			rep.metadataVersion = commitData.metadataVersion;
			rep.version = commitData.committedVersion.get();
			req.reply.send(rep);
		}
		when(ProxySnapRequest snapReq = waitNext(proxy.proxySnapReq.getFuture())) {
			TraceEvent(SevDebug, "SnapMasterEnqueue");
			addActor.send(proxySnapCreate(snapReq, &commitData));
		}
		when(ExclusionSafetyCheckRequest exclCheckReq = waitNext(proxy.exclusionSafetyCheckReq.getFuture())) {
			addActor.send(proxyCheckSafeExclusion(db, exclCheckReq));
		}
		when(state TxnStateRequest req = waitNext(proxy.txnState.getFuture())) {
			state ReplyPromise<Void> reply = req.reply;
			if(req.last) maxSequence = req.sequence + 1;
			if (!txnSequences.count(req.sequence)) {
				txnSequences.insert(req.sequence);

				ASSERT(!commitData.validState.isSet()); // Although we may receive the CommitTransactionRequest for the recovery transaction before all of the TxnStateRequest, we will not get a resolution result from any resolver until the master has submitted its initial (sequence 0) resolution request, which it doesn't do until we have acknowledged all TxnStateRequests

				for(auto& kv : req.data)
					commitData.txnStateStore->set(kv, &req.arena);
				commitData.txnStateStore->commit(true);

				if(txnSequences.size() == maxSequence) {
					state KeyRange txnKeys = allKeys;
					Standalone<RangeResultRef> UIDtoTagMap = commitData.txnStateStore->readRange( serverTagKeys ).get();
					state std::map<Tag, UID> tag_uid;
					for (const KeyValueRef kv : UIDtoTagMap) {
						tag_uid[decodeServerTagValue(kv.value)] = decodeServerTagKey(kv.key);
					}
					loop {
						wait(yield());
						Standalone<RangeResultRef> data = commitData.txnStateStore->readRange(txnKeys, SERVER_KNOBS->BUGGIFIED_ROW_LIMIT, SERVER_KNOBS->APPLY_MUTATION_BYTES).get();
						if(!data.size()) break;
						((KeyRangeRef&)txnKeys) = KeyRangeRef( keyAfter(data.back().key, txnKeys.arena()), txnKeys.end );

						MutationsVec mutations;
						std::vector<std::pair<MapPair<Key,ServerCacheInfo>,int>> keyInfoData;
						vector<UID> src, dest;
						ServerCacheInfo info;
						for(auto &kv : data) {
							if( kv.key.startsWith(keyServersPrefix) ) {
								KeyRef k = kv.key.removePrefix(keyServersPrefix);
								if(k != allKeys.end) {
									decodeKeyServersValue(tag_uid, kv.value, src, dest);
									info.tags.clear();
									info.src_info.clear();
									info.dest_info.clear();
									for (const auto& id : src) {
										auto storageInfo = getStorageInfo(id, &commitData.storageCache, commitData.txnStateStore);
										ASSERT(storageInfo->tag != invalidTag);
										info.tags.push_back( storageInfo->tag );
										info.src_info.push_back( storageInfo );
									}
									for (const auto& id : dest) {
										auto storageInfo = getStorageInfo(id, &commitData.storageCache, commitData.txnStateStore);
										ASSERT(storageInfo->tag != invalidTag);
										info.tags.push_back( storageInfo->tag );
										info.dest_info.push_back( storageInfo );
									}
									uniquify(info.tags);
									keyInfoData.emplace_back(MapPair<Key,ServerCacheInfo>(k, info), 1);
								}
							} else {
								mutations.push_back(mutations.arena(), MutationRef(MutationRef::SetValue, kv.key, kv.value));
							}
						}

						//insert keyTag data separately from metadata mutations so that we can do one bulk insert which avoids a lot of map lookups.
						commitData.keyInfo.rawInsert(keyInfoData);

						Arena arena;
						bool confChanges;
						applyMetadataMutations(commitData.dbgid, arena, mutations, commitData.txnStateStore, nullptr, &confChanges, Reference<ILogSystem>(), 0, &commitData.vecBackupKeys, &commitData.keyInfo, &commitData.cacheInfo, commitData.firstProxy ? &commitData.uid_applyMutationsData : nullptr, commitData.commit, commitData.cx, &commitData.committedVersion, &commitData.storageCache, &commitData.tag_popped, true );
					}

					auto lockedKey = commitData.txnStateStore->readValue(databaseLockedKey).get();
					commitData.locked = lockedKey.present() && lockedKey.get().size();
					commitData.metadataVersion = commitData.txnStateStore->readValue(metadataVersionKey).get();

					commitData.txnStateStore->enableSnapshot();
				}
			}
			addActor.send(broadcastTxnRequest(req, SERVER_KNOBS->TXN_STATE_SEND_AMOUNT, true));
			wait(yield());
		}
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo>> db, uint64_t recoveryCount, MasterProxyInterface myInterface) {
	loop{
		if (db->get().recoveryCount >= recoveryCount && !std::count(db->get().client.proxies.begin(), db->get().client.proxies.end(), myInterface)) {
			throw worker_removed();
		}
		wait(db->onChange());
	}
}

ACTOR Future<Void> masterProxyServer(
	MasterProxyInterface proxy,
	InitializeMasterProxyRequest req,
	Reference<AsyncVar<ServerDBInfo>> db,
	std::string whitelistBinPaths)
{
	try {
		state Future<Void> core =
		    masterProxyServerCore(proxy, req.master, db, req.recoveryCount, req.recoveryTransactionVersion,
		                          req.firstProxy, whitelistBinPaths, req.readTxnLifetime);
		wait(core || checkRemoved(db, req.recoveryCount, proxy));
	}
	catch (Error& e) {
		TraceEvent("MasterProxyTerminated", proxy.id()).error(e, true);

		if (e.code() != error_code_worker_removed && e.code() != error_code_tlog_stopped &&
			e.code() != error_code_master_tlog_failed && e.code() != error_code_coordinators_changed &&
			e.code() != error_code_coordinated_state_conflict && e.code() != error_code_new_coordinators_timed_out) {
			throw;
		}
	}
	return Void();
}
