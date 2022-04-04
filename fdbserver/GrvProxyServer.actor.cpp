/*
 * GrvProxyServer.actor.cpp
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

#include "fdbclient/Knobs.h"
#include "fdbclient/Notified.h"
#include "fdbclient/TransactionLineage.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/LogSystemDiskQueueAdapter.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/GrvProxyInterface.h"
#include "fdbclient/VersionVector.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbrpc/sim_validation.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct GrvProxyStats {
	CounterCollection cc;
	Counter txnRequestIn, txnRequestOut, txnRequestErrors;
	Counter txnStartIn, txnStartOut, txnStartBatch;
	Counter txnSystemPriorityStartIn, txnSystemPriorityStartOut;
	Counter txnBatchPriorityStartIn, txnBatchPriorityStartOut;
	Counter txnDefaultPriorityStartIn, txnDefaultPriorityStartOut;
	Counter txnThrottled;
	Counter updatesFromRatekeeper, leaseTimeouts;
	int systemGRVQueueSize, defaultGRVQueueSize, batchGRVQueueSize;
	double transactionRateAllowed, batchTransactionRateAllowed;
	double transactionLimit, batchTransactionLimit;
	// how much of the GRV requests queue was processed in one attempt to hand out read version.
	double percentageOfDefaultGRVQueueProcessed;
	double percentageOfBatchGRVQueueProcessed;

	bool lastBatchQueueThrottled;
	bool lastDefaultQueueThrottled;
	double batchThrottleStartTime;
	double defaultThrottleStartTime;

	LatencySample defaultTxnGRVTimeInQueue;
	LatencySample batchTxnGRVTimeInQueue;

	LatencyBands grvLatencyBands;
	LatencySample grvLatencySample; // GRV latency metric sample of default priority
	LatencySample grvBatchLatencySample; // GRV latency metric sample of batched priority

	Future<Void> logger;

	int recentRequests;
	Deque<int> requestBuckets;
	double lastBucketBegin;
	double bucketInterval;
	Reference<Histogram> grvConfirmEpochLiveDist;
	Reference<Histogram> grvGetCommittedVersionRpcDist;

	void updateRequestBuckets() {
		while (now() - lastBucketBegin > bucketInterval) {
			lastBucketBegin += bucketInterval;
			recentRequests -= requestBuckets.front();
			requestBuckets.pop_front();
			requestBuckets.push_back(0);
		}
	}

	void addRequest(int transactionCount) {
		updateRequestBuckets();
		recentRequests += transactionCount;
		requestBuckets.back() += transactionCount;
	}

	int getRecentRequests() {
		updateRequestBuckets();
		return recentRequests /
		       (FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE - (lastBucketBegin + bucketInterval - now()));
	}

	// Current stats maintained for a given grv proxy server
	explicit GrvProxyStats(UID id)
	  : cc("GrvProxyStats", id.toString()),

	    txnRequestIn("TxnRequestIn", cc), txnRequestOut("TxnRequestOut", cc), txnRequestErrors("TxnRequestErrors", cc),
	    txnStartIn("TxnStartIn", cc), txnStartOut("TxnStartOut", cc), txnStartBatch("TxnStartBatch", cc),
	    txnSystemPriorityStartIn("TxnSystemPriorityStartIn", cc),
	    txnSystemPriorityStartOut("TxnSystemPriorityStartOut", cc),
	    txnBatchPriorityStartIn("TxnBatchPriorityStartIn", cc),
	    txnBatchPriorityStartOut("TxnBatchPriorityStartOut", cc),
	    txnDefaultPriorityStartIn("TxnDefaultPriorityStartIn", cc),
	    txnDefaultPriorityStartOut("TxnDefaultPriorityStartOut", cc), txnThrottled("TxnThrottled", cc),
	    updatesFromRatekeeper("UpdatesFromRatekeeper", cc), leaseTimeouts("LeaseTimeouts", cc), systemGRVQueueSize(0),
	    defaultGRVQueueSize(0), batchGRVQueueSize(0), transactionRateAllowed(0), batchTransactionRateAllowed(0),
	    transactionLimit(0), batchTransactionLimit(0), percentageOfDefaultGRVQueueProcessed(0),
	    percentageOfBatchGRVQueueProcessed(0), lastBatchQueueThrottled(false), lastDefaultQueueThrottled(false),
	    batchThrottleStartTime(0.0), defaultThrottleStartTime(0.0),
	    defaultTxnGRVTimeInQueue("DefaultTxnGRVTimeInQueue",
	                             id,
	                             SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                             SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    batchTxnGRVTimeInQueue("BatchTxnGRVTimeInQueue",
	                           id,
	                           SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                           SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    grvLatencyBands("GRVLatencyBands", id, SERVER_KNOBS->STORAGE_LOGGING_DELAY),
	    grvLatencySample("GRVLatencyMetrics",
	                     id,
	                     SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                     SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    grvBatchLatencySample("GRVBatchLatencyMetrics",
	                          id,
	                          SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                          SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    recentRequests(0), lastBucketBegin(now()),
	    bucketInterval(FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE / FLOW_KNOBS->BASIC_LOAD_BALANCE_BUCKETS),
	    grvConfirmEpochLiveDist(Histogram::getHistogram(LiteralStringRef("GrvProxy"),
	                                                    LiteralStringRef("GrvConfirmEpochLive"),
	                                                    Histogram::Unit::microseconds)),
	    grvGetCommittedVersionRpcDist(Histogram::getHistogram(LiteralStringRef("GrvProxy"),
	                                                          LiteralStringRef("GrvGetCommittedVersionRpc"),
	                                                          Histogram::Unit::microseconds)) {
		// The rate at which the limit(budget) is allowed to grow.
		specialCounter(cc, "SystemGRVQueueSize", [this]() { return this->systemGRVQueueSize; });
		specialCounter(cc, "DefaultGRVQueueSize", [this]() { return this->defaultGRVQueueSize; });
		specialCounter(cc, "BatchGRVQueueSize", [this]() { return this->batchGRVQueueSize; });
		specialCounter(
		    cc, "SystemAndDefaultTxnRateAllowed", [this]() { return int64_t(this->transactionRateAllowed); });
		specialCounter(
		    cc, "BatchTransactionRateAllowed", [this]() { return int64_t(this->batchTransactionRateAllowed); });
		specialCounter(cc, "SystemAndDefaultTxnLimit", [this]() { return int64_t(this->transactionLimit); });
		specialCounter(cc, "BatchTransactionLimit", [this]() { return int64_t(this->batchTransactionLimit); });
		specialCounter(cc, "PercentageOfDefaultGRVQueueProcessed", [this]() {
			return int64_t(100 * this->percentageOfDefaultGRVQueueProcessed);
		});
		specialCounter(cc, "PercentageOfBatchGRVQueueProcessed", [this]() {
			return int64_t(100 * this->percentageOfBatchGRVQueueProcessed);
		});

		logger = traceCounters("GrvProxyMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "GrvProxyMetrics");
		for (int i = 0; i < FLOW_KNOBS->BASIC_LOAD_BALANCE_BUCKETS; i++) {
			requestBuckets.push_back(0);
		}
	}
};

struct GrvTransactionRateInfo {
	double rate;
	double limit;
	double budget;

	bool disabled;

	Smoother smoothRate;
	Smoother smoothReleased;

	GrvTransactionRateInfo(double rate)
	  : rate(rate), limit(0), budget(0), disabled(true), smoothRate(SERVER_KNOBS->START_TRANSACTION_RATE_WINDOW),
	    smoothReleased(SERVER_KNOBS->START_TRANSACTION_RATE_WINDOW) {}

	void reset() {
		// Determine the number of transactions that this proxy is allowed to release
		// Roughly speaking, this is done by computing the number of transactions over some historical window that we
		// could have started but didn't, and making that our limit. More precisely, we track a smoothed rate limit and
		// release rate, the difference of which is the rate of additional transactions that we could have released
		// based on that window. Then we multiply by the window size to get a number of transactions.
		//
		// Limit can be negative in the event that we are releasing more transactions than we are allowed (due to the
		// use of our budget or because of higher priority transactions).
		double releaseRate = smoothRate.smoothTotal() - smoothReleased.smoothRate();
		limit = SERVER_KNOBS->START_TRANSACTION_RATE_WINDOW * releaseRate;
	}

	bool canStart(int64_t numAlreadyStarted, int64_t count) {
		return numAlreadyStarted + count <=
		       std::min(limit + budget, SERVER_KNOBS->START_TRANSACTION_MAX_TRANSACTIONS_TO_START);
	}

	void updateBudget(int64_t numStartedAtPriority, bool queueEmptyAtPriority, double elapsed) {
		// Update the budget to accumulate any extra capacity available or remove any excess that was used.
		// The actual delta is the portion of the limit we didn't use multiplied by the fraction of the window that
		// elapsed.
		//
		// We may have exceeded our limit due to the budget or because of higher priority transactions, in which case
		// this delta will be negative. The delta can also be negative in the event that our limit was negative, which
		// can happen if we had already started more transactions in our window than our rate would have allowed.
		//
		// This budget has the property that when the budget is required to start transactions (because batches are
		// big), the sum limit+budget will increase linearly from 0 to the batch size over time and decrease by the
		// batch size upon starting a batch. In other words, this works equivalently to a model where we linearly
		// accumulate budget over time in the case that our batches are too big to take advantage of the window based
		// limits.
		budget = std::max(
		    0.0, budget + elapsed * (limit - numStartedAtPriority) / SERVER_KNOBS->START_TRANSACTION_RATE_WINDOW);

		// If we are emptying out the queue of requests, then we don't need to carry much budget forward
		// If we did keep accumulating budget, then our responsiveness to changes in workflow could be compromised
		if (queueEmptyAtPriority) {
			budget = std::min(budget, SERVER_KNOBS->START_TRANSACTION_MAX_EMPTY_QUEUE_BUDGET);
		}

		smoothReleased.addDelta(numStartedAtPriority);
	}

	void disable() {
		disabled = true;
		// Use smoothRate.setTotal(0) instead of setting rate to 0 so txns will not be throttled immediately.
		smoothRate.setTotal(0);
	}

	void setRate(double rate) {
		ASSERT(rate >= 0 && rate != std::numeric_limits<double>::infinity() && !std::isnan(rate));

		this->rate = rate;
		if (disabled) {
			smoothRate.reset(rate);
			disabled = false;
		} else {
			smoothRate.setTotal(rate);
		}
	}
};

struct GrvProxyData {
	GrvProxyInterface proxy;
	UID dbgid;

	GrvProxyStats stats;
	MasterInterface master;
	RequestStream<GetReadVersionRequest> getConsistentReadVersion;
	Reference<ILogSystem> logSystem;

	Database cx;
	Reference<AsyncVar<ServerDBInfo> const> db;

	Optional<LatencyBandConfig> latencyBandConfig;
	double lastStartCommit;
	double lastCommitLatency;
	LatencySample versionVectorSizeOnGRVReply;
	int updateCommitRequests;
	NotifiedDouble lastCommitTime;

	Version minKnownCommittedVersion; // we should ask master for this version.

	// Cache of the latest commit versions of storage servers.
	VersionVector ssVersionVectorCache;

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

		latencyBandConfig = newLatencyBandConfig;
	}

	GrvProxyData(UID dbgid,
	             MasterInterface master,
	             RequestStream<GetReadVersionRequest> getConsistentReadVersion,
	             Reference<AsyncVar<ServerDBInfo> const> db)
	  : dbgid(dbgid), stats(dbgid), master(master), getConsistentReadVersion(getConsistentReadVersion),
	    cx(openDBOnServer(db, TaskPriority::DefaultEndpoint, LockAware::True)), db(db), lastStartCommit(0),
	    lastCommitLatency(SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION),
	    versionVectorSizeOnGRVReply("VersionVectorSizeOnGRVReply",
	                                dbgid,
	                                SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                                SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    updateCommitRequests(0), lastCommitTime(0), minKnownCommittedVersion(invalidVersion) {}
};

ACTOR Future<Void> healthMetricsRequestServer(GrvProxyInterface grvProxy,
                                              GetHealthMetricsReply* healthMetricsReply,
                                              GetHealthMetricsReply* detailedHealthMetricsReply) {
	loop {
		choose {
			when(GetHealthMetricsRequest req = waitNext(grvProxy.getHealthMetrics.getFuture())) {
				if (req.detailed)
					req.reply.send(*detailedHealthMetricsReply);
				else
					req.reply.send(*healthMetricsReply);
			}
		}
	}
}

// Get transaction rate info from RateKeeper.
ACTOR Future<Void> getRate(UID myID,
                           Reference<AsyncVar<ServerDBInfo> const> db,
                           int64_t* inTransactionCount,
                           int64_t* inBatchTransactionCount,
                           GrvTransactionRateInfo* transactionRateInfo,
                           GrvTransactionRateInfo* batchTransactionRateInfo,
                           GetHealthMetricsReply* healthMetricsReply,
                           GetHealthMetricsReply* detailedHealthMetricsReply,
                           TransactionTagMap<uint64_t>* transactionTagCounter,
                           PrioritizedTransactionTagMap<ClientTagThrottleLimits>* throttledTags,
                           GrvProxyStats* stats) {
	state Future<Void> nextRequestTimer = Never();
	state Future<Void> leaseTimeout = Never();
	state Future<GetRateInfoReply> reply = Never();
	state double lastDetailedReply = 0.0; // request detailed metrics immediately
	state bool expectingDetailedReply = false;
	state int64_t lastTC = 0;

	if (db->get().ratekeeper.present())
		nextRequestTimer = Void();
	loop choose {
		when(wait(db->onChange())) {
			if (db->get().ratekeeper.present()) {
				TraceEvent("ProxyRatekeeperChanged", myID).detail("RKID", db->get().ratekeeper.get().id());
				nextRequestTimer = Void(); // trigger GetRate request
			} else {
				TraceEvent("ProxyRatekeeperDied", myID).log();
				nextRequestTimer = Never();
				reply = Never();
			}
		}
		when(wait(nextRequestTimer)) {
			nextRequestTimer = Never();
			bool detailed = now() - lastDetailedReply > SERVER_KNOBS->DETAILED_METRIC_UPDATE_RATE;

			TransactionTagMap<uint64_t> tagCounts;
			for (auto itr : *throttledTags) {
				for (auto priorityThrottles : itr.second) {
					tagCounts[priorityThrottles.first] = (*transactionTagCounter)[priorityThrottles.first];
				}
			}
			reply = brokenPromiseToNever(db->get().ratekeeper.get().getRateInfo.getReply(GetRateInfoRequest(
			    myID, *inTransactionCount, *inBatchTransactionCount, *transactionTagCounter, detailed)));
			transactionTagCounter->clear();
			expectingDetailedReply = detailed;
		}
		when(GetRateInfoReply rep = wait(reply)) {
			reply = Never();

			transactionRateInfo->setRate(rep.transactionRate);
			batchTransactionRateInfo->setRate(rep.batchTransactionRate);
			stats->transactionRateAllowed = rep.transactionRate;
			stats->batchTransactionRateAllowed = rep.batchTransactionRate;
			++stats->updatesFromRatekeeper;
			//TraceEvent("GrvProxyRate", myID).detail("Rate", rep.transactionRate).detail("BatchRate", rep.batchTransactionRate).detail("Lease", rep.leaseDuration).detail("ReleasedTransactions", *inTransactionCount - lastTC);
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
			if (rep.throttledTags.present()) {
				*throttledTags = std::move(rep.throttledTags.get());
			}
		}
		when(wait(leaseTimeout)) {
			transactionRateInfo->disable();
			batchTransactionRateInfo->disable();
			++stats->leaseTimeouts;
			TraceEvent(SevWarn, "GrvProxyRateLeaseExpired", myID).suppressFor(5.0);
			//TraceEvent("GrvProxyRate", myID).detail("Rate", 0.0).detail("BatchRate", 0.0).detail("Lease", 0);
			leaseTimeout = Never();
		}
	}
}

// Respond with an error to the GetReadVersion request when the GRV limit is hit.
void proxyGRVThresholdExceeded(const GetReadVersionRequest* req, GrvProxyStats* stats) {
	++stats->txnRequestErrors;
	req->reply.sendError(proxy_memory_limit_exceeded());
	if (req->priority == TransactionPriority::IMMEDIATE) {
		TraceEvent(SevWarnAlways, "ProxyGRVThresholdExceededSystem").suppressFor(60);
	} else if (req->priority == TransactionPriority::DEFAULT) {
		TraceEvent(SevWarnAlways, "ProxyGRVThresholdExceededDefault").suppressFor(60);
	} else {
		TraceEvent(SevWarnAlways, "ProxyGRVThresholdExceededBatch").suppressFor(60);
	}
}

// Drop a GetReadVersion request from a queue, by responding an error to the request.
void dropRequestFromQueue(Deque<GetReadVersionRequest>* queue, GrvProxyStats* stats) {
	proxyGRVThresholdExceeded(&queue->front(), stats);
	queue->pop_front();
}

// Put a GetReadVersion request into the queue corresponding to its priority.
ACTOR Future<Void> queueGetReadVersionRequests(Reference<AsyncVar<ServerDBInfo> const> db,
                                               SpannedDeque<GetReadVersionRequest>* systemQueue,
                                               SpannedDeque<GetReadVersionRequest>* defaultQueue,
                                               SpannedDeque<GetReadVersionRequest>* batchQueue,
                                               FutureStream<GetReadVersionRequest> readVersionRequests,
                                               PromiseStream<Void> GRVTimer,
                                               double* lastGRVTime,
                                               double* GRVBatchTime,
                                               FutureStream<double> normalGRVLatency,
                                               GrvProxyStats* stats,
                                               GrvTransactionRateInfo* batchRateInfo,
                                               TransactionTagMap<uint64_t>* transactionTagCounter) {
	getCurrentLineage()->modify(&TransactionLineage::operation) =
	    TransactionLineage::Operation::GetConsistentReadVersion;
	loop choose {
		when(GetReadVersionRequest req = waitNext(readVersionRequests)) {
			// auto lineage = make_scoped_lineage(&TransactionLineage::txID, req.spanContext.first());
			// getCurrentLineage()->modify(&TransactionLineage::txID) =
			// WARNING: this code is run at a high priority, so it needs to do as little work as possible
			bool canBeQueued = true;
			if (stats->txnRequestIn.getValue() - stats->txnRequestOut.getValue() >
			    SERVER_KNOBS->START_TRANSACTION_MAX_QUEUE_SIZE) {
				// When the limit is hit, try to drop requests from the lower priority queues.
				if (req.priority == TransactionPriority::BATCH) {
					canBeQueued = false;
				} else if (req.priority == TransactionPriority::DEFAULT) {
					if (!batchQueue->empty()) {
						dropRequestFromQueue(batchQueue, stats);
						--stats->batchGRVQueueSize;
					} else {
						canBeQueued = false;
					}
				} else {
					if (!batchQueue->empty()) {
						dropRequestFromQueue(batchQueue, stats);
						--stats->batchGRVQueueSize;
					} else if (!defaultQueue->empty()) {
						dropRequestFromQueue(defaultQueue, stats);
						--stats->defaultGRVQueueSize;
					} else {
						canBeQueued = false;
					}
				}
			}
			if (!canBeQueued) {
				proxyGRVThresholdExceeded(&req, stats);
			} else {
				stats->addRequest(req.transactionCount);
				// TODO: check whether this is reasonable to do in the fast path
				for (auto tag : req.tags) {
					(*transactionTagCounter)[tag.first] += tag.second;
				}

				if (req.debugID.present())
					g_traceBatch.addEvent("TransactionDebug",
					                      req.debugID.get().first(),
					                      "GrvProxyServer.queueTransactionStartRequests.Before");

				if (systemQueue->empty() && defaultQueue->empty() && batchQueue->empty()) {
					forwardPromise(GRVTimer,
					               delayJittered(std::max(0.0, *GRVBatchTime - (now() - *lastGRVTime)),
					                             TaskPriority::ProxyGRVTimer));
				}

				if (req.priority >= TransactionPriority::IMMEDIATE) {
					++stats->txnRequestIn;
					stats->txnStartIn += req.transactionCount;
					stats->txnSystemPriorityStartIn += req.transactionCount;
					++stats->systemGRVQueueSize;
					systemQueue->push_back(req);
					// systemQueue->span.addParent(req.spanContext);
				} else if (req.priority >= TransactionPriority::DEFAULT) {
					++stats->txnRequestIn;
					stats->txnStartIn += req.transactionCount;
					stats->txnDefaultPriorityStartIn += req.transactionCount;
					++stats->defaultGRVQueueSize;
					defaultQueue->push_back(req);
					// defaultQueue->span.addParent(req.spanContext);
				} else {
					// Return error for batch_priority GRV requests
					int64_t proxiesCount = std::max((int)db->get().client.grvProxies.size(), 1);
					if (batchRateInfo->rate <= (1.0 / proxiesCount)) {
						req.reply.sendError(batch_transaction_throttled());
						stats->txnThrottled += req.transactionCount;
					} else {
						++stats->txnRequestIn;
						stats->txnStartIn += req.transactionCount;
						stats->txnBatchPriorityStartIn += req.transactionCount;
						++stats->batchGRVQueueSize;
						batchQueue->push_back(req);
						// batchQueue->span.addParent(req.spanContext);
					}
				}
			}
		}
		// dynamic batching monitors reply latencies
		when(double reply_latency = waitNext(normalGRVLatency)) {
			double target_latency = reply_latency * SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION;
			*GRVBatchTime = std::max(
			    SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MIN,
			    std::min(SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MAX,
			             target_latency * SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA +
			                 *GRVBatchTime * (1 - SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA)));
		}
	}
}

ACTOR Future<Void> updateLastCommit(GrvProxyData* self, Optional<UID> debugID = Optional<UID>()) {
	state double confirmStart = now();
	self->lastStartCommit = confirmStart;
	self->updateCommitRequests++;
	wait(self->logSystem->confirmEpochLive(debugID));
	self->updateCommitRequests--;
	self->lastCommitLatency = now() - confirmStart;
	self->lastCommitTime = std::max(self->lastCommitTime.get(), confirmStart);
	return Void();
}

ACTOR Future<Void> lastCommitUpdater(GrvProxyData* self, PromiseStream<Future<Void>> addActor) {
	loop {
		double interval = std::max(SERVER_KNOBS->MIN_CONFIRM_INTERVAL,
		                           (SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION - self->lastCommitLatency) / 2.0);
		double elapsed = now() - self->lastStartCommit;
		if (elapsed < interval) {
			wait(delay(interval + 0.0001 - elapsed));
		} else {
			// May want to change the default value of MAX_COMMIT_UPDATES since we don't have
			if (self->updateCommitRequests < SERVER_KNOBS->MAX_COMMIT_UPDATES) {
				addActor.send(updateLastCommit(self));
			} else {
				TraceEvent(g_network->isSimulated() ? SevInfo : SevWarnAlways, "TooManyLastCommitUpdates")
				    .suppressFor(1.0);
				self->lastStartCommit = now();
			}
		}
	}
}

ACTOR Future<GetReadVersionReply> getLiveCommittedVersion(SpanID parentSpan,
                                                          GrvProxyData* grvProxyData,
                                                          uint32_t flags,
                                                          Optional<UID> debugID,
                                                          int transactionCount,
                                                          int systemTransactionCount,
                                                          int defaultPriTransactionCount,
                                                          int batchPriTransactionCount) {
	// Returns a version which (1) is committed, and (2) is >= the latest version reported committed (by a commit
	// response) when this request was sent (1) The version returned is the committedVersion of some proxy at some point
	// before the request returns, so it is committed. (2) No proxy on our list reported committed a higher version
	// before this request was received, because then its committedVersion would have been higher,
	//     and no other proxy could have already committed anything without first ending the epoch
	state Span span("GP:getLiveCommittedVersion"_loc, parentSpan);
	++grvProxyData->stats.txnStartBatch;

	state double grvStart = now();
	state Future<GetRawCommittedVersionReply> replyFromMasterFuture;
	replyFromMasterFuture = grvProxyData->master.getLiveCommittedVersion.getReply(
	    GetRawCommittedVersionRequest(span.context, debugID, grvProxyData->ssVersionVectorCache.getMaxVersion()),
	    TaskPriority::GetLiveCommittedVersionReply);

	if (!SERVER_KNOBS->ALWAYS_CAUSAL_READ_RISKY && !(flags & GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY)) {
		wait(updateLastCommit(grvProxyData, debugID));
	} else if (SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION > 0 &&
	           now() - SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION > grvProxyData->lastCommitTime.get()) {
		wait(grvProxyData->lastCommitTime.whenAtLeast(now() - SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION));
	}

	state double grvConfirmEpochLive = now();
	grvProxyData->stats.grvConfirmEpochLiveDist->sampleSeconds(grvConfirmEpochLive - grvStart);
	if (debugID.present()) {
		g_traceBatch.addEvent(
		    "TransactionDebug", debugID.get().first(), "GrvProxyServer.getLiveCommittedVersion.confirmEpochLive");
	}

	GetRawCommittedVersionReply repFromMaster = wait(replyFromMasterFuture);
	grvProxyData->minKnownCommittedVersion =
	    std::max(grvProxyData->minKnownCommittedVersion, repFromMaster.minKnownCommittedVersion);
	// TODO add to "status json"
	grvProxyData->ssVersionVectorCache.applyDelta(repFromMaster.ssVersionVectorDelta);
	grvProxyData->stats.grvGetCommittedVersionRpcDist->sampleSeconds(now() - grvConfirmEpochLive);
	GetReadVersionReply rep;
	rep.version = repFromMaster.version;
	rep.locked = repFromMaster.locked;
	rep.metadataVersion = repFromMaster.metadataVersion;
	rep.processBusyTime =
	    FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION *
	    std::min((std::numeric_limits<int>::max() / FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION) - 1,
	             grvProxyData->stats.getRecentRequests());
	rep.processBusyTime += FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION *
	                       (g_network->isSimulated() ? deterministicRandom()->random01()
	                                                 : g_network->networkInfo.metrics.lastRunLoopBusyness);

	if (debugID.present()) {
		g_traceBatch.addEvent(
		    "TransactionDebug", debugID.get().first(), "GrvProxyServer.getLiveCommittedVersion.After");
	}

	grvProxyData->stats.txnStartOut += transactionCount;
	grvProxyData->stats.txnSystemPriorityStartOut += systemTransactionCount;
	grvProxyData->stats.txnDefaultPriorityStartOut += defaultPriTransactionCount;
	grvProxyData->stats.txnBatchPriorityStartOut += batchPriTransactionCount;

	return rep;
}

// Returns the current read version (or minimum known committed version if requested),
// to each request in the provided list. Also check if the request should be throttled.
// Update GRV statistics according to the request's priority.
ACTOR Future<Void> sendGrvReplies(Future<GetReadVersionReply> replyFuture,
                                  std::vector<GetReadVersionRequest> requests,
                                  GrvProxyData* grvProxyData,
                                  GrvProxyStats* stats,
                                  Version minKnownCommittedVersion,
                                  PrioritizedTransactionTagMap<ClientTagThrottleLimits> throttledTags,
                                  int64_t midShardSize = 0) {
	GetReadVersionReply _reply = wait(replyFuture);
	GetReadVersionReply reply = _reply;
	Version replyVersion = reply.version;

	double end = g_network->timer();
	for (GetReadVersionRequest const& request : requests) {
		double duration = end - request.requestTime();
		if (request.priority == TransactionPriority::BATCH) {
			stats->grvBatchLatencySample.addMeasurement(duration);
		}

		if (request.priority == TransactionPriority::DEFAULT) {
			stats->grvLatencySample.addMeasurement(duration);
		}

		if (request.priority >= TransactionPriority::DEFAULT) {
			stats->grvLatencyBands.addMeasurement(duration);
		}

		if (request.flags & GetReadVersionRequest::FLAG_USE_MIN_KNOWN_COMMITTED_VERSION) {
			// Only backup worker may infrequently use this flag.
			reply.version = minKnownCommittedVersion;
		} else {
			reply.version = replyVersion;
		}
		reply.midShardSize = midShardSize;
		reply.tagThrottleInfo.clear();
		grvProxyData->ssVersionVectorCache.getDelta(request.maxVersion, reply.ssVersionVectorDelta);
		grvProxyData->versionVectorSizeOnGRVReply.addMeasurement(reply.ssVersionVectorDelta.size());
		reply.proxyId = grvProxyData->dbgid;

		if (!request.tags.empty()) {
			auto& priorityThrottledTags = throttledTags[request.priority];
			for (auto tag : request.tags) {
				auto tagItr = priorityThrottledTags.find(tag.first);
				if (tagItr != priorityThrottledTags.end()) {
					if (tagItr->second.expiration > now()) {
						if (tagItr->second.tpsRate == std::numeric_limits<double>::max()) {
							TEST(true); // Auto TPS rate is unlimited
						} else {
							TEST(true); // GRV proxy returning tag throttle
							reply.tagThrottleInfo[tag.first] = tagItr->second;
						}
					} else {
						// This isn't required, but we might as well
						TEST(true); // GRV proxy expiring tag throttle
						priorityThrottledTags.erase(tagItr);
					}
				}
			}
		}

		if (stats->lastBatchQueueThrottled) {
			// Check if this throttling has been sustained for a certain amount of time to avoid false positives
			if (now() - stats->batchThrottleStartTime > CLIENT_KNOBS->GRV_SUSTAINED_THROTTLING_THRESHOLD) {
				reply.rkBatchThrottled = true;
			}
		}
		if (stats->lastDefaultQueueThrottled) {
			// Check if this throttling has been sustained for a certain amount of time to avoid false positives
			if (now() - stats->defaultThrottleStartTime > CLIENT_KNOBS->GRV_SUSTAINED_THROTTLING_THRESHOLD) {
				// Consider the batch queue throttled if the default is throttled
				// to deal with a potential lull in activity for that priority.
				// Avoids mistakenly thinking batch is unthrottled while default is still throttled.
				reply.rkBatchThrottled = true;
				reply.rkDefaultThrottled = true;
			}
		}
		request.reply.send(reply);
		++stats->txnRequestOut;
	}

	return Void();
}

ACTOR Future<Void> monitorDDMetricsChanges(int64_t* midShardSize, Reference<AsyncVar<ServerDBInfo> const> db) {
	state Future<Void> nextRequestTimer = Never();
	state Future<GetDataDistributorMetricsReply> nextReply = Never();

	if (db->get().distributor.present())
		nextRequestTimer = Void();
	loop {
		try {
			choose {
				when(wait(db->onChange())) {
					if (db->get().distributor.present()) {
						TraceEvent("DataDistributorChanged", db->get().id)
						    .detail("DDID", db->get().distributor.get().id());
						nextRequestTimer = Void();
					} else {
						TraceEvent("DataDistributorDied", db->get().id);
						nextRequestTimer = Never();
					}
					nextReply = Never();
				}
				when(wait(nextRequestTimer)) {
					nextRequestTimer = Never();
					if (db->get().distributor.present()) {
						nextReply = brokenPromiseToNever(db->get().distributor.get().dataDistributorMetrics.getReply(
						    GetDataDistributorMetricsRequest(normalKeys, CLIENT_KNOBS->TOO_MANY, true)));
					} else
						nextReply = Never();
				}
				when(GetDataDistributorMetricsReply reply = wait(nextReply)) {
					nextReply = Never();
					ASSERT(reply.midShardSize.present());
					*midShardSize = reply.midShardSize.get();
					nextRequestTimer = delay(CLIENT_KNOBS->MID_SHARD_SIZE_MAX_STALENESS);
				}
			}
		} catch (Error& e) {
			TraceEvent("DDMidShardSizeUpdateFail").error(e);
			if (e.code() != error_code_timed_out && e.code() != error_code_dd_not_found)
				throw;
			nextRequestTimer = delay(CLIENT_KNOBS->MID_SHARD_SIZE_MAX_STALENESS);
			nextReply = Never();
		}
	}
}

ACTOR static Future<Void> transactionStarter(GrvProxyInterface proxy,
                                             Reference<AsyncVar<ServerDBInfo> const> db,
                                             PromiseStream<Future<Void>> addActor,
                                             GrvProxyData* grvProxyData,
                                             GetHealthMetricsReply* healthMetricsReply,
                                             GetHealthMetricsReply* detailedHealthMetricsReply) {
	state double lastGRVTime = 0;
	state PromiseStream<Void> GRVTimer;
	state double GRVBatchTime = SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MIN;

	state int64_t transactionCount = 0;
	state int64_t batchTransactionCount = 0;
	state GrvTransactionRateInfo normalRateInfo(10);
	state GrvTransactionRateInfo batchRateInfo(0);

	state SpannedDeque<GetReadVersionRequest> systemQueue("GP:transactionStarterSystemQueue"_loc);
	state SpannedDeque<GetReadVersionRequest> defaultQueue("GP:transactionStarterDefaultQueue"_loc);
	state SpannedDeque<GetReadVersionRequest> batchQueue("GP:transactionStarterBatchQueue"_loc);

	state TransactionTagMap<uint64_t> transactionTagCounter;
	state PrioritizedTransactionTagMap<ClientTagThrottleLimits> throttledTags;

	state PromiseStream<double> normalGRVLatency;
	// state Span span;

	state int64_t midShardSize = SERVER_KNOBS->MIN_SHARD_BYTES;
	getCurrentLineage()->modify(&TransactionLineage::operation) =
	    TransactionLineage::Operation::GetConsistentReadVersion;
	addActor.send(monitorDDMetricsChanges(&midShardSize, db));

	addActor.send(getRate(proxy.id(),
	                      db,
	                      &transactionCount,
	                      &batchTransactionCount,
	                      &normalRateInfo,
	                      &batchRateInfo,
	                      healthMetricsReply,
	                      detailedHealthMetricsReply,
	                      &transactionTagCounter,
	                      &throttledTags,
	                      &grvProxyData->stats));
	addActor.send(queueGetReadVersionRequests(db,
	                                          &systemQueue,
	                                          &defaultQueue,
	                                          &batchQueue,
	                                          proxy.getConsistentReadVersion.getFuture(),
	                                          GRVTimer,
	                                          &lastGRVTime,
	                                          &GRVBatchTime,
	                                          normalGRVLatency.getFuture(),
	                                          &grvProxyData->stats,
	                                          &batchRateInfo,
	                                          &transactionTagCounter));

	while (std::find(db->get().client.grvProxies.begin(), db->get().client.grvProxies.end(), proxy) ==
	       db->get().client.grvProxies.end()) {
		wait(db->onChange());
	}

	ASSERT(db->get().recoveryState >=
	       RecoveryState::ACCEPTING_COMMITS); // else potentially we could return uncommitted read versions from master.
	TraceEvent("GrvProxyReadyForTxnStarts", proxy.id());

	loop {
		waitNext(GRVTimer.getFuture());
		// Select zero or more transactions to start
		double t = now();
		double elapsed = now() - lastGRVTime;
		lastGRVTime = t;

		// Resolve a possible indeterminate multiplication with infinite transaction rate
		if (elapsed == 0) {
			elapsed = 1e-15;
		}

		normalRateInfo.reset();
		batchRateInfo.reset();

		grvProxyData->stats.transactionLimit = normalRateInfo.limit;
		grvProxyData->stats.batchTransactionLimit = batchRateInfo.limit;

		int transactionsStarted[2] = { 0, 0 };
		int systemTransactionsStarted[2] = { 0, 0 };
		int defaultPriTransactionsStarted[2] = { 0, 0 };
		int batchPriTransactionsStarted[2] = { 0, 0 };

		std::vector<std::vector<GetReadVersionRequest>> start(
		    2); // start[0] is transactions starting with !(flags&CAUSAL_READ_RISKY), start[1] is transactions starting
		        // with flags&CAUSAL_READ_RISKY
		Optional<UID> debugID;

		int requestsToStart = 0;

		uint32_t defaultQueueSize = defaultQueue.size();
		uint32_t batchQueueSize = batchQueue.size();
		while (requestsToStart < SERVER_KNOBS->START_TRANSACTION_MAX_REQUESTS_TO_START) {
			SpannedDeque<GetReadVersionRequest>* transactionQueue;
			if (!systemQueue.empty()) {
				transactionQueue = &systemQueue;
			} else if (!defaultQueue.empty()) {
				transactionQueue = &defaultQueue;
			} else if (!batchQueue.empty()) {
				transactionQueue = &batchQueue;
			} else {
				break;
			}
			// transactionQueue->span.swap(span);

			auto& req = transactionQueue->front();
			int tc = req.transactionCount;

			if (req.priority < TransactionPriority::DEFAULT &&
			    !batchRateInfo.canStart(transactionsStarted[0] + transactionsStarted[1], tc)) {
				break;
			} else if (req.priority < TransactionPriority::IMMEDIATE &&
			           !normalRateInfo.canStart(transactionsStarted[0] + transactionsStarted[1], tc)) {
				break;
			}

			if (req.debugID.present()) {
				if (!debugID.present())
					debugID = nondeterministicRandom()->randomUniqueID();
				g_traceBatch.addAttach("TransactionAttachID", req.debugID.get().first(), debugID.get().first());
			}

			transactionsStarted[req.flags & 1] += tc;
			double currentTime = g_network->timer();
			if (req.priority >= TransactionPriority::IMMEDIATE) {
				systemTransactionsStarted[req.flags & 1] += tc;
				--grvProxyData->stats.systemGRVQueueSize;
			} else if (req.priority >= TransactionPriority::DEFAULT) {
				defaultPriTransactionsStarted[req.flags & 1] += tc;
				grvProxyData->stats.defaultTxnGRVTimeInQueue.addMeasurement(currentTime - req.requestTime());
				--grvProxyData->stats.defaultGRVQueueSize;
			} else {
				batchPriTransactionsStarted[req.flags & 1] += tc;
				grvProxyData->stats.batchTxnGRVTimeInQueue.addMeasurement(currentTime - req.requestTime());
				--grvProxyData->stats.batchGRVQueueSize;
			}
			start[req.flags & 1].push_back(std::move(req));
			static_assert(GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY == 1, "Implementation dependent on flag value");
			transactionQueue->pop_front();
			requestsToStart++;
		}
		if (!batchQueue.empty()) {
			if (!grvProxyData->stats.lastBatchQueueThrottled) {
				grvProxyData->stats.lastBatchQueueThrottled = true;
				grvProxyData->stats.batchThrottleStartTime = now();
			}
		} else {
			grvProxyData->stats.lastBatchQueueThrottled = false;
		}
		if (!defaultQueue.empty()) {
			if (!grvProxyData->stats.lastDefaultQueueThrottled) {
				grvProxyData->stats.lastDefaultQueueThrottled = true;
				grvProxyData->stats.defaultThrottleStartTime = now();
			}
		} else {
			grvProxyData->stats.lastDefaultQueueThrottled = false;
		}

		if (!systemQueue.empty() || !defaultQueue.empty() || !batchQueue.empty()) {
			forwardPromise(
			    GRVTimer,
			    delayJittered(SERVER_KNOBS->START_TRANSACTION_BATCH_QUEUE_CHECK_INTERVAL, TaskPriority::ProxyGRVTimer));
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

		normalRateInfo.updateBudget(
		    systemTotalStarted + normalTotalStarted, systemQueue.empty() && defaultQueue.empty(), elapsed);
		batchRateInfo.updateBudget(systemTotalStarted + normalTotalStarted + batchTotalStarted,
		                           systemQueue.empty() && defaultQueue.empty() && batchQueue.empty(),
		                           elapsed);

		if (debugID.present()) {
			g_traceBatch.addEvent("TransactionDebug",
			                      debugID.get().first(),
			                      "GrvProxyServer.transactionStarter.AskLiveCommittedVersionFromMaster");
		}

		int defaultGRVProcessed = 0;
		int batchGRVProcessed = 0;
		for (int i = 0; i < start.size(); i++) {
			if (start[i].size()) {
				Future<GetReadVersionReply> readVersionReply = getLiveCommittedVersion(UID() /*span.context*/,
				                                                                       grvProxyData,
				                                                                       i,
				                                                                       debugID,
				                                                                       transactionsStarted[i],
				                                                                       systemTransactionsStarted[i],
				                                                                       defaultPriTransactionsStarted[i],
				                                                                       batchPriTransactionsStarted[i]);
				addActor.send(sendGrvReplies(readVersionReply,
				                             start[i],
				                             grvProxyData,
				                             &grvProxyData->stats,
				                             grvProxyData->minKnownCommittedVersion,
				                             throttledTags,
				                             midShardSize));

				// Use normal priority transaction's GRV latency to dynamically calculate transaction batching interval.
				if (i == 0) {
					addActor.send(timeReply(readVersionReply, normalGRVLatency));
				}
				defaultGRVProcessed += defaultPriTransactionsStarted[i];
				batchGRVProcessed += batchPriTransactionsStarted[i];
			}
		}
		// span = Span(span.location);

		grvProxyData->stats.percentageOfDefaultGRVQueueProcessed =
		    defaultQueueSize ? (double)defaultGRVProcessed / defaultQueueSize : 1;
		grvProxyData->stats.percentageOfBatchGRVQueueProcessed =
		    batchQueueSize ? (double)batchGRVProcessed / batchQueueSize : 1;
	}
}

ACTOR Future<Void> grvProxyServerCore(GrvProxyInterface proxy,
                                      MasterInterface master,
                                      LifetimeToken masterLifetime,
                                      Reference<AsyncVar<ServerDBInfo> const> db) {
	state GrvProxyData grvProxyData(proxy.id(), master, proxy.getConsistentReadVersion, db);

	state PromiseStream<Future<Void>> addActor;
	state Future<Void> onError = transformError(actorCollection(addActor.getFuture()), broken_promise(), tlog_failed());

	state GetHealthMetricsReply healthMetricsReply;
	state GetHealthMetricsReply detailedHealthMetricsReply;

	addActor.send(waitFailureServer(proxy.waitFailure.getFuture()));
	addActor.send(traceRole(Role::GRV_PROXY, proxy.id()));

	TraceEvent("GrvProxyServerCore", proxy.id())
	    .detail("MasterId", master.id())
	    .detail("MasterLifetime", masterLifetime.toString())
	    .detail("RecoveryCount", db->get().recoveryCount);

	// Wait until we can load the "real" logsystem, since we don't support switching them currently
	while (!(masterLifetime.isEqual(grvProxyData.db->get().masterLifetime) &&
	         grvProxyData.db->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS)) {
		wait(grvProxyData.db->onChange());
	}
	// Do we need to wait for any db info change? Yes. To update latency band.
	state Future<Void> dbInfoChange = grvProxyData.db->onChange();
	grvProxyData.logSystem = ILogSystem::fromServerDBInfo(proxy.id(), grvProxyData.db->get(), false, addActor);

	grvProxyData.updateLatencyBandConfig(grvProxyData.db->get().latencyBandConfig);

	addActor.send(transactionStarter(
	    proxy, grvProxyData.db, addActor, &grvProxyData, &healthMetricsReply, &detailedHealthMetricsReply));
	addActor.send(healthMetricsRequestServer(proxy, &healthMetricsReply, &detailedHealthMetricsReply));

	if (SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION > 0) {
		addActor.send(lastCommitUpdater(&grvProxyData, addActor));
	}

	loop choose {
		when(wait(dbInfoChange)) {
			dbInfoChange = grvProxyData.db->onChange();

			if (masterLifetime.isEqual(grvProxyData.db->get().masterLifetime) &&
			    grvProxyData.db->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION) {
				grvProxyData.logSystem =
				    ILogSystem::fromServerDBInfo(proxy.id(), grvProxyData.db->get(), false, addActor);
			}
			grvProxyData.updateLatencyBandConfig(grvProxyData.db->get().latencyBandConfig);
		}
		when(wait(onError)) {}
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo> const> db,
                                uint64_t recoveryCount,
                                GrvProxyInterface myInterface) {
	loop {
		if (db->get().recoveryCount >= recoveryCount &&
		    !std::count(db->get().client.grvProxies.begin(), db->get().client.grvProxies.end(), myInterface)) {
			throw worker_removed();
		}
		wait(db->onChange());
	}
}

ACTOR Future<Void> grvProxyServer(GrvProxyInterface proxy,
                                  InitializeGrvProxyRequest req,
                                  Reference<AsyncVar<ServerDBInfo> const> db) {
	try {
		state Future<Void> core = grvProxyServerCore(proxy, req.master, req.masterLifetime, db);
		wait(core || checkRemoved(db, req.recoveryCount, proxy));
	} catch (Error& e) {
		TraceEvent("GrvProxyTerminated", proxy.id()).errorUnsuppressed(e);

		if (e.code() != error_code_worker_removed && e.code() != error_code_tlog_stopped &&
		    e.code() != error_code_tlog_failed && e.code() != error_code_coordinators_changed &&
		    e.code() != error_code_coordinated_state_conflict && e.code() != error_code_new_coordinators_timed_out) {
			throw;
		}
	}
	return Void();
}
