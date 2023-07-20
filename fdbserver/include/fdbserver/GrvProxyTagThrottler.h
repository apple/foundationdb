/*
 * GrvProxyTagThrottler.h
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

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/GrvTransactionRateInfo.h"
#include "fdbserver/LatencyBandsMap.h"

// GrvProxyTagThrottler is used to throttle GetReadVersionRequests based on tag quotas
// before they're pushed into priority-partitioned queues.
//
// A GrvTransactionRateInfo object and a request queue are maintained for each tag.
// The GrvTransactionRateInfo object is used to determine when a request can be released.
//
// Between each set of waits, releaseTransactions is run, releasing queued transactions
// that have passed the tag throttling stage. Transactions that are not yet ready
// are requeued during releaseTransactions.
class GrvProxyTagThrottler {
	class DelayedRequest {
		static uint64_t lastSequenceNumber;
		double startTime;

	public:
		GetReadVersionRequest req;
		uint64_t sequenceNumber;

		explicit DelayedRequest(GetReadVersionRequest const& req)
		  : req(req), startTime(now()), sequenceNumber(++lastSequenceNumber) {}

		void updateProxyTagThrottledDuration(LatencyBandsMap&);
		bool isMaxThrottled(double maxThrottleDuration) const;
	};

	struct TagQueue {
		Optional<GrvTransactionRateInfo> rateInfo;
		Deque<DelayedRequest> requests;

		TagQueue() = default;
		explicit TagQueue(double rate)
		  : rateInfo(GrvTransactionRateInfo(SERVER_KNOBS->TAG_THROTTLE_RATE_WINDOW,
		                                    SERVER_KNOBS->TAG_THROTTLE_MAX_EMPTY_QUEUE_BUDGET,
		                                    rate)) {}

		void setRate(double rate);
		bool isMaxThrottled(double maxThrottleDuration) const;
		void rejectRequests(LatencyBandsMap&);
		void endReleaseWindow(int64_t numStarted, double elapsed);
	};

	// Track the budgets for each tag
	TransactionTagMap<TagQueue> queues;
	double maxThrottleDuration;

	// Track latency bands for each tag
	LatencyBandsMap latencyBandsMap;

public:
	explicit GrvProxyTagThrottler(double maxThrottleDuration);

	// Called with rates received from ratekeeper
	void updateRates(TransactionTagMap<double> const& newRates);

	// elapsed indicates the amount of time since the last epoch was run.
	// If a request is ready to be executed, it is sent to the deque
	// corresponding to its priority. If not, the request remains queued.
	void releaseTransactions(double elapsed,
	                         Deque<GetReadVersionRequest>& outBatchPriority,
	                         Deque<GetReadVersionRequest>& outDefaultPriority);

	void addRequest(GetReadVersionRequest const&);

	void addLatencyBandThreshold(double value);

public: // testing
	// Returns number of tags tracked
	uint32_t size() const;
};
