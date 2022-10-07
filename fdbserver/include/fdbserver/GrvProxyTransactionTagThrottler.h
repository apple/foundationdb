#pragma once

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/GrvTransactionRateInfo.h"

// GrvProxyTransactionTagThrottler is used to throttle GetReadVersionRequests based on tag quotas
// before they're pushed into priority-partitioned queues.
//
// A GrvTransactionRateInfo object and a request queue are maintained for each tag.
// The GrvTransactionRateInfo object is used to determine when a request can be released.
//
// Between each set of waits, releaseTransactions is run, releasing queued transactions
// that have passed the tag throttling stage. Transactions that are not yet ready
// are requeued during releaseTransactions.
class GrvProxyTransactionTagThrottler {
	struct DelayedRequest {
		GetReadVersionRequest req;
		double startTime;

		explicit DelayedRequest(GetReadVersionRequest const& req, double startTime = now())
		  : req(req), startTime(startTime) {}
	};

	struct TagQueue {
		Optional<GrvTransactionRateInfo> rateInfo;
		Deque<DelayedRequest> requests;

		explicit TagQueue(double rate = 0.0) : rateInfo(rate) {}

		void releaseTransactions(double elapsed,
		                         SpannedDeque<GetReadVersionRequest>& outBatchPriority,
		                         SpannedDeque<GetReadVersionRequest>& outDefaultPriority);

		void setRate(double rate) {
			if (rateInfo.present()) {
				rateInfo.get().setRate(rate);
			} else {
				rateInfo = GrvTransactionRateInfo(rate);
			}
		}
	};

	// Track the budgets for each tag
	TransactionTagMap<TagQueue> queues;

	// These requests are simply passed through with no throttling
	Deque<GetReadVersionRequest> untaggedRequests;

public:
	// Called with rates received from ratekeeper
	void updateRates(TransactionTagMap<double> const& newRates);

	// elapsed indicates the amount of time since the last epoch was run.
	// If a request is ready to be executed, it is sent to the deque
	// corresponding to its priority. If not, the request remains queued.
	void releaseTransactions(double elapsed,
	                         SpannedDeque<GetReadVersionRequest>& outBatchPriority,
	                         SpannedDeque<GetReadVersionRequest>& outDefaultPriority);

	void addRequest(GetReadVersionRequest const&);
};
