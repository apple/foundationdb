#pragma once

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/GrvTransactionRateInfo.h"

// TagQueue is used to throttle GetReadVersionRequests based on tag quotas
// before they're pushed into priority-partitioned queues.
//
// A GrvTransactionRateInfo object is maintained for each tag. This object
// is used to determine when a request can be released.
//
// Between each set of waits, runEpoch is run, releasing queued transactions
// that have passed the tag throttling stage. Transactions that are not yet ready
// are requeued during runEpoch.
class TagQueue {
	struct DelayedRequest {
		double startTime;
		GetReadVersionRequest req;
		explicit DelayedRequest(GetReadVersionRequest req) : startTime(now()), req(req) {}
		double delayTime() const { return now() - startTime; }
	};

	// Track the budgets for each tag
	TransactionTagMap<GrvTransactionRateInfo> rateInfos;

	// Requests that have not yet been processed
	Deque<GetReadVersionRequest> newRequests;

	// Requests that have been delayed at least once
	Deque<DelayedRequest> delayedRequests;

	// Checks if count transactions can be released, given that
	// alreadyReleased transactions have already been released in this epoch.
	bool canStart(TransactionTag tag, int64_t alreadyReleased, int64_t count) const;

	// Checks if a request can be released
	bool canStart(GetReadVersionRequest req, TransactionTagMap<int64_t>& releasedInEpoch) const;

public:
	// Called with rates received from ratekeeper
	void updateRates(TransactionTagMap<double> const& newRates);

	// elapsed indicates the amount of time since the last epoch was run.
	// If a request is ready to be executed, it is sent to the deque
	// corresponding to its priority. If not, the request remains queued.
	void runEpoch(double elapsed,
	              SpannedDeque<GetReadVersionRequest>& outBatchPriority,
	              SpannedDeque<GetReadVersionRequest>& outDefaultPriority);

	void addRequest(GetReadVersionRequest);
};
