#pragma once

#include "fdbclient/CommitProxyInterface.h"
#include "fdbserver/GrvTransactionRateInfo.h"

#include <map>

class TagQueue {
	struct DelayedRequest {
		double startTime;
		GetReadVersionRequest req;
		explicit DelayedRequest(GetReadVersionRequest req) : startTime(now()), req(req) {}
	};

	std::map<TransactionTag, GrvTransactionRateInfo> rateInfos;
	std::map<TransactionTag, int64_t> releasedInEpoch;
	Deque<GetReadVersionRequest> newRequests;
	Deque<DelayedRequest> delayedRequests;

	bool canStart(TransactionTag tag, int64_t count) const;
	bool canStart(GetReadVersionRequest req) const;
	void startEpoch();
	void endEpoch(double elapsed);

public:
	void updateRates(std::map<TransactionTag, double> const& newRates);
	void runEpoch(double elapsed,
	              SpannedDeque<GetReadVersionRequest>& outBatchPriority,
	              SpannedDeque<GetReadVersionRequest>& outDefaultPriority,
	              SpannedDeque<GetReadVersionRequest>& outImmediatePriority);
	void addRequest(GetReadVersionRequest);
};
