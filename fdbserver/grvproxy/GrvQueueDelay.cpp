/*
 * GrvQueueDelay.cpp
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

#include "GrvQueueDelay.h"

#include <algorithm>

void GrvQueueTransactionCounts::add(TransactionPriority priority, int64_t transactionCount) {
	ASSERT_GE(transactionCount, 0);
	if (priority >= TransactionPriority::IMMEDIATE) {
		systemPriority += transactionCount;
	} else if (priority >= TransactionPriority::DEFAULT) {
		defaultPriority += transactionCount;
	} else {
		batchPriority += transactionCount;
	}
}

void GrvQueueTransactionCounts::add(GetReadVersionRequest const& req) {
	add(req.priority, req.transactionCount);
}

void GrvQueueTransactionCounts::remove(TransactionPriority priority, int64_t transactionCount) {
	ASSERT_GE(transactionCount, 0);
	if (priority >= TransactionPriority::IMMEDIATE) {
		systemPriority -= transactionCount;
		ASSERT_GE(systemPriority, 0);
	} else if (priority >= TransactionPriority::DEFAULT) {
		defaultPriority -= transactionCount;
		ASSERT_GE(defaultPriority, 0);
	} else {
		batchPriority -= transactionCount;
		ASSERT_GE(batchPriority, 0);
	}
}

void GrvQueueTransactionCounts::remove(GetReadVersionRequest const& req) {
	remove(req.priority, req.transactionCount);
}

void GrvQueueTransactionCounts::add(GrvProxyTagThrottler::ReleaseTransactionsResult const& releaseStats) {
	defaultPriority += static_cast<int64_t>(releaseStats.defaultPriorityTransactionsReleased);
	batchPriority += static_cast<int64_t>(releaseStats.batchPriorityTransactionsReleased);
}

int64_t GrvQueueTransactionCounts::normalRateQueuedTransactions() const {
	return systemPriority + defaultPriority;
}

int64_t GrvQueueTransactionCounts::batchRateQueuedTransactions() const {
	return normalRateQueuedTransactions() + batchPriority;
}

GrvQueueDelayEstimate estimateRemainingGrvQueueDelay(TransactionPriority priority,
                                                     int64_t transactionCount,
                                                     GrvQueueTransactionCounts const& queueTransactionCounts,
                                                     GrvTransactionRateInfo const* normalRateInfo,
                                                     GrvTransactionRateInfo const* batchRateInfo) {
	double normalRateDelay =
	    normalRateInfo->estimateDelay(queueTransactionCounts.normalRateQueuedTransactions(), transactionCount);
	GrvQueueDelayEstimate estimate{ normalRateDelay, Optional<double>() };

	if (priority < TransactionPriority::DEFAULT) {
		estimate.batchRateDelay =
		    batchRateInfo->estimateDelay(queueTransactionCounts.batchRateQueuedTransactions(), transactionCount);
	}

	return estimate;
}

bool shouldRejectForMaxGrvQueueDelay(GetReadVersionRequest const& req, double remainingDelay) {
	if (!req.maxGrvQueueDelayMS.present()) {
		return false;
	}

	double elapsedQueueDelay = now() - req.requestTime() - req.proxyTagThrottledDuration;
	double estimatedQueueDelay = std::max(0.0, elapsedQueueDelay) + remainingDelay;
	return estimatedQueueDelay > req.maxGrvQueueDelayMS.get() / 1000.0;
}
