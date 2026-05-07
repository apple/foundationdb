/*
 * GrvQueueDelayTests.cpp
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

#include "fdbserver/core/Knobs.h"
#include "flow/UnitTest.h"

#include <array>
#include <cmath>
#include <limits>
#include <string_view>

void forceLinkGrvQueueDelayTests() {}

namespace {

constexpr double EXPECTED_RATE = 10.0;

void assertNear(double expected, double actual) {
	ASSERT_LT(std::abs(expected - actual), 1e-9);
}

GrvTransactionRateInfo makeRateInfo() {
	GrvTransactionRateInfo rateInfo(/*rateWindow=*/1.0, /*maxEmptyQueueBudget=*/0.0, EXPECTED_RATE);
	rateInfo.setRate(EXPECTED_RATE);
	rateInfo.startReleaseWindow();
	return rateInfo;
}

double expectedEstimateDelay(int64_t queuedTransactions, int64_t transactionCount) {
	double capacity = std::min(EXPECTED_RATE, SERVER_KNOBS->START_TRANSACTION_MAX_TRANSACTIONS_TO_START);
	double deficit = queuedTransactions + transactionCount - capacity;
	if (deficit <= 0.0) {
		return 0.0;
	}
	return deficit / EXPECTED_RATE;
}

bool expectedShouldReject(Optional<int64_t> maxDelayMS, double elapsedQueueDelay, double remainingDelay) {
	if (!maxDelayMS.present()) {
		return false;
	}
	double estimatedQueueDelay = std::max(0.0, elapsedQueueDelay) + remainingDelay;
	return estimatedQueueDelay > maxDelayMS.get() / 1000.0;
}

} // namespace

TEST_CASE("/fdbserver/grvproxy/maxGrvQueueDelay/queueTransactionCounts") {
	GrvQueueTransactionCounts counts;

	GetReadVersionRequest systemReq;
	systemReq.priority = TransactionPriority::IMMEDIATE;
	systemReq.transactionCount = 2;
	counts.add(systemReq);

	GetReadVersionRequest defaultReq;
	defaultReq.priority = TransactionPriority::DEFAULT;
	defaultReq.transactionCount = 3;
	counts.add(defaultReq);

	GetReadVersionRequest batchReq;
	batchReq.priority = TransactionPriority::BATCH;
	batchReq.transactionCount = 5;
	counts.add(batchReq);

	ASSERT_EQ(counts.systemPriority, 2);
	ASSERT_EQ(counts.defaultPriority, 3);
	ASSERT_EQ(counts.batchPriority, 5);
	ASSERT_EQ(counts.normalRateQueuedTransactions(), 5);
	ASSERT_EQ(counts.batchRateQueuedTransactions(), 10);

	counts.remove(defaultReq);
	ASSERT_EQ(counts.defaultPriority, 0);
	ASSERT_EQ(counts.normalRateQueuedTransactions(), 2);

	GrvProxyTagThrottler::ReleaseTransactionsResult releaseStats;
	releaseStats.defaultPriorityTransactionsReleased = 7;
	releaseStats.batchPriorityTransactionsReleased = 11;
	counts.add(releaseStats);

	ASSERT_EQ(counts.defaultPriority, 7);
	ASSERT_EQ(counts.batchPriority, 16);
	ASSERT_EQ(counts.normalRateQueuedTransactions(), 9);
	ASSERT_EQ(counts.batchRateQueuedTransactions(), 25);

	return Void();
}

TEST_CASE("/fdbserver/grvproxy/maxGrvQueueDelay/remainingDelayEstimate/table") {
	struct Case {
		std::string_view name;
		TransactionPriority priority;
		int64_t systemPriority;
		int64_t defaultPriority;
		int64_t batchPriority;
		int64_t transactionCount;
		bool expectBatchDelay;
	};

	std::array<Case, 6> cases{ {
		{ "default under limit", TransactionPriority::DEFAULT, 4, 5, 50, 1, false },
		{ "default exactly at limit", TransactionPriority::DEFAULT, 5, 4, 50, 1, false },
		{ "default over limit", TransactionPriority::DEFAULT, 5, 5, 50, 1, false },
		{ "batch under both limits", TransactionPriority::BATCH, 4, 5, 0, 1, true },
		{ "batch over batch limit only", TransactionPriority::BATCH, 4, 5, 20, 1, true },
		{ "batch over both limits", TransactionPriority::BATCH, 5, 5, 20, 1, true },
	} };

	for (auto const& c : cases) {
		GrvTransactionRateInfo normalRateInfo = makeRateInfo();
		GrvTransactionRateInfo batchRateInfo = makeRateInfo();
		GrvQueueTransactionCounts counts;
		counts.systemPriority = c.systemPriority;
		counts.defaultPriority = c.defaultPriority;
		counts.batchPriority = c.batchPriority;

		auto estimate =
		    estimateRemainingGrvQueueDelay(c.priority, c.transactionCount, counts, &normalRateInfo, &batchRateInfo);

		assertNear(expectedEstimateDelay(c.systemPriority + c.defaultPriority, c.transactionCount),
		           estimate.normalRateDelay);
		ASSERT_EQ(estimate.batchRateDelay.present(), c.expectBatchDelay);
		if (c.expectBatchDelay) {
			assertNear(
			    expectedEstimateDelay(c.systemPriority + c.defaultPriority + c.batchPriority, c.transactionCount),
			    estimate.batchRateDelay.get());
		}
	}

	return Void();
}

TEST_CASE("/fdbserver/grvproxy/maxGrvQueueDelay/rejectDecision/table") {
	struct Case {
		std::string_view name;
		Optional<int64_t> maxDelayMS;
		double proxyTagThrottledDuration;
		double remainingDelay;
		bool expectedReject;
	};

	std::array<Case, 7> cases{ {
		{ "option absent", Optional<int64_t>(), 3600.0, std::numeric_limits<double>::infinity(), false },
		{ "under threshold", Optional<int64_t>(100), 3600.0, 0.099, false },
		{ "exactly at threshold", Optional<int64_t>(100), 3600.0, 0.100, false },
		{ "over threshold", Optional<int64_t>(100), 3600.0, 0.101, true },
		{ "negative elapsed is clamped", Optional<int64_t>(100), 3600.0, 0.050, false },
		{ "elapsed queue time contributes", Optional<int64_t>(49), -0.010, 0.040, true },
		{ "elapsed queue time under threshold", Optional<int64_t>(100), -0.010, 0.040, false },
	} };

	for (auto const& c : cases) {
		GetReadVersionRequest req;
		req.setRequestTime(now());
		req.maxGrvQueueDelayMS = c.maxDelayMS;
		req.proxyTagThrottledDuration = c.proxyTagThrottledDuration;

		ASSERT_EQ(shouldRejectForMaxGrvQueueDelay(req, c.remainingDelay), c.expectedReject);

		double elapsedLowerBound = c.proxyTagThrottledDuration < 0.0 ? -c.proxyTagThrottledDuration : 0.0;
		ASSERT_EQ(expectedShouldReject(c.maxDelayMS, elapsedLowerBound, c.remainingDelay), c.expectedReject);
	}

	return Void();
}
