/*
 * GrvQueueDelay.h
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

#include "fdbclient/GrvProxyInterface.h"
#include "GrvProxyTagThrottler.h"
#include "GrvTransactionRateInfo.h"
#include "flow/flow.h"

struct GrvQueueTransactionCounts {
	int64_t systemPriority{ 0 };
	int64_t defaultPriority{ 0 };
	int64_t batchPriority{ 0 };

	void add(TransactionPriority priority, int64_t transactionCount);
	void add(GetReadVersionRequest const& req);

	void remove(TransactionPriority priority, int64_t transactionCount);
	void remove(GetReadVersionRequest const& req);

	void add(GrvProxyTagThrottler::ReleaseTransactionsResult const& releaseStats);

	int64_t normalRateQueuedTransactions() const;
	int64_t batchRateQueuedTransactions() const;
};

struct GrvQueueDelayEstimate {
	double normalRateDelay;
	Optional<double> batchRateDelay;
};

GrvQueueDelayEstimate estimateRemainingGrvQueueDelay(TransactionPriority priority,
                                                     int64_t transactionCount,
                                                     GrvQueueTransactionCounts const& queueTransactionCounts,
                                                     GrvTransactionRateInfo const* normalRateInfo,
                                                     GrvTransactionRateInfo const* batchRateInfo);

bool shouldRejectForMaxGrvQueueDelay(GetReadVersionRequest const& req, double remainingDelay);
