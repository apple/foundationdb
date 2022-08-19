/*
 * ConflictSet.h
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

#ifndef CONFLICTSET_H
#define CONFLICTSET_H
#pragma once

#include <utility>
#include <vector>

#include "fdbclient/CommitTransaction.h"

struct ConflictSet;
ConflictSet* newConflictSet();
void clearConflictSet(ConflictSet*, Version);
void destroyConflictSet(ConflictSet*);

struct ConflictBatch {
	explicit ConflictBatch(ConflictSet*,
	                       std::map<int, VectorRef<int>>* conflictingKeyRangeMap = nullptr,
	                       Arena* resolveBatchReplyArena = nullptr);
	~ConflictBatch();

	enum TransactionCommitResult {
		TransactionConflict = 0,
		TransactionTooOld,
		TransactionTenantFailure,
		TransactionCommitted,
	};

	void addTransaction(const CommitTransactionRef& transaction);
	void detectConflicts(Version now,
	                     Version newOldestVersion,
	                     std::vector<int>& nonConflicting,
	                     std::vector<int>* tooOldTransactions = nullptr);
	void GetTooOldTransactions(std::vector<int>& tooOldTransactions);

private:
	ConflictSet* cs;
	Standalone<VectorRef<struct TransactionInfo*>> transactionInfo;
	std::vector<struct KeyInfo> points;
	int transactionCount;
	std::vector<std::pair<StringRef, StringRef>> combinedWriteConflictRanges;
	std::vector<struct ReadConflictRange> combinedReadConflictRanges;
	bool* transactionConflictStatus;
	// Stores the map: a transaction -> conflicted transactions' indices
	std::map<int, VectorRef<int>>* conflictingKeyRangeMap;
	Arena* resolveBatchReplyArena;

	void checkIntraBatchConflicts();
	void combineWriteConflictRanges();
	void checkReadConflictRanges();
	void mergeWriteConflictRanges(Version now);
	void addConflictRanges(Version now,
	                       std::vector<std::pair<StringRef, StringRef>>::iterator begin,
	                       std::vector<std::pair<StringRef, StringRef>>::iterator end,
	                       class SkipList* part);
};

#endif
