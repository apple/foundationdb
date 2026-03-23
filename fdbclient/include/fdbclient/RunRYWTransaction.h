/*
 * RunRYWTransaction.h
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

#include <utility>

#include "flow/flow.h"
#include "fdbclient/RunTransaction.h"
#include "fdbclient/ReadYourWrites.h"

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsubobject-linkage"
#endif

template <class Function>
using RunRYWTransactionResult = decltype(std::declval<Function>()(Reference<ReadYourWritesTransaction>()).getValue());

// Runs a RYW transaction in a retry loop on the given Database.
//
// Takes a function func that accepts a Reference<ReadYourWritesTransaction> as a parameter and returns a non-Void
// Future. This function is run inside the transaction, and when the transaction is successfully committed the result of
// the function is returned.
//
// The supplied function should be idempotent. Otherwise, outcome of this function will depend on how many times the
// transaction is retried.
template <class Function>
Future<RunRYWTransactionResult<Function>> runRYWTransaction(Database cx, Function func) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	while (true) {
		Error err;
		try {
			if constexpr (std::is_same_v<RunRYWTransactionResult<Function>, Void>) {
				co_await func(tr);
				co_await tr->commit();
				co_return;
			} else {
				RunRYWTransactionResult<Function> result = co_await func(tr);
				co_await tr->commit();
				co_return result;
			}
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}
}

// Debug version of runRYWTransaction. It logs the function name and the committed version of the transaction.
// Note the function name is required, e.g., taskFunc->getName() for TaskFuncBase.
template <class Function>
Future<RunRYWTransactionResult<Function>> runRYWTransactionDebug(Database cx, StringRef name, Function func) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	while (true) {
		Error err;
		try {
			// func should be idempodent; otherwise, retry will get undefined result
			if constexpr (std::is_same_v<RunRYWTransactionResult<Function>, Void>) {
				co_await func(tr);
				co_await tr->commit();
				TraceEvent("DebugRunRYWTransaction")
				    .detail("Function", name)
				    .detail("CommitVersion", tr->getCommittedVersion());
				co_return;
			} else {
				RunRYWTransactionResult<Function> result = co_await func(tr);
				co_await tr->commit();
				TraceEvent("DebugRunRYWTransaction")
				    .detail("Function", name)
				    .detail("CommitVersion", tr->getCommittedVersion());
				co_return result;
			}
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}
}

// Runs a RYW transaction in a retry loop on the given Database.
//
// Takes a function func that accepts a Reference<ReadYourWritesTransaction> as a parameter and returns a Void
// Future. This function is run inside the transaction.
//
// The supplied function should be idempotent. Otherwise, outcome of this function will depend on how many times the
// transaction is retried.
template <class Function>
Future<Void> runRYWTransactionVoid(Database cx, Function func) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	while (true) {
		Error err;
		try {
			co_await func(tr);
			co_await tr->commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}
}

template <class Function>
Future<RunRYWTransactionResult<Function>> runRYWTransactionFailIfLocked(Database cx, Function func) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	while (true) {
		Error err;
		try {
			if constexpr (std::is_same_v<RunRYWTransactionResult<Function>, Void>) {
				co_await func(tr);
				co_await tr->commit();
				co_return;
			} else {
				RunRYWTransactionResult<Function> result = co_await func(tr);
				co_await tr->commit();
				co_return result;
			}
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_database_locked)
			throw err;
		co_await tr->onError(err);
	}
}

template <class Function>
Future<RunRYWTransactionResult<Function>> runRYWTransactionNoRetry(Database cx, Function func) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	if constexpr (std::is_same_v<RunRYWTransactionResult<Function>, Void>) {
		co_await func(tr);
		co_await tr->commit();
		co_return;
	} else {
		RunRYWTransactionResult<Function> result = co_await func(tr);
		co_await tr->commit();
		co_return result;
	}
}

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
