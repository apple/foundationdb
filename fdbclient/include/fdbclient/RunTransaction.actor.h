/*
 * RunTransaction.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_RUNTRANSACTION_ACTOR_G_H)
#define FDBCLIENT_RUNTRANSACTION_ACTOR_G_H
#include "fdbclient/RunTransaction.actor.g.h"
#elif !defined(FDBCLIENT_RUNTRANSACTION_ACTOR_H)
#define FDBCLIENT_RUNTRANSACTION_ACTOR_H

#include <utility>

#include "flow/flow.h"
#include "fdbclient/FDBOptions.g.h"
#include "flow/actorcompiler.h" // This must be the last #include.

template <typename, typename = void>
struct transaction_option_setter : std::false_type {};

template <typename T>
struct transaction_option_setter<Reference<T>> : transaction_option_setter<T> {};

template <typename T>
constexpr bool can_set_transaction_options = transaction_option_setter<T>::value;

ACTOR template <class Function, class DB>
Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue())> runTransaction(
    Reference<DB> db,
    Function func) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	loop {
		if constexpr (can_set_transaction_options<DB>) {
			db->setOptions(tr);
		}

		try {
			// func should be idempotent; otherwise, retry will get undefined result
			state decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue()) result =
			    wait(func(tr));
			wait(safeThreadFutureToFuture(tr->commit()));
			return result;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Function, class DB>
Future<Void> runTransactionVoid(Reference<DB> db, Function func) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	loop {
		if constexpr (can_set_transaction_options<DB>) {
			db->setOptions(tr);
		}
		try {
			// func should be idempotent; otherwise, retry will get undefined result
			wait(func(tr));
			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// SystemTransactionGenerator is a Database-like wrapper which produces transactions which have selected
// options set for lock awareness, reading and optionally writing the system keys, and immediate priority.
// All options are false by default.
template <typename DB>
struct SystemTransactionGenerator : ReferenceCounted<SystemTransactionGenerator<DB>> {
	typedef typename DB::TransactionT TransactionT;

	SystemTransactionGenerator(Reference<DB> db, bool write, bool lockAware, bool immediate)
	  : db(db), write(write), lockAware(lockAware), immediate(immediate) {}

	Reference<TransactionT> createTransaction() const { return db->createTransaction(); }

	void setOptions(Reference<TransactionT> tr) const {
		if (write) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		} else {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		}

		if (immediate) {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		}

		if (lockAware) {
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		}
	}

	Reference<DB> db;
	bool write;
	bool lockAware;
	bool immediate;
};

template <typename DB>
struct transaction_option_setter<SystemTransactionGenerator<DB>> : std::true_type {};

// Convenient wrapper for creating SystemTransactionGenerators.
template <typename DB>
auto SystemDB(Reference<DB> db, bool write = false, bool lockAware = false, bool immediate = false) {
	return makeReference<SystemTransactionGenerator<DB>>(db, write, lockAware, immediate);
}

// SystemDB with all options true
template <typename DB>
auto SystemDBWriteLockedNow(Reference<DB> db) {
	return makeReference<SystemTransactionGenerator<DB>>(db, true, true, true);
}

#include "flow/unactorcompiler.h"
#endif
