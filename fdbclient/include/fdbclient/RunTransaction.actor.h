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
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR template <class Function, class DB>
Future<decltype(std::declval<Function>()(Reference<typename DB::TransactionT>()).getValue())> runTransaction(
    Reference<DB> db,
    Function func) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	loop {
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

#include "flow/unactorcompiler.h"
#endif
