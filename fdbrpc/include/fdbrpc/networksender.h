/*
 * networksender.h
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

#ifndef FDBRPC_NETWORKSENDER_H
#define FDBRPC_NETWORKSENDER_H
#pragma once

#include "fdbrpc/FlowTransport.h"
#include "flow/flow.h"

template <class T>
struct NetworkSenderTable {
	using type = EnsureTableRef<T>;
};

template <class T>
struct NetworkSenderTable<CachedSerialization<T>> {
	using type = EnsureTable<CachedSerialization<T>>;
};

template <class T>
using NetworkSenderTableT = typename NetworkSenderTable<T>::type;

// Used by FlowTransport to serialize the response to a ReplyPromise across the network.
template <class T>
coro::DetachedCoroutine networkSender(Uncancellable, Future<T> input, const Endpoint* endpoint) {
	try {
		co_await input;
		const T& value = input.get();
		FlowTransport::transport().sendUnreliable(
		    SerializeSource<ErrorOr<NetworkSenderTableT<T>>>(value), *endpoint, false);
	} catch (Error& err) {
		// if (err.code() == error_code_broken_promise) return;
		if (err.code() == error_code_never_reply) {
			co_return;
		}
		ASSERT(err.code() != error_code_actor_cancelled);
		FlowTransport::transport().sendUnreliable(
		    SerializeSource<ErrorOr<NetworkSenderTableT<T>>>(err), *endpoint, false);
	}
	co_return;
}

#endif
