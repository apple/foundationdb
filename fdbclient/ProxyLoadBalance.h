/*
 * ProxyLoadBalance.h
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

#include <tuple>
#include <type_traits>
#include <utility>

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/GrvProxyInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/LoadBalance.actor.h"

// Stores constructor arguments so a request can be rebuilt on each retry.
template <class Req, class... Args>
class ReqBuilder {
	std::tuple<Args...> args;

public:
	explicit ReqBuilder(Args... args) : args(std::move(args)...) {}

	Req build() const {
		return std::apply([](auto const&... unpackedArgs) { return Req(unpackedArgs...); }, args);
	}
};

// Infers the stored argument types while ensuring the request is constructible.
template <class Req, class... Args>
ReqBuilder<Req, std::decay_t<Args>...> makeReqBuilder(Args&&... args) {
	static_assert(std::is_constructible_v<Req, std::decay_t<Args>...>);
	return ReqBuilder<Req, std::decay_t<Args>...>(std::forward<Args>(args)...);
}

// Retries a commit-proxy request whenever the proxy set changes before a reply arrives.
template <class Req, class Builder, bool IsPublicStream>
AsyncResult<REPLY_TYPE(Req)> commitProxyLoadBalance(Database cx,
                                                    Builder reqBuilder,
                                                    RequestStream<Req, IsPublicStream> CommitProxyInterface::* channel,
                                                    UseProvisionalProxies useProvisionalProxies,
                                                    TaskPriority taskID,
                                                    AtMostOnce atMostOnce = AtMostOnce::False,
                                                    ExplicitVoid = {}) {
	while (true) {
		Future<REPLY_TYPE(Req)> replyFuture = basicLoadBalance(
		    cx->getCommitProxies(useProvisionalProxies), channel, reqBuilder.build(), taskID, atMostOnce);
		auto res = co_await race(replyFuture, cx->onProxiesChanged());
		if (res.index() == 0) {
			co_return std::get<0>(std::move(res));
		}
	}
}

template <class Req, class Builder, bool IsPublicStream>
AsyncResult<REPLY_TYPE(Req)> commitProxyLoadBalance(Database cx,
                                                    Builder reqBuilder,
                                                    RequestStream<Req, IsPublicStream> CommitProxyInterface::* channel,
                                                    AtMostOnce atMostOnce = AtMostOnce::False,
                                                    ExplicitVoid = {}) {
	return commitProxyLoadBalance(cx, reqBuilder, channel, UseProvisionalProxies::False, cx->taskID, atMostOnce);
}

// Retries a GRV-proxy request whenever the proxy set changes before a reply arrives.
template <class Req, class Builder, bool IsPublicStream>
AsyncResult<REPLY_TYPE(Req)> grvProxyLoadBalance(Database cx,
                                                 Builder reqBuilder,
                                                 RequestStream<Req, IsPublicStream> GrvProxyInterface::* channel,
                                                 AtMostOnce atMostOnce = AtMostOnce::False,
                                                 ExplicitVoid = {}) {
	while (true) {
		Future<REPLY_TYPE(Req)> replyFuture = basicLoadBalance(
		    cx->getGrvProxies(UseProvisionalProxies::False), channel, reqBuilder.build(), cx->taskID, atMostOnce);
		auto res = co_await race(replyFuture, cx->onProxiesChanged());
		if (res.index() == 0) {
			co_return std::get<0>(std::move(res));
		}
	}
}
