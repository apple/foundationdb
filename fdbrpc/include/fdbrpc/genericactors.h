/*
 * genericactors.h
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

#include "flow/genericactors.actor.h"
#include "flow/CodeProbe.h"
#include "flow/Hostname.h"

#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/WellKnownEndpoints.h"

#include <type_traits>

#include "flow/CoroUtils.h"

// To avoid directly access INetworkConnection::net()->removeCachedDNS(), which will require heavy include budget, put
// the call to FlowTransport.cpp as a external function.
extern void removeCachedDNS(const std::string& host, const std::string& service);

template <class Req, bool P>
Future<REPLY_TYPE(Req)> retryBrokenPromise(RequestStream<Req, P> to, Req request) {
	// Like to.getReply(request), except that a broken_promise exception results in retrying request immediately.
	// Suitable for use with well known endpoints, which are likely to return to existence after the other process
	// restarts. Not normally useful for ordinary endpoints, which conventionally are permanently destroyed after
	// replying with broken_promise.
	while (true) {
		try {
			co_return co_await to.getReply(request);
		} catch (Error& e) {
			if (e.code() != error_code_broken_promise)
				throw;
		}
		resetReply(request);
		co_await delayJittered(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
		CODE_PROBE(true, "retryBrokenPromise");
	}
}

template <class Req, bool P>
Future<REPLY_TYPE(Req)> retryBrokenPromise(RequestStream<Req, P> to, Req request, TaskPriority taskID) {
	// Like to.getReply(request), except that a broken_promise exception results in retrying request immediately.
	// Suitable for use with well known endpoints, which are likely to return to existence after the other process
	// restarts. Not normally useful for ordinary endpoints, which conventionally are permanently destroyed after
	// replying with broken_promise.
	while (true) {
		try {
			co_return co_await to.getReply(request, taskID);
		} catch (Error& e) {
			if (e.code() != error_code_broken_promise)
				throw;
		}
		resetReply(request);
		co_await delayJittered(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY, taskID);
		CODE_PROBE(true, "retryBrokenPromise with taskID");
	}
}

template <class Req>
Future<ErrorOr<REPLY_TYPE(Req)>> tryGetReplyFromHostname(Req request, Hostname hostname, WellKnownEndpoints token) {
	// A wrapper of tryGetReply(request), except that the request is sent to an address resolved from a hostname.
	// If resolving fails, return lookup_failed().
	// Otherwise, return tryGetReply(request).
	Optional<NetworkAddress> address = co_await hostname.resolve();
	if (!address.present()) {
		co_return ErrorOr<REPLY_TYPE(Req)>(lookup_failed());
	}
	RequestStream<Req> to(Endpoint::wellKnown({ address.get() }, token));
	ErrorOr<REPLY_TYPE(Req)> reply = co_await to.tryGetReply(request);
	if (reply.isError()) {
		resetReply(request);
		if (reply.getError().code() == error_code_request_maybe_delivered) {
			// Connection failure.
			removeCachedDNS(hostname.host, hostname.service);
		}
	}
	co_return reply;
}

template <class Req>
Future<ErrorOr<REPLY_TYPE(Req)>> tryGetReplyFromHostname(Req request,
                                                         Hostname hostname,
                                                         WellKnownEndpoints token,
                                                         TaskPriority taskID) {
	// A wrapper of tryGetReply(request), except that the request is sent to an address resolved from a hostname.
	// If resolving fails, return lookup_failed().
	// Otherwise, return tryGetReply(request).
	Optional<NetworkAddress> address = co_await hostname.resolve();
	if (!address.present()) {
		co_return ErrorOr<REPLY_TYPE(Req)>(lookup_failed());
	}
	RequestStream<Req> to(Endpoint::wellKnown({ address.get() }, token));
	ErrorOr<REPLY_TYPE(Req)> reply = co_await to.tryGetReply(request, taskID);
	if (reply.isError()) {
		resetReply(request);
		if (reply.getError().code() == error_code_request_maybe_delivered) {
			// Connection failure.
			removeCachedDNS(hostname.host, hostname.service);
		}
	}
	co_return reply;
}

template <class Req>
Future<REPLY_TYPE(Req)> retryGetReplyFromHostname(Req request,
                                                  Hostname hostname,
                                                  WellKnownEndpoints token,
                                                  ExplicitVoid = {}) {
	// Like tryGetReplyFromHostname, except that request_maybe_delivered results in re-resolving the hostname.
	// Suitable for use with hostname, where RequestStream is NOT initialized yet.
	// Not normally useful for endpoints initialized with NetworkAddress.
	double reconnectInterval = FLOW_KNOBS->HOSTNAME_RECONNECT_INIT_INTERVAL;
	std::unique_ptr<RequestStream<Req>> to;
	while (true) {
		NetworkAddress address = co_await hostname.resolveWithRetry();
		if (to == nullptr || to->getEndpoint().getPrimaryAddress() != address) {
			to = std::make_unique<RequestStream<Req>>(Endpoint::wellKnown({ address }, token));
		}
		ErrorOr<REPLY_TYPE(Req)> reply = co_await to->tryGetReply(request);
		if (reply.isError()) {
			resetReply(request);
			if (reply.getError().code() == error_code_request_maybe_delivered) {
				// Connection failure.
				co_await delay(reconnectInterval);
				reconnectInterval = std::min(2 * reconnectInterval, FLOW_KNOBS->HOSTNAME_RECONNECT_MAX_INTERVAL);
				removeCachedDNS(hostname.host, hostname.service);
			} else {
				throw reply.getError();
			}
		} else {
			co_return reply.get();
		}
	}
}

template <class Req>
Future<REPLY_TYPE(Req)> retryGetReplyFromHostname(Req request,
                                                  Hostname hostname,
                                                  WellKnownEndpoints token,
                                                  TaskPriority taskID,
                                                  ExplicitVoid = {}) {
	// Like tryGetReplyFromHostname, except that request_maybe_delivered results in re-resolving the hostname.
	// Suitable for use with hostname, where RequestStream is NOT initialized yet.
	// Not normally useful for endpoints initialized with NetworkAddress.
	double reconnectInitInterval = FLOW_KNOBS->HOSTNAME_RECONNECT_INIT_INTERVAL;
	std::unique_ptr<RequestStream<Req>> to;
	while (true) {
		NetworkAddress address = co_await hostname.resolveWithRetry();
		if (to == nullptr || to->getEndpoint().getPrimaryAddress() != address) {
			to = std::make_unique<RequestStream<Req>>(Endpoint::wellKnown({ address }, token));
		}
		ErrorOr<REPLY_TYPE(Req)> reply = co_await to->tryGetReply(request, taskID);
		if (reply.isError()) {
			resetReply(request);
			if (reply.getError().code() == error_code_request_maybe_delivered) {
				// Connection failure.
				co_await delay(reconnectInitInterval);
				reconnectInitInterval =
				    std::min(2 * reconnectInitInterval, FLOW_KNOBS->HOSTNAME_RECONNECT_MAX_INTERVAL);
				removeCachedDNS(hostname.host, hostname.service);
			} else {
				throw reply.getError();
			}
		} else {
			co_return reply.get();
		}
	}
}

template <class T>
Future<T> timeoutWarning(Future<T> what, double time, PromiseStream<Void> output, ExplicitVoid = {}) {
	Future<Void> end = delay(time);
	while (true) {
		auto res = co_await race(what, end);
		if (res.index() == 0) {
			co_return std::get<0>(std::move(res));
		} else if (res.index() == 1) {
			output.send(Void());
			end = delay(time);
		}
	}
}

template <class T>
Future<Void> forwardPromise(Uncancellable, Promise<T> output, Future<T> input, ExplicitVoid = {}) {
	try {
		T value = co_await input;
		output.send(std::move(value));
	} catch (Error& err) {
		output.sendError(err);
	}
	co_return Void();
}

template <class T>
Future<Void> forwardPromise(Uncancellable, ReplyPromise<T> output, Future<T> input, ExplicitVoid = {}) {
	try {
		T value = co_await input;
		output.send(std::move(value));
	} catch (Error& err) {
		output.sendError(err);
	}
	co_return Void();
}

template <class T>
Future<Void> forwardPromise(Uncancellable, PromiseStream<T> output, Future<T> input, ExplicitVoid = {}) {
	try {
		T value = co_await input;
		output.send(std::move(value));
	} catch (Error& e) {
		output.sendError(e);
	}
	co_return Void();
}

template <class T>
Future<Void> broadcast(Future<T> input, std::vector<Promise<T>> output, ExplicitVoid = {}) {
	T value = co_await input;
	for (int i = 0; i < output.size(); i++) {
		output[i].send(value);
	}
	co_return Void();
}

template <class T>
Future<Void> broadcast(Future<T> input, std::vector<ReplyPromise<T>> output, ExplicitVoid = {}) {
	T value = co_await input;
	for (int i = 0; i < output.size(); i++) {
		output[i].send(value);
	}
	co_return Void();
}

template <class T>
Future<Void> incrementalBroadcast(Future<T> input, std::vector<Promise<T>> output, int batchSize, ExplicitVoid = {}) {
	T value = co_await input;
	for (int i = 0; i < output.size(); i++) {
		output[i].send(value);
		if ((i + 1) % batchSize == 0) {
			co_await delay(0);
		}
	}
	co_return Void();
}

template <class T>
Future<Void> incrementalBroadcast(Future<T> input,
                                  std::vector<ReplyPromise<T>> output,
                                  int batchSize,
                                  ExplicitVoid = {}) {
	T value = co_await input;
	for (int i = 0; i < output.size(); i++) {
		output[i].send(value);
		if ((i + 1) % batchSize == 0) {
			co_await delay(0);
		}
	}
	co_return Void();
}

template <class T>
Future<Void> incrementalBroadcastWithError(Future<T> input,
                                           std::vector<Promise<T>> output,
                                           int batchSize,
                                           ExplicitVoid = {}) {
	int i = 0;
	Error err;
	try {
		T value = co_await input;
		for (; i < output.size(); i++) {
			output[i].send(value);
			if ((i + 1) % batchSize == 0) {
				co_await delay(0);
			}
		}
		co_return Void();
	} catch (Error& e) {
		err = e;
	}
	if (err.code() == error_code_operation_cancelled) {
		throw err;
	}
	for (; i < output.size(); i++) {
		output[i].sendError(err);
		if ((i + 1) % batchSize == 0) {
			co_await delay(0);
		}
	}
	co_return Void();
}

// Needed for the call to endpointNotFound()
#include "fdbrpc/FailureMonitor.h"

struct PeerHolder {
	Reference<Peer> peer;
	explicit PeerHolder(Reference<Peer> peer) : peer(peer) {
		if (peer) {
			peer->outstandingReplies++;
		}
	}
	~PeerHolder() {
		if (peer) {
			peer->outstandingReplies--;
		}
	}
};

// Implements getReplyStream, this coroutine has the same lifetime as the input ReplyPromiseStream.
// Because this coroutine holds a reference to the stream, normally it would be impossible to know when there are no
// other references. To get around this, there is a SAV inside the stream that has one less promise reference than it
// should (caused by getErrorFutureAndDelPromiseRef()). When that SAV gets a broken promise because no one besides this
// coroutine is referencing it, this coroutine will get a broken_promise dropping the final reference to the full
// ReplyPromiseStream
template <class X>
Future<Void> endStreamOnDisconnect(Uncancellable,
                                   Future<Void> signal,
                                   ReplyPromiseStream<X> stream,
                                   Endpoint endpoint,
                                   Reference<Peer> peer = Reference<Peer>()) {
	PeerHolder holder = PeerHolder(peer);
	stream.setRequestStreamEndpoint(endpoint);
	Error err;
	try {
		auto res = co_await race(
		    signal, peer.isValid() ? peer->disconnect.getFuture() : Never(), stream.getErrorFutureAndDelPromiseRef());
		if (res.index() == 0) {
			stream.sendError(connection_failed());
		} else if (res.index() == 1) {
			stream.sendError(connection_failed());
		}
		co_return;
	} catch (Error& e) {
		err = e;
	}
	if (err.code() == error_code_broken_promise) {
		// getErrorFutureAndDelPromiseRef returned, wait on stream connect or error
		if (!stream.connected()) {
			co_await (signal || stream.onConnected());
		}
	}
	// Notify BEFORE dropping last reference, causing broken_promise to send on stream before destructor is called
	stream.notifyFailed();
}

// Implements tryGetReply, getReplyUnlessFailedFor
template <class X>
Future<ErrorOr<X>> waitValueOrSignal(Future<X> value,
                                     Future<Void> signal,
                                     Endpoint endpoint,
                                     ReplyPromise<X> holdme = ReplyPromise<X>(),
                                     Reference<Peer> peer = Reference<Peer>()) {
	PeerHolder holder = PeerHolder(peer);
	while (true) {
		try {
			auto res = co_await race(value, signal, peer.isValid() ? peer->disconnect.getFuture() : Never());
			if (res.index() == 0) {
				X x = std::get<0>(std::move(res));

				co_return x;
			} else if (res.index() == 1) {

				co_return ErrorOr<X>(IFailureMonitor::failureMonitor().knownUnauthorized(endpoint)
				                         ? unauthorized_attempt()
				                         : request_maybe_delivered());
			} else if (res.index() == 2) {
				CODE_PROBE(true, "waitValueOrSignal detected peer disconnect");
				TraceEvent("WaitValueOrSignalPeerDisconnect")
				    .suppressFor(1.0)
				    .detail("Endpoint", endpoint.getPrimaryAddress())
				    .detail("Token", endpoint.token);
				co_return ErrorOr<X>(IFailureMonitor::failureMonitor().knownUnauthorized(endpoint)
				                         ? unauthorized_attempt()
				                         : request_maybe_delivered());
			}
		} catch (Error& e) {
			if (signal.isError()) {
				TraceEvent(SevError, "WaitValueOrSignalError").error(signal.getError());
				co_return ErrorOr<X>(internal_error());
			}

			if (e.code() == error_code_actor_cancelled)
				throw e;

			// broken_promise error normally means an endpoint failure, which in tryGetReply has the same semantics as
			// receiving the failure signal
			if (e.code() != error_code_broken_promise || signal.isError())
				co_return ErrorOr<X>(e);
			IFailureMonitor::failureMonitor().endpointNotFound(endpoint);
			value = Never();
		}
	}
}

template <class T>
Future<T> sendCanceler(ReplyPromise<T> reply, ReliablePacket* send, Endpoint endpoint, ExplicitVoid = {}) {
	bool didCancelReliable = false;
	try {
		while (true) {
			if (IFailureMonitor::failureMonitor().permanentlyFailed(endpoint)) {
				FlowTransport::transport().cancelReliable(send);
				didCancelReliable = true;
				if (IFailureMonitor::failureMonitor().knownUnauthorized(endpoint)) {
					throw unauthorized_attempt();
				} else {
					co_await Future<Void>(Never());
				}
			}
			auto res = co_await race(reply.getFuture(), IFailureMonitor::failureMonitor().onStateChanged(endpoint));
			if (res.index() == 0) {
				T t = std::get<0>(std::move(res));

				FlowTransport::transport().cancelReliable(send);
				didCancelReliable = true;
				co_return t;
			}
		}
	} catch (Error& e) {
		if (!didCancelReliable) {
			FlowTransport::transport().cancelReliable(send);
		}
		if (e.code() == error_code_broken_promise) {
			IFailureMonitor::failureMonitor().endpointNotFound(endpoint);
		}
		throw;
	}
}

template <class X>
Future<X> reportEndpointFailure(Future<X> value, Endpoint endpoint, ExplicitVoid = {}) {
	try {
		co_return co_await value;
	} catch (Error& e) {
		if (e.code() == error_code_broken_promise) {
			IFailureMonitor::failureMonitor().endpointNotFound(endpoint);
		}
		throw;
	}
}
