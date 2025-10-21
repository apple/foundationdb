/*
 * genericactors.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDBRPC_GENERICACTORS_ACTOR_G_H)
#define FDBRPC_GENERICACTORS_ACTOR_G_H
#include "fdbrpc/genericactors.actor.g.h"
#elif !defined(RPCGENERICACTORS_ACTOR_H)
#define RPCGENERICACTORS_ACTOR_H

#include "flow/genericactors.actor.h"
#include "flow/CodeProbe.h"
#include "flow/Hostname.h"

#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/WellKnownEndpoints.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// To avoid directly access INetworkConnection::net()->removeCachedDNS(), which will require heavy include budget, put
// the call to FlowTransport.actor.cpp as a external function.
extern void removeCachedDNS(const std::string& host, const std::string& service);

ACTOR template <class Req, bool P>
Future<REPLY_TYPE(Req)> retryBrokenPromise(RequestStream<Req, P> to, Req request) {
	// Like to.getReply(request), except that a broken_promise exception results in retrying request immediately.
	// Suitable for use with well known endpoints, which are likely to return to existence after the other process
	// restarts. Not normally useful for ordinary endpoints, which conventionally are permanently destroyed after
	// replying with broken_promise.
	loop {
		try {
			REPLY_TYPE(Req) reply = wait(to.getReply(request));
			return reply;
		} catch (Error& e) {
			if (e.code() != error_code_broken_promise)
				throw;
			resetReply(request);
			wait(delayJittered(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
			CODE_PROBE(true, "retryBrokenPromise");
		}
	}
}

ACTOR template <class Req, bool P>
Future<REPLY_TYPE(Req)> retryBrokenPromise(RequestStream<Req, P> to, Req request, TaskPriority taskID) {
	// Like to.getReply(request), except that a broken_promise exception results in retrying request immediately.
	// Suitable for use with well known endpoints, which are likely to return to existence after the other process
	// restarts. Not normally useful for ordinary endpoints, which conventionally are permanently destroyed after
	// replying with broken_promise.
	loop {
		try {
			REPLY_TYPE(Req) reply = wait(to.getReply(request, taskID));
			return reply;
		} catch (Error& e) {
			if (e.code() != error_code_broken_promise)
				throw;
			resetReply(request);
			wait(delayJittered(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY, taskID));
			CODE_PROBE(true, "retryBrokenPromise with taskID");
		}
	}
}

ACTOR template <class Req>
Future<Void> tryInitializeRequestStream(RequestStream<Req>* stream, Hostname hostname, WellKnownEndpoints token) {
	Optional<NetworkAddress> address = wait(hostname.resolve());
	if (!address.present()) {
		return Void();
	}
	ASSERT(stream != nullptr);
	*stream = RequestStream<Req>(Endpoint::wellKnown({ address.get() }, token));
	return Void();
}

ACTOR template <class Req>
Future<ErrorOr<REPLY_TYPE(Req)>> tryGetReplyFromHostname(Req request, Hostname hostname, WellKnownEndpoints token) {
	// A wrapper of tryGetReply(request), except that the request is sent to an address resolved from a hostname.
	// If resolving fails, return lookup_failed().
	// Otherwise, return tryGetReply(request).
	Optional<NetworkAddress> address = wait(hostname.resolve());
	if (!address.present()) {
		return ErrorOr<REPLY_TYPE(Req)>(lookup_failed());
	}
	RequestStream<Req> to(Endpoint::wellKnown({ address.get() }, token));
	state ErrorOr<REPLY_TYPE(Req)> reply = wait(to.tryGetReply(request));
	if (reply.isError()) {
		resetReply(request);
		if (reply.getError().code() == error_code_request_maybe_delivered) {
			// Connection failure.
			removeCachedDNS(hostname.host, hostname.service);
		}
	}
	return reply;
}

ACTOR template <class Req>
Future<ErrorOr<REPLY_TYPE(Req)>> tryGetReplyFromHostname(Req request,
                                                         Hostname hostname,
                                                         WellKnownEndpoints token,
                                                         TaskPriority taskID) {
	// A wrapper of tryGetReply(request), except that the request is sent to an address resolved from a hostname.
	// If resolving fails, return lookup_failed().
	// Otherwise, return tryGetReply(request).
	Optional<NetworkAddress> address = wait(hostname.resolve());
	if (!address.present()) {
		return ErrorOr<REPLY_TYPE(Req)>(lookup_failed());
	}
	RequestStream<Req> to(Endpoint::wellKnown({ address.get() }, token));
	state ErrorOr<REPLY_TYPE(Req)> reply = wait(to.tryGetReply(request, taskID));
	if (reply.isError()) {
		resetReply(request);
		if (reply.getError().code() == error_code_request_maybe_delivered) {
			// Connection failure.
			removeCachedDNS(hostname.host, hostname.service);
		}
	}
	return reply;
}

ACTOR template <class Req>
Future<REPLY_TYPE(Req)> retryGetReplyFromHostname(Req request, Hostname hostname, WellKnownEndpoints token) {
	// Like tryGetReplyFromHostname, except that request_maybe_delivered results in re-resolving the hostname.
	// Suitable for use with hostname, where RequestStream is NOT initialized yet.
	// Not normally useful for endpoints initialized with NetworkAddress.
	state double reconnectInterval = FLOW_KNOBS->HOSTNAME_RECONNECT_INIT_INTERVAL;
	state std::unique_ptr<RequestStream<Req>> to;
	loop {
		NetworkAddress address = wait(hostname.resolveWithRetry());
		if (to == nullptr || to->getEndpoint().getPrimaryAddress() != address) {
			to = std::make_unique<RequestStream<Req>>(Endpoint::wellKnown({ address }, token));
		}
		state ErrorOr<REPLY_TYPE(Req)> reply = wait(to->tryGetReply(request));
		if (reply.isError()) {
			resetReply(request);
			if (reply.getError().code() == error_code_request_maybe_delivered) {
				// Connection failure.
				wait(delay(reconnectInterval));
				reconnectInterval = std::min(2 * reconnectInterval, FLOW_KNOBS->HOSTNAME_RECONNECT_MAX_INTERVAL);
				removeCachedDNS(hostname.host, hostname.service);
			} else {
				throw reply.getError();
			}
		} else {
			return reply.get();
		}
	}
}

ACTOR template <class Req>
Future<REPLY_TYPE(Req)> retryGetReplyFromHostname(Req request,
                                                  Hostname hostname,
                                                  WellKnownEndpoints token,
                                                  TaskPriority taskID) {
	// Like tryGetReplyFromHostname, except that request_maybe_delivered results in re-resolving the hostname.
	// Suitable for use with hostname, where RequestStream is NOT initialized yet.
	// Not normally useful for endpoints initialized with NetworkAddress.
	state double reconnectInitInterval = FLOW_KNOBS->HOSTNAME_RECONNECT_INIT_INTERVAL;
	state std::unique_ptr<RequestStream<Req>> to;
	loop {
		NetworkAddress address = wait(hostname.resolveWithRetry());
		if (to == nullptr || to->getEndpoint().getPrimaryAddress() != address) {
			to = std::make_unique<RequestStream<Req>>(Endpoint::wellKnown({ address }, token));
		}
		state ErrorOr<REPLY_TYPE(Req)> reply = wait(to->tryGetReply(request, taskID));
		if (reply.isError()) {
			resetReply(request);
			if (reply.getError().code() == error_code_request_maybe_delivered) {
				// Connection failure.
				wait(delay(reconnectInitInterval));
				reconnectInitInterval =
				    std::min(2 * reconnectInitInterval, FLOW_KNOBS->HOSTNAME_RECONNECT_MAX_INTERVAL);
				removeCachedDNS(hostname.host, hostname.service);
			} else {
				throw reply.getError();
			}
		} else {
			return reply.get();
		}
	}
}

ACTOR template <class T>
Future<T> timeoutWarning(Future<T> what, double time, PromiseStream<Void> output) {
	state Future<Void> end = delay(time);
	loop choose {
		when(T t = wait(what)) {
			return t;
		}
		when(wait(end)) {
			output.send(Void());
			end = delay(time);
		}
	}
}

ACTOR template <class T>
void forwardPromise(Promise<T> output, Future<T> input) {
	try {
		T value = wait(input);
		output.send(value);
	} catch (Error& err) {
		output.sendError(err);
	}
}

ACTOR template <class T>
void forwardPromise(ReplyPromise<T> output, Future<T> input) {
	try {
		T value = wait(input);
		output.send(value);
	} catch (Error& err) {
		output.sendError(err);
	}
}

ACTOR template <class T>
void forwardPromise(PromiseStream<T> output, Future<T> input) {
	try {
		T value = wait(input);
		output.send(value);
	} catch (Error& e) {
		output.sendError(e);
	}
}

ACTOR template <class T>
Future<Void> broadcast(Future<T> input, std::vector<Promise<T>> output) {
	T value = wait(input);
	for (int i = 0; i < output.size(); i++)
		output[i].send(value);
	return Void();
}

ACTOR template <class T>
Future<Void> broadcast(Future<T> input, std::vector<ReplyPromise<T>> output) {
	T value = wait(input);
	for (int i = 0; i < output.size(); i++)
		output[i].send(value);
	return Void();
}

ACTOR template <class T>
Future<Void> incrementalBroadcast(Future<T> input, std::vector<Promise<T>> output, int batchSize) {
	state T value = wait(input);
	state int i = 0;
	for (; i < output.size(); i++) {
		output[i].send(value);
		if ((i + 1) % batchSize == 0) {
			wait(delay(0));
		}
	}
	return Void();
}

ACTOR template <class T>
Future<Void> incrementalBroadcast(Future<T> input, std::vector<ReplyPromise<T>> output, int batchSize) {
	state T value = wait(input);
	state int i = 0;
	for (; i < output.size(); i++) {
		output[i].send(value);
		if ((i + 1) % batchSize == 0) {
			wait(delay(0));
		}
	}
	return Void();
}

ACTOR template <class T>
Future<Void> incrementalBroadcastWithError(Future<T> input, std::vector<Promise<T>> output, int batchSize) {
	state int i = 0;
	try {
		state T value = wait(input);
		for (; i < output.size(); i++) {
			output[i].send(value);
			if ((i + 1) % batchSize == 0) {
				wait(delay(0));
			}
		}
	} catch (Error& _e) {
		if (_e.code() == error_code_operation_cancelled) {
			throw _e;
		}
		state Error e = _e;
		for (; i < output.size(); i++) {
			output[i].sendError(e);
			if ((i + 1) % batchSize == 0) {
				wait(delay(0));
			}
		}
	}
	return Void();
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

// Implements getReplyStream, this a void actor with the same lifetime as the input ReplyPromiseStream.
// Because this actor holds a reference to the stream, normally it would be impossible to know when there are no other
// references. To get around this, there is a SAV inside the stream that has one less promise reference than it should
// (caused by getErrorFutureAndDelPromiseRef()). When that SAV gets a broken promise because no one besides this void
// actor is referencing it, this void actor will get a broken_promise dropping the final reference to the full
// ReplyPromiseStream
ACTOR template <class X>
void endStreamOnDisconnect(Future<Void> signal,
                           ReplyPromiseStream<X> stream,
                           Endpoint endpoint,
                           Reference<Peer> peer = Reference<Peer>()) {
	state PeerHolder holder = PeerHolder(peer);
	stream.setRequestStreamEndpoint(endpoint);
	try {
		choose {
			when(wait(signal)) {
				stream.sendError(connection_failed());
			}
			when(wait(peer.isValid() ? peer->disconnect.getFuture() : Never())) {
				stream.sendError(connection_failed());
			}
			when(wait(stream.getErrorFutureAndDelPromiseRef())) {}
		}
	} catch (Error& e) {
		if (e.code() == error_code_broken_promise) {
			// getErrorFutureAndDelPromiseRef returned, wait on stream connect or error
			if (!stream.connected()) {
				wait(signal || stream.onConnected());
			}
		}
		// Notify BEFORE dropping last reference, causing broken_promise to send on stream before destructor is called
		stream.notifyFailed();
	}
}

// Implements tryGetReply, getReplyUnlessFailedFor
ACTOR template <class X>
Future<ErrorOr<X>> waitValueOrSignal(Future<X> value,
                                     Future<Void> signal,
                                     Endpoint endpoint,
                                     ReplyPromise<X> holdme = ReplyPromise<X>(),
                                     Reference<Peer> peer = Reference<Peer>()) {
	state PeerHolder holder = PeerHolder(peer);
	loop {
		try {
			choose {
				when(X x = wait(value)) {
					return x;
				}
				when(wait(signal)) {
					return ErrorOr<X>(IFailureMonitor::failureMonitor().knownUnauthorized(endpoint)
					                      ? unauthorized_attempt()
					                      : request_maybe_delivered());
				}
			}
		} catch (Error& e) {
			if (signal.isError()) {
				TraceEvent(SevError, "WaitValueOrSignalError").error(signal.getError());
				return ErrorOr<X>(internal_error());
			}

			if (e.code() == error_code_actor_cancelled)
				throw e;

			// broken_promise error normally means an endpoint failure, which in tryGetReply has the same semantics as
			// receiving the failure signal
			if (e.code() != error_code_broken_promise || signal.isError())
				return ErrorOr<X>(e);
			IFailureMonitor::failureMonitor().endpointNotFound(endpoint);
			value = Never();
		}
	}
}

ACTOR template <class T>
Future<T> sendCanceler(ReplyPromise<T> reply, ReliablePacket* send, Endpoint endpoint) {
	state bool didCancelReliable = false;
	try {
		loop {
			if (IFailureMonitor::failureMonitor().permanentlyFailed(endpoint)) {
				FlowTransport::transport().cancelReliable(send);
				didCancelReliable = true;
				if (IFailureMonitor::failureMonitor().knownUnauthorized(endpoint)) {
					throw unauthorized_attempt();
				} else {
					wait(Never());
				}
			}
			choose {
				when(T t = wait(reply.getFuture())) {
					FlowTransport::transport().cancelReliable(send);
					didCancelReliable = true;
					return t;
				}
				when(wait(IFailureMonitor::failureMonitor().onStateChanged(endpoint))) {}
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

ACTOR template <class X>
Future<X> reportEndpointFailure(Future<X> value, Endpoint endpoint) {
	try {
		X x = wait(value);
		return x;
	} catch (Error& e) {
		if (e.code() == error_code_broken_promise) {
			IFailureMonitor::failureMonitor().endpointNotFound(endpoint);
		}
		throw;
	}
}

#include "flow/unactorcompiler.h"

#endif
