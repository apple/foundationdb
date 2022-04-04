/*
 * genericactors.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDBRPC_GENERICACTORS_ACTOR_G_H)
#define FDBRPC_GENERICACTORS_ACTOR_G_H
#include "fdbrpc/genericactors.actor.g.h"
#elif !defined(RPCGENERICACTORS_ACTOR_H)
#define RPCGENERICACTORS_ACTOR_H

#include "flow/genericactors.actor.h"
#include "fdbrpc/fdbrpc.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR template <class Req>
Future<REPLY_TYPE(Req)> retryBrokenPromise(RequestStream<Req> to, Req request) {
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
			TEST(true); // retryBrokenPromise
		}
	}
}

ACTOR template <class Req>
Future<REPLY_TYPE(Req)> retryBrokenPromise(RequestStream<Req> to, Req request, TaskPriority taskID) {
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
			TEST(true); // retryBrokenPromise with taskID
		}
	}
}

ACTOR template <class T>
Future<T> timeoutWarning(Future<T> what, double time, PromiseStream<Void> output) {
	state Future<Void> end = delay(time);
	loop choose {
		when(T t = wait(what)) { return t; }
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
			when(wait(signal)) { stream.sendError(connection_failed()); }
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
				when(X x = wait(value)) { return x; }
				when(wait(signal)) { return ErrorOr<X>(request_maybe_delivered()); }
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
	try {
		T t = wait(reply.getFuture());
		FlowTransport::transport().cancelReliable(send);
		return t;
	} catch (Error& e) {
		FlowTransport::transport().cancelReliable(send);
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

Future<Void> disableConnectionFailuresAfter(double const& time, std::string const& context);

#include "flow/unactorcompiler.h"

#endif
