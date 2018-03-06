/*
 * genericactors.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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


// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source version.
#if defined(NO_INTELLISENSE) && !defined(FDBRPC_GENERICACTORS_ACTOR_G_H)
	#define FDBRPC_GENERICACTORS_ACTOR_G_H
	#include "genericactors.actor.g.h"
#elif !defined(RPCGENERICACTORS_ACTOR_H)
	#define RPCGENERICACTORS_ACTOR_H

#include "flow/genericactors.actor.h"
#include "fdbrpc.h"

ACTOR template <class Req>
Future<REPLY_TYPE(Req)> retryBrokenPromise( RequestStream<Req> to, Req request ) {
	// Like to.getReply(request), except that a broken_promise exception results in retrying request immediately.
	// Suitable for use with well known endpoints, which are likely to return to existence after the other process restarts.
	// Not normally useful for ordinary endpoints, which conventionally are permanently destroyed after replying with broken_promise.
	loop {
		try {
			REPLY_TYPE(Req) reply = wait( to.getReply( request ) );
			return reply;
		} catch( Error& e ) {
			if (e.code() != error_code_broken_promise)
				throw;
			resetReply( request );
			Void _ = wait( delayJittered(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY) );
			TEST(true); // retryBrokenPromise
		}
	}
}

ACTOR template <class Req>
Future<REPLY_TYPE(Req)> retryBrokenPromise( RequestStream<Req> to, Req request, int taskID ) {
	// Like to.getReply(request), except that a broken_promise exception results in retrying request immediately.
	// Suitable for use with well known endpoints, which are likely to return to existence after the other process restarts.
	// Not normally useful for ordinary endpoints, which conventionally are permanently destroyed after replying with broken_promise.
	loop {
		try {
			REPLY_TYPE(Req) reply = wait( to.getReply( request, taskID ) );
			return reply;
		} catch( Error& e ) {
			if (e.code() != error_code_broken_promise)
				throw;
			resetReply( request );
			Void _ = wait( delayJittered(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY, taskID) );
			TEST(true); // retryBrokenPromise
		}
	}
}

ACTOR template <class T>
Future<T> timeoutWarning( Future<T> what, double time, PromiseStream<Void> output ) {
	state double start = now();
	state Future<Void> end = delay( time );
	loop choose {
		when ( T t = wait( what ) ) { return t; }
		when ( Void _ = wait( end ) ) {
			output.send( Void() );
			end = delay( time ); 
		}
	}
}


ACTOR template <class T> 
Future<T> sendCanceler( ReplyPromise<T> reply, PacketID send ) {
	try {
		T t = wait( reply.getFuture() );
		FlowTransport::transport().cancelReliable(send);
		return t;
	} catch (...) {
		FlowTransport::transport().cancelReliable(send);
		throw;
	}
}

ACTOR template <class T>
void networkSender( Future<T> input, Endpoint endpoint ) {
	try {
		T value = wait( input );
		FlowTransport::transport().sendUnreliable( SerializeBoolAnd<T>(true, value), endpoint );
	} catch (Error& err) {
		//if (err.code() == error_code_broken_promise) return;
		ASSERT( err.code() != error_code_actor_cancelled );
		FlowTransport::transport().sendUnreliable( SerializeBoolAnd<Error>(false, err), endpoint );
	}
}

ACTOR template <class T>
void forwardPromise( Promise<T> output, Future<T> input ) {
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
	}
	catch (Error& err) {
		output.sendError(err);
	}
}

ACTOR template <class T>
void forwardPromise( PromiseStream<T> output, Future<T> input ) {
	try{
		T value = wait(input);
		output.send(value);
	} catch (Error& e) {
		output.sendError(e);
	}
}



ACTOR template <class T> Future<Void> broadcast(Future<T> input, std::vector<Promise<T>> output) {
	T value = wait(input);
	for (int i = 0; i<output.size(); i++)
		output[i].send(value);
	return Void();
}

ACTOR template <class T> Future<Void> broadcast( Future<T> input, std::vector<ReplyPromise<T>> output ) {
	T value = wait( input );
	for(int i=0; i<output.size(); i++)
		output[i].send(value);
	return Void();
}




// Needed for the call to endpointNotFound()
#include "FailureMonitor.h"

// Implements tryGetReply, getReplyUnlessFailedFor
ACTOR template <class X>
Future<ErrorOr<X>> waitValueOrSignal( Future<X> value, Future<Void> signal, Endpoint endpoint, ReplyPromise<X> holdme = ReplyPromise<X>() ) {
	loop {
		try {
			choose {
				when ( X x = wait(value) ) {
					return x; 
				}
				when ( Void _ = wait(signal) ) {
					return ErrorOr<X>(request_maybe_delivered()); 
				}
			}
		} catch (Error& e) {
			if (signal.isError()) {
				TraceEvent(SevError, "WaitValueOrSignalError").error(signal.getError());
				return ErrorOr<X>(internal_error());
			}

			if( e.code() == error_code_actor_cancelled )
				throw e;

			// broken_promise error normally means an endpoint failure, which in tryGetReply has the same semantics as receiving the failure signal
			if (e.code() != error_code_broken_promise || signal.isError())
				return ErrorOr<X>(e);
			
			IFailureMonitor::failureMonitor().endpointNotFound( endpoint );
			value = Never();	
		}
	}
}


Future<Void> disableConnectionFailuresAfter( double const& time, std::string const& context );


#endif
