/*
 * fdbrpc.h
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

#ifndef FDBRPC_FDBRPC_H
#define FDBRPC_FDBRPC_H
#pragma once

#include "flow/flow.h"
#include "FlowTransport.h" // NetworkMessageReceiver Endpoint
#include "FailureMonitor.h"


struct FlowReceiver : private NetworkMessageReceiver {
	// Common endpoint code for NetSAV<> and NetNotifiedQueue<>

	Endpoint endpoint;
	bool m_isLocalEndpoint;

	FlowReceiver() : m_isLocalEndpoint(false) {}
	FlowReceiver(Endpoint const& remoteEndpoint) : endpoint(remoteEndpoint), m_isLocalEndpoint(false) {}
	~FlowReceiver() {
		if (m_isLocalEndpoint)
			FlowTransport::transport().removeEndpoint(endpoint, this);
	}

	bool isLocalEndpoint() { return m_isLocalEndpoint; }
	bool isRemoteEndpoint() { return endpoint.isValid() && !m_isLocalEndpoint; }

	// If already a remote endpoint, returns that.  Otherwise makes this
	//   a local endpoint and returns that.
	const Endpoint& getEndpoint(int taskID) {
		if (!endpoint.isValid()) {
			m_isLocalEndpoint = true;
			FlowTransport::transport().addEndpoint(endpoint, this, taskID);
		}
		return endpoint;
	}

	void makeWellKnownEndpoint(Endpoint::Token token, int taskID) {
		ASSERT(!endpoint.isValid());
		m_isLocalEndpoint = true;
		endpoint.token = token;
		FlowTransport::transport().addWellKnownEndpoint(endpoint, this, taskID);
	}
};

template <class T>
struct NetSAV : SAV<T>, FlowReceiver, FastAllocated<NetSAV<T>> {
	using FastAllocated<NetSAV<T>>::operator new;
	using FastAllocated<NetSAV<T>>::operator delete;

	NetSAV(int futures, int promises) : SAV<T>(futures, promises) {}
	NetSAV(int futures, int promises, const Endpoint& remoteEndpoint) : SAV<T>(futures, promises), FlowReceiver(remoteEndpoint) {}

	virtual void destroy() { delete this; }
	virtual void receive(ArenaReader& reader) {
		if (!SAV<T>::canBeSet()) return;  // load balancing and retries can result in the same request being answered twice
		this->addPromiseRef();
		bool ok;
		reader >> ok;
		if (ok) {
			T message;
			reader >> message;
			SAV<T>::sendAndDelPromiseRef(message);
		}
		else {
			Error error;
			reader >> error;
			SAV<T>::sendErrorAndDelPromiseRef(error);
		}
	}
};



template <class T>
class ReplyPromise sealed
{
public:
	template <class U>
	void send(U && value) const {
		sav->send(std::forward<U>(value));
	}
	template <class E>
	void sendError(const E& exc) const { sav->sendError(exc); }

	Future<T> getFuture() const { sav->addFutureRef(); return Future<T>(sav); }
	bool isSet() { return sav->isSet(); }
	bool isValid() const { return sav != NULL; }
	ReplyPromise() : sav(new NetSAV<T>(0, 1)) {}
	ReplyPromise(const ReplyPromise& rhs) : sav(rhs.sav) { sav->addPromiseRef(); }
	ReplyPromise(ReplyPromise&& rhs) noexcept(true) : sav(rhs.sav) { rhs.sav = 0; }
	~ReplyPromise() { if (sav) sav->delPromiseRef(); }

	ReplyPromise(const Endpoint& endpoint) : sav(new NetSAV<T>(0, 1, endpoint)) {}
	const Endpoint& getEndpoint(int taskID = TaskDefaultPromiseEndpoint) const { return sav->getEndpoint(taskID); }

	void operator=(const ReplyPromise& rhs) {
		if (rhs.sav) rhs.sav->addPromiseRef();
		if (sav) sav->delPromiseRef();
		sav = rhs.sav;
	}
	void operator=(ReplyPromise && rhs) noexcept(true) {
		if (sav != rhs.sav) {
			if (sav) sav->delPromiseRef();
			sav = rhs.sav;
			rhs.sav = 0;
		}
	}
	void reset() {
		*this = ReplyPromise<T>();
	}
	void swap(ReplyPromise& other) {
		std::swap(sav, other.sav);
	}

	// Beware, these operations are very unsafe
	SAV<T>* extractRawPointer() { auto ptr = sav; sav = NULL; return ptr; }
	explicit ReplyPromise<T>(SAV<T>* ptr) : sav(ptr) {}

	int getFutureReferenceCount() const { return sav->getFutureReferenceCount(); }
	int getPromiseReferenceCount() const { return sav->getPromiseReferenceCount(); }

private:
	NetSAV<T> *sav;
};

template <class Ar, class T>
void save(Ar& ar, const ReplyPromise<T>& value) {
	auto const& ep = value.getEndpoint();
	ar << ep;
	ASSERT(!ep.address.isValid() || ep.address.isPublic()); // No re-serializing non-public addresses (the reply connection won't be available to any other process)
}

template <class Ar, class T>
void load(Ar& ar, ReplyPromise<T>& value) {
	Endpoint endpoint;
	FlowTransport::transport().loadEndpoint(ar, endpoint);
	value = ReplyPromise<T>(endpoint);
	networkSender(value.getFuture(), endpoint);
}


template <class Reply>
ReplyPromise<Reply> const& getReplyPromise(ReplyPromise<Reply> const& p) { return p; }



template <class Request>
void resetReply(Request& r) { r.reply.reset(); }

template <class Reply>
void resetReply(ReplyPromise<Reply> & p) { p.reset(); }

template <class Request>
void resetReply(Request& r, int taskID) { r.reply.reset(); r.reply.getEndpoint(taskID); }

template <class Reply>
void resetReply(ReplyPromise<Reply> & p, int taskID) { p.reset(); p.getEndpoint(taskID); }

template <class Request>
void setReplyPriority(Request& r, int taskID) { r.reply.getEndpoint(taskID); }

template <class Reply>
void setReplyPriority(ReplyPromise<Reply> & p, int taskID) { p.getEndpoint(taskID); }

template <class Reply>
void setReplyPriority(const ReplyPromise<Reply> & p, int taskID) { p.getEndpoint(taskID); }





template <class T>
struct NetNotifiedQueue : NotifiedQueue<T>, FlowReceiver, FastAllocated<NetNotifiedQueue<T>> {
	using FastAllocated<NetNotifiedQueue<T>>::operator new;
	using FastAllocated<NetNotifiedQueue<T>>::operator delete;

	NetNotifiedQueue(int futures, int promises) : NotifiedQueue<T>(futures, promises) {}
	NetNotifiedQueue(int futures, int promises, const Endpoint& remoteEndpoint) : NotifiedQueue<T>(futures, promises), FlowReceiver(remoteEndpoint) {}

	virtual void destroy() { delete this; }
	virtual void receive(ArenaReader& reader) {
		this->addPromiseRef();
		T message;
		reader >> message;
		this->send(std::move(message));
		this->delPromiseRef();
	}
	virtual bool isStream() const { return true; }
};


template <class T>
class RequestStream {
public:
	// stream.send( request )
	//   Unreliable at most once delivery: Delivers request unless there is a connection failure (zero or one times)

	void send(const T& value) const {
		if (queue->isRemoteEndpoint()) {
			FlowTransport::transport().sendUnreliable(SerializeSource<T>(value), getEndpoint());
		}
		else
			queue->send(value);
	}
	/*void sendError(const Error& error) const {
	ASSERT( !queue->isRemoteEndpoint() );
	queue->sendError(error);
	}*/

	// stream.getReply( request )
	//   Reliable at least once delivery: Eventually delivers request at least once and returns one of the replies if communication is possible.  Might deliver request
	//      more than once.
	//   If a reply is returned, request was or will be delivered one or more times.
	//   If cancelled, request was or will be delivered zero or more times.
	template <class X>
	Future< REPLY_TYPE(X) > getReply(const X& value) const {
		if (queue->isRemoteEndpoint()) {
			return sendCanceler(getReplyPromise(value), FlowTransport::transport().sendReliable(SerializeSource<T>(value), getEndpoint()));
		}
		send(value);
		return getReplyPromise(value).getFuture();
	}
	template <class X>
	Future<REPLY_TYPE(X)> getReply(const X& value, int taskID) const {
		setReplyPriority(value, taskID);
		return getReply(value);
	}
	template <class X>
	Future<X> getReply() const {
		return getReply(ReplyPromise<X>());
	}
	template <class X>
	Future<X> getReplyWithTaskID(int taskID) const {
		ReplyPromise<X> reply;
		reply.getEndpoint(taskID);
		return getReply(reply);
	}

	// stream.tryGetReply( request )
	//   Unreliable at most once delivery: Either delivers request and returns a reply, or returns failure (Optional<T>()) eventually.
	//   If a reply is returned, request was delivered exactly once.
	//   If cancelled or returns failure, request was or will be delivered zero or one times.
	//   The caller must be capable of retrying if this request returns failure
	template <class X>
	Future<ErrorOr<REPLY_TYPE(X)>> tryGetReply(const X& value, int taskID) const {
		setReplyPriority(value, taskID);
		if (queue->isRemoteEndpoint()) {
			Future<Void> disc = makeDependent<T>(IFailureMonitor::failureMonitor()).onDisconnectOrFailure(getEndpoint(taskID));
			if (disc.isReady()) {
				return ErrorOr<REPLY_TYPE(X)>(request_maybe_delivered());
			}
			FlowTransport::transport().sendUnreliable(SerializeSource<T>(value), getEndpoint(taskID));
			auto& p = getReplyPromise(value);
			return waitValueOrSignal(p.getFuture(), disc, getEndpoint(taskID), p);
		}
		send(value);
		auto& p = getReplyPromise(value);
		return waitValueOrSignal(p.getFuture(), Never(), getEndpoint(taskID), p);
	}

	template <class X>
	Future<ErrorOr<REPLY_TYPE(X)>> tryGetReply(const X& value) const {
		if (queue->isRemoteEndpoint()) {
			Future<Void> disc = makeDependent<T>(IFailureMonitor::failureMonitor()).onDisconnectOrFailure(getEndpoint());
			if (disc.isReady()) {
				return ErrorOr<REPLY_TYPE(X)>(request_maybe_delivered());
			}
			FlowTransport::transport().sendUnreliable(SerializeSource<T>(value), getEndpoint());
			auto& p = getReplyPromise(value);
			return waitValueOrSignal(p.getFuture(), disc, getEndpoint(), p);
		}
		else {
			send(value);
			auto& p = getReplyPromise(value);
			return waitValueOrSignal(p.getFuture(), Never(), getEndpoint(), p);
		}
	}

	// stream.getReplyUnlessFailedFor( request, double sustainedFailureDuration, double sustainedFailureSlope )
	//   Reliable at least once delivery: Like getReply, delivers request at least once and returns one of the replies. However, if
	//     the failure detector considers the endpoint failed permanently or for the given amount of time, returns failure instead.
	//   If a reply is returned, request was or will be delivered one or more times.
	//   If cancelled or returns failure, request was or will be delivered zero or more times.
	//   If it returns failure, the failure detector considers the endpoint failed permanently or for the given amount of time
	//   See IFailureMonitor::onFailedFor() for an explanation of the duration and slope parameters.
	template <class X>
	Future<ErrorOr<REPLY_TYPE(X)>> getReplyUnlessFailedFor(const X& value, double sustainedFailureDuration, double sustainedFailureSlope, int taskID) const {
		return waitValueOrSignal(getReply(value, taskID), makeDependent<T>(IFailureMonitor::failureMonitor()).onFailedFor(getEndpoint(taskID), sustainedFailureDuration, sustainedFailureSlope), getEndpoint(taskID));
	}

	template <class X>
	Future<ErrorOr<REPLY_TYPE(X)>> getReplyUnlessFailedFor(const X& value, double sustainedFailureDuration, double sustainedFailureSlope) const {
		return waitValueOrSignal(getReply(value), makeDependent<T>(IFailureMonitor::failureMonitor()).onFailedFor(getEndpoint(), sustainedFailureDuration, sustainedFailureSlope), getEndpoint());
	}

	explicit RequestStream(const Endpoint& endpoint) : queue(new NetNotifiedQueue<T>(0, 1, endpoint)) {}

	FutureStream<T> getFuture() const { queue->addFutureRef(); return FutureStream<T>(queue); }
	RequestStream() : queue(new NetNotifiedQueue<T>(0, 1)) {}
	RequestStream(const RequestStream& rhs) : queue(rhs.queue) { queue->addPromiseRef(); }
	RequestStream(RequestStream&& rhs) noexcept(true) : queue(rhs.queue) { rhs.queue = 0; }
	void operator=(const RequestStream& rhs) {
		rhs.queue->addPromiseRef();
		if (queue) queue->delPromiseRef();
		queue = rhs.queue;
	}
	void operator=(RequestStream&& rhs) noexcept(true) {
		if (queue != rhs.queue) {
			if (queue) queue->delPromiseRef();
			queue = rhs.queue;
			rhs.queue = 0;
		}
	}
	~RequestStream() {
		if (queue)
			queue->delPromiseRef();
		//queue = (NetNotifiedQueue<T>*)0xdeadbeef;
	}

	Endpoint getEndpoint(int taskID = TaskDefaultEndpoint) const { return queue->getEndpoint(taskID); }
	void makeWellKnownEndpoint(Endpoint::Token token, int taskID) {
		queue->makeWellKnownEndpoint(token, taskID);
	}

	bool operator == (const RequestStream<T>& rhs) const { return queue == rhs.queue; }
	bool isEmpty() const { return !queue->isReady(); }

private:
	NetNotifiedQueue<T>* queue;
};

template <class Ar, class T>
void save(Ar& ar, const RequestStream<T>& value) {
	auto const& ep = value.getEndpoint();
	ar << ep;
	UNSTOPPABLE_ASSERT(ep.address.isValid());  // No serializing PromiseStreams on a client with no public address
}

template <class Ar, class T>
void load(Ar& ar, RequestStream<T>& value) {
	Endpoint endpoint;
	FlowTransport::transport().loadEndpoint(ar, endpoint);
	value = RequestStream<T>(endpoint);
}



#endif
#include "genericactors.actor.g.h"
