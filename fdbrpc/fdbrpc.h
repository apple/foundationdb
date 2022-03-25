/*
 * fdbrpc.h
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

#ifndef FDBRPC_FDBRPC_H
#define FDBRPC_FDBRPC_H
#pragma once

#include "flow/flow.h"
#include "flow/serialize.h"
#include "fdbrpc/FlowTransport.h" // NetworkMessageReceiver Endpoint
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/networksender.actor.h"

struct FlowReceiver : public NetworkMessageReceiver {
	// Common endpoint code for NetSAV<> and NetNotifiedQueue<>

	FlowReceiver() : m_isLocalEndpoint(false), m_stream(false) {}

	FlowReceiver(Endpoint const& remoteEndpoint, bool stream)
	  : endpoint(remoteEndpoint), m_isLocalEndpoint(false), m_stream(stream) {
		FlowTransport::transport().addPeerReference(endpoint, m_stream);
	}

	~FlowReceiver() {
		if (m_isLocalEndpoint) {
			FlowTransport::transport().removeEndpoint(endpoint, this);
		} else {
			FlowTransport::transport().removePeerReference(endpoint, m_stream);
		}
	}

	bool isLocalEndpoint() { return m_isLocalEndpoint; }
	bool isRemoteEndpoint() { return endpoint.isValid() && !m_isLocalEndpoint; }

	// If already a remote endpoint, returns that.  Otherwise makes this
	//   a local endpoint and returns that.
	const Endpoint& getEndpoint(TaskPriority taskID) {
		ASSERT(taskID != TaskPriority::UnknownEndpoint);
		if (!endpoint.isValid()) {
			m_isLocalEndpoint = true;
			FlowTransport::transport().addEndpoint(endpoint, this, taskID);
		}
		return endpoint;
	}

	void setEndpoint(Endpoint const& e) {
		ASSERT(!endpoint.isValid());
		m_isLocalEndpoint = true;
		endpoint = e;
	}

	void setPeerCompatibilityPolicy(const PeerCompatibilityPolicy& policy) { peerCompatibilityPolicy_ = policy; }

	PeerCompatibilityPolicy peerCompatibilityPolicy() const override {
		return peerCompatibilityPolicy_.orDefault(NetworkMessageReceiver::peerCompatibilityPolicy());
	}

	void makeWellKnownEndpoint(Endpoint::Token token, TaskPriority taskID) {
		ASSERT(!endpoint.isValid());
		m_isLocalEndpoint = true;
		endpoint.token = token;
		FlowTransport::transport().addWellKnownEndpoint(endpoint, this, taskID);
	}

	const Endpoint& getRawEndpoint() { return endpoint; }

private:
	Optional<PeerCompatibilityPolicy> peerCompatibilityPolicy_;
	Endpoint endpoint;
	bool m_isLocalEndpoint;
	bool m_stream;
};

template <class T>
struct NetSAV final : SAV<T>, FlowReceiver, FastAllocated<NetSAV<T>> {
	using FastAllocated<NetSAV<T>>::operator new;
	using FastAllocated<NetSAV<T>>::operator delete;

	NetSAV(int futures, int promises) : SAV<T>(futures, promises) {}
	NetSAV(int futures, int promises, const Endpoint& remoteEndpoint)
	  : SAV<T>(futures, promises), FlowReceiver(remoteEndpoint, false) {}

	void destroy() override { delete this; }
	void receive(ArenaObjectReader& reader) override {
		if (!SAV<T>::canBeSet())
			return;
		this->addPromiseRef();
		ErrorOr<EnsureTable<T>> message;
		reader.deserialize(message);
		if (message.isError()) {
			SAV<T>::sendErrorAndDelPromiseRef(message.getError());
		} else {
			SAV<T>::sendAndDelPromiseRef(message.get().asUnderlyingType());
		}
	}
};

template <class T>
class ReplyPromise final : public ComposedIdentifier<T, 1> {
public:
	template <class U>
	void send(U&& value) const {
		sav->send(std::forward<U>(value));
	}
	template <class E>
	void sendError(const E& exc) const {
		sav->sendError(exc);
	}

	void send(Never) { sendError(never_reply()); }

	Future<T> getFuture() const {
		sav->addFutureRef();
		return Future<T>(sav);
	}
	bool isSet() { return sav->isSet(); }
	bool isValid() const { return sav != nullptr; }
	ReplyPromise() : sav(new NetSAV<T>(0, 1)) {}
	explicit ReplyPromise(const PeerCompatibilityPolicy& policy) : ReplyPromise() {
		sav->setPeerCompatibilityPolicy(policy);
	}
	ReplyPromise(const ReplyPromise& rhs) : sav(rhs.sav) { sav->addPromiseRef(); }
	ReplyPromise(ReplyPromise&& rhs) noexcept : sav(rhs.sav) { rhs.sav = 0; }
	~ReplyPromise() {
		if (sav)
			sav->delPromiseRef();
	}

	ReplyPromise(const Endpoint& endpoint) : sav(new NetSAV<T>(0, 1, endpoint)) {}
	const Endpoint& getEndpoint(TaskPriority taskID = TaskPriority::DefaultPromiseEndpoint) const {
		return sav->getEndpoint(taskID);
	}

	void operator=(const ReplyPromise& rhs) {
		if (rhs.sav)
			rhs.sav->addPromiseRef();
		if (sav)
			sav->delPromiseRef();
		sav = rhs.sav;
	}
	void operator=(ReplyPromise&& rhs) noexcept {
		if (sav != rhs.sav) {
			if (sav)
				sav->delPromiseRef();
			sav = rhs.sav;
			rhs.sav = 0;
		}
	}
	void reset() { *this = ReplyPromise<T>(); }
	void swap(ReplyPromise& other) { std::swap(sav, other.sav); }

	// Beware, these operations are very unsafe
	SAV<T>* extractRawPointer() {
		auto ptr = sav;
		sav = nullptr;
		return ptr;
	}
	explicit ReplyPromise<T>(SAV<T>* ptr) : sav(ptr) {}

	int getFutureReferenceCount() const { return sav->getFutureReferenceCount(); }
	int getPromiseReferenceCount() const { return sav->getPromiseReferenceCount(); }

private:
	NetSAV<T>* sav;
};

template <class Ar, class T>
void save(Ar& ar, const ReplyPromise<T>& value) {
	auto const& ep = value.getEndpoint().token;
	ar << ep;
}

template <class Ar, class T>
void load(Ar& ar, ReplyPromise<T>& value) {
	UID token;
	ar >> token;
	Endpoint endpoint = FlowTransport::transport().loadedEndpoint(token);
	value = ReplyPromise<T>(endpoint);
	networkSender(value.getFuture(), endpoint);
}

template <class T>
struct serializable_traits<ReplyPromise<T>> : std::true_type {
	template <class Archiver>
	static void serialize(Archiver& ar, ReplyPromise<T>& p) {
		if constexpr (Archiver::isDeserializing) {
			UID token;
			serializer(ar, token);
			auto endpoint = FlowTransport::transport().loadedEndpoint(token);
			p = ReplyPromise<T>(endpoint);
			networkSender(p.getFuture(), endpoint);
		} else {
			const auto& ep = p.getEndpoint().token;
			serializer(ar, ep);
		}
	}
};

template <class Reply>
ReplyPromise<Reply> const& getReplyPromise(ReplyPromise<Reply> const& p) {
	return p;
}

template <class Request>
void resetReply(Request& r) {
	r.reply.reset();
}

template <class Reply>
void resetReply(ReplyPromise<Reply>& p) {
	p.reset();
}

template <class Request>
void resetReply(Request& r, TaskPriority taskID) {
	r.reply.reset();
	r.reply.getEndpoint(taskID);
}

template <class Reply>
void resetReply(ReplyPromise<Reply>& p, TaskPriority taskID) {
	p.reset();
	p.getEndpoint(taskID);
}

template <class Request>
void setReplyPriority(Request& r, TaskPriority taskID) {
	r.reply.getEndpoint(taskID);
}

template <class Reply>
void setReplyPriority(ReplyPromise<Reply>& p, TaskPriority taskID) {
	p.getEndpoint(taskID);
}

template <class Reply>
void setReplyPriority(const ReplyPromise<Reply>& p, TaskPriority taskID) {
	p.getEndpoint(taskID);
}

struct ReplyPromiseStreamReply {
	Optional<UID> acknowledgeToken;
	uint16_t sequence;
	ReplyPromiseStreamReply() {}
};

struct AcknowledgementReply {
	constexpr static FileIdentifier file_identifier = 1389929;
	int64_t bytes;

	AcknowledgementReply() : bytes(0) {}
	explicit AcknowledgementReply(int64_t bytes) : bytes(bytes) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, bytes);
	}
};

// Registered on the server to receive acknowledgements that the client has received stream data. This prevents the
// server from sending too much data to the client if the client is not consuming it.
struct AcknowledgementReceiver final : FlowReceiver, FastAllocated<AcknowledgementReceiver> {
	using FastAllocated<AcknowledgementReceiver>::operator new;
	using FastAllocated<AcknowledgementReceiver>::operator delete;

	uint16_t sequence = 0;
	int64_t bytesSent = 0;
	int64_t bytesAcknowledged = 0;
	int64_t bytesLimit = 0;
	Promise<Void> ready;
	Future<Void> failures;

	AcknowledgementReceiver() : ready(nullptr) {}
	AcknowledgementReceiver(const Endpoint& remoteEndpoint) : FlowReceiver(remoteEndpoint, false), ready(nullptr) {}

	void receive(ArenaObjectReader& reader) override {
		ErrorOr<AcknowledgementReply> message;
		reader.deserialize(message);
		if (message.isError()) {
			// The client will send an operation_obsolete error on the acknowledgement stream when it cancels the
			// ReplyPromiseStream
			if (!ready.isValid()) {
				ready = Promise<Void>();
			}
			// Sending the error can lead to the destruction of the acknowledgementReceiver so we keep a local copy
			Promise<Void> hold = ready;
			hold.sendError(message.getError());
		} else {
			ASSERT(message.get().bytes > bytesAcknowledged || (message.get().bytes < 0 && bytesAcknowledged > 0));
			bytesAcknowledged = message.get().bytes;
			if (ready.isValid() && bytesSent - bytesAcknowledged < bytesLimit) {
				Promise<Void> hold = ready;
				ready = Promise<Void>(nullptr);
				// Sending to this promise could cause the ready to be replaced, so we need to hold a local copy
				hold.send(Void());
			}
		}
	}
};

// A version of NetNotifiedQueue which adds support for acknowledgments.
template <class T>
struct NetNotifiedQueueWithAcknowledgements final : NotifiedQueue<T>,
                                                    FlowReceiver,
                                                    FastAllocated<NetNotifiedQueueWithAcknowledgements<T>> {
	using FastAllocated<NetNotifiedQueueWithAcknowledgements<T>>::operator new;
	using FastAllocated<NetNotifiedQueueWithAcknowledgements<T>>::operator delete;

	AcknowledgementReceiver acknowledgements;
	Endpoint requestStreamEndpoint;
	bool sentError = false;
	Promise<Void> onConnect;

	NetNotifiedQueueWithAcknowledgements(int futures, int promises)
	  : NotifiedQueue<T>(futures, promises), onConnect(nullptr) {}
	NetNotifiedQueueWithAcknowledgements(int futures, int promises, const Endpoint& remoteEndpoint)
	  : NotifiedQueue<T>(futures, promises), FlowReceiver(remoteEndpoint, true), onConnect(nullptr) {
		// A ReplyPromiseStream will be terminated on the server side if the network connection with the client breaks
		acknowledgements.failures = tagError<Void>(FlowTransport::transport().loadedDisconnect(), operation_obsolete());
	}

	void destroy() override { delete this; }
	void receive(ArenaObjectReader& reader) override {
		this->addPromiseRef();
		ErrorOr<EnsureTable<T>> message;
		reader.deserialize(message);

		if (message.isError()) {
			if (message.getError().code() == error_code_broken_promise) {
				ASSERT(requestStreamEndpoint.isValid());
				// We will get a broken_promise on the client side only if the ReplyPromiseStream was cancelled without
				// sending an error. In this case the storage server actor must have been cancelled so future
				// GetKeyValuesStream requests on the same endpoint will fail
				IFailureMonitor::failureMonitor().endpointNotFound(requestStreamEndpoint);
			}
			if (onConnect.isValid() && onConnect.canBeSet()) {
				onConnect.send(Void());
			}
			this->sendError(message.getError());
		} else {
			if (message.get().asUnderlyingType().acknowledgeToken.present()) {
				acknowledgements = AcknowledgementReceiver(
				    FlowTransport::transport().loadedEndpoint(message.get().asUnderlyingType().acknowledgeToken.get()));
				if (onConnect.isValid() && onConnect.canBeSet()) {
					onConnect.send(Void());
				}
			}
			if (acknowledgements.sequence != message.get().asUnderlyingType().sequence) {
				TraceEvent(SevError, "StreamSequenceMismatch")
				    .detail("Expected", acknowledgements.sequence)
				    .detail("Actual", message.get().asUnderlyingType().sequence);
				ASSERT_WE_THINK(false);
				this->sendError(connection_failed());
			} else {
				acknowledgements.sequence++;
				if (this->shouldFireImmediately()) {
					// This message is going to be consumed by the client immediately (and therefore will not call
					// pop()) so send an ack immediately
					if (acknowledgements.getRawEndpoint().isValid()) {
						acknowledgements.bytesAcknowledged += message.get().asUnderlyingType().expectedSize();
						FlowTransport::transport().sendUnreliable(
						    SerializeSource<ErrorOr<AcknowledgementReply>>(
						        AcknowledgementReply(acknowledgements.bytesAcknowledged)),
						    acknowledgements.getEndpoint(TaskPriority::ReadSocket),
						    false);
					}
				}

				this->send(std::move(message.get().asUnderlyingType()));
			}
		}
		this->delPromiseRef();
	}

	T pop() override {
		T res = this->popImpl();
		// A reply that has been queued up is being consumed, so send an ack to the server
		if (acknowledgements.getRawEndpoint().isValid()) {
			acknowledgements.bytesAcknowledged += res.expectedSize();
			FlowTransport::transport().sendUnreliable(SerializeSource<ErrorOr<AcknowledgementReply>>(
			                                              AcknowledgementReply(acknowledgements.bytesAcknowledged)),
			                                          acknowledgements.getEndpoint(TaskPriority::ReadSocket),
			                                          false);
		}
		return res;
	}

	~NetNotifiedQueueWithAcknowledgements() {
		if (acknowledgements.getRawEndpoint().isValid() && acknowledgements.isRemoteEndpoint() && !this->hasError()) {
			// Notify the server that a client is not using this ReplyPromiseStream anymore
			FlowTransport::transport().sendUnreliable(
			    SerializeSource<ErrorOr<AcknowledgementReply>>(operation_obsolete()),
			    acknowledgements.getEndpoint(TaskPriority::ReadSocket),
			    false);
		}
		if (isRemoteEndpoint() && !sentError && !acknowledgements.failures.isReady()) {
			// Notify the client ReplyPromiseStream was cancelled before sending an error, so the storage server must
			// have died
			FlowTransport::transport().sendUnreliable(SerializeSource<ErrorOr<EnsureTable<T>>>(broken_promise()),
			                                          getEndpoint(TaskPriority::ReadSocket),
			                                          false);
		}
	}

	bool isStream() const override { return true; }
};

template <class T>
class ReplyPromiseStream {
public:
	// stream.send( request )
	//   Unreliable at most once delivery: Delivers request unless there is a connection failure (zero or one times)

	template <class U>
	void send(U&& value) const {
		if (queue->isRemoteEndpoint()) {
			if (queue->acknowledgements.failures.isError()) {
				throw queue->acknowledgements.failures.getError();
			}
			if (!queue->acknowledgements.getRawEndpoint().isValid()) {
				// register acknowledge receiver on sender and tell the receiver where to send acknowledge messages
				value.acknowledgeToken = queue->acknowledgements.getEndpoint(TaskPriority::ReadSocket).token;
			}
			value.sequence = queue->acknowledgements.sequence++;
			queue->acknowledgements.bytesSent += value.expectedSize();
			FlowTransport::transport().sendUnreliable(
			    SerializeSource<ErrorOr<EnsureTable<T>>>(value), getEndpoint(), false);
		} else {
			queue->send(std::forward<U>(value));
		}
	}

	template <class E>
	void sendError(const E& exc) const {
		if (queue->isRemoteEndpoint()) {
			if (!queue->sentError && !queue->acknowledgements.failures.isError()) {
				queue->sentError = true;
				FlowTransport::transport().sendUnreliable(
				    SerializeSource<ErrorOr<EnsureTable<T>>>(exc), getEndpoint(), false);
			}
		} else {
			queue->sendError(exc);
			if (errors && errors->canBeSet()) {
				errors->sendError(exc);
			}
		}
	}

	FutureStream<T> getFuture() const {
		queue->addFutureRef();
		return FutureStream<T>(queue);
	}
	ReplyPromiseStream() : queue(new NetNotifiedQueueWithAcknowledgements<T>(0, 1)), errors(new SAV<Void>(0, 1)) {}
	ReplyPromiseStream(const ReplyPromiseStream& rhs) : queue(rhs.queue), errors(rhs.errors) {
		queue->addPromiseRef();
		if (errors) {
			errors->addPromiseRef();
		}
	}
	ReplyPromiseStream(ReplyPromiseStream&& rhs) noexcept : queue(rhs.queue), errors(rhs.errors) {
		rhs.queue = nullptr;
		rhs.errors = nullptr;
	}
	explicit ReplyPromiseStream(const Endpoint& endpoint)
	  : queue(new NetNotifiedQueueWithAcknowledgements<T>(0, 1, endpoint)), errors(nullptr) {}

	// Used by endStreamOnDisconnect to detect when all references to the ReplyPromiseStream have been dropped
	Future<Void> getErrorFutureAndDelPromiseRef() {
		ASSERT(errors && errors->getPromiseReferenceCount() > 1);
		errors->addFutureRef();
		errors->delPromiseRef();
		Future<Void> res(errors);
		errors = nullptr;
		return res;
	}

	void setRequestStreamEndpoint(const Endpoint& endpoint) { queue->requestStreamEndpoint = endpoint; }

	bool connected() { return queue->acknowledgements.getRawEndpoint().isValid() || queue->error.isValid(); }

	Future<Void> onConnected() {
		if (connected()) {
			return Void();
		}
		if (!queue->onConnect.isValid()) {
			queue->onConnect = Promise<Void>();
		}
		return queue->onConnect.getFuture();
	}

	~ReplyPromiseStream() {
		if (queue)
			queue->delPromiseRef();
		if (errors)
			errors->delPromiseRef();
	}

	// The endpoints of a ReplyPromiseStream must be initialized at Task::ReadSocket, because with lower priorities
	// a delay(0) in FlowTransport deliver can cause out of order delivery.
	const Endpoint& getEndpoint() const { return queue->getEndpoint(TaskPriority::ReadSocket); }

	bool operator==(const ReplyPromiseStream<T>& rhs) const { return queue == rhs.queue; }
	bool operator!=(const ReplyPromiseStream<T>& rhs) const { return !(*this == rhs); }

	bool isEmpty() const { return !queue->isReady(); }

	Future<Void> onEmpty() {
		if (isEmpty()) {
			return Void();
		}
		if (!queue->onEmpty.isValid()) {
			queue->onEmpty = Promise<Void>();
		}
		return queue->onEmpty.getFuture();
	}

	bool isError() const { return !queue->isError(); }

	// throws, used to short circuit waiting on the queue if there has been an unexpected error
	Future<Void> onError() {
		if (queue->hasError() && queue->error.code() != error_code_end_of_stream) {
			throw queue->error;
		}
		if (!queue->onError.isValid()) {
			queue->onError = Promise<Void>();
		}
		return queue->onError.getFuture();
	}

	uint32_t size() const { return queue->size(); }

	// Must be called on the server before sending results on the stream to ratelimit the amount of data outstanding to
	// the client
	Future<Void> onReady() const {
		ASSERT(queue->acknowledgements.bytesLimit > 0);
		if (queue->acknowledgements.failures.isError()) {
			return queue->acknowledgements.failures.getError();
		}
		if (queue->acknowledgements.ready.isValid() && queue->acknowledgements.ready.isSet()) {
			return queue->acknowledgements.ready.getFuture().getError();
		}
		if (queue->acknowledgements.bytesSent - queue->acknowledgements.bytesAcknowledged <
		    queue->acknowledgements.bytesLimit) {
			return Void();
		}
		if (!queue->acknowledgements.ready.isValid()) {
			queue->acknowledgements.ready = Promise<Void>();
		}
		return queue->acknowledgements.ready.getFuture() || queue->acknowledgements.failures;
	}

	// Must be called on the server before using a ReplyPromiseStream to limit the amount of outstanding bytes to the
	// client
	void setByteLimit(int64_t byteLimit) { queue->acknowledgements.bytesLimit = byteLimit; }

	void operator=(const ReplyPromiseStream& rhs) {
		rhs.queue->addPromiseRef();
		if (queue)
			queue->delPromiseRef();
		queue = rhs.queue;
		if (rhs.errors)
			rhs.errors->addPromiseRef();
		if (errors)
			errors->delPromiseRef();
		errors = rhs.errors;
	}
	void operator=(ReplyPromiseStream&& rhs) noexcept {
		if (queue != rhs.queue) {
			if (queue)
				queue->delPromiseRef();
			queue = rhs.queue;
			rhs.queue = 0;
		}
		if (errors != rhs.errors) {
			if (errors)
				errors->delPromiseRef();
			errors = rhs.errors;
			rhs.errors = 0;
		}
	}

	void reset() { *this = ReplyPromiseStream<T>(); }

private:
	NetNotifiedQueueWithAcknowledgements<T>* queue;
	SAV<Void>* errors;
};

template <class Ar, class T>
void save(Ar& ar, const ReplyPromiseStream<T>& value) {
	auto const& ep = value.getEndpoint().token;
	ar << ep;
}

template <class Ar, class T>
void load(Ar& ar, ReplyPromiseStream<T>& value) {
	UID token;
	ar >> token;
	Endpoint endpoint = FlowTransport::transport().loadedEndpoint(token);
	value = ReplyPromiseStream<T>(endpoint);
}

template <class T>
struct serializable_traits<ReplyPromiseStream<T>> : std::true_type {
	template <class Archiver>
	static void serialize(Archiver& ar, ReplyPromiseStream<T>& p) {
		if constexpr (Archiver::isDeserializing) {
			UID token;
			serializer(ar, token);
			auto endpoint = FlowTransport::transport().loadedEndpoint(token);
			p = ReplyPromiseStream<T>(endpoint);
		} else {
			const auto& ep = p.getEndpoint().token;
			serializer(ar, ep);
		}
	}
};

template <class T>
struct NetNotifiedQueue final : NotifiedQueue<T>, FlowReceiver, FastAllocated<NetNotifiedQueue<T>> {
	using FastAllocated<NetNotifiedQueue<T>>::operator new;
	using FastAllocated<NetNotifiedQueue<T>>::operator delete;

	NetNotifiedQueue(int futures, int promises) : NotifiedQueue<T>(futures, promises) {}
	NetNotifiedQueue(int futures, int promises, const Endpoint& remoteEndpoint)
	  : NotifiedQueue<T>(futures, promises), FlowReceiver(remoteEndpoint, true) {}

	void destroy() override { delete this; }
	void receive(ArenaObjectReader& reader) override {
		this->addPromiseRef();
		T message;
		reader.deserialize(message);
		this->send(std::move(message));
		this->delPromiseRef();
	}
	bool isStream() const override { return true; }
};

template <class T>
class RequestStream {
public:
	// stream.send( request )
	//   Unreliable at most once delivery: Delivers request unless there is a connection failure (zero or one times)

	template <class U>
	void send(U&& value) const {
		if (queue->isRemoteEndpoint()) {
			FlowTransport::transport().sendUnreliable(SerializeSource<T>(std::forward<U>(value)), getEndpoint(), true);
		} else
			queue->send(std::forward<U>(value));
	}

	/*void sendError(const Error& error) const {
	ASSERT( !queue->isRemoteEndpoint() );
	queue->sendError(error);
	}*/

	// stream.getReply( request )
	//   Reliable at least once delivery: Eventually delivers request at least once and returns one of the replies if
	//   communication is possible.  Might deliver request
	//      more than once.
	//   If a reply is returned, request was or will be delivered one or more times.
	//   If cancelled, request was or will be delivered zero or more times.
	template <class X>
	Future<REPLY_TYPE(X)> getReply(const X& value) const {
		ASSERT(!getReplyPromise(value).getFuture().isReady());
		if (queue->isRemoteEndpoint()) {
			return sendCanceler(getReplyPromise(value),
			                    FlowTransport::transport().sendReliable(SerializeSource<T>(value), getEndpoint()),
			                    getEndpoint());
		}
		send(value);
		return reportEndpointFailure(getReplyPromise(value).getFuture(), getEndpoint());
	}
	template <class X>
	Future<REPLY_TYPE(X)> getReply(const X& value, TaskPriority taskID) const {
		setReplyPriority(value, taskID);
		return getReply(value);
	}
	template <class X>
	Future<X> getReply() const {
		return getReply(ReplyPromise<X>());
	}
	template <class X>
	Future<X> getReplyWithTaskID(TaskPriority taskID) const {
		ReplyPromise<X> reply;
		reply.getEndpoint(taskID);
		return getReply(reply);
	}

	// stream.tryGetReply( request )
	//   Unreliable at most once delivery: Either delivers request and returns a reply, or returns an error eventually.
	//   If a reply is returned, request was delivered exactly once. If cancelled or returns
	//   failure, request was or will be delivered zero or one times. The caller must be capable of retrying if this
	//   request returns failure
	template <class X>
	Future<ErrorOr<REPLY_TYPE(X)>> tryGetReply(const X& value, TaskPriority taskID) const {
		setReplyPriority(value, taskID);
		if (queue->isRemoteEndpoint()) {
			Future<Void> disc =
			    makeDependent<T>(IFailureMonitor::failureMonitor()).onDisconnectOrFailure(getEndpoint(taskID));
			if (disc.isReady()) {
				return ErrorOr<REPLY_TYPE(X)>(request_maybe_delivered());
			}
			Reference<Peer> peer =
			    FlowTransport::transport().sendUnreliable(SerializeSource<T>(value), getEndpoint(taskID), true);
			auto& p = getReplyPromise(value);
			return waitValueOrSignal(p.getFuture(), disc, getEndpoint(taskID), p, peer);
		}
		send(value);
		auto& p = getReplyPromise(value);
		return waitValueOrSignal(p.getFuture(), Never(), getEndpoint(taskID), p);
	}

	template <class X>
	Future<ErrorOr<REPLY_TYPE(X)>> tryGetReply(const X& value) const {
		if (queue->isRemoteEndpoint()) {
			Future<Void> disc =
			    makeDependent<T>(IFailureMonitor::failureMonitor()).onDisconnectOrFailure(getEndpoint());
			if (disc.isReady()) {
				return ErrorOr<REPLY_TYPE(X)>(request_maybe_delivered());
			}
			Reference<Peer> peer =
			    FlowTransport::transport().sendUnreliable(SerializeSource<T>(value), getEndpoint(), true);
			auto& p = getReplyPromise(value);
			return waitValueOrSignal(p.getFuture(), disc, getEndpoint(), p, peer);
		} else {
			send(value);
			auto& p = getReplyPromise(value);
			return waitValueOrSignal(p.getFuture(), Never(), getEndpoint(), p);
		}
	}

	// stream.getReplyStream( request )
	//   Unreliable at most once delivery.
	//   Registers the request with the remote endpoint which sends back a stream of replies, followed by an
	//   end_of_stream error. If the connection is ever broken the remote endpoint will stop attempting to send replies.
	//   The caller sends acknowledgements to the remote endpoint so that at most 2MB of replies is ever inflight.

	template <class X>
	ReplyPromiseStream<REPLYSTREAM_TYPE(X)> getReplyStream(const X& value) const {
		if (queue->isRemoteEndpoint()) {
			Future<Void> disc =
			    makeDependent<T>(IFailureMonitor::failureMonitor()).onDisconnectOrFailure(getEndpoint());
			auto& p = getReplyPromiseStream(value);
			if (disc.isReady()) {
				p.sendError(request_maybe_delivered());
			} else {
				Reference<Peer> peer =
				    FlowTransport::transport().sendUnreliable(SerializeSource<T>(value), getEndpoint(), true);
				endStreamOnDisconnect(disc, p, getEndpoint(), peer);
			}
			return p;
		} else {
			send(value);
			auto& p = getReplyPromiseStream(value);
			return p;
		}
	}

	// stream.getReplyUnlessFailedFor( request, double sustainedFailureDuration, double sustainedFailureSlope )
	//   Reliable at least once delivery: Like getReply, delivers request at least once and returns one of the replies.
	//   However, if
	//     the failure detector considers the endpoint failed permanently or for the given amount of time, returns
	//     failure instead.
	//   If a reply is returned, request was or will be delivered one or more times.
	//   If cancelled or returns failure, request was or will be delivered zero or more times.
	//   If it returns failure, the failure detector considers the endpoint failed permanently or for the given amount
	//   of time See IFailureMonitor::onFailedFor() for an explanation of the duration and slope parameters.
	template <class X>
	Future<ErrorOr<REPLY_TYPE(X)>> getReplyUnlessFailedFor(const X& value,
	                                                       double sustainedFailureDuration,
	                                                       double sustainedFailureSlope,
	                                                       TaskPriority taskID) const {
		// If it is local endpoint, no need for failure monitoring
		return waitValueOrSignal(getReply(value, taskID),
		                         makeDependent<T>(IFailureMonitor::failureMonitor())
		                             .onFailedFor(getEndpoint(taskID), sustainedFailureDuration, sustainedFailureSlope),
		                         getEndpoint(taskID));
	}

	template <class X>
	Future<ErrorOr<REPLY_TYPE(X)>> getReplyUnlessFailedFor(const X& value,
	                                                       double sustainedFailureDuration,
	                                                       double sustainedFailureSlope) const {
		// If it is local endpoint, no need for failure monitoring
		return waitValueOrSignal(getReply(value),
		                         makeDependent<T>(IFailureMonitor::failureMonitor())
		                             .onFailedFor(getEndpoint(), sustainedFailureDuration, sustainedFailureSlope),
		                         getEndpoint());
	}

	template <class X>
	Future<ErrorOr<X>> getReplyUnlessFailedFor(double sustainedFailureDuration, double sustainedFailureSlope) const {
		return getReplyUnlessFailedFor(ReplyPromise<X>(), sustainedFailureDuration, sustainedFailureSlope);
	}

	explicit RequestStream(const Endpoint& endpoint) : queue(new NetNotifiedQueue<T>(0, 1, endpoint)) {}

	FutureStream<T> getFuture() const {
		queue->addFutureRef();
		return FutureStream<T>(queue);
	}
	RequestStream() : queue(new NetNotifiedQueue<T>(0, 1)) {}
	explicit RequestStream(PeerCompatibilityPolicy policy) : RequestStream() {
		queue->setPeerCompatibilityPolicy(policy);
	}
	RequestStream(const RequestStream& rhs) : queue(rhs.queue) { queue->addPromiseRef(); }
	RequestStream(RequestStream&& rhs) noexcept : queue(rhs.queue) { rhs.queue = 0; }
	void operator=(const RequestStream& rhs) {
		rhs.queue->addPromiseRef();
		if (queue)
			queue->delPromiseRef();
		queue = rhs.queue;
	}
	void operator=(RequestStream&& rhs) noexcept {
		if (queue != rhs.queue) {
			if (queue)
				queue->delPromiseRef();
			queue = rhs.queue;
			rhs.queue = 0;
		}
	}
	~RequestStream() {
		if (queue)
			queue->delPromiseRef();
		// queue = (NetNotifiedQueue<T>*)0xdeadbeef;
	}

	const Endpoint& getEndpoint(TaskPriority taskID = TaskPriority::DefaultEndpoint) const {
		return queue->getEndpoint(taskID);
	}

	void makeWellKnownEndpoint(uint64_t wlTokenID, TaskPriority taskID) {
		queue->makeWellKnownEndpoint(Endpoint::Token(-1, wlTokenID), taskID);
	}

	bool operator==(const RequestStream<T>& rhs) const { return queue == rhs.queue; }
	bool operator!=(const RequestStream<T>& rhs) const { return !(*this == rhs); }
	bool isEmpty() const { return !queue->isReady(); }
	uint32_t size() const { return queue->size(); }

	std::pair<FlowReceiver*, TaskPriority> getReceiver(TaskPriority taskID = TaskPriority::DefaultEndpoint) {
		return std::make_pair((FlowReceiver*)queue, taskID);
	}

private:
	NetNotifiedQueue<T>* queue;
};

template <class Ar, class T>
void save(Ar& ar, const RequestStream<T>& value) {
	auto const& ep = value.getEndpoint();
	ar << ep;
	UNSTOPPABLE_ASSERT(
	    ep.getPrimaryAddress().isValid()); // No serializing PromiseStreams on a client with no public address
}

template <class Ar, class T>
void load(Ar& ar, RequestStream<T>& value) {
	Endpoint endpoint;
	ar >> endpoint;
	value = RequestStream<T>(endpoint);
}

template <class T>
struct serializable_traits<RequestStream<T>> : std::true_type {
	template <class Archiver>
	static void serialize(Archiver& ar, RequestStream<T>& stream) {
		if constexpr (Archiver::isDeserializing) {
			Endpoint endpoint;
			serializer(ar, endpoint);
			stream = RequestStream<T>(endpoint);
		} else {
			const auto& ep = stream.getEndpoint();
			serializer(ar, ep);
			if constexpr (Archiver::isSerializing) { // Don't assert this when collecting vtable for flatbuffers
				UNSTOPPABLE_ASSERT(ep.getPrimaryAddress()
				                       .isValid()); // No serializing PromiseStreams on a client with no public address
			}
		}
	}
};

#endif
#include "fdbrpc/genericactors.actor.h"
