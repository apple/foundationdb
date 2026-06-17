/*
 * CDCProxyInterface.h
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

#ifndef FDBCLIENT_CDCPROXYINTERFACE_H
#define FDBCLIENT_CDCPROXYINTERFACE_H
#pragma once

#include "fdbclient/CommitTransaction.h"
#include "flow/FileIdentifier.h"
#include "fdbrpc/fdbrpc.h"

struct CDCCursor {
	constexpr static FileIdentifier file_identifier = 10949553;
	CDCStreamId streamId = 0;
	Version lastConsumedVersion = invalidVersion;

	CDCCursor() = default;
	CDCCursor(CDCStreamId streamId, Version lastConsumedVersion)
	  : streamId(streamId), lastConsumedVersion(lastConsumedVersion) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, streamId, lastConsumedVersion);
	}
};

struct VersionedMutationsRef {
	constexpr static FileIdentifier file_identifier = 3297577;
	Version version = invalidVersion;
	VectorRef<MutationRef> mutations;

	VersionedMutationsRef() = default;
	VersionedMutationsRef(Version version, VectorRef<MutationRef> mutations) : version(version), mutations(mutations) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, mutations);
	}
};

struct CDCRegisterStreamReply {
	constexpr static FileIdentifier file_identifier = 3217071;
	CDCStreamId streamId = 0;

	CDCRegisterStreamReply() = default;
	explicit CDCRegisterStreamReply(CDCStreamId streamId) : streamId(streamId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, streamId);
	}
};

struct CDCRegisterStreamRequest {
	constexpr static FileIdentifier file_identifier = 1269096;
	Key name;
	KeyRange keys;
	ReplyPromise<CDCRegisterStreamReply> reply;

	CDCRegisterStreamRequest() = default;
	CDCRegisterStreamRequest(Key name, KeyRange keys) : name(name), keys(keys) {}

	bool verify() const { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, name, keys, reply);
	}
};

struct CDCRemoveStreamRequest {
	constexpr static FileIdentifier file_identifier = 3683857;
	Key name;
	CDCStreamId streamId = 0;
	ReplyPromise<Void> reply;

	CDCRemoveStreamRequest() = default;
	CDCRemoveStreamRequest(Key name, CDCStreamId streamId) : name(name), streamId(streamId) {}

	bool verify() const { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, name, streamId, reply);
	}
};

struct CDCConsumeReply {
	constexpr static FileIdentifier file_identifier = 12940542;
	Arena arena;
	VectorRef<VersionedMutationsRef> mutations;
	Version lastConsumedVersion = invalidVersion;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mutations, lastConsumedVersion, arena);
	}
};

struct CDCConsumeRequest {
	constexpr static FileIdentifier file_identifier = 8178243;
	CDCCursor cursor;
	ReplyPromise<CDCConsumeReply> reply;

	CDCConsumeRequest() = default;
	explicit CDCConsumeRequest(CDCCursor cursor) : cursor(cursor) {}

	bool verify() const { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, cursor, reply);
	}
};

struct CDCAckRequest {
	constexpr static FileIdentifier file_identifier = 15923892;
	CDCStreamId streamId = 0;
	Version version = invalidVersion;
	ReplyPromise<Void> reply;

	CDCAckRequest() = default;
	CDCAckRequest(CDCStreamId streamId, Version version) : streamId(streamId), version(version) {}

	bool verify() const { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, streamId, version, reply);
	}
};

struct HaltCDCProxyRequest {
	constexpr static FileIdentifier file_identifier = 6992638;
	ReplyPromise<Void> reply;

	bool verify() const { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct CDCProxyBufferStatus {
	constexpr static FileIdentifier file_identifier = 9770616;
	int64_t bufferedBytes = 0;
	int64_t activePermits = 0;
	int64_t peakActivePermits = 0;
	int64_t bufferLimit = 0;
	int waiters = 0;
	int activeConsumeRequests = 0;
	int readDemand = 0;
	int64_t popRequests = 0;
	int64_t popAttempts = 0;
	int64_t popCompletions = 0;
	int64_t popCancellations = 0;
	bool popsPaused = false;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           bufferedBytes,
		           activePermits,
		           peakActivePermits,
		           bufferLimit,
		           waiters,
		           activeConsumeRequests,
		           readDemand,
		           popRequests,
		           popAttempts,
		           popCompletions,
		           popCancellations,
		           popsPaused);
	}
};

struct GetCDCProxyBufferStatusRequest {
	constexpr static FileIdentifier file_identifier = 10770616;
	ReplyPromise<CDCProxyBufferStatus> reply;

	bool verify() const { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct SetCDCProxyPopsPausedRequest {
	constexpr static FileIdentifier file_identifier = 1463231;
	bool paused = false;
	ReplyPromise<Void> reply;

	SetCDCProxyPopsPausedRequest() = default;
	explicit SetCDCProxyPopsPausedRequest(bool paused) : paused(paused) {}

	bool verify() const { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, paused, reply);
	}
};

struct CDCProxyInterface {
	constexpr static FileIdentifier file_identifier = 6689609;
	enum { LocationAwareLoadBalance = 1 };
	enum { AlwaysFresh = 1 };

	Optional<Key> processId;
	PublicRequestStream<CDCConsumeRequest> consume;
	PublicRequestStream<CDCRegisterStreamRequest> registerStream;
	PublicRequestStream<CDCRemoveStreamRequest> removeStream;
	PublicRequestStream<CDCAckRequest> ack;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<HaltCDCProxyRequest> haltForTesting;
	RequestStream<GetCDCProxyBufferStatusRequest> getBufferStatusForTesting;
	RequestStream<SetCDCProxyPopsPausedRequest> setPopsPausedForTesting;

	UID id() const { return consume.getEndpoint().token; }
	std::string toString() const { return id().shortString(); }
	bool operator==(CDCProxyInterface const& r) const { return id() == r.id(); }
	bool operator!=(CDCProxyInterface const& r) const { return id() != r.id(); }
	NetworkAddress address() const { return consume.getEndpoint().getPrimaryAddress(); }
	NetworkAddressList addresses() const { return consume.getEndpoint().addresses; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, processId, consume);
		if (Ar::isDeserializing) {
			registerStream =
			    PublicRequestStream<CDCRegisterStreamRequest>(consume.getEndpoint().getAdjustedEndpoint(1));
			removeStream = PublicRequestStream<CDCRemoveStreamRequest>(consume.getEndpoint().getAdjustedEndpoint(2));
			ack = PublicRequestStream<CDCAckRequest>(consume.getEndpoint().getAdjustedEndpoint(3));
			waitFailure = RequestStream<ReplyPromise<Void>>(consume.getEndpoint().getAdjustedEndpoint(4));
			haltForTesting = RequestStream<HaltCDCProxyRequest>(consume.getEndpoint().getAdjustedEndpoint(5));
			getBufferStatusForTesting =
			    RequestStream<GetCDCProxyBufferStatusRequest>(consume.getEndpoint().getAdjustedEndpoint(6));
			setPopsPausedForTesting =
			    RequestStream<SetCDCProxyPopsPausedRequest>(consume.getEndpoint().getAdjustedEndpoint(7));
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(consume.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(registerStream.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(removeStream.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(ack.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(haltForTesting.getReceiver());
		streams.push_back(getBufferStatusForTesting.getReceiver());
		streams.push_back(setPopsPausedForTesting.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
};

#endif // FDBCLIENT_CDCPROXYINTERFACE_H
