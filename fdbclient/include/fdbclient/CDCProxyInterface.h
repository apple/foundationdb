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
	constexpr static FileIdentifier file_identifier = 16776001;
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
	constexpr static FileIdentifier file_identifier = 16776002;
	Version version = invalidVersion;
	VectorRef<MutationRef> mutations;

	VersionedMutationsRef() = default;
	VersionedMutationsRef(Version version, VectorRef<MutationRef> mutations) : version(version), mutations(mutations) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, mutations);
	}
};

struct CDCStreamInfoRef {
	constexpr static FileIdentifier file_identifier = 16776003;
	StringRef name;
	CDCStreamId streamId = 0;
	KeyRangeRef keys;
	Version minVersion = invalidVersion;

	CDCStreamInfoRef() = default;
	CDCStreamInfoRef(StringRef name, CDCStreamId streamId, KeyRangeRef keys, Version minVersion)
	  : name(name), streamId(streamId), keys(keys), minVersion(minVersion) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, name, streamId, keys, minVersion);
	}
};

struct CDCRegisterStreamReply {
	constexpr static FileIdentifier file_identifier = 16776012;
	CDCStreamId streamId = 0;

	CDCRegisterStreamReply() = default;
	explicit CDCRegisterStreamReply(CDCStreamId streamId) : streamId(streamId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, streamId);
	}
};

struct CDCRegisterStreamRequest {
	constexpr static FileIdentifier file_identifier = 16776004;
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
	constexpr static FileIdentifier file_identifier = 16776005;
	Key name;
	ReplyPromise<Void> reply;

	CDCRemoveStreamRequest() = default;
	explicit CDCRemoveStreamRequest(Key name) : name(name) {}

	bool verify() const { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, name, reply);
	}
};

struct CDCListStreamsReply {
	constexpr static FileIdentifier file_identifier = 16776006;
	Arena arena;
	VectorRef<CDCStreamInfoRef> streams;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, streams, arena);
	}
};

struct CDCListStreamsRequest {
	constexpr static FileIdentifier file_identifier = 16776007;
	ReplyPromise<CDCListStreamsReply> reply;

	bool verify() const { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct CDCConsumeReply {
	constexpr static FileIdentifier file_identifier = 16776008;
	Arena arena;
	VectorRef<VersionedMutationsRef> mutations;
	Version lastConsumedVersion = invalidVersion;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mutations, lastConsumedVersion, arena);
	}
};

struct CDCConsumeRequest {
	constexpr static FileIdentifier file_identifier = 16776009;
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
	constexpr static FileIdentifier file_identifier = 16776010;
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

struct CDCProxyInterface {
	constexpr static FileIdentifier file_identifier = 16776011;
	enum { LocationAwareLoadBalance = 1 };
	enum { AlwaysFresh = 1 };

	Optional<Key> processId;
	PublicRequestStream<CDCConsumeRequest> consume;
	PublicRequestStream<CDCRegisterStreamRequest> registerStream;
	PublicRequestStream<CDCRemoveStreamRequest> removeStream;
	PublicRequestStream<CDCListStreamsRequest> listStreams;
	PublicRequestStream<CDCAckRequest> ack;
	RequestStream<ReplyPromise<Void>> waitFailure;

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
			listStreams = PublicRequestStream<CDCListStreamsRequest>(consume.getEndpoint().getAdjustedEndpoint(3));
			ack = PublicRequestStream<CDCAckRequest>(consume.getEndpoint().getAdjustedEndpoint(4));
			waitFailure = RequestStream<ReplyPromise<Void>>(consume.getEndpoint().getAdjustedEndpoint(5));
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(consume.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(registerStream.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(removeStream.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(listStreams.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(ack.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(waitFailure.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
};

#endif // FDBCLIENT_CDCPROXYINTERFACE_H
