/*
 * BlobWorkerInterface.h
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

#ifndef FDBCLIENT_BLOBWORKERINTERFACE_H
#define FDBCLIENT_BLOBWORKERINTERFACE_H
#pragma once

#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"

struct BlobWorkerInterface {
	constexpr static FileIdentifier file_identifier = 8358753;
	// TODO should we just implement off the bad what the StorageServerInterface does with sequential endpoint IDs?
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct BlobGranuleFileRequest> blobGranuleFileRequest;
	RequestStream<struct AssignBlobRangeRequest> assignBlobRangeRequest;
	struct LocalityData locality;
	UID myId;

	BlobWorkerInterface() {}
	explicit BlobWorkerInterface(const struct LocalityData& l, UID id) : locality(l), myId(id) {}

	void initEndpoints() {}
	UID id() const { return myId; }
	NetworkAddress address() const { return blobGranuleFileRequest.getEndpoint().getPrimaryAddress(); }
	bool operator==(const BlobWorkerInterface& r) const { return id() == r.id(); }
	bool operator!=(const BlobWorkerInterface& r) const { return !(*this == r); }
	std::string toString() const { return id().shortString(); }

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, waitFailure, blobGranuleFileRequest, assignBlobRangeRequest, locality, myId);
	}
};

struct BlobGranuleFileReply {
	// TODO is there a "proper" way to generate file_identifier?
	constexpr static FileIdentifier file_identifier = 6858612;
	Arena arena;
	VectorRef<BlobGranuleChunkRef> chunks;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, chunks, arena);
	}
};

// TODO could do a reply promise stream of file mutations to bound memory requirements?
// Have to load whole snapshot file into memory though so it doesn't actually matter too much
struct BlobGranuleFileRequest {
	constexpr static FileIdentifier file_identifier = 4150141;
	Arena arena;
	KeyRangeRef keyRange;
	Version beginVersion = 0;
	Version readVersion;
	ReplyPromise<BlobGranuleFileReply> reply;

	BlobGranuleFileRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, beginVersion, readVersion, reply, arena);
	}
};

struct AssignBlobRangeRequest {
	constexpr static FileIdentifier file_identifier = 4844288;
	Arena arena;
	KeyRangeRef keyRange;
	Version assignVersion;
	bool isAssign; // true if assignment, false if revoke
	ReplyPromise<Void> reply;

	AssignBlobRangeRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, assignVersion, isAssign, reply, arena);
	}
};

// TODO once this

#endif