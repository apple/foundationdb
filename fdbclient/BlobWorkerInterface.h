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

#include "fdbclient/CommitProxyInterface.h"
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

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, waitFailure, blobGranuleFileRequest, assignBlobRangeRequest, locality, myId);
	}
};

struct MutationAndVersion {
	constexpr static FileIdentifier file_identifier = 4268041;
	MutationRef m;
	Version v;

	MutationAndVersion() {}
	MutationAndVersion(Arena& to, MutationRef m, Version v) : m(to, m), v(v) {}
	MutationAndVersion(Arena& to, const MutationAndVersion& from) : m(to, from.m), v(from.v) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, m, v);
	}
};

// TODO should name all things that don't have their own arena *Ref
// file format of actual blob files
struct GranuleSnapshot : VectorRef<KeyValueRef> {

	constexpr static FileIdentifier file_identifier = 4268040;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<KeyValueRef>&)*this));
	}
};

struct GranuleDeltas : VectorRef<MutationAndVersion> {
	constexpr static FileIdentifier file_identifier = 4268042;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<MutationAndVersion>&)*this));
	}
};

// the assumption of this response is that the client will deserialize the files and apply the mutations themselves
// TODO could filter out delta files that don't intersect the key range being requested?
// TODO since client request passes version, we don't need to include the version of each mutation in the response if we
// pruned it there
struct BlobGranuleChunk {
	constexpr static FileIdentifier file_identifier = 991434;
	KeyRangeRef keyRange;
	StringRef snapshotFileName;
	VectorRef<StringRef> deltaFileNames;
	GranuleDeltas newDeltas;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, snapshotFileName, deltaFileNames, newDeltas);
	}
};

struct BlobGranuleFileReply {
	// TODO is there a "proper" way to generate file_identifier?
	constexpr static FileIdentifier file_identifier = 6858612;
	Arena arena;
	VectorRef<BlobGranuleChunk> chunks;

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
	Version readVersion;
	ReplyPromise<BlobGranuleFileReply> reply;

	BlobGranuleFileRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, readVersion, reply, arena);
	}
};

struct AssignBlobRangeRequest {
	constexpr static FileIdentifier file_identifier = 4150141;
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