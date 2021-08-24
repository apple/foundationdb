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

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/StorageServerInterface.h" // just for MutationsAndVersion, TODO pull that out maybe?
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

// TODO should name all things that don't have their own arena *Ref
// file format of actual blob files
struct GranuleSnapshot : VectorRef<KeyValueRef> {

	constexpr static FileIdentifier file_identifier = 1300395;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<KeyValueRef>&)*this));
	}
};

struct GranuleDeltas : VectorRef<MutationsAndVersionRef> {
	constexpr static FileIdentifier file_identifier = 8563013;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<MutationsAndVersionRef>&)*this));
	}
};

// TODO better name?
struct BlobFilenameRef {
	constexpr static FileIdentifier file_identifier = 5253554;
	StringRef filename;
	int64_t offset;
	int64_t length;

	BlobFilenameRef() {}
	BlobFilenameRef(Arena& to, std::string filename, int64_t offset, int64_t length)
	  : filename(to, filename), offset(offset), length(length) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, filename, offset, length);
	}

	std::string toString() {
		std::stringstream ss;
		ss << filename.toString() << ":" << offset << ":" << length;
		return std::move(ss).str();
	}
};

// the assumption of this response is that the client will deserialize the files and apply the mutations themselves
// TODO could filter out delta files that don't intersect the key range being requested?
// TODO since client request passes version, we don't need to include the version of each mutation in the response if we
// pruned it there
struct BlobGranuleChunkRef {
	constexpr static FileIdentifier file_identifier = 991434;
	KeyRangeRef keyRange;
	Version includedVersion;
	Optional<BlobFilenameRef> snapshotFile; // not set if it's an incremental read
	VectorRef<BlobFilenameRef> deltaFiles;
	GranuleDeltas newDeltas;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, includedVersion, snapshotFile, deltaFiles, newDeltas);
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