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
	RequestStream<struct RevokeBlobRangeRequest> revokeBlobRangeRequest;
	RequestStream<struct GranuleStatusStreamRequest> granuleStatusStreamRequest;
	RequestStream<struct HaltBlobWorkerRequest> haltBlobWorker;

	struct LocalityData locality;
	UID myId;

	BlobWorkerInterface() {}
	explicit BlobWorkerInterface(const struct LocalityData& l, UID id) : locality(l), myId(id) {}

	void initEndpoints() {}
	UID id() const { return myId; }
	NetworkAddress address() const { return blobGranuleFileRequest.getEndpoint().getPrimaryAddress(); }
	NetworkAddress stableAddress() const { return blobGranuleFileRequest.getEndpoint().getStableAddress(); }
	bool operator==(const BlobWorkerInterface& r) const { return id() == r.id(); }
	bool operator!=(const BlobWorkerInterface& r) const { return !(*this == r); }
	std::string toString() const { return id().shortString(); }

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar,
		           waitFailure,
		           blobGranuleFileRequest,
		           assignBlobRangeRequest,
		           revokeBlobRangeRequest,
		           granuleStatusStreamRequest,
		           haltBlobWorker,
		           locality,
		           myId);
	}
};

struct BlobGranuleFileReply {
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

struct AssignBlobRangeReply {
	constexpr static FileIdentifier file_identifier = 6431923;
	bool epochOk; // false if the worker has seen a new manager

	AssignBlobRangeReply() {}
	explicit AssignBlobRangeReply(bool epochOk) : epochOk(epochOk) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, epochOk);
	}
};

struct RevokeBlobRangeRequest {
	constexpr static FileIdentifier file_identifier = 4844288;
	Arena arena;
	KeyRangeRef keyRange;
	int64_t managerEpoch;
	int64_t managerSeqno;
	bool dispose;
	ReplyPromise<AssignBlobRangeReply> reply;

	RevokeBlobRangeRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, managerEpoch, managerSeqno, dispose, reply, arena);
	}
};

struct AssignBlobRangeRequest {
	constexpr static FileIdentifier file_identifier = 905381;
	Arena arena;
	KeyRangeRef keyRange;
	int64_t managerEpoch;
	int64_t managerSeqno;
	// If continueAssignment is true, this is just to instruct the worker that it *still* owns the range, so it should
	// re-snapshot it and continue.

	// For an initial assignment, reassignent, split, or merge, continueAssignment==false.
	bool continueAssignment;

	ReplyPromise<AssignBlobRangeReply> reply;

	AssignBlobRangeRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, managerEpoch, managerSeqno, continueAssignment, reply, arena);
	}
};

// reply per granule
// TODO: could eventually add other types of metrics to report back to the manager here
struct GranuleStatusReply : public ReplyPromiseStreamReply {
	constexpr static FileIdentifier file_identifier = 7563104;

	KeyRange granuleRange;
	bool doSplit;
	int64_t epoch;
	int64_t seqno;
	UID granuleID;
	Version startVersion;
	Version latestVersion;

	GranuleStatusReply() {}
	explicit GranuleStatusReply(KeyRange range,
	                            bool doSplit,
	                            int64_t epoch,
	                            int64_t seqno,
	                            UID granuleID,
	                            Version startVersion,
	                            Version latestVersion)
	  : granuleRange(range), doSplit(doSplit), epoch(epoch), seqno(seqno), granuleID(granuleID),
	    startVersion(startVersion), latestVersion(latestVersion) {}

	int expectedSize() const { return sizeof(GranuleStatusReply) + granuleRange.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           ReplyPromiseStreamReply::acknowledgeToken, ReplyPromiseStreamReply::sequence,
		           granuleRange,
		           doSplit,
		           epoch,
		           seqno,
		           granuleID,
		           startVersion,
		           latestVersion);
	}
};

// manager makes one request per worker, it sends all range updates through this stream
struct GranuleStatusStreamRequest {
	constexpr static FileIdentifier file_identifier = 2289677;

	int64_t managerEpoch;

	ReplyPromiseStream<GranuleStatusReply> reply;

	GranuleStatusStreamRequest() {}
	explicit GranuleStatusStreamRequest(int64_t managerEpoch) : managerEpoch(managerEpoch) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, managerEpoch, reply);
	}
};

struct HaltBlobWorkerRequest {
	constexpr static FileIdentifier file_identifier = 1985879;
	UID requesterID;
	ReplyPromise<Void> reply;

	int64_t managerEpoch;

	HaltBlobWorkerRequest() {}
	explicit HaltBlobWorkerRequest(int64_t managerEpoch, UID uid) : requesterID(uid), managerEpoch(managerEpoch) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, managerEpoch, requesterID, reply);
	}
};

#endif
