/*
 * VersionIndexerInterface.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_VERSIONINDEXERINTERFACE_H
#define FDBSERVER_VERSIONINDEXERINTERFACE_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbclient/FDBTypes.h"

struct VersionIndexerInterface {
	constexpr static FileIdentifier file_identifier = 6865162;

	LocalityData locality;
	UID uniqueID;
	RequestStream<struct VersionIndexerCommitRequest> commit;
	RequestStream<struct VersionIndexerPeekRequest> peek;

	RequestStream<ReplyPromise<Void>> waitFailure;

	VersionIndexerInterface() : uniqueID(deterministicRandom()->randomUniqueID()) {}
	UID id() const { return uniqueID; }
	std::string toString() const { return id().shortString(); }

	bool operator==(const VersionIndexerInterface& other) const { return id() == other.id(); }
	bool operator!=(const VersionIndexerInterface& other) const { return id() != other.id(); }

	NetworkAddress address() const { return commit.getEndpoint().getPrimaryAddress(); }
	NetworkAddressList addresses() const { return commit.getEndpoint().addresses; }

	void initEndpoints() { waitFailure.getEndpoint(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, uniqueID, locality, waitFailure, commit, peek);
	}
};

// A VersionIndexerCommitRequest is a message from the commit proxy
// notifying the version indexer about a pending commit and its
// affected tags.
//
// Returns: nothing?
//
// Errors: ???
struct VersionIndexerCommitRequest {
	constexpr static FileIdentifier file_identifier = 7036778;

	// commitVersion is the version of a pending commit.
	Version commitVersion;

	// previousVersion is the version prior to commitVersion, for
	// the purposes of chaining togethere versions and detecting
	// gaps.
	Version previousVersion;

	// minKnownCommittedVersion is a lower bound on the value of
	// the known committed version that will be used during
	// recovery. This information isn't needed directly by the
	// version indexer, but instead is routed through them to the
	// storage servers. It is used by storage servers, to decide
	// which versions they can safely serve to readers.
	//
	// XXX(dadkins): aren't the storage servers already getting this
	// information from read requests via the GRV proxy? What does
	// the version indexer do with this?
	Version minKnownCommittedVersion;

	// tags contains the set of tags affected by this commit.
	std::vector<Tag> tags;

	// There is no response to this request, only an acknowledgement.
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, commitVersion, previousVersion, minKnownCommittedVersion, tags, reply);
	}
};

// A VersionIndexerPeekReply is a response to a version peek request,
// containing information about committed (and uncommitted) versions
// affecting the caller.
struct VersionIndexerPeekReply {
	constexpr static FileIdentifier file_identifier = 10553306;

	// minKnownCommittedVersion is a lower bound on the known
	// committed version that will be used during recovery.  It's
	// not relevant to the version indexer, except that it is
	// being passed from commit proxy to storage server via this
	// service.
	Version minKnownCommittedVersion;

	// previousVersion is the version immediately prior to the
	// versions in this response.
	Version previousVersion;

	// versions is a sequence of versions following the last known
	// version in the request. Each entry has a boolean flag,
	// has_data, which is true if any data was written to the
	// request tag at that version. There are no gaps in this
	// sequence.
	//
	// XXX(dadkins): why do we care about versions with no data?
	// Why not just stream the versions for the requested tag?
	std::vector<std::pair<Version, bool>> versions;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, minKnownCommittedVersion, previousVersion, versions);
	}
};

// A VersionIndexerPeekRequest is a message from a storage server to the
// version indexer asking for all known (and committed???) versions since the
// last known versions the storage server is aware of.
//
// XXX(dadkins): Is the version indexer letting us known about
// uncommitted versions, or only the committed ones?
//
// XXX(dadkins): If there are no versions currently available, does
// the version indexer block for some time, or immediately return
// effectively an empty response?
//
// XXX(dadkins): Do we inform about every version? Or just those associated with a tag?
//
// Returns: VersionIndexerPeekReplay, a sequence of (version, has_data), in asecnding order.
//
// Errors: ???
struct VersionIndexerPeekRequest {
	constexpr static FileIdentifier file_identifier = 8334354;

	// lastKnownVersion is the last version of which the caller is already aware.
	// Results can be expected to be strictly greater versions.
	Version lastKnownVersion;

	// tag is the storage server's identifier. Part of the version indexer response
	// will contain a boolean, has_data, which indicates if a version has any data
	// /for this particular tag/.
	Tag tag;

	// XXX(dadkins): what is history for? Something to do with
	// truncating the window of versions the version indexer keeps
	// around (in memory).
	std::vector<std::pair<Version, Tag>> history;

	// The response is a sequence of versions.
	ReplyPromise<struct VersionIndexerPeekReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, lastKnownVersion, tag, history, reply);
	}
};

#endif
