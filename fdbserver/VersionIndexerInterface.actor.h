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
#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_VERSION_INDEXER_INTERFACE_ACTOR_G_H)
#define FDBSERVER_VERSION_INDEXER_INTERFACE_ACTOR_G_H
#include "fdbserver/VersionIndexerInterface.actor.g.h"
#elif !defined(FDBSERVER_VERSION_INDEXER_INTERFACE_ACTOR_H)
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbclient/FDBTypes.h"

struct VersionIndexerInterface {
	constexpr static FileIdentifier file_identifier = 6865162;

	UID uniqueID = deterministicRandom()->randomUniqueID();
	LocalityData locality;
	UID id() const { return uniqueID; }
	std::string toString() const { return id().shortString(); }
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct VersionIndexerCommitRequest> commit;
	RequestStream<struct VersionIndexerPeekRequest> peek;

	bool operator==(const VersionIndexerInterface& other) const { return id() == other.id(); }
	bool operator!=(const VersionIndexerInterface& other) const { return id() != other.id(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, uniqueID, locality, waitFailure, commit, peek);
	}
};

struct VersionIndexerCommitRequest {
	constexpr static FileIdentifier file_identifier = 7036778;

	Version version, previousVersion, committedVersion;
	std::vector<Tag> tags;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, previousVersion, committedVersion, tags, reply);
	}
};

struct VersionIndexerPeekReply {
	constexpr static FileIdentifier file_identifier = 10553306;

	Version committedVersion, previousVersion;
	std::vector<std::pair<Version, bool>> versions;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, committedVersion, previousVersion, versions);
	}
};

struct VersionIndexerPeekRequest {
	constexpr static FileIdentifier file_identifier = 8334354;

	Version lastKnownVersion;
	Tag tag;
	ReplyPromise<struct VersionIndexerPeekReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, lastKnownVersion, tag, reply);
	}
};

#endif