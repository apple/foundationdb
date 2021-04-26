/*
 * ConfigFollowerInterface.h
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

#pragma once

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

struct ConfigFollowerGetVersionReply {
	static constexpr FileIdentifier file_identifier = 1028349;
	Version version;

	explicit ConfigFollowerGetVersionReply(Version version = -1) : version(version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version);
	}
};

struct ConfigFollowerGetVersionRequest {
	static constexpr FileIdentifier file_identifier = 9840156;
	ReplyPromise<ConfigFollowerGetVersionReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct ConfigFollowerGetFullDatabaseReply {
	static constexpr FileIdentifier file_identifier = 1734095;
	std::map<Key, Value> database;

	ConfigFollowerGetFullDatabaseReply() = default;
	explicit ConfigFollowerGetFullDatabaseReply(std::map<Key, Value> const& database) : database(database) {
		// TODO: Support move constructor as well
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, database);
	}
};

struct ConfigFollowerGetFullDatabaseRequest {
	static constexpr FileIdentifier file_identifier = 294811;
	Version version;
	Standalone<VectorRef<KeyRef>> filter;
	ReplyPromise<ConfigFollowerGetFullDatabaseReply> reply;

	ConfigFollowerGetFullDatabaseRequest() : version(-1) {}
	explicit ConfigFollowerGetFullDatabaseRequest(Version version, Standalone<VectorRef<KeyRef>> filter)
	  : version(version), filter(filter) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, filter, reply);
	}
};

struct VersionedMutationRef {
	Version version;
	MutationRef mutation;

	VersionedMutationRef()=default;
	explicit VersionedMutationRef(Arena &arena, Version version, MutationRef mutation) : version(version), mutation(arena, mutation) {}
	explicit VersionedMutationRef(Arena& arena, VersionedMutationRef const& rhs)
	  : version(rhs.version), mutation(arena, rhs.mutation) {}

	size_t expectedSize() const { return sizeof(Version) + mutation.expectedSize(); }

	template<class Ar>
	void serialize(Ar &ar) {
		serializer(ar, version, mutation);
	}
};

struct ConfigFollowerGetChangesReply {
	static constexpr FileIdentifier file_identifier = 234859;
	Version mostRecentVersion;
	Standalone<VectorRef<VersionedMutationRef>> versionedMutations;

	ConfigFollowerGetChangesReply() : mostRecentVersion(0) {}
	explicit ConfigFollowerGetChangesReply(Version mostRecentVersion,
	                                       Standalone<VectorRef<VersionedMutationRef>> const& versionedMutations)
	  : mostRecentVersion(mostRecentVersion), versionedMutations(versionedMutations) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mostRecentVersion, versionedMutations);
	}
};

struct ConfigFollowerGetChangesRequest {
	static constexpr FileIdentifier file_identifier = 178935;
	Version lastSeenVersion;
	Standalone<VectorRef<KeyRef>> filter;
	ReplyPromise<ConfigFollowerGetChangesReply> reply;

	ConfigFollowerGetChangesRequest() : lastSeenVersion(-1) {}
	explicit ConfigFollowerGetChangesRequest(Version lastSeenVersion, Standalone<VectorRef<KeyRef>> filter)
	  : lastSeenVersion(lastSeenVersion), filter(filter) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, lastSeenVersion, filter, reply);
	}
};

struct ConfigFollowerCompactRequest {
	static constexpr FileIdentifier file_identifier = 568910;
	Version version;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, reply);
	}
};

struct ConfigFollowerInterface {
	static constexpr FileIdentifier file_identifier = 7721102;
	RequestStream<ConfigFollowerGetVersionRequest> getVersion;
	RequestStream<ConfigFollowerGetFullDatabaseRequest> getFullDatabase;
	RequestStream<ConfigFollowerGetChangesRequest> getChanges;
	RequestStream<ConfigFollowerCompactRequest> compact;

	ConfigFollowerInterface() = default;
	void setupWellKnownEndpoints();
	ConfigFollowerInterface(NetworkAddress const& remote);

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, getVersion, getFullDatabase, getChanges);
	}
};
