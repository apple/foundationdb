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
#include "fdbclient/ConfigKnobs.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

class ConfigClassSet {
	std::set<Key> classes;
public:
	static constexpr FileIdentifier file_identifier = 9854021;

	bool operator==(ConfigClassSet const& rhs) const { return classes == rhs.classes; }
	bool operator!=(ConfigClassSet const& rhs) const { return !(*this == rhs); }

	ConfigClassSet();
	ConfigClassSet(VectorRef<KeyRef> configClasses);

	bool contains(KeyRef configClass) const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, classes);
	}
};

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

struct ConfigFollowerGetSnapshotReply {
	static constexpr FileIdentifier file_identifier = 1734095;
	std::map<ConfigKey, Value> snapshot;

	ConfigFollowerGetSnapshotReply() = default;
	explicit ConfigFollowerGetSnapshotReply(std::map<ConfigKey, Value> const& snapshot) : snapshot(snapshot) {
		// TODO: Support move constructor as well
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, snapshot);
	}
};

struct ConfigFollowerGetSnapshotRequest {
	static constexpr FileIdentifier file_identifier = 294811;
	Version version;
	ConfigClassSet configClassSet;
	ReplyPromise<ConfigFollowerGetSnapshotReply> reply;

	ConfigFollowerGetSnapshotRequest() : version(-1) {}
	explicit ConfigFollowerGetSnapshotRequest(Version version, ConfigClassSet const& configClassSet)
	  : version(version), configClassSet(configClassSet) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, configClassSet, reply);
	}
};

struct VersionedConfigMutationRef {
	Version version;
	ConfigMutationRef mutation;

	VersionedConfigMutationRef() = default;
	explicit VersionedConfigMutationRef(Arena& arena, Version version, ConfigMutationRef mutation)
	  : version(version), mutation(arena, mutation) {}
	explicit VersionedConfigMutationRef(Arena& arena, VersionedConfigMutationRef const& rhs)
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
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;

	ConfigFollowerGetChangesReply() : mostRecentVersion(0) {}
	explicit ConfigFollowerGetChangesReply(Version mostRecentVersion,
	                                       Standalone<VectorRef<VersionedConfigMutationRef>> const& versionedMutations)
	  : mostRecentVersion(mostRecentVersion), versionedMutations(versionedMutations) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mostRecentVersion, versionedMutations);
	}
};

struct ConfigFollowerGetChangesRequest {
	static constexpr FileIdentifier file_identifier = 178935;
	Version lastSeenVersion;
	ConfigClassSet configClassSet;
	ReplyPromise<ConfigFollowerGetChangesReply> reply;

	ConfigFollowerGetChangesRequest() : lastSeenVersion(::invalidVersion) {}
	explicit ConfigFollowerGetChangesRequest(Version lastSeenVersion, ConfigClassSet const& configClassSet)
	  : lastSeenVersion(lastSeenVersion), configClassSet(configClassSet) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, lastSeenVersion, configClassSet, reply);
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

class ConfigFollowerInterface {
	UID _id;

public:
	static constexpr FileIdentifier file_identifier = 7721102;
	RequestStream<ConfigFollowerGetVersionRequest> getVersion;
	RequestStream<ConfigFollowerGetSnapshotRequest> getSnapshot;
	RequestStream<ConfigFollowerGetChangesRequest> getChanges;
	RequestStream<ConfigFollowerCompactRequest> compact;

	ConfigFollowerInterface();
	void setupWellKnownEndpoints();
	ConfigFollowerInterface(NetworkAddress const& remote);
	bool operator==(ConfigFollowerInterface const& rhs) const;
	bool operator!=(ConfigFollowerInterface const& rhs) const;
	UID id() const { return _id; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, _id, getVersion, getSnapshot, getChanges);
	}
};
