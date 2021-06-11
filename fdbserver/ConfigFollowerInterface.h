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

struct VersionedConfigMutationRef {
	Version version;
	ConfigMutationRef mutation;

	VersionedConfigMutationRef() = default;
	explicit VersionedConfigMutationRef(Arena& arena, Version version, ConfigMutationRef mutation)
	  : version(version), mutation(arena, mutation) {}
	explicit VersionedConfigMutationRef(Arena& arena, VersionedConfigMutationRef const& rhs)
	  : version(rhs.version), mutation(arena, rhs.mutation) {}

	size_t expectedSize() const { return mutation.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, mutation);
	}
};
using VersionedConfigMutation = Standalone<VersionedConfigMutationRef>;

struct VersionedConfigCommitAnnotationRef {
	Version version;
	ConfigCommitAnnotationRef annotation;

	VersionedConfigCommitAnnotationRef() = default;
	explicit VersionedConfigCommitAnnotationRef(Arena& arena, Version version, ConfigCommitAnnotationRef annotation)
	  : version(version), annotation(arena, annotation) {}
	explicit VersionedConfigCommitAnnotationRef(Arena& arena, VersionedConfigCommitAnnotationRef rhs)
	  : version(rhs.version), annotation(arena, rhs.annotation) {}

	size_t expectedSize() const { return annotation.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, annotation);
	}
};
using VersionedConfigCommitAnnotation = Standalone<VersionedConfigCommitAnnotationRef>;

struct ConfigFollowerGetSnapshotAndChangesReply {
	static constexpr FileIdentifier file_identifier = 1734095;
	Version snapshotVersion;
	Version changesVersion;
	std::map<ConfigKey, KnobValue> snapshot;
	// TODO: Share arena
	Standalone<VectorRef<VersionedConfigMutationRef>> changes;
	Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations;

	ConfigFollowerGetSnapshotAndChangesReply() = default;
	template <class Snapshot>
	explicit ConfigFollowerGetSnapshotAndChangesReply(
	    Version snapshotVersion,
	    Version changesVersion,
	    Snapshot&& snapshot,
	    Standalone<VectorRef<VersionedConfigMutationRef>> changes,
	    Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations)
	  : snapshotVersion(snapshotVersion), changesVersion(changesVersion), snapshot(std::forward<Snapshot>(snapshot)),
	    changes(changes), annotations(annotations) {
		ASSERT_GE(changesVersion, snapshotVersion);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, snapshotVersion, changesVersion, snapshot, changes);
	}
};

struct ConfigFollowerGetSnapshotAndChangesRequest {
	static constexpr FileIdentifier file_identifier = 294811;
	ReplyPromise<ConfigFollowerGetSnapshotAndChangesReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct ConfigFollowerGetChangesReply {
	static constexpr FileIdentifier file_identifier = 234859;
	Version mostRecentVersion;
	// TODO: Share arena
	Standalone<VectorRef<VersionedConfigMutationRef>> changes;
	Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations;

	ConfigFollowerGetChangesReply() : mostRecentVersion(0) {}
	explicit ConfigFollowerGetChangesReply(Version mostRecentVersion,
	                                       Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
	                                       Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations)
	  : mostRecentVersion(mostRecentVersion), changes(changes), annotations(annotations) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mostRecentVersion, changes, annotations);
	}
};

struct ConfigFollowerGetChangesRequest {
	static constexpr FileIdentifier file_identifier = 178935;
	Version lastSeenVersion{ 0 };
	ReplyPromise<ConfigFollowerGetChangesReply> reply;

	ConfigFollowerGetChangesRequest() = default;
	explicit ConfigFollowerGetChangesRequest(Version lastSeenVersion) : lastSeenVersion(lastSeenVersion) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, lastSeenVersion, reply);
	}
};

struct ConfigFollowerCompactRequest {
	static constexpr FileIdentifier file_identifier = 568910;
	Version version{ 0 };
	ReplyPromise<Void> reply;

	ConfigFollowerCompactRequest() = default;
	explicit ConfigFollowerCompactRequest(Version version) : version(version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, reply);
	}
};

/*
 * Configuration database nodes serve a ConfigFollowerInterface which contains well known endpoints,
 * used by workers to receive configuration database updates
 */
class ConfigFollowerInterface {
	UID _id;

public:
	static constexpr FileIdentifier file_identifier = 7721102;
	RequestStream<ConfigFollowerGetSnapshotAndChangesRequest> getSnapshotAndChanges;
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
		serializer(ar, _id, getSnapshotAndChanges, getChanges, compact);
	}
};
