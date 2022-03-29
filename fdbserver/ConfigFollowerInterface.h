/*
 * ConfigFollowerInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
	std::map<ConfigKey, KnobValue> snapshot;
	// TODO: Share arena
	Standalone<VectorRef<VersionedConfigMutationRef>> changes;
	Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations;

	ConfigFollowerGetSnapshotAndChangesReply() = default;
	template <class Snapshot>
	explicit ConfigFollowerGetSnapshotAndChangesReply(
	    Version snapshotVersion,
	    Snapshot&& snapshot,
	    Standalone<VectorRef<VersionedConfigMutationRef>> changes,
	    Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations)
	  : snapshotVersion(snapshotVersion), snapshot(std::forward<Snapshot>(snapshot)), changes(changes),
	    annotations(annotations) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, snapshotVersion, snapshot, changes);
	}
};

struct ConfigFollowerGetSnapshotAndChangesRequest {
	static constexpr FileIdentifier file_identifier = 294811;
	ReplyPromise<ConfigFollowerGetSnapshotAndChangesReply> reply;
	Version mostRecentVersion;

	ConfigFollowerGetSnapshotAndChangesRequest() = default;
	explicit ConfigFollowerGetSnapshotAndChangesRequest(Version mostRecentVersion)
	  : mostRecentVersion(mostRecentVersion) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply, mostRecentVersion);
	}
};

struct ConfigFollowerGetChangesReply {
	static constexpr FileIdentifier file_identifier = 234859;
	// TODO: Share arena
	Standalone<VectorRef<VersionedConfigMutationRef>> changes;
	Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations;

	ConfigFollowerGetChangesReply() = default;
	explicit ConfigFollowerGetChangesReply(Version mostRecentVersion,
	                                       Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
	                                       Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations)
	  : changes(changes), annotations(annotations) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, changes, annotations);
	}
};

struct ConfigFollowerGetChangesRequest {
	static constexpr FileIdentifier file_identifier = 178935;
	Version lastSeenVersion{ 0 };
	Version mostRecentVersion{ 0 };
	ReplyPromise<ConfigFollowerGetChangesReply> reply;

	ConfigFollowerGetChangesRequest() = default;
	explicit ConfigFollowerGetChangesRequest(Version lastSeenVersion, Version mostRecentVersion)
	  : lastSeenVersion(lastSeenVersion), mostRecentVersion(mostRecentVersion) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, lastSeenVersion, mostRecentVersion, reply);
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

struct ConfigFollowerRollforwardRequest {
	static constexpr FileIdentifier file_identifier = 678894;
	Optional<Version> rollback;
	Version lastKnownCommitted{ 0 };
	Version target{ 0 };
	Standalone<VectorRef<VersionedConfigMutationRef>> mutations;
	Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations;
	ReplyPromise<Void> reply;

	ConfigFollowerRollforwardRequest() = default;
	explicit ConfigFollowerRollforwardRequest(Optional<Version> rollback,
	                                          Version lastKnownCommitted,
	                                          Version target,
	                                          Standalone<VectorRef<VersionedConfigMutationRef>> mutations,
	                                          Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations)
	  : rollback(rollback), lastKnownCommitted(lastKnownCommitted), target(target), mutations(mutations),
	    annotations(annotations) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rollback, lastKnownCommitted, target, mutations, annotations, reply);
	}
};

struct ConfigFollowerGetCommittedVersionReply {
	static constexpr FileIdentifier file_identifier = 9214735;
	Version lastCompacted;
	Version lastCommitted;

	ConfigFollowerGetCommittedVersionReply() = default;
	explicit ConfigFollowerGetCommittedVersionReply(Version lastCompacted, Version lastCommitted)
	  : lastCompacted(lastCompacted), lastCommitted(lastCommitted) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, lastCompacted, lastCommitted);
	}
};

struct ConfigFollowerGetCommittedVersionRequest {
	static constexpr FileIdentifier file_identifier = 1093472;
	ReplyPromise<ConfigFollowerGetCommittedVersionReply> reply;

	ConfigFollowerGetCommittedVersionRequest() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
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
	RequestStream<ConfigFollowerRollforwardRequest> rollforward;
	RequestStream<ConfigFollowerGetCommittedVersionRequest> getCommittedVersion;

	ConfigFollowerInterface();
	void setupWellKnownEndpoints();
	ConfigFollowerInterface(NetworkAddress const& remote);
	bool operator==(ConfigFollowerInterface const& rhs) const;
	bool operator!=(ConfigFollowerInterface const& rhs) const;
	UID id() const { return _id; }
	NetworkAddress address() const { return getSnapshotAndChanges.getEndpoint().getPrimaryAddress(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, _id, getSnapshotAndChanges, getChanges, compact, rollforward, getCommittedVersion);
	}
};
