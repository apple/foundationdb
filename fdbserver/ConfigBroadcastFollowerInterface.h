/*
 * ConfigBroadcastFollowerInterface.h
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

#include "fdbserver/ConfigFollowerInterface.h"

class ConfigClassSet {
	std::set<Key> classes;

public:
	static constexpr FileIdentifier file_identifier = 9854021;

	bool operator==(ConfigClassSet const& rhs) const { return classes == rhs.classes; }
	bool operator!=(ConfigClassSet const& rhs) const { return !(*this == rhs); }

	ConfigClassSet() = default;
	ConfigClassSet(VectorRef<KeyRef> configClasses) {
		for (const auto& configClass : configClasses) {
			classes.insert(configClass);
		}
	}

	bool contains(KeyRef configClass) const { return classes.count(configClass); }
	std::set<Key> const& getClasses() const { return classes; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, classes);
	}
};

template <>
struct Traceable<ConfigClassSet> : std::true_type {
	static std::string toString(ConfigClassSet const& value) { return describe(value.getClasses()); }
};

struct ConfigBroadcastFollowerGetSnapshotReply {
	static constexpr FileIdentifier file_identifier = 8701983;
	Version version{ 0 };
	std::map<ConfigKey, KnobValue> snapshot;

	ConfigBroadcastFollowerGetSnapshotReply() = default;
	template <class Snapshot>
	explicit ConfigBroadcastFollowerGetSnapshotReply(Version version, Snapshot&& snapshot)
	  : version(version), snapshot(std::forward<Snapshot>(snapshot)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, snapshot);
	}
};

struct ConfigBroadcastFollowerGetSnapshotRequest {
	static constexpr FileIdentifier file_identifier = 10911924;
	ConfigClassSet configClassSet;
	ReplyPromise<ConfigBroadcastFollowerGetSnapshotReply> reply;

	ConfigBroadcastFollowerGetSnapshotRequest() = default;
	explicit ConfigBroadcastFollowerGetSnapshotRequest(ConfigClassSet const& configClassSet)
	  : configClassSet(configClassSet) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, configClassSet, reply);
	}
};

struct ConfigBroadcastFollowerGetChangesReply {
	static constexpr FileIdentifier file_identifier = 4014927;
	Version mostRecentVersion;
	Standalone<VectorRef<VersionedConfigMutationRef>> changes;

	ConfigBroadcastFollowerGetChangesReply()=default;
	explicit ConfigBroadcastFollowerGetChangesReply(Version mostRecentVersion, Standalone<VectorRef<VersionedConfigMutationRef>> const& changes)
		: mostRecentVersion(mostRecentVersion), changes(changes) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mostRecentVersion, changes);
	}
};

struct ConfigBroadcastFollowerGetChangesRequest {
	static constexpr FileIdentifier file_identifier = 601280;
	Version lastSeenVersion;
	ConfigClassSet configClassSet;
	ReplyPromise<ConfigBroadcastFollowerGetChangesReply> reply;

	ConfigBroadcastFollowerGetChangesRequest() = default;
	explicit ConfigBroadcastFollowerGetChangesRequest(Version lastSeenVersion, ConfigClassSet const& configClassSet)
	  : lastSeenVersion(lastSeenVersion), configClassSet(configClassSet) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, lastSeenVersion, configClassSet, reply);
	}
};

/*
 * The ConfigBroadcaster serves a ConfigBroadcastFollowerInterface which all
 * workers use to fetch updates.
 */
class ConfigBroadcastFollowerInterface {
	UID _id;

public:
	static constexpr FileIdentifier file_identifier = 1984391;
	RequestStream<ConfigBroadcastFollowerGetSnapshotRequest> getSnapshot;
	RequestStream<ConfigBroadcastFollowerGetChangesRequest> getChanges;

	ConfigBroadcastFollowerInterface() : _id(deterministicRandom()->randomUniqueID()) {}

	bool operator==(ConfigBroadcastFollowerInterface const& rhs) const { return (_id == rhs._id); }
	bool operator!=(ConfigBroadcastFollowerInterface const& rhs) const { return !(*this == rhs); }
	UID id() const { return _id; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, _id, getSnapshot, getChanges);
	}
};
