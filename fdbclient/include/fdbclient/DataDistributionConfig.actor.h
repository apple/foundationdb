/*
 * DataDistributionConfig.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_G_H)
#define FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_G_H
#include "fdbclient/DataDistributionConfig.actor.g.h"
#elif !defined(FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_H)
#define FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_H

#include "flow/serialize.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/KeyBackedRangeMap.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// DD Configuration for a key range range.
// There are many properties here, all Optional, because for a given key range range a property
// can be unspecified which means it continues the setting of the preceding range.
struct DDRangeConfig {
	constexpr static FileIdentifier file_identifier = 9193856;

	DDRangeConfig(Optional<int> replicationFactor = {}, Optional<int> teamID = {})
	  : replicationFactor(replicationFactor), teamID(teamID) {}

	Optional<int> replicationFactor;
	// Ranges with different team IDs should be assigned to different storage teams
	// Shards with the same team ID can be assigned to the same storage team but nothing enforces/prefers this.
	Optional<int> teamID;

	DDRangeConfig apply(DDRangeConfig const& update) const {
		DDRangeConfig result = *this;
		if (update.replicationFactor.present()) {
			result.replicationFactor = update.replicationFactor;
		}
		if (update.teamID.present()) {
			result.teamID = update.teamID;
		}

		return result;
	}

	bool operator==(DDRangeConfig const& rhs) const = default;

	// String description of the range config
	std::string toString() const { return fmt::format("replication={} teamID={}", replicationFactor, teamID); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, replicationFactor, teamID);
	}

	json_spirit::mObject toJSON() const {
		json_spirit::mObject doc;
		if (teamID.present()) {
			doc["teamID"] = *teamID;
		}
		if (replicationFactor.present()) {
			doc["replicationFactor"] = *replicationFactor;
		}
		return doc;
	}
};

template <>
struct Traceable<DDRangeConfig> : std::true_type {
	static std::string toString(const DDRangeConfig& c) { return c.toString(); }
};

template <>
struct fmt::formatter<DDRangeConfig> : FormatUsingTraceable<DDRangeConfig> {};

struct DDConfiguration : public KeyBackedClass {
	DDConfiguration(KeyRef prefix = SystemKey("\xff\x02/ddconfig/"_sr)) : KeyBackedClass(prefix) {}

	// RangeConfigMap is a  KeyBackedRangeMap of DDRangeConfig values describing various option overrides for key ranges
	typedef KeyBackedRangeMap<Key,
	                          DDRangeConfig,
	                          TupleCodec<Key>,
	                          ObjectCodec<DDRangeConfig, decltype(IncludeVersion())>>
	    RangeConfigMap;

	typedef RangeConfigMap::LocalSnapshot RangeConfigMapSnapshot;

	// Range configuration options set by Users
	RangeConfigMap userRangeConfig() const { return { subspace.pack(__FUNCTION__sr), trigger, IncludeVersion() }; }

	static json_spirit::mValue toJSON(RangeConfigMapSnapshot const& snapshot, bool includeDefaultRanges = false);
};

#include "flow/unactorcompiler.h"
#endif
