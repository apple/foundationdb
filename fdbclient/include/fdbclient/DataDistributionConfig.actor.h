/*
 * DataDistributionConfig.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_G_H)
#define FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_G_H
#include "fdbclient/DataDistributionConfig.actor.g.h"
#elif !defined(FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_H)
#define FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_H

#include "flow/serialize.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// DD Configuration for a key range range.
// There are many properties here, all Optional, because for a given key range range a property
// can be unspecified which means it continues the setting of the preceding range.
struct DDRangeConfig {
	constexpr static FileIdentifier file_identifier = 9193856;

	bool forceBoundary;
	Optional<int> replicationFactor;

	DDRangeConfig update(DDRangeConfig const& update) const {
		DDRangeConfig result = *this;
		if (update.forceBoundary) {
			result.forceBoundary = true;
		}
		if (update.replicationFactor.present()) {
			result.replicationFactor = update.replicationFactor;
		}

		return result;
	}

	bool canMerge(DDRangeConfig const& next) const {
		// If either range has a forced boundary, they cannot merge
		if (forceBoundary || next.forceBoundary) {
			return false;
		}

		return replicationFactor == next.replicationFactor;
	}

	// String description of the range config
	std::string toString() const {
		return fmt::format("forceBoundary={} replication={}", forceBoundary, replicationFactor);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, forceBoundary, replicationFactor);
	}
};

template <>
struct Traceable<DDRangeConfig> : std::true_type {
	static std::string toString(const DDRangeConfig& c) { return c.toString(); }
};

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
	RangeConfigMap userRangeConfig() const { return { subSpace.pack(__FUNCTION__sr), trigger, IncludeVersion() }; }

	Future<RangeConfigMapSnapshot> userRangeConfigSnapshot(Reference<ReadYourWritesTransaction> tr) const {
		return readRangeMap(tr, userRangeConfig());
	}

	Future<RangeConfigMapSnapshot> userRangeConfigSnapshot(Database cx) const {
		return runTransaction(cx.getReference(),
		                      [=](Reference<ReadYourWritesTransaction> tr) { return userRangeConfigSnapshot(tr); });
	}

private:
	// Read all key ranges of a RangeConfigMap and return it as a RangeConfigMapSnapshot
	// allKeys.begin/end will be set to default values in the result if they do not exist in the database
	ACTOR static Future<RangeConfigMapSnapshot> readRangeMap(Reference<ReadYourWritesTransaction> tr,
	                                                         RangeConfigMap map);
};

#include "flow/unactorcompiler.h"
#endif
