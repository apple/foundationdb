/*
 * DataDistributionConfig.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/DataDistributionConfig.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/json_spirit/json_spirit_value.h"

namespace {
void checkRangeReplicationRequest(Database const& cx, KeyRangeRef range) {
	if (!cx->apiVersion.hasRangeReplication()) {
		throw unsupported_operation();
	}
	if (!allKeys.contains(range)) {
		throw key_outside_legal_range();
	}
}

Future<Void> updateRangeReplication(Database cx, KeyRange range, Optional<int> replicas) {
	checkRangeReplicationRequest(cx, range);
	if (replicas.present() && *replicas <= 0) {
		throw invalid_option_value();
	}
	if (range.empty()) {
		co_return;
	}
	co_await runTransaction(SystemDBWriteLockedNow(cx.getReference()), [range, replicas](auto tr) -> Future<Void> {
		auto config = DDConfiguration().userRangeConfig();
		auto snapshot = co_await config.getSnapshot(tr, range.begin, range.end);
		// Rewriting an unchanged boundary would fire the DD config watch
		for (auto entry : snapshot->ranges()) {
			if (entry.value().replicationFactor != replicas) {
				co_await DDConfiguration::RangeConfigMap::modifyRange(
				    config, tr, range.begin, range.end, [replicas](DDRangeConfig value) {
					    value.replicationFactor = replicas;
					    return value;
				    });
				co_return;
			}
		}
	});
}
} // namespace

Future<Void> setRangeReplication(Database cx, KeyRange range, int replicas) {
	return updateRangeReplication(cx, range, replicas);
}

Future<Void> clearRangeReplication(Database cx, KeyRange range) {
	return updateRangeReplication(cx, range, {});
}

Future<std::vector<RangeReplicationOverride>> getRangeReplicationOverrides(Database cx, KeyRange range) {
	checkRangeReplicationRequest(cx, range);
	std::vector<RangeReplicationOverride> overrides;
	if (range.empty()) {
		co_return overrides;
	}
	auto snapshot = co_await DDConfiguration().userRangeConfig().getSnapshot(
	    SystemDB(cx.getReference(), /*write=*/false, /*lockAware=*/true, /*immediate=*/true), range.begin, range.end);
	for (auto entry : snapshot->ranges()) {
		auto replicas = entry.value().replicationFactor;
		if (!replicas.present()) {
			continue;
		}
		auto bounds = entry.range();
		KeyRangeRef clipped = range & KeyRangeRef(bounds.begin, bounds.end);
		if (!overrides.empty() && overrides.back().range.end == clipped.begin &&
		    overrides.back().replicas == *replicas) {
			overrides.back().range = KeyRangeRef(overrides.back().range.begin, clipped.end);
		} else {
			overrides.push_back({ clipped, *replicas });
		}
	}
	co_return overrides;
}

json_spirit::mValue DDConfiguration::toJSON(RangeConfigMapSnapshot const& config, bool includeDefaultRanges) {
	json_spirit::mObject doc;
	json_spirit::mArray& ranges = (doc["ranges"] = json_spirit::mArray()).get_array();
	int defaultRanges = 0;
	int configuredRanges = 0;

	// Range config with no options set
	DDRangeConfig defaultRangeConfig;

	// Add each non-default configured range to the output ranges doc
	for (auto const& rv : config.ranges()) {
		bool configured = rv.value() != defaultRangeConfig;
		if (configured) {
			++configuredRanges;
		} else {
			++defaultRanges;
		}

		if (includeDefaultRanges || configured) {
			json_spirit::mObject range;
			range["begin"] = rv.range().begin.toString();
			range["end"] = rv.range().end.toString();
			range["configuration"] = rv.value().toJSON();
			ranges.push_back(range);
		}
	}

	doc["numConfiguredRanges"] = configuredRanges;
	doc["numDefaultRanges"] = defaultRanges;
	doc["numBoundaries"] = (int)config.map.size();

	return doc;
}
