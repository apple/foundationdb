/*
 * DataDistributionConfig.actor.cpp
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

#include "fdbclient/DataDistributionConfig.actor.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "flow/actorcompiler.h" // This must be the last #include.

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
			ranges.push_back(std::move(range));
		}
	}

	doc["numConfiguredRanges"] = configuredRanges;
	doc["numDefaultRanges"] = defaultRanges;
	doc["numBoundaries"] = (int)config.map.size();

	return doc;
}
