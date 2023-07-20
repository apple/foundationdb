/*
 * LatencyBandsMap.h
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

#include "fdbclient/TagThrottle.actor.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/Knobs.h"

class LatencyBandsMap {
	friend class LatencyBandsMapImpl;

	std::string name;
	UID id;
	double loggingInterval;
	int maxSize;
	Future<Void> expireOldTags;

	struct ExpirableBands {
		LatencyBands latencyBands;
		double lastUpdated;

		explicit ExpirableBands(LatencyBands&&);
	};

	TransactionTagMap<ExpirableBands> map;
	// Manually added thresholds (does not include "infinite" threshold automatically
	// added by LatencyBands)
	std::vector<double> thresholds;

	// Get or create an LatencyBands object stored in map.
	// Updates the lastUpdated field corresponding to this LatencyBands object.
	// Returns pointer to this object, or an empty optional if object
	// cannot be created.
	Optional<LatencyBands*> getLatencyBands(TransactionTag tag);

public:
	LatencyBandsMap(std::string const& name, UID id, double loggingInterval, int maxSize);

	void addMeasurement(TransactionTag tag, double measurement, int count = 1);
	void addThreshold(double value);
	int size() const { return map.size(); }
};
