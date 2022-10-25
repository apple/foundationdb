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

class LatencyBandsMap {
	std::string name;
	UID id;
	double loggingInterval;
	int maxSize;

	TransactionTagMap<LatencyBands> map;
	// Manually added thresholds (does not include "infinite" threshold automatically
	// added by LatencyBands)
	std::vector<double> thresholds;

	LatencyBands& getLatencyBands(TransactionTag tag);

public:
	LatencyBandsMap(std::string const& name, UID id, double loggingInterval, int maxSize)
	  : name(name), id(id), loggingInterval(loggingInterval), maxSize(maxSize) {}

	void addMeasurement(TransactionTag tag, double measurement);
	void addThreshold(double value);
	void clear();
};
