/*
 * KnobProtectiveGroups.h
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

#ifndef FDBSERVER_KNOBPROTECTIVEGROUPS_H
#define FDBSERVER_KNOBPROTECTIVEGROUPS_H

#include <array>
#include <unordered_map>

#include "fdbclient/IKnobCollection.h"

// A list of knob key value pairs
class KnobKeyValuePairs {
public:
	using container_t = std::unordered_map<std::string, ParsedKnobValue>;

private:
	// Here the knob value is directly stored, unlike KnobValue, for simplicity
	container_t knobs;

public:
	// Sets a value for a given knob
	void set(const std::string& name, const ParsedKnobValue value);

	// Gets a list of knobs for given type
	const container_t& getKnobs() const;
};

// For knobs, temporarily change the values, the original values will be recovered
class KnobProtectiveGroup {
	KnobKeyValuePairs overriddenKnobs;
	KnobKeyValuePairs originalKnobs;

	// Snapshots the current knob values base on those knob keys in overriddenKnobs
	void snapshotOriginalKnobs();

	void assignKnobs(const KnobKeyValuePairs& overrideKnobs);

public:
	KnobProtectiveGroup(const KnobKeyValuePairs& overridenKnobs_);
	~KnobProtectiveGroup();
};

#endif // FDBSERVER_KNOBPROTECTIVEGROUPS_H