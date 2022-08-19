/*
 * KnobProtectiveGroups.cpp
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

#include "fdbserver/KnobProtectiveGroups.h"

#include <array>

#include "fdbclient/Knobs.h"
#include "fdbclient/ServerKnobCollection.h"
#include "fdbserver/Knobs.h"

void KnobKeyValuePairs::set(const std::string& name, const ParsedKnobValue value) {
	ASSERT(knobs.count(name) == 0);

	knobs[name] = value;
}

const KnobKeyValuePairs::container_t& KnobKeyValuePairs::getKnobs() const {
	return knobs;
}

KnobProtectiveGroup::KnobProtectiveGroup(const KnobKeyValuePairs& overriddenKnobKeyValuePairs_)
  : overriddenKnobs(overriddenKnobKeyValuePairs_) {
	snapshotOriginalKnobs();
	assignKnobs(overriddenKnobs);
}

KnobProtectiveGroup::~KnobProtectiveGroup() {
	assignKnobs(originalKnobs);
}

void KnobProtectiveGroup::snapshotOriginalKnobs() {
	for (const auto& [name, _] : overriddenKnobs.getKnobs()) {
		ParsedKnobValue value = CLIENT_KNOBS->getKnob(name);
		if (std::get_if<NoKnobFound>(&value)) {
			value = SERVER_KNOBS->getKnob(name);
		}
		if (std::get_if<NoKnobFound>(&value)) {
			ASSERT(false);
		}
		originalKnobs.set(name, value);
		TraceEvent(SevInfo, "SnapshotKnobValue")
		    .detail("KnobName", name)
		    .detail("KnobValue", KnobValueRef::create(value).toString());
	}
}

void KnobProtectiveGroup::assignKnobs(const KnobKeyValuePairs& overrideKnobs) {
	auto& mutableServerKnobs = dynamic_cast<ServerKnobCollection&>(IKnobCollection::getMutableGlobalKnobCollection());

	for (const auto& [name, value] : overrideKnobs.getKnobs()) {
		Standalone<KnobValueRef> valueRef = KnobValueRef::create(value);
		ASSERT(mutableServerKnobs.trySetKnob(name, valueRef));
		TraceEvent(SevInfo, "AssignKnobValue").detail("KnobName", name).detail("KnobValue", valueRef.toString());
	}
}