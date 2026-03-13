/*
 * KnobProtectiveGroups.cpp
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

#include "fdbserver/KnobProtectiveGroups.h"

#include <array>

#include "flow/Knobs.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ServerKnobCollection.h"
#include "fdbserver/Knobs.h"

void KnobKeyValuePairs::set(std::string const& name, ParsedKnobValue const value) {
	ASSERT(!knobs.contains(name));

	knobs[name] = value;
}

KnobKeyValuePairs::container_t const& KnobKeyValuePairs::getKnobs() const {
	return knobs;
}

KnobProtectiveGroup::KnobProtectiveGroup(KnobKeyValuePairs const& overriddenKnobKeyValuePairs_)
  : overriddenKnobs(overriddenKnobKeyValuePairs_) {
	snapshotOriginalKnobs();
	assignKnobs(overriddenKnobs);
}

KnobProtectiveGroup::~KnobProtectiveGroup() {
	assignKnobs(originalKnobs);
}

void KnobProtectiveGroup::snapshotOriginalKnobs() {
	for (auto const& [name, _] : overriddenKnobs.getKnobs()) {
		ParsedKnobValue value = CLIENT_KNOBS->getKnob(name);
		if (std::get_if<NoKnobFound>(&value)) {
			value = SERVER_KNOBS->getKnob(name);
		}
		if (std::get_if<NoKnobFound>(&value)) {
			value = FLOW_KNOBS->getKnob(name);
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

void KnobProtectiveGroup::assignKnobs(KnobKeyValuePairs const& overrideKnobs) {
	auto& mutableServerKnobs = dynamic_cast<ServerKnobCollection&>(IKnobCollection::getMutableGlobalKnobCollection());

	for (auto const& [name, value] : overrideKnobs.getKnobs()) {
		Standalone<KnobValueRef> valueRef = KnobValueRef::create(value);
		bool success = mutableServerKnobs.trySetKnob(name, valueRef);
		if (!success) {
			TraceEvent(SevError, "FailedToAssignKnob")
			    .detail("KnobName", name)
			    .detail("KnobValue", valueRef.toString());
		}
		ASSERT(success);
		TraceEvent(SevInfo, "AssignKnobValue").detail("KnobName", name).detail("KnobValue", valueRef.toString());
	}
}
