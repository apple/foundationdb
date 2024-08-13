/*
 * ClientKnobCollection.cpp
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

#include "fdbclient/ClientKnobCollection.h"

ClientKnobCollection::ClientKnobCollection(Randomize randomize, IsSimulated isSimulated)
  : flowKnobs(randomize, isSimulated), clientKnobs(randomize) {}

void ClientKnobCollection::initialize(Randomize randomize, IsSimulated isSimulated) {
	flowKnobs.initialize(randomize, isSimulated);
	clientKnobs.initialize(randomize);
}

void ClientKnobCollection::reset(Randomize randomize, IsSimulated isSimulated) {
	flowKnobs.reset(randomize, isSimulated);
	clientKnobs.reset(randomize);
}

Optional<KnobValue> ClientKnobCollection::tryParseKnobValue(std::string const& knobName,
                                                            std::string const& knobValue) const {
	auto parsedKnobValue = flowKnobs.parseKnobValue(knobName, knobValue);
	if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
		return KnobValueRef::create(parsedKnobValue);
	}
	parsedKnobValue = clientKnobs.parseKnobValue(knobName, knobValue);
	if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
		return KnobValueRef::create(parsedKnobValue);
	}
	return {};
}

bool ClientKnobCollection::trySetKnob(std::string const& knobName, KnobValueRef const& knobValue) {
	return knobValue.visitSetKnob(knobName, flowKnobs) || knobValue.visitSetKnob(knobName, clientKnobs);
}

bool ClientKnobCollection::isAtomic(std::string const& knobName) const {
	return flowKnobs.isAtomic(knobName) || clientKnobs.isAtomic(knobName);
}
