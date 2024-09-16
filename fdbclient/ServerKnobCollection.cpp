/*
 * ServerKnobCollection.cpp
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

#include "fdbclient/ServerKnobCollection.h"

ServerKnobCollection::ServerKnobCollection(Randomize randomize, IsSimulated isSimulated)
  : clientKnobCollection(randomize, isSimulated),
    serverKnobs(randomize, &clientKnobCollection.getMutableClientKnobs(), isSimulated) {}

void ServerKnobCollection::initialize(Randomize randomize, IsSimulated isSimulated) {
	clientKnobCollection.initialize(randomize, isSimulated);
	serverKnobs.initialize(randomize, &clientKnobCollection.getMutableClientKnobs(), isSimulated);
}

void ServerKnobCollection::reset(Randomize randomize, IsSimulated isSimulated) {
	clientKnobCollection.reset(randomize, isSimulated);
	serverKnobs.reset(randomize, &clientKnobCollection.getMutableClientKnobs(), isSimulated);
}

Optional<KnobValue> ServerKnobCollection::tryParseKnobValue(std::string const& knobName,
                                                            std::string const& knobValue) const {
	auto result = clientKnobCollection.tryParseKnobValue(knobName, knobValue);
	if (result.present()) {
		return result;
	}
	auto parsedKnobValue = serverKnobs.parseKnobValue(knobName, knobValue);
	if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
		return KnobValueRef::create(parsedKnobValue);
	}
	return {};
}

bool ServerKnobCollection::trySetKnob(std::string const& knobName, KnobValueRef const& knobValue) {
	return clientKnobCollection.trySetKnob(knobName, knobValue) || knobValue.visitSetKnob(knobName, serverKnobs);
}

bool ServerKnobCollection::isAtomic(std::string const& knobName) const {
	return clientKnobCollection.isAtomic(knobName) || serverKnobs.isAtomic(knobName);
}
