/*
 * Knobs.h
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

#pragma once

#include <vector>

#include "fdbclient/ClientKnobs.h"
#include "fdbclient/IKnobCollection.h"

extern ClientKnobs const* CLIENT_KNOBS;

inline const ClientKnobs& getClientKnobs() {
	return *CLIENT_KNOBS;
}

void resetClientKnobs(Randomize randomize, IsSimulated isSimulated);
void initializeClientKnobs(Randomize randomize, IsSimulated isSimulated);
Optional<KnobValue> tryParseClientKnobValue(std::string const& knobName, std::string const& knobValue);
KnobValue parseClientKnobValue(std::string const& knobName, std::string const& knobValue);
bool trySetClientKnob(std::string const& knobName, KnobValueRef const& knobValue);
void setClientKnob(std::string const& knobName, KnobValueRef const& knobValue);
void setupClientKnobs(std::vector<std::pair<std::string, std::string>> const& knobs);
