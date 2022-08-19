/*
 * ServerKnobCollection.h
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

#ifndef FDBCLIENT_SERVERKNOBCOLLECTION_H
#define FDBCLIENT_SERVERKNOBCOLLECTION_H

#pragma once

#include "fdbclient/ClientKnobCollection.h"
#include "fdbclient/IKnobCollection.h"

/*
 * Stores both flow knobs, client knobs, and server knobs. Attempting to access test knobs
 * results in a run-time error
 */
class ServerKnobCollection : public IKnobCollection {
	ClientKnobCollection clientKnobCollection;
	ServerKnobs serverKnobs;

public:
	ServerKnobCollection(Randomize randomize, IsSimulated isSimulated);
	void initialize(Randomize randomize, IsSimulated isSimulated) override;
	void reset(Randomize randomize, IsSimulated isSimulated) override;
	FlowKnobs const& getFlowKnobs() const override { return clientKnobCollection.getFlowKnobs(); }
	ClientKnobs const& getClientKnobs() const override { return clientKnobCollection.getClientKnobs(); }
	ServerKnobs const& getServerKnobs() const override { return serverKnobs; }
	TestKnobs const& getTestKnobs() const override { throw internal_error(); }
	Optional<KnobValue> tryParseKnobValue(std::string const& knobName, std::string const& knobValue) const override;
	bool trySetKnob(std::string const& knobName, KnobValueRef const& knobValue) override;
	bool isAtomic(std::string const& knobName) const override;
};

#endif // FDBCLIENT_SERVERKNOBCOLLECTION_H