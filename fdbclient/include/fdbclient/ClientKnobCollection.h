/*
 * ClientKnobCollection.h
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

#ifndef FDBCLIENT_CLIENTKNOBCOLLECTION_H
#define FDBCLIENT_CLIENTKNOBCOLLECTION_H

#pragma once

#include "fdbclient/ClientKnobs.h"
#include "fdbclient/IKnobCollection.h"
#include "flow/Knobs.h"

/*
 * Stores both flow knobs and client knobs, attempting to access server knobs or test knobs
 * results in a run-time error
 */
class ClientKnobCollection : public IKnobCollection {
	FlowKnobs flowKnobs;
	ClientKnobs clientKnobs;

public:
	ClientKnobCollection(Randomize randomize, IsSimulated isSimulated);
	void initialize(Randomize randomize, IsSimulated isSimulated) override;
	void reset(Randomize randomize, IsSimulated isSimulated) override;
	FlowKnobs const& getFlowKnobs() const override { return flowKnobs; }
	ClientKnobs const& getClientKnobs() const override { return clientKnobs; }
	ClientKnobs& getMutableClientKnobs() { return clientKnobs; }
	ServerKnobs const& getServerKnobs() const override { throw internal_error(); }
	TestKnobs const& getTestKnobs() const override { throw internal_error(); }
	Optional<KnobValue> tryParseKnobValue(std::string const& knobName, std::string const& knobValue) const override;
	bool trySetKnob(std::string const& knobName, KnobValueRef const& knobValue) override;
	bool isAtomic(std::string const& knobName) const override;
};

#endif // FDBCLIENT_CLIENTKNOBCCOLLECTION_H