/*
 * ClientKnobCollection.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

class ClientKnobCollection : public IKnobCollection {
	FlowKnobs flowKnobs;
	ClientKnobs clientKnobs;

public:
	ClientKnobCollection(bool randomize = false, bool isSimulated = false) { initialize(randomize, isSimulated); }

	void initialize(bool randomize = false, bool isSimulated = false) override {
		flowKnobs.initialize(randomize, isSimulated);
		clientKnobs.initialize(randomize);
	}

	void reset(bool randomize = false, bool isSimulated = false) override {
		flowKnobs.reset(randomize, isSimulated);
		clientKnobs.reset(randomize);
	}

	FlowKnobs const& getFlowKnobs() const override { return flowKnobs; }

	FlowKnobs& getMutableFlowKnobs() { return flowKnobs; }

	ClientKnobs const& getClientKnobs() const override { return clientKnobs; }

	ClientKnobs& getMutableClientKnobs() { return clientKnobs; }

	ServerKnobs const& getServerKnobs() const override { throw internal_error(); }

	TestKnobs const& getTestKnobs() const override { throw internal_error(); }

	KnobValue parseKnobValue(std::string const& knobName, std::string const& knobValue) const override {
		auto parsedKnobValue = flowKnobs.parseKnobValue(knobName, knobValue);
		if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
			return KnobValueRef::create(parsedKnobValue);
		}
		parsedKnobValue = clientKnobs.parseKnobValue(knobName, knobValue);
		if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
			return KnobValueRef::create(parsedKnobValue);
		}
		throw invalid_option();
	}

	void setKnob(std::string const& knobName, KnobValueRef const& knobValue) override {
		// Assert because we should validate inputs beforehand
		ASSERT(knobValue.setKnob(knobName, flowKnobs) || knobValue.setKnob(knobName, clientKnobs));
	}
};

std::unique_ptr<IKnobCollection> createClientKnobs(bool randomize = false, bool isSimulated = false) {
	return std::make_unique<ClientKnobCollection>(randomize, isSimulated);
}
