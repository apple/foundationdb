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

class ServerKnobCollection : public IKnobCollection {
	ClientKnobCollection clientKnobCollection;
	ServerKnobs serverKnobs;
	TestKnobs testKnobs;

public:
	ServerKnobCollection(bool randomize, bool isSimulated) : clientKnobCollection(randomize, isSimulated) {
		initialize(randomize, isSimulated);
	}

	void initialize(bool randomize = false, bool isSimulated = false) override {
		clientKnobCollection.initialize(randomize, isSimulated);
		serverKnobs.initialize(randomize, &clientKnobCollection.getMutableClientKnobs(), isSimulated);
		testKnobs.initialize();
	}

	void reset(bool randomize = false, bool isSimulated = false) override {
		clientKnobCollection.reset(randomize, isSimulated);
		serverKnobs.reset(randomize, &clientKnobCollection.getMutableClientKnobs(), isSimulated);
		testKnobs.reset();
	}

	FlowKnobs const& getFlowKnobs() const override { return clientKnobCollection.getFlowKnobs(); }

	ClientKnobs const& getClientKnobs() const override { return clientKnobCollection.getClientKnobs(); }

	ServerKnobs const& getServerKnobs() const override { return serverKnobs; }

	TestKnobs const& getTestKnobs() const override { return testKnobs; }

	// TODO: Reduce duplicate code
	KnobValue parseKnobValue(std::string const& knobName, std::string const& knobValue) const override {
		auto parsedKnobValue = getFlowKnobs().parseKnobValue(knobName, knobValue);
		if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
			return KnobValueRef::create(parsedKnobValue);
		}
		parsedKnobValue = getClientKnobs().parseKnobValue(knobName, knobValue);
		if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
			return KnobValueRef::create(parsedKnobValue);
		}
		parsedKnobValue = serverKnobs.parseKnobValue(knobName, knobValue);
		if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
			return KnobValueRef::create(parsedKnobValue);
		}
		parsedKnobValue = testKnobs.parseKnobValue(knobName, knobValue);
		if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
			return KnobValueRef::create(parsedKnobValue);
		}
		throw invalid_option();
	}

	void setKnob(std::string const& knobName, KnobValueRef const& knobValue) override {
		// Assert because we should validate inputs beforehand
		ASSERT(knobValue.setKnob(knobName, clientKnobCollection.getMutableFlowKnobs()) ||
		       knobValue.setKnob(knobName, clientKnobCollection.getMutableClientKnobs()) ||
		       knobValue.setKnob(knobName, serverKnobs) || knobValue.setKnob(knobName, testKnobs));
	}
};

std::unique_ptr<IKnobCollection> createServerKnobs(bool randomize = false, bool isSimulated = false) {
	return std::make_unique<ServerKnobCollection>(randomize, isSimulated);
}
