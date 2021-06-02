/*
 * KnobCollection.h
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

#include <memory>

#include "fdbclient/ConfigKnobs.h"
#include "fdbclient/ServerKnobs.h"

class TestKnobs : public KnobsImpl<TestKnobs> {
public:
	TestKnobs();
	int64_t TEST_LONG;
	int TEST_INT;
	double TEST_DOUBLE;
	bool TEST_BOOL;
	std::string TEST_STRING;
	bool operator==(TestKnobs const&) const;
	bool operator!=(TestKnobs const&) const;
	void initialize();
};

class KnobCollection {
	FlowKnobs flowKnobs;
	ClientKnobs clientKnobs;
	std::unique_ptr<ServerKnobs> serverKnobs;
	std::unique_ptr<TestKnobs> testKnobs;

	struct ConstructorTag {};

public:
	KnobCollection(ConstructorTag,
	               bool randomize,
	               bool isSimulated,
	               std::unique_ptr<ServerKnobs>&& serverKnobs = {},
	               std::unique_ptr<TestKnobs>&& testKnobs = {})
	  : serverKnobs(std::move(serverKnobs)), testKnobs(std::move(testKnobs)) {
		initialize(randomize, isSimulated);
	}

	// TODO: Use separate classes for client and server knob collections
	static std::unique_ptr<KnobCollection> createClientKnobs(bool randomize, bool isSimulated) {
		return std::make_unique<KnobCollection>(ConstructorTag{}, randomize, isSimulated);
	}

	static std::unique_ptr<KnobCollection> createServerKnobs(bool randomize, bool isSimulated) {
		return std::make_unique<KnobCollection>(
		    ConstructorTag{}, randomize, isSimulated, std::make_unique<ServerKnobs>(), std::make_unique<TestKnobs>());
	}

	void initialize(bool randomize = false, bool isSimulated = false) {
		flowKnobs.initialize(randomize, isSimulated);
		clientKnobs.initialize(randomize);
		if (serverKnobs) {
			serverKnobs->initialize(randomize, &clientKnobs, isSimulated);
		}
		if (testKnobs) {
			testKnobs->initialize();
		}
	}

	void reset(bool randomize = false, bool isSimulated = false) {
		flowKnobs.reset(randomize, isSimulated);
		clientKnobs.reset(randomize);
		if (serverKnobs) {
			serverKnobs->reset(randomize, &clientKnobs, isSimulated);
		}
		if (testKnobs) {
			testKnobs->reset();
		}
	}

	bool setKnob(std::string const& knobName, KnobValueRef const& knobValue) {
		return knobValue.setKnob(knobName, flowKnobs) || knobValue.setKnob(knobName, clientKnobs) ||
		       (serverKnobs && knobValue.setKnob(knobName, *serverKnobs)) ||
		       (testKnobs && knobValue.setKnob(knobName, *testKnobs));
	}

	FlowKnobs const& getFlowKnobs() const { return flowKnobs; }

	ClientKnobs const& getClientKnobs() const { return clientKnobs; }

	ServerKnobs const& getServerKnobs() const {
		ASSERT(serverKnobs);
		return *serverKnobs;
	}

	TestKnobs const& getTestKnobs() const {
		ASSERT(testKnobs);
		return *testKnobs;
	}

	static KnobValue parseKnobValue(std::string const& knobName,
	                                std::string const& knobValue,
	                                bool includeServerKnobs) {
		static auto clientKnobs = createClientKnobs(false, false);
		static auto serverKnobs = createServerKnobs(false, false);
		if (includeServerKnobs) {
			return serverKnobs->parseKnobValue(knobName, knobValue);
		} else {
			return clientKnobs->parseKnobValue(knobName, knobValue);
		}
	}

	KnobValue parseKnobValue(std::string const& knobName, std::string const& knobValue) {
		auto parsedKnobValue = flowKnobs.parseKnobValue(knobName, knobValue);
		if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
			return KnobValueRef::create(parsedKnobValue);
		}
		parsedKnobValue = clientKnobs.parseKnobValue(knobName, knobValue);
		if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
			return KnobValueRef::create(parsedKnobValue);
		}
		if (serverKnobs) {
			parsedKnobValue = serverKnobs->parseKnobValue(knobName, knobValue);
			if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
				return KnobValueRef::create(parsedKnobValue);
			}
		}
		if (testKnobs) {
			parsedKnobValue = testKnobs->parseKnobValue(knobName, knobValue);
			if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
				return KnobValueRef::create(parsedKnobValue);
			}
		}
		throw invalid_option();
	}
};

extern std::unique_ptr<KnobCollection> g_knobs;
