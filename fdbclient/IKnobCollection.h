/*
 * IKnobCollection.h
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
#include "fdbclient/Knobs.h"
#include "fdbclient/ServerKnobs.h"
#include "flow/Knobs.h"

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

class IKnobCollection {
public:
	virtual void initialize(bool randomize = false, bool isSimulated = false) = 0;
	virtual void reset(bool randomize = false, bool isSimulated = false) = 0;
	virtual FlowKnobs const& getFlowKnobs() const = 0;
	virtual ClientKnobs const& getClientKnobs() const = 0;
	virtual ServerKnobs const& getServerKnobs() const = 0;
	virtual class TestKnobs const& getTestKnobs() const = 0;
	virtual KnobValue parseKnobValue(std::string const& knobName, std::string const& knobValue) const = 0;
	virtual void setKnob(std::string const& knobName, KnobValueRef const& knobValue) = 0;
	static KnobValue parseKnobValue(std::string const& knobName, std::string const& knobValue, bool includeServerKnobs);
	static std::unique_ptr<IKnobCollection> createClientKnobCollection(bool randomize, bool isSimulated);
	static std::unique_ptr<IKnobCollection> createServerKnobCollection(bool randomize, bool isSimulated);
};

extern std::unique_ptr<IKnobCollection> g_knobs;
