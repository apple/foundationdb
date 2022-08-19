/*
 * TestKnobCollection.h
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

#ifndef FDBCLIENT_TESTKNOBCOLLECTION_H
#define FDBCLIENT_TESTKNOBCOLLECTION_H

#pragma once

#include "fdbclient/IKnobCollection.h"
#include "fdbclient/ServerKnobCollection.h"

class TestKnobs : public KnobsImpl<TestKnobs> {
public:
	TestKnobs();

	// Nonatomic test knobs
	int64_t TEST_LONG;
	int TEST_INT;
	double TEST_DOUBLE;
	bool TEST_BOOL;
	std::string TEST_STRING;

	// Atomic test knobs
	int64_t TEST_ATOMIC_LONG;
	int TEST_ATOMIC_INT;
	double TEST_ATOMIC_DOUBLE;
	bool TEST_ATOMIC_BOOL;
	std::string TEST_ATOMIC_STRING;

	bool operator==(TestKnobs const&) const;
	bool operator!=(TestKnobs const&) const;
	void initialize();
};

/*
 * Stores both flow knobs, client knobs, server knobs, and test knobs. As the
 * name implies, this class is only meant to be used for testing.
 */
class TestKnobCollection : public IKnobCollection {
	ServerKnobCollection serverKnobCollection;
	TestKnobs testKnobs;

public:
	TestKnobCollection(Randomize randomize, IsSimulated isSimulated);
	void initialize(Randomize randomize, IsSimulated isSimulated) override;
	void reset(Randomize randomize, IsSimulated isSimulated) override;
	FlowKnobs const& getFlowKnobs() const override { return serverKnobCollection.getFlowKnobs(); }
	ClientKnobs const& getClientKnobs() const override { return serverKnobCollection.getClientKnobs(); }
	ServerKnobs const& getServerKnobs() const override { return serverKnobCollection.getServerKnobs(); }
	TestKnobs const& getTestKnobs() const override { return testKnobs; }
	void clearTestKnobs() override;
	Optional<KnobValue> tryParseKnobValue(std::string const& knobName, std::string const& knobValue) const override;
	bool trySetKnob(std::string const& knobName, KnobValueRef const& knobValue) override;
	bool isAtomic(std::string const& knobName) const override;
};

#endif // FDBCLIENT_TESTKNOBCOLLECTION_H