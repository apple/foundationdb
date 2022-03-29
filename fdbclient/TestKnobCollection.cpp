/*
 * TestKnobCollection.cpp
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

#include "fdbclient/TestKnobCollection.h"

TestKnobCollection::TestKnobCollection(Randomize randomize, IsSimulated isSimulated)
  : serverKnobCollection(randomize, isSimulated) {
	initialize(randomize, isSimulated);
}

void TestKnobCollection::initialize(Randomize randomize, IsSimulated isSimulated) {
	serverKnobCollection.initialize(randomize, isSimulated);
	testKnobs.initialize();
}

void TestKnobCollection::reset(Randomize randomize, IsSimulated isSimulated) {
	serverKnobCollection.reset(randomize, isSimulated);
	testKnobs.reset();
}

void TestKnobCollection::clearTestKnobs() {
	testKnobs.reset();
}

Optional<KnobValue> TestKnobCollection::tryParseKnobValue(std::string const& knobName,
                                                          std::string const& knobValue) const {
	auto result = serverKnobCollection.tryParseKnobValue(knobName, knobValue);
	if (result.present()) {
		return result;
	}
	auto parsedKnobValue = testKnobs.parseKnobValue(knobName, knobValue);
	if (!std::holds_alternative<NoKnobFound>(parsedKnobValue)) {
		return KnobValueRef::create(parsedKnobValue);
	}
	return {};
}

bool TestKnobCollection::trySetKnob(std::string const& knobName, KnobValueRef const& knobValue) {
	return serverKnobCollection.trySetKnob(knobName, knobValue) || knobValue.visitSetKnob(knobName, testKnobs);
}

bool TestKnobCollection::isAtomic(std::string const& knobName) const {
	return serverKnobCollection.isAtomic(knobName) || testKnobs.isAtomic(knobName);
}

#define init(knob, value, atomic) initKnob(knob, value, #knob, atomic)

TestKnobs::TestKnobs() {
	initialize();
}

void TestKnobs::initialize() {
	init(TEST_LONG, 0, Atomic::NO);
	init(TEST_INT, 0, Atomic::NO);
	init(TEST_DOUBLE, 0.0, Atomic::NO);
	init(TEST_BOOL, false, Atomic::NO);
	init(TEST_STRING, "", Atomic::NO);

	init(TEST_ATOMIC_LONG, 0, Atomic::YES);
	init(TEST_ATOMIC_INT, 0, Atomic::YES);
	init(TEST_ATOMIC_DOUBLE, 0.0, Atomic::YES);
	init(TEST_ATOMIC_BOOL, false, Atomic::YES);
	init(TEST_ATOMIC_STRING, "", Atomic::YES);
}

bool TestKnobs::operator==(TestKnobs const& rhs) const {
	return (TEST_LONG == rhs.TEST_LONG) && (TEST_INT == rhs.TEST_INT) && (TEST_DOUBLE == rhs.TEST_DOUBLE) &&
	       (TEST_BOOL == rhs.TEST_BOOL) && (TEST_STRING == rhs.TEST_STRING) &&
	       (TEST_ATOMIC_LONG == rhs.TEST_ATOMIC_LONG) && (TEST_ATOMIC_INT == rhs.TEST_ATOMIC_INT) &&
	       (TEST_ATOMIC_DOUBLE == rhs.TEST_ATOMIC_DOUBLE) && (TEST_ATOMIC_BOOL == rhs.TEST_ATOMIC_BOOL) &&
	       (TEST_ATOMIC_STRING == rhs.TEST_ATOMIC_STRING);
}

bool TestKnobs::operator!=(TestKnobs const& rhs) const {
	return !(*this == rhs);
}
