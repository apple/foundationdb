/*
 * DeterministicRandom.h
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

#ifndef FLOW_DETERIMINISTIC_RANDOM_H
#define FLOW_DETERIMINISTIC_RANDOM_H
#pragma once

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

#include <cinttypes>
#include "flow/IRandom.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/FastRef.h"

#include <random>

// FIXME: Remove once https://github.com/apple/swift/issues/61620 is fixed.
#define SWIFT_CXX_REF_DETERMINISTICRANDOM                                                                              \
	__attribute__((swift_attr("import_reference"))) __attribute__((swift_attr("retain:addref_DeterministicRandom")))   \
	__attribute__((swift_attr("release:delref_DeterministicRandom")))

class SWIFT_CXX_REF_DETERMINISTICRANDOM DeterministicRandom final : public IRandom,
                                                                    public ReferenceCounted<DeterministicRandom> {
private:
	// Use boost::random::mt19937 to get consistent output across
	// different compilers and therefore across different C++ standard
	// library implementations. In other words, don't rely on the
	// standard library for this.
	//
	// This is not expected to affect performance.  See e.g.
	// https://chatgpt.com/share/68800ee9-3270-800b-aa84-4567167f02ab
	boost::random::mt19937 rng;
	uint64_t next;
	bool useRandLog;

	uint64_t gen64();

public:
	DeterministicRandom(uint32_t seed, bool useRandLog = false);
	double random01() override;
	int randomInt(int min, int maxPlusOne) override;
	int64_t randomInt64(int64_t min, int64_t maxPlusOne) override;
	uint32_t randomUInt32() override;
	uint64_t randomUInt64() override;
	uint32_t randomSkewedUInt32(uint32_t min, uint32_t maxPlusOne) override;
	UID randomUniqueID() override;
	char randomAlphaNumeric() override;
	std::string randomAlphaNumeric(int length) override;
	void randomBytes(uint8_t* buf, int length) override;
	uint64_t peek() const override;
	void addref() override;
	void delref() override;
};

// FIXME: Remove once https://github.com/apple/swift/issues/61620 is fixed.
inline void addref_DeterministicRandom(DeterministicRandom* ptr) {
	addref(ptr);
}

// FIXME: Remove once https://github.com/apple/swift/issues/61620 is fixed.
inline void delref_DeterministicRandom(DeterministicRandom* ptr) {
	delref(ptr);
}

#endif
