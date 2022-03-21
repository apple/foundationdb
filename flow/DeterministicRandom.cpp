/*
 * DeterministicRandom.cpp
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

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "flow/DeterministicRandom.h"

#include <cstring>

uint64_t DeterministicRandom::gen64() {
	uint64_t curr = next;
	next = (uint64_t(random()) << 32) ^ random();
	if (TRACE_SAMPLE())
		TraceEvent(SevSample, "Random").log();
	return curr;
}

DeterministicRandom::DeterministicRandom(uint32_t seed, bool useRandLog)
  : random((unsigned long)seed), next((uint64_t(random()) << 32) ^ random()), useRandLog(useRandLog) {
	UNSTOPPABLE_ASSERT(seed != 0); // docs for mersenne twister say x0>0
};

double DeterministicRandom::random01() {
	double d = gen64() / double(uint64_t(-1));
	if (randLog && useRandLog)
		fprintf(randLog, "R01  %f\n", d);
	return d;
}

int DeterministicRandom::randomInt(int min, int maxPlusOne) {
	ASSERT(min < maxPlusOne);
	unsigned int range;
	if (maxPlusOne < 0)
		range = std::abs(maxPlusOne - min);
	else {
		range = maxPlusOne;
		range -= min;
	}
	uint64_t v = (gen64() % range);
	int i;
	if (min < 0 && (-static_cast<unsigned int>(min + 1)) >= v)
		i = -static_cast<int>(-static_cast<unsigned int>(min + 1) - v) - 1;
	else
		i = v + min;
	if (randLog && useRandLog)
		fprintf(randLog, "Rint %d\n", i);
	return i;
}

int64_t DeterministicRandom::randomInt64(int64_t min, int64_t maxPlusOne) {
	ASSERT(min < maxPlusOne);
	uint64_t range;
	if (maxPlusOne < 0)
		range = std::abs(maxPlusOne - min);
	else {
		range = maxPlusOne;
		range -= min;
	}
	uint64_t v = (gen64() % range);
	int64_t i;
	if (min < 0 && (-static_cast<uint64_t>(min + 1)) >= v)
		i = -static_cast<int64_t>(-static_cast<uint64_t>(min + 1) - v) - 1;
	else
		i = v + min;
	if (randLog && useRandLog)
		fmt::print(randLog, "Rint64 {}\n", i);
	return i;
}

uint32_t DeterministicRandom::randomUInt32() {
	return gen64();
}

uint64_t DeterministicRandom::randomUInt64() {
	return gen64();
}

uint32_t DeterministicRandom::randomSkewedUInt32(uint32_t min, uint32_t maxPlusOne) {
	std::uniform_real_distribution<double> distribution(std::log(min), std::log(maxPlusOne - 1));
	double logpower = distribution(random);
	uint32_t loguniform = static_cast<uint32_t>(std::pow(10, logpower));
	// doubles can be imprecise, so let's make sure we don't violate an edge case.
	return std::max(std::min(loguniform, maxPlusOne - 1), min);
}

UID DeterministicRandom::randomUniqueID() {
	uint64_t x, y;
	x = gen64();
	y = gen64();
	if (randLog && useRandLog)
		fmt::print(randLog, "Ruid {0} {1}\n", x, y);
	return UID(x, y);
}

char DeterministicRandom::randomAlphaNumeric() {
	static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	char c = alphanum[gen64() % 62];
	if (randLog && useRandLog)
		fprintf(randLog, "Rchar %c\n", c);
	return c;
}

std::string DeterministicRandom::randomAlphaNumeric(int length) {
	std::string s;
	s.reserve(length);
	for (int i = 0; i < length; i++)
		s += randomAlphaNumeric();
	return s;
}

uint64_t DeterministicRandom::peek() const {
	return next;
}

void DeterministicRandom::addref() {
	ReferenceCounted<DeterministicRandom>::addref();
}
void DeterministicRandom::delref() {
	ReferenceCounted<DeterministicRandom>::delref();
}

void generateRandomData(uint8_t* buffer, int length) {
	for (int i = 0; i < length; i += sizeof(uint32_t)) {
		uint32_t val = deterministicRandom()->randomUInt32();
		memcpy(&buffer[i], &val, std::min(length - i, (int)sizeof(uint32_t)));
	}
}
