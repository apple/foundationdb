/*
 * DeterministicRandom.h
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

#ifndef FLOW_DETERIMINISTIC_RANDOM_H
#define FLOW_DETERIMINISTIC_RANDOM_H
#pragma once

#include <cinttypes>
#include "IRandom.h"

#include <random>

class DeterministicRandom : public IRandom {
private:
	std::mt19937 random;
	uint64_t next;

	uint64_t gen64() {
		uint64_t curr = next;
		next = (uint64_t(random()) << 32) ^ random();
		if (TRACE_SAMPLE()) TraceEvent(SevSample, "random");
		return curr;
	}

public:
	DeterministicRandom( uint32_t seed ) : random( (unsigned long)seed ), next( (uint64_t(random()) << 32) ^ random() ) {
		UNSTOPPABLE_ASSERT( seed != 0 );  // docs for mersenne twister say x0>0
	};

	double random01() {
		double d = gen64() / double(uint64_t(-1));
		if (randLog && g_random==this) fprintf(randLog, "R01  %f\n", d);
		return d;
	}

	int randomInt(int min, int maxPlusOne) {
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
		if (min < 0 && ((unsigned int) -min) > v)
			i = -(((unsigned int) -min) - v);
		else
			i = v + min;
		if (randLog && g_random==this) fprintf(randLog, "Rint %d\n", i);
		return i;
	}

	int64_t randomInt64(int64_t min, int64_t maxPlusOne) {
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
		if (min < 0 && ((uint64_t) -min) > v)
			i = -(((uint64_t) -min) - v);
		else
			i = v + min;
		if (randLog && g_random==this) fprintf(randLog, "Rint64 %" PRId64 "\n", i);
		return i;
	}

	uint32_t randomUInt32() { return gen64(); }

	UID randomUniqueID() {
		uint64_t x,y;
		x = gen64();
		y = gen64();
		if (randLog && g_random == this) fprintf(randLog, "Ruid %" PRIx64 " %" PRIx64 "\n", x, y);
		return UID(x,y);
	}

	char randomAlphaNumeric() {
		static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
		char c = alphanum[ gen64() % 62 ];
		if (randLog && g_random==this) fprintf(randLog, "Rchar %c\n", c);
		return c;
	}

	std::string randomAlphaNumeric( int length ) {
		std::string s;
		s.reserve( length );
		for( int i = 0; i < length; i++ )
			s += randomAlphaNumeric();
		return s;
	}

	uint64_t peek() const { return next; }
};

#endif
