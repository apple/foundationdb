/*
 * IRandom.h
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

#ifndef FLOW_IRANDOM_H
#define FLOW_IRANDOM_H
#pragma once

#include "flow/Platform.h"
#include "flow/FileIdentifier.h"
#include "flow/ObjectSerializerTraits.h"
#include "flow/FastRef.h"
#include <stdint.h>
#if (defined(__APPLE__))
#include <ext/hash_map>
#else
#include <unordered_map>
#endif
#include <functional>
#include <utility>

// Until we move to C++20, we'll need something to take the place of operator<=>.
// This is as good a place as any, I guess.

template <typename T>
typename std::enable_if<std::is_integral<T>::value, int>::type compare(T l, T r) {
	const int gt = l > r;
	const int lt = l < r;
	return gt - lt;
	// GCC also emits branchless code for the following, but the above performs
	// slightly better in benchmarks as of this writing.
	// return l < r ? -1 : l == r ? 0 : 1;
}

template <typename T, typename U>
typename std::enable_if<!std::is_integral<T>::value, int>::type compare(T const& l, U const& r) {
	return l.compare(r);
}

template <class K, class V>
int compare(std::pair<K, V> const& l, std::pair<K, V> const& r) {
	if (int cmp = compare(l.first, r.first)) {
		return cmp;
	}
	return compare(l.second, r.second);
}

class UID {
	uint64_t part[2];

public:
	constexpr static FileIdentifier file_identifier = 15597147;
	UID() { part[0] = part[1] = 0; }
	constexpr UID(uint64_t a, uint64_t b) : part{ a, b } {}
	std::string toString() const;
	std::string shortString() const;
	bool isValid() const { return part[0] || part[1]; }

	int compare(const UID& r) const {
		if (int cmp = ::compare(part[0], r.part[0])) {
			return cmp;
		}
		return ::compare(part[1], r.part[1]);
	}
	bool operator==(const UID& r) const { return part[0] == r.part[0] && part[1] == r.part[1]; }
	bool operator!=(const UID& r) const { return part[0] != r.part[0] || part[1] != r.part[1]; }
	bool operator<(const UID& r) const { return part[0] < r.part[0] || (part[0] == r.part[0] && part[1] < r.part[1]); }
	bool operator>(const UID& r) const { return r < *this; }
	bool operator<=(const UID& r) const { return !(*this > r); }
	bool operator>=(const UID& r) const { return !(*this < r); }

	uint64_t hash() const { return first(); }
	uint64_t first() const { return part[0]; }
	uint64_t second() const { return part[1]; }

	static UID fromString(std::string const&);

	template <class Ar>
	void serialize_unversioned(
	    Ar& ar) { // Changing this serialization format will affect key definitions, so can't simply be versioned!
		serializer(ar, part[0], part[1]);
	}
};

template <class Ar>
void load(Ar& ar, UID& uid) {
	uid.serialize_unversioned(ar);
}
template <class Ar>
void save(Ar& ar, UID const& uid) {
	const_cast<UID&>(uid).serialize_unversioned(ar);
}

template <>
struct scalar_traits<UID> : std::true_type {
	constexpr static size_t size = sizeof(uint64_t[2]);
	template <class Context>
	static void save(uint8_t* out, const UID& uid, Context&) {
		uint64_t* outI = reinterpret_cast<uint64_t*>(out);
		outI[0] = uid.first();
		outI[1] = uid.second();
	}

	template <class Context>
	static void load(const uint8_t* i, UID& out, Context& context) {
		const uint64_t* in = reinterpret_cast<const uint64_t*>(i);
		out = UID(in[0], in[1]);
	}
};

namespace std {
template <>
class hash<UID> {
public:
	size_t operator()(UID const& u) const { return u.hash(); }
};
} // namespace std

class IRandom {
public:
	virtual double random01() = 0; // return random value in [0, 1]
	virtual int randomInt(int min, int maxPlusOne) = 0;
	virtual int64_t randomInt64(int64_t min, int64_t maxPlusOne) = 0;
	virtual uint32_t randomUInt32() = 0;
	virtual uint64_t randomUInt64() = 0;
	virtual UID randomUniqueID() = 0;
	virtual char randomAlphaNumeric() = 0;
	virtual std::string randomAlphaNumeric(int length) = 0;
	virtual uint32_t randomSkewedUInt32(uint32_t min, uint32_t maxPlusOne) = 0;
	virtual uint64_t peek() const = 0; // returns something that is probably different for different random states.
	                                   // Deterministic (and idempotent) for a deterministic generator.

	virtual void addref() = 0;
	virtual void delref() = 0;

	// The following functions have fixed implementations for now:
	template <class C>
	decltype((std::declval<const C>()[0])) randomChoice(const C& c) {
		return c[randomInt(0, (int)c.size())];
	}

	template <class C>
	void randomShuffle(C& container) {
		int s = (int)container.size();
		for (int i = 0; i < s; i++) {
			int j = randomInt(i, s);
			if (i != j) {
				std::swap(container[i], container[j]);
			}
		}
	}

	bool coinflip() { return (this->random01() < 0.5); }
};

extern FILE* randLog;

// Sets the seed for the deterministic random number generator on the current thread
void setThreadLocalDeterministicRandomSeed(uint32_t seed);

// Returns the random number generator that can be seeded. This generator should only
// be used in contexts where the choice to call it is deterministic.
//
// This generator is only deterministic if given a seed using setThreadLocalDeterministicRandomSeed
Reference<IRandom> deterministicRandom();

// A random number generator that cannot be manually seeded and may be called in
// non-deterministic contexts.
Reference<IRandom> nondeterministicRandom();

// This returns a deterministic random number generator initialized with the same seed as the one returned by
// deterministicRandom. The main use-case for this is to generate deterministic random numbers without changing the
// determinism of the simulator. This is useful for things like generating random UIDs for debug transactions.
// WARNING: This is not thread safe and must not be called from any other thread than the network thread!
Reference<IRandom> debugRandom();

// Populates a buffer with a random sequence of bytes
void generateRandomData(uint8_t* buffer, int length);

#endif
