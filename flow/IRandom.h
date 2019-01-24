/*
 * IRandom.h
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

#ifndef FLOW_IRANDOM_H
#define FLOW_IRANDOM_H
#pragma once

#include "flow/Platform.h"
#include <stdint.h>
#if (defined(__APPLE__))
#include <ext/hash_map>
#else
#include <unordered_map>
#endif

class UID {
	uint64_t part[2];
public:
	UID() { part[0] = part[1] = 0; }
	UID( uint64_t a, uint64_t b ) { part[0]=a; part[1]=b; }
	std::string toString() const;
	std::string shortString() const;
	bool isValid() const { return part[0] || part[1]; }

	bool operator == ( const UID& r ) const { return part[0]==r.part[0] && part[1]==r.part[1]; }
	bool operator != ( const UID& r ) const { return part[0]!=r.part[0] || part[1]!=r.part[1]; }
	bool operator < ( const UID& r ) const { return part[0] < r.part[0] || (part[0] == r.part[0] && part[1] < r.part[1]); }

	uint64_t hash() const { return first(); }
	uint64_t first() const { return part[0]; }
	uint64_t second() const { return part[1]; }

	static UID fromString( std::string const& );

	template <class Ar>
	void serialize_unversioned(Ar& ar) { // Changing this serialization format will affect key definitions, so can't simply be versioned!
		serializer(ar, part[0], part[1]);
	}
};

template <class Ar> void load( Ar& ar, UID& uid ) { uid.serialize_unversioned(ar); }
template <class Ar> void save( Ar& ar, UID const& uid ) { const_cast<UID&>(uid).serialize_unversioned(ar); }

namespace std {
	template <>
	class hash<UID> : public unary_function<UID,size_t> {
	public:
		size_t operator()(UID const& u) const { return u.hash(); }
	};
}

class IRandom {
public:
	virtual double random01() = 0;
	virtual int randomInt(int min, int maxPlusOne) = 0;
	virtual int64_t randomInt64(int64_t min, int64_t maxPlusOne) = 0;
	virtual uint32_t randomUInt32() = 0;
	virtual UID randomUniqueID() = 0;
	virtual char randomAlphaNumeric()  = 0;
	virtual std::string randomAlphaNumeric( int length ) = 0;
	virtual uint64_t peek() const = 0;  // returns something that is probably different for different random states.  Deterministic (and idempotent) for a deterministic generator.

	// The following functions have fixed implementations for now:
	template <class C>
	decltype((fake<const C>()[0])) randomChoice( const C& c ) { return c[randomInt(0,(int)c.size())]; }

	template <class C>
	void randomShuffle( C& container ) {
		int s = (int)container.size();
		for(int i=0; i<s; i++) {
			int j = randomInt( i, s );
			if (i != j) {
				std::swap( container[i], container[j] );
			}
		}
	}

	bool coinflip() { return (this->random01() < 0.5); }
};

extern IRandom* g_random;
extern IRandom* g_nondeterministic_random;
extern IRandom* g_debug_random;
extern FILE* randLog;

#endif
