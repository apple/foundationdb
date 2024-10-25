/*
 * GUID.cpp
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

#include "flow/GUID.h"

#include <bit>
#include <iomanip>
#include <sstream>

std::string GUID::toString() const {
	std::stringstream ss;
	ss << std::setfill('0') << std::setw(sizeof(uint64_t) * 2) << first();
	ss << std::setfill('0') << std::setw(sizeof(uint64_t) * 2) << second();
	return ss.str();
}

namespace details {
// The following hash-combination algorithm is copied from
// https://stackoverflow.com/a/50978188/178732

template <typename T>
T xorshift(const T& n, int i) {
	return n ^ (n >> i);
}

// a hash function with another name as to not confuse with std::hash
uint64_t distribute(const uint64_t& n) {
	static constexpr uint64_t p = 0x5555555555555555ull; // pattern of alternating 0 and 1
	static constexpr uint64_t c = 17316035218449499591ull; // random uneven integer constant;
	return c * xorshift(p * xorshift(n, 32), 32);
}

// call this function with the old seed and the new key to be hashed and combined into the new seed value, respectively
// the final hash
template <class T>
inline size_t hash_combine(std::size_t seed, const T& v) {
	return std::rotl(seed, std::numeric_limits<size_t>::digits / 3) ^ distribute(std::hash<T>{}(v));
}

} // namespace details

namespace std {
size_t hash<GUID>::operator()(const GUID& guid) const {
	return ::details::hash_combine(hash<decltype(guid.first())>{}(guid.first()), guid.second());
}
} // namespace std
