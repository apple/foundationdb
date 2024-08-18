/*
 * GUID.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_GUID_H
#define FLOW_GUID_H
#pragma once

#include <cstdint>
#include <string>
#include <functional>

// GUID is a simple implementation of globally unique identifier (see .NET Guid Struct)
class GUID {
	uint64_t p1;
	uint64_t p2;

public:
	constexpr GUID() : p1(0ULL), p2(0ULL) {}
	constexpr GUID(const uint64_t p1_, const uint64_t p2_) : p1(p1_), p2(p2_) {}

	bool operator==(const GUID& other) const noexcept { return p1 == other.p1 && p2 == other.p2; }
	bool operator!=(const GUID& other) const noexcept { return !(*this == other); }

	uint64_t first() const noexcept { return p1; }
	uint64_t second() const noexcept { return p2; }

	std::string toString() const;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, p1, p2);
	}
};

namespace std {
template <>
struct hash<GUID> {
	size_t operator()(const GUID& guid) const;
};
} // namespace std

#endif // FLOW_GUID_H