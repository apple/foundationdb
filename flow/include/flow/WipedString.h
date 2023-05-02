/*
 * WipedString.h
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
#ifndef WIPED_STRING_H
#define WIPED_STRING_H
#pragma once
#include <cstring>
#include <type_traits>
#include "flow/Arena.h"
#include "flow/FileIdentifier.h"
#include "flow/serialize.h"

// String that wipes its memory after use
// Intentionally diverged from Standalone<StringRef> for more robust preservation of wiping guarantees through more
// restrictive interface e.g. no non-const contents() member or direct access to StringRef&, and no non-const arena()
// member.
class WipedString {
	Arena arena;
	StringRef string;

public:
	constexpr static FileIdentifier file_identifier = 6228563;

	WipedString() noexcept = default;

	explicit WipedString(StringRef s) : arena(), string() {
		if (!s.empty()) {
			auto buf = new (arena, WipeAfterUse{}) uint8_t[s.size()];
			::memcpy(buf, s.begin(), s.size());
			string = StringRef(buf, s.size());
		}
	}

	WipedString(const WipedString& other) noexcept = default;
	WipedString(WipedString&& other) noexcept = default;
	WipedString& operator=(const WipedString& other) noexcept = default;
	WipedString& operator=(WipedString&& other) noexcept = default;

	// Optional::castTo<T>() support
	operator StringRef() const noexcept { return string; }

	StringRef contents() const noexcept { return string; }

	template <class Archive>
	void serialize(Archive& ar) {
		// does not force the deserialized string to wipe because packet containing this string
		// is received as a non-wiping chunk of binary string and we cannot control how packets are received at this
		// layer
		serializer(ar, string, arena);
	}
};

#endif /* WIPED_STRING_H */
