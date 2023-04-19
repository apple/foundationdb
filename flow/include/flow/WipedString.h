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
#include <concepts>
#include <cstring>
#include <type_traits>
#include "flow/serialize.h"
#include "flow/Arena.h"
#include "flow/FileIdentifier.h"
#include "flow/ObjectSerializerTraits.h"

namespace detail {
// wraps StringRef for the sole purpose of offering distinct serializable trait
// we avoid inheritance by choice, to guarantee that no traits are shared with StringRef by unintended implicit
// conversion
class WipedStringRef {
	StringRef* value;

	WipedStringRef(StringRef& s) noexcept : value(&s) {}

public:
	WipedStringRef() noexcept : value(nullptr) {}

	// deliberately avoid WipedStringRef(StringRef) to prevent implicit conversion
	static WipedStringRef create(StringRef& s) noexcept { return WipedStringRef(s); }

	StringRef& get() const noexcept { return *value; }
};

template <class Context>
concept is_wipe_enabled = requires(Context& context) {
	                          {
		                          context.markForWipe(std::declval<uint8_t*>(), std::declval<size_t>())
		                          } -> std::same_as<void>;
                          };

} // namespace detail

// This trait is meant to be used by WipedString::serialize()
template <>
struct dynamic_size_traits<detail::WipedStringRef> : std::true_type {
	template <class Context>
	static size_t size(const detail::WipedStringRef& t, Context&) {
		return t.get().size();
	}

	template <class Context>
	static void save(uint8_t* out, const detail::WipedStringRef& t, Context& context) {
		if (!t.get().empty()) {
			::memcpy(out, t.get().begin(), t.get().size());
			if constexpr (detail::is_wipe_enabled<Context>) {
				context.markForWipe(out, t.get().size());
				// below condition is only active with unit test
				if (keepalive_allocator::isActive())
					keepalive_allocator::trackWipedArea(out, t.get().size());
			}
		}
	}

	template <class Context>
	static void load(const uint8_t* ptr, size_t sz, detail::WipedStringRef& t, Context& context) {
		dynamic_size_traits<StringRef>::load(ptr, sz, t.get(), context);
	}
};

// String that wipes its memory after use.
// Also wipes any buffer containing its content upon serialization.
// NOTE: This class intentionally diverged from Standalone<StringRef> for more robust preservation of
//       wiping guarantees through more restrictive interface: e.g. not allowing non-const access to arena or StringRef.
// IMPORTANT: currently deserialized WipedString does not make the deserialized WipedString
//            to wipe upon destruction, simply because there's no need for it.
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

	WipedString(StringRef s, Arena& arena) {
		if (!s.empty()) {
			auto buf = new (arena, WipeAfterUse{}) uint8_t[s.size()];
			::memcpy(buf, s.begin(), s.size());
			string = StringRef(buf, s.size());
			this->arena = arena;
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
		auto ws = detail::WipedStringRef::create(string);
		serializer(ar, ws, arena);
	}
};

#endif /* WIPED_STRING_H */
