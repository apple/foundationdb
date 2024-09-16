/*
 * Traceable.h
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

#ifndef FLOW_TRACEABLE_H
#define FLOW_TRACEABLE_H
#pragma once

#include <stdarg.h>
#include <stdint.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <fmt/format.h>

#include "flow/BooleanParam.h"

#define PRINTABLE_COMPRESS_NULLS 0

template <class IntType>
char base16Char(IntType c) {
	switch ((c % 16 + 16) % 16) {
	case 0:
		return '0';
	case 1:
		return '1';
	case 2:
		return '2';
	case 3:
		return '3';
	case 4:
		return '4';
	case 5:
		return '5';
	case 6:
		return '6';
	case 7:
		return '7';
	case 8:
		return '8';
	case 9:
		return '9';
	case 10:
		return 'a';
	case 11:
		return 'b';
	case 12:
		return 'c';
	case 13:
		return 'd';
	case 14:
		return 'e';
	case 15:
		return 'f';
	default:
		std::abort();
	}
}

// forward declare format from flow.h as we
// can't include flow.h here
std::string format(const char* form, ...);

template <class T, class T2 = void>
struct Traceable : std::false_type {};

#define FORMAT_TRACEABLE(type, fmt)                                                                                    \
	template <>                                                                                                        \
	struct Traceable<type> : std::true_type {                                                                          \
		static std::string toString(type value) { return format(fmt, value); }                                         \
	}

FORMAT_TRACEABLE(bool, "%d");
FORMAT_TRACEABLE(signed char, "%d");
FORMAT_TRACEABLE(unsigned char, "%d");
FORMAT_TRACEABLE(short, "%d");
FORMAT_TRACEABLE(unsigned short, "%d");
FORMAT_TRACEABLE(int, "%d");
FORMAT_TRACEABLE(unsigned, "%u");
FORMAT_TRACEABLE(long int, "%ld");
FORMAT_TRACEABLE(unsigned long int, "%lu");
FORMAT_TRACEABLE(long long int, "%lld");
FORMAT_TRACEABLE(unsigned long long int, "%llu");
FORMAT_TRACEABLE(float, "%g");
FORMAT_TRACEABLE(double, "%g");
FORMAT_TRACEABLE(void*, "%p");
FORMAT_TRACEABLE(volatile long, "%ld");
FORMAT_TRACEABLE(volatile unsigned long, "%lu");
FORMAT_TRACEABLE(volatile long long, "%lld");
FORMAT_TRACEABLE(volatile unsigned long long, "%llu");
FORMAT_TRACEABLE(volatile double, "%g");

template <class Enum>
struct Traceable<Enum, std::enable_if_t<std::is_enum_v<Enum>>> : std::true_type {
	static std::string toString(Enum e) { return format("%lld", (int64_t)e); }
};

template <class Str>
struct TraceableString {
	static auto begin(const Str& value) -> decltype(value.begin()) { return value.begin(); }

	static bool atEnd(const Str& value, decltype(value.begin()) iter) { return iter == value.end(); }

	static std::string toString(const Str& value) { return value.toString(); }
};

template <>
struct TraceableString<std::string> {
	static auto begin(const std::string& value) -> decltype(value.begin()) { return value.begin(); }

	static bool atEnd(const std::string& value, decltype(value.begin()) iter) { return iter == value.end(); }

	template <class S>
	static std::string toString(S&& value) {
		return std::forward<S>(value);
	}
};

template <>
struct TraceableString<std::string_view> {
	static auto begin(const std::string_view& value) -> decltype(value.begin()) { return value.begin(); }

	static bool atEnd(const std::string_view& value, decltype(value.begin()) iter) { return iter == value.end(); }

	static std::string toString(const std::string_view& value) { return std::string(value); }
};

template <>
struct TraceableString<const char*> {
	static const char* begin(const char* value) { return value; }

	static bool atEnd(const char* value, const char* iter) { return *iter == '\0'; }

	static std::string toString(const char* value) { return std::string(value); }
};

std::string traceableStringToString(const char* value, size_t S);

template <size_t S>
struct TraceableString<char[S]> {
	static_assert(S > 0, "Only string literals are supported.");
	static const char* begin(const char* value) { return value; }

	static bool atEnd(const char* value, const char* iter) {
		return iter - value == S - 1; // Exclude trailing \0 byte
	}

	static std::string toString(const char* value) { return traceableStringToString(value, S); }
};

template <>
struct TraceableString<char*> {
	static const char* begin(char* value) { return value; }

	static bool atEnd(char* value, const char* iter) { return *iter == '\0'; }

	static std::string toString(char* value) { return std::string(value); }
};

template <class T>
struct TraceableStringImpl : std::true_type {
	static constexpr bool isPrintable(char c) { return 32 <= c && c <= 126; }

	template <class Str>
	static std::string toString(Str&& value) {
		// if all characters are printable ascii, we simply return the string
		int nonPrintables = 0;
		int numBackslashes = 0;
		int size = 0;
		for (auto iter = TraceableString<T>::begin(value); !TraceableString<T>::atEnd(value, iter); ++iter) {
			++size;
			if (!isPrintable(char(*iter))) {
				++nonPrintables;
			} else if (*iter == '\\') {
				++numBackslashes;
			}
		}
		if (nonPrintables == 0 && numBackslashes == 0) {
			return TraceableString<T>::toString(std::forward<Str>(value));
		}
		std::string result;
		result.reserve(size - nonPrintables + (nonPrintables * 4) + numBackslashes);
		int numNull = 0;
		for (auto iter = TraceableString<T>::begin(value); !TraceableString<T>::atEnd(value, iter); ++iter) {
			if (*iter == '\\') {
				if (numNull > 0) {
					result += format("[%d]", numNull);
					numNull = 0;
				}
				result.push_back('\\');
				result.push_back('\\');
			} else if (isPrintable(*iter)) {
				if (numNull > 0) {
					result += format("[%d]", numNull);
					numNull = 0;
				}
				result.push_back(*iter);
			} else {
				const uint8_t byte = *iter;
				if (PRINTABLE_COMPRESS_NULLS && byte == 0) {
					numNull++;
				} else {
					result.push_back('\\');
					result.push_back('x');
					result.push_back(base16Char(byte / 16));
					result.push_back(base16Char(byte));
				}
			}
		}
		if (numNull > 0) {
			result += format("[%d]", numNull);
			numNull = 0;
		}
		return result;
	}
};

template <>
struct Traceable<const char*> : TraceableStringImpl<const char*> {};
template <>
struct Traceable<char*> : TraceableStringImpl<char*> {};
template <size_t S>
struct Traceable<char[S]> : TraceableStringImpl<char[S]> {};
template <>
struct Traceable<std::string> : TraceableStringImpl<std::string> {};
template <>
struct Traceable<std::string_view> : TraceableStringImpl<std::string_view> {};

template <class T>
struct Traceable<std::atomic<T>> : std::true_type {
	static std::string toString(const std::atomic<T>& value) { return Traceable<T>::toString(value.load()); }
};

template <class BooleanParamSub>
struct Traceable<BooleanParamSub, std::enable_if_t<std::is_base_of_v<BooleanParam, BooleanParamSub>>> : std::true_type {
	static std::string toString(BooleanParamSub const& value) { return Traceable<bool>::toString(value); }
};

// Adapter to redirect fmt::formatter calls to Traceable for a supported type
template <typename T>
struct FormatUsingTraceable : fmt::formatter<std::string> {
	auto format(const T& val, fmt::format_context& ctx) const {
		return fmt::formatter<std::string>::format(Traceable<T>::toString(val), ctx);
	}
};

#endif
