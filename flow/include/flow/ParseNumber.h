/*
 * ParseNumber.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_PARSE_NUMBER_H
#define FLOW_PARSE_NUMBER_H
#pragma once

#include "flow/Arena.h"

#include <cerrno>
#include <cstdlib>
#include <limits>
#include <type_traits>

// Parses a numeric prefix without reading beyond input. Leading C whitespace and signs are accepted.
// Returns absent for a missing number or a conversion outside T's range. On success, consumed includes
// leading whitespace; on failure it is unchanged. Integer bases are 0 or 2..36. Floating-point input
// uses the C strto* grammar and requires base 10. Unsigned conversions retain strtoull's sign wrapping
// before checking T's range, so uint64_t accepts "-1" as UINT64_MAX.
template <class T>
Optional<T> parseNumberPrefix(StringRef input, int base = 10, int* consumed = nullptr) {
	static_assert((std::is_integral_v<T> && !std::is_same_v<T, bool>) || std::is_floating_point_v<T>);
	if constexpr (std::is_integral_v<T>) {
		if (base != 0 && (base < 2 || base > 36)) {
			return {};
		}
	} else if (base != 10) {
		return {};
	}

	const std::string text = input.toString();
	char* end = nullptr;
	errno = 0;
	T result;
	if constexpr (std::is_same_v<T, float>) {
		result = std::strtof(text.c_str(), &end);
	} else if constexpr (std::is_same_v<T, double>) {
		result = std::strtod(text.c_str(), &end);
	} else if constexpr (std::is_same_v<T, long double>) {
		result = std::strtold(text.c_str(), &end);
	} else if constexpr (std::is_signed_v<T>) {
		const long long parsed = std::strtoll(text.c_str(), &end, base);
		if (parsed < std::numeric_limits<T>::min() || parsed > std::numeric_limits<T>::max()) {
			return {};
		}
		result = static_cast<T>(parsed);
	} else {
		const unsigned long long parsed = (std::strtoull)(text.c_str(), &end, base);
		if (parsed > std::numeric_limits<T>::max()) {
			return {};
		}
		result = static_cast<T>(parsed);
	}
	if (errno == ERANGE || end == text.c_str()) {
		return {};
	}
	if (consumed != nullptr) {
		*consumed = static_cast<int>(end - text.c_str());
	}
	return result;
}

// Like parseNumberPrefix, but requires all input bytes to be part of the number. Trailing whitespace,
// other suffixes, and embedded NUL bytes are rejected.
template <class T>
Optional<T> parseNumber(StringRef input, int base = 10) {
	int consumed = 0;
	auto result = parseNumberPrefix<T>(input, base, &consumed);
	if (!result.present() || consumed != input.size()) {
		return {};
	}
	return result;
}

#endif
