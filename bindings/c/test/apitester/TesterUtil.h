/*
 * TesterUtil.h
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

#pragma once

#ifndef APITESTER_UTIL_H
#define APITESTER_UTIL_H

#include <random>
#include <ostream>
#include <optional>
#include <fmt/format.h>
#include <chrono>

namespace fmt {

// fmt::format formatting for std::optional<T>
template <typename T>
struct formatter<std::optional<T>> : fmt::formatter<T> {

	template <typename FormatContext>
	auto format(const std::optional<T>& opt, FormatContext& ctx) {
		if (opt) {
			fmt::formatter<T>::format(*opt, ctx);
			return ctx.out();
		}
		return fmt::format_to(ctx.out(), "<empty>");
	}
};

} // namespace fmt

namespace FdbApiTester {

std::string lowerCase(const std::string& str);

class Random {
public:
	Random();

	static Random& get();

	int randomInt(int min, int max);

	std::string randomStringLowerCase(int minLength, int maxLength);

	bool randomBool(double trueRatio);

	std::mt19937 random;
};

class TesterError : public std::runtime_error {
public:
	explicit TesterError(const char* message) : std::runtime_error(message) {}
	explicit TesterError(const std::string& message) : std::runtime_error(message) {}
	TesterError(const TesterError&) = default;
	TesterError& operator=(const TesterError&) = default;
	TesterError(TesterError&&) = default;
	TesterError& operator=(TesterError&&) = default;
};

void print_internal_error(const char* msg, const char* file, int line);

#define ASSERT(condition)                                                                                              \
	do {                                                                                                               \
		if (!(condition)) {                                                                                            \
			print_internal_error(#condition, __FILE__, __LINE__);                                                      \
			abort();                                                                                                   \
		}                                                                                                              \
	} while (false) // For use in destructors, where throwing exceptions is extremely dangerous

using TimePoint = std::chrono::steady_clock::time_point;
using TimeDuration = std::chrono::microseconds::rep;

static inline TimePoint timeNow() {
	return std::chrono::steady_clock::now();
}

static inline TimeDuration timeElapsedInUs(const TimePoint& start, const TimePoint& end) {
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

static inline TimeDuration timeElapsedInUs(const TimePoint& start) {
	return timeElapsedInUs(start, timeNow());
}

static inline double microsecToSec(TimeDuration timeUs) {
	return timeUs / 1000000.0;
}

} // namespace FdbApiTester

#endif