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

#include "test/fdb_api.hpp"

#undef ERROR
#define ERROR(name, number, description) enum { error_code_##name = number };

#include "flow/error_definitions.h"

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

fdb::ByteString lowerCase(fdb::BytesRef str);

class Random {
public:
	Random();

	static Random& get();

	int randomInt(int min, int max);

	template <class StringType>
	StringType randomStringLowerCase(int minLength, int maxLength) {
		int length = randomInt(minLength, maxLength);
		StringType str;
		str.reserve(length);
		for (int i = 0; i < length; i++) {
			str += (char)randomInt('a', 'z');
		}
		return str;
	}

	fdb::ByteString randomByteStringLowerCase(int minLength, int maxLength) {
		return randomStringLowerCase<fdb::ByteString>(minLength, maxLength);
	}

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

std::optional<fdb::Value> copyValueRef(fdb::future_var::ValueRef::Type value);

using KeyValueArray = std::pair<std::vector<fdb::KeyValue>, bool>;
KeyValueArray copyKeyValueArray(fdb::future_var::KeyValueRefArray::Type array);

using KeyRangeArray = std::vector<fdb::KeyRange>;
KeyRangeArray copyKeyRangeArray(fdb::future_var::KeyRangeRefArray::Type array);

using GranuleSummaryArray = std::vector<fdb::GranuleSummary>;
GranuleSummaryArray copyGranuleSummaryArray(fdb::future_var::GranuleSummaryRefArray::Type array);

static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "Do not support non-little-endian systems");

// Converts a little-endian encoded number into an integral type.
template <class T, typename = std::enable_if_t<std::is_integral<T>::value>>
static T toInteger(fdb::BytesRef value) {
	ASSERT(value.size() == sizeof(T));
	T output;
	memcpy(&output, value.data(), value.size());
	return output;
}

// Converts an integral type to a little-endian encoded byte string.
template <class T, typename = std::enable_if_t<std::is_integral<T>::value>>
static fdb::ByteString toByteString(T value) {
	fdb::ByteString output(sizeof(T), 0);
	memcpy(output.data(), (const uint8_t*)&value, sizeof(value));
	return output;
}

// Creates a temporary file; file gets destroyed/deleted along with object destruction.
struct TmpFile {
public:
	~TmpFile();
	void create(std::string_view dir, std::string_view prefix);
	void write(std::string_view data);
	void remove();
	const std::string& getFileName() const { return filename; }

private:
	std::string filename;
};

} // namespace FdbApiTester

#endif
