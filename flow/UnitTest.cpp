/*
 * UnitTest.cpp
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

#include "flow/UnitTest.h"
#include "flow/ParseNumber.h"

#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <limits>

UnitTestCollection g_unittests = { nullptr };

UnitTest::UnitTest(const char* name, const char* file, int line, TestFunction func)
  : name(name), file(file), line(line), func(func), next(g_unittests.tests) {
	g_unittests.tests = this;
}

void UnitTestParameters::set(const std::string& name, const std::string& value) {
	printf("setting %s = %s\n", name.c_str(), value.c_str());
	params[name] = value;
}

Optional<std::string> UnitTestParameters::get(const std::string& name) const {
	auto it = params.find(name);
	if (it != params.end()) {
		return it->second;
	}
	return {};
}

void UnitTestParameters::set(const std::string& name, int64_t value) {
	set(name, format("%" PRId64, value));
};

void UnitTestParameters::set(const std::string& name, double value) {
	set(name, format("%g", value));
};

Optional<int64_t> UnitTestParameters::getInt(const std::string& name) const {
	auto opt = get(name);
	if (opt.present()) {
		auto parsed = parseNumber<int64_t>(opt.get());
		if (!parsed.present()) {
			throw invalid_option_value();
		}
		return parsed;
	}
	return {};
}

Optional<double> UnitTestParameters::getDouble(const std::string& name) const {
	auto opt = get(name);
	if (opt.present()) {
		auto parsed = parseNumber<double>(opt.get());
		if (!parsed.present()) {
			throw invalid_option_value();
		}
		return parsed;
	}
	return {};
}

std::string UnitTestParameters::getDataDir() const {
	return dataDir.get();
}

void UnitTestParameters::setDataDir(std::string const& dataDir) {
	this->dataDir = dataDir;
}

TEST_CASE("/flow/ParseNumber/checked") {
	int consumed = -1;
	ASSERT_EQ(parseNumberPrefix<int>(" \t+12suffix"_sr, 10, &consumed).get(), 12);
	ASSERT_EQ(consumed, 5);
	ASSERT(!parseNumber<int>(" \t+12suffix"_sr).present());
	ASSERT_EQ(parseNumber<int>(" \t+12"_sr).get(), 12);
	ASSERT(!parseNumber<int>("12 "_sr).present());
	ASSERT(!parseNumber<int>("12\0suffix"_sr).present());
	ASSERT_EQ(parseNumberPrefix<int>("12\0suffix"_sr).get(), 12);
	ASSERT_EQ(parseNumber<uint64_t>("ffffffffffffffff"_sr, 16).get(), std::numeric_limits<uint64_t>::max());
	ASSERT_EQ(parseNumber<int>("0xff"_sr, 0).get(), 255);
	ASSERT_EQ(parseNumber<int>("ff"_sr, 16).get(), 255);
	ASSERT_EQ(parseNumber<int>("010"_sr).get(), 10);
	ASSERT_EQ(parseNumber<uint64_t>("-1"_sr).get(), std::numeric_limits<uint64_t>::max());
	ASSERT(!parseNumber<uint8_t>("-1"_sr).present());
	ASSERT(!parseNumber<uint8_t>("256"_sr).present());
	ASSERT(!parseNumber<int8_t>("128"_sr).present());
	ASSERT(!parseNumber<int8_t>("-129"_sr).present());
	ASSERT_EQ(parseNumber<float>("0.1"_sr).get(), 0.1f);
	ASSERT_EQ(parseNumber<double>("0.1"_sr).get(), 0.1);
	ASSERT(parseNumber<long double>("1.25"_sr).get() == 1.25L);
	ASSERT_EQ(parseNumber<double>("0x1p2"_sr).get(), 4.0);
	ASSERT(!parseNumber<double>("1e9999"_sr).present());
	ASSERT(!parseNumber<float>("1e-9999"_sr).present());
	ASSERT(!parseNumber<int>("1"_sr, 1).present());
	ASSERT(!parseNumber<double>("1"_sr, 16).present());
	for (StringRef text : { ""_sr, "+"_sr, " \t"_sr, "9223372036854775808"_sr }) {
		consumed = -1;
		ASSERT(!parseNumberPrefix<int64_t>(text, 10, &consumed).present());
		ASSERT_EQ(consumed, -1);
	}
	const uint8_t backing[] = { '1', '2', '3', 0 };
	ASSERT_EQ(parseNumber<int>(StringRef(backing, 2)).get(), 12);
	return Void();
}

TEST_CASE("/flow/UnitTestParameters/numericValues") {
	UnitTestParameters numericParams;
	ASSERT(!numericParams.getInt("missing").present());
	ASSERT(!numericParams.getDouble("missing").present());

	const int64_t intMin = std::numeric_limits<int64_t>::min();
	const int64_t intMax = std::numeric_limits<int64_t>::max();
	for (const auto& [text, expected] :
	     std::vector<std::pair<std::string, int64_t>>{ { "0", 0 },
	                                                   { " \t+0012", 12 },
	                                                   { "-1", -1 },
	                                                   { std::to_string(intMin), intMin },
	                                                   { std::to_string(intMax), intMax } }) {
		numericParams.set("integer", text);
		errno = ERANGE;
		ASSERT_EQ(numericParams.getInt("integer").get(), expected);
	}
	for (const std::string& text : std::vector<std::string>{
	         "", " \t", "+", "1x", "1 ", "9223372036854775808", "-9223372036854775809", std::string("12\0junk", 7) }) {
		numericParams.set("integer", text);
		try {
			(void)numericParams.getInt("integer");
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_invalid_option_value);
		}
	}

	for (const auto& [text, expected] : std::vector<std::pair<std::string, double>>{
	         { "0", 0.0 },
	         { " \t+1.25e2", 125.0 },
	         { "-0x1p2", -4.0 },
	         { ".5", 0.5 },
	         { "1.7976931348623157e308", std::numeric_limits<double>::max() },
	         { "2.2250738585072014e-308", std::numeric_limits<double>::min() } }) {
		numericParams.set("double", text);
		errno = ERANGE;
		ASSERT_EQ(numericParams.getDouble("double").get(), expected);
	}
	numericParams.set("double", std::string("nan"));
	ASSERT(std::isnan(numericParams.getDouble("double").get()));
	numericParams.set("double", std::string("inf"));
	ASSERT(std::isinf(numericParams.getDouble("double").get()));
	numericParams.set("double", std::string("-0"));
	ASSERT(std::signbit(numericParams.getDouble("double").get()));
	for (const std::string& text :
	     std::vector<std::string>{ "", " \t", "+", "1x", "1 ", "1e9999", "1e-9999", std::string("12\0junk", 7) }) {
		numericParams.set("double", text);
		try {
			(void)numericParams.getDouble("double");
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_invalid_option_value);
		}
	}

	return Void();
}

TEST_CASE("/flow/UnitTestParameters/coroutineOwnership") {
	const std::string marker = "unitTestParameterOwnershipProbe";
	if (params.get(marker).present()) {
		co_await delay(0.001);
		ASSERT_EQ(params.get(marker).get(), std::string("original"));
		co_return;
	}

	UnitTest* registered = g_unittests.tests;
	while (registered != nullptr && StringRef(registered->name) != "/flow/UnitTestParameters/coroutineOwnership"_sr) {
		registered = registered->next;
	}
	ASSERT(registered != nullptr);

	Future<Void> pending;
	{
		UnitTestParameters callerParams;
		callerParams.set(marker, std::string("original"));
		pending = registered->func(callerParams);
		ASSERT(!pending.isReady());
		callerParams.set(marker, std::string("changed"));
	}
	co_await pending;
}
