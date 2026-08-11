/*
 * ClientOptionValidation.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ClientOptionValidation.h"

#include "flow/Error.h"
#include "flow/UnitTest.h"

void validateOptionValuePresent(Optional<StringRef> value) {
	if (!value.present()) {
		throw invalid_option_value();
	}
}

void validateOptionValueNotPresent(Optional<StringRef> value) {
	if (value.present() && !value.get().empty()) {
		throw invalid_option_value();
	}
}

int64_t extractIntOption(Optional<StringRef> value, int64_t minValue, int64_t maxValue) {
	validateOptionValuePresent(value);
	if (value.get().size() != 8) {
		throw invalid_option_value();
	}

	int64_t passed = *((int64_t*)(value.get().begin()));
	if (passed > maxValue || passed < minValue) {
		throw invalid_option_value();
	}

	return passed;
}

namespace {

template <class Validate>
void expectInvalidOptionValue(Validate&& validate) {
	try {
		validate();
		ASSERT(false);
	} catch (Error& error) {
		ASSERT_EQ(error.code(), error_code_invalid_option_value);
	}
}

} // namespace

TEST_CASE("/fdbclient/ClientOptionValidation/Presence") {
	const Optional<StringRef> missing;
	const Optional<StringRef> empty = StringRef();
	const Optional<StringRef> nonEmpty = "value"_sr;

	expectInvalidOptionValue([&] { validateOptionValuePresent(missing); });
	validateOptionValuePresent(empty);
	validateOptionValuePresent(nonEmpty);

	validateOptionValueNotPresent(missing);
	validateOptionValueNotPresent(empty);
	expectInvalidOptionValue([&] { validateOptionValueNotPresent(nonEmpty); });

	return Void();
}

TEST_CASE("/fdbclient/ClientOptionValidation/Integer") {
	int64_t encodedValue = -123;
	const Optional<StringRef> value = StringRef(reinterpret_cast<const uint8_t*>(&encodedValue), sizeof(encodedValue));
	const Optional<StringRef> missing;
	const Optional<StringRef> empty = StringRef();
	const uint8_t oversized[sizeof(encodedValue) + 1] = {};

	ASSERT_EQ(extractIntOption(value), encodedValue);
	ASSERT_EQ(extractIntOption(value, encodedValue, encodedValue), encodedValue);
	expectInvalidOptionValue([&] { extractIntOption(missing); });
	expectInvalidOptionValue([&] { extractIntOption(empty); });
	expectInvalidOptionValue([&] {
		extractIntOption(StringRef(reinterpret_cast<const uint8_t*>(&encodedValue), sizeof(encodedValue) - 1));
	});
	expectInvalidOptionValue([&] { extractIntOption(StringRef(oversized, sizeof(oversized))); });
	expectInvalidOptionValue([&] { extractIntOption(value, encodedValue + 1); });
	expectInvalidOptionValue([&] { extractIntOption(value, std::numeric_limits<int64_t>::min(), encodedValue - 1); });

	encodedValue = std::numeric_limits<int64_t>::min();
	ASSERT_EQ(extractIntOption(value), encodedValue);
	encodedValue = std::numeric_limits<int64_t>::max();
	ASSERT_EQ(extractIntOption(value), encodedValue);

	return Void();
}
