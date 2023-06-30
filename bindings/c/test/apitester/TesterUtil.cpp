/*
 * TesterUtil.cpp
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

#include "TesterUtil.h"
#include "fmt/core.h"
#include "fmt/chrono.h"
#include <cstdio>
#include <algorithm>
#include <ctype.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <sstream>

namespace FdbApiTester {

fdb::ByteString lowerCase(fdb::BytesRef str) {
	fdb::ByteString res(str);
	std::transform(res.begin(), res.end(), res.begin(), ::tolower);
	return res;
}

Random::Random() {
	std::random_device dev;
	random.seed(dev());
}

int Random::randomInt(int min, int max) {
	return std::uniform_int_distribution<int>(min, max)(random);
}

Random& Random::get() {
	static thread_local Random random;
	return random;
}

bool Random::randomBool(double trueRatio) {
	return std::uniform_real_distribution<double>(0.0, 1.0)(random) <= trueRatio;
}
namespace log {
Logger& Logger::get() {
	static Logger logger;
	return logger;
}

void Logger::logMessage(log::Level lvl, std::string_view msg) {
	if (lvl > level) {
		return;
	}
	const char* lvlLabel = "";
	switch (lvl) {
	case Level::ERROR:
		lvlLabel = "ERROR";
		break;
	case Level::WARN:
		lvlLabel = "WARN";
		break;
	case Level::INFO:
		lvlLabel = "INFO";
		break;
	case Level::DEBUG:
		lvlLabel = "DEBUG";
		break;
	}
	using namespace std::chrono;
	auto time = system_clock::now();
	// Format in form:
	// [INFO] 2023-06-26T19:29:18.253 A log message
	fmt::print(stderr,
	           "[{}] {:%FT%H:%M:}{:%S} {}\n",
	           lvlLabel,
	           time,
	           duration_cast<milliseconds>(time.time_since_epoch()),
	           msg);
	fflush(stderr);
}

} // namespace log

void print_internal_error(const char* msg, const char* file, int line) {
	log::error("Assertion {} failed @ {}:{}", msg, file, line);
}

std::optional<fdb::Value> copyValueRef(fdb::future_var::ValueRef::Type value) {
	if (value) {
		return std::make_optional(fdb::Value(value.value()));
	} else {
		return std::nullopt;
	}
}

KeyValueArray copyKeyValueArray(fdb::future_var::KeyValueRefArray::Type array) {
	auto& [in_kvs, in_count, in_more] = array;

	KeyValueArray out;
	auto& [out_kv, out_more] = out;

	out_more = in_more;
	out_kv.clear();
	for (int i = 0; i < in_count; ++i) {
		fdb::native::FDBKeyValue nativeKv = *in_kvs++;
		fdb::KeyValue kv;
		kv.key = fdb::Key(nativeKv.key, nativeKv.key_length);
		kv.value = fdb::Value(nativeKv.value, nativeKv.value_length);
		out_kv.push_back(kv);
	}
	return out;
};

KeyRangeArray copyKeyRangeArray(fdb::future_var::KeyRangeRefArray::Type array) {
	auto& [in_ranges, in_count] = array;

	KeyRangeArray out;

	for (int i = 0; i < in_count; ++i) {
		fdb::native::FDBKeyRange nativeKr = *in_ranges++;
		fdb::KeyRange range;
		range.beginKey = fdb::Key(nativeKr.begin_key, nativeKr.begin_key_length);
		range.endKey = fdb::Key(nativeKr.end_key, nativeKr.end_key_length);
		out.push_back(range);
	}
	return out;
};

GranuleSummaryArray copyGranuleSummaryArray(fdb::future_var::GranuleSummaryRefArray::Type array) {
	auto& [in_summaries, in_count] = array;

	GranuleSummaryArray out;

	for (int i = 0; i < in_count; ++i) {
		fdb::native::FDBGranuleSummary nativeSummary = *in_summaries++;
		fdb::GranuleSummary summary(nativeSummary);
		out.push_back(summary);
	}
	return out;
};

TmpFile::~TmpFile() {
	if (!filename.empty()) {
		remove();
	}
}

void TmpFile::create(std::string_view dir, std::string_view prefix) {
	while (true) {
		filename = fmt::format("{}/{}-{}", dir, prefix, Random::get().randomStringLowerCase<std::string>(6, 6));
		if (!std::filesystem::exists(std::filesystem::path(filename))) {
			break;
		}
	}

	// Create an empty tmp file
	std::fstream tmpFile(filename, std::fstream::out);
	if (!tmpFile.good()) {
		throw TesterError(fmt::format("Failed to create temporary file {}", filename));
	}
}

void TmpFile::write(std::string_view data) {
	std::ofstream ofs(filename, std::fstream::out | std::fstream::binary);
	if (!ofs.good()) {
		throw TesterError(fmt::format("Failed to write to the temporary file {}", filename));
	}
	ofs.write(data.data(), data.size());
}

void TmpFile::remove() {
	if (!std::filesystem::remove(std::filesystem::path(filename))) {
		log::warn("Failed to remove file {}", filename);
	}
}

} // namespace FdbApiTester