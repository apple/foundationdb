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
#include <cstdio>
#include <algorithm>
#include <ctype.h>
#include <chrono>

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

fdb::ByteString Random::randomStringLowerCase(int minLength, int maxLength) {
	int length = randomInt(minLength, maxLength);
	fdb::ByteString str;
	str.reserve(length);
	for (int i = 0; i < length; i++) {
		str += (char)randomInt('a', 'z');
	}
	return str;
}

bool Random::randomBool(double trueRatio) {
	return std::uniform_real_distribution<double>(0.0, 1.0)(random) <= trueRatio;
}

void print_internal_error(const char* msg, const char* file, int line) {
	fprintf(stderr, "Assertion %s failed @ %s %d:\n", msg, file, line);
	fflush(stderr);
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

} // namespace FdbApiTester