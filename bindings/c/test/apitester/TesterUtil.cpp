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

namespace FdbApiTester {

Random::Random() {
	std::random_device dev;
	random.seed(dev());
}

int Random::randomInt(int min, int max) {
	return std::uniform_int_distribution<int>(min, max)(random);
}

std::string Random::randomStringLowerCase(int minLength, int maxLength) {
	int length = randomInt(minLength, maxLength);
	std::string str;
	str.reserve(length);
	for (int i = 0; i < length; i++) {
		str += (char)randomInt('a', 'z');
	}
	return str;
}

bool Random::randomBool(double trueRatio) {
	return std::uniform_real_distribution<double>(0.0, 1.0)(random) <= trueRatio;
}

int vsformat(std::string& outputString, const char* form, va_list args) {
	char buf[200];

	va_list args2;
	va_copy(args2, args);
	int size = vsnprintf(buf, sizeof(buf), form, args2);
	va_end(args2);

	if (size >= 0 && size < sizeof(buf)) {
		outputString = std::string(buf, size);
		return size;
	}

#ifdef _WIN32
	// Microsoft's non-standard vsnprintf doesn't return a correct size, but just an error, so determine the necessary
	// size
	va_copy(args2, args);
	size = _vscprintf(form, args2);
	va_end(args2);
#endif

	if (size < 0) {
		return -1;
	}

	outputString.resize(size + 1);
	size = vsnprintf(&outputString[0], outputString.size(), form, args);
	if (size < 0 || size >= outputString.size()) {
		return -1;
	}

	outputString.resize(size);
	return size;
}

std::string format(const char* form, ...) {
	va_list args;
	va_start(args, form);

	std::string str;
	int result = vsformat(str, form, args);
	va_end(args);

	ASSERT(result >= 0);
	return str;
}

} // namespace FdbApiTester