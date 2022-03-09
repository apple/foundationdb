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

Random& Random::get() {
	static thread_local Random random;
	return random;
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

void print_internal_error(const char* msg, const char* file, int line) {
	fprintf(stderr, "Assertion %s failed @ %s %d:\n", msg, file, line);
}

} // namespace FdbApiTester