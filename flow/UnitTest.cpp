/*
 * UnitTest.cpp
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

#include "flow/UnitTest.h"

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
		return atoll(opt.get().c_str());
	}
	return {};
}

Optional<double> UnitTestParameters::getDouble(const std::string& name) const {
	auto opt = get(name);
	if (opt.present()) {
		return atof(opt.get().c_str());
	}
	return {};
}

std::string UnitTestParameters::getDataDir() const {
	return dataDir.get();
}

void UnitTestParameters::setDataDir(std::string const& dataDir) {
	this->dataDir = dataDir;
}
