/*
 * TesterTestSpec.cpp
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

#include "TesterTestSpec.h"
#include "TesterUtil.h"
#include <toml.hpp>
#include <fmt/format.h>
#include <functional>

namespace FdbApiTester {

namespace {

void processIntOption(const std::string& value, const std::string& optionName, int& res, int minVal, int maxVal) {
	char* endptr;
	res = strtol(value.c_str(), &endptr, 10);
	if (*endptr != '\0') {
		throw TesterError(fmt::format("Invalid test file. Invalid value {} for {}", value, optionName));
	}
	if (res < minVal || res > maxVal) {
		throw TesterError(
		    fmt::format("Invalid test file. Value for {} must be between {} and {}", optionName, minVal, maxVal));
	}
}

std::unordered_map<std::string, std::function<void(const std::string& value, TestSpec* spec)>> testSpecTestKeys = {
	{ "title",
	  [](const std::string& value, TestSpec* spec) { //
	      spec->title = value;
	  } },
	{ "blockOnFutures",
	  [](const std::string& value, TestSpec* spec) { //
	      spec->blockOnFutures = (value == "true");
	  } },
	{ "buggify",
	  [](const std::string& value, TestSpec* spec) { //
	      spec->buggify = (value == "true");
	  } },
	{ "multiThreaded",
	  [](const std::string& value, TestSpec* spec) { //
	      spec->multiThreaded = (value == "true");
	  } },
	{ "fdbCallbacksOnExternalThreads",
	  [](const std::string& value, TestSpec* spec) { //
	      spec->fdbCallbacksOnExternalThreads = (value == "true");
	  } },
	{ "databasePerTransaction",
	  [](const std::string& value, TestSpec* spec) { //
	      spec->databasePerTransaction = (value == "true");
	  } },
	{ "tamperClusterFile",
	  [](const std::string& value, TestSpec* spec) { //
	      spec->tamperClusterFile = (value == "true");
	  } },
	{ "minFdbThreads",
	  [](const std::string& value, TestSpec* spec) { //
	      processIntOption(value, "minFdbThreads", spec->minFdbThreads, 1, 1000);
	  } },
	{ "maxFdbThreads",
	  [](const std::string& value, TestSpec* spec) { //
	      processIntOption(value, "maxFdbThreads", spec->maxFdbThreads, 1, 1000);
	  } },
	{ "minClientThreads",
	  [](const std::string& value, TestSpec* spec) { //
	      processIntOption(value, "minClientThreads", spec->minClientThreads, 1, 1000);
	  } },
	{ "maxClientThreads",
	  [](const std::string& value, TestSpec* spec) { //
	      processIntOption(value, "maxClientThreads", spec->maxClientThreads, 1, 1000);
	  } },
	{ "minDatabases",
	  [](const std::string& value, TestSpec* spec) { //
	      processIntOption(value, "minDatabases", spec->minDatabases, 1, 1000);
	  } },
	{ "maxDatabases",
	  [](const std::string& value, TestSpec* spec) { //
	      processIntOption(value, "maxDatabases", spec->maxDatabases, 1, 1000);
	  } },
	{ "minClients",
	  [](const std::string& value, TestSpec* spec) { //
	      processIntOption(value, "minClients", spec->minClients, 1, 1000);
	  } },
	{ "maxClients",
	  [](const std::string& value, TestSpec* spec) { //
	      processIntOption(value, "maxClients", spec->maxClients, 1, 1000);
	  } },
	{ "disableClientBypass",
	  [](const std::string& value, TestSpec* spec) { //
	      spec->disableClientBypass = (value == "true");
	  } },
	{ "minTenants",
	  [](const std::string& value, TestSpec* spec) { //
	      processIntOption(value, "minTenants", spec->minTenants, 1, 1000);
	  } },
	{ "maxTenants",
	  [](const std::string& value, TestSpec* spec) { //
	      processIntOption(value, "maxTenants", spec->maxTenants, 1, 1000);
	  } },
	{ "runLoopProfiler",
	  [](const std::string& value, TestSpec* spec) { //
	      spec->runLoopProfiler = (value == "true");
	  } }
};

template <typename T>
std::string toml_to_string(const T& value) {
	// TOML formatting converts numbers to strings exactly how they're in the file
	// and thus, is equivalent to testspec.  However, strings are quoted, so we
	// must remove the quotes.
	if (value.type() == toml::value_t::string) {
		const std::string& formatted = toml::format(value);
		return formatted.substr(1, formatted.size() - 2);
	} else {
		return toml::format(value);
	}
}

} // namespace

// In the current TOML scope, look for "knobs" field. If exists, store all options as knob key-value pairs
void getOverriddenKnobKeyValues(const toml::value& context, TestSpec::KnobKeyValues& result) {}

TestSpec readTomlTestSpec(std::string fileName) {
	TestSpec spec;
	WorkloadSpec workloadSpec;

	const toml::value& conf = toml::parse(fileName);

	// Then parse each test
	const toml::array& tests = toml::find(conf, "test").as_array();
	if (tests.size() == 0) {
		throw TesterError("Invalid test file. No [test] section found");
	} else if (tests.size() > 1) {
		throw TesterError("Invalid test file. More than one [test] section found");
	}

	const toml::value& test = tests[0];

	// First handle all test-level settings
	for (const auto& [k, v] : test.as_table()) {
		if (k == "workload") {
			continue;
		}
		if (testSpecTestKeys.find(k) != testSpecTestKeys.end()) {
			testSpecTestKeys[k](toml_to_string(v), &spec);
		} else {
			throw TesterError(fmt::format(
			    "Invalid test file. Unrecognized test parameter. Name: {}, value {}", k, toml_to_string(v)));
		}
	}

	// Look for "knobs" section. If exists, store all options as knob key-value pairs
	try {
		const toml::array& overrideKnobs = toml::find(conf, "knobs").as_array();
		for (const toml::value& knob : overrideKnobs) {
			for (const auto& [key, value_] : knob.as_table()) {
				const std::string& value = toml_to_string(value_);
				spec.knobs.emplace_back(key, value);
			}
		}
	} catch (const std::out_of_range&) {
		// Not an error because "knobs" section is optional
	}

	// And then copy the workload attributes to spec.options
	const toml::array& workloads = toml::find(test, "workload").as_array();
	for (const toml::value& workload : workloads) {
		workloadSpec = WorkloadSpec();
		auto& options = workloadSpec.options;
		for (const auto& [attrib, v] : workload.as_table()) {
			options[attrib] = toml_to_string(v);
		}
		auto itr = options.find("name");
		if (itr == options.end()) {
			throw TesterError("Invalid test file. Unspecified workload name.");
		}
		workloadSpec.name = itr->second;
		spec.workloads.push_back(workloadSpec);
	}

	return spec;
}

} // namespace FdbApiTester