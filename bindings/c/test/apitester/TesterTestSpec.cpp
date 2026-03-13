/*
 * TesterTestSpec.cpp
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

#include "TesterTestSpec.h"
#include "TesterUtil.h"
#include <toml.hpp>
#include <fmt/format.h>
#include <functional>

namespace FdbApiTester {

namespace {

void processIntOption(std::string const& value, std::string const& optionName, int& res, int minVal, int maxVal) {
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

std::unordered_map<std::string, std::function<void(std::string const& value, TestSpec* spec)>> testSpecTestKeys = {
	{ "title",
	  [](std::string const& value, TestSpec* spec) { //
	      spec->title = value;
	  } },
	{ "blockOnFutures",
	  [](std::string const& value, TestSpec* spec) { //
	      spec->blockOnFutures = (value == "true");
	  } },
	{ "buggify",
	  [](std::string const& value, TestSpec* spec) { //
	      spec->buggify = (value == "true");
	  } },
	{ "multiThreaded",
	  [](std::string const& value, TestSpec* spec) { //
	      spec->multiThreaded = (value == "true");
	  } },
	{ "fdbCallbacksOnExternalThreads",
	  [](std::string const& value, TestSpec* spec) { //
	      spec->fdbCallbacksOnExternalThreads = (value == "true");
	  } },
	{ "databasePerTransaction",
	  [](std::string const& value, TestSpec* spec) { //
	      spec->databasePerTransaction = (value == "true");
	  } },
	{ "tamperClusterFile",
	  [](std::string const& value, TestSpec* spec) { //
	      spec->tamperClusterFile = (value == "true");
	  } },
	{ "minFdbThreads",
	  [](std::string const& value, TestSpec* spec) { //
	      processIntOption(value, "minFdbThreads", spec->minFdbThreads, 1, 1000);
	  } },
	{ "maxFdbThreads",
	  [](std::string const& value, TestSpec* spec) { //
	      processIntOption(value, "maxFdbThreads", spec->maxFdbThreads, 1, 1000);
	  } },
	{ "minClientThreads",
	  [](std::string const& value, TestSpec* spec) { //
	      processIntOption(value, "minClientThreads", spec->minClientThreads, 1, 1000);
	  } },
	{ "maxClientThreads",
	  [](std::string const& value, TestSpec* spec) { //
	      processIntOption(value, "maxClientThreads", spec->maxClientThreads, 1, 1000);
	  } },
	{ "minDatabases",
	  [](std::string const& value, TestSpec* spec) { //
	      processIntOption(value, "minDatabases", spec->minDatabases, 1, 1000);
	  } },
	{ "maxDatabases",
	  [](std::string const& value, TestSpec* spec) { //
	      processIntOption(value, "maxDatabases", spec->maxDatabases, 1, 1000);
	  } },
	{ "minClients",
	  [](std::string const& value, TestSpec* spec) { //
	      processIntOption(value, "minClients", spec->minClients, 1, 1000);
	  } },
	{ "maxClients",
	  [](std::string const& value, TestSpec* spec) { //
	      processIntOption(value, "maxClients", spec->maxClients, 1, 1000);
	  } },
	{ "disableClientBypass",
	  [](std::string const& value, TestSpec* spec) { //
	      spec->disableClientBypass = (value == "true");
	  } },
	{ "runLoopProfiler",
	  [](std::string const& value, TestSpec* spec) { //
	      spec->runLoopProfiler = (value == "true");
	  } }
};

template <typename T>
std::string toml_to_string(T const& value) {
	// TOML formatting converts numbers to strings exactly how they're in the file
	// and thus, is equivalent to testspec.  However, strings are quoted, so we
	// must remove the quotes.
	if (value.type() == toml::value_t::string) {
		std::string const& formatted = toml::format(value);
		return formatted.substr(1, formatted.size() - 2);
	} else {
		return toml::format(value);
	}
}

} // namespace

// In the current TOML scope, look for "knobs" field. If exists, store all options as knob key-value pairs
void getOverriddenKnobKeyValues(toml::value const& context, TestSpec::KnobKeyValues& result) {}

TestSpec readTomlTestSpec(std::string fileName) {
	TestSpec spec;
	WorkloadSpec workloadSpec;

	toml::value const& conf = toml::parse(fileName);

	// Then parse each test
	toml::array const& tests = toml::find(conf, "test").as_array();
	if (tests.empty()) {
		throw TesterError("Invalid test file. No [test] section found");
	} else if (tests.size() > 1) {
		throw TesterError("Invalid test file. More than one [test] section found");
	}

	toml::value const& test = tests[0];

	// First handle all test-level settings
	for (auto const& [k, v] : test.as_table()) {
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
		toml::array const& overrideKnobs = toml::find(conf, "knobs").as_array();
		for (toml::value const& knob : overrideKnobs) {
			for (auto const& [key, value_] : knob.as_table()) {
				std::string const& value = toml_to_string(value_);
				spec.knobs.emplace_back(key, value);
			}
		}
	} catch (std::out_of_range const&) {
		// Not an error because "knobs" section is optional
	}

	// And then copy the workload attributes to spec.options
	toml::array const& workloads = toml::find(test, "workload").as_array();
	for (toml::value const& workload : workloads) {
		workloadSpec = WorkloadSpec();
		auto& options = workloadSpec.options;
		for (auto const& [attrib, v] : workload.as_table()) {
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
