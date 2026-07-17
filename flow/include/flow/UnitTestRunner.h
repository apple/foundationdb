/*
 * UnitTestRunner.h
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

#ifndef FLOW_UNIT_TEST_RUNNER_H
#define FLOW_UNIT_TEST_RUNNER_H
#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "flow/flow.h"

class UnitTestRunnerConfig {
public:
	using SimulationInitializer = std::function<Future<Void>()>;
	using NetworkInitializer = std::function<void()>;

	explicit UnitTestRunnerConfig(std::string_view sourceSubDir,
	                              SimulationInitializer simulationInitializer = {},
	                              NetworkInitializer networkInitializer = {},
	                              std::vector<std::string> normalTestsIgnored = {},
	                              std::vector<std::string> simulationTestsIgnored = {});

	std::string_view suiteName() const;
	std::string dataDir() const;
	std::string traceName() const;
	bool supportsSimulation() const;
	Future<Void> initializeSimulation() const;
	void initializeNetwork() const;
	const std::vector<std::string>& defaultTestsIgnored(bool simulation) const;

private:
	std::string_view sourceSubDir;
	SimulationInitializer simulationInitializer;
	NetworkInitializer networkInitializer;
	std::vector<std::string> normalTestsIgnored;
	std::vector<std::string> simulationTestsIgnored;
};

int runUnitTests(int argc, char** argv, const UnitTestRunnerConfig& config);

#endif
