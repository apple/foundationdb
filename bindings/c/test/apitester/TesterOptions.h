/*
 * TesterOptions.h
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

#pragma once

#ifndef APITESTER_TESTER_OPTIONS_H
#define APITESTER_TESTER_OPTIONS_H

#include "TesterTestSpec.h"

namespace FdbApiTester {

class TesterOptions {
public:
	std::string clusterFile;
	bool trace = false;
	std::string traceDir;
	std::string traceFormat;
	std::string logGroup;
	std::string externalClientLibrary;
	std::string testFile;
	int numFdbThreads;
	int numClientThreads;
	int numDatabases;
	int numClients;
	std::vector<std::pair<std::string, std::string>> knobs;
	TestSpec testSpec;
};

} // namespace FdbApiTester

#endif
