/*
 * TesterTestSpec.h
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

#ifndef APITESTER_CONFIG_READER_H
#define APITESTER_CONFIG_READER_H

#include <string>
#include <unordered_map>
#include <vector>

#define FDB_API_VERSION 710

namespace FdbApiTester {

struct WorkloadSpec {
	std::string name;
	std::unordered_map<std::string, std::string> options;
};

struct TestSpec {
	std::string title;
	// api version, using the latest version by default
	int apiVersion = FDB_API_VERSION;
	bool blockOnFutures = false;
	bool multiThreaded = false;
	bool buggify = false;
	bool fdbCallbacksOnExternalThreads = false;
	bool databasePerTransaction = false;
	int minFdbThreads = 1;
	int maxFdbThreads = 1;
	int minClientThreads = 1;
	int maxClientThreads = 1;
	int minDatabases = 1;
	int maxDatabases = 1;
	int minClients = 1;
	int maxClients = 10;
	std::string testFile;
	std::vector<WorkloadSpec> workloads;
};

TestSpec readTomlTestSpec(std::string fileName);

} // namespace FdbApiTester

#endif