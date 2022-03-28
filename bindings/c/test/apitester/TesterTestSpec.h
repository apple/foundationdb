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

/// Workload specification
struct WorkloadSpec {
	std::string name;
	std::unordered_map<std::string, std::string> options;
};

// Test speficification loaded from a *.toml file
struct TestSpec {
	// Title of the test
	std::string title;

	// FDB API version, using the latest version by default
	int apiVersion = FDB_API_VERSION;

	// Use blocking waits on futures instead of scheduling callbacks
	bool blockOnFutures = false;

	// Use multi-threaded FDB client
	bool multiThreaded = false;

	// Enable injection of errors in FDB client
	bool buggify = false;

	// Execute future callbacks on the threads of the external FDB library
	// rather than on the main thread of the local FDB client library
	bool fdbCallbacksOnExternalThreads = false;

	// Execute each transaction in a separate database instance
	bool databasePerTransaction = false;

	// Size of the FDB client thread pool (a random number in the [min,max] range)
	int minFdbThreads = 1;
	int maxFdbThreads = 1;

	// Size of the thread pool for test workloads (a random number in the [min,max] range)
	int minClientThreads = 1;
	int maxClientThreads = 1;

	// Size of the database instance pool (a random number in the [min,max] range)
	// Each transaction is assigned randomly to one of the databases in the pool
	int minDatabases = 1;
	int maxDatabases = 1;

	// Number of workload clients (a random number in the [min,max] range)
	int minClients = 1;
	int maxClients = 10;

	// List of workloads with their options
	std::vector<WorkloadSpec> workloads;
};

// Read the test specfication from a *.toml file
TestSpec readTomlTestSpec(std::string fileName);

} // namespace FdbApiTester

#endif