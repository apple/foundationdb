/*
 * TesterApiWorkload.h
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

#ifndef APITESTER_API_WORKLOAD_H
#define APITESTER_API_WORKLOAD_H

#include "TesterWorkload.h"
#include "TesterKeyValueStore.h"

namespace FdbApiTester {

/**
 * Base class for implementing API testing workloads.
 * Provides various helper methods and reusable configuration parameters
 */
class ApiWorkload : public WorkloadBase {
public:
	void start() override;

	// Method to be overridden to run specific tests
	virtual void runTests() = 0;

protected:
	// The minimum length of a key
	int minKeyLength;

	// The maximum length of a key
	int maxKeyLength;

	// The minimum length of a value
	int minValueLength;

	// The maximum length of a value
	int maxValueLength;

	// Maximum number of keys to be accessed by a transaction
	int maxKeysPerTransaction;

	// Initial data size (number of key-value pairs)
	int initialSize;

	// The ratio of reading existing keys
	double readExistingKeysRatio;

	// Key prefix
	std::string keyPrefix;

	// In-memory store maintaining expected database state
	KeyValueStore store;

	ApiWorkload(const WorkloadConfig& config);

	// Methods for generating random keys and values
	std::string randomKeyName();
	std::string randomValue();
	std::string randomNotExistingKey();
	std::string randomExistingKey();
	std::string randomKey(double existingKeyRatio);

	// Generate initial random data for the workload
	void populateData(TTaskFct cont);

	// Clear the data of the workload
	void clearData(TTaskFct cont);

private:
	void populateDataTx(TTaskFct cont);
};

} // namespace FdbApiTester

#endif