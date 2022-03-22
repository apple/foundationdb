/*
 * TesterApiWorkload.cpp
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

#include "TesterApiWorkload.h"
#include "TesterUtil.h"
#include <fmt/format.h>

namespace FdbApiTester {

ApiWorkload::ApiWorkload(const WorkloadConfig& config) : WorkloadBase(config) {
	minKeyLength = config.getIntOption("minKeyLength", 1);
	maxKeyLength = config.getIntOption("maxKeyLength", 64);
	minValueLength = config.getIntOption("minValueLength", 1);
	maxValueLength = config.getIntOption("maxValueLength", 1000);
	maxKeysPerTransaction = config.getIntOption("maxKeysPerTransaction", 50);
	initialSize = config.getIntOption("initialSize", 1000);
	readExistingKeysRatio = config.getFloatOption("readExistingKeysRatio", 0.9);
	keyPrefix = fmt::format("{}/", workloadId);
}

void ApiWorkload::start() {
	schedule([this]() {
		// 1. Clear data
		clearData([this]() {
			// 2. Populate initial data
			populateData([this]() {
				// 3. Generate random workload
				runTests();
			});
		});
	});
}

std::string ApiWorkload::randomKeyName() {
	return keyPrefix + Random::get().randomStringLowerCase(minKeyLength, maxKeyLength);
}

std::string ApiWorkload::randomValue() {
	return Random::get().randomStringLowerCase(minValueLength, maxValueLength);
}

std::string ApiWorkload::randomNotExistingKey() {
	while (true) {
		std::string key = randomKeyName();
		if (!store.exists(key)) {
			return key;
		}
	}
}

std::string ApiWorkload::randomExistingKey() {
	std::string genKey = randomKeyName();
	std::string key = store.getKey(genKey, true, 1);
	if (key != store.endKey()) {
		return key;
	}
	key = store.getKey(genKey, true, 0);
	if (key != store.startKey()) {
		return key;
	}
	info("No existing key found, using a new random key.");
	return genKey;
}

std::string ApiWorkload::randomKey(double existingKeyRatio) {
	if (Random::get().randomBool(existingKeyRatio)) {
		return randomExistingKey();
	} else {
		return randomNotExistingKey();
	}
}

void ApiWorkload::populateDataTx(TTaskFct cont) {
	int numKeys = maxKeysPerTransaction;
	auto kvPairs = std::make_shared<std::vector<KeyValue>>();
	for (int i = 0; i < numKeys; i++) {
		kvPairs->push_back(KeyValue{ randomNotExistingKey(), randomValue() });
	}
	execTransaction(
	    [kvPairs](auto ctx) {
		    for (const KeyValue& kv : *kvPairs) {
			    ctx->tx()->set(kv.key, kv.value);
		    }
		    ctx->commit();
	    },
	    [this, kvPairs, cont]() {
		    for (const KeyValue& kv : *kvPairs) {
			    store.set(kv.key, kv.value);
		    }
		    schedule(cont);
	    });
}

void ApiWorkload::clearData(TTaskFct cont) {
	execTransaction(
	    [this](auto ctx) {
		    ctx->tx()->clearRange(keyPrefix, fmt::format("{}\xff", keyPrefix));
		    ctx->commit();
	    },
	    [this, cont]() { schedule(cont); });
}

void ApiWorkload::populateData(TTaskFct cont) {
	if (store.size() < initialSize) {
		populateDataTx([this, cont]() { populateData(cont); });
	} else {
		info("Data population completed");
		schedule(cont);
	}
}

} // namespace FdbApiTester
