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
#include "test/fdb_api.hpp"
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
	runUntilStop = config.getBoolOption("runUntilStop", false);
	numRandomOperations = config.getIntOption("numRandomOperations", 1000);
	numOperationsForProgressCheck = config.getIntOption("numOperationsForProgressCheck", 10);
	keyPrefix = fdb::toBytesRef(fmt::format("{}/", workloadId));
	numRandomOpLeft = 0;
	stopReceived = false;
	checkingProgress = false;
	apiVersion = config.apiVersion;
}

IWorkloadControlIfc* ApiWorkload::getControlIfc() {
	if (runUntilStop) {
		return this;
	} else {
		return nullptr;
	}
}

void ApiWorkload::stop() {
	ASSERT(runUntilStop);
	stopReceived = true;
}

void ApiWorkload::checkProgress() {
	ASSERT(runUntilStop);
	numRandomOpLeft = numOperationsForProgressCheck;
	checkingProgress = true;
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

void ApiWorkload::runTests() {
	if (!runUntilStop) {
		numRandomOpLeft = numRandomOperations;
	}
	randomOperations();
}

void ApiWorkload::randomOperations() {
	if (runUntilStop) {
		if (stopReceived)
			return;
		if (checkingProgress) {
			int numLeft = numRandomOpLeft--;
			if (numLeft == 0) {
				checkingProgress = false;
				confirmProgress();
			}
		}
	} else {
		int numLeft = numRandomOpLeft--;
		if (numLeft == 0)
			return;
	}
	randomOperation([this]() { randomOperations(); });
}

void ApiWorkload::randomOperation(TTaskFct cont) {
	// Must be overridden if used
	ASSERT(false);
}

fdb::Key ApiWorkload::randomKeyName() {
	return keyPrefix + Random::get().randomByteStringLowerCase(minKeyLength, maxKeyLength);
}

fdb::Value ApiWorkload::randomValue() {
	return Random::get().randomByteStringLowerCase(minValueLength, maxValueLength);
}

fdb::Key ApiWorkload::randomNotExistingKey() {
	while (true) {
		fdb::Key key = randomKeyName();
		if (!store.exists(key)) {
			return key;
		}
	}
}

fdb::Key ApiWorkload::randomExistingKey() {
	fdb::Key genKey = randomKeyName();
	fdb::Key key = store.getKey(genKey, true, 1);
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

fdb::Key ApiWorkload::randomKey(double existingKeyRatio) {
	if (Random::get().randomBool(existingKeyRatio)) {
		return randomExistingKey();
	} else {
		return randomNotExistingKey();
	}
}

void ApiWorkload::populateDataTx(TTaskFct cont) {
	int numKeys = maxKeysPerTransaction;
	auto kvPairs = std::make_shared<std::vector<fdb::KeyValue>>();
	for (int i = 0; i < numKeys; i++) {
		kvPairs->push_back(fdb::KeyValue{ randomNotExistingKey(), randomValue() });
	}
	execTransaction(
	    [kvPairs](auto ctx) {
		    for (const fdb::KeyValue& kv : *kvPairs) {
			    ctx->tx().set(kv.key, kv.value);
		    }
		    ctx->commit();
	    },
	    [this, kvPairs, cont]() {
		    for (const fdb::KeyValue& kv : *kvPairs) {
			    store.set(kv.key, kv.value);
		    }
		    schedule(cont);
	    });
}

void ApiWorkload::clearData(TTaskFct cont) {
	execTransaction(
	    [this](auto ctx) {
		    ctx->tx().clearRange(keyPrefix, keyPrefix + fdb::Key(1, '\xff'));
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

void ApiWorkload::randomInsertOp(TTaskFct cont) {
	int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
	auto kvPairs = std::make_shared<std::vector<fdb::KeyValue>>();
	for (int i = 0; i < numKeys; i++) {
		kvPairs->push_back(fdb::KeyValue{ randomNotExistingKey(), randomValue() });
	}
	execTransaction(
	    [kvPairs](auto ctx) {
		    for (const fdb::KeyValue& kv : *kvPairs) {
			    ctx->tx().set(kv.key, kv.value);
		    }
		    ctx->commit();
	    },
	    [this, kvPairs, cont]() {
		    for (const fdb::KeyValue& kv : *kvPairs) {
			    store.set(kv.key, kv.value);
		    }
		    schedule(cont);
	    });
}

void ApiWorkload::randomClearOp(TTaskFct cont) {
	int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
	auto keys = std::make_shared<std::vector<fdb::Key>>();
	for (int i = 0; i < numKeys; i++) {
		keys->push_back(randomExistingKey());
	}
	execTransaction(
	    [keys](auto ctx) {
		    for (const auto& key : *keys) {
			    ctx->tx().clear(key);
		    }
		    ctx->commit();
	    },
	    [this, keys, cont]() {
		    for (const auto& key : *keys) {
			    store.clear(key);
		    }
		    schedule(cont);
	    });
}

void ApiWorkload::randomClearRangeOp(TTaskFct cont) {
	fdb::Key begin = randomKeyName();
	fdb::Key end = randomKeyName();
	if (begin > end) {
		std::swap(begin, end);
	}
	execTransaction(
	    [begin, end](auto ctx) {
		    ctx->tx().clearRange(begin, end);
		    ctx->commit();
	    },
	    [this, begin, end, cont]() {
		    store.clear(begin, end);
		    schedule(cont);
	    });
}

} // namespace FdbApiTester
