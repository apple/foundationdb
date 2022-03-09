/*
 * TesterCorrectnessWorkload.cpp
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
#include "TesterWorkload.h"
#include "TesterUtil.h"
#include "TesterKeyValueStore.h"
#include "test/apitester/TesterScheduler.h"
#include <memory>
#include <optional>
#include <string_view>
#include <fmt/format.h>

namespace FdbApiTester {

class ApiCorrectnessWorkload : public WorkloadBase {
public:
	enum OpType { OP_INSERT, OP_GET, OP_CLEAR, OP_CLEAR_RANGE, OP_COMMIT_READ, OP_LAST = OP_COMMIT_READ };

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

	// The number of operations to be executed
	int numRandomOperations;

	// The ratio of reading existing keys
	double readExistingKeysRatio;

	// Key prefix
	std::string keyPrefix;

	ApiCorrectnessWorkload(const WorkloadConfig& config) : WorkloadBase(config) {
		minKeyLength = config.getIntOption("minKeyLength", 1);
		maxKeyLength = config.getIntOption("maxKeyLength", 64);
		minValueLength = config.getIntOption("minValueLength", 1);
		maxValueLength = config.getIntOption("maxValueLength", 1000);
		maxKeysPerTransaction = config.getIntOption("maxKeysPerTransaction", 50);
		initialSize = config.getIntOption("initialSize", 1000);
		numRandomOperations = config.getIntOption("numRandomOperations", 1000);
		readExistingKeysRatio = config.getFloatOption("readExistingKeysRatio", 0.9);
		keyPrefix = fmt::format("{}/", workloadId);
		numOpLeft = numRandomOperations;
	}

	void start() override {
		schedule([this]() {
			// 1. Clear data
			clearData([this]() {
				// 2. Populate initial data
				populateData([this]() {
					// 3. Generate random workload
					randomOperations();
				});
			});
		});
	}

private:
	std::string randomKeyName() { return keyPrefix + Random::get().randomStringLowerCase(minKeyLength, maxKeyLength); }

	std::string randomValue() { return Random::get().randomStringLowerCase(minValueLength, maxValueLength); }

	std::string randomNotExistingKey() {
		while (true) {
			std::string key = randomKeyName();
			if (!store.exists(key)) {
				return key;
			}
		}
	}

	std::string randomExistingKey() {
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

	std::string randomKey(double existingKeyRatio) {
		if (Random::get().randomBool(existingKeyRatio)) {
			return randomExistingKey();
		} else {
			return randomNotExistingKey();
		}
	}

	void randomInsertOp(TTaskFct cont) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
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

	void randomCommitReadOp(TTaskFct cont) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
		auto kvPairs = std::make_shared<std::vector<KeyValue>>();
		for (int i = 0; i < numKeys; i++) {
			kvPairs->push_back(KeyValue{ randomKey(readExistingKeysRatio), randomValue() });
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
			    auto results = std::make_shared<std::vector<std::optional<std::string>>>();
			    execTransaction(
			        [kvPairs, results](auto ctx) {
				        // TODO: Enable after merging with GRV caching
				        // ctx->tx()->setOption(FDB_TR_OPTION_USE_GRV_CACHE);
				        auto futures = std::make_shared<std::vector<Future>>();
				        for (const auto& kv : *kvPairs) {
					        futures->push_back(ctx->tx()->get(kv.key, false));
				        }
				        ctx->continueAfterAll(futures, [ctx, futures, results]() {
					        for (auto& f : *futures) {
						        results->push_back(((ValueFuture&)f).getValue());
					        }
					        ctx->done();
				        });
			        },
			        [this, kvPairs, results, cont]() {
				        ASSERT(results->size() == kvPairs->size());
				        for (int i = 0; i < kvPairs->size(); i++) {
					        auto expected = store.get((*kvPairs)[i].key);
					        if ((*results)[i] != expected) {
						        error(
						            fmt::format("randomCommitReadOp mismatch. key: {} expected: {:.80} actual: {:.80}",
						                        (*kvPairs)[i].key,
						                        expected,
						                        (*results)[i]));
					        }
				        }
				        schedule(cont);
			        });
		    });
	}

	void randomGetOp(TTaskFct cont) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
		auto keys = std::make_shared<std::vector<std::string>>();
		auto results = std::make_shared<std::vector<std::optional<std::string>>>();
		for (int i = 0; i < numKeys; i++) {
			keys->push_back(randomKey(readExistingKeysRatio));
		}
		execTransaction(
		    [keys, results](auto ctx) {
			    auto futures = std::make_shared<std::vector<Future>>();
			    for (const auto& key : *keys) {
				    futures->push_back(ctx->tx()->get(key, false));
			    }
			    ctx->continueAfterAll(futures, [ctx, futures, results]() {
				    for (auto& f : *futures) {
					    results->push_back(((ValueFuture&)f).getValue());
				    }
				    ctx->done();
			    });
		    },
		    [this, keys, results, cont]() {
			    ASSERT(results->size() == keys->size());
			    for (int i = 0; i < keys->size(); i++) {
				    auto expected = store.get((*keys)[i]);
				    if ((*results)[i] != expected) {
					    error(fmt::format("randomGetOp mismatch. key: {} expected: {:.80} actual: {:.80}",
					                      (*keys)[i],
					                      expected,
					                      (*results)[i]));
				    }
			    }
			    schedule(cont);
		    });
	}

	void randomClearOp(TTaskFct cont) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
		auto keys = std::make_shared<std::vector<std::string>>();
		for (int i = 0; i < numKeys; i++) {
			keys->push_back(randomExistingKey());
		}
		execTransaction(
		    [keys](auto ctx) {
			    for (const auto& key : *keys) {
				    ctx->tx()->clear(key);
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

	void randomClearRangeOp(TTaskFct cont) {
		std::string begin = randomKeyName();
		std::string end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		execTransaction(
		    [begin, end](auto ctx) {
			    ctx->tx()->clearRange(begin, end);
			    ctx->commit();
		    },
		    [this, begin, end, cont]() {
			    store.clear(begin, end);
			    schedule(cont);
		    });
	}

	void randomOperation(TTaskFct cont) {
		OpType txType = (store.size() == 0) ? OP_INSERT : (OpType)Random::get().randomInt(0, OP_LAST);
		switch (txType) {
		case OP_INSERT:
			randomInsertOp(cont);
			break;
		case OP_GET:
			randomGetOp(cont);
			break;
		case OP_CLEAR:
			randomClearOp(cont);
			break;
		case OP_CLEAR_RANGE:
			randomClearRangeOp(cont);
			break;
		case OP_COMMIT_READ:
			randomCommitReadOp(cont);
			break;
		}
	}

	void clearData(TTaskFct cont) {
		execTransaction(
		    [this](auto ctx) {
			    ctx->tx()->clearRange(keyPrefix, fmt::format("{}\xff", keyPrefix));
			    ctx->commit();
		    },
		    [this, cont]() { schedule(cont); });
	}

	void populateData(TTaskFct cont) {
		if (store.size() < initialSize) {
			randomInsertOp([this, cont]() { populateData(cont); });
		} else {
			info("Data population completed");
			schedule(cont);
		}
	}

	void randomOperations() {
		if (numOpLeft == 0)
			return;

		numOpLeft--;
		randomOperation([this]() { randomOperations(); });
	}

	int numOpLeft;
	KeyValueStore store;
};

WorkloadFactory<ApiCorrectnessWorkload> ApiCorrectnessWorkloadFactory("ApiCorrectness");

} // namespace FdbApiTester