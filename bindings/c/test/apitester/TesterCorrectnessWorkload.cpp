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
#include "TesterApiWorkload.h"
#include "TesterUtil.h"
#include <memory>
#include <fmt/format.h>

namespace FdbApiTester {

class ApiCorrectnessWorkload : public ApiWorkload {
public:
	ApiCorrectnessWorkload(const WorkloadConfig& config) : ApiWorkload(config) {
		numRandomOperations = config.getIntOption("numRandomOperations", 1000);
		numOpLeft = numRandomOperations;
	}

	void runTests() override { randomOperations(); }

private:
	enum OpType { OP_INSERT, OP_GET, OP_CLEAR, OP_CLEAR_RANGE, OP_COMMIT_READ, OP_LAST = OP_COMMIT_READ };

	// The number of operations to be executed
	int numRandomOperations;

	// Operations counter
	int numOpLeft;

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
				        ctx->continueAfterAll(*futures, [ctx, futures, results]() {
					        results->clear();
					        for (auto& f : *futures) {
						        results->push_back(((ValueFuture&)f).getValue());
					        }
					        ASSERT(results->size() == futures->size());
					        ctx->done();
				        });
			        },
			        [this, kvPairs, results, cont]() {
				        ASSERT(results->size() == kvPairs->size());
				        for (int i = 0; i < kvPairs->size(); i++) {
					        auto expected = store.get((*kvPairs)[i].key);
					        auto actual = (*results)[i];
					        if (actual != expected) {
						        error(
						            fmt::format("randomCommitReadOp mismatch. key: {} expected: {:.80} actual: {:.80}",
						                        (*kvPairs)[i].key,
						                        expected,
						                        actual));
						        ASSERT(false);
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
			    ctx->continueAfterAll(*futures, [ctx, futures, results]() {
				    results->clear();
				    for (auto& f : *futures) {
					    results->push_back(((ValueFuture&)f).getValue());
				    }
				    ASSERT(results->size() == futures->size());
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

	void randomOperations() {
		if (numOpLeft == 0)
			return;

		numOpLeft--;
		randomOperation([this]() { randomOperations(); });
	}
};

WorkloadFactory<ApiCorrectnessWorkload> ApiCorrectnessWorkloadFactory("ApiCorrectness");

} // namespace FdbApiTester