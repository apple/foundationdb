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
#include "test/fdb_api.hpp"
#include <memory>
#include <fmt/format.h>

namespace FdbApiTester {

class ApiCorrectnessWorkload : public ApiWorkload {
public:
	ApiCorrectnessWorkload(const WorkloadConfig& config) : ApiWorkload(config) {}

private:
	enum OpType { OP_INSERT, OP_GET, OP_CLEAR, OP_CLEAR_RANGE, OP_COMMIT_READ, OP_LAST = OP_COMMIT_READ };

	void randomCommitReadOp(TTaskFct cont) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
		auto kvPairs = std::make_shared<std::vector<fdb::KeyValue>>();
		for (int i = 0; i < numKeys; i++) {
			kvPairs->push_back(fdb::KeyValue{ randomKey(readExistingKeysRatio), randomValue() });
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
			    auto results = std::make_shared<std::vector<std::optional<fdb::Value>>>();
			    execTransaction(
			        [kvPairs, results, this](auto ctx) {
				        if (apiVersion >= 710) {
					        // Test GRV caching in 7.1 and later
					        ctx->tx().setOption(FDB_TR_OPTION_USE_GRV_CACHE);
				        }
				        auto futures = std::make_shared<std::vector<fdb::Future>>();
				        for (const auto& kv : *kvPairs) {
					        futures->push_back(ctx->tx().get(kv.key, false));
				        }
				        ctx->continueAfterAll(*futures, [ctx, futures, results]() {
					        results->clear();
					        for (auto& f : *futures) {
						        results->push_back(copyValueRef(f.get<fdb::future_var::ValueRef>()));
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
						                        fdb::toCharsRef((*kvPairs)[i].key),
						                        fdb::toCharsRef(expected.value()),
						                        fdb::toCharsRef(actual.value())));
						        ASSERT(false);
					        }
				        }
				        schedule(cont);
			        });
		    });
	}

	void randomGetOp(TTaskFct cont) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
		auto keys = std::make_shared<std::vector<fdb::Key>>();
		auto results = std::make_shared<std::vector<std::optional<fdb::Value>>>();
		for (int i = 0; i < numKeys; i++) {
			keys->push_back(randomKey(readExistingKeysRatio));
		}
		execTransaction(
		    [keys, results](auto ctx) {
			    auto futures = std::make_shared<std::vector<fdb::Future>>();
			    for (const auto& key : *keys) {
				    futures->push_back(ctx->tx().get(key, false));
			    }
			    ctx->continueAfterAll(*futures, [ctx, futures, results]() {
				    results->clear();
				    for (auto& f : *futures) {
					    results->push_back(copyValueRef(f.get<fdb::future_var::ValueRef>()));
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
					                      fdb::toCharsRef((*keys)[i]),
					                      fdb::toCharsRef(expected.value()),
					                      fdb::toCharsRef((*results)[i].value())));
				    }
			    }
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
};

WorkloadFactory<ApiCorrectnessWorkload> ApiCorrectnessWorkloadFactory("ApiCorrectness");

} // namespace FdbApiTester
