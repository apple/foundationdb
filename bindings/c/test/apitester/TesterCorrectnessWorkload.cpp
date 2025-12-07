/*
 * TesterCorrectnessWorkload.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
	enum OpType {
		OP_INSERT,
		OP_GET,
		OP_GET_KEY,
		OP_CLEAR,
		OP_GET_RANGE,
		OP_CLEAR_RANGE,
		OP_COMMIT_READ,
		OP_LAST = OP_COMMIT_READ
	};

	void randomCommitReadOp(TTaskFct cont, std::optional<int> tenantId) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
		auto kvPairs = std::make_shared<std::vector<fdb::KeyValue>>();
		for (int i = 0; i < numKeys; i++) {
			kvPairs->push_back(fdb::KeyValue{ randomKey(readExistingKeysRatio, tenantId), randomValue() });
		}
		execTransaction(
		    [kvPairs](auto ctx) {
			    for (const fdb::KeyValue& kv : *kvPairs) {
				    ctx->tx().addReadConflictRange(kv.key, kv.key + fdb::Key(1, '\x00'));
				    ctx->tx().set(kv.key, kv.value);
			    }
			    ctx->commit();
		    },
		    [this, kvPairs, cont, tenantId]() {
			    for (const fdb::KeyValue& kv : *kvPairs) {
				    stores[tenantId].set(kv.key, kv.value);
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
			        [this, kvPairs, results, cont, tenantId]() {
				        ASSERT(results->size() == kvPairs->size());
				        for (int i = 0; i < kvPairs->size(); i++) {
					        auto expected = stores[tenantId].get((*kvPairs)[i].key);
					        auto actual = (*results)[i];
					        if (actual != expected) {
						        error(
						            fmt::format("randomCommitReadOp mismatch. key: {} expected: {:.80} actual: {:.80}",
						                        fdb::toCharsRef((*kvPairs)[i].key),
						                        fdb::toCharsRef(expected),
						                        fdb::toCharsRef(actual)));
						        ASSERT(false);
					        }
				        }
				        schedule(cont);
			        },
			        getTenant(tenantId));
		    },
		    getTenant(tenantId));
	}

	void randomGetOp(TTaskFct cont, std::optional<int> tenantId) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
		auto keys = std::make_shared<std::vector<fdb::Key>>();
		auto results = std::make_shared<std::vector<std::optional<fdb::Value>>>();
		for (int i = 0; i < numKeys; i++) {
			keys->push_back(randomKey(readExistingKeysRatio, tenantId));
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
		    [this, keys, results, cont, tenantId]() {
			    ASSERT(results->size() == keys->size());
			    for (int i = 0; i < keys->size(); i++) {
				    auto expected = stores[tenantId].get((*keys)[i]);
				    if ((*results)[i] != expected) {
					    error(fmt::format("randomGetOp mismatch. key: {} expected: {:.80} actual: {:.80}",
					                      fdb::toCharsRef((*keys)[i]),
					                      fdb::toCharsRef(expected),
					                      fdb::toCharsRef((*results)[i])));
				    }
			    }
			    schedule(cont);
		    },
		    getTenant(tenantId));
	}

	void randomGetKeyOp(TTaskFct cont, std::optional<int> tenantId) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
		auto keysWithSelectors = std::make_shared<std::vector<std::pair<fdb::Key, fdb::KeySelector>>>();
		auto results = std::make_shared<std::vector<fdb::Key>>();
		keysWithSelectors->reserve(numKeys);
		for (int i = 0; i < numKeys; i++) {
			auto key = randomKey(readExistingKeysRatio, tenantId);
			fdb::KeySelector selector;
			selector.keyLength = key.size();
			selector.orEqual = Random::get().randomBool(0.5);
			selector.offset = Random::get().randomInt(0, 4);
			keysWithSelectors->emplace_back(std::move(key), std::move(selector));
			// We would ideally do the following above:
			//   selector.key = key.data();
			// but key.data() may become invalid after the key is moved to the vector.
			// So instead, we update the pointer here to the string already in the vector.
			keysWithSelectors->back().second.key = keysWithSelectors->back().first.data();
		}
		execTransaction(
		    [keysWithSelectors, results](auto ctx) {
			    auto futures = std::make_shared<std::vector<fdb::Future>>();
			    for (const auto& keyWithSelector : *keysWithSelectors) {
				    auto key = keyWithSelector.first;
				    auto selector = keyWithSelector.second;
				    futures->push_back(ctx->tx().getKey(selector, false));
			    }
			    ctx->continueAfterAll(*futures, [ctx, futures, results]() {
				    results->clear();
				    for (auto& f : *futures) {
					    results->push_back(fdb::Key(f.get<fdb::future_var::KeyRef>()));
				    }
				    ASSERT(results->size() == futures->size());
				    ctx->done();
			    });
		    },
		    [this, keysWithSelectors, results, cont, tenantId]() {
			    ASSERT(results->size() == keysWithSelectors->size());
			    for (int i = 0; i < keysWithSelectors->size(); i++) {
				    auto const& key = (*keysWithSelectors)[i].first;
				    auto const& selector = (*keysWithSelectors)[i].second;
				    auto expected = stores[tenantId].getKey(key, selector.orEqual, selector.offset);
				    auto actual = (*results)[i];
				    // Local store only contains data for the current client, while fdb contains data from multiple
				    // clients. If getKey returned a key outside of the range for the current client, adjust the result
				    // to match what would be expected in the local store.
				    if (actual.substr(0, keyPrefix.size()) < keyPrefix) {
					    actual = stores[tenantId].startKey();
				    } else if ((*results)[i].substr(0, keyPrefix.size()) > keyPrefix) {
					    actual = stores[tenantId].endKey();
				    }
				    if (actual != expected) {
					    error(fmt::format("randomGetKeyOp mismatch. key: {}, orEqual: {}, offset: {}, expected: {} "
					                      "actual: {}",
					                      fdb::toCharsRef(key),
					                      selector.orEqual,
					                      selector.offset,
					                      fdb::toCharsRef(expected),
					                      fdb::toCharsRef(actual)));
				    }
			    }
			    schedule(cont);
		    },
		    getTenant(tenantId));
	}

	void getRangeLoop(std::shared_ptr<ITransactionContext> ctx,
	                  fdb::KeySelector begin,
	                  fdb::Key endKey,
	                  std::shared_ptr<std::vector<fdb::KeyValue>> results) {
		auto f = ctx->tx().getRange(begin,
		                            fdb::key_select::firstGreaterOrEqual(endKey),
		                            0 /*limit*/,
		                            0 /*target_bytes*/,
		                            FDB_STREAMING_MODE_WANT_ALL,
		                            0 /*iteration*/,
		                            false /*snapshot*/,
		                            false /*reverse*/);
		ctx->continueAfter(f, [this, ctx, f, endKey, results]() {
			auto out = copyKeyValueArray(f.get());
			results->insert(results->end(), out.first.begin(), out.first.end());
			const bool more = out.second;
			if (more) {
				// Fetch the remaining results.
				getRangeLoop(ctx, fdb::key_select::firstGreaterThan(results->back().key), endKey, results);
			} else {
				ctx->done();
			}
		});
	}

	void randomGetRangeOp(TTaskFct cont, std::optional<int> tenantId) {
		auto begin = randomKey(readExistingKeysRatio, tenantId);
		auto end = randomKey(readExistingKeysRatio, tenantId);
		auto results = std::make_shared<std::vector<fdb::KeyValue>>();

		execTransaction(
		    [this, begin, end, results](auto ctx) {
			    // Clear the results vector, in case the transaction is retried.
			    results->clear();

			    getRangeLoop(ctx, fdb::key_select::firstGreaterOrEqual(begin), end, results);
		    },
		    [this, begin, end, results, cont, tenantId]() {
			    auto expected = stores[tenantId].getRange(begin, end, results->size() + 10, false);
			    if (results->size() != expected.size()) {
				    error(fmt::format("randomGetRangeOp mismatch. expected {} keys, actual {} keys",
				                      expected.size(),
				                      results->size()));
			    } else {
				    auto expected_kv = expected.begin();
				    for (auto actual_kv : *results) {
					    if (actual_kv.key != expected_kv->key || actual_kv.value != expected_kv->value) {
						    error(fmt::format(
						        "randomGetRangeOp mismatch. expected key: {} actual key: {} expected value: "
						        "{:.80} actual value: {:.80}",
						        fdb::toCharsRef(expected_kv->key),
						        fdb::toCharsRef(actual_kv.key),
						        fdb::toCharsRef(expected_kv->value),
						        fdb::toCharsRef(actual_kv.value)));
					    }
					    expected_kv++;
				    }
			    }
			    schedule(cont);
		    },
		    getTenant(tenantId));
	}

	void randomOperation(TTaskFct cont) {
		std::optional<int> tenantId = randomTenant();
		OpType txType = (stores[tenantId].size() == 0) ? OP_INSERT : (OpType)Random::get().randomInt(0, OP_LAST);

		switch (txType) {
		case OP_INSERT:
			randomInsertOp(cont, tenantId);
			break;
		case OP_GET:
			randomGetOp(cont, tenantId);
			break;
		case OP_GET_KEY:
			randomGetKeyOp(cont, tenantId);
			break;
		case OP_CLEAR:
			randomClearOp(cont, tenantId);
			break;
		case OP_GET_RANGE:
			randomGetRangeOp(cont, tenantId);
			break;
		case OP_CLEAR_RANGE:
			randomClearRangeOp(cont, tenantId);
			break;
		case OP_COMMIT_READ:
			randomCommitReadOp(cont, tenantId);
			break;
		}
	}
};

WorkloadFactory<ApiCorrectnessWorkload> ApiCorrectnessWorkloadFactory("ApiCorrectness");

} // namespace FdbApiTester
