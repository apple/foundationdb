/*
 * TesterMultiTenantCorrectnessWorkload.cpp
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
#include <map>
#include <fmt/format.h>

namespace FdbApiTester {

class MultiTenantCorrectnessWorkload : public ApiWorkload {
public:
	MultiTenantCorrectnessWorkload(const WorkloadConfig& config) : ApiWorkload(config) {
		numTenants = config.numTenants;
	}

private:
	enum OpType { OP_INSERT, OP_GET, OP_GET_RANGE, OP_CLEAR, OP_CLEAR_RANGE, OP_LAST = OP_CLEAR_RANGE };
	int numTenants = -1;

	void start() override {
		schedule([this]() { setupTenants([this]() { runTests(); }, this->numTenants); });
	}

	void setupTenants(TTaskFct cont, int numTenants) {
		execTransaction(
		    [numTenants](auto ctx) {
			    for (int i = 0; i < numTenants; i++) {
				    std::string tenant_name = "tenant" + std::to_string(i);
				    fdb::Tenant::createTenant(ctx->tx(), fdb::toBytesRef(tenant_name));
			    }
			    ctx->commit();
		    },
		    [this, cont]() { schedule(cont); });
	}

	void randomGetOp(TTaskFct cont, int tenantId) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
		auto keys = std::make_shared<std::vector<fdb::Key>>();
		auto results = std::make_shared<std::vector<std::optional<fdb::Value>>>();
		int i = 0;
		while (i < numKeys) {
			fdb::Key key = randomKey(readExistingKeysRatio);
			if (key.compare(0, keyPrefix.size(), keyPrefix) == 0) {
				keys->push_back(key);
				i++;
			}
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
		    },
		    tenantId);
	}

	void getRangeLoop(std::shared_ptr<ITransactionContext> ctx,
	                  fdb::KeySelector begin,
	                  fdb::KeySelector end,
	                  std::shared_ptr<std::vector<fdb::KeyValue>> results) {
		auto f = ctx->tx().getRange(begin,
		                            end,
		                            0 /*limit*/,
		                            0 /*target_bytes*/,
		                            FDB_STREAMING_MODE_WANT_ALL,
		                            0 /*iteration*/,
		                            false /*snapshot*/,
		                            false /*reverse*/);
		ctx->continueAfter(f, [this, ctx, f, end, results]() {
			auto out = copyKeyValueArray(f.get());
			results->insert(results->end(), out.first.begin(), out.first.end());
			const bool more = out.second;
			if (more) {
				// Fetch the remaining results.
				getRangeLoop(ctx, fdb::key_select::firstGreaterThan(results->back().key), end, results);
			} else {
				ctx->done();
			}
		});
	}

	void randomGetRangeOp(TTaskFct cont, int tenantId) {
		auto begin = randomKey(readExistingKeysRatio);
		auto end = randomKey(readExistingKeysRatio);
		if (begin > end) {
			std::swap(begin, end);
		}

		auto results = std::make_shared<std::vector<fdb::KeyValue>>();

		execTransaction(
		    [this, begin, end, results](auto ctx) {
			    // Clear the results vector, in case the transaction is retried.
			    results->clear();

			    getRangeLoop(ctx,
			                 fdb::key_select::firstGreaterOrEqual(begin),
			                 fdb::key_select::firstGreaterOrEqual(end),
			                 results);
		    },
		    [this, begin, end, results, cont]() {
			    auto expected = store.getRange(begin, end, results->size() + 10, false);
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
		    tenantId);
	}

	void randomClearRangeOp(TTaskFct cont, int tenantId) {
		auto begin = randomKeyName();
		auto end = randomKeyName();
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
		    },
		    tenantId);
	}

	void randomClearOp(TTaskFct cont, int tenantId) {
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
		    },
		    tenantId);
	}

	void randomInsertOp(TTaskFct cont, int tenantId) {
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
		    },
		    tenantId);
	}

	void randomOperation(TTaskFct cont) override {
		OpType txType = (store.size() < initialSize) ? OP_INSERT : (OpType)Random::get().randomInt(0, OP_LAST);
		int tenantId = Random::get().randomInt(0, numTenants - 1);
		keyPrefix = fdb::toBytesRef(fmt::format("{}/{}/", workloadId, tenantId));
		switch (txType) {
		case OP_INSERT:
			randomInsertOp(cont, tenantId);
			break;
		case OP_CLEAR:
			randomClearOp(cont, tenantId);
			break;
		case OP_CLEAR_RANGE:
			randomClearRangeOp(cont, tenantId);
			break;
		case OP_GET:
			randomGetOp(cont, tenantId);
			break;
		case OP_GET_RANGE:
			randomGetRangeOp(cont, tenantId);
			break;
		}
	}
};

WorkloadFactory<MultiTenantCorrectnessWorkload> MultiTenantCorrectnessWorkloadFactory("MultiTenantCorrectness");

} // namespace FdbApiTester
