/*
 * TesterCancelTransactionWorkload.cpp
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

namespace FdbApiTester {

class CancelTransactionWorkload : public ApiWorkload {
public:
	CancelTransactionWorkload(const WorkloadConfig& config) : ApiWorkload(config) {}

private:
	enum OpType { OP_CANCEL_GET, OP_CANCEL_AFTER_FIRST_GET, OP_LAST = OP_CANCEL_AFTER_FIRST_GET };

	// Start multiple concurrent gets and cancel the transaction
	void randomCancelGetTx(TTaskFct cont, std::optional<int> tenantId) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
		auto keys = std::make_shared<std::vector<fdb::Key>>();
		for (int i = 0; i < numKeys; i++) {
			keys->push_back(randomKey(readExistingKeysRatio, tenantId));
		}
		execTransaction(
		    [keys](auto ctx) {
			    std::vector<fdb::Future> futures;
			    for (const auto& key : *keys) {
				    futures.push_back(ctx->tx().get(key, false).eraseType());
			    }
			    ctx->done();
		    },
		    [this, cont]() { schedule(cont); },
		    getTenant(tenantId));
	}

	// Start multiple concurrent gets and cancel the transaction after the first get returns
	void randomCancelAfterFirstResTx(TTaskFct cont, std::optional<int> tenantId) {
		int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
		auto keys = std::make_shared<std::vector<fdb::Key>>();
		for (int i = 0; i < numKeys; i++) {
			keys->push_back(randomKey(readExistingKeysRatio, tenantId));
		}
		execTransaction(
		    [this, keys, tenantId](auto ctx) {
			    std::vector<fdb::Future> futures;
			    for (const auto& key : *keys) {
				    futures.push_back(ctx->tx().get(key, false).eraseType());
			    }
			    for (int i = 0; i < keys->size(); i++) {
				    fdb::Future f = futures[i];
				    auto expectedVal = stores[tenantId].get((*keys)[i]);
				    ctx->continueAfter(f, [expectedVal, f, this, ctx]() {
					    auto val = f.get<fdb::future_var::ValueRef>();
					    if (expectedVal != val) {
						    error(fmt::format("cancelAfterFirstResTx mismatch. expected: {:.80} actual: {:.80}",
						                      fdb::toCharsRef(expectedVal),
						                      fdb::toCharsRef(val)));
					    }
					    ctx->done();
				    });
			    }
		    },
		    [this, cont]() { schedule(cont); },
		    getTenant(tenantId));
	}

	void randomOperation(TTaskFct cont) override {
		std::optional<int> tenantId = randomTenant();
		OpType txType = (OpType)Random::get().randomInt(0, OP_LAST);

		switch (txType) {
		case OP_CANCEL_GET:
			randomCancelGetTx(cont, tenantId);
			break;
		case OP_CANCEL_AFTER_FIRST_GET:
			randomCancelAfterFirstResTx(cont, tenantId);
			break;
		}
	}
};

WorkloadFactory<CancelTransactionWorkload> MiscTestWorkloadFactory("CancelTransaction");

} // namespace FdbApiTester
