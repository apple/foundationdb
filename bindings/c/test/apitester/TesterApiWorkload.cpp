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

	for (int i = 0; i < config.numTenants; ++i) {
		tenants.push_back(fdb::ByteString(fdb::toBytesRef("tenant" + std::to_string(i))));
	}
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
			// 2. Create tenants if necessary.
			createTenantsIfNecessary([this] {
				// 3. Workload setup.
				setup([this]() {
					// 4. Populate initial data
					populateData([this]() {
						// 5. Generate random workload
						runTests();
					});
				});
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

fdb::Key ApiWorkload::randomNotExistingKey(std::optional<int> tenantId) {
	while (true) {
		fdb::Key key = randomKeyName();
		if (!stores[tenantId].exists(key)) {
			return key;
		}
	}
}

fdb::Key ApiWorkload::randomExistingKey(std::optional<int> tenantId) {
	fdb::Key genKey = randomKeyName();
	fdb::Key key = stores[tenantId].getKey(genKey, true, 1);
	if (key != stores[tenantId].endKey()) {
		return key;
	}
	key = stores[tenantId].getKey(genKey, true, 0);
	if (key != stores[tenantId].startKey()) {
		return key;
	}
	info("No existing key found, using a new random key.");
	return genKey;
}

fdb::Key ApiWorkload::randomKey(double existingKeyRatio, std::optional<int> tenantId) {
	if (Random::get().randomBool(existingKeyRatio)) {
		return randomExistingKey(tenantId);
	} else {
		return randomNotExistingKey(tenantId);
	}
}

fdb::KeyRange ApiWorkload::randomNonEmptyKeyRange() {
	fdb::KeyRange keyRange;
	keyRange.beginKey = randomKeyName();
	// avoid empty key range
	do {
		keyRange.endKey = randomKeyName();
	} while (keyRange.beginKey == keyRange.endKey);

	if (keyRange.beginKey > keyRange.endKey) {
		std::swap(keyRange.beginKey, keyRange.endKey);
	}
	ASSERT(keyRange.beginKey < keyRange.endKey);
	return keyRange;
}

std::optional<int> ApiWorkload::randomTenant() {
	if (tenants.size() > 0) {
		return Random::get().randomInt(0, tenants.size() - 1);
	} else {
		return {};
	}
}

void ApiWorkload::populateDataTx(TTaskFct cont, std::optional<int> tenantId) {
	int numKeys = maxKeysPerTransaction;
	auto kvPairs = std::make_shared<std::vector<fdb::KeyValue>>();
	for (int i = 0; i < numKeys; i++) {
		kvPairs->push_back(fdb::KeyValue{ randomNotExistingKey(tenantId), randomValue() });
	}
	execTransaction(
	    [kvPairs](auto ctx) {
		    ctx->makeSelfConflicting();
		    for (const fdb::KeyValue& kv : *kvPairs) {
			    ctx->tx().set(kv.key, kv.value);
		    }
		    ctx->commit();
	    },
	    [this, tenantId, kvPairs, cont]() {
		    for (const fdb::KeyValue& kv : *kvPairs) {
			    stores[tenantId].set(kv.key, kv.value);
		    }
		    schedule(cont);
	    },
	    getTenant(tenantId));
}

void ApiWorkload::clearTenantData(TTaskFct cont, std::optional<int> tenantId) {
	execTransaction(
	    [this](auto ctx) {
		    ctx->tx().clearRange(keyPrefix, keyPrefix + fdb::Key(1, '\xff'));
		    ctx->commit();
	    },
	    [this, tenantId, cont]() {
		    if (tenantId && tenantId.value() < tenants.size() - 1) {
			    clearTenantData(cont, tenantId.value() + 1);
		    } else {
			    schedule(cont);
		    }
	    },
	    getTenant(tenantId));
}

void ApiWorkload::clearData(TTaskFct cont) {
	execTransaction(
	    [this](auto ctx) {
		    // Make this self-conflicting, so that if we're retrying on timeouts
		    // once we get a successful commit all previous attempts are no
		    // longer in-flight.
		    ctx->makeSelfConflicting();
		    ctx->tx().clearRange(keyPrefix, keyPrefix + fdb::Key(1, '\xff'));
		    ctx->commit();
	    },
	    [this, cont]() { schedule(cont); });
}

void ApiWorkload::populateTenantData(TTaskFct cont, std::optional<int> tenantId) {
	while (stores[tenantId].size() >= initialSize && tenantId && tenantId.value() < tenants.size()) {
		++tenantId.value();
	}

	if (tenantId >= tenants.size() || stores[tenantId].size() >= initialSize) {
		info("Data population completed");
		schedule(cont);
	} else {
		populateDataTx([this, cont, tenantId]() { populateTenantData(cont, tenantId); }, tenantId);
	}
}

void ApiWorkload::createTenants(TTaskFct cont) {
	execTransaction(
	    [this](auto ctx) {
		    auto futures = std::make_shared<std::vector<fdb::Future>>();
		    for (auto tenant : tenants) {
			    futures->push_back(fdb::Tenant::getTenant(ctx->tx(), tenant));
		    }
		    ctx->continueAfterAll(*futures, [this, ctx, futures]() {
			    for (int i = 0; i < futures->size(); ++i) {
				    if (!(*futures)[i].get<fdb::future_var::ValueRef>()) {
					    fdb::Tenant::createTenant(ctx->tx(), tenants[i]);
				    }
			    }
			    ctx->commit();
		    });
	    },
	    [this, cont]() { schedule(cont); });
}

void ApiWorkload::createTenantsIfNecessary(TTaskFct cont) {
	if (tenants.size() > 0) {
		createTenants(cont);
	} else {
		schedule(cont);
	}
}

void ApiWorkload::populateData(TTaskFct cont) {
	if (tenants.size() > 0) {
		populateTenantData(cont, std::make_optional(0));
	} else {
		populateTenantData(cont, {});
	}
}

void ApiWorkload::setup(TTaskFct cont) {
	schedule(cont);
}

void ApiWorkload::randomInsertOp(TTaskFct cont, std::optional<int> tenantId) {
	int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
	auto kvPairs = std::make_shared<std::vector<fdb::KeyValue>>();
	for (int i = 0; i < numKeys; i++) {
		kvPairs->push_back(fdb::KeyValue{ randomNotExistingKey(tenantId), randomValue() });
	}
	execTransaction(
	    [kvPairs](auto ctx) {
		    ctx->makeSelfConflicting();
		    for (const fdb::KeyValue& kv : *kvPairs) {
			    ctx->tx().set(kv.key, kv.value);
		    }
		    ctx->commit();
	    },
	    [this, kvPairs, cont, tenantId]() {
		    for (const fdb::KeyValue& kv : *kvPairs) {
			    stores[tenantId].set(kv.key, kv.value);
		    }
		    schedule(cont);
	    },
	    getTenant(tenantId));
}

void ApiWorkload::randomClearOp(TTaskFct cont, std::optional<int> tenantId) {
	int numKeys = Random::get().randomInt(1, maxKeysPerTransaction);
	auto keys = std::make_shared<std::vector<fdb::Key>>();
	for (int i = 0; i < numKeys; i++) {
		keys->push_back(randomExistingKey(tenantId));
	}
	execTransaction(
	    [keys](auto ctx) {
		    for (const auto& key : *keys) {
			    ctx->makeSelfConflicting();
			    ctx->tx().clear(key);
		    }
		    ctx->commit();
	    },
	    [this, keys, cont, tenantId]() {
		    for (const auto& key : *keys) {
			    stores[tenantId].clear(key);
		    }
		    schedule(cont);
	    },
	    getTenant(tenantId));
}

void ApiWorkload::randomClearRangeOp(TTaskFct cont, std::optional<int> tenantId) {
	fdb::Key begin = randomKeyName();
	fdb::Key end = randomKeyName();
	if (begin > end) {
		std::swap(begin, end);
	}
	execTransaction(
	    [begin, end](auto ctx) {
		    ctx->makeSelfConflicting();
		    ctx->tx().clearRange(begin, end);
		    ctx->commit();
	    },
	    [this, begin, end, cont, tenantId]() {
		    stores[tenantId].clear(begin, end);
		    schedule(cont);
	    },
	    getTenant(tenantId));
}

std::optional<fdb::BytesRef> ApiWorkload::getTenant(std::optional<int> tenantId) {
	if (tenantId) {
		return tenants[*tenantId];
	} else {
		return {};
	}
}

std::string ApiWorkload::debugTenantStr(std::optional<int> tenantId) {
	return tenantId.has_value() ? fmt::format("(tenant {0})", tenantId.value()) : "()";
}

// BlobGranule setup.
// This blobbifies ['\x00', '\xff') per tenant or for the whole database if there are no tenants.
void ApiWorkload::setupBlobGranules(TTaskFct cont) {
	// This count is used to synchronize the # of tenant blobbifyRange() calls to ensure
	// we only start the workload once blobbification has fully finished.
	auto blobbifiedCount = std::make_shared<std::atomic<int>>(1);

	if (tenants.empty()) {
		blobbifiedCount->store(1);
		blobbifyTenant({}, blobbifiedCount, cont);
	} else {
		blobbifiedCount->store(tenants.size());
		for (int i = 0; i < tenants.size(); i++) {
			schedule([=]() { blobbifyTenant(i, blobbifiedCount, cont); });
		}
	}
}

void ApiWorkload::blobbifyTenant(std::optional<int> tenantId,
                                 std::shared_ptr<std::atomic<int>> blobbifiedCount,
                                 TTaskFct cont) {
	execOperation(
	    [=](auto ctx) {
		    fdb::Key begin(1, '\x00');
		    fdb::Key end(1, '\xff');

		    info(fmt::format("setup: blobbifying {}: [\\x00 - \\xff)\n", debugTenantStr(tenantId)));

		    // wait for blobbification before returning
		    fdb::Future f = ctx->dbOps()->blobbifyRangeBlocking(begin, end).eraseType();
		    ctx->continueAfter(f, [ctx, f]() {
			    bool success = f.get<fdb::future_var::Bool>();
			    ASSERT(success);
			    ctx->done();
		    });
	    },
	    [=]() {
		    info(fmt::format("setup: blobbify done {}: [\\x00 - \\xff)\n", debugTenantStr(tenantId)));
		    if (blobbifiedCount->fetch_sub(1) == 1) {
			    schedule(cont);
		    }
	    },
	    /*tenant=*/getTenant(tenantId),
	    /* failOnError = */ false);
}

} // namespace FdbApiTester
