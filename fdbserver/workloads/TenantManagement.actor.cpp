/*
 * TenantManagement.actor.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct TenantManagementWorkload : TestWorkload {
	struct TenantState {
		int64_t id;
		bool empty;

		TenantState() : id(-1), empty(true) {}
		TenantState(int64_t id, bool empty) : id(id), empty(empty) {}
	};

	std::map<TenantName, TenantState> createdTenants;
	int64_t maxId = -1;
	Key tenantSubspace;

	const Key keyName = "key"_sr;
	const Key tenantSubspaceKey = "tenant_subspace"_sr;
	const Value noTenantValue = "no_tenant"_sr;

	int maxTenants;
	double testDuration;

	TenantManagementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		maxTenants = getOption(options, "maxTenants"_sr, 1000);
		testDuration = getOption(options, "testDuration"_sr, 60.0);
	}

	std::string description() const override { return "TenantManagement"; }

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	ACTOR Future<Void> _setup(Database cx, TenantManagementWorkload* self) {
		state Transaction tr(cx);
		if (self->clientId == 0) {
			self->tenantSubspace = makeString(deterministicRandom()->randomInt(0, 10));
			generateRandomData(mutateString(self->tenantSubspace), self->tenantSubspace.size());
			loop {
				try {
					tr.setOption(FDBTransactionOptions::RAW_ACCESS);
					tr.set(self->keyName, self->noTenantValue);
					tr.set(self->tenantSubspaceKey, self->tenantSubspace);
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		} else {
			loop {
				try {
					tr.setOption(FDBTransactionOptions::RAW_ACCESS);
					Optional<Value> val = wait(tr.get(self->tenantSubspaceKey));
					if (val.present()) {
						self->tenantSubspace = val.get();
						break;
					}

					wait(delay(1.0));
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

		return Void();
	}

	TenantName chooseTenantName() {
		TenantName tenant(format("tenant_%d_%d", clientId, deterministicRandom()->randomInt(0, maxTenants)));
		if (deterministicRandom()->random01() < 0.02) {
			tenant = tenant.withPrefix("\xff"_sr);
		}

		return tenant;
	}

	ACTOR Future<Void> createTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();
		state bool alreadyExists = self->createdTenants.count(tenant);
		try {
			wait(ManagementAPI::createTenant(cx.getReference(), tenant));
			ASSERT(!alreadyExists);
			ASSERT(!tenant.startsWith("\xff"_sr));

			state Optional<TenantMapEntry> entry = wait(ManagementAPI::tryGetTenant(cx.getReference(), tenant));
			ASSERT(entry.present());
			ASSERT(entry.get().id > self->maxId);
			ASSERT(entry.get().prefix.startsWith(self->tenantSubspace));

			self->maxId = entry.get().id;

			state bool insertData = deterministicRandom()->random01() < 0.5;
			if (insertData) {
				state Transaction tr(cx, tenant);
				loop {
					try {
						tr.set(self->keyName, tenant);
						wait(tr.commit());
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}

				tr = Transaction(cx);
				loop {
					try {
						tr.setOption(FDBTransactionOptions::RAW_ACCESS);
						Optional<Value> val = wait(tr.get(self->keyName.withPrefix(entry.get().prefix)));
						ASSERT(val.present());
						ASSERT(val.get() == tenant);
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}
			}

			self->createdTenants[tenant] = TenantState(entry.get().id, insertData);
		} catch (Error& e) {
			if (e.code() == error_code_tenant_already_exists) {
				ASSERT(alreadyExists);
			} else if (e.code() == error_code_invalid_tenant_name) {
				ASSERT(tenant.startsWith("\xff"_sr));
			} else {
				TraceEvent(SevError, "CreateTenantFailure").detail("TenantName", tenant).error(e);
			}
		}

		return Void();
	}

	ACTOR Future<Void> deleteTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();

		auto itr = self->createdTenants.find(tenant);
		state bool alreadyExists = itr != self->createdTenants.end();
		state bool isEmpty = (itr == self->createdTenants.end() || itr->second.empty);

		try {
			if (alreadyExists && deterministicRandom()->random01() < 0.5) {
				state Transaction tr(cx, tenant);
				loop {
					try {
						tr.clear(self->keyName);
						wait(tr.commit());
						isEmpty = true;
						auto itr = self->createdTenants.find(tenant);
						ASSERT(itr != self->createdTenants.end());
						itr->second.empty = true;
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}
			}

			wait(ManagementAPI::deleteTenant(cx.getReference(), tenant));
			ASSERT(alreadyExists);
			ASSERT(isEmpty);
			self->createdTenants.erase(tenant);
		} catch (Error& e) {
			if (e.code() == error_code_tenant_not_found) {
				ASSERT(!alreadyExists);
			} else if (e.code() == error_code_tenant_not_empty) {
				ASSERT(!isEmpty);
			} else {
				TraceEvent(SevError, "DeleteTenantFailure").detail("TenantName", tenant).error(e);
			}
		}

		return Void();
	}

	ACTOR Future<Void> checkTenant(Database cx,
	                               TenantManagementWorkload* self,
	                               TenantName tenant,
	                               TenantState tenantState) {
		state Transaction tr(cx, tenant);
		loop {
			try {
				RangeResult result = wait(tr.getRange(KeyRangeRef(""_sr, "\xff"_sr), 2));
				if (tenantState.empty) {
					ASSERT(result.size() == 0);
				} else {
					ASSERT(result.size() == 1);
					ASSERT(result[0].key == self->keyName);
					ASSERT(result[0].value == tenant);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<Void> getTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName();
		auto itr = self->createdTenants.find(tenant);
		state bool alreadyExists = itr != self->createdTenants.end();
		state TenantState tenantState = itr->second;

		try {
			TenantMapEntry entry = wait(ManagementAPI::getTenant(cx.getReference(), tenant));
			ASSERT(alreadyExists);
			ASSERT(entry.id == tenantState.id);
			wait(self->checkTenant(cx, self, tenant, tenantState));
		} catch (Error& e) {
			if (e.code() == error_code_tenant_not_found) {
				ASSERT(!alreadyExists);
			} else {
				TraceEvent(SevError, "GetTenantFailure").detail("TenantName", tenant).error(e);
			}
		}

		return Void();
	}

	ACTOR Future<Void> listTenants(Database cx, TenantManagementWorkload* self) {
		state TenantName beginTenant = self->chooseTenantName();
		state TenantName endTenant = self->chooseTenantName();
		state int limit = std::min(CLIENT_KNOBS->TOO_MANY, deterministicRandom()->randomInt(0, self->maxTenants * 2));

		try {
			Standalone<VectorRef<TenantNameRef>> tenants =
			    wait(ManagementAPI::listTenants(cx.getReference(), beginTenant, endTenant, limit));

			ASSERT(tenants.size() <= limit);

			int index = 0;
			auto itr = self->createdTenants.begin();
			for (; index < tenants.size(); ++itr) {
				ASSERT(itr != self->createdTenants.end());
				ASSERT(itr->first == tenants[index++]);
			}

			ASSERT(tenants.size() == limit || itr == self->createdTenants.end());
			if (tenants.size() == limit) {
				ASSERT(itr == self->createdTenants.end() || itr->first >= endTenant);
			}
		} catch (Error& e) {
			TraceEvent(SevError, "ListTenantFailure")
			    .detail("BeginTenant", beginTenant)
			    .detail("EndTenant", endTenant)
			    .error(e);
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	ACTOR Future<Void> _start(Database cx, TenantManagementWorkload* self) {
		state double start = now();
		while (now() < start + self->testDuration) {
			state int operation = deterministicRandom()->randomInt(0, 4);
			if (operation == 0) {
				wait(self->createTenant(cx, self));
			} else if (operation == 1) {
				wait(self->deleteTenant(cx, self));
			} else if (operation == 2) {
				wait(self->getTenant(cx, self));
			} else {
				wait(self->listTenants(cx, self));
			}
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }
	ACTOR Future<bool> _check(Database cx, TenantManagementWorkload* self) {
		state Transaction tr(cx);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
				Optional<Value> val = wait(tr.get(self->keyName));
				ASSERT(val.present() && val.get() == self->noTenantValue);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		state std::map<TenantName, TenantState>::iterator itr = self->createdTenants.begin();
		state std::vector<Future<Void>> checkTenants;
		state TenantName beginTenant = ""_sr;

		loop {
			Standalone<VectorRef<TenantNameRef>> tenants =
			    wait(ManagementAPI::listTenants(cx.getReference(), beginTenant, "\xff\xff"_sr, 1000));

			for (auto tenant : tenants) {
				ASSERT(!tenant.startsWith("\xff"_sr));
				ASSERT(tenant == itr->first);
				checkTenants.push_back(self->checkTenant(cx, self, tenant, itr->second));
				++itr;
			}

			if (tenants.size() < 1000) {
				break;
			} else {
				beginTenant = keyAfter(tenants[tenants.size() - 1]);
			}
		}

		ASSERT(itr == self->createdTenants.end());
		wait(waitForAll(checkTenants));

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantManagementWorkload> TenantManagementWorkload("TenantManagement");
