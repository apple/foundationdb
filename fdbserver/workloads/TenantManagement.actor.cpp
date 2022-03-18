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
	const TenantName tenantNamePrefix = "tenant_management_workload_"_sr;
	TenantName localTenantNamePrefix;

	const Key specialKeysTenantMapPrefix = TenantMapRangeImpl::submoduleRange.begin.withPrefix(
	    SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin);

	int maxTenants;
	double testDuration;

	enum class OperationType { SPECIAL_KEYS, MANAGEMENT_DATABASE, MANAGEMENT_TRANSACTION };

	static OperationType randomOperationType() {
		int randomNum = deterministicRandom()->randomInt(0, 3);
		if (randomNum == 0) {
			return OperationType::SPECIAL_KEYS;
		} else if (randomNum == 1) {
			return OperationType::MANAGEMENT_DATABASE;
		} else {
			return OperationType::MANAGEMENT_TRANSACTION;
		}
	}

	TenantManagementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		maxTenants = std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 1000));
		testDuration = getOption(options, "testDuration"_sr, 60.0);

		localTenantNamePrefix = format("%stenant_%d_", tenantNamePrefix.toString().c_str(), clientId);
	}

	std::string description() const override { return "TenantManagement"; }

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	ACTOR Future<Void> _setup(Database cx, TenantManagementWorkload* self) {
		state Transaction tr(cx);
		if (self->clientId == 0) {
			self->tenantSubspace = makeString(deterministicRandom()->randomInt(0, 10));
			loop {
				generateRandomData(mutateString(self->tenantSubspace), self->tenantSubspace.size());
				if (!self->tenantSubspace.startsWith(systemKeys.begin)) {
					break;
				}
			}
			loop {
				try {
					tr.setOption(FDBTransactionOptions::RAW_ACCESS);
					tr.set(self->keyName, self->noTenantValue);
					tr.set(self->tenantSubspaceKey, self->tenantSubspace);
					tr.set(tenantDataPrefixKey, self->tenantSubspace);
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

	TenantName chooseTenantName(bool allowSystemTenant) {
		TenantName tenant(format(
		    "%s%08d", localTenantNamePrefix.toString().c_str(), deterministicRandom()->randomInt(0, maxTenants)));
		if (allowSystemTenant && deterministicRandom()->random01() < 0.02) {
			tenant = tenant.withPrefix("\xff"_sr);
		}

		return tenant;
	}

	ACTOR Future<Void> createTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName(true);
		state bool alreadyExists = self->createdTenants.count(tenant);
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		loop {
			try {
				if (operationType == OperationType::SPECIAL_KEYS) {
					tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					Key key = self->specialKeysTenantMapPrefix.withSuffix(tenant);
					tr->set(key, ""_sr);
					wait(tr->commit());
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					wait(ManagementAPI::createTenant(cx.getReference(), tenant));
				} else {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					Optional<TenantMapEntry> _ = wait(ManagementAPI::createTenantTransaction(tr, tenant));
					wait(tr->commit());
				}

				if (operationType != OperationType::MANAGEMENT_DATABASE && alreadyExists) {
					return Void();
				}

				ASSERT(!alreadyExists);
				ASSERT(!tenant.startsWith("\xff"_sr));

				state Optional<TenantMapEntry> entry = wait(ManagementAPI::tryGetTenant(cx.getReference(), tenant));
				ASSERT(entry.present());
				ASSERT(entry.get().id > self->maxId);
				ASSERT(entry.get().prefix.startsWith(self->tenantSubspace));

				self->maxId = entry.get().id;
				self->createdTenants[tenant] = TenantState(entry.get().id, true);

				state bool insertData = deterministicRandom()->random01() < 0.5;
				if (insertData) {
					state Transaction insertTr(cx, tenant);
					loop {
						try {
							insertTr.set(self->keyName, tenant);
							wait(insertTr.commit());
							break;
						} catch (Error& e) {
							wait(insertTr.onError(e));
						}
					}

					self->createdTenants[tenant].empty = false;

					state Transaction checkTr(cx);
					loop {
						try {
							checkTr.setOption(FDBTransactionOptions::RAW_ACCESS);
							Optional<Value> val = wait(checkTr.get(self->keyName.withPrefix(entry.get().prefix)));
							ASSERT(val.present());
							ASSERT(val.get() == tenant);
							break;
						} catch (Error& e) {
							wait(checkTr.onError(e));
						}
					}
				}

				wait(self->checkTenant(cx, self, tenant, self->createdTenants[tenant]));
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_invalid_tenant_name) {
					ASSERT(tenant.startsWith("\xff"_sr));
					return Void();
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					if (e.code() == error_code_tenant_already_exists) {
						ASSERT(alreadyExists && operationType == OperationType::MANAGEMENT_DATABASE);
					} else {
						TraceEvent(SevError, "CreateTenantFailure").error(e).detail("TenantName", tenant);
					}
					return Void();
				} else {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						TraceEvent(SevError, "CreateTenantFailure").error(e).detail("TenantName", tenant);
						return Void();
					}
				}
			}
		}
	}

	ACTOR Future<Void> deleteTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName(true);
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		state Optional<TenantName> endTenant = operationType != OperationType::MANAGEMENT_DATABASE &&
		                                               !tenant.startsWith("\xff"_sr) &&
		                                               deterministicRandom()->random01() < 0.2
		                                           ? Optional<TenantName>(self->chooseTenantName(false))
		                                           : Optional<TenantName>();

		if (endTenant.present() && endTenant < tenant) {
			TenantName temp = tenant;
			tenant = endTenant.get();
			endTenant = temp;
		}

		auto itr = self->createdTenants.find(tenant);
		state bool alreadyExists = itr != self->createdTenants.end();
		state bool isEmpty = true;

		state std::vector<TenantName> tenants;
		if (!endTenant.present()) {
			tenants.push_back(tenant);
		} else if (endTenant.present()) {
			for (auto itr = self->createdTenants.lower_bound(tenant);
			     itr != self->createdTenants.end() && itr->first < endTenant.get();
			     ++itr) {
				tenants.push_back(itr->first);
			}
		}

		try {
			if (alreadyExists || endTenant.present()) {
				state int tenantIndex = 0;
				for (; tenantIndex < tenants.size(); ++tenantIndex) {
					if (deterministicRandom()->random01() < 0.9) {
						state Transaction clearTr(cx, tenants[tenantIndex]);
						loop {
							try {
								clearTr.clear(self->keyName);
								wait(clearTr.commit());
								auto itr = self->createdTenants.find(tenants[tenantIndex]);
								ASSERT(itr != self->createdTenants.end());
								itr->second.empty = true;
								break;
							} catch (Error& e) {
								wait(clearTr.onError(e));
							}
						}
					} else {
						auto itr = self->createdTenants.find(tenants[tenantIndex]);
						ASSERT(itr != self->createdTenants.end());
						isEmpty = isEmpty && itr->second.empty;
					}
				}
			}
		} catch (Error& e) {
			TraceEvent(SevError, "DeleteTenantFailure")
			    .error(e)
			    .detail("TenantName", tenant)
			    .detail("EndTenant", endTenant);
			return Void();
		}

		loop {
			try {
				if (operationType == OperationType::SPECIAL_KEYS) {
					tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					Key key = self->specialKeysTenantMapPrefix.withSuffix(tenant);
					if (endTenant.present()) {
						tr->clear(KeyRangeRef(key, self->specialKeysTenantMapPrefix.withSuffix(endTenant.get())));
					} else {
						tr->clear(key);
					}
					wait(tr->commit());
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					ASSERT(tenants.size() == 1);
					for (auto tenant : tenants) {
						wait(ManagementAPI::deleteTenant(cx.getReference(), tenant));
					}
				} else {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					for (auto tenant : tenants) {
						wait(ManagementAPI::deleteTenantTransaction(tr, tenant));
					}

					wait(tr->commit());
				}

				if (!alreadyExists && !endTenant.present() && operationType != OperationType::MANAGEMENT_DATABASE) {
					return Void();
				}

				ASSERT(alreadyExists || endTenant.present());
				ASSERT(isEmpty);
				for (auto tenant : tenants) {
					self->createdTenants.erase(tenant);
				}
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_not_empty) {
					ASSERT(!isEmpty);
					return Void();
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					if (e.code() == error_code_tenant_not_found) {
						ASSERT(!alreadyExists && !endTenant.present());
					} else {
						TraceEvent(SevError, "DeleteTenantFailure")
						    .error(e)
						    .detail("TenantName", tenant)
						    .detail("EndTenant", endTenant);
					}
					return Void();
				} else {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						TraceEvent(SevError, "DeleteTenantFailure")
						    .error(e)
						    .detail("TenantName", tenant)
						    .detail("EndTenant", endTenant);
						return Void();
					}
				}
			}
		}
	}

	ACTOR Future<Void> checkTenant(Database cx,
	                               TenantManagementWorkload* self,
	                               TenantName tenant,
	                               TenantState tenantState) {
		state Transaction tr(cx, tenant);
		loop {
			try {
				state RangeResult result = wait(tr.getRange(KeyRangeRef(""_sr, "\xff"_sr), 2));
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

	static TenantMapEntry jsonToTenantMapEntry(ValueRef tenantJson) {
		json_spirit::mValue jsonObject;
		json_spirit::read_string(tenantJson.toString(), jsonObject);
		JSONDoc jsonDoc(jsonObject);

		int64_t id;
		std::string prefix;
		jsonDoc.get("id", id);
		jsonDoc.get("prefix", prefix);

		Key prefixKey = KeyRef(prefix);
		TenantMapEntry entry(id, prefixKey.substr(0, prefixKey.size() - 8));

		ASSERT(entry.prefix == prefixKey);
		return entry;
	}

	ACTOR Future<Void> getTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName(true);
		auto itr = self->createdTenants.find(tenant);
		state bool alreadyExists = itr != self->createdTenants.end();
		state TenantState tenantState = itr->second;
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		loop {
			try {
				state TenantMapEntry entry;
				if (operationType == OperationType::SPECIAL_KEYS) {
					Key key = self->specialKeysTenantMapPrefix.withSuffix(tenant);
					Optional<Value> value = wait(tr->get(key));
					if (!value.present()) {
						throw tenant_not_found();
					}
					entry = TenantManagementWorkload::jsonToTenantMapEntry(value.get());
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					TenantMapEntry _entry = wait(ManagementAPI::getTenant(cx.getReference(), tenant));
					entry = _entry;
				} else {
					tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					TenantMapEntry _entry = wait(ManagementAPI::getTenantTransaction(tr, tenant));
					entry = _entry;
				}
				ASSERT(alreadyExists);
				ASSERT(entry.id == tenantState.id);
				wait(self->checkTenant(cx, self, tenant, tenantState));
				return Void();
			} catch (Error& e) {
				state bool retry = true;
				state Error error = e;

				if (e.code() == error_code_tenant_not_found) {
					ASSERT(!alreadyExists);
					return Void();
				} else if (operationType != OperationType::MANAGEMENT_DATABASE) {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						error = e;
						retry = false;
					}
				}

				if (!retry) {
					TraceEvent(SevError, "GetTenantFailure").error(error).detail("TenantName", tenant);
					return Void();
				}
			}
		}
	}

	ACTOR Future<Void> listTenants(Database cx, TenantManagementWorkload* self) {
		state TenantName beginTenant = self->chooseTenantName(false);
		state TenantName endTenant = self->chooseTenantName(false);
		state int limit = std::min(CLIENT_KNOBS->TOO_MANY, deterministicRandom()->randomInt(1, self->maxTenants * 2));
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		if (beginTenant > endTenant) {
			std::swap(beginTenant, endTenant);
		}

		loop {
			try {
				state std::map<TenantName, TenantMapEntry> tenants;
				if (operationType == OperationType::SPECIAL_KEYS) {
					KeyRange range = KeyRangeRef(beginTenant, endTenant).withPrefix(self->specialKeysTenantMapPrefix);
					RangeResult results = wait(tr->getRange(range, limit));
					for (auto result : results) {
						tenants[result.key.removePrefix(self->specialKeysTenantMapPrefix)] =
						    TenantManagementWorkload::jsonToTenantMapEntry(result.value);
					}
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					std::map<TenantName, TenantMapEntry> _tenants =
					    wait(ManagementAPI::listTenants(cx.getReference(), beginTenant, endTenant, limit));
					tenants = _tenants;
				} else {
					tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					std::map<TenantName, TenantMapEntry> _tenants =
					    wait(ManagementAPI::listTenantsTransaction(tr, beginTenant, endTenant, limit));
					tenants = _tenants;
				}

				ASSERT(tenants.size() <= limit);

				auto localItr = self->createdTenants.lower_bound(beginTenant);
				auto tenantMapItr = tenants.begin();
				for (; tenantMapItr != tenants.end(); ++tenantMapItr, ++localItr) {
					ASSERT(localItr != self->createdTenants.end());
					ASSERT(localItr->first == tenantMapItr->first);
				}

				if (!(tenants.size() == limit || localItr == self->createdTenants.end())) {
					for (auto tenant : self->createdTenants) {
						TraceEvent("ExistingTenant").detail("Tenant", tenant.first);
					}
				}
				ASSERT(tenants.size() == limit || localItr == self->createdTenants.end() ||
				       localItr->first >= endTenant);
				return Void();
			} catch (Error& e) {
				state bool retry = true;
				state Error error = e;
				if (operationType != OperationType::MANAGEMENT_DATABASE) {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						error = e;
						retry = false;
					}
				}

				if (!retry) {
					TraceEvent(SevError, "ListTenantFailure")
					    .error(error)
					    .detail("BeginTenant", beginTenant)
					    .detail("EndTenant", endTenant);

					return Void();
				}
			}
		}
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
		state TenantName beginTenant = ""_sr.withPrefix(self->localTenantNamePrefix);
		state TenantName endTenant = "\xff\xff"_sr.withPrefix(self->localTenantNamePrefix);

		loop {
			std::map<TenantName, TenantMapEntry> tenants =
			    wait(ManagementAPI::listTenants(cx.getReference(), beginTenant, endTenant, 1000));

			TenantNameRef lastTenant;
			for (auto tenant : tenants) {
				ASSERT(itr != self->createdTenants.end());
				ASSERT(tenant.first == itr->first);
				checkTenants.push_back(self->checkTenant(cx, self, tenant.first, itr->second));
				lastTenant = tenant.first;
				++itr;
			}

			if (tenants.size() < 1000) {
				break;
			} else {
				beginTenant = keyAfter(lastTenant);
			}
		}

		ASSERT(itr == self->createdTenants.end());
		wait(waitForAll(checkTenants));

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantManagementWorkload> TenantManagementWorkload("TenantManagement");
