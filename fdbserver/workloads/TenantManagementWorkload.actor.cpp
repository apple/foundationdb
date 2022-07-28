/*
 * TenantManagementWorkload.actor.cpp
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
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/TenantSpecialKeys.actor.h"
#include "libb64/decode.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct TenantManagementWorkload : TestWorkload {
	struct TenantData {
		int64_t id;
		Optional<TenantGroupName> tenantGroup;
		bool empty;

		TenantData() : id(-1), empty(true) {}
		TenantData(int64_t id, Optional<TenantGroupName> tenantGroup, bool empty)
		  : id(id), tenantGroup(tenantGroup), empty(empty) {}
	};

	struct TenantGroupData {
		int64_t tenantCount = 0;
	};

	std::map<TenantName, TenantData> createdTenants;
	std::map<TenantGroupName, TenantGroupData> createdTenantGroups;
	int64_t maxId = -1;

	const Key keyName = "key"_sr;
	const Value noTenantValue = "no_tenant"_sr;
	const TenantName tenantNamePrefix = "tenant_management_workload_"_sr;
	TenantName localTenantNamePrefix;
	TenantName localTenantGroupNamePrefix;

	const Key specialKeysTenantMapPrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                           .begin.withSuffix(TenantRangeImpl<true>::submoduleRange.begin)
	                                           .withSuffix(TenantRangeImpl<true>::mapSubRange.begin);
	const Key specialKeysTenantConfigPrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                              .begin.withSuffix(TenantRangeImpl<true>::submoduleRange.begin)
	                                              .withSuffix(TenantRangeImpl<true>::configureSubRange.begin);
	const Key specialKeysTenantRenamePrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                              .begin.withSuffix(TenantRangeImpl<true>::submoduleRange.begin)
	                                              .withSuffix(TenantRangeImpl<true>::renameSubRange.begin);

	int maxTenants;
	int maxTenantGroups;
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
		maxTenantGroups = std::min<int>(2 * maxTenants, getOption(options, "maxTenantGroups"_sr, 20));
		testDuration = getOption(options, "testDuration"_sr, 60.0);

		localTenantNamePrefix = format("%stenant_%d_", tenantNamePrefix.toString().c_str(), clientId);
		localTenantGroupNamePrefix = format("%stenantgroup_%d_", tenantNamePrefix.toString().c_str(), clientId);
	}

	std::string description() const override { return "TenantManagement"; }

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	ACTOR Future<Void> _setup(Database cx, TenantManagementWorkload* self) {
		state Transaction tr(cx);
		if (self->clientId == 0) {
			loop {
				try {
					tr.setOption(FDBTransactionOptions::RAW_ACCESS);
					tr.set(self->keyName, self->noTenantValue);
					wait(tr.commit());
					break;
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

	Optional<TenantGroupName> chooseTenantGroup(bool allowSystemTenantGroup) {
		Optional<TenantGroupName> tenantGroup;
		if (deterministicRandom()->coinflip()) {
			tenantGroup = TenantGroupNameRef(format("%s%08d",
			                                        localTenantGroupNamePrefix.toString().c_str(),
			                                        deterministicRandom()->randomInt(0, maxTenantGroups)));
			if (allowSystemTenantGroup && deterministicRandom()->random01() < 0.02) {
				tenantGroup = tenantGroup.get().withPrefix("\xff"_sr);
			}
		}

		return tenantGroup;
	}

	// Creates tenant(s) using the specified operation type
	ACTOR static Future<Void> createImpl(Database cx,
	                                     Reference<ReadYourWritesTransaction> tr,
	                                     std::map<TenantName, TenantMapEntry> tenantsToCreate,
	                                     OperationType operationType,
	                                     TenantManagementWorkload* self) {
		if (operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (auto [tenant, entry] : tenantsToCreate) {
				tr->set(self->specialKeysTenantMapPrefix.withSuffix(tenant), ""_sr);
				if (entry.tenantGroup.present()) {
					tr->set(self->specialKeysTenantConfigPrefix.withSuffix(
					            Tuple().append(tenant).append("tenant_group"_sr).pack()),
					        entry.tenantGroup.get());
				}
			}
			wait(tr->commit());
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			ASSERT(tenantsToCreate.size() == 1);
			wait(success(TenantAPI::createTenant(
			    cx.getReference(), tenantsToCreate.begin()->first, tenantsToCreate.begin()->second)));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			int64_t _nextId = wait(TenantAPI::getNextTenantId(tr));
			int64_t nextId = _nextId;

			std::vector<Future<Void>> createFutures;
			for (auto [tenant, entry] : tenantsToCreate) {
				entry.setId(nextId++);
				createFutures.push_back(success(TenantAPI::createTenantTransaction(tr, tenant, entry)));
			}
			TenantMetadata::lastTenantId.set(tr, nextId - 1);
			wait(waitForAll(createFutures));
			wait(tr->commit());
		}

		return Void();
	}

	ACTOR static Future<Void> createTenant(Database cx, TenantManagementWorkload* self) {
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		int numTenants = 1;

		// For transaction-based operations, test creating multiple tenants in the same transaction
		if (operationType == OperationType::SPECIAL_KEYS || operationType == OperationType::MANAGEMENT_TRANSACTION) {
			numTenants = deterministicRandom()->randomInt(1, 5);
		}

		// Tracks whether any tenant exists in the database or not. This variable is updated if we have to retry
		// the creation.
		state bool alreadyExists = false;

		// True if any tenant name starts with \xff
		state bool hasSystemTenant = false;

		// True if any tenant group name starts with \xff
		state bool hasSystemTenantGroup = false;

		state int newTenants = 0;
		state std::map<TenantName, TenantMapEntry> tenantsToCreate;
		for (int i = 0; i < numTenants; ++i) {
			TenantName tenant = self->chooseTenantName(true);
			while (tenantsToCreate.count(tenant)) {
				tenant = self->chooseTenantName(true);
			}

			TenantMapEntry entry;
			entry.tenantGroup = self->chooseTenantGroup(true);

			if (self->createdTenants.count(tenant)) {
				alreadyExists = true;
			} else if (!tenantsToCreate.count(tenant)) {
				++newTenants;
			}

			tenantsToCreate[tenant] = entry;
			hasSystemTenant = hasSystemTenant || tenant.startsWith("\xff"_sr);
			hasSystemTenantGroup = hasSystemTenantGroup || entry.tenantGroup.orDefault(""_sr).startsWith("\xff"_sr);
		}

		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		state int64_t minTenantCount = std::numeric_limits<int64_t>::max();
		state int64_t finalTenantCount = 0;

		loop {
			try {
				if (operationType != OperationType::MANAGEMENT_DATABASE) {
					tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					wait(store(finalTenantCount, TenantMetadata::tenantCount.getD(tr, Snapshot::False, 0)));
					minTenantCount = std::min(finalTenantCount, minTenantCount);
				}

				wait(createImpl(cx, tr, tenantsToCreate, operationType, self));

				if (operationType == OperationType::MANAGEMENT_DATABASE) {
					ASSERT(!alreadyExists);
				} else {
					// Make sure that we had capacity to create the tenants. This cannot be validated for database
					// operations because we cannot determine the tenant count in the same transaction that the tenant
					// is created
					ASSERT(minTenantCount + newTenants <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
				}

				// It is not legal to create a tenant or tenant group starting with \xff
				ASSERT(!hasSystemTenant);
				ASSERT(!hasSystemTenantGroup);

				state std::map<TenantName, TenantMapEntry>::iterator tenantItr;
				for (tenantItr = tenantsToCreate.begin(); tenantItr != tenantsToCreate.end(); ++tenantItr) {
					// Ignore any tenants that already existed
					if (self->createdTenants.count(tenantItr->first)) {
						continue;
					}

					// Read the created tenant object and verify that its state is correct
					state Optional<TenantMapEntry> entry =
					    wait(TenantAPI::tryGetTenant(cx.getReference(), tenantItr->first));
					ASSERT(entry.present());
					ASSERT(entry.get().id > self->maxId);
					ASSERT(entry.get().tenantGroup == tenantItr->second.tenantGroup);
					ASSERT(entry.get().tenantState == TenantState::READY);

					// Update our local tenant state to include the newly created one
					self->maxId = entry.get().id;
					self->createdTenants[tenantItr->first] =
					    TenantData(entry.get().id, tenantItr->second.tenantGroup, true);

					// If this tenant has a tenant group, create or update the entry for it
					if (tenantItr->second.tenantGroup.present()) {
						self->createdTenantGroups[tenantItr->second.tenantGroup.get()].tenantCount++;
					}

					// Randomly decide to insert a key into the tenant
					state bool insertData = deterministicRandom()->random01() < 0.5;
					if (insertData) {
						state Transaction insertTr(cx, tenantItr->first);
						loop {
							try {
								// The value stored in the key will be the name of the tenant
								insertTr.set(self->keyName, tenantItr->first);
								wait(insertTr.commit());
								break;
							} catch (Error& e) {
								wait(insertTr.onError(e));
							}
						}

						self->createdTenants[tenantItr->first].empty = false;

						// Make sure that the key inserted correctly concatenates the tenant prefix with the
						// relative key
						state Transaction checkTr(cx);
						loop {
							try {
								checkTr.setOption(FDBTransactionOptions::RAW_ACCESS);
								Optional<Value> val = wait(checkTr.get(self->keyName.withPrefix(entry.get().prefix)));
								ASSERT(val.present());
								ASSERT(val.get() == tenantItr->first);
								break;
							} catch (Error& e) {
								wait(checkTr.onError(e));
							}
						}
					}

					// Perform some final tenant validation
					wait(checkTenantContents(cx, self, tenantItr->first, self->createdTenants[tenantItr->first]));
				}

				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_invalid_tenant_name) {
					ASSERT(hasSystemTenant);
					return Void();
				} else if (e.code() == error_code_invalid_tenant_group_name) {
					ASSERT(hasSystemTenantGroup);
					return Void();
				} else if (e.code() == error_code_cluster_no_capacity) {
					// Confirm that we overshot our capacity. This check cannot be done for database operations
					// because we cannot transactionally get the tenant count with the creation.
					if (operationType != OperationType::MANAGEMENT_DATABASE) {
						ASSERT(finalTenantCount + newTenants > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
					}
					return Void();
				}

				// Database-based operations should not need to be retried
				else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					if (e.code() == error_code_tenant_already_exists) {
						ASSERT(alreadyExists && operationType == OperationType::MANAGEMENT_DATABASE);
					} else {
						ASSERT(tenantsToCreate.size() == 1);
						TraceEvent(SevError, "CreateTenantFailure")
						    .error(e)
						    .detail("TenantName", tenantsToCreate.begin()->first);
					}
					return Void();
				}

				// Transaction-based operations should be retried
				else {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						for (auto [tenant, _] : tenantsToCreate) {
							TraceEvent(SevError, "CreateTenantFailure").error(e).detail("TenantName", tenant);
						}
						return Void();
					}
				}
			}
		}
	}

	ACTOR static Future<Void> deleteImpl(Database cx,
	                                     Reference<ReadYourWritesTransaction> tr,
	                                     TenantName beginTenant,
	                                     Optional<TenantName> endTenant,
	                                     std::vector<TenantName> tenants,
	                                     OperationType operationType,
	                                     TenantManagementWorkload* self) {
		state int tenantIndex;
		if (operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			Key key = self->specialKeysTenantMapPrefix.withSuffix(beginTenant);
			if (endTenant.present()) {
				tr->clear(KeyRangeRef(key, self->specialKeysTenantMapPrefix.withSuffix(endTenant.get())));
			} else {
				tr->clear(key);
			}
			wait(tr->commit());
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			ASSERT(tenants.size() == 1);
			for (tenantIndex = 0; tenantIndex != tenants.size(); ++tenantIndex) {
				wait(TenantAPI::deleteTenant(cx.getReference(), tenants[tenantIndex]));
			}
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			std::vector<Future<Void>> deleteFutures;
			for (tenantIndex = 0; tenantIndex != tenants.size(); ++tenantIndex) {
				deleteFutures.push_back(TenantAPI::deleteTenantTransaction(tr, tenants[tenantIndex]));
			}

			wait(waitForAll(deleteFutures));
			wait(tr->commit());
		}

		return Void();
	}

	ACTOR static Future<Void> deleteTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName beginTenant = self->chooseTenantName(true);
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		// For transaction-based deletion, we randomly allow the deletion of a range of tenants
		state Optional<TenantName> endTenant = operationType != OperationType::MANAGEMENT_DATABASE &&
		                                               !beginTenant.startsWith("\xff"_sr) &&
		                                               deterministicRandom()->random01() < 0.2
		                                           ? Optional<TenantName>(self->chooseTenantName(false))
		                                           : Optional<TenantName>();

		if (endTenant.present() && endTenant < beginTenant) {
			TenantName temp = beginTenant;
			beginTenant = endTenant.get();
			endTenant = temp;
		}

		auto itr = self->createdTenants.find(beginTenant);

		// True if the beginTenant should exist and be deletable. This is updated if a deletion fails and gets
		// retried.
		state bool alreadyExists = itr != self->createdTenants.end();

		// True if all of the tenants in the range are empty and can be deleted
		state bool isEmpty = true;

		// Collect a list of all tenants that we expect should be deleted by this operation
		state std::vector<TenantName> tenants;
		if (!endTenant.present()) {
			tenants.push_back(beginTenant);
		} else if (endTenant.present()) {
			for (auto itr = self->createdTenants.lower_bound(beginTenant);
			     itr != self->createdTenants.end() && itr->first < endTenant.get();
			     ++itr) {
				tenants.push_back(itr->first);
			}
		}

		// Check whether each tenant is empty.
		state int tenantIndex;
		try {
			if (alreadyExists || endTenant.present()) {
				for (tenantIndex = 0; tenantIndex < tenants.size(); ++tenantIndex) {
					// For most tenants, we will delete the contents and make them empty
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
					}
					// Otherwise, we will just report the current emptiness of the tenant
					else {
						auto itr = self->createdTenants.find(tenants[tenantIndex]);
						ASSERT(itr != self->createdTenants.end());
						isEmpty = isEmpty && itr->second.empty;
					}
				}
			}
		} catch (Error& e) {
			TraceEvent(SevError, "DeleteTenantFailure")
			    .error(e)
			    .detail("TenantName", beginTenant)
			    .detail("EndTenant", endTenant);
			return Void();
		}

		loop {
			try {
				// Attempt to delete the tenant(s)
				wait(deleteImpl(cx, tr, beginTenant, endTenant, tenants, operationType, self));

				// Transaction-based operations do not fail if the tenant isn't present. If we attempted to delete a
				// single tenant that didn't exist, we can just return.
				if (!alreadyExists && !endTenant.present() && operationType != OperationType::MANAGEMENT_DATABASE) {
					return Void();
				}

				ASSERT(alreadyExists || endTenant.present());

				// Deletion should not succeed if any tenant in the range wasn't empty
				ASSERT(isEmpty);

				// Update our local state to remove the deleted tenants
				for (auto tenant : tenants) {
					auto itr = self->createdTenants.find(tenant);
					ASSERT(itr != self->createdTenants.end());

					// If the tenant group has no tenants remaining, stop tracking it
					if (itr->second.tenantGroup.present()) {
						auto tenantGroupItr = self->createdTenantGroups.find(itr->second.tenantGroup.get());
						ASSERT(tenantGroupItr != self->createdTenantGroups.end());
						if (--tenantGroupItr->second.tenantCount == 0) {
							self->createdTenantGroups.erase(tenantGroupItr);
						}
					}

					self->createdTenants.erase(tenant);
				}
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_not_empty) {
					ASSERT(!isEmpty);
					return Void();
				}

				// Database-based operations do not need to be retried
				else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					if (e.code() == error_code_tenant_not_found) {
						ASSERT(!alreadyExists && !endTenant.present());
					} else {
						TraceEvent(SevError, "DeleteTenantFailure")
						    .error(e)
						    .detail("TenantName", beginTenant)
						    .detail("EndTenant", endTenant);
					}
					return Void();
				}

				// Transaction-based operations should be retried
				else {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						TraceEvent(SevError, "DeleteTenantFailure")
						    .error(e)
						    .detail("TenantName", beginTenant)
						    .detail("EndTenant", endTenant);
						return Void();
					}
				}
			}
		}
	}

	// Performs some validation on a tenant's contents
	ACTOR static Future<Void> checkTenantContents(Database cx,
	                                              TenantManagementWorkload* self,
	                                              TenantName tenant,
	                                              TenantData tenantData) {
		state Transaction tr(cx, tenant);
		loop {
			try {
				// We only every store a single key in each tenant. Therefore we expect a range read of the entire
				// tenant to return either 0 or 1 keys, depending on whether that key has been set.
				state RangeResult result = wait(tr.getRange(KeyRangeRef(""_sr, "\xff"_sr), 2));

				// An empty tenant should have no data
				if (tenantData.empty) {
					ASSERT(result.size() == 0);
				}
				// A non-empty tenant should have our single key. The value of that key should be the name of the
				// tenant.
				else {
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

	// Convert the JSON document returned by the special-key space when reading tenant metadata
	// into a TenantMapEntry
	static TenantMapEntry jsonToTenantMapEntry(ValueRef tenantJson) {
		json_spirit::mValue jsonObject;
		json_spirit::read_string(tenantJson.toString(), jsonObject);
		JSONDoc jsonDoc(jsonObject);

		int64_t id;

		std::string prefix;
		std::string base64Prefix;
		std::string printablePrefix;
		std::string tenantStateStr;
		std::string base64TenantGroup;
		std::string printableTenantGroup;

		jsonDoc.get("id", id);
		jsonDoc.get("prefix.base64", base64Prefix);
		jsonDoc.get("prefix.printable", printablePrefix);

		prefix = base64::decoder::from_string(base64Prefix);
		ASSERT(prefix == unprintable(printablePrefix));

		jsonDoc.get("tenant_state", tenantStateStr);

		Optional<TenantGroupName> tenantGroup;
		if (jsonDoc.tryGet("tenant_group.base64", base64TenantGroup)) {
			jsonDoc.get("tenant_group.printable", printableTenantGroup);
			std::string tenantGroupStr = base64::decoder::from_string(base64TenantGroup);
			ASSERT(tenantGroupStr == unprintable(printableTenantGroup));
			tenantGroup = TenantGroupNameRef(tenantGroupStr);
		}

		TenantMapEntry entry(id, TenantState::READY, tenantGroup);
		ASSERT(entry.prefix == prefix);
		return entry;
	}

	// Gets the metadata for a tenant using the specified operation type
	ACTOR static Future<TenantMapEntry> getImpl(Database cx,
	                                            Reference<ReadYourWritesTransaction> tr,
	                                            TenantName tenant,
	                                            OperationType operationType,
	                                            TenantManagementWorkload* self) {
		state TenantMapEntry entry;
		if (operationType == OperationType::SPECIAL_KEYS) {
			Key key = self->specialKeysTenantMapPrefix.withSuffix(tenant);
			Optional<Value> value = wait(tr->get(key));
			if (!value.present()) {
				throw tenant_not_found();
			}
			entry = TenantManagementWorkload::jsonToTenantMapEntry(value.get());
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			TenantMapEntry _entry = wait(TenantAPI::getTenant(cx.getReference(), tenant));
			entry = _entry;
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			TenantMapEntry _entry = wait(TenantAPI::getTenantTransaction(tr, tenant));
			entry = _entry;
		}

		return entry;
	}

	ACTOR static Future<Void> getTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName(true);
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		// True if the tenant should should exist and return a result
		auto itr = self->createdTenants.find(tenant);
		state bool alreadyExists = itr != self->createdTenants.end();
		state TenantData tenantData = alreadyExists ? itr->second : TenantData();

		loop {
			try {
				// Get the tenant metadata and check that it matches our local state
				state TenantMapEntry entry = wait(getImpl(cx, tr, tenant, operationType, self));
				ASSERT(alreadyExists);
				ASSERT(entry.id == tenantData.id);
				ASSERT(entry.tenantGroup == tenantData.tenantGroup);
				wait(self->checkTenantContents(cx, self, tenant, tenantData));
				return Void();
			} catch (Error& e) {
				state bool retry = false;
				state Error error = e;

				if (e.code() == error_code_tenant_not_found) {
					ASSERT(!alreadyExists);
					return Void();
				}

				// Transaction-based operations should retry
				else if (operationType != OperationType::MANAGEMENT_DATABASE) {
					try {
						wait(tr->onError(e));
						retry = true;
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

	// Gets a list of tenants using the specified operation type
	ACTOR static Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listImpl(
	    Database cx,
	    Reference<ReadYourWritesTransaction> tr,
	    TenantName beginTenant,
	    TenantName endTenant,
	    int limit,
	    OperationType operationType,
	    TenantManagementWorkload* self) {
		state std::vector<std::pair<TenantName, TenantMapEntry>> tenants;

		if (operationType == OperationType::SPECIAL_KEYS) {
			KeyRange range = KeyRangeRef(beginTenant, endTenant).withPrefix(self->specialKeysTenantMapPrefix);
			RangeResult results = wait(tr->getRange(range, limit));
			for (auto result : results) {
				tenants.push_back(std::make_pair(result.key.removePrefix(self->specialKeysTenantMapPrefix),
				                                 TenantManagementWorkload::jsonToTenantMapEntry(result.value)));
			}
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			std::vector<std::pair<TenantName, TenantMapEntry>> _tenants =
			    wait(TenantAPI::listTenants(cx.getReference(), beginTenant, endTenant, limit));
			tenants = _tenants;
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			std::vector<std::pair<TenantName, TenantMapEntry>> _tenants =
			    wait(TenantAPI::listTenantsTransaction(tr, beginTenant, endTenant, limit));
			tenants = _tenants;
		}

		return tenants;
	}

	ACTOR static Future<Void> listTenants(Database cx, TenantManagementWorkload* self) {
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
				// Attempt to read the chosen list of tenants
				state std::vector<std::pair<TenantName, TenantMapEntry>> tenants =
				    wait(listImpl(cx, tr, beginTenant, endTenant, limit, operationType, self));

				ASSERT(tenants.size() <= limit);

				// Compare the resulting tenant list to the list we expected to get
				auto localItr = self->createdTenants.lower_bound(beginTenant);
				auto tenantMapItr = tenants.begin();
				for (; tenantMapItr != tenants.end(); ++tenantMapItr, ++localItr) {
					ASSERT(localItr != self->createdTenants.end());
					ASSERT(localItr->first == tenantMapItr->first);
				}

				// Make sure the list terminated at the right spot
				ASSERT(tenants.size() == limit || localItr == self->createdTenants.end() ||
				       localItr->first >= endTenant);
				return Void();
			} catch (Error& e) {
				state bool retry = false;
				state Error error = e;

				// Transaction-based operations need to be retried
				if (operationType != OperationType::MANAGEMENT_DATABASE) {
					try {
						retry = true;
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

	// Helper function that checks tenant keyspace and updates internal Tenant Map after a rename operation
	ACTOR Future<Void> verifyTenantRename(Database cx,
	                                      TenantManagementWorkload* self,
	                                      TenantName oldTenantName,
	                                      TenantName newTenantName) {
		state Optional<TenantMapEntry> oldTenantEntry = wait(TenantAPI::tryGetTenant(cx.getReference(), oldTenantName));
		state Optional<TenantMapEntry> newTenantEntry = wait(TenantAPI::tryGetTenant(cx.getReference(), newTenantName));
		ASSERT(!oldTenantEntry.present());
		ASSERT(newTenantEntry.present());
		TenantData tData = self->createdTenants[oldTenantName];
		self->createdTenants[newTenantName] = tData;
		self->createdTenants.erase(oldTenantName);
		state Transaction insertTr(cx, newTenantName);
		if (!tData.empty) {
			loop {
				try {
					insertTr.set(self->keyName, newTenantName);
					wait(insertTr.commit());
					break;
				} catch (Error& e) {
					wait(insertTr.onError(e));
				}
			}
		}
		return Void();
	}

	ACTOR Future<Void> verifyTenantRenames(Database cx,
	                                       TenantManagementWorkload* self,
	                                       std::map<TenantName, TenantName> tenantRenames) {
		state std::map<TenantName, TenantName> tenantRenamesCopy = tenantRenames;
		state std::map<TenantName, TenantName>::iterator iter = tenantRenamesCopy.begin();
		for (; iter != tenantRenamesCopy.end(); ++iter) {
			wait(self->verifyTenantRename(cx, self, iter->first, iter->second));
			wait(self->checkTenantContents(cx, self, iter->second, self->createdTenants[iter->second]));
		}
		return Void();
	}

	ACTOR static Future<Void> renameImpl(Database cx,
	                                     Reference<ReadYourWritesTransaction> tr,
	                                     OperationType operationType,
	                                     std::map<TenantName, TenantName> tenantRenames,
	                                     bool tenantNotFound,
	                                     bool tenantExists,
	                                     bool tenantOverlap,
	                                     TenantManagementWorkload* self) {
		if (operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (auto& iter : tenantRenames) {
				tr->set(self->specialKeysTenantRenamePrefix.withSuffix(iter.first), iter.second);
			}
			wait(tr->commit());
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			ASSERT(tenantRenames.size() == 1);
			auto iter = tenantRenames.begin();
			wait(TenantAPI::renameTenant(cx.getReference(), iter->first, iter->second));
			ASSERT(!tenantNotFound && !tenantExists);
		} else { // operationType == OperationType::MANAGEMENT_TRANSACTION
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			std::vector<Future<Void>> renameFutures;
			for (auto& iter : tenantRenames) {
				renameFutures.push_back(success(TenantAPI::renameTenantTransaction(tr, iter.first, iter.second)));
			}
			wait(waitForAll(renameFutures));
			wait(tr->commit());
			ASSERT(!tenantNotFound && !tenantExists);
		}
		return Void();
	}

	ACTOR static Future<Void> renameTenant(Database cx, TenantManagementWorkload* self) {
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		state int numTenants = 1;
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		if (operationType == OperationType::SPECIAL_KEYS || operationType == OperationType::MANAGEMENT_TRANSACTION) {
			numTenants = deterministicRandom()->randomInt(1, 5);
		}

		state std::map<TenantName, TenantName> tenantRenames;
		state std::set<TenantName> allTenantNames;

		// Tenant Error flags
		state bool tenantExists = false;
		state bool tenantNotFound = false;
		state bool tenantOverlap = false;
		state bool unknownResult = false;

		for (int i = 0; i < numTenants; ++i) {
			TenantName oldTenant = self->chooseTenantName(false);
			TenantName newTenant = self->chooseTenantName(false);
			bool checkOverlap =
			    oldTenant == newTenant || allTenantNames.count(oldTenant) || allTenantNames.count(newTenant);
			// renameTenantTransaction does not handle rename collisions:
			// reject the rename here if it has overlap and we are doing a transaction operation
			// and then pick another combination
			if (checkOverlap && operationType == OperationType::MANAGEMENT_TRANSACTION) {
				--i;
				continue;
			}
			tenantOverlap = tenantOverlap || checkOverlap;
			tenantRenames[oldTenant] = newTenant;
			allTenantNames.insert(oldTenant);
			allTenantNames.insert(newTenant);
			if (!self->createdTenants.count(oldTenant)) {
				tenantNotFound = true;
			}
			if (self->createdTenants.count(newTenant)) {
				tenantExists = true;
			}
		}

		loop {
			try {
				wait(renameImpl(
				    cx, tr, operationType, tenantRenames, tenantNotFound, tenantExists, tenantOverlap, self));
				wait(self->verifyTenantRenames(cx, self, tenantRenames));
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_not_found) {
					TraceEvent("RenameTenantOldTenantNotFound")
					    .detail("TenantRenames", describe(tenantRenames))
					    .detail("CommitUnknownResult", unknownResult);
					if (unknownResult) {
						wait(self->verifyTenantRenames(cx, self, tenantRenames));
					} else {
						ASSERT(tenantNotFound);
					}
					return Void();
				} else if (e.code() == error_code_tenant_already_exists) {
					TraceEvent("RenameTenantNewTenantAlreadyExists")
					    .detail("TenantRenames", describe(tenantRenames))
					    .detail("CommitUnknownResult", unknownResult);
					if (unknownResult) {
						wait(self->verifyTenantRenames(cx, self, tenantRenames));
					} else {
						ASSERT(tenantExists);
					}
					return Void();
				} else if (e.code() == error_code_special_keys_api_failure) {
					TraceEvent("RenameTenantNameConflict").detail("TenantRenames", describe(tenantRenames));
					ASSERT(tenantOverlap);
					return Void();
				} else {
					try {
						// In the case of commit_unknown_result, assume we continue retrying
						// until it's successful. Next loop around may throw error because it's
						// already been moved, so account for that and update internal map as needed.
						if (e.code() == error_code_commit_unknown_result) {
							TraceEvent("RenameTenantCommitUnknownResult").error(e);
							ASSERT(operationType != OperationType::MANAGEMENT_DATABASE);
							unknownResult = true;
						}
						wait(tr->onError(e));
					} catch (Error& e) {
						TraceEvent(SevError, "RenameTenantFailure")
						    .error(e)
						    .detail("TenantRenames", describe(tenantRenames));
						return Void();
					}
				}
			}
		}
	}

	// Changes the configuration of a tenant
	ACTOR static Future<Void> configureImpl(Reference<ReadYourWritesTransaction> tr,
	                                        TenantName tenant,
	                                        std::map<Standalone<StringRef>, Optional<Value>> configParameters,
	                                        OperationType operationType,
	                                        bool specialKeysUseInvalidTuple,
	                                        TenantManagementWorkload* self) {
		if (operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (auto const& [config, value] : configParameters) {
				Tuple t;
				if (specialKeysUseInvalidTuple) {
					// Wrong number of items
					if (deterministicRandom()->coinflip()) {
						int numItems = deterministicRandom()->randomInt(0, 3);
						if (numItems > 0) {
							t.append(tenant);
						}
						if (numItems > 1) {
							t.append(config).append(""_sr);
						}
					}
					// Wrong data types
					else {
						if (deterministicRandom()->coinflip()) {
							t.append(0).append(config);
						} else {
							t.append(tenant).append(0);
						}
					}
				} else {
					t.append(tenant).append(config);
				}
				if (value.present()) {
					tr->set(self->specialKeysTenantConfigPrefix.withSuffix(t.pack()), value.get());
				} else {
					tr->clear(self->specialKeysTenantConfigPrefix.withSuffix(t.pack()));
				}
			}

			wait(tr->commit());
			ASSERT(!specialKeysUseInvalidTuple);
		} else {
			// We don't have a transaction or database variant of this function
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> configureTenant(Database cx, TenantManagementWorkload* self) {
		state OperationType operationType = OperationType::SPECIAL_KEYS;

		state TenantName tenant = self->chooseTenantName(true);
		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		state std::map<Standalone<StringRef>, Optional<Value>> configuration;
		state Optional<TenantGroupName> newTenantGroup;

		// If true, the options generated may include an unknown option
		state bool hasInvalidOption = deterministicRandom()->random01() < 0.1;

		// True if any tenant group name starts with \xff
		state bool hasSystemTenantGroup = false;

		state bool specialKeysUseInvalidTuple =
		    operationType == OperationType::SPECIAL_KEYS && deterministicRandom()->random01() < 0.1;

		// Generate a tenant group. Sometimes do this at the same time that we include an invalid option to ensure
		// that the configure function still fails
		if (!hasInvalidOption || deterministicRandom()->coinflip()) {
			newTenantGroup = self->chooseTenantGroup(true);
			hasSystemTenantGroup = hasSystemTenantGroup || newTenantGroup.orDefault(""_sr).startsWith("\xff"_sr);
			configuration["tenant_group"_sr] = newTenantGroup;
		}
		if (hasInvalidOption) {
			configuration["invalid_option"_sr] = ""_sr;
		}

		loop {
			try {
				wait(configureImpl(tr, tenant, configuration, operationType, specialKeysUseInvalidTuple, self));

				ASSERT(exists);
				ASSERT(!hasInvalidOption);
				ASSERT(!hasSystemTenantGroup);
				ASSERT(!specialKeysUseInvalidTuple);

				auto itr = self->createdTenants.find(tenant);
				if (itr->second.tenantGroup.present()) {
					auto tenantGroupItr = self->createdTenantGroups.find(itr->second.tenantGroup.get());
					ASSERT(tenantGroupItr != self->createdTenantGroups.end());
					if (--tenantGroupItr->second.tenantCount == 0) {
						self->createdTenantGroups.erase(tenantGroupItr);
					}
				}
				if (newTenantGroup.present()) {
					self->createdTenantGroups[newTenantGroup.get()].tenantCount++;
				}
				itr->second.tenantGroup = newTenantGroup;
				return Void();
			} catch (Error& e) {
				state Error error = e;
				if (e.code() == error_code_tenant_not_found) {
					ASSERT(!exists);
					return Void();
				} else if (e.code() == error_code_special_keys_api_failure) {
					ASSERT(hasInvalidOption || specialKeysUseInvalidTuple);
					return Void();
				} else if (e.code() == error_code_invalid_tenant_configuration) {
					ASSERT(hasInvalidOption);
					return Void();
				} else if (e.code() == error_code_invalid_tenant_group_name) {
					ASSERT(hasSystemTenantGroup);
					return Void();
				}

				try {
					wait(tr->onError(e));
				} catch (Error&) {
					TraceEvent(SevError, "ConfigureTenantFailure").error(error).detail("TenantName", tenant);
					return Void();
				}
			}
		}
	}

	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	ACTOR Future<Void> _start(Database cx, TenantManagementWorkload* self) {
		state double start = now();

		// Run a random sequence of tenant management operations for the duration of the test
		while (now() < start + self->testDuration) {
			state int operation = deterministicRandom()->randomInt(0, 6);
			if (operation == 0) {
				wait(createTenant(cx, self));
			} else if (operation == 1) {
				wait(deleteTenant(cx, self));
			} else if (operation == 2) {
				wait(getTenant(cx, self));
			} else if (operation == 3) {
				wait(listTenants(cx, self));
			} else if (operation == 4) {
				wait(renameTenant(cx, self));
			} else if (operation == 5) {
				wait(configureTenant(cx, self));
			}
		}

		return Void();
	}

	// Verify that the tenant count matches the actual number of tenants in the cluster and that we haven't created too
	// many
	ACTOR static Future<Void> checkTenantCount(Database cx) {
		state Reference<ReadYourWritesTransaction> tr = cx->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				state int64_t tenantCount = wait(TenantMetadata::tenantCount.getD(tr, Snapshot::False, 0));
				KeyBackedRangeResult<std::pair<TenantName, TenantMapEntry>> tenants =
				    wait(TenantMetadata::tenantMap.getRange(tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));

				ASSERT(tenants.results.size() == tenantCount && !tenants.more);
				ASSERT(tenantCount <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	// Verify that the set of tenants in the database matches our local state
	ACTOR static Future<Void> compareTenants(Database cx, TenantManagementWorkload* self) {
		state std::map<TenantName, TenantData>::iterator localItr = self->createdTenants.begin();
		state std::vector<Future<Void>> checkTenants;
		state TenantName beginTenant = ""_sr.withPrefix(self->localTenantNamePrefix);
		state TenantName endTenant = "\xff\xff"_sr.withPrefix(self->localTenantNamePrefix);

		loop {
			// Read the tenant list
			state std::vector<std::pair<TenantName, TenantMapEntry>> tenants =
			    wait(TenantAPI::listTenants(cx.getReference(), beginTenant, endTenant, 1000));

			auto dataItr = tenants.begin();

			TenantNameRef lastTenant;
			while (dataItr != tenants.end()) {
				ASSERT(localItr != self->createdTenants.end());
				ASSERT(dataItr->first == localItr->first);
				ASSERT(dataItr->second.tenantGroup == localItr->second.tenantGroup);

				checkTenants.push_back(checkTenantContents(cx, self, dataItr->first, localItr->second));
				lastTenant = dataItr->first;

				++localItr;
				++dataItr;
			}

			if (tenants.size() < 1000) {
				break;
			} else {
				beginTenant = keyAfter(lastTenant);
			}
		}

		ASSERT(localItr == self->createdTenants.end());
		wait(waitForAll(checkTenants));
		return Void();
	}

	// Check that the given tenant group has the expected number of tenants
	ACTOR template <class DB>
	static Future<Void> checkTenantGroupTenantCount(Reference<DB> db, TenantGroupName tenantGroup, int expectedCount) {
		TenantGroupName const& tenantGroupRef = tenantGroup;
		int const& expectedCountRef = expectedCount;

		KeyBackedSet<Tuple>::RangeResultType tenants =
		    wait(runTransaction(db, [tenantGroupRef, expectedCountRef](Reference<typename DB::TransactionT> tr) {
			    tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			    return TenantMetadata::tenantGroupTenantIndex.getRange(tr,
			                                                           Tuple::makeTuple(tenantGroupRef),
			                                                           Tuple::makeTuple(keyAfter(tenantGroupRef)),
			                                                           expectedCountRef + 1);
		    }));

		ASSERT(tenants.results.size() == expectedCount && !tenants.more);
		return Void();
	}

	// Verify that the set of tenants in the database matches our local state
	ACTOR static Future<Void> compareTenantGroups(Database cx, TenantManagementWorkload* self) {
		// Verify that the set of tena
		state std::map<TenantName, TenantGroupData>::iterator localItr = self->createdTenantGroups.begin();
		state TenantName beginTenantGroup = ""_sr.withPrefix(self->localTenantGroupNamePrefix);
		state TenantName endTenantGroup = "\xff\xff"_sr.withPrefix(self->localTenantGroupNamePrefix);
		state std::vector<Future<Void>> checkTenantGroups;

		loop {
			// Read the tenant group list
			state KeyBackedRangeResult<std::pair<TenantGroupName, TenantGroupEntry>> tenantGroups;
			TenantName const& beginTenantGroupRef = beginTenantGroup;
			TenantName const& endTenantGroupRef = endTenantGroup;
			KeyBackedRangeResult<std::pair<TenantGroupName, TenantGroupEntry>> _tenantGroups = wait(runTransaction(
			    cx.getReference(), [beginTenantGroupRef, endTenantGroupRef](Reference<ReadYourWritesTransaction> tr) {
				    tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				    return TenantMetadata::tenantGroupMap.getRange(tr, beginTenantGroupRef, endTenantGroupRef, 1000);
			    }));
			tenantGroups = _tenantGroups;

			auto dataItr = tenantGroups.results.begin();

			TenantGroupNameRef lastTenantGroup;
			while (dataItr != tenantGroups.results.end()) {
				ASSERT(localItr != self->createdTenantGroups.end());
				ASSERT(dataItr->first == localItr->first);
				lastTenantGroup = dataItr->first;

				checkTenantGroups.push_back(
				    checkTenantGroupTenantCount(cx.getReference(), dataItr->first, localItr->second.tenantCount));

				++localItr;
				++dataItr;
			}

			if (!tenantGroups.more) {
				break;
			} else {
				beginTenantGroup = keyAfter(lastTenantGroup);
			}
		}

		ASSERT(localItr == self->createdTenantGroups.end());
		return Void();
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }
	ACTOR static Future<bool> _check(Database cx, TenantManagementWorkload* self) {
		state Transaction tr(cx);

		// Check that the key we set outside of the tenant is present and has the correct value
		// This is the same key we set inside some of our tenants, so this checks that no tenant
		// writes accidentally happened in the raw key-space
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

		if (self->clientId == 0) {
			wait(checkTenantCount(cx));
		}

		wait(compareTenants(cx, self) && compareTenantGroups(cx, self));
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantManagementWorkload> TenantManagementWorkload("TenantManagement");
