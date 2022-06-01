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
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
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

	std::map<TenantName, TenantData> createdTenants;
	int64_t maxId = -1;
	Key tenantSubspace;

	const Key keyName = "key"_sr;
	const Key tenantSubspaceKey = "tenant_subspace"_sr;
	const Value noTenantValue = "no_tenant"_sr;
	const TenantName tenantNamePrefix = "tenant_management_workload_"_sr;
	TenantName localTenantNamePrefix;

	const Key specialKeysTenantMapPrefix =
	    TenantRangeImpl::mapSubRange.begin.withPrefix(TenantRangeImpl::submoduleRange.begin.withPrefix(
	        SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin));
	const Key specialKeysTenantConfigPrefix =
	    TenantRangeImpl::configureSubRange.begin.withPrefix(TenantRangeImpl::submoduleRange.begin.withPrefix(
	        SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin));

	int maxTenants;
	int maxTenantGroups;
	double testDuration;
	bool useMetacluster;

	Reference<IDatabase> mvDb;
	Database dataDb;

	// This test exercises multiple different ways to work with tenants
	enum class OperationType {
		// Use the special key-space APIs
		SPECIAL_KEYS,
		// Use the ManagementAPI functions that take a Database object and implement a retry loop
		MANAGEMENT_DATABASE,
		// Use the ManagementAPI functions that take a Transaction object
		MANAGEMENT_TRANSACTION,
		// Use the Metacluster API, if applicable. Note: not all APIs have a metacluster variant,
		// and if there isn't one this will choose one of the other options.
		METACLUSTER
	};

	OperationType randomOperationType() {
		double metaclusterProb = useMetacluster ? 0.9 : 0.1;

		if (deterministicRandom()->random01() < metaclusterProb) {
			return OperationType::METACLUSTER;
		} else {
			return (OperationType)deterministicRandom()->randomInt(0, 3);
		}
	}

	TenantManagementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		maxTenants = std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 1000));
		maxTenantGroups = std::min<int>(2 * maxTenants, getOption(options, "maxTenantGroups"_sr, 20));
		testDuration = getOption(options, "testDuration"_sr, 60.0);

		localTenantNamePrefix = format("%stenant_%d_", tenantNamePrefix.toString().c_str(), clientId);
		useMetacluster = false;
	}

	std::string description() const override { return "TenantManagement"; }

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	ACTOR Future<Void> _setup(Database cx, TenantManagementWorkload* self) {
		Reference<IDatabase> threadSafeHandle =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));
		TraceEvent("CreatedThreadSafeHandle");

		MultiVersionApi::api->selectApiVersion(cx->apiVersion);
		self->mvDb = MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeHandle);

		if (self->useMetacluster) {

			auto extraFile = makeReference<ClusterConnectionMemoryRecord>(*g_simulator.extraDB);
			self->dataDb = Database::createDatabase(extraFile, -1);

			if (self->clientId == 0) {
				wait(success(ManagementAPI::changeConfig(cx.getReference(), "tenant_mode=management", true)));

				DataClusterEntry entry;
				entry.capacity.numTenantGroups = 1e9;
				wait(MetaclusterAPI::registerCluster(self->mvDb, "cluster1"_sr, *g_simulator.extraDB, entry));
			}
		} else {
			self->dataDb = cx;
		}
		state Transaction tr(self->dataDb);
		if (self->clientId == 0) {
			// Configure the tenant subspace prefix that is applied to all tenants
			// This feature isn't supported in a metacluster, so we skip it if doing a metacluster test.
			if (self->useMetacluster) {
				self->tenantSubspace = ""_sr;
			} else {
			self->tenantSubspace = makeString(deterministicRandom()->randomInt(0, 10));
			loop {
				generateRandomData(mutateString(self->tenantSubspace), self->tenantSubspace.size());
				if (!self->tenantSubspace.startsWith(systemKeys.begin)) {
					break;
				}
			}
			}

			// Set a key outside of all tenants to make sure that our tenants aren't writing to the regular key-space
			// Also communicates the chosen tenant subspace to all other clients by storing it in a key
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
			// Read the tenant subspace chosen and saved by client 0
			loop {
				try {
					tr.setOption(FDBTransactionOptions::RAW_ACCESS);
					Optional<Value> val = wait(tr.get(self->tenantSubspaceKey));
					if (val.present()) {
						self->tenantSubspace = val.get();
						break;
					}

					wait(delay(1.0));
					tr.reset();
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

	Optional<TenantGroupName> chooseTenantGroup() {
		Optional<TenantGroupName> tenantGroup;
		if (deterministicRandom()->coinflip()) {
			tenantGroup =
			    TenantGroupNameRef(format("tenantgroup%08d", deterministicRandom()->randomInt(0, maxTenantGroups)));
		}

		return tenantGroup;
	}

	// Creates tenant(s) using the specified operation type
	ACTOR Future<Void> createImpl(Database cx,
	                              Reference<ReadYourWritesTransaction> tr,
	                              std::map<TenantName, TenantMapEntry> tenantsToCreate,
	                              OperationType operationType,
	                              TenantManagementWorkload* self) {
				if (operationType == OperationType::SPECIAL_KEYS) {
					tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (auto [tenant, entry] : tenantsToCreate) {
					tr->set(self->specialKeysTenantMapPrefix.withSuffix(tenant), ""_sr);
				if (entry.tenantGroup.present()) {
						tr->set(self->specialKeysTenantConfigPrefix.withSuffix("tenant_group/"_sr).withSuffix(tenant),
					        entry.tenantGroup.get());
				}
					}
					wait(tr->commit());
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			ASSERT(tenantsToCreate.size() == 1);
			TenantMapEntry result = wait(ManagementAPI::createTenant(
			    self->dataDb.getReference(), tenantsToCreate.begin()->first, tenantsToCreate.begin()->second));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			std::vector<Future<Void>> createFutures;
			for (auto [tenant, entry] : tenantsToCreate) {
				createFutures.push_back(success(ManagementAPI::createTenantTransaction(tr, tenant, entry)));
			}
			wait(waitForAll(createFutures));
					wait(tr->commit());
		} else {
			ASSERT(tenantsToCreate.size() == 1);
			wait(MetaclusterAPI::createTenant(
			    self->mvDb, tenantsToCreate.begin()->first, tenantsToCreate.begin()->second));
			TraceEvent("MetaclusterCreatedTenant");
		}
				}

					return Void();
				}

	ACTOR Future<Void> createTenant(Database cx, TenantManagementWorkload* self) {
		state OperationType operationType = self->randomOperationType();
		int numTenants = 1;

		// For transaction-based operations, test creating multiple tenants in the same transaction
		/*if (operationType == OperationType::SPECIAL_KEYS || operationType == OperationType::MANAGEMENT_TRANSACTION) {
		    numTenants = deterministicRandom()->randomInt(1, 5);
		}*/

		// Tracks whether any tenant exists in the database or not. This variable is updated if we have to retry
		// the creation.
		state bool alreadyExists = false;

		// True if any tenant name starts with \xff
		state bool hasSystemTenant = false;

		state std::map<TenantName, TenantMapEntry> tenantsToCreate;
		for (int i = 0; i < numTenants; ++i) {
			TenantName tenant = self->chooseTenantName(true);
			TenantMapEntry entry;
			entry.tenantGroup = self->chooseTenantGroup();
			tenantsToCreate[tenant] = entry;

			alreadyExists = alreadyExists || self->createdTenants.count(tenant);
			hasSystemTenant = tenant.startsWith("\xff"_sr);
		}

		// If any tenant existed at the start of this function, then we expect the creation to fail or be a no-op,
		// depending on the type of create operation being executed
		state bool existedAtStart = alreadyExists;

		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		loop {
			try {
				// First, attempt to create the tenants
				loop {
					try {
						Optional<Void> result =
						    wait(timeout(self->createImpl(cx, tr, tenantsToCreate, operationType, self), 30));

						if (result.present()) {
							break;
						}
					} catch (Error& e) {
						// If we retried the creation after our initial attempt succeeded, then we proceed with the rest
						// of the creation steps normally. Otherwise, the creation happened elsewhere and we failed
						// here, so we can rethrow the error.
						if (e.code() == error_code_tenant_already_exists && !existedAtStart) {
							ASSERT(operationType == OperationType::METACLUSTER ||
							       operationType == OperationType::MANAGEMENT_DATABASE);
							ASSERT(alreadyExists);
							break;
						} else {
							throw;
		}
					}

					// Check the state of the first created tenant
					Optional<TenantMapEntry> resultEntry =
					    wait(ManagementAPI::tryGetTenant(cx.getReference(), tenantsToCreate.begin()->first));

					if (resultEntry.present()) {
						if (resultEntry.get().tenantState == TenantState::READY) {
							// The tenant now exists, so we will retry and expect the creation to react accordingly
							alreadyExists = true;
						} else {
							// Only a metacluster tenant creation can end up in a partially created state
							// We should be able to retry and pick up where we left off
							ASSERT(operationType == OperationType::METACLUSTER);
							ASSERT(resultEntry.get().tenantState == TenantState::REGISTERING);
						}
					}
				}

				// Check that using the wrong creation type fails depending on whether we are using a metacluster
				ASSERT(self->useMetacluster == (operationType == OperationType::METACLUSTER));

				// Transaction-based creation modes will not fail if the tenant already existed, so we can just return
				// instead.
				if ((operationType == OperationType::MANAGEMENT_TRANSACTION ||
				     operationType == OperationType::SPECIAL_KEYS) &&
				    existedAtStart) {
					return Void();
				}

				ASSERT(!existedAtStart);

				// It is not legal to create a tenant starting with \xff
				ASSERT(!hasSystemTenant);

				state std::map<TenantName, TenantMapEntry>::iterator tenantItr;
				for (tenantItr = tenantsToCreate.begin(); tenantItr != tenantsToCreate.end(); ++tenantItr) {
					// Read the created tenant object and verify that its state is correct
					state Optional<TenantMapEntry> entry =
					    wait(ManagementAPI::tryGetTenant(cx.getReference(), tenantItr->first));

				ASSERT(entry.present());
				ASSERT(entry.get().id > self->maxId);
				ASSERT(entry.get().prefix.startsWith(self->tenantSubspace));
					ASSERT(entry.get().tenantGroup == tenantItr->second.tenantGroup);
					ASSERT(entry.get().tenantState == TenantState::READY);

					if (self->useMetacluster) {
						// In a metacluster, we should also see that the tenant was created on the data cluster
						Optional<TenantMapEntry> dataEntry =
						    wait(ManagementAPI::tryGetTenant(self->dataDb.getReference(), tenantItr->first));
						ASSERT(dataEntry.present());
						ASSERT(dataEntry.get().id == entry.get().id);
						ASSERT(dataEntry.get().prefix.size() == 8);
						ASSERT(dataEntry.get().tenantGroup == entry.get().tenantGroup);
						ASSERT(dataEntry.get().tenantState == TenantState::READY);
					}

					// Update our local tenant state to include the newly created one
				self->maxId = entry.get().id;
					self->createdTenants[tenantItr->first] =
					    TenantData(entry.get().id, tenantItr->second.tenantGroup, true);

					// Randomly decide to insert a key into the tenant
				state bool insertData = deterministicRandom()->random01() < 0.5;
				if (insertData) {
						state Transaction insertTr(self->dataDb, tenantItr->first);
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

						// Make sure that the key inserted correctly concatenates the tenant prefix with the relative
						// key
						state Transaction checkTr(self->dataDb);
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
					wait(self->checkTenantContents(self, tenantItr->first, self->createdTenants[tenantItr->first]));
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_invalid_tenant_name) {
					ASSERT(hasSystemTenant);
					return Void();
				} else if (e.code() == error_code_tenants_disabled) {
					ASSERT((operationType == OperationType::METACLUSTER) != self->useMetacluster);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER && !self->useMetacluster);
					return Void();
				}

				// Database-based operations should not need to be retried
				else if (operationType == OperationType::MANAGEMENT_DATABASE ||
				         operationType == OperationType::METACLUSTER) {
					if (e.code() == error_code_tenant_already_exists) {
						ASSERT(existedAtStart);
					} else {
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

	// Deletes the tenant or tenant range using the specified operation type
	ACTOR Future<Void> deleteImpl(Database cx,
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
			ASSERT(!endTenant.present() && tenants.size() == 1);
			wait(ManagementAPI::deleteTenant(self->dataDb.getReference(), beginTenant));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			std::vector<Future<Void>> deleteFutures;
			for (tenantIndex = 0; tenantIndex != tenants.size(); ++tenantIndex) {
				deleteFutures.push_back(ManagementAPI::deleteTenantTransaction(tr, tenants[tenantIndex]));
			}

			wait(waitForAll(deleteFutures));
			wait(tr->commit());
		} else {
			ASSERT(!endTenant.present() && tenants.size() == 1);
			wait(MetaclusterAPI::deleteTenant(self->mvDb, beginTenant));
		}

		return Void();
	}

	ACTOR Future<Void> deleteTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName beginTenant = self->chooseTenantName(true);
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		// For transaction-based deletion, we randomly allow the deletion of a range of tenants
		state Optional<TenantName> endTenant =
		    operationType != OperationType::MANAGEMENT_DATABASE && operationType != OperationType::METACLUSTER &&
		            !beginTenant.startsWith("\xff"_sr) && deterministicRandom()->random01() < 0.2
		                                           ? Optional<TenantName>(self->chooseTenantName(false))
		                                           : Optional<TenantName>();

		if (endTenant.present() && endTenant < beginTenant) {
			TenantName temp = beginTenant;
			beginTenant = endTenant.get();
			endTenant = temp;
		}

		auto itr = self->createdTenants.find(beginTenant);

		// True if the beginTenant should exist and be deletable. This is updated if a deletion fails and gets retried.
		state bool alreadyExists = itr != self->createdTenants.end();

		// True if the beginTenant existed at the start of this function
		state bool existedAtStart = alreadyExists;

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
						state Transaction clearTr(self->dataDb, tenants[tenantIndex]);
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
				loop {
					try {
						Optional<Void> result = wait(timeout(
						    self->deleteImpl(cx, tr, beginTenant, endTenant, tenants, operationType, self), 30));

						if (result.present()) {
							break;
						}
					} catch (Error& e) {
						// If we retried the deletion after our initial attempt succeeded, then we proceed with the rest
						// of the deletion steps normally. Otherwise, the deletion happened elsewhere and we failed
						// here, so we can rethrow the error.
						if (e.code() == error_code_tenant_not_found && existedAtStart) {
							ASSERT(operationType == OperationType::METACLUSTER ||
							       operationType == OperationType::MANAGEMENT_DATABASE);
							ASSERT(!alreadyExists);
							break;
					} else {
							throw;
					}
					}

					// Check the state of the first deleted tenant
					Optional<TenantMapEntry> resultEntry =
					    wait(ManagementAPI::tryGetTenant(cx.getReference(), *tenants.begin()));

					if (!resultEntry.present()) {
						alreadyExists = false;
					} else if (resultEntry.get().tenantState == TenantState::REMOVING) {
						ASSERT(operationType == OperationType::METACLUSTER);
				} else {
						ASSERT(resultEntry.get().tenantState == TenantState::READY);
					}
					}

				// The management transaction operation is a no-op if there are no tenants to delete in a range delete
				if (tenants.size() == 0 && operationType == OperationType::MANAGEMENT_TRANSACTION) {
					return Void();
				}

				// The special keys operation is a no-op if the begin and end tenant are equal (i.e. the range is empty)
				if (endTenant.present() && beginTenant == endTenant.get() &&
				    operationType == OperationType::SPECIAL_KEYS) {
					return Void();
				}

				// Check that using the wrong deletion type fails depending on whether we are using a metacluster
				ASSERT(self->useMetacluster == (operationType == OperationType::METACLUSTER));

				// Transaction-based operations do not fail if the tenant isn't present. If we attempted to delete a
				// single tenant that didn't exist, we can just return.
				if (!existedAtStart && !endTenant.present() &&
				    (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				     operationType == OperationType::SPECIAL_KEYS)) {
					return Void();
				}

				ASSERT(existedAtStart || endTenant.present());

				// Deletion should not succeed if any tenant in the range wasn't empty
				ASSERT(isEmpty);

				// Update our local state to remove the deleted tenants
				for (auto tenant : tenants) {
					self->createdTenants.erase(tenant);
				}
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_not_empty) {
					ASSERT(!isEmpty);
					return Void();
				} else if (e.code() == error_code_tenants_disabled) {
					ASSERT((operationType == OperationType::METACLUSTER) != self->useMetacluster);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER && !self->useMetacluster);
					return Void();
				}

				// Database-based operations do not need to be retried
				else if (operationType == OperationType::MANAGEMENT_DATABASE ||
				         operationType == OperationType::METACLUSTER) {
					if (e.code() == error_code_tenant_not_found) {
						ASSERT(!existedAtStart && !endTenant.present());
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
	ACTOR Future<Void> checkTenantContents(TenantManagementWorkload* self, TenantName tenant, TenantData tenantData) {
		state Transaction tr(self->dataDb, tenant);
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
		std::string tenantStateStr;
		std::string assignedClusterStr;
		std::string tenantGroupStr;
		jsonDoc.get("id", id);
		jsonDoc.get("prefix", prefix);
		jsonDoc.get("tenant_state", tenantStateStr);

		Optional<ClusterName> assignedCluster;
		if (jsonDoc.tryGet("assigned_cluster", assignedClusterStr)) {
			assignedCluster = ClusterNameRef(assignedClusterStr);
		}

		Optional<TenantGroupName> tenantGroup;
		if (jsonDoc.tryGet("tenant_group", tenantGroupStr)) {
			tenantGroup = TenantGroupNameRef(tenantGroupStr);
		}

		Key prefixKey = KeyRef(prefix);
		TenantMapEntry entry(id,
		                     prefixKey.substr(0, prefixKey.size() - 8),
		                     tenantGroup,
		                     TenantMapEntry::stringToTenantState(tenantStateStr));

		ASSERT(entry.prefix == prefixKey);
		return entry;
	}

	// Gets the metadata for a tenant using the specified operation type
	ACTOR Future<TenantMapEntry> getImpl(Database cx,
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
			TenantMapEntry _entry = wait(ManagementAPI::getTenant(self->dataDb.getReference(), tenant));
					entry = _entry;
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
					tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					TenantMapEntry _entry = wait(ManagementAPI::getTenantTransaction(tr, tenant));
					entry = _entry;
		} else {
			TenantMapEntry _entry = wait(ManagementAPI::getTenant(self->mvDb, tenant));
			entry = _entry;
		}

		return entry;
				}

	ACTOR Future<Void> getTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName(true);
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		// True if the tenant should should exist and return a result
		auto itr = self->createdTenants.find(tenant);
		state bool alreadyExists = itr != self->createdTenants.end();

		state TenantData tenantData;
		if (alreadyExists) {
			tenantData = itr->second;
		}

		loop {
			try {
				// Get the tenant metadata and check that it matches our local state
				state TenantMapEntry entry = wait(self->getImpl(cx, tr, tenant, operationType, self));
				ASSERT(alreadyExists);
				ASSERT(entry.id == tenantData.id);
				ASSERT(entry.tenantGroup == tenantData.tenantGroup);
				wait(self->checkTenantContents(self, tenant, tenantData));
				return Void();
			} catch (Error& e) {
				state bool retry = true;
				state Error error = e;

				if (e.code() == error_code_tenant_not_found) {
					ASSERT(!alreadyExists);
					return Void();
				}

				// Transaction-based operations should retry
				else if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				         operationType == OperationType::SPECIAL_KEYS) {
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

	// Gets a list of tenants using the specified operation type
	ACTOR Future<std::map<TenantName, TenantMapEntry>> listImpl(Database cx,
	                                                            Reference<ReadYourWritesTransaction> tr,
	                                                            TenantName beginTenant,
	                                                            TenantName endTenant,
	                                                            int limit,
	                                                            OperationType operationType,
	                                                            TenantManagementWorkload* self) {
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
			    wait(ManagementAPI::listTenants(self->dataDb.getReference(), beginTenant, endTenant, limit));
					tenants = _tenants;
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
					tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					std::map<TenantName, TenantMapEntry> _tenants =
					    wait(ManagementAPI::listTenantsTransaction(tr, beginTenant, endTenant, limit));
					tenants = _tenants;
		} else {
			std::map<TenantName, TenantMapEntry> _tenants =
			    wait(ManagementAPI::listTenants(self->mvDb, beginTenant, endTenant, limit));
			tenants = _tenants;
				}

		return tenants;
	}

	ACTOR Future<Void> listTenants(Database cx, TenantManagementWorkload* self) {
		state TenantName beginTenant = self->chooseTenantName(false);
		state TenantName endTenant = self->chooseTenantName(false);
		state int limit = std::min(CLIENT_KNOBS->TOO_MANY, deterministicRandom()->randomInt(1, self->maxTenants * 2));
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		if (beginTenant > endTenant) {
			std::swap(beginTenant, endTenant);
		}

		loop {
			try {
				// Attempt to read the chosen list of tenants
				state std::map<TenantName, TenantMapEntry> tenants =
				    wait(self->listImpl(cx, tr, beginTenant, endTenant, limit, operationType, self));

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
				state bool retry = true;
				state Error error = e;

				// Transaction-based operations need to be retried
				if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				    operationType == OperationType::SPECIAL_KEYS) {
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
		state Transaction tr(self->dataDb);

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

		// Verify that the set of tenants in the database matches our local state
		state std::map<TenantName, TenantData>::iterator localItr = self->createdTenants.begin();
		state std::vector<Future<Void>> checkTenants;
		state TenantName beginTenant = ""_sr.withPrefix(self->localTenantNamePrefix);
		state TenantName endTenant = "\xff\xff"_sr.withPrefix(self->localTenantNamePrefix);

		loop {
			// Read the tenant map from the primary cluster (either management cluster in a metacluster, or just the
			// cluster otherwise).
			state std::map<TenantName, TenantMapEntry> tenants =
			    wait(ManagementAPI::listTenants(cx.getReference(), beginTenant, endTenant, 1000));

			// Read the tenant map from the data cluster. If this is not a metacluster it will read from the same
			// database as above, making it superfluous but still correct.
			std::map<TenantName, TenantMapEntry> dataClusterTenants =
			    wait(ManagementAPI::listTenants(self->dataDb.getReference(), beginTenant, endTenant, 1000));

			auto managementItr = tenants.begin();
			auto dataItr = dataClusterTenants.begin();

			TenantNameRef lastTenant;
			while (managementItr != tenants.end()) {
				ASSERT(localItr != self->createdTenants.end());
				ASSERT(dataItr != dataClusterTenants.end());
				ASSERT(managementItr->first == localItr->first);
				ASSERT(managementItr->first == dataItr->first);

				checkTenants.push_back(self->checkTenantContents(self, managementItr->first, localItr->second));
				lastTenant = managementItr->first;

				++localItr;
				++managementItr;
				++dataItr;
			}

			ASSERT(dataItr == dataClusterTenants.end());

			if (tenants.size() < 1000) {
				break;
			} else {
				beginTenant = keyAfter(lastTenant);
			}
		}

		ASSERT(localItr == self->createdTenants.end());
		wait(waitForAll(checkTenants));

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantManagementWorkload> TenantManagementWorkload("TenantManagement");
