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
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/TenantSpecialKeys.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbclient/libb64/decode.h"
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
	const Key testParametersKey = "test_parameters"_sr;
	const Value noTenantValue = "no_tenant"_sr;
	const TenantName tenantNamePrefix = "tenant_management_workload_"_sr;
	TenantName localTenantNamePrefix;

	const Key specialKeysTenantMapPrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                           .begin.withSuffix(TenantRangeImpl<true>::submoduleRange.begin)
	                                           .withSuffix(TenantRangeImpl<true>::mapSubRange.begin);
	const Key specialKeysTenantConfigPrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                              .begin.withSuffix(TenantRangeImpl<true>::submoduleRange.begin)
	                                              .withSuffix(TenantRangeImpl<true>::configureSubRange.begin);

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

		bool defaultUseMetacluster = false;
		if (clientId == 0 && g_network->isSimulated() && !g_simulator.extraDatabases.empty()) {
			defaultUseMetacluster = deterministicRandom()->coinflip();
		}

		useMetacluster = getOption(options, "useMetacluster"_sr, defaultUseMetacluster);
	}

	std::string description() const override { return "TenantManagement"; }

	struct TestParameters {
		constexpr static FileIdentifier file_identifier = 1527576;

		Key tenantSubspace;
		bool useMetacluster = false;

		TestParameters() {}
		TestParameters(Key tenantSubspace, bool useMetacluster)
		  : tenantSubspace(tenantSubspace), useMetacluster(useMetacluster) {}

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, tenantSubspace, useMetacluster);
		}

		Value encode() const { return ObjectWriter::toValue(*this, Unversioned()); }

		static TestParameters decode(ValueRef const& value) {
			TestParameters params;
			ObjectReader reader(value.begin(), Unversioned());
			reader.deserialize(params);
			return params;
		}
	};

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	ACTOR Future<Void> _setup(Database cx, TenantManagementWorkload* self) {
		Reference<IDatabase> threadSafeHandle =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));

		MultiVersionApi::api->selectApiVersion(cx->apiVersion);
		self->mvDb = MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeHandle);

		if (self->useMetacluster && self->clientId == 0) {
			wait(success(MetaclusterAPI::createMetacluster(cx.getReference(), "management_cluster"_sr)));

			DataClusterEntry entry;
			entry.capacity.numTenantGroups = 1e9;
			wait(MetaclusterAPI::registerCluster(self->mvDb, "cluster1"_sr, g_simulator.extraDatabases[0], entry));
		}

		state Transaction tr(cx);
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

			// Communicates test parameters to all other clients by storing it in a key
			loop {
				try {
					tr.setOption(FDBTransactionOptions::RAW_ACCESS);
					tr.set(self->testParametersKey,
					       TestParameters(self->tenantSubspace, self->useMetacluster).encode());
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
					Optional<Value> val = wait(tr.get(self->testParametersKey));
					if (val.present()) {
						TestParameters params = TestParameters::decode(val.get());
						self->tenantSubspace = params.tenantSubspace;
						self->useMetacluster = params.useMetacluster;
						break;
					}

					wait(delay(1.0));
					tr.reset();
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

		if (self->useMetacluster) {
			ASSERT(g_simulator.extraDatabases.size() == 1);
			auto extraFile = makeReference<ClusterConnectionMemoryRecord>(g_simulator.extraDatabases[0]);
			self->dataDb = Database::createDatabase(extraFile, -1);
		} else {
			self->dataDb = cx;
		}

		if (self->clientId == 0) {
			// Set a key outside of all tenants to make sure that our tenants aren't writing to the regular key-space
			state Transaction dataTr(self->dataDb);
			loop {
				try {
					dataTr.setOption(FDBTransactionOptions::RAW_ACCESS);
					dataTr.set(self->keyName, self->noTenantValue);
					wait(dataTr.commit());
					break;
				} catch (Error& e) {
					wait(dataTr.onError(e));
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

	Future<Optional<TenantMapEntry>> tryGetTenant(TenantName tenantName, OperationType operationType) {
		if (operationType == OperationType::METACLUSTER) {
			return MetaclusterAPI::tryGetTenant(mvDb, tenantName);
		} else {
			return TenantAPI::tryGetTenant(dataDb.getReference(), tenantName);
		}
	}

	// Creates tenant(s) using the specified operation type
	ACTOR static Future<Void> createImpl(Reference<ReadYourWritesTransaction> tr,
	                                     std::map<TenantName, TenantMapEntry> tenantsToCreate,
	                                     OperationType operationType,
	                                     TenantManagementWorkload* self) {
		if (operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (auto [tenant, entry] : tenantsToCreate) {
				tr->set(self->specialKeysTenantMapPrefix.withSuffix(tenant), ""_sr);
				if (entry.tenantGroup.present()) {
					tr->set(self->specialKeysTenantConfigPrefix.withSuffix(
					            Tuple::makeTuple(tenant, "tenant_group"_sr).pack()),
					        entry.tenantGroup.get());
				}
			}
			wait(tr->commit());
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			ASSERT(tenantsToCreate.size() == 1);
			Optional<TenantMapEntry> result = wait(TenantAPI::createTenant(
			    self->dataDb.getReference(), tenantsToCreate.begin()->first, tenantsToCreate.begin()->second));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			int64_t _nextId = wait(TenantAPI::getNextTenantId(tr));
			int64_t nextId = _nextId;

			std::vector<Future<Void>> createFutures;
			for (auto [tenant, entry] : tenantsToCreate) {
				entry.id = nextId++;
				createFutures.push_back(success(TenantAPI::createTenantTransaction(tr, tenant, entry)));
			}
			TenantMetadata::lastTenantId.set(tr, nextId - 1);
			wait(waitForAll(createFutures));
			wait(tr->commit());
		} else {
			ASSERT(tenantsToCreate.size() == 1);
			wait(MetaclusterAPI::createTenant(
			    self->mvDb, tenantsToCreate.begin()->first, tenantsToCreate.begin()->second));
		}

		return Void();
	}

	ACTOR static Future<Void> createTenant(TenantManagementWorkload* self) {
		state OperationType operationType = self->randomOperationType();
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

		state std::map<TenantName, TenantMapEntry> tenantsToCreate;
		for (int i = 0; i < numTenants; ++i) {
			TenantName tenant = self->chooseTenantName(true);
			TenantMapEntry entry;
			entry.tenantGroup = self->chooseTenantGroup();
			tenantsToCreate[tenant] = entry;

			alreadyExists = alreadyExists || self->createdTenants.count(tenant);
			hasSystemTenant = hasSystemTenant || tenant.startsWith("\xff"_sr);
		}

		// If any tenant existed at the start of this function, then we expect the creation to fail or be a no-op,
		// depending on the type of create operation being executed
		state bool existedAtStart = alreadyExists;

		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		loop {
			try {
				// First, attempt to create the tenants
				state bool retried = false;
				loop {
					try {
						Optional<Void> result = wait(timeout(createImpl(tr, tenantsToCreate, operationType, self),
						                                     deterministicRandom()->randomInt(1, 30)));

						if (result.present()) {
							// Database operations shouldn't get here if the tenant already exists
							ASSERT(operationType == OperationType::SPECIAL_KEYS ||
							       operationType == OperationType::MANAGEMENT_TRANSACTION || !alreadyExists);
							break;
						}

						retried = true;
						tr->reset();
					} catch (Error& e) {
						// If we retried the creation after our initial attempt succeeded, then we proceed with the rest
						// of the creation steps normally. Otherwise, the creation happened elsewhere and we failed
						// here, so we can rethrow the error.
						if (e.code() == error_code_tenant_already_exists && !existedAtStart) {
							ASSERT(operationType == OperationType::METACLUSTER ||
							       operationType == OperationType::MANAGEMENT_DATABASE);
							ASSERT(retried);
							break;
						} else {
							throw;
						}
					}

					// Check the state of the first created tenant
					Optional<TenantMapEntry> resultEntry =
					    wait(self->tryGetTenant(tenantsToCreate.begin()->first, operationType));

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

				// Database-based creation modes will fail if the tenant already existed
				if (operationType == OperationType::MANAGEMENT_DATABASE ||
				    operationType == OperationType::METACLUSTER) {
					ASSERT(!existedAtStart);
				}

				// It is not legal to create a tenant starting with \xff
				ASSERT(!hasSystemTenant);

				state std::map<TenantName, TenantMapEntry>::iterator tenantItr;
				for (tenantItr = tenantsToCreate.begin(); tenantItr != tenantsToCreate.end(); ++tenantItr) {
					// Ignore any tenants that already existed
					if (self->createdTenants.count(tenantItr->first)) {
						continue;
					}

					// Read the created tenant object and verify that its state is correct
					state Optional<TenantMapEntry> entry = wait(self->tryGetTenant(tenantItr->first, operationType));

					ASSERT(entry.present());
					ASSERT(entry.get().id > self->maxId);
					ASSERT(entry.get().prefix.startsWith(self->tenantSubspace));
					ASSERT(entry.get().tenantGroup == tenantItr->second.tenantGroup);
					ASSERT(entry.get().tenantState == TenantState::READY);

					if (self->useMetacluster) {
						// In a metacluster, we should also see that the tenant was created on the data cluster
						Optional<TenantMapEntry> dataEntry =
						    wait(TenantAPI::tryGetTenant(self->dataDb.getReference(), tenantItr->first));
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

						// Make sure that the key inserted correctly concatenates the tenant prefix with the
						// relative key
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
					wait(checkTenantContents(self, tenantItr->first, self->createdTenants[tenantItr->first]));
				}

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

	// Deletes the tenant or tenant range using the specified operation type
	ACTOR static Future<Void> deleteImpl(Reference<ReadYourWritesTransaction> tr,
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
			wait(TenantAPI::deleteTenant(self->dataDb.getReference(), beginTenant));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			std::vector<Future<Void>> deleteFutures;
			for (tenantIndex = 0; tenantIndex != tenants.size(); ++tenantIndex) {
				deleteFutures.push_back(TenantAPI::deleteTenantTransaction(tr, tenants[tenantIndex]));
			}

			wait(waitForAll(deleteFutures));
			wait(tr->commit());
		} else {
			ASSERT(!endTenant.present() && tenants.size() == 1);
			wait(MetaclusterAPI::deleteTenant(self->mvDb, beginTenant));
		}

		return Void();
	}

	ACTOR static Future<Void> deleteTenant(TenantManagementWorkload* self) {
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

		// True if the beginTenant should exist and be deletable. This is updated if a deletion fails and gets
		// retried.
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
				state bool retried = false;
				loop {
					try {
						Optional<Void> result =
						    wait(timeout(deleteImpl(tr, beginTenant, endTenant, tenants, operationType, self),
						                 deterministicRandom()->randomInt(1, 30)));

						if (result.present()) {
							// Database operations shouldn't get here if the tenant didn't exist
							ASSERT(operationType == OperationType::SPECIAL_KEYS ||
							       operationType == OperationType::MANAGEMENT_TRANSACTION || alreadyExists);
							break;
						}

						retried = true;
						tr->reset();
					} catch (Error& e) {
						// If we retried the deletion after our initial attempt succeeded, then we proceed with the
						// rest of the deletion steps normally. Otherwise, the deletion happened elsewhere and we
						// failed here, so we can rethrow the error.
						if (e.code() == error_code_tenant_not_found && existedAtStart) {
							ASSERT(operationType == OperationType::METACLUSTER ||
							       operationType == OperationType::MANAGEMENT_DATABASE);
							ASSERT(retried);
							break;
						} else {
							throw;
						}
					}

					if (!tenants.empty()) {
						// Check the state of the first deleted tenant
						Optional<TenantMapEntry> resultEntry =
						    wait(self->tryGetTenant(*tenants.begin(), operationType));

						if (!resultEntry.present()) {
							alreadyExists = false;
						} else if (resultEntry.get().tenantState == TenantState::REMOVING) {
							ASSERT(operationType == OperationType::METACLUSTER);
						} else {
							ASSERT(resultEntry.get().tenantState == TenantState::READY);
						}
					}
				}

				// The management transaction operation is a no-op if there are no tenants to delete in a range
				// delete
				if (tenants.size() == 0 && operationType == OperationType::MANAGEMENT_TRANSACTION) {
					return Void();
				}

				// The special keys operation is a no-op if the begin and end tenant are equal (i.e. the range is
				// empty)
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
	ACTOR static Future<Void> checkTenantContents(TenantManagementWorkload* self,
	                                              TenantName tenant,
	                                              TenantData tenantData) {
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
		std::string base64Prefix;
		std::string printablePrefix;
		std::string tenantStateStr;
		std::string assignedClusterStr;
		std::string base64TenantGroup;
		std::string printableTenantGroup;

		jsonDoc.get("id", id);
		jsonDoc.get("prefix.base64", base64Prefix);
		jsonDoc.get("prefix.printable", printablePrefix);

		prefix = base64::decoder::from_string(base64Prefix);
		ASSERT(prefix == unprintable(printablePrefix));

		jsonDoc.get("tenant_state", tenantStateStr);

		Optional<ClusterName> assignedCluster;
		if (jsonDoc.tryGet("assigned_cluster", assignedClusterStr)) {
			assignedCluster = ClusterNameRef(assignedClusterStr);
		}

		Optional<TenantGroupName> tenantGroup;
		if (jsonDoc.tryGet("tenant_group.base64", base64TenantGroup)) {
			jsonDoc.get("tenant_group.printable", printableTenantGroup);
			std::string tenantGroupStr = base64::decoder::from_string(base64TenantGroup);
			ASSERT(tenantGroupStr == unprintable(printableTenantGroup));
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
	ACTOR static Future<TenantMapEntry> getImpl(Reference<ReadYourWritesTransaction> tr,
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
			TenantMapEntry _entry = wait(TenantAPI::getTenant(self->dataDb.getReference(), tenant));
			entry = _entry;
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			TenantMapEntry _entry = wait(TenantAPI::getTenantTransaction(tr, tenant));
			entry = _entry;
		} else {
			TenantMapEntry _entry = wait(MetaclusterAPI::getTenant(self->mvDb, tenant));
			entry = _entry;
		}

		return entry;
	}

	ACTOR static Future<Void> getTenant(TenantManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName(true);
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		// True if the tenant should should exist and return a result
		auto itr = self->createdTenants.find(tenant);
		state bool alreadyExists = itr != self->createdTenants.end() &&
		                           !(operationType == OperationType::METACLUSTER && !self->useMetacluster);
		state TenantData tenantData = alreadyExists ? itr->second : TenantData();

		loop {
			try {
				// Get the tenant metadata and check that it matches our local state
				state TenantMapEntry entry = wait(getImpl(tr, tenant, operationType, self));
				ASSERT(alreadyExists);
				ASSERT(entry.id == tenantData.id);
				ASSERT(entry.tenantGroup == tenantData.tenantGroup);
				wait(checkTenantContents(self, tenant, tenantData));
				return Void();
			} catch (Error& e) {
				state bool retry = false;
				state Error error = e;

				if (e.code() == error_code_tenant_not_found) {
					ASSERT(!alreadyExists);
					return Void();
				}

				// Transaction-based operations should retry
				else if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				         operationType == OperationType::SPECIAL_KEYS) {
					try {
						retry = true;
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
	ACTOR static Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listImpl(
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
			    wait(TenantAPI::listTenants(self->dataDb.getReference(), beginTenant, endTenant, limit));
			tenants = _tenants;
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			std::vector<std::pair<TenantName, TenantMapEntry>> _tenants =
			    wait(TenantAPI::listTenantsTransaction(tr, beginTenant, endTenant, limit));
			tenants = _tenants;
		} else {
			std::vector<std::pair<TenantName, TenantMapEntry>> _tenants =
			    wait(MetaclusterAPI::listTenants(self->mvDb, beginTenant, endTenant, limit));
			tenants = _tenants;
		}

		return tenants;
	}

	ACTOR static Future<Void> listTenants(TenantManagementWorkload* self) {
		state TenantName beginTenant = self->chooseTenantName(false);
		state TenantName endTenant = self->chooseTenantName(false);
		state int limit = std::min(CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1,
		                           deterministicRandom()->randomInt(1, self->maxTenants * 2));
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		if (beginTenant > endTenant) {
			std::swap(beginTenant, endTenant);
		}

		loop {
			try {
				// Attempt to read the chosen list of tenants
				state std::vector<std::pair<TenantName, TenantMapEntry>> tenants =
				    wait(listImpl(tr, beginTenant, endTenant, limit, operationType, self));

				// Attempting to read the list of tenants using the metacluster API in a non-metacluster should
				// return nothing in this test
				if (operationType == OperationType::METACLUSTER && !self->useMetacluster) {
					ASSERT(tenants.size() == 0);
					return Void();
				}

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
				if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				    operationType == OperationType::SPECIAL_KEYS) {
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

	ACTOR static Future<Void> renameTenant(TenantManagementWorkload* self) {
		// Currently only supporting MANAGEMENT_DATABASE op, so numTenants should always be 1
		// state OperationType operationType = TenantManagementWorkload::randomOperationType();
		int numTenants = 1;

		state std::vector<TenantName> oldTenantNames;
		state std::vector<TenantName> newTenantNames;
		state bool tenantExists = false;
		state bool tenantNotFound = false;
		for (int i = 0; i < numTenants; ++i) {
			TenantName oldTenant = self->chooseTenantName(false);
			TenantName newTenant = self->chooseTenantName(false);
			newTenantNames.push_back(newTenant);
			oldTenantNames.push_back(oldTenant);
			if (!self->createdTenants.count(oldTenant)) {
				tenantNotFound = true;
			}
			if (self->createdTenants.count(newTenant)) {
				tenantExists = true;
			}
		}

		loop {
			try {
				ASSERT(oldTenantNames.size() == 1);
				state int tenantIndex = 0;
				for (; tenantIndex != oldTenantNames.size(); ++tenantIndex) {
					state TenantName oldTenantName = oldTenantNames[tenantIndex];
					state TenantName newTenantName = newTenantNames[tenantIndex];
					// Perform rename, then check against the DB for the new results
					wait(TenantAPI::renameTenant(self->dataDb.getReference(), oldTenantName, newTenantName));
					ASSERT(!tenantNotFound && !tenantExists);
					state Optional<TenantMapEntry> oldTenantEntry =
					    wait(self->tryGetTenant(oldTenantName, OperationType::SPECIAL_KEYS));
					state Optional<TenantMapEntry> newTenantEntry =
					    wait(self->tryGetTenant(newTenantName, OperationType::SPECIAL_KEYS));
					ASSERT(!oldTenantEntry.present());
					ASSERT(newTenantEntry.present());

					// Update Internal Tenant Map and check for correctness
					TenantData tData = self->createdTenants[oldTenantName];
					self->createdTenants[newTenantName] = tData;
					self->createdTenants.erase(oldTenantName);
					if (!tData.empty) {
						state Transaction insertTr(self->dataDb, newTenantName);
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
					wait(checkTenantContents(self, newTenantName, self->createdTenants[newTenantName]));
				}
				return Void();
			} catch (Error& e) {
				ASSERT(oldTenantNames.size() == 1);
				if (e.code() == error_code_tenant_not_found) {
					TraceEvent("RenameTenantOldTenantNotFound")
					    .detail("OldTenantName", oldTenantNames[0])
					    .detail("NewTenantName", newTenantNames[0]);
					ASSERT(tenantNotFound);
				} else if (e.code() == error_code_tenant_already_exists) {
					TraceEvent("RenameTenantNewTenantAlreadyExists")
					    .detail("OldTenantName", oldTenantNames[0])
					    .detail("NewTenantName", newTenantNames[0]);
					ASSERT(tenantExists);
				} else {
					TraceEvent(SevError, "RenameTenantFailure")
					    .error(e)
					    .detail("OldTenantName", oldTenantNames[0])
					    .detail("NewTenantName", newTenantNames[0]);
				}
				return Void();
			}
		}
	}

	// Gets a list of tenants using the specified operation type
	ACTOR static Future<bool> configureImpl(Reference<ReadYourWritesTransaction> tr,
	                                        TenantName tenant,
	                                        std::map<Standalone<StringRef>, Optional<Value>> configParameters,
	                                        OperationType operationType,
	                                        TenantManagementWorkload* self) {
		if (operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			state bool hasInvalidSpecialKeyTuple = deterministicRandom()->random01() < 0.05;

			try {
				for (auto const& [config, value] : configParameters) {
					Tuple t;
					if (hasInvalidSpecialKeyTuple) {
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
				ASSERT(!hasInvalidSpecialKeyTuple);
				return true;
			} catch (Error& e) {
				if (e.code() == error_code_special_keys_api_failure && hasInvalidSpecialKeyTuple) {
					return false;
				}

				throw;
			}
		} else if (operationType == OperationType::METACLUSTER) {
			wait(MetaclusterAPI::configureTenant(self->mvDb, tenant, configParameters));
			return true;
		} else {
			// We don't have a transaction or database variant of this function
			ASSERT(false);
			throw internal_error();
		}
	}

	ACTOR static Future<Void> configureTenant(TenantManagementWorkload* self) {
		state OperationType operationType =
		    deterministicRandom()->coinflip() ? OperationType::SPECIAL_KEYS : OperationType::METACLUSTER;

		state TenantName tenant = self->chooseTenantName(true);
		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		state std::map<Standalone<StringRef>, Optional<Value>> configuration;
		state Optional<TenantGroupName> newTenantGroup;
		state bool hasInvalidOption = deterministicRandom()->random01() < 0.1;

		state bool hasInvalidTenantGroupChange = false;

		if (!hasInvalidOption || deterministicRandom()->coinflip()) {
			newTenantGroup = self->chooseTenantGroup();
			configuration["tenant_group"_sr] = newTenantGroup;

			// We cannot currently change tenant groups in a metacluster
			if (operationType == OperationType::METACLUSTER && exists && newTenantGroup.present() &&
			    newTenantGroup != itr->second.tenantGroup) {
				hasInvalidTenantGroupChange = true;
			}
		}
		if (hasInvalidOption) {
			configuration["invalid_option"_sr] = ""_sr;
		}

		loop {
			try {
				bool configured = wait(configureImpl(tr, tenant, configuration, operationType, self));

				if (configured) {
					ASSERT(exists);
					ASSERT(!hasInvalidOption);
					ASSERT(!hasInvalidTenantGroupChange);
					self->createdTenants[tenant].tenantGroup = newTenantGroup;
				}
				return Void();
			} catch (Error& e) {
				state Error error = e;
				if (e.code() == error_code_tenant_not_found) {
					ASSERT(!exists);
					return Void();
				} else if (e.code() == error_code_special_keys_api_failure) {
					ASSERT(hasInvalidOption);
					return Void();
				} else if (e.code() == error_code_invalid_tenant_configuration) {
					ASSERT(hasInvalidOption || hasInvalidTenantGroupChange);
					return Void();
				} else if (e.code() == error_code_tenants_disabled) {
					ASSERT((operationType == OperationType::METACLUSTER) != self->useMetacluster);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER && !self->useMetacluster);
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
				wait(createTenant(self));
			} else if (operation == 1) {
				wait(deleteTenant(self));
			} else if (operation == 2) {
				wait(getTenant(self));
			} else if (operation == 3) {
				wait(listTenants(self));
			} else if (operation == 4 && !self->useMetacluster) {
				// TODO: reenable this for metacluster once it is supported
				wait(renameTenant(self));
			} else if (operation == 5) {
				wait(configureTenant(self));
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
			// Read the tenant list from the data cluster.
			state std::vector<std::pair<TenantName, TenantMapEntry>> dataClusterTenants =
			    wait(TenantAPI::listTenants(self->dataDb.getReference(), beginTenant, endTenant, 1000));

			// Read the tenant list from the management cluster. If there is no management cluster, this is
			// just a duplicate of the data cluster tenants
			state std::vector<std::pair<TenantName, TenantMapEntry>> managementClusterTenants;
			if (self->useMetacluster) {
				std::vector<std::pair<TenantName, TenantMapEntry>> _managementClusterTenants =
				    wait(MetaclusterAPI::listTenants(cx.getReference(), beginTenant, endTenant, 1000));
				managementClusterTenants = _managementClusterTenants;
			} else {
				managementClusterTenants = dataClusterTenants;
			}

			auto managementItr = managementClusterTenants.begin();
			auto dataItr = dataClusterTenants.begin();

			TenantNameRef lastTenant;
			while (managementItr != managementClusterTenants.end()) {
				ASSERT(localItr != self->createdTenants.end());
				ASSERT(dataItr != dataClusterTenants.end());
				ASSERT(managementItr->first == localItr->first);
				ASSERT(managementItr->first == dataItr->first);
				ASSERT(managementItr->second.tenantGroup == localItr->second.tenantGroup);
				ASSERT(managementItr->second.matchesConfiguration(dataItr->second));

				checkTenants.push_back(checkTenantContents(self, managementItr->first, localItr->second));
				lastTenant = managementItr->first;

				++localItr;
				++managementItr;
				++dataItr;
			}

			ASSERT(dataItr == dataClusterTenants.end());

			if (dataClusterTenants.size() < 1000) {
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
