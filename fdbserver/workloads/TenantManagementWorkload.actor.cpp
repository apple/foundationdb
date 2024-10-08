/*
 * TenantManagementWorkload.actor.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/TenantSpecialKeys.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/ApiVersion.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/flow.h"
#include "libb64/decode.h"

#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterConsistency.actor.h"
#include "metacluster/TenantConsistency.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct TenantManagementWorkload : TestWorkload {
	static constexpr auto NAME = "TenantManagement";

	struct TenantTestData {
		Reference<Tenant> tenant;
		Optional<TenantGroupName> tenantGroup;
		TenantAPI::TenantLockState lockState = TenantAPI::TenantLockState::UNLOCKED;
		Optional<UID> lockId;
		bool empty;

		TenantTestData() : empty(true) {}
		TenantTestData(int64_t id, Optional<TenantGroupName> tenantGroup, bool empty)
		  : tenant(makeReference<Tenant>(id)), tenantGroup(tenantGroup), empty(empty) {}
		TenantTestData(int64_t id, Optional<TenantName> tName, Optional<TenantGroupName> tenantGroup, bool empty)
		  : tenant(makeReference<Tenant>(id, tName)), tenantGroup(tenantGroup), empty(empty) {}
	};

	struct TenantGroupData {
		int64_t tenantCount = 0;
	};

	std::map<TenantName, TenantTestData> createdTenants;
	std::map<TenantGroupName, TenantGroupData> createdTenantGroups;
	// Contains references to ALL tenants that were created by this client
	// Possible to have been deleted, but will be tracked historically here
	std::vector<Reference<Tenant>> allTestTenants;
	int64_t maxId = -1;

	const Key keyName = "key"_sr;
	const Key testParametersKey = nonMetadataSystemKeys.begin.withSuffix("/tenant_test/test_parameters"_sr);
	const Value noTenantValue = "no_tenant"_sr;
	const TenantName tenantNamePrefix = "tenant_management_workload_"_sr;
	ClusterName dataClusterName;
	TenantName localTenantNamePrefix;
	TenantName localTenantGroupNamePrefix;

	const Key specialKeysTenantMapPrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                           .begin.withSuffix(TenantRangeImpl::submoduleRange.begin)
	                                           .withSuffix(TenantRangeImpl::mapSubRange.begin);
	const Key specialKeysTenantConfigPrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                              .begin.withSuffix(TenantRangeImpl::submoduleRange.begin)
	                                              .withSuffix(TenantRangeImpl::configureSubRange.begin);
	const Key specialKeysTenantRenamePrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                              .begin.withSuffix(TenantRangeImpl::submoduleRange.begin)
	                                              .withSuffix(TenantRangeImpl::renameSubRange.begin);

	int maxTenants;
	int maxTenantGroups;
	double testDuration;
	bool useMetacluster;
	bool singleClient;

	Version oldestDeletionVersion = 0;
	Version newestDeletionVersion = 0;

	Reference<IDatabase> mvDb;
	Database dataDb;
	bool hasNoTenantKey = false; // whether this workload has non-tenant key
	int64_t tenantIdPrefix = 0;

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
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		singleClient = getOption(options, "singleClient"_sr, false);

		localTenantNamePrefix = format("%stenant_%d_", tenantNamePrefix.toString().c_str(), clientId);
		localTenantGroupNamePrefix = format("%stenantgroup_%d_", tenantNamePrefix.toString().c_str(), clientId);

		bool defaultUseMetacluster = false;
		if (clientId == 0 && g_network->isSimulated() && !g_simulator->extraDatabases.empty()) {
			defaultUseMetacluster = deterministicRandom()->coinflip();
		}

		useMetacluster = getOption(options, "useMetacluster"_sr, defaultUseMetacluster);
	}

	struct TestParameters {
		constexpr static FileIdentifier file_identifier = 1527576;

		bool useMetacluster = false;

		TestParameters() {}
		TestParameters(bool useMetacluster) : useMetacluster(useMetacluster) {}

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, useMetacluster);
		}

		Value encode() const { return ObjectWriter::toValue(*this, Unversioned()); }
		static TestParameters decode(ValueRef const& value) {
			return ObjectReader::fromStringRef<TestParameters>(value, Unversioned());
		}
	};

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0 && g_network->isSimulated() && BUGGIFY) {
			IKnobCollection::getMutableGlobalKnobCollection().setKnob(
			    "max_tenants_per_cluster", KnobValueRef::create(int{ deterministicRandom()->randomInt(20, 100) }));
		}

		if (clientId == 0 || !singleClient) {
			return _setup(cx, this);
		} else {
			return Void();
		}
	}

	// Set a key outside of all tenants to make sure that our tenants aren't writing to the regular key-space
	Future<Void> writeNonTenantKey() const {
		return runRYWTransaction(dataDb, [this](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			tr->set(keyName, noTenantValue);
			return Future<Void>(Void());
		});
	}

	// load test parameters from metacluster
	ACTOR static Future<Void> loadTestParameters(Database cx, TenantManagementWorkload* self) {
		state Transaction tr(cx);
		// Read the parameters chosen and saved by client 0
		loop {
			try {
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
				Optional<Value> val = wait(tr.get(self->testParametersKey));
				if (val.present()) {
					TestParameters params = TestParameters::decode(val.get());
					self->useMetacluster = params.useMetacluster;
					return Void();
				}
				wait(delay(1.0));
				tr.reset();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// send test parameters from metacluster
	ACTOR static Future<Void> sendTestParameters(Database cx, TenantManagementWorkload* self) {
		// Communicates test parameters to all other clients by storing it in a key
		fmt::print("Sending test parameters ...\n");
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
				tr.set(self->testParametersKey, TestParameters(self->useMetacluster).encode());
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	// only the first client will do this setup
	ACTOR static Future<Void> firstClientSetup(Database cx, TenantManagementWorkload* self) {
		wait(sendTestParameters(cx, self));

		if (self->dataDb->getTenantMode() != TenantMode::REQUIRED) {
			self->hasNoTenantKey = true;
			wait(self->writeNonTenantKey());
		}
		return Void();
	}

	ACTOR static Future<Void> _setup(Database cx, TenantManagementWorkload* self) {
		if (self->clientId != 0) {
			wait(loadTestParameters(cx, self));
		}

		metacluster::util::SkipMetaclusterCreation skipMetaclusterCreation(!self->useMetacluster ||
		                                                                   self->clientId != 0);
		Optional<metacluster::DataClusterEntry> entry;
		if (!skipMetaclusterCreation) {
			entry = metacluster::DataClusterEntry();
			entry.get().capacity.numTenantGroups = 1e9;
		}

		state metacluster::util::SimulatedMetacluster simMetacluster = wait(
		    metacluster::util::createSimulatedMetacluster(cx, self->tenantIdPrefix, entry, skipMetaclusterCreation));

		self->mvDb = simMetacluster.managementDb;

		if (self->useMetacluster) {
			ASSERT_EQ(simMetacluster.dataDbs.size(), 1);
			self->dataClusterName = simMetacluster.dataDbs.begin()->first;
			self->dataDb = simMetacluster.dataDbs.begin()->second;
		} else {
			self->dataDb = cx;
		}

		if (self->clientId == 0) {
			wait(firstClientSetup(cx, self));
		}

		return Void();
	}

	ACTOR template <class DB>
	static Future<Versionstamp> getLastTenantModification(Reference<DB> db, OperationType type) {
		state Reference<typename DB::TransactionT> tr = db->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				if (type == OperationType::METACLUSTER) {
					Versionstamp vs =
					    wait(metacluster::metadata::management::tenantMetadata().lastTenantModification.getD(
					        tr, Snapshot::False, Versionstamp()));
					return vs;
				}
				Versionstamp vs =
				    wait(TenantMetadata::lastTenantModification().getD(tr, Snapshot::False, Versionstamp()));
				return vs;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	ACTOR template <class DB>
	static Future<Version> getLatestReadVersion(Reference<DB> db, OperationType type) {
		state Reference<typename DB::TransactionT> tr = db->createTransaction();
		loop {
			try {
				Version readVersion = wait(safeThreadFutureToFuture(tr->getReadVersion()));
				return readVersion;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	static Future<Versionstamp> getLastTenantModification(TenantManagementWorkload* self, OperationType type) {
		if (type == OperationType::METACLUSTER) {
			return getLastTenantModification(self->mvDb, type);
		} else {
			return getLastTenantModification(self->dataDb.getReference(), type);
		}
	}

	static Future<Version> getLatestReadVersion(TenantManagementWorkload* self, OperationType type) {
		if (type == OperationType::METACLUSTER) {
			return getLatestReadVersion(self->mvDb, type);
		} else {
			return getLatestReadVersion(self->dataDb.getReference(), type);
		}
	}

	TenantName chooseTenantName(bool allowSystemTenant) {
		TenantName tenant(format(
		    "%s%08d", localTenantNamePrefix.toString().c_str(), deterministicRandom()->randomInt(0, maxTenants)));
		if (allowSystemTenant && deterministicRandom()->random01() < 0.02) {
			tenant = tenant.withPrefix("\xff"_sr);
		}

		return tenant;
	}

	Optional<TenantGroupName> chooseTenantGroup(bool allowSystemTenantGroup, bool allowEmptyGroup = true) {
		Optional<TenantGroupName> tenantGroup;
		if (!allowEmptyGroup || deterministicRandom()->coinflip()) {
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
	ACTOR static Future<Void> createTenantImpl(Reference<ReadYourWritesTransaction> tr,
	                                           std::map<TenantName, TenantMapEntry> tenantsToCreate,
	                                           OperationType operationType,
	                                           TenantManagementWorkload* self) {
		state metacluster::MetaclusterTenantMapEntry entry;
		state metacluster::AssignClusterAutomatically assign = metacluster::AssignClusterAutomatically::True;
		state metacluster::IgnoreCapacityLimit ignoreCapacityLimit(deterministicRandom()->coinflip());

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
			wait(success(TenantAPI::createTenant(
			    self->dataDb.getReference(), tenantsToCreate.begin()->first, tenantsToCreate.begin()->second)));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			int64_t _nextId = wait(TenantAPI::getNextTenantId(tr));
			int64_t nextId = _nextId;

			std::vector<Future<Void>> createFutures;
			for (auto [tenant, entry] : tenantsToCreate) {
				entry.setId(nextId++);
				createFutures.push_back(success(TenantAPI::createTenantTransaction(tr, entry)));
			}
			TenantMetadata::lastTenantId().set(tr, nextId - 1);
			wait(waitForAll(createFutures));
			wait(tr->commit());
		} else {
			ASSERT_EQ(operationType, OperationType::METACLUSTER);
			ASSERT_EQ(tenantsToCreate.size(), 1);
			entry = metacluster::MetaclusterTenantMapEntry::fromTenantMapEntry(tenantsToCreate.begin()->second);
			if (deterministicRandom()->coinflip()) {
				entry.assignedCluster = self->dataClusterName;
				assign = metacluster::AssignClusterAutomatically::False;
			}

			try {
				wait(metacluster::createTenant(self->mvDb, entry, assign, ignoreCapacityLimit));
				ASSERT(!assign || !ignoreCapacityLimit);
			} catch (Error& e) {
				if (e.code() == error_code_invalid_tenant_configuration) {
					ASSERT(assign && ignoreCapacityLimit);
				}
				throw e;
			}
			return Void();
		}

		return Void();
	}

	ACTOR template <class TenantMapEntryImpl>
	static Future<Void> verifyTenantCreate(TenantManagementWorkload* self,
	                                       Optional<TenantMapEntryImpl> entry,
	                                       TenantName itrName,
	                                       Optional<TenantGroupName> tGroup) {
		ASSERT(entry.present());
		ASSERT(entry.get().id > self->maxId);
		ASSERT(entry.get().tenantGroup == tGroup);
		ASSERT(TenantAPI::getTenantIdPrefix(entry.get().id) == self->tenantIdPrefix);

		if (self->useMetacluster) {
			// In a metacluster, we should also see that the tenant was created on the data cluster
			Optional<TenantMapEntry> dataEntry = wait(TenantAPI::tryGetTenant(self->dataDb.getReference(), itrName));
			ASSERT(dataEntry.present());
			ASSERT(dataEntry.get().id == entry.get().id);
			ASSERT(TenantAPI::getTenantIdPrefix(dataEntry.get().id) == self->tenantIdPrefix);
			ASSERT(dataEntry.get().tenantGroup == entry.get().tenantGroup);
		}

		// Update our local tenant state to include the newly created one
		self->maxId = entry.get().id;
		TenantTestData tData = TenantTestData(entry.get().id, itrName, tGroup, true);
		self->createdTenants[itrName] = tData;
		self->allTestTenants.push_back(tData.tenant);
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

		// True if any tenant group name starts with \xff
		state bool hasSystemTenantGroup = false;

		state int newTenants = 0;
		state std::map<TenantName, TenantMapEntry> tenantsToCreate;
		for (int i = 0; i < numTenants; ++i) {
			TenantName tenant = self->chooseTenantName(true);
			while (tenantsToCreate.contains(tenant)) {
				tenant = self->chooseTenantName(true);
			}

			TenantMapEntry entry;
			entry.tenantName = tenant;
			entry.tenantGroup = self->chooseTenantGroup(true);

			if (self->createdTenants.contains(tenant)) {
				alreadyExists = true;
			} else if (!tenantsToCreate.contains(tenant)) {
				++newTenants;
			}

			tenantsToCreate[tenant] = entry;
			hasSystemTenant = hasSystemTenant || tenant.startsWith("\xff"_sr);
			hasSystemTenantGroup = hasSystemTenantGroup || entry.tenantGroup.orDefault(""_sr).startsWith("\xff"_sr);
		}

		// If any tenant existed at the start of this function, then we expect the creation to fail or be a no-op,
		// depending on the type of create operation being executed
		state bool existedAtStart = alreadyExists;

		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);
		state int64_t minTenantCount = std::numeric_limits<int64_t>::max();
		state int64_t finalTenantCount = 0;

		state Version originalReadVersion = wait(self->getLatestReadVersion(self, operationType));
		loop {
			try {
				// First, attempt to create the tenants
				state bool retried = false;
				loop {
					if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
					    operationType == OperationType::SPECIAL_KEYS) {
						tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
						wait(store(finalTenantCount, TenantMetadata::tenantCount().getD(tr, Snapshot::False, 0)));
						minTenantCount = std::min(finalTenantCount, minTenantCount);
					}

					try {
						Optional<Void> result = wait(timeout(createTenantImpl(tr, tenantsToCreate, operationType, self),
						                                     deterministicRandom()->randomInt(1, 30)));

						if (result.present()) {
							// Make sure that we had capacity to create the tenants. This cannot be validated for
							// database operations because we cannot determine the tenant count in the same transaction
							// that the tenant is created
							if (operationType == OperationType::SPECIAL_KEYS ||
							    operationType == OperationType::MANAGEMENT_TRANSACTION) {
								ASSERT(minTenantCount + newTenants <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
							} else {
								// Database operations shouldn't get here if the tenant already exists
								ASSERT(!alreadyExists);
							}

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
						} else if (e.code() == error_code_invalid_tenant_configuration) {
							ASSERT_EQ(operationType, OperationType::METACLUSTER);
						} else {
							throw;
						}
					}

					// Check the state of the first created tenant
					if (operationType == OperationType::METACLUSTER) {
						Optional<metacluster::MetaclusterTenantMapEntry> resultEntry =
						    wait(metacluster::tryGetTenant(self->mvDb, tenantsToCreate.begin()->first));
						if (resultEntry.present()) {
							if (resultEntry.get().tenantState == metacluster::TenantState::READY) {
								// The tenant now exists, so we will retry and expect the creation to react accordingly
								alreadyExists = true;
							} else {
								// Only a metacluster tenant creation can end up in a partially created state
								// We should be able to retry and pick up where we left off
								ASSERT(resultEntry.get().tenantState == metacluster::TenantState::REGISTERING);
							}
						} else {
							CODE_PROBE(true, "Tenant creation (metacluster) aborted before writing data.");
						}
					} else {
						Optional<TenantMapEntry> tenantEntry =
						    wait(TenantAPI::tryGetTenant(self->dataDb.getReference(), tenantsToCreate.begin()->first));
						if (tenantEntry.present()) {
							alreadyExists = true;
						} else {
							CODE_PROBE(true, "Tenant creation (non-metacluster) aborted before writing data.");
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

				// It is not legal to create a tenant or tenant group starting with \xff
				ASSERT(!hasSystemTenant);
				ASSERT(!hasSystemTenantGroup);

				state typename std::map<TenantName, TenantMapEntry>::iterator tenantItr;
				for (tenantItr = tenantsToCreate.begin(); tenantItr != tenantsToCreate.end(); ++tenantItr) {
					// Ignore any tenants that already existed
					if (self->createdTenants.contains(tenantItr->first)) {
						continue;
					}

					// Read the created tenant object and verify that its state is correct
					state StringRef tPrefix;
					if (operationType == OperationType::METACLUSTER) {
						state Optional<metacluster::MetaclusterTenantMapEntry> metaEntry =
						    wait(metacluster::tryGetTenant(self->mvDb, tenantItr->first));
						wait(verifyTenantCreate<metacluster::MetaclusterTenantMapEntry>(
						    self, metaEntry, tenantItr->first, tenantItr->second.tenantGroup));
						ASSERT(metaEntry.get().tenantState == metacluster::TenantState::READY);
						tPrefix = metaEntry.get().prefix;
					} else {
						state Optional<TenantMapEntry> normalEntry =
						    wait(TenantAPI::tryGetTenant(self->dataDb.getReference(), tenantItr->first));
						wait(verifyTenantCreate<TenantMapEntry>(
						    self, normalEntry, tenantItr->first, tenantItr->second.tenantGroup));
						tPrefix = normalEntry.get().prefix;
					}

					Versionstamp currentVersionstamp = wait(getLastTenantModification(self, operationType));
					ASSERT_GT(currentVersionstamp.version, originalReadVersion);

					// If this tenant has a tenant group, create or update the entry for it
					if (tenantItr->second.tenantGroup.present()) {
						self->createdTenantGroups[tenantItr->second.tenantGroup.get()].tenantCount++;
					}

					// Randomly decide to insert a key into the tenant
					state bool insertData = deterministicRandom()->random01() < 0.5;
					if (insertData) {
						state Transaction insertTr(self->dataDb, self->createdTenants[tenantItr->first].tenant);
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
								Optional<Value> val = wait(checkTr.get(self->keyName.withPrefix(tPrefix)));
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
				} else if (e.code() == error_code_invalid_tenant_group_name) {
					ASSERT(hasSystemTenantGroup);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER != self->useMetacluster);
					return Void();
				} else if (e.code() == error_code_cluster_no_capacity) {
					// Confirm that we overshot our capacity. This check cannot be done for database operations
					// because we cannot transactionally get the tenant count with the creation.
					if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
					    operationType == OperationType::SPECIAL_KEYS) {
						ASSERT(finalTenantCount + newTenants > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
					}
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
						    .errorUnsuppressed(e)
						    .detail("TenantName", tenantsToCreate.begin()->first);
						ASSERT(false);
					}

					return Void();
				}

				// Transaction-based operations should be retried
				else {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						for (auto [tenant, _] : tenantsToCreate) {
							TraceEvent(SevError, "CreateTenantFailure")
							    .errorUnsuppressed(e)
							    .detail("TenantName", tenant);
						}
						ASSERT(false);
					}
				}
			}
		}
	}

	// set a random watch on a tenant
	ACTOR static Future<Void> watchTenant(TenantManagementWorkload* self, Reference<Tenant> tenant) {
		loop {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->dataDb, tenant));
			try {
				state Future<Void> watch = tr->watch(doubleToTestKey(deterministicRandom()->random01()));
				wait(tr->commit());
				wait(watch);
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	void checkWatchOnDeletedTenant(std::vector<std::pair<TenantName, Future<ErrorOr<Void>>>>& watchFutures) {
		for (auto& [t, f] : watchFutures) {
			auto& res = f.get();
			if (res.isError(error_code_tenant_removed) || res.isError(error_code_tenant_not_found)) {
				CODE_PROBE(res.isError(error_code_tenant_removed),
				           "Watch Triggered because the tenant is deleted during watch");
			} else {
				TraceEvent(SevError, "WatchDeletedTenantCheckFailed")
				    .detail("Tenant", t)
				    .detail("IsError", res.isError() ? res.getError().what() : "Void()");
			}
		}
	}

	ACTOR static Future<bool> clearTenantData(TenantManagementWorkload* self, TenantName tenantName) {
		state Transaction clearTr(self->dataDb, self->createdTenants[tenantName].tenant);
		loop {
			try {
				clearTr.clear(self->keyName);
				wait(clearTr.commit());
				auto itr = self->createdTenants.find(tenantName);
				ASSERT(itr != self->createdTenants.end());
				ASSERT_EQ(itr->second.lockState, TenantAPI::TenantLockState::UNLOCKED);
				itr->second.empty = true;
				return true;
			} catch (Error& e) {
				if (e.code() == error_code_tenant_locked) {
					ASSERT_NE(self->createdTenants[tenantName].lockState, TenantAPI::TenantLockState::UNLOCKED);
					return false;
				}
				wait(clearTr.onError(e));
			}
		}
	}

	// Deletes the tenant or tenant range using the specified operation type
	ACTOR static Future<Void> deleteTenantImpl(Reference<ReadYourWritesTransaction> tr,
	                                           TenantName beginTenant,
	                                           Optional<TenantName> endTenant,
	                                           std::map<TenantName, int64_t> tenants,
	                                           OperationType operationType,
	                                           TenantManagementWorkload* self) {
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
			for (auto const& [name, id] : tenants) {
				if (id != TenantInfo::INVALID_TENANT) {
					deleteFutures.push_back(TenantAPI::deleteTenantTransaction(tr, id));
				}
			}

			wait(waitForAll(deleteFutures));
			wait(tr->commit());
		} else { // operationType == OperationType::METACLUSTER
			ASSERT(!endTenant.present() && tenants.size() == 1);
			// Read the entry first and then issue delete by ID
			// getTenant throwing tenant_not_found will break some test cases because it is not wrapped
			// by runManagementTransaction. For such cases, fall back to delete by name and allow
			// the errors to flow through there
			Optional<metacluster::MetaclusterTenantMapEntry> entry =
			    wait(metacluster::tryGetTenant(self->mvDb, beginTenant));
			if (entry.present() && deterministicRandom()->coinflip()) {
				wait(metacluster::deleteTenant(self->mvDb, entry.get().id));
				CODE_PROBE(true, "Deleted tenant by ID");
			} else {
				wait(metacluster::deleteTenant(self->mvDb, beginTenant));
			}
		}

		return Void();
	}

	ACTOR static Future<Void> deleteTenant(TenantManagementWorkload* self) {
		state bool watchTenantCheck = deterministicRandom()->coinflip();
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

		// True if we expect that some tenant will be deleted
		state bool anyExists = alreadyExists;

		// Collect a list of all tenants that we expect should be deleted by this operation
		state std::map<TenantName, int64_t> tenants;
		if (!endTenant.present()) {
			tenants[beginTenant] = anyExists ? itr->second.tenant->id() : TenantInfo::INVALID_TENANT;
		} else if (endTenant.present()) {
			anyExists = false;
			for (auto itr = self->createdTenants.lower_bound(beginTenant);
			     itr != self->createdTenants.end() && itr->first < endTenant.get();
			     ++itr) {
				tenants[itr->first] = itr->second.tenant->id();
				anyExists = true;
			}
		}

		// Check whether each tenant is empty.
		state std::map<TenantName, int64_t>::iterator tenantItr;
		state std::vector<std::pair<TenantName, Future<ErrorOr<Void>>>> watchFutures;
		try {
			if (alreadyExists || endTenant.present()) {
				for (tenantItr = tenants.begin(); tenantItr != tenants.end(); ++tenantItr) {
					// For most tenants, we will delete the contents and make them empty
					state bool cleared = false;
					if (deterministicRandom()->random01() < 0.9) {
						wait(store(cleared, clearTenantData(self, tenantItr->first)));

						// watch the tenant to be deleted
						if (cleared && watchTenantCheck) {
							watchFutures.emplace_back(
							    tenantItr->first,
							    errorOr(watchTenant(self, self->createdTenants[tenantItr->first].tenant)));
						}
					}

					// Otherwise, we will just report the current emptiness of the tenant
					if (!cleared) {
						auto itr = self->createdTenants.find(tenantItr->first);
						ASSERT(itr != self->createdTenants.end());
						isEmpty = isEmpty && itr->second.empty;
					}
				}
			}
		} catch (Error& e) {
			TraceEvent(SevError, "DeleteTenantFailure")
			    .errorUnsuppressed(e)
			    .detail("TenantName", beginTenant)
			    .detail("EndTenant", endTenant);
			ASSERT(false);
			return Void();
		}

		// Tenants deletion retry loop
		state Version originalReadVersion = wait(self->getLatestReadVersion(self, operationType));
		loop {
			try {
				// Attempt to delete the tenant(s)
				state bool retried = false;
				loop {
					try {
						state Version beforeVersion =
						    wait(getLatestReadVersion(self, OperationType::MANAGEMENT_DATABASE));
						Optional<Void> result =
						    wait(timeout(deleteTenantImpl(tr, beginTenant, endTenant, tenants, operationType, self),
						                 deterministicRandom()->randomInt(1, 30)));

						if (result.present()) {
							if (anyExists) {
								if (self->oldestDeletionVersion == 0 && !tenants.empty()) {
									Version afterVersion =
									    wait(self->getLatestReadVersion(self, OperationType::MANAGEMENT_DATABASE));
									self->oldestDeletionVersion = afterVersion;
								}
								self->newestDeletionVersion = beforeVersion;
							}

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
						} else if (e.code() == error_code_grv_proxy_memory_limit_exceeded ||
						           e.code() == error_code_batch_transaction_throttled) {
							// GRV proxy returns an error
							wait(tr->onError(e));
							continue;
						} else {
							throw;
						}
					}

					if (!tenants.empty()) {
						if (operationType == OperationType::METACLUSTER) {
							// Check the state of the first deleted tenant
							Optional<metacluster::MetaclusterTenantMapEntry> resultEntry =
							    wait(metacluster::tryGetTenant(self->mvDb, tenants.begin()->first));
							if (!resultEntry.present()) {
								alreadyExists = false;
							} else {
								ASSERT(resultEntry.get().tenantState == metacluster::TenantState::READY ||
								       resultEntry.get().tenantState == metacluster::TenantState::REMOVING);
							}
						} else {
							Optional<TenantMapEntry> tenantEntry =
							    wait(TenantAPI::tryGetTenant(self->dataDb.getReference(), tenants.begin()->first));
							if (!tenantEntry.present()) {
								alreadyExists = false;
							}
						}
					}
				}

				// The management transaction operation is a no-op if there are no tenants to delete
				if (!anyExists && operationType == OperationType::MANAGEMENT_TRANSACTION) {
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

				if (tenants.size() > 0) {
					Versionstamp currentVersionstamp = wait(getLastTenantModification(self, operationType));
					ASSERT_GT(currentVersionstamp.version, originalReadVersion);
				}

				// Update our local state to remove the deleted tenants
				for (auto const& [name, id] : tenants) {
					auto itr = self->createdTenants.find(name);
					ASSERT(itr != self->createdTenants.end());

					// If the tenant group has no tenants remaining, stop tracking it
					if (itr->second.tenantGroup.present()) {
						auto tenantGroupItr = self->createdTenantGroups.find(itr->second.tenantGroup.get());
						ASSERT(tenantGroupItr != self->createdTenantGroups.end());
						if (--tenantGroupItr->second.tenantCount == 0) {
							self->createdTenantGroups.erase(tenantGroupItr);
						}
					}

					self->createdTenants.erase(name);
				}

				// check for watch result
				state int wi = 0;
				for (; wi < watchFutures.size(); ++wi) {
					wait(ready(watchFutures[wi].second));
				}
				self->checkWatchOnDeletedTenant(watchFutures);

				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_not_empty) {
					ASSERT(!isEmpty);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER != self->useMetacluster);
					return Void();
				}

				// Database-based operations do not need to be retried
				else if (operationType == OperationType::MANAGEMENT_DATABASE ||
				         operationType == OperationType::METACLUSTER) {
					if (e.code() == error_code_tenant_not_found) {
						ASSERT(!existedAtStart && !endTenant.present());
					} else {
						TraceEvent(SevError, "DeleteTenantFailure")
						    .errorUnsuppressed(e)
						    .detail("TenantName", beginTenant)
						    .detail("EndTenant", endTenant);
						ASSERT(false);
					}
					return Void();
				}

				// Transaction-based operations should be retried
				else {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						TraceEvent(SevError, "DeleteTenantFailure")
						    .errorUnsuppressed(e)
						    .detail("TenantName", beginTenant)
						    .detail("EndTenant", endTenant);
						ASSERT(false);
					}
				}
			}
		}
	}

	// Performs some validation on a tenant's contents
	ACTOR static Future<Void> checkTenantContents(TenantManagementWorkload* self,
	                                              TenantName tenantName,
	                                              TenantTestData tenantData) {
		state Transaction tr(self->dataDb, self->createdTenants[tenantName].tenant);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

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
					CODE_PROBE(true, "Check tenant contents with data");
					ASSERT(result.size() == 1);
					ASSERT(result[0].key == self->keyName);
					ASSERT(result[0].value == tenantName);
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

		std::string name;
		std::string base64Name;
		std::string printableName;
		std::string prefix;
		std::string base64Prefix;
		std::string printablePrefix;
		std::string tenantStateStr;
		std::string base64TenantGroup;
		std::string printableTenantGroup;
		std::string assignedClusterStr;

		jsonDoc.get("id", id);
		jsonDoc.get("name.base64", base64Name);
		jsonDoc.get("name.printable", printableName);

		name = base64::decoder::from_string(base64Name);
		ASSERT(name == unprintable(printableName));

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

		TenantMapEntry entry(id, TenantNameRef(name), tenantGroup);
		ASSERT(entry.prefix == prefix);
		return entry;
	}

	// Gets the metadata for a tenant using the specified operation type
	ACTOR static Future<TenantMapEntry> getTenantImpl(Reference<ReadYourWritesTransaction> tr,
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
			wait(store(entry, TenantAPI::getTenant(self->dataDb.getReference(), tenant)));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			wait(store(entry, TenantAPI::getTenantTransaction(tr, tenant)));
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
		state TenantTestData tenantData = alreadyExists ? itr->second : TenantTestData();

		loop {
			try {
				// Get the tenant metadata and check that it matches our local state
				state int64_t entryId;
				state Optional<TenantGroupName> tGroup;
				if (operationType == OperationType::METACLUSTER) {
					state metacluster::MetaclusterTenantMapEntry metaEntry =
					    wait(metacluster::getTenant(self->mvDb, tenant));
					entryId = metaEntry.id;
					tGroup = metaEntry.tenantGroup;
				} else {
					state TenantMapEntry normalEntry = wait(getTenantImpl(tr, tenant, operationType, self));
					entryId = normalEntry.id;
					tGroup = normalEntry.tenantGroup;
				}
				ASSERT(alreadyExists);
				ASSERT(entryId == tenantData.tenant->id());
				ASSERT(tGroup == tenantData.tenantGroup);
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
						retry = true;
					} catch (Error& e) {
						error = e;
						retry = false;
					}
				}

				if (!retry) {
					TraceEvent(SevError, "GetTenantFailure").errorUnsuppressed(error).detail("TenantName", tenant);
					ASSERT(false);
				}
			}
		}
	}

	// Gets a list of tenants using the specified operation type
	ACTOR static Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listTenantsImpl(
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
			wait(store(tenants,
			           TenantAPI::listTenantMetadata(self->dataDb.getReference(), beginTenant, endTenant, limit)));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			wait(store(tenants, TenantAPI::listTenantMetadataTransaction(tr, beginTenant, endTenant, limit)));
		}

		return tenants;
	}

	template <class TenantMapEntryImpl>
	static Future<Void> verifyTenantList(TenantManagementWorkload* self,
	                                     std::vector<std::pair<TenantName, TenantMapEntryImpl>> tenants,
	                                     int limit,
	                                     TenantName beginTenant,
	                                     TenantName endTenant,
	                                     Optional<TenantGroupName> tenantGroup) {
		ASSERT(tenants.size() <= limit);

		// Compare the resulting tenant list to the list we expected to get
		auto localItr = self->createdTenants.lower_bound(beginTenant);
		auto tenantMapItr = tenants.begin();
		for (; tenantMapItr != tenants.end(); ++tenantMapItr, ++localItr) {
			if (tenantGroup.present()) {
				while (localItr != self->createdTenants.end() && localItr->second.tenantGroup != tenantGroup) {
					++localItr;
				}
			}
			ASSERT(localItr != self->createdTenants.end());
			ASSERT(localItr->first == tenantMapItr->first);
		}
		// "tenants" exhausted to end. If tenantGroup was specified,
		// continue iterating localItr until end to verify there are no matches
		if (tenantGroup.present() && tenants.size() < limit) {
			CODE_PROBE(localItr != self->createdTenants.end() && localItr->first < endTenant,
			           "Listed range contained extra tenants not in group");
			while (localItr != self->createdTenants.end() && localItr->first < endTenant) {
				ASSERT(localItr->second.tenantGroup != tenantGroup);
				++localItr;
			}
		}

		// Make sure the list terminated at the right spot
		ASSERT(tenants.size() == limit || localItr == self->createdTenants.end() || localItr->first >= endTenant);
		return Void();
	}

	ACTOR static Future<Void> listTenants(TenantManagementWorkload* self) {
		state TenantName beginTenant = self->chooseTenantName(false);
		state TenantName endTenant = self->chooseTenantName(false);
		state int limit = std::min(CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1,
		                           deterministicRandom()->randomInt(1, self->maxTenants * 2));
		state OperationType operationType =
		    self->useMetacluster ? OperationType::METACLUSTER : self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);
		state Optional<TenantGroupName> tGroup =
		    deterministicRandom()->coinflip() ? self->chooseTenantGroup(false, false) : Optional<TenantGroupName>();

		if (beginTenant > endTenant) {
			std::swap(beginTenant, endTenant);
		}

		loop {
			try {
				if (self->useMetacluster) {
					state std::vector<std::pair<TenantName, metacluster::MetaclusterTenantMapEntry>> metaTenants =
					    wait(metacluster::listTenantMetadata(self->mvDb,
					                                         beginTenant,
					                                         endTenant,
					                                         limit,
					                                         /*offset=*/0,
					                                         /*filters=*/std::vector<metacluster::TenantState>(),
					                                         tGroup));
					verifyTenantList<metacluster::MetaclusterTenantMapEntry>(
					    self, metaTenants, limit, beginTenant, endTenant, tGroup);
				} else {
					if (tGroup.present()) {
						state std::vector<std::pair<TenantName, int64_t>> tenantsFiltered = wait(
						    TenantAPI::listTenantGroupTenants(self->mvDb, tGroup.get(), beginTenant, endTenant, limit));
						verifyTenantList<int64_t>(self, tenantsFiltered, limit, beginTenant, endTenant, tGroup);
					} else {
						state std::vector<std::pair<TenantName, TenantMapEntry>> tenants =
						    wait(listTenantsImpl(tr, beginTenant, endTenant, limit, operationType, self));
						if (operationType == OperationType::METACLUSTER) {
							ASSERT_EQ(tenants.size(), 0);
							return Void();
						}
						verifyTenantList<TenantMapEntry>(self, tenants, limit, beginTenant, endTenant, tGroup);
					}
				}
				return Void();
			} catch (Error& e) {
				state bool retry = false;
				state Error error = e;

				// Transaction-based operations need to be retried
				if (!self->useMetacluster && (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				                              operationType == OperationType::SPECIAL_KEYS)) {
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
					    .errorUnsuppressed(error)
					    .detail("BeginTenant", beginTenant)
					    .detail("EndTenant", endTenant);

					ASSERT(false);
				}
			}
		}
	}

	// Helper function that checks tenant keyspace and updates internal Tenant Map after a rename operation
	ACTOR static Future<Void> verifyTenantRename(TenantManagementWorkload* self,
	                                             TenantName oldTenantName,
	                                             TenantName newTenantName) {
		state Optional<TenantMapEntry> oldTenantEntry =
		    wait(TenantAPI::tryGetTenant(self->dataDb.getReference(), oldTenantName));
		state Optional<TenantMapEntry> newTenantEntry =
		    wait(TenantAPI::tryGetTenant(self->dataDb.getReference(), newTenantName));
		ASSERT(!oldTenantEntry.present());
		ASSERT(newTenantEntry.present());
		TenantTestData tData = self->createdTenants[oldTenantName];
		tData.tenant->name = newTenantName;
		self->createdTenants[newTenantName] = tData;
		self->createdTenants.erase(oldTenantName);
		state Transaction insertTr(self->dataDb, tData.tenant);
		if (!tData.empty) {
			loop {
				try {
					insertTr.setOption(FDBTransactionOptions::LOCK_AWARE);
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

	ACTOR static Future<Void> verifyTenantRenames(TenantManagementWorkload* self,
	                                              std::map<TenantName, TenantName> tenantRenames) {
		state std::map<TenantName, TenantName> tenantRenamesCopy = tenantRenames;
		state std::map<TenantName, TenantName>::iterator iter = tenantRenamesCopy.begin();
		for (; iter != tenantRenamesCopy.end(); ++iter) {
			wait(verifyTenantRename(self, iter->first, iter->second));
			wait(checkTenantContents(self, iter->second, self->createdTenants[iter->second]));
		}
		return Void();
	}

	ACTOR static Future<Void> renameTenantImpl(TenantManagementWorkload* self,
	                                           Reference<ReadYourWritesTransaction> tr,
	                                           OperationType operationType,
	                                           std::map<TenantName, TenantName> tenantRenames,
	                                           bool tenantNotFound,
	                                           bool tenantExists) {
		if (operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (auto& iter : tenantRenames) {
				tr->set(self->specialKeysTenantRenamePrefix.withSuffix(iter.first), iter.second);
			}
			wait(tr->commit());
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			ASSERT(tenantRenames.size() == 1);
			auto iter = tenantRenames.begin();
			wait(TenantAPI::renameTenant(self->dataDb.getReference(), iter->first, iter->second));
			ASSERT(!tenantNotFound && !tenantExists);
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			std::vector<Future<Void>> renameFutures;
			for (auto& iter : tenantRenames) {
				renameFutures.push_back(success(TenantAPI::renameTenantTransaction(tr, iter.first, iter.second)));
			}
			wait(waitForAll(renameFutures));
			wait(tr->commit());
			ASSERT(!tenantNotFound && !tenantExists);
		} else { // operationType == OperationType::METACLUSTER
			ASSERT(tenantRenames.size() == 1);
			auto iter = tenantRenames.begin();
			wait(metacluster::renameTenant(self->mvDb, iter->first, iter->second));
		}
		return Void();
	}

	ACTOR static Future<Void> renameTenant(TenantManagementWorkload* self) {
		state OperationType operationType = self->randomOperationType();
		state int numTenants = 1;
		state Reference<ReadYourWritesTransaction> tr = self->dataDb->createTransaction();

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
			    oldTenant == newTenant || allTenantNames.contains(oldTenant) || allTenantNames.contains(newTenant);
			// These operation types do not handle rename collisions
			// reject the rename here if it has overlap
			if (checkOverlap && (operationType == OperationType::MANAGEMENT_TRANSACTION ||
			                     operationType == OperationType::MANAGEMENT_DATABASE)) {
				--i;
				continue;
			}
			tenantOverlap = tenantOverlap || checkOverlap;
			tenantRenames[oldTenant] = newTenant;
			allTenantNames.insert(oldTenant);
			allTenantNames.insert(newTenant);
			if (!self->createdTenants.contains(oldTenant)) {
				tenantNotFound = true;
			}
			if (self->createdTenants.contains(newTenant)) {
				tenantExists = true;
			}
		}

		CODE_PROBE(tenantOverlap, "Attempting overlapping tenant renames");

		state Version originalReadVersion = wait(self->getLatestReadVersion(self, operationType));
		loop {
			try {
				wait(renameTenantImpl(self, tr, operationType, tenantRenames, tenantNotFound, tenantExists));
				wait(verifyTenantRenames(self, tenantRenames));
				Versionstamp currentVersionstamp = wait(getLastTenantModification(self, operationType));
				ASSERT_GT(currentVersionstamp.version, originalReadVersion);
				// Check that using the wrong rename API fails depending on whether we are using a metacluster
				ASSERT(self->useMetacluster == (operationType == OperationType::METACLUSTER));
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_not_found) {
					if (unknownResult) {
						wait(verifyTenantRenames(self, tenantRenames));
					} else {
						ASSERT(tenantNotFound);
					}
					return Void();
				} else if (e.code() == error_code_tenant_already_exists) {
					if (unknownResult) {
						wait(verifyTenantRenames(self, tenantRenames));
					} else {
						ASSERT(tenantExists);
					}
					return Void();
				} else if (e.code() == error_code_special_keys_api_failure) {
					ASSERT(tenantOverlap);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER != self->useMetacluster);
					return Void();
				} else if (e.code() == error_code_cluster_no_capacity) {
					// This error should only occur on metacluster due to the multi-stage process.
					// Having temporary tenants may exceed capacity, so we disallow the rename.
					ASSERT(operationType == OperationType::METACLUSTER);
					return Void();
				} else {
					try {
						// In the case of commit_unknown_result, assume we continue retrying
						// until it's successful. Next loop around may throw error because it's
						// already been moved, so account for that and update internal map as needed.
						if (e.code() == error_code_commit_unknown_result) {
							ASSERT(operationType != OperationType::MANAGEMENT_DATABASE &&
							       operationType != OperationType::METACLUSTER);
							unknownResult = true;
						}
						wait(tr->onError(e));
					} catch (Error& e) {
						TraceEvent(SevError, "RenameTenantFailure")
						    .errorUnsuppressed(e)
						    .detail("TenantRenames", describe(tenantRenames));
						ASSERT(false);
					}
				}
			}
		}
	}

	// Changes the configuration of a tenant
	ACTOR static Future<Void> configureTenantImpl(Reference<ReadYourWritesTransaction> tr,
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
		} else if (operationType == OperationType::METACLUSTER) {
			wait(metacluster::configureTenant(self->mvDb,
			                                  tenant,
			                                  configParameters,
			                                  metacluster::IgnoreCapacityLimit(deterministicRandom()->coinflip())));
		} else {
			// We don't have a transaction or database variant of this function
			ASSERT(false);
		}

		return Void();
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

		// True if any tenant group name starts with \xff
		state bool hasSystemTenantGroup = false;

		state bool specialKeysUseInvalidTuple =
		    operationType == OperationType::SPECIAL_KEYS && deterministicRandom()->random01() < 0.1;

		// True if the tenant's tenant group will change, and we would expect an update to be written.
		state bool tenantGroupChanging = false;

		// Generate a tenant group. Sometimes do this at the same time that we include an invalid option to ensure
		// that the configure function still fails
		if (deterministicRandom()->coinflip()) {
			newTenantGroup = self->chooseTenantGroup(true);
			hasSystemTenantGroup = hasSystemTenantGroup || newTenantGroup.orDefault(""_sr).startsWith("\xff"_sr);
			configuration["tenant_group"_sr] = newTenantGroup;
			if (exists && itr->second.tenantGroup != newTenantGroup) {
				tenantGroupChanging = true;
			}
		}
		// Configuring 'assignedCluster' requires reading existing tenant entry. It is relevant only in
		// the case of metacluster.
		state bool assignToDifferentCluster = false;
		if (operationType == OperationType::METACLUSTER && deterministicRandom()->coinflip()) {
			ClusterName newClusterName = "newcluster"_sr;
			if (deterministicRandom()->coinflip()) {
				newClusterName = self->dataClusterName;
			}
			configuration["assigned_cluster"_sr] = newClusterName;
			assignToDifferentCluster = (newClusterName != self->dataClusterName);
		}

		// In the future after we enable tenant movement, this may be
		// state bool configurationChanging = tenangGroupCanging || assignToDifferentCluster.
		state bool configurationChanging = tenantGroupChanging;

		// If true, the options generated may include an unknown option
		state bool hasInvalidOption = false;
		if (configuration.empty() || deterministicRandom()->coinflip()) {
			configuration["invalid_option"_sr] = ""_sr;
			hasInvalidOption = true;
		}

		state Version originalReadVersion = wait(self->getLatestReadVersion(self, operationType));
		loop {
			try {
				wait(configureTenantImpl(tr, tenant, configuration, operationType, specialKeysUseInvalidTuple, self));

				ASSERT(exists);
				ASSERT(!hasInvalidOption);
				ASSERT(!hasSystemTenantGroup);
				ASSERT(!specialKeysUseInvalidTuple);
				ASSERT(!assignToDifferentCluster);
				ASSERT_EQ(operationType == OperationType::METACLUSTER, self->useMetacluster);
				Versionstamp currentVersionstamp = wait(getLastTenantModification(self, operationType));
				if (configurationChanging) {
					ASSERT_GT(currentVersionstamp.version, originalReadVersion);
				}
				if (tenantGroupChanging) {
					ASSERT(configuration.contains("tenant_group"_sr));
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
				}
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
					ASSERT(hasInvalidOption || assignToDifferentCluster);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER != self->useMetacluster);
					return Void();
				} else if (e.code() == error_code_invalid_tenant_group_name) {
					ASSERT(hasSystemTenantGroup);
					return Void();
				}

				try {
					wait(tr->onError(e));
				} catch (Error&) {
					TraceEvent(SevError, "ConfigureTenantFailure")
					    .errorUnsuppressed(error)
					    .detail("TenantName", tenant);
					ASSERT(false);
				}
			}
		}
	}

	// Gets the metadata for a tenant group using the specified operation type
	ACTOR static Future<Optional<TenantGroupEntry>> getTenantGroupImpl(Reference<ReadYourWritesTransaction> tr,
	                                                                   TenantGroupName tenantGroupName,
	                                                                   OperationType operationType,
	                                                                   TenantManagementWorkload* self) {
		state Optional<TenantGroupEntry> entry;
		if (operationType == OperationType::MANAGEMENT_DATABASE) {
			wait(store(entry, TenantAPI::tryGetTenantGroup(self->dataDb.getReference(), tenantGroupName)));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
		           operationType == OperationType::SPECIAL_KEYS) {
			// There is no special-keys interface for reading tenant groups currently, so read them
			// using the TenantAPI.
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			wait(store(entry, TenantAPI::tryGetTenantGroupTransaction(tr, tenantGroupName)));
		} else {
			UNREACHABLE();
		}

		return entry;
	}

	ACTOR static Future<Void> getTenantGroup(TenantManagementWorkload* self) {
		state TenantGroupName tenantGroup = self->chooseTenantGroup(true, false).get();
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		// True if the tenant group should should exist and return a result
		auto itr = self->createdTenantGroups.find(tenantGroup);
		state bool alreadyExists = itr != self->createdTenantGroups.end() &&
		                           !(operationType == OperationType::METACLUSTER && !self->useMetacluster);

		loop {
			try {
				// Get the tenant group metadata and check that it matches our local state
				if (operationType == OperationType::METACLUSTER) {
					state Optional<metacluster::MetaclusterTenantGroupEntry> mEntry =
					    wait(metacluster::tryGetTenantGroup(self->mvDb, tenantGroup));
					ASSERT(alreadyExists == mEntry.present());
				} else {
					state Optional<TenantGroupEntry> entry =
					    wait(getTenantGroupImpl(tr, tenantGroup, operationType, self));
					ASSERT(alreadyExists == entry.present());
				}
				return Void();
			} catch (Error& e) {
				state bool retry = false;
				state Error error = e;

				// Transaction-based operations should retry
				if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				    operationType == OperationType::SPECIAL_KEYS) {
					try {
						wait(tr->onError(e));
						retry = true;
					} catch (Error& e) {
						error = e;
						retry = false;
					}
				}

				if (!retry) {
					TraceEvent(SevError, "GetTenantGroupFailure")
					    .errorUnsuppressed(error)
					    .detail("TenantGroupName", tenantGroup);
					ASSERT(false);
				}
			}
		}
	}

	// Gets a list of tenant groups using the specified operation type
	ACTOR static Future<std::vector<std::pair<TenantGroupName, TenantGroupEntry>>> listTenantGroupsImpl(
	    Reference<ReadYourWritesTransaction> tr,
	    TenantGroupName beginTenantGroup,
	    TenantGroupName endTenantGroup,
	    int limit,
	    OperationType operationType,
	    TenantManagementWorkload* self) {
		state std::vector<std::pair<TenantGroupName, TenantGroupEntry>> tenantGroups;

		if (operationType == OperationType::MANAGEMENT_DATABASE) {
			wait(store(
			    tenantGroups,
			    TenantAPI::listTenantGroups(self->dataDb.getReference(), beginTenantGroup, endTenantGroup, limit)));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
		           operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			wait(store(tenantGroups,
			           TenantAPI::listTenantGroupsTransaction(tr, beginTenantGroup, endTenantGroup, limit)));
		} else {
			UNREACHABLE();
		}

		return tenantGroups;
	}

	template <class TenantMapEntryImpl>
	static void verifyTenantGroupList(TenantManagementWorkload* self,
	                                  std::vector<std::pair<TenantGroupName, TenantMapEntryImpl>> tenantGroups,
	                                  TenantGroupName beginTenantGroup,
	                                  TenantGroupName endTenantGroup,
	                                  int limit) {
		ASSERT(tenantGroups.size() <= limit);

		// Compare the resulting tenant group list to the list we expected to get
		auto localItr = self->createdTenantGroups.lower_bound(beginTenantGroup);
		auto tenantMapItr = tenantGroups.begin();
		for (; tenantMapItr != tenantGroups.end(); ++tenantMapItr, ++localItr) {
			ASSERT(localItr != self->createdTenantGroups.end());
			ASSERT(localItr->first == tenantMapItr->first);
		}

		// Make sure the list terminated at the right spot
		ASSERT(tenantGroups.size() == limit || localItr == self->createdTenantGroups.end() ||
		       localItr->first >= endTenantGroup);
	}

	ACTOR static Future<Void> listTenantGroups(TenantManagementWorkload* self) {
		state TenantGroupName beginTenantGroup = self->chooseTenantGroup(false, false).get();
		state TenantGroupName endTenantGroup = self->chooseTenantGroup(false, false).get();
		state int limit = std::min(CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1,
		                           deterministicRandom()->randomInt(1, self->maxTenants * 2));
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		if (beginTenantGroup > endTenantGroup) {
			std::swap(beginTenantGroup, endTenantGroup);
		}

		loop {
			try {
				// Attempt to read the chosen list of tenant groups
				if (operationType == OperationType::METACLUSTER) {
					state std::vector<std::pair<TenantGroupName, metacluster::MetaclusterTenantGroupEntry>>
					    mTenantGroups =
					        wait(metacluster::listTenantGroups(self->mvDb, beginTenantGroup, endTenantGroup, limit));
					// Attempting to read the list of tenant groups using the metacluster API in a non-metacluster
					// should return nothing in this test
					if (!self->useMetacluster) {
						ASSERT(mTenantGroups.size() == 0);
						return Void();
					}
					verifyTenantGroupList(self, mTenantGroups, beginTenantGroup, endTenantGroup, limit);
				} else {
					state std::vector<std::pair<TenantGroupName, TenantGroupEntry>> tenantGroups =
					    wait(listTenantGroupsImpl(tr, beginTenantGroup, endTenantGroup, limit, operationType, self));
					verifyTenantGroupList(self, tenantGroups, beginTenantGroup, endTenantGroup, limit);
				}
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
					TraceEvent(SevError, "ListTenantGroupFailure")
					    .errorUnsuppressed(error)
					    .detail("BeginTenant", beginTenantGroup)
					    .detail("EndTenant", endTenantGroup);
					ASSERT(false);
				}
			}
		}
	}

	ACTOR static Future<Void> operateOnTenantKey(TenantManagementWorkload* self) {
		if (self->allTestTenants.size() == 0) {
			return Void();
		}

		state Reference<Tenant> tenant = deterministicRandom()->randomChoice(self->allTestTenants);
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb, tenant);
		state TenantName tName = tenant->name.get();
		state bool tenantPresent = false;
		state TenantTestData tData = TenantTestData();

		int mode = deterministicRandom()->randomInt(0, 3);
		state bool readKey = mode == 0 || mode == 2;
		state bool writeKey = mode == 1 || mode == 2;
		state bool clearKey = deterministicRandom()->coinflip();
		state bool lockAware = deterministicRandom()->coinflip();
		state TenantAPI::TenantLockState lockState = TenantAPI::TenantLockState::UNLOCKED;

		auto itr = self->createdTenants.find(tName);
		if (itr != self->createdTenants.end() && itr->second.tenant->id() == tenant->id()) {
			tenantPresent = true;
			tData = itr->second;
			lockState = itr->second.lockState;
		}

		state bool keyPresent = tenantPresent && !tData.empty;
		state bool maybeCommitted = false;

		loop {
			try {
				if (lockAware) {
					tr->setOption(writeKey || deterministicRandom()->coinflip()
					                  ? FDBTransactionOptions::LOCK_AWARE
					                  : FDBTransactionOptions::READ_LOCK_AWARE);
				}
				if (readKey) {
					Optional<Value> val = wait(tr->get(self->keyName));
					if (val.present()) {
						CODE_PROBE(true, "Read tenant key has value");
						ASSERT((keyPresent && val.get() == tName) || (maybeCommitted && writeKey && !clearKey));
					} else {
						CODE_PROBE(true, "Read tenant key is empty");
						ASSERT((tenantPresent && tData.empty) || (maybeCommitted && writeKey && clearKey));
					}

					ASSERT(lockAware || lockState != TenantAPI::TenantLockState::LOCKED);
				}
				if (writeKey) {
					if (clearKey) {
						tr->clear(self->keyName);
					} else {
						tr->set(self->keyName, tName);
					}

					wait(tr->commit());
					CODE_PROBE(clearKey, "Clear tenant key");
					CODE_PROBE(!clearKey, "Set tenant key");

					ASSERT(lockAware || lockState == TenantAPI::TenantLockState::UNLOCKED);
					self->createdTenants[tName].empty = clearKey;
				}

				break;
			} catch (Error& e) {
				state Error err = e;
				if (err.code() == error_code_tenant_not_found) {
					ASSERT(!tenantPresent);
					CODE_PROBE(true, "Attempted to read key from non-existent tenant");
					return Void();
				} else if (err.code() == error_code_tenant_locked) {
					ASSERT(!lockAware);
					if (!writeKey) {
						ASSERT_EQ(lockState, TenantAPI::TenantLockState::LOCKED);
					} else {
						ASSERT_NE(lockState, TenantAPI::TenantLockState::UNLOCKED);
					}

					return Void();
				}

				try {
					maybeCommitted = maybeCommitted || err.code() == error_code_commit_unknown_result;
					CODE_PROBE(maybeCommitted, "Modify tenant key maybe committed");
					wait(tr->onError(err));
				} catch (Error& error) {
					TraceEvent(SevError, "ReadKeyFailure").errorUnsuppressed(error).detail("TenantName", tenant);
					ASSERT(false);
				}
			}
		}
		return Void();
	}

	ACTOR static Future<Void> changeLockStateImpl(Reference<ReadYourWritesTransaction> tr,
	                                              TenantName tenant,
	                                              TenantAPI::TenantLockState lockState,
	                                              UID lockId,
	                                              OperationType operationType,
	                                              TenantManagementWorkload* self) {
		if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			TenantMapEntry entry = wait(TenantAPI::getTenantTransaction(tr, tenant));
			wait(TenantAPI::changeLockState(tr, entry.id, lockState, lockId));
			wait(tr->commit());
		} else if (operationType == OperationType::METACLUSTER) {
			wait(metacluster::changeTenantLockState(self->mvDb, tenant, lockState, lockId));
		} else {
			// We don't have a special keys or database variant of this function
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> changeLockState(TenantManagementWorkload* self) {
		state OperationType operationType =
		    deterministicRandom()->coinflip() ? OperationType::METACLUSTER : OperationType::MANAGEMENT_TRANSACTION;

		state TenantName tenant = self->chooseTenantName(true);
		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		state TenantAPI::TenantLockState lockState = TenantAPI::TenantLockState(deterministicRandom()->randomInt(0, 3));
		state UID lockId = deterministicRandom()->coinflip() || !exists || !itr->second.lockId.present()
		                       ? deterministicRandom()->randomUniqueID()
		                       : itr->second.lockId.get();

		state bool lockStateChanging = exists && itr->second.lockState != lockState;
		state bool legalLockChange =
		    exists && (itr->second.lockId == lockId || (itr->second.lockState == TenantAPI::TenantLockState::UNLOCKED));

		state Version originalReadVersion = wait(self->getLatestReadVersion(self, operationType));
		loop {
			try {
				wait(changeLockStateImpl(tr, tenant, lockState, lockId, operationType, self));

				ASSERT(exists);
				ASSERT(legalLockChange);
				ASSERT_EQ(operationType == OperationType::METACLUSTER, self->useMetacluster);

				auto itr = self->createdTenants.find(tenant);
				ASSERT(itr != self->createdTenants.end());
				itr->second.lockState = lockState;
				if (lockState != TenantAPI::TenantLockState::UNLOCKED) {
					itr->second.lockId = lockId;
				}

				Versionstamp currentVersionstamp = wait(getLastTenantModification(self, operationType));
				if (lockStateChanging) {
					ASSERT_GT(currentVersionstamp.version, originalReadVersion);
				}

				return Void();
			} catch (Error& e) {
				state Error error = e;
				if (e.code() == error_code_tenant_not_found) {
					ASSERT(!exists);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT_NE(operationType == OperationType::METACLUSTER, self->useMetacluster);
					return Void();
				} else if (e.code() == error_code_tenant_locked) {
					ASSERT(!legalLockChange);
					return Void();
				}

				try {
					wait(tr->onError(e));
				} catch (Error&) {
					TraceEvent(SevError, "ConfigureTenantFailure")
					    .errorUnsuppressed(error)
					    .detail("TenantName", tenant);
					ASSERT(false);
				}
			}
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0 || !singleClient) {
			return _start(cx, this);
		} else {
			return Void();
		}
	}

	ACTOR static Future<Void> delayedTrace(int opCode) {
		state double start = now();
		loop {
			wait(delay(1.0));
			TraceEvent("TenantManagementOpNotFinishedAfter")
			    .detail("Duration", now() - start)
			    .detail("OpCode", opCode)
			    .log();
		}
	}

	ACTOR Future<Void> _start(Database cx, TenantManagementWorkload* self) {
		state double start = now();

		// Run a random sequence of tenant management operations for the duration of the test
		while (now() < start + self->testDuration) {
			state int operation = deterministicRandom()->randomInt(0, 10);
			state Future<Void> logger = delayedTrace(operation);
			if (operation == 0) {
				wait(createTenant(self));
			} else if (operation == 1) {
				wait(deleteTenant(self));
			} else if (operation == 2) {
				wait(getTenant(self));
			} else if (operation == 3) {
				wait(listTenants(self));
			} else if (operation == 4) {
				wait(renameTenant(self));
			} else if (operation == 5) {
				wait(configureTenant(self));
			} else if (operation == 6) {
				wait(getTenantGroup(self));
			} else if (operation == 7) {
				wait(listTenantGroups(self));
			} else if (operation == 8) {
				wait(operateOnTenantKey(self));
			} else if (operation == 9) {
				wait(changeLockState(self));
			}
		}

		return Void();
	}

	// Verify that the set of tenants in the database matches our local state
	ACTOR static Future<Void> compareTenants(TenantManagementWorkload* self) {
		state std::map<TenantName, TenantTestData>::iterator localItr = self->createdTenants.begin();
		state std::vector<Future<Void>> checkTenants;
		state TenantName beginTenant = ""_sr.withPrefix(self->localTenantNamePrefix);
		state TenantName endTenant = "\xff\xff"_sr.withPrefix(self->localTenantNamePrefix);

		loop {
			// Read the tenant list from the data cluster.
			state std::vector<std::pair<TenantName, TenantMapEntry>> dataClusterTenants =
			    wait(TenantAPI::listTenantMetadata(self->dataDb.getReference(), beginTenant, endTenant, 1000));

			auto dataItr = dataClusterTenants.begin();

			TenantNameRef lastTenant;
			while (dataItr != dataClusterTenants.end()) {
				ASSERT(localItr != self->createdTenants.end());
				ASSERT(dataItr->first == localItr->first);
				ASSERT(dataItr->second.tenantGroup == localItr->second.tenantGroup);

				checkTenants.push_back(checkTenantContents(self, dataItr->first, localItr->second));
				lastTenant = dataItr->first;

				++localItr;
				++dataItr;
			}

			if (dataClusterTenants.size() < 1000) {
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
			    return TenantMetadata::tenantGroupTenantIndex().getRange(tr,
			                                                             Tuple::makeTuple(tenantGroupRef),
			                                                             Tuple::makeTuple(keyAfter(tenantGroupRef)),
			                                                             expectedCountRef + 1);
		    }));

		ASSERT(tenants.results.size() == expectedCount && !tenants.more);
		return Void();
	}

	// Verify that the set of tenants in the database matches our local state
	ACTOR static Future<Void> compareTenantGroups(TenantManagementWorkload* self) {
		state std::map<TenantName, TenantGroupData>::iterator localItr = self->createdTenantGroups.begin();
		state TenantName beginTenantGroup = ""_sr.withPrefix(self->localTenantGroupNamePrefix);
		state TenantName endTenantGroup = "\xff\xff"_sr.withPrefix(self->localTenantGroupNamePrefix);
		state std::vector<Future<Void>> checkTenantGroups;

		loop {
			// Read the tenant group list from the data cluster.
			state KeyBackedRangeResult<std::pair<TenantGroupName, TenantGroupEntry>> dataClusterTenantGroups;
			TenantName const& beginTenantGroupRef = beginTenantGroup;
			TenantName const& endTenantGroupRef = endTenantGroup;
			wait(
			    store(dataClusterTenantGroups,
			          runTransaction(self->dataDb.getReference(),
			                         [beginTenantGroupRef, endTenantGroupRef](Reference<ReadYourWritesTransaction> tr) {
				                         tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				                         return TenantMetadata::tenantGroupMap().getRange(
				                             tr, beginTenantGroupRef, endTenantGroupRef, 1000);
			                         })));

			auto dataItr = dataClusterTenantGroups.results.begin();

			TenantGroupNameRef lastTenantGroup;
			while (dataItr != dataClusterTenantGroups.results.end()) {
				ASSERT(localItr != self->createdTenantGroups.end());
				ASSERT(dataItr->first == localItr->first);
				lastTenantGroup = dataItr->first;

				checkTenantGroups.push_back(checkTenantGroupTenantCount(
				    self->dataDb.getReference(), dataItr->first, localItr->second.tenantCount));

				++localItr;
				++dataItr;
			}

			if (!dataClusterTenantGroups.more) {
				break;
			} else {
				beginTenantGroup = keyAfter(lastTenantGroup);
			}
		}

		ASSERT(localItr == self->createdTenantGroups.end());
		wait(waitForAll(checkTenantGroups));

		return Void();
	}

	// Check that the tenant tombstones are properly cleaned up
	ACTOR static Future<Void> checkTombstoneCleanup(TenantManagementWorkload* self) {
		state Reference<ReadYourWritesTransaction> tr = self->dataDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

				Optional<TenantTombstoneCleanupData> tombstoneCleanupData =
				    wait(TenantMetadata::tombstoneCleanupData().get(tr));

				if (self->oldestDeletionVersion != 0) {
					CODE_PROBE(true, "Tenant tombstone oldest deletion version non-zero");
					ASSERT(tombstoneCleanupData.present());
					if (self->newestDeletionVersion - self->oldestDeletionVersion >
					    CLIENT_KNOBS->TENANT_TOMBSTONE_CLEANUP_INTERVAL * CLIENT_KNOBS->VERSIONS_PER_SECOND) {
						CODE_PROBE(tombstoneCleanupData.get().tombstonesErasedThrough > 0, "Tenant tombstones erased");
						ASSERT(tombstoneCleanupData.get().tombstonesErasedThrough >= 0);
					}
				}
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0 || !singleClient) {
			return _check(cx, this);
		} else {
			return true;
		}
	}

	ACTOR static Future<bool> _check(Database cx, TenantManagementWorkload* self) {
		wait(checkNonTenantKey(self));
		wait(compareTenants(self) && compareTenantGroups(self));

		if (self->useMetacluster) {
			// The metacluster consistency check runs the tenant consistency check for each cluster
			state metacluster::util::MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
			    self->mvDb, metacluster::util::AllowPartialMetaclusterOperations::False);
			wait(metaclusterConsistencyCheck.run());
			wait(checkTombstoneCleanup(self));
		} else {
			state metacluster::util::TenantConsistencyCheck<DatabaseContext, StandardTenantTypes>
			    tenantConsistencyCheck(self->dataDb.getReference(), &TenantMetadata::instance());
			wait(tenantConsistencyCheck.run());
		}

		return true;
	}

	ACTOR static Future<Void> checkNonTenantKey(const TenantManagementWorkload* self) {
		if (!self->hasNoTenantKey)
			return Void();

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
		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantManagementWorkload> TenantManagementWorkload;
