/*
 * GenericManagementAPI.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_GENERIC_MANAGEMENT_API_ACTOR_G_H)
#define FDBCLIENT_GENERIC_MANAGEMENT_API_ACTOR_G_H
#include "fdbclient/GenericManagementAPI.actor.g.h"
#elif !defined(FDBCLIENT_GENERIC_MANAGEMENT_API_ACTOR_H)
#define FDBCLIENT_GENERIC_MANAGEMENT_API_ACTOR_H

/* This file defines "management" interfaces that have been templated to support both IClientAPI
and Native version of databases, transactions, etc., and includes functions for performing cluster
managment tasks. It isn't exposed to C clients or anywhere outside our code base and doesn't need
to be versioned. It doesn't do anything you can't do with the standard API and some knowledge of
the contents of the system key space.
*/

#include <string>
#include <map>
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/Status.h"
#include "fdbclient/Subspace.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/Status.h"
#include "fdbclient/SystemData.h"
#include "flow/actorcompiler.h" // has to be last include

// ConfigurationResult enumerates normal outcomes of changeConfig() and various error
// conditions specific to it.  changeConfig may also throw an Error to report other problems.
enum class ConfigurationResult {
	NO_OPTIONS_PROVIDED,
	CONFLICTING_OPTIONS,
	UNKNOWN_OPTION,
	INCOMPLETE_CONFIGURATION,
	INVALID_CONFIGURATION,
	STORAGE_MIGRATION_DISABLED,
	DATABASE_ALREADY_CREATED,
	DATABASE_CREATED,
	DATABASE_UNAVAILABLE,
	STORAGE_IN_UNKNOWN_DCID,
	REGION_NOT_FULLY_REPLICATED,
	MULTIPLE_ACTIVE_REGIONS,
	REGIONS_CHANGED,
	NOT_ENOUGH_WORKERS,
	REGION_REPLICATION_MISMATCH,
	DCID_MISSING,
	LOCKED_NOT_NEW,
	SUCCESS_WARN_PPW_GRADUAL,
	SUCCESS,
};

enum class CoordinatorsResult {
	INVALID_NETWORK_ADDRESSES,
	SAME_NETWORK_ADDRESSES,
	NOT_COORDINATORS, // FIXME: not detected
	DATABASE_UNREACHABLE, // FIXME: not detected
	BAD_DATABASE_STATE,
	COORDINATOR_UNREACHABLE,
	NOT_ENOUGH_MACHINES,
	SUCCESS
};

struct ConfigureAutoResult {
	std::map<NetworkAddress, ProcessClass> address_class;
	int32_t processes;
	int32_t machines;

	std::string old_replication;
	int32_t old_commit_proxies;
	int32_t old_grv_proxies;
	int32_t old_resolvers;
	int32_t old_logs;
	int32_t old_processes_with_transaction;
	int32_t old_machines_with_transaction;

	std::string auto_replication;
	int32_t auto_commit_proxies;
	int32_t auto_grv_proxies;
	int32_t auto_resolvers;
	int32_t auto_logs;
	int32_t auto_processes_with_transaction;
	int32_t auto_machines_with_transaction;

	int32_t desired_commit_proxies;
	int32_t desired_grv_proxies;
	int32_t desired_resolvers;
	int32_t desired_logs;

	ConfigureAutoResult()
	  : processes(-1), machines(-1), old_commit_proxies(-1), old_grv_proxies(-1), old_resolvers(-1), old_logs(-1),
	    old_processes_with_transaction(-1), old_machines_with_transaction(-1), auto_commit_proxies(-1),
	    auto_grv_proxies(-1), auto_resolvers(-1), auto_logs(-1), auto_processes_with_transaction(-1),
	    auto_machines_with_transaction(-1), desired_commit_proxies(-1), desired_grv_proxies(-1), desired_resolvers(-1),
	    desired_logs(-1) {}

	bool isValid() const { return processes != -1; }
};

ConfigurationResult buildConfiguration(
    std::vector<StringRef> const& modeTokens,
    std::map<std::string, std::string>& outConf); // Accepts a vector of configuration tokens
ConfigurationResult buildConfiguration(
    std::string const& modeString,
    std::map<std::string, std::string>& outConf); // Accepts tokens separated by spaces in a single string

bool isCompleteConfiguration(std::map<std::string, std::string> const& options);

ConfigureAutoResult parseConfig(StatusObject const& status);

// Management API written in template code to support both IClientAPI and NativeAPI
namespace ManagementAPI {

ACTOR template <class DB>
Future<Void> changeCachedRange(Reference<DB> db, KeyRangeRef range, bool add) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state KeyRange sysRange = KeyRangeRef(storageCacheKey(range.begin), storageCacheKey(range.end));
	state KeyRange sysRangeClear = KeyRangeRef(storageCacheKey(range.begin), keyAfter(storageCacheKey(range.end)));
	state KeyRange privateRange = KeyRangeRef(cacheKeysKey(0, range.begin), cacheKeysKey(0, range.end));
	state Value trueValue = storageCacheValue(std::vector<uint16_t>{ 0 });
	state Value falseValue = storageCacheValue(std::vector<uint16_t>{});
	loop {
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			tr->clear(sysRangeClear);
			tr->clear(privateRange);
			tr->addReadConflictRange(privateRange);
			// hold the returned standalone object's memory
			state typename DB::TransactionT::template FutureT<RangeResult> previousFuture =
			    tr->getRange(KeyRangeRef(storageCachePrefix, sysRange.begin), 1, Snapshot::False, Reverse::True);
			RangeResult previous = wait(safeThreadFutureToFuture(previousFuture));
			bool prevIsCached = false;
			if (!previous.empty()) {
				std::vector<uint16_t> prevVal;
				decodeStorageCacheValue(previous[0].value, prevVal);
				prevIsCached = !prevVal.empty();
			}
			if (prevIsCached && !add) {
				// we need to uncache from here
				tr->set(sysRange.begin, falseValue);
				tr->set(privateRange.begin, serverKeysFalse);
			} else if (!prevIsCached && add) {
				// we need to cache, starting from here
				tr->set(sysRange.begin, trueValue);
				tr->set(privateRange.begin, serverKeysTrue);
			}
			// hold the returned standalone object's memory
			state typename DB::TransactionT::template FutureT<RangeResult> afterFuture =
			    tr->getRange(KeyRangeRef(sysRange.end, storageCacheKeys.end), 1, Snapshot::False, Reverse::False);
			RangeResult after = wait(safeThreadFutureToFuture(afterFuture));
			bool afterIsCached = false;
			if (!after.empty()) {
				std::vector<uint16_t> afterVal;
				decodeStorageCacheValue(after[0].value, afterVal);
				afterIsCached = afterVal.empty();
			}
			if (afterIsCached && !add) {
				tr->set(sysRange.end, trueValue);
				tr->set(privateRange.end, serverKeysTrue);
			} else if (!afterIsCached && add) {
				tr->set(sysRange.end, falseValue);
				tr->set(privateRange.end, serverKeysFalse);
			}
			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			state Error err = e;
			wait(safeThreadFutureToFuture(tr->onError(e)));
			TraceEvent(SevDebug, "ChangeCachedRangeError").error(err);
		}
	}
}

template <class DB>
Future<Void> addCachedRange(Reference<DB> db, KeyRangeRef range) {
	return changeCachedRange(db, range, true);
}

template <class DB>
Future<Void> removeCachedRange(Reference<DB> db, KeyRangeRef range) {
	return changeCachedRange(db, range, false);
}

ACTOR template <class Tr>
Future<std::vector<ProcessData>> getWorkers(Reference<Tr> tr,
                                            typename Tr::template FutureT<RangeResult> processClassesF,
                                            typename Tr::template FutureT<RangeResult> processDataF) {
	// processClassesF and processDataF are used to hold standalone memory
	processClassesF = tr->getRange(processClassKeys, CLIENT_KNOBS->TOO_MANY);
	processDataF = tr->getRange(workerListKeys, CLIENT_KNOBS->TOO_MANY);
	state Future<RangeResult> processClasses = safeThreadFutureToFuture(processClassesF);
	state Future<RangeResult> processData = safeThreadFutureToFuture(processDataF);

	wait(success(processClasses) && success(processData));
	ASSERT(!processClasses.get().more && processClasses.get().size() < CLIENT_KNOBS->TOO_MANY);
	ASSERT(!processData.get().more && processData.get().size() < CLIENT_KNOBS->TOO_MANY);

	std::map<Optional<Standalone<StringRef>>, ProcessClass> id_class;
	for (int i = 0; i < processClasses.get().size(); i++) {
		id_class[decodeProcessClassKey(processClasses.get()[i].key)] =
		    decodeProcessClassValue(processClasses.get()[i].value);
	}

	std::vector<ProcessData> results;

	for (int i = 0; i < processData.get().size(); i++) {
		ProcessData data = decodeWorkerListValue(processData.get()[i].value);
		ProcessClass processClass = id_class[data.locality.processId()];

		if (processClass.classSource() == ProcessClass::DBSource ||
		    data.processClass.classType() == ProcessClass::UnsetClass)
			data.processClass = processClass;

		if (data.processClass.classType() != ProcessClass::TesterClass)
			results.push_back(data);
	}

	return results;
}

// All versions of changeConfig apply the given set of configuration tokens to the database, and return a
// ConfigurationResult (or error).

// Accepts a full configuration in key/value format (from buildConfiguration)
ACTOR template <class DB>
Future<ConfigurationResult> changeConfig(Reference<DB> db, std::map<std::string, std::string> m, bool force) {
	state StringRef initIdKey = LiteralStringRef("\xff/init_id");
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	if (!m.size()) {
		return ConfigurationResult::NO_OPTIONS_PROVIDED;
	}

	// make sure we have essential configuration options
	std::string initKey = configKeysPrefix.toString() + "initialized";
	state bool creating = m.count(initKey) != 0;
	state Optional<UID> locked;
	{
		auto iter = m.find(databaseLockedKey.toString());
		if (iter != m.end()) {
			if (!creating) {
				return ConfigurationResult::LOCKED_NOT_NEW;
			}
			locked = UID::fromString(iter->second);
			m.erase(iter);
		}
	}
	if (creating) {
		m[initIdKey.toString()] = deterministicRandom()->randomUniqueID().toString();
		if (!isCompleteConfiguration(m)) {
			return ConfigurationResult::INCOMPLETE_CONFIGURATION;
		}
	}

	state Future<Void> tooLong = delay(60);
	state Key versionKey = BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned());
	state bool oldReplicationUsesDcId = false;
	state bool warnPPWGradual = false;
	state bool warnChangeStorageNoMigrate = false;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);

			if (!creating && !force) {
				state typename DB::TransactionT::template FutureT<RangeResult> fConfigF =
				    tr->getRange(configKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<RangeResult> fConfig = safeThreadFutureToFuture(fConfigF);
				state typename DB::TransactionT::template FutureT<RangeResult> processClassesF;
				state typename DB::TransactionT::template FutureT<RangeResult> processDataF;
				state Future<std::vector<ProcessData>> fWorkers = getWorkers(tr, processClassesF, processDataF);
				wait(success(fConfig) || tooLong);

				if (!fConfig.isReady()) {
					return ConfigurationResult::DATABASE_UNAVAILABLE;
				}

				if (fConfig.isReady()) {
					ASSERT(fConfig.get().size() < CLIENT_KNOBS->TOO_MANY);
					state DatabaseConfiguration oldConfig;
					oldConfig.fromKeyValues((VectorRef<KeyValueRef>)fConfig.get());
					state DatabaseConfiguration newConfig = oldConfig;
					for (auto kv : m) {
						newConfig.set(kv.first, kv.second);
					}
					if (!newConfig.isValid()) {
						return ConfigurationResult::INVALID_CONFIGURATION;
					}

					if (newConfig.tLogPolicy->attributeKeys().count("dcid") && newConfig.regions.size() > 0) {
						return ConfigurationResult::REGION_REPLICATION_MISMATCH;
					}

					oldReplicationUsesDcId =
					    oldReplicationUsesDcId || oldConfig.tLogPolicy->attributeKeys().count("dcid");

					if (oldConfig.usableRegions != newConfig.usableRegions) {
						// cannot change region configuration
						std::map<Key, int32_t> dcId_priority;
						for (auto& it : newConfig.regions) {
							dcId_priority[it.dcId] = it.priority;
						}
						for (auto& it : oldConfig.regions) {
							if (!dcId_priority.count(it.dcId) || dcId_priority[it.dcId] != it.priority) {
								return ConfigurationResult::REGIONS_CHANGED;
							}
						}

						// must only have one region with priority >= 0
						int activeRegionCount = 0;
						for (auto& it : newConfig.regions) {
							if (it.priority >= 0) {
								activeRegionCount++;
							}
						}
						if (activeRegionCount > 1) {
							return ConfigurationResult::MULTIPLE_ACTIVE_REGIONS;
						}
					}

					state typename DB::TransactionT::template FutureT<RangeResult> fServerListF =
					    tr->getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
					state Future<RangeResult> fServerList =
					    (newConfig.regions.size()) ? safeThreadFutureToFuture(fServerListF) : Future<RangeResult>();

					if (newConfig.usableRegions == 2) {
						if (oldReplicationUsesDcId) {
							state typename DB::TransactionT::template FutureT<RangeResult> fLocalityListF =
							    tr->getRange(tagLocalityListKeys, CLIENT_KNOBS->TOO_MANY);
							state Future<RangeResult> fLocalityList = safeThreadFutureToFuture(fLocalityListF);
							wait(success(fLocalityList) || tooLong);
							if (!fLocalityList.isReady()) {
								return ConfigurationResult::DATABASE_UNAVAILABLE;
							}
							RangeResult localityList = fLocalityList.get();
							ASSERT(!localityList.more && localityList.size() < CLIENT_KNOBS->TOO_MANY);

							std::set<Key> localityDcIds;
							for (auto& s : localityList) {
								auto dc = decodeTagLocalityListKey(s.key);
								if (dc.present()) {
									localityDcIds.insert(dc.get());
								}
							}

							for (auto& it : newConfig.regions) {
								if (localityDcIds.count(it.dcId) == 0) {
									return ConfigurationResult::DCID_MISSING;
								}
							}
						} else {
							// all regions with priority >= 0 must be fully replicated
							state std::vector<typename DB::TransactionT::template FutureT<Optional<Value>>>
							    replicasFuturesF;
							state std::vector<Future<Optional<Value>>> replicasFutures;
							for (auto& it : newConfig.regions) {
								if (it.priority >= 0) {
									replicasFuturesF.push_back(tr->get(datacenterReplicasKeyFor(it.dcId)));
									replicasFutures.push_back(safeThreadFutureToFuture(replicasFuturesF.back()));
								}
							}
							wait(waitForAll(replicasFutures) || tooLong);

							for (auto& it : replicasFutures) {
								if (!it.isReady()) {
									return ConfigurationResult::DATABASE_UNAVAILABLE;
								}
								if (!it.get().present()) {
									return ConfigurationResult::REGION_NOT_FULLY_REPLICATED;
								}
							}
						}
					}

					if (newConfig.regions.size()) {
						// all storage servers must be in one of the regions
						wait(success(fServerList) || tooLong);
						if (!fServerList.isReady()) {
							return ConfigurationResult::DATABASE_UNAVAILABLE;
						}
						RangeResult serverList = fServerList.get();
						ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

						std::set<Key> newDcIds;
						for (auto& it : newConfig.regions) {
							newDcIds.insert(it.dcId);
						}
						std::set<Optional<Key>> missingDcIds;
						for (auto& s : serverList) {
							auto ssi = decodeServerListValue(s.value);
							if (!ssi.locality.dcId().present() || !newDcIds.count(ssi.locality.dcId().get())) {
								missingDcIds.insert(ssi.locality.dcId());
							}
						}
						if (missingDcIds.size() > (oldReplicationUsesDcId ? 1 : 0)) {
							return ConfigurationResult::STORAGE_IN_UNKNOWN_DCID;
						}
					}

					wait(success(fWorkers) || tooLong);
					if (!fWorkers.isReady()) {
						return ConfigurationResult::DATABASE_UNAVAILABLE;
					}

					if (newConfig.regions.size()) {
						std::map<Optional<Key>, std::set<Optional<Key>>> dcId_zoneIds;
						for (auto& it : fWorkers.get()) {
							if (it.processClass.machineClassFitness(ProcessClass::Storage) <= ProcessClass::WorstFit) {
								dcId_zoneIds[it.locality.dcId()].insert(it.locality.zoneId());
							}
						}
						for (auto& region : newConfig.regions) {
							if (dcId_zoneIds[region.dcId].size() <
							    std::max(newConfig.storageTeamSize, newConfig.tLogReplicationFactor)) {
								return ConfigurationResult::NOT_ENOUGH_WORKERS;
							}
							if (region.satelliteTLogReplicationFactor > 0 && region.priority >= 0) {
								int totalSatelliteProcesses = 0;
								for (auto& sat : region.satellites) {
									totalSatelliteProcesses += dcId_zoneIds[sat.dcId].size();
								}
								if (totalSatelliteProcesses < region.satelliteTLogReplicationFactor) {
									return ConfigurationResult::NOT_ENOUGH_WORKERS;
								}
							}
						}
					} else {
						std::set<Optional<Key>> zoneIds;
						for (auto& it : fWorkers.get()) {
							if (it.processClass.machineClassFitness(ProcessClass::Storage) <= ProcessClass::WorstFit) {
								zoneIds.insert(it.locality.zoneId());
							}
						}
						if (zoneIds.size() < std::max(newConfig.storageTeamSize, newConfig.tLogReplicationFactor)) {
							return ConfigurationResult::NOT_ENOUGH_WORKERS;
						}
					}

					if (newConfig.storageServerStoreType != oldConfig.storageServerStoreType &&
					    newConfig.storageMigrationType == StorageMigrationType::DISABLED) {
						return ConfigurationResult::STORAGE_MIGRATION_DISABLED;
					} else if (newConfig.storageMigrationType == StorageMigrationType::GRADUAL &&
					           newConfig.perpetualStorageWiggleSpeed == 0) {
						warnPPWGradual = true;
					}
				}
			}
			if (creating) {
				tr->setOption(FDBTransactionOptions::INITIALIZE_NEW_DATABASE);
				tr->addReadConflictRange(singleKeyRange(initIdKey));
			} else if (m.size()) {
				// might be used in an emergency transaction, so make sure it is retry-self-conflicting and
				// CAUSAL_WRITE_RISKY
				tr->setOption(FDBTransactionOptions::CAUSAL_WRITE_RISKY);
				tr->addReadConflictRange(singleKeyRange(m.begin()->first));
			}

			if (locked.present()) {
				ASSERT(creating);
				tr->atomicOp(databaseLockedKey,
				             BinaryWriter::toValue(locked.get(), Unversioned())
				                 .withPrefix(LiteralStringRef("0123456789"))
				                 .withSuffix(LiteralStringRef("\x00\x00\x00\x00")),
				             MutationRef::SetVersionstampedValue);
			}

			for (auto i = m.begin(); i != m.end(); ++i) {
				tr->set(StringRef(i->first), StringRef(i->second));
			}

			tr->addReadConflictRange(singleKeyRange(moveKeysLockOwnerKey));
			tr->set(moveKeysLockOwnerKey, versionKey);

			wait(safeThreadFutureToFuture(tr->commit()));
			break;
		} catch (Error& e) {
			state Error e1(e);
			if ((e.code() == error_code_not_committed || e.code() == error_code_transaction_too_old) && creating) {
				// The database now exists.  Determine whether we created it or it was already existing/created by
				// someone else.  The latter is an error.
				tr->reset();
				loop {
					try {
						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);
						tr->setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);

						state typename DB::TransactionT::template FutureT<Optional<Value>> vF = tr->get(initIdKey);
						Optional<Value> v = wait(safeThreadFutureToFuture(vF));
						if (v != m[initIdKey.toString()])
							return ConfigurationResult::DATABASE_ALREADY_CREATED;
						else
							return ConfigurationResult::DATABASE_CREATED;
					} catch (Error& e2) {
						wait(safeThreadFutureToFuture(tr->onError(e2)));
					}
				}
			}
			wait(safeThreadFutureToFuture(tr->onError(e1)));
		}
	}

	if (warnPPWGradual) {
		return ConfigurationResult::SUCCESS_WARN_PPW_GRADUAL;
	} else {
		return ConfigurationResult::SUCCESS;
	}
}

ACTOR template <class DB>
Future<ConfigurationResult> autoConfig(Reference<DB> db, ConfigureAutoResult conf) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state Key versionKey = BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned());

	if (!conf.address_class.size())
		return ConfigurationResult::INCOMPLETE_CONFIGURATION; // FIXME: correct return type

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);

			state typename DB::TransactionT::template FutureT<RangeResult> processClassesF;
			state typename DB::TransactionT::template FutureT<RangeResult> processDataF;
			std::vector<ProcessData> workers = wait(getWorkers(tr, processClassesF, processDataF));
			std::map<NetworkAddress, Optional<Standalone<StringRef>>> address_processId;
			for (auto& w : workers) {
				address_processId[w.address] = w.locality.processId();
			}

			for (auto& it : conf.address_class) {
				if (it.second.classSource() == ProcessClass::CommandLineSource) {
					tr->clear(processClassKeyFor(address_processId[it.first].get()));
				} else {
					tr->set(processClassKeyFor(address_processId[it.first].get()), processClassValue(it.second));
				}
			}

			if (conf.address_class.size())
				tr->set(processClassChangeKey, deterministicRandom()->randomUniqueID().toString());

			if (conf.auto_logs != conf.old_logs)
				tr->set(configKeysPrefix.toString() + "auto_logs", format("%d", conf.auto_logs));

			if (conf.auto_commit_proxies != conf.old_commit_proxies)
				tr->set(configKeysPrefix.toString() + "auto_commit_proxies", format("%d", conf.auto_commit_proxies));

			if (conf.auto_grv_proxies != conf.old_grv_proxies)
				tr->set(configKeysPrefix.toString() + "auto_grv_proxies", format("%d", conf.auto_grv_proxies));

			if (conf.auto_resolvers != conf.old_resolvers)
				tr->set(configKeysPrefix.toString() + "auto_resolvers", format("%d", conf.auto_resolvers));

			if (conf.auto_replication != conf.old_replication) {
				std::vector<StringRef> modes;
				modes.push_back(conf.auto_replication);
				std::map<std::string, std::string> m;
				auto r = buildConfiguration(modes, m);
				if (r != ConfigurationResult::SUCCESS)
					return r;

				for (auto& kv : m)
					tr->set(kv.first, kv.second);
			}

			tr->addReadConflictRange(singleKeyRange(moveKeysLockOwnerKey));
			tr->set(moveKeysLockOwnerKey, versionKey);

			wait(safeThreadFutureToFuture(tr->commit()));
			return ConfigurationResult::SUCCESS;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// Accepts tokens separated by spaces in a single string
template <class DB>
Future<ConfigurationResult> changeConfig(Reference<DB> db, std::string const& modes, bool force) {
	TraceEvent("ChangeConfig").detail("Mode", modes);
	std::map<std::string, std::string> m;
	auto r = buildConfiguration(modes, m);
	if (r != ConfigurationResult::SUCCESS)
		return r;
	return changeConfig(db, m, force);
}

// Accepts a vector of configuration tokens
template <class DB>
Future<ConfigurationResult> changeConfig(Reference<DB> db,
                                         std::vector<StringRef> const& modes,
                                         Optional<ConfigureAutoResult> const& conf,
                                         bool force) {
	if (modes.size() && modes[0] == LiteralStringRef("auto") && conf.present()) {
		return autoConfig(db, conf.get());
	}

	std::map<std::string, std::string> m;
	auto r = buildConfiguration(modes, m);
	if (r != ConfigurationResult::SUCCESS)
		return r;
	return changeConfig(db, m, force);
}

// return the corresponding error message for the CoordinatorsResult
// used by special keys and fdbcli
std::string generateErrorMessage(const CoordinatorsResult& res);

ACTOR template <class Transaction>
Future<Optional<TenantMapEntry>> tryGetTenantTransaction(Transaction tr, TenantName name) {
	state Key tenantMapKey = name.withPrefix(tenantMapPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);

	Optional<Value> val = wait(safeThreadFutureToFuture(tr->get(tenantMapKey)));
	return val.map<TenantMapEntry>([](Optional<Value> v) { return decodeTenantEntry(v.get()); });
}

ACTOR template <class DB>
Future<Optional<TenantMapEntry>> tryGetTenant(Reference<DB> db, TenantName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, name));
			return entry;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<TenantMapEntry> getTenantTransaction(Transaction tr, TenantName name) {
	Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, name));
	if (!entry.present()) {
		throw tenant_not_found();
	}

	return entry.get();
}

ACTOR template <class DB>
Future<TenantMapEntry> getTenant(Reference<DB> db, TenantName name) {
	Optional<TenantMapEntry> entry = wait(tryGetTenant(db, name));
	if (!entry.present()) {
		throw tenant_not_found();
	}

	return entry.get();
}

// Creates a tenant with the given name. If the tenant already exists, an empty optional will be returned.
ACTOR template <class Transaction>
Future<Optional<TenantMapEntry>> createTenantTransaction(Transaction tr, TenantNameRef name) {
	state Key tenantMapKey = name.withPrefix(tenantMapPrefix);

	if (name.startsWith("\xff"_sr)) {
		throw invalid_tenant_name();
	}

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Future<Optional<TenantMapEntry>> tenantEntryFuture = tryGetTenantTransaction(tr, name);
	state Future<Optional<Value>> tenantDataPrefixFuture = safeThreadFutureToFuture(tr->get(tenantDataPrefixKey));
	state Future<Optional<Value>> lastIdFuture = safeThreadFutureToFuture(tr->get(tenantLastIdKey));

	Optional<Value> tenantMode = wait(safeThreadFutureToFuture(tr->get(configKeysPrefix.withSuffix("tenant_mode"_sr))));

	if (!tenantMode.present() || tenantMode.get() == StringRef(format("%d", TenantMode::DISABLED))) {
		throw tenants_disabled();
	}

	Optional<TenantMapEntry> tenantEntry = wait(tenantEntryFuture);
	if (tenantEntry.present()) {
		return Optional<TenantMapEntry>();
	}

	state Optional<Value> lastIdVal = wait(lastIdFuture);
	Optional<Value> tenantDataPrefix = wait(tenantDataPrefixFuture);

	state TenantMapEntry newTenant(lastIdVal.present() ? TenantMapEntry::prefixToId(lastIdVal.get()) + 1 : 0,
	                               tenantDataPrefix.present() ? (KeyRef)tenantDataPrefix.get() : ""_sr);

	RangeResult contents = wait(safeThreadFutureToFuture(tr->getRange(prefixRange(newTenant.prefix), 1)));
	if (!contents.empty()) {
		throw tenant_prefix_allocator_conflict();
	}

	tr->set(tenantLastIdKey, TenantMapEntry::idToPrefix(newTenant.id));
	tr->set(tenantMapKey, encodeTenantEntry(newTenant));

	return newTenant;
}

ACTOR template <class DB>
Future<Void> createTenant(Reference<DB> db, TenantName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool firstTry = true;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			if (firstTry) {
				Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, name));
				if (entry.present()) {
					throw tenant_already_exists();
				}

				firstTry = false;
			}

			state Optional<TenantMapEntry> newTenant = wait(createTenantTransaction(tr, name));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			wait(safeThreadFutureToFuture(tr->commit()));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			TraceEvent("CreatedTenant")
			    .detail("Tenant", name)
			    .detail("TenantId", newTenant.present() ? newTenant.get().id : -1)
			    .detail("Prefix", newTenant.present() ? (StringRef)newTenant.get().prefix : "Unknown"_sr)
			    .detail("Version", tr->getCommittedVersion());

			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<Void> deleteTenantTransaction(Transaction tr, TenantNameRef name) {
	state Key tenantMapKey = name.withPrefix(tenantMapPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Optional<TenantMapEntry> tenantEntry = wait(tryGetTenantTransaction(tr, name));
	if (!tenantEntry.present()) {
		return Void();
	}

	RangeResult contents = wait(safeThreadFutureToFuture(tr->getRange(prefixRange(tenantEntry.get().prefix), 1)));
	if (!contents.empty()) {
		throw tenant_not_empty();
	}

	tr->clear(tenantMapKey);

	return Void();
}

ACTOR template <class DB>
Future<Void> deleteTenant(Reference<DB> db, TenantName name) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state bool firstTry = true;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			if (firstTry) {
				Optional<TenantMapEntry> entry = wait(tryGetTenantTransaction(tr, name));
				if (!entry.present()) {
					throw tenant_not_found();
				}

				firstTry = false;
			}

			wait(deleteTenantTransaction(tr, name));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			wait(safeThreadFutureToFuture(tr->commit()));

			if (BUGGIFY) {
				throw commit_unknown_result();
			}

			TraceEvent("DeletedTenant").detail("Tenant", name).detail("Version", tr->getCommittedVersion());
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction>
Future<std::map<TenantName, TenantMapEntry>> listTenantsTransaction(Transaction tr,
                                                                    TenantNameRef begin,
                                                                    TenantNameRef end,
                                                                    int limit) {
	state KeyRange range = KeyRangeRef(begin, end).withPrefix(tenantMapPrefix);

	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);

	RangeResult results = wait(safeThreadFutureToFuture(
	    tr->getRange(firstGreaterOrEqual(range.begin), firstGreaterOrEqual(range.end), limit)));

	std::map<TenantName, TenantMapEntry> tenants;
	for (auto kv : results) {
		tenants[kv.key.removePrefix(tenantMapPrefix)] = decodeTenantEntry(kv.value);
	}

	return tenants;
}

ACTOR template <class DB>
Future<std::map<TenantName, TenantMapEntry>> listTenants(Reference<DB> db,
                                                         TenantName begin,
                                                         TenantName end,
                                                         int limit) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			std::map<TenantName, TenantMapEntry> tenants = wait(listTenantsTransaction(tr, begin, end, limit));
			return tenants;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}
} // namespace ManagementAPI

#include "flow/unactorcompiler.h"
#endif
