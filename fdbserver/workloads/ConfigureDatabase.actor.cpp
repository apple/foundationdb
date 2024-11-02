/*
 * ConfigureDatabase.actor.cpp
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

#include <algorithm>

#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/SimulatedCluster.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

static const char* storageMigrationTypes[] = { "perpetual_storage_wiggle=0 storage_migration_type=aggressive",
	                                           "perpetual_storage_wiggle=1",
	                                           "perpetual_storage_wiggle=1 storage_migration_type=gradual",
	                                           "storage_migration_type=aggressive" };
static const char* logTypes[] = { "log_engine:=1",
	                              "log_engine:=2",
	                              "log_spill:=1",
	                              "log_spill:=2",
	                              "log_version:=2",
	                              "log_version:=3",
	                              "log_version:=4",
	                              "log_version:=5",
	                              "log_version:=6",
	                              // downgrade incompatible log version
	                              "log_version:=7" };
static const char* redundancies[] = { "single", "double", "triple" };
static const char* backupTypes[] = { "backup_worker_enabled:=0", "backup_worker_enabled:=1" };

std::string generateRegions() {
	std::string result;
	if (g_simulator->physicalDatacenters == 1 ||
	    (g_simulator->physicalDatacenters == 2 && deterministicRandom()->random01() < 0.25) ||
	    g_simulator->physicalDatacenters == 3) {
		return " usable_regions=1 regions=\"\"";
	}

	if (deterministicRandom()->random01() < 0.25) {
		return format(" usable_regions=%d", deterministicRandom()->randomInt(1, 3));
	}

	int primaryPriority = 1;
	int remotePriority = -1;
	double priorityType = deterministicRandom()->random01();
	if (priorityType < 0.1) {
		primaryPriority = -1;
		remotePriority = 1;
	} else if (priorityType < 0.2) {
		remotePriority = 1;
		primaryPriority = 1;
	}

	StatusObject primaryObj;
	StatusObject primaryDcObj;
	primaryDcObj["id"] = "0";
	primaryDcObj["priority"] = primaryPriority;
	StatusArray primaryDcArr;
	primaryDcArr.push_back(primaryDcObj);

	StatusObject remoteObj;
	StatusObject remoteDcObj;
	remoteDcObj["id"] = "1";
	remoteDcObj["priority"] = remotePriority;
	StatusArray remoteDcArr;
	remoteDcArr.push_back(remoteDcObj);

	if (g_simulator->physicalDatacenters > 3 && deterministicRandom()->random01() < 0.5) {
		StatusObject primarySatelliteObj;
		primarySatelliteObj["id"] = "2";
		primarySatelliteObj["priority"] = 1;
		primarySatelliteObj["satellite"] = 1;
		if (deterministicRandom()->random01() < 0.25)
			primarySatelliteObj["satellite_logs"] = deterministicRandom()->randomInt(1, 7);
		primaryDcArr.push_back(primarySatelliteObj);

		StatusObject remoteSatelliteObj;
		remoteSatelliteObj["id"] = "3";
		remoteSatelliteObj["priority"] = 1;
		remoteSatelliteObj["satellite"] = 1;
		if (deterministicRandom()->random01() < 0.25)
			remoteSatelliteObj["satellite_logs"] = deterministicRandom()->randomInt(1, 7);
		remoteDcArr.push_back(remoteSatelliteObj);

		if (g_simulator->physicalDatacenters > 5 && deterministicRandom()->random01() < 0.5) {
			StatusObject primarySatelliteObjB;
			primarySatelliteObjB["id"] = "4";
			primarySatelliteObjB["priority"] = 1;
			primarySatelliteObjB["satellite"] = 1;
			if (deterministicRandom()->random01() < 0.25)
				primarySatelliteObjB["satellite_logs"] = deterministicRandom()->randomInt(1, 7);
			primaryDcArr.push_back(primarySatelliteObjB);

			StatusObject remoteSatelliteObjB;
			remoteSatelliteObjB["id"] = "5";
			remoteSatelliteObjB["priority"] = 1;
			remoteSatelliteObjB["satellite"] = 1;
			if (deterministicRandom()->random01() < 0.25)
				remoteSatelliteObjB["satellite_logs"] = deterministicRandom()->randomInt(1, 7);
			remoteDcArr.push_back(remoteSatelliteObjB);

			int satellite_replication_type = deterministicRandom()->randomInt(0, 3);
			switch (satellite_replication_type) {
			case 0: {
				CODE_PROBE(true, "Simulated cluster using no satellite redundancy mode");
				break;
			}
			case 1: {
				CODE_PROBE(true, "Simulated cluster using two satellite fast redundancy mode");
				primaryObj["satellite_redundancy_mode"] = "two_satellite_fast";
				remoteObj["satellite_redundancy_mode"] = "two_satellite_fast";
				break;
			}
			case 2: {
				CODE_PROBE(true, "Simulated cluster using two satellite safe redundancy mode");
				primaryObj["satellite_redundancy_mode"] = "two_satellite_safe";
				remoteObj["satellite_redundancy_mode"] = "two_satellite_safe";
				break;
			}
			default:
				ASSERT(false); // Programmer forgot to adjust cases.
			}
		} else {
			int satellite_replication_type = deterministicRandom()->randomInt(0, 4);
			switch (satellite_replication_type) {
			case 0: {
				// FIXME: implement
				CODE_PROBE(true, "Simulated cluster using custom satellite redundancy mode");
				break;
			}
			case 1: {
				CODE_PROBE(true, "Simulated cluster using no satellite redundancy mode (<5 datacenters)");
				break;
			}
			case 2: {
				CODE_PROBE(true, "Simulated cluster using single satellite redundancy mode");
				primaryObj["satellite_redundancy_mode"] = "one_satellite_single";
				remoteObj["satellite_redundancy_mode"] = "one_satellite_single";
				break;
			}
			case 3: {
				CODE_PROBE(true, "Simulated cluster using double satellite redundancy mode");
				primaryObj["satellite_redundancy_mode"] = "one_satellite_double";
				remoteObj["satellite_redundancy_mode"] = "one_satellite_double";
				break;
			}
			default:
				ASSERT(false); // Programmer forgot to adjust cases.
			}
		}

		if (deterministicRandom()->random01() < 0.25)
			primaryObj["satellite_logs"] = deterministicRandom()->randomInt(1, 7);
		if (deterministicRandom()->random01() < 0.25)
			remoteObj["satellite_logs"] = deterministicRandom()->randomInt(1, 7);

		int remote_replication_type = deterministicRandom()->randomInt(0, 4);
		switch (remote_replication_type) {
		case 0: {
			// FIXME: implement
			CODE_PROBE(true, "Simulated cluster using custom remote redundancy mode");
			break;
		}
		case 1: {
			CODE_PROBE(true, "Simulated cluster using default remote redundancy mode");
			break;
		}
		case 2: {
			CODE_PROBE(true, "Simulated cluster using single remote redundancy mode");
			result += " remote_single";
			break;
		}
		case 3: {
			CODE_PROBE(true, "Simulated cluster using double remote redundancy mode");
			result += " remote_double";
			break;
		}
		default:
			ASSERT(false); // Programmer forgot to adjust cases.
		}

		result += format(" log_routers=%d", deterministicRandom()->randomInt(1, 7));
		result += format(" remote_logs=%d", deterministicRandom()->randomInt(1, 7));
	}

	primaryObj["datacenters"] = primaryDcArr;
	remoteObj["datacenters"] = remoteDcArr;

	StatusArray regionArr;
	regionArr.push_back(primaryObj);

	if (deterministicRandom()->random01() < 0.8) {
		regionArr.push_back(remoteObj);
		if (deterministicRandom()->random01() < 0.25) {
			result += format(" usable_regions=%d", deterministicRandom()->randomInt(1, 3));
		}
	}

	result +=
	    " regions=" + json_spirit::write_string(json_spirit::mValue(regionArr), json_spirit::Output_options::none);
	return result;
}

struct ConfigureDatabaseWorkload : TestWorkload {
	static constexpr auto NAME = "ConfigureDatabase";
	double testDuration;
	int additionalDBs;
	bool allowDescriptorChange;
	bool allowTestStorageMigration; // allow change storage migration and perpetual wiggle conf
	bool storageMigrationCompatibleConf; // only allow generating configuration suitable for storage migration test
	bool waitStoreTypeCheck;
	bool downgradeTest1; // if this is true, don't pick up downgrade incompatible config
	std::vector<int> storageEngineExcludeTypes;
	std::vector<Future<Void>> clients;
	PerfIntCounter retries;

	ConfigureDatabaseWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), retries("Retries") {
		testDuration = getOption(options, "testDuration"_sr, 200.0);
		allowDescriptorChange =
		    getOption(options, "allowDescriptorChange"_sr, SERVER_KNOBS->ENABLE_CROSS_CLUSTER_SUPPORT);
		allowTestStorageMigration =
		    getOption(options, "allowTestStorageMigration"_sr, false) && g_simulator->allowStorageMigrationTypeChange;
		storageMigrationCompatibleConf = getOption(options, "storageMigrationCompatibleConf"_sr, false);
		waitStoreTypeCheck = getOption(options, "waitStoreTypeCheck"_sr, false);
		downgradeTest1 = getOption(options, "downgradeTest1"_sr, false);
		storageEngineExcludeTypes = getOption(options, "storageEngineExcludeTypes"_sr);
		g_simulator->usableRegions = 1;
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("Attrition"); }

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }
	Future<bool> check(Database const& cx) override { return _check(this, cx); }

	void getMetrics(std::vector<PerfMetric>& m) override { m.push_back(retries.getMetric()); }

	static inline uint64_t valueToUInt64(const StringRef& v) {
		long long unsigned int x = 0;
		sscanf(v.toString().c_str(), "%llx", &x);
		return x;
	}

	static inline Standalone<StringRef> getDatabaseName(ConfigureDatabaseWorkload* self, int dbIndex) {
		return StringRef(format("DestroyDB%d", dbIndex));
	}

	static Future<ConfigurationResult> IssueConfigurationChange(Database cx, const std::string& config, bool force) {
		printf("Issuing configuration change: %s\n", config.c_str());
		return ManagementAPI::changeConfig(cx.getReference(), config, force);
	}

	ACTOR Future<Void> _setup(Database cx, ConfigureDatabaseWorkload* self) {
		wait(success(ManagementAPI::changeConfig(cx.getReference(), "single storage_migration_type=aggressive", true)));
		return Void();
	}

	ACTOR Future<Void> _start(ConfigureDatabaseWorkload* self, Database cx) {
		// Redwood is the only storage engine type supporting encryption.
		DatabaseConfiguration config = wait(getDatabaseConfiguration(cx));
		TraceEvent("ConfigureDatabase_Config").detail("Config", config.toString());
		if (config.encryptionAtRestMode.isEncryptionEnabled()) {
			TraceEvent("ConfigureDatabase_EncryptionEnabled");
			self->storageEngineExcludeTypes = { (int)SimulationStorageEngine::SSD,
				                                (int)SimulationStorageEngine::MEMORY,
				                                (int)SimulationStorageEngine::RADIX_TREE,
				                                (int)SimulationStorageEngine::ROCKSDB,
				                                (int)SimulationStorageEngine::SHARDED_ROCKSDB };
		}
		if (!SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
			self->storageEngineExcludeTypes.push_back((int)SimulationStorageEngine::SHARDED_ROCKSDB);
		}
		if (self->clientId == 0) {
			self->clients.push_back(timeout(self->singleDB(self, cx), self->testDuration, Void()));
			wait(waitForAll(self->clients));
		}
		return Void();
	}

	ACTOR Future<bool> _check(ConfigureDatabaseWorkload* self, Database cx) {
		wait(delay(30.0));
		// only storage_migration_type=gradual && perpetual_storage_wiggle=1 need this check because in QuietDatabase
		// perpetual wiggle will be forced to close For other cases, later ConsistencyCheck will check KV store type
		// there
		if (self->allowTestStorageMigration || self->waitStoreTypeCheck) {
			loop {
				// There exists a race where the check can start before the last transaction that singleDB issued
				// finishes, if singleDB gets actor cancelled from a timeout at the end of a test. This means the
				// configuration needs to be re-read in case it changed since the last loop, since it could
				// read a stale storage engine type from the configuration initially.
				state DatabaseConfiguration conf = wait(getDatabaseConfiguration(cx));

				state std::string wiggleLocalityKeyValue = conf.perpetualStorageWiggleLocality;
				state std::vector<std::pair<Optional<Value>, Optional<Value>>> wiggleLocalityKeyValues =
				    ParsePerpetualStorageWiggleLocality(wiggleLocalityKeyValue);
				state int i;

				state bool pass = true;
				state std::vector<StorageServerInterface> storageServers = wait(getStorageServers(cx));

				for (i = 0; i < storageServers.size(); i++) {
					// Check that each storage server has the correct key value store type
					if (!storageServers[i].isTss() &&
					    (wiggleLocalityKeyValue == "0" ||
					     localityMatchInList(wiggleLocalityKeyValues, storageServers[i].locality))) {
						ReplyPromise<KeyValueStoreType> typeReply;
						ErrorOr<KeyValueStoreType> keyValueStoreType =
						    wait(storageServers[i].getKeyValueStoreType.getReplyUnlessFailedFor(typeReply, 2, 0));
						if (keyValueStoreType.present() && keyValueStoreType.get() != conf.storageServerStoreType) {
							TraceEvent(SevWarn, "ConfigureDatabase_WrongStoreType")
							    .suppressFor(5.0)
							    .detail("ServerID", storageServers[i].id())
							    .detail("ProcessID", storageServers[i].locality.processId())
							    .detail("ServerStoreType",
							            keyValueStoreType.present() ? keyValueStoreType.get().toString() : "?")
							    .detail("ConfigStoreType", conf.storageServerStoreType.toString());
							pass = false;
							break;
						}
					}
				}
				if (pass)
					break;
				wait(delay(g_network->isSimulated() ? 2.0 : 30.0));
			}
		}
		return true;
	}

	static int randomRoleNumber() {
		int i = deterministicRandom()->randomInt(0, 4);
		return i ? i : -1;
	}

	ACTOR Future<Void> singleDB(ConfigureDatabaseWorkload* self, Database cx) {
		state Transaction tr;
		loop {
			if (g_simulator->speedUpSimulation) {
				return Void();
			}
			state int randomChoice;
			if (self->allowTestStorageMigration) {
				randomChoice = (deterministicRandom()->random01() < 0.375) ? deterministicRandom()->randomInt(0, 3)
				                                                           : deterministicRandom()->randomInt(4, 9);
			} else if (self->storageMigrationCompatibleConf) {
				randomChoice = (deterministicRandom()->random01() < 3.0 / 7) ? deterministicRandom()->randomInt(0, 3)
				                                                             : deterministicRandom()->randomInt(4, 8);
			} else {
				randomChoice = deterministicRandom()->randomInt(0, 8);
			}
			if (randomChoice == 0) {
				wait(success(
				    runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
					    return tr->get("This read is only to ensure that the database recovered"_sr);
				    })));
				wait(delay(20 + 10 * deterministicRandom()->random01()));
			} else if (randomChoice < 3) {
				double waitDuration = 3.0 * deterministicRandom()->random01();
				//TraceEvent("ConfigureTestWaitAfter").detail("WaitDuration",waitDuration);
				wait(delay(waitDuration));
			} else if (randomChoice == 3) {
				//TraceEvent("ConfigureTestConfigureBegin").detail("NewConfig", newConfig);
				int maxRedundancies = sizeof(redundancies) / sizeof(redundancies[0]);
				if (g_simulator->physicalDatacenters == 2 || g_simulator->physicalDatacenters > 3) {
					maxRedundancies--; // There are not enough machines for triple replication in fearless
					                   // configurations
				}
				int redundancy = deterministicRandom()->randomInt(0, maxRedundancies);
				std::string config = redundancies[redundancy];

				if (config == "triple" && g_simulator->physicalDatacenters == 3) {
					config = "three_data_hall ";
				}

				config += generateRegions();

				if (deterministicRandom()->random01() < 0.5)
					config += " logs=" + format("%d", randomRoleNumber());

				if (deterministicRandom()->random01() < 0.2) {
					config += " proxies=" + format("%d", deterministicRandom()->randomInt(2, 5));
				} else {
					if (deterministicRandom()->random01() < 0.5)
						config += " commit_proxies=" + format("%d", randomRoleNumber());
					if (deterministicRandom()->random01() < 0.5)
						config += " grv_proxies=" + format("%d", randomRoleNumber());
				}
				if (deterministicRandom()->random01() < 0.5)
					config += " resolvers=" + format("%d", randomRoleNumber());

				wait(success(IssueConfigurationChange(cx, config, false)));

				//TraceEvent("ConfigureTestConfigureEnd").detail("NewConfig", newConfig);
			} else if (randomChoice == 4) {
				//TraceEvent("ConfigureTestQuorumBegin");
				auto ch = autoQuorumChange();
				std::string desiredClusterName = "NewName%d";
				if (!self->allowDescriptorChange) {
					// if configuration does not allow changing the descriptor, pass empty string (keep old descriptor)
					desiredClusterName = "";
				}
				if (deterministicRandom()->randomInt(0, 2))
					ch = nameQuorumChange(format(desiredClusterName.c_str(), deterministicRandom()->randomInt(0, 100)),
					                      ch);
				wait(success(changeQuorum(cx, ch)));
				//TraceEvent("ConfigureTestConfigureEnd").detail("NewQuorum", s);
			} else if (randomChoice == 5) {
				int storeType = 0;
				while (true) {
					storeType = deterministicRandom()->randomInt(0, 6);
					if (std::find(self->storageEngineExcludeTypes.begin(),
					              self->storageEngineExcludeTypes.end(),
					              storeType) == self->storageEngineExcludeTypes.end()) {
						break;
					}
				}
				constexpr std::array ssdTypes{ "ssd", "ssd-1", "ssd-2" };
				constexpr std::array memoryTypes{ "memory", "memory-1", "memory-2" };
				const char* storeTypeStr = nullptr;
				switch (storeType) {
				case 0:
					storeTypeStr = ssdTypes[deterministicRandom()->randomInt(0, 3)];
					break;
				case 1:
					storeTypeStr = memoryTypes[deterministicRandom()->randomInt(0, 3)];
					break;
				case 2:
					storeTypeStr = "memory-radixtree";
					break;
				case 3:
					storeTypeStr = "ssd-redwood-1";
					break;
				case 4:
					storeTypeStr = "ssd-rocksdb-v1";
					break;
				case 5:
					storeTypeStr = "ssd-sharded-rocksdb";
					break;
				default:
					ASSERT(false);
				}
				wait(success(IssueConfigurationChange(cx, storeTypeStr, true)));
			} else if (randomChoice == 6) {
				// Some configurations will be invalid, and that's fine.
				int length = sizeof(logTypes) / sizeof(logTypes[0]);

				if (self->downgradeTest1) {
					length -= 1;
				}

				wait(success(
				    IssueConfigurationChange(cx, logTypes[deterministicRandom()->randomInt(0, length)], false)));
			} else if (randomChoice == 7) {
				wait(success(IssueConfigurationChange(
				    cx,
				    backupTypes[deterministicRandom()->randomInt(0, sizeof(backupTypes) / sizeof(backupTypes[0]))],
				    false)));
			} else if (randomChoice == 8) {
				if (self->allowTestStorageMigration) {
					CODE_PROBE(true, "storage migration type change");

					// randomly configuring perpetual_storage_wiggle_locality
					state std::string randomPerpetualWiggleLocality;
					if (deterministicRandom()->random01() < 0.25) {
						state std::vector<StorageServerInterface> storageServers = wait(getStorageServers(cx));
						std::string localityFilter;
						int selectSSCount =
						    deterministicRandom()->randomInt(1, std::min(4, (int)(storageServers.size())));
						std::vector<StringRef> localityKeys = { LocalityData::keyDcId,
							                                    LocalityData::keyDataHallId,
							                                    LocalityData::keyZoneId,
							                                    LocalityData::keyMachineId,
							                                    LocalityData::keyProcessId };
						for (int i = 0; i < selectSSCount; ++i) {
							StorageServerInterface randomSS =
							    storageServers[deterministicRandom()->randomInt(0, storageServers.size())];
							StringRef randomLocalityKey =
							    localityKeys[deterministicRandom()->randomInt(0, localityKeys.size())];
							if (randomSS.locality.isPresent(randomLocalityKey)) {
								if (localityFilter.size() > 0) {
									localityFilter += ";";
								}
								localityFilter += randomLocalityKey.toString() + ":" +
								                  randomSS.locality.get(randomLocalityKey).get().toString();
							}
						}

						if (localityFilter.size() > 0) {
							TraceEvent("ConfigureTestSettingWiggleLocality").detail("LocalityFilter", localityFilter);
							randomPerpetualWiggleLocality = " perpetual_storage_wiggle_locality=" + localityFilter;
						}
					}

					wait(success(IssueConfigurationChange(
					    cx,
					    storageMigrationTypes[deterministicRandom()->randomInt(
					        0, sizeof(storageMigrationTypes) / sizeof(storageMigrationTypes[0]))] +
					        randomPerpetualWiggleLocality,
					    false)));
				}
			} else {
				ASSERT(false);
			}
		}
	}
};

WorkloadFactory<ConfigureDatabaseWorkload> DestroyDatabaseWorkloadFactory;
