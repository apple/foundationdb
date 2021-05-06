/*
 * ConfigureDatabase.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// "ssd" is an alias to the preferred type which skews the random distribution toward it but that's okay.
static const char* storeTypes[] = {
	"ssd", "ssd-1", "ssd-2", "memory", "memory-1", "memory-2", "memory-radixtree-beta"
};
static const char* logTypes[] = { "log_engine:=1",  "log_engine:=2",  "log_spill:=1",
	                              "log_spill:=2",   "log_version:=2", "log_version:=3",
	                              "log_version:=4", "log_version:=5", "log_version:=6" };
static const char* redundancies[] = { "single", "double", "triple" };
static const char* backupTypes[] = { "backup_worker_enabled:=0", "backup_worker_enabled:=1" };

std::string generateRegions() {
	std::string result;
	if (g_simulator.physicalDatacenters == 1 ||
	    (g_simulator.physicalDatacenters == 2 && deterministicRandom()->random01() < 0.25) ||
	    g_simulator.physicalDatacenters == 3) {
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

	if (g_simulator.physicalDatacenters > 3 && deterministicRandom()->random01() < 0.5) {
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

		if (g_simulator.physicalDatacenters > 5 && deterministicRandom()->random01() < 0.5) {
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
				TEST(true); // Simulated cluster using no satellite redundancy mode
				break;
			}
			case 1: {
				TEST(true); // Simulated cluster using two satellite fast redundancy mode
				primaryObj["satellite_redundancy_mode"] = "two_satellite_fast";
				remoteObj["satellite_redundancy_mode"] = "two_satellite_fast";
				break;
			}
			case 2: {
				TEST(true); // Simulated cluster using two satellite safe redundancy mode
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
				TEST(true); // Simulated cluster using custom satellite redundancy mode
				break;
			}
			case 1: {
				TEST(true); // Simulated cluster using no satellite redundancy mode (<5 datacenters)
				break;
			}
			case 2: {
				TEST(true); // Simulated cluster using single satellite redundancy mode
				primaryObj["satellite_redundancy_mode"] = "one_satellite_single";
				remoteObj["satellite_redundancy_mode"] = "one_satellite_single";
				break;
			}
			case 3: {
				TEST(true); // Simulated cluster using double satellite redundancy mode
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
			TEST(true); // Simulated cluster using custom remote redundancy mode
			break;
		}
		case 1: {
			TEST(true); // Simulated cluster using default remote redundancy mode
			break;
		}
		case 2: {
			TEST(true); // Simulated cluster using single remote redundancy mode
			result += " remote_single";
			break;
		}
		case 3: {
			TEST(true); // Simulated cluster using double remote redundancy mode
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
	double testDuration;
	int additionalDBs;

	vector<Future<Void>> clients;
	PerfIntCounter retries;

	ConfigureDatabaseWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), retries("Retries") {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 200.0);
		g_simulator.usableRegions = 1;
	}

	std::string description() const override { return "DestroyDatabaseWorkload"; }

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }
	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(vector<PerfMetric>& m) override { m.push_back(retries.getMetric()); }

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
		return changeConfig(cx, config, force);
	}

	ACTOR Future<Void> _setup(Database cx, ConfigureDatabaseWorkload* self) {
		wait(success(changeConfig(cx, "single", true)));
		return Void();
	}

	ACTOR Future<Void> _start(ConfigureDatabaseWorkload* self, Database cx) {
		if (self->clientId == 0) {
			self->clients.push_back(timeout(self->singleDB(self, cx), self->testDuration, Void()));
			wait(waitForAll(self->clients));
		}
		return Void();
	}

	static int randomRoleNumber() {
		int i = deterministicRandom()->randomInt(0, 4);
		return i ? i : -1;
	}

	ACTOR Future<Void> singleDB(ConfigureDatabaseWorkload* self, Database cx) {
		state Transaction tr;
		loop {
			if (g_simulator.speedUpSimulation) {
				return Void();
			}
			state int randomChoice = deterministicRandom()->randomInt(0, 8);

			if (randomChoice == 0) {
				wait(success(
				    runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
					    return tr->get(LiteralStringRef("This read is only to ensure that the database recovered"));
				    })));
				wait(delay(20 + 10 * deterministicRandom()->random01()));
			} else if (randomChoice < 3) {
				double waitDuration = 3.0 * deterministicRandom()->random01();
				//TraceEvent("ConfigureTestWaitAfter").detail("WaitDuration",waitDuration);
				wait(delay(waitDuration));
			} else if (randomChoice == 3) {
				//TraceEvent("ConfigureTestConfigureBegin").detail("NewConfig", newConfig);
				int maxRedundancies = sizeof(redundancies) / sizeof(redundancies[0]);
				if (g_simulator.physicalDatacenters == 2 || g_simulator.physicalDatacenters > 3) {
					maxRedundancies--; // There are not enough machines for triple replication in fearless
					                   // configurations
				}
				int redundancy = deterministicRandom()->randomInt(0, maxRedundancies);
				std::string config = redundancies[redundancy];

				if (config == "triple" && g_simulator.physicalDatacenters == 3) {
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
				//TraceEvent("ConfigureTestQuorumBegin").detail("NewQuorum", s);
				auto ch = autoQuorumChange();
				std::string desiredClusterName = "NewName%d";
				if (!SERVER_KNOBS->ENABLE_CROSS_CLUSTER_SUPPORT) {
					// if configuration does not allow changing the descriptor, pass empty string (keep old descriptor)
					desiredClusterName = "";
				}
				if (deterministicRandom()->randomInt(0, 2))
					ch = nameQuorumChange(format(desiredClusterName.c_str(), deterministicRandom()->randomInt(0, 100)),
					                      ch);
				wait(success(changeQuorum(cx, ch)));
				//TraceEvent("ConfigureTestConfigureEnd").detail("NewQuorum", s);
			} else if (randomChoice == 5) {
				wait(success(IssueConfigurationChange(
				    cx,
				    storeTypes[deterministicRandom()->randomInt(0, sizeof(storeTypes) / sizeof(storeTypes[0]))],
				    true)));
			} else if (randomChoice == 6) {
				// Some configurations will be invalid, and that's fine.
				wait(success(IssueConfigurationChange(
				    cx, logTypes[deterministicRandom()->randomInt(0, sizeof(logTypes) / sizeof(logTypes[0]))], false)));
			} else if (randomChoice == 7) {
				wait(success(IssueConfigurationChange(
				    cx,
				    backupTypes[deterministicRandom()->randomInt(0, sizeof(backupTypes) / sizeof(backupTypes[0]))],
				    false)));
			} else {
				ASSERT(false);
			}
		}
	}
};

WorkloadFactory<ConfigureDatabaseWorkload> DestroyDatabaseWorkloadFactory("ConfigureDatabase");
