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

#include "fdbclient/NativeAPI.h"
#include "fdbserver/TesterInterface.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbserver/workloads/workloads.h"
#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

// "ssd" is an alias to the preferred type which skews the random distribution toward it but that's okay.
static const char* storeTypes[] = { "ssd", "ssd-1", "ssd-2", "memory" };
static const char* redundancies[] = { "single", "double", "triple" };

std::string generateRegions() {
	std::string result;
	if(g_simulator.physicalDatacenters == 1 || (g_simulator.physicalDatacenters == 2 && g_random->random01() < 0.25) || g_simulator.physicalDatacenters == 3) {
		return " usable_regions=1 regions=\"\"";
	}

	if(g_random->random01() < 0.25) {
		return format(" usable_regions=%d", g_random->randomInt(1,3));
	}

	int primaryPriority = 1;
	int remotePriority = -1;
	double priorityType = g_random->random01();
	if(priorityType < 0.1) {
		primaryPriority = -1;
		remotePriority = 1;
	} else if(priorityType < 0.2) {
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

	if(g_simulator.physicalDatacenters > 3 && g_random->random01() < 0.5) {
		StatusObject primarySatelliteObj;
		primarySatelliteObj["id"] = "2";
		primarySatelliteObj["priority"] = 1;
		primarySatelliteObj["satellite"] = 1;
		primaryDcArr.push_back(primarySatelliteObj);

		StatusObject remoteSatelliteObj;
		remoteSatelliteObj["id"] = "3";
		remoteSatelliteObj["priority"] = 1;
		remoteSatelliteObj["satellite"] = 1;
		remoteDcArr.push_back(remoteSatelliteObj);

		if(g_simulator.physicalDatacenters > 5 && g_random->random01() < 0.5) {
			StatusObject primarySatelliteObjB;
			primarySatelliteObjB["id"] = "4";
			primarySatelliteObjB["priority"] = 1;
			primarySatelliteObjB["satellite"] = 1;
			primaryDcArr.push_back(primarySatelliteObjB);

			StatusObject remoteSatelliteObjB;
			remoteSatelliteObjB["id"] = "5";
			remoteSatelliteObjB["priority"] = 1;
			remoteSatelliteObjB["satellite"] = 1;
			remoteDcArr.push_back(remoteSatelliteObjB);

			int satellite_replication_type = g_random->randomInt(0,3);
			switch (satellite_replication_type) {
			case 0: {
				TEST( true );  // Simulated cluster using no satellite redundancy mode
				break;
			}
			case 1: {
				TEST( true );  // Simulated cluster using two satellite fast redundancy mode
				primaryObj["satellite_redundancy_mode"] = "two_satellite_fast";
				remoteObj["satellite_redundancy_mode"] = "two_satellite_fast";
				break;
			}
			case 2: {
				TEST( true );  // Simulated cluster using two satellite safe redundancy mode
				primaryObj["satellite_redundancy_mode"] = "two_satellite_safe";
				remoteObj["satellite_redundancy_mode"] = "two_satellite_safe";
				break;
			}
			default:
				ASSERT(false);  // Programmer forgot to adjust cases.
			}
		} else {
			int satellite_replication_type = g_random->randomInt(0,4);
			switch (satellite_replication_type) {
			case 0: {
				//FIXME: implement
				TEST( true );  // Simulated cluster using custom satellite redundancy mode
				break;
			}
			case 1: {
				TEST( true );  // Simulated cluster using no satellite redundancy mode
				break;
			}
			case 2: {
				TEST( true );  // Simulated cluster using single satellite redundancy mode
				primaryObj["satellite_redundancy_mode"] = "one_satellite_single";
				remoteObj["satellite_redundancy_mode"] = "one_satellite_single";
				break;
			}
			case 3: {
				TEST( true );  // Simulated cluster using double satellite redundancy mode
				primaryObj["satellite_redundancy_mode"] = "one_satellite_double";
				remoteObj["satellite_redundancy_mode"] = "one_satellite_double";
				break;
			}
			default:
				ASSERT(false);  // Programmer forgot to adjust cases.
			}
		}

		if (g_random->random01() < 0.25) {
			int logs = g_random->randomInt(1,7);
			primaryObj["satellite_logs"] = logs;
			remoteObj["satellite_logs"] = logs;
		}

		int remote_replication_type = g_random->randomInt(0, 4);
		switch (remote_replication_type) {
		case 0: {
			//FIXME: implement
			TEST( true );  // Simulated cluster using custom remote redundancy mode
			break;
		}
		case 1: {
			TEST( true );  // Simulated cluster using default remote redundancy mode
			break;
		}
		case 2: {
			TEST( true );  // Simulated cluster using single remote redundancy mode
			result += " remote_single";
			break;
		}
		case 3: {
			TEST( true );  // Simulated cluster using double remote redundancy mode
			result += " remote_double";
			break;
		}
		default:
			ASSERT(false);  // Programmer forgot to adjust cases.
		}

		result += format(" log_routers=%d", g_random->randomInt(1,7));
		result += format(" remote_logs=%d", g_random->randomInt(1,7));
	}

	primaryObj["datacenters"] = primaryDcArr;
	remoteObj["datacenters"] = remoteDcArr;

	StatusArray regionArr;
	regionArr.push_back(primaryObj);

	if(g_random->random01() < 0.8) {
		regionArr.push_back(remoteObj);
		if(g_random->random01() < 0.25) {
			result += format(" usable_regions=%d", g_random->randomInt(1,3));
		}
	}

	result += " regions=" + json_spirit::write_string(json_spirit::mValue(regionArr), json_spirit::Output_options::none);
	return result;
}



struct ConfigureDatabaseWorkload : TestWorkload {
	double testDuration;
	int additionalDBs;

	vector<Future<Void>> clients;
	PerfIntCounter retries;

	ConfigureDatabaseWorkload( WorkloadContext const& wcx )
		: TestWorkload(wcx), retries("Retries")
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 200.0 );
		g_simulator.usableRegions = 1;
	}

	virtual std::string description() { return "DestroyDatabaseWorkload"; }

	virtual Future<Void> setup( Database const& cx ) {
		return _setup( cx, this );
	}

	virtual Future<Void> start( Database const& cx ) {
		return _start( this, cx );
	}
	virtual Future<bool> check( Database const& cx ) {
		return true;
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
		m.push_back( retries.getMetric() );
	}

	static inline uint64_t valueToUInt64( const StringRef& v ) {
		long long unsigned int x = 0;
		sscanf( v.toString().c_str(), "%llx", &x );
		return x;
	}

	static inline Standalone<StringRef> getDatabaseName( ConfigureDatabaseWorkload *self, int dbIndex ) {
		return StringRef(format("DestroyDB%d", dbIndex));
	}

	ACTOR Future<Void> _setup( Database cx, ConfigureDatabaseWorkload *self ) {
		ConfigurationResult::Type _ = wait( changeConfig( cx, "single", true ) );
		return Void();
	}

	ACTOR Future<Void> _start( ConfigureDatabaseWorkload *self, Database cx ) {
		if( self->clientId == 0 ) {
			self->clients.push_back( timeout( self->singleDB( self, cx ), self->testDuration, Void() ) );
			wait( waitForAll( self->clients ) );
		}
		return Void();
	}

	static int randomRoleNumber() {
		int i = g_random->randomInt(0,4);
		return i ? i : -1;
	}

	ACTOR Future<Void> singleDB( ConfigureDatabaseWorkload *self, Database cx ) {
		state Transaction tr;
		state int i;
		state bool firstFearless = false;
		loop {
			if(g_simulator.speedUpSimulation) {
				return Void();
			}
			state int randomChoice = g_random->randomInt(0, 6);
			if( randomChoice == 0 ) {
				double waitDuration = 3.0 * g_random->random01();
				//TraceEvent("ConfigureTestWaitAfter").detail("WaitDuration",waitDuration);
				wait( delay( waitDuration ) );
			}
			else if( randomChoice == 1 ) {
				tr = Transaction( cx );
				loop {
					try {
						tr.clear( normalKeys );
						wait( tr.commit() );
						break;
					} catch( Error &e ) {
						wait( tr.onError(e) );
					}
				}
			}
			else if( randomChoice == 2 ) {
				state double loadDuration = g_random->random01() * 10.0;
				state double startTime = now();
				state int amtLoaded = 0;

				loop {
					if( now() - startTime > loadDuration )
						break;
					loop {
						tr = Transaction( cx );
						try {
							for( i = 0; i < 10; i++ ) {
								state Key randomKey( "ConfigureTest" + g_random->randomUniqueID().toString() );
								Optional<Value> val = wait( tr.get( randomKey ) );
								uint64_t nextVal = val.present() ? valueToUInt64( val.get() ) + 1 : 0;
								tr.set( randomKey, format( "%016llx", nextVal ) );
							}
							wait( tr.commit() );
							amtLoaded += 10;
							break;
						}
						catch( Error& e ) {
							wait( tr.onError( e ) );
							++self->retries;
						}
					}
					wait( delay( 0.1 ) );
				}

				//TraceEvent("ConfigureTestLoadData").detail("LoadTime", now() - startTime).detail("AmountLoaded",amtLoaded);
			}
			else if( randomChoice == 3 ) {
				//TraceEvent("ConfigureTestConfigureBegin").detail("NewConfig", newConfig);
				int maxRedundancies = sizeof(redundancies)/sizeof(redundancies[0]);
				if(g_simulator.physicalDatacenters == 2 || g_simulator.physicalDatacenters > 3) {
					maxRedundancies--; //There are not enough machines for triple replication in fearless configurations
				}
				int redundancy = g_random->randomInt(0, maxRedundancies);
				std::string config = redundancies[redundancy];

				if(config == "triple" && g_simulator.physicalDatacenters == 3) {
					config = "three_data_hall ";
				}

				config += generateRegions();

				if (g_random->random01() < 0.5) config += " logs=" + format("%d", randomRoleNumber());
				if (g_random->random01() < 0.5) config += " proxies=" + format("%d", randomRoleNumber());
				if (g_random->random01() < 0.5) config += " resolvers=" + format("%d", randomRoleNumber());

				ConfigurationResult::Type _ = wait( changeConfig( cx, config, false ) );

				//TraceEvent("ConfigureTestConfigureEnd").detail("NewConfig", newConfig);
			}
			else if( randomChoice == 4 ) {
				//TraceEvent("ConfigureTestQuorumBegin").detail("NewQuorum", s);
				auto ch = autoQuorumChange();
				if (g_random->randomInt(0,2))
					ch = nameQuorumChange( format("NewName%d", g_random->randomInt(0,100)), ch );
				CoordinatorsResult::Type _ = wait( changeQuorum( cx, ch ) );
				//TraceEvent("ConfigureTestConfigureEnd").detail("NewQuorum", s);
			}
			else if ( randomChoice == 5) {
				ConfigurationResult::Type _ = wait( changeConfig( cx, storeTypes[g_random->randomInt( 0, sizeof(storeTypes)/sizeof(storeTypes[0]))], true ) );
			}
			else {
				ASSERT(false);
			}
		}
	}
};

WorkloadFactory<ConfigureDatabaseWorkload> DestroyDatabaseWorkloadFactory("ConfigureDatabase");
