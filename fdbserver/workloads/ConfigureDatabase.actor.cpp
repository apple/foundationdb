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

#include "flow/actorcompiler.h"
#include "fdbclient/NativeAPI.h"
#include "fdbserver/TesterInterface.h"
#include "fdbclient/ManagementAPI.h"
#include "workloads.h"
#include "fdbrpc/simulator.h"

// "ssd" is an alias to the preferred type which skews the random distribution toward it but that's okay.
static const char* storeTypes[] = { "ssd", "ssd-1", "ssd-2", "memory" };
static const char* redundancies[] = { "single", "double", "triple" };

struct ConfigureDatabaseWorkload : TestWorkload {
	double testDuration;
	int additionalDBs;

	vector<Future<Void>> clients;
	PerfIntCounter retries;

	ConfigureDatabaseWorkload( WorkloadContext const& wcx )
		: TestWorkload(wcx), retries("Retries")
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 200.0 );
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
		state Future<Void> disabler = disableConnectionFailuresAfter(600, "ConfigureDatabaseSetup");
		ConfigurationResult::Type _ = wait( changeConfig( cx, "single" ) );
		return Void();
	}

	ACTOR Future<Void> _start( ConfigureDatabaseWorkload *self, Database cx ) {
		if( self->clientId == 0 ) {
			self->clients.push_back( timeout( self->singleDB( self, cx ), self->testDuration, Void() ) );
			Void _ = wait( waitForAll( self->clients ) );
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
		loop {
			if(g_simulator.speedUpSimulation) {
				return Void();
			}
			state int randomChoice = g_random->randomInt(0, 6);
			if( randomChoice == 0 ) {
				double waitDuration = 3.0 * g_random->random01();
				//TraceEvent("ConfigureTestWaitAfter").detail("WaitDuration",waitDuration);
				Void _ = wait( delay( waitDuration ) );
			}
			else if( randomChoice == 1 ) {
				tr = Transaction( cx );
				loop {
					try {
						tr.clear( normalKeys );
						Void _ = wait( tr.commit() );
						break;
					} catch( Error &e ) {
						Void _ = wait( tr.onError(e) );
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
							Void _ = wait( tr.commit() );
							amtLoaded += 10;
							break;
						}
						catch( Error& e ) {
							Void _ = wait( tr.onError( e ) );
							++self->retries;
						}
					}
					Void _ = wait( delay( 0.1 ) );
				}

				//TraceEvent("ConfigureTestLoadData").detail("LoadTime", now() - startTime).detail("AmountLoaded",amtLoaded);
			}
			else if( randomChoice == 3 ) {
				//TraceEvent("ConfigureTestConfigureBegin").detail("newConfig", newConfig);
				int redundancy = g_random->randomInt( 0, sizeof(redundancies)/sizeof(redundancies[0]));
				std::string config = redundancies[redundancy];
				if(config == "triple" && g_simulator.physicalDatacenters > 2) {
					config = "three_data_hall";
				}

				if (g_random->random01() < 0.5) config += " logs=" + format("%d", randomRoleNumber());
				if (g_random->random01() < 0.5) config += " proxies=" + format("%d", randomRoleNumber());
				if (g_random->random01() < 0.5) config += " resolvers=" + format("%d", randomRoleNumber());

				ConfigurationResult::Type _ = wait( changeConfig( cx, config ) );
				//TraceEvent("ConfigureTestConfigureEnd").detail("newConfig", newConfig);
			}
			else if( randomChoice == 4 ) {
				//TraceEvent("ConfigureTestQuorumBegin").detail("newQuorum", s);
				auto ch = autoQuorumChange();
				if (g_random->randomInt(0,2))
					ch = nameQuorumChange( format("NewName%d", g_random->randomInt(0,100)), ch );
				CoordinatorsResult::Type _ = wait( changeQuorum( cx, ch ) );
				//TraceEvent("ConfigureTestConfigureEnd").detail("newQuorum", s);
			}
			else if ( randomChoice == 5) {
				ConfigurationResult::Type _ = wait( changeConfig( cx, storeTypes[g_random->randomInt( 0, sizeof(storeTypes)/sizeof(storeTypes[0]))] ) );
			}
			else {
				ASSERT(false);
			}
		}
	}
};

WorkloadFactory<ConfigureDatabaseWorkload> DestroyDatabaseWorkloadFactory("ConfigureDatabase");
