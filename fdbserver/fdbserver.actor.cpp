/*
 * fdbserver.actor.cpp
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
#include "fdbrpc/simulator.h"
#include "flow/DeterministicRandom.h"
#include "fdbrpc/PerfMetric.h"
#include "flow/Platform.h"
#include "flow/SystemMonitor.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/FailureMonitorClient.h"
#include "CoordinationInterface.h"
#include "WorkerInterface.h"
#include "ClusterRecruitmentInterface.h"
#include "ServerDBInfo.h"
#include "MoveKeys.h"
#include "ConflictSet.h"
#include "DataDistribution.h"
#include "NetworkTest.h"
#include "IKeyValueStore.h"
#include <stdarg.h>
#include <stdio.h>
#include <fstream>
#include "pubsub.h"
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#undef min
#undef max
#endif
#include "SimulatedCluster.h"
#include "TesterInterface.h"
#include "workloads/workloads.h"
#include <time.h>
#include "Status.h"
#include "fdbrpc/TLSConnection.h"
#include "fdbrpc/Net2FileSystem.h"
#include "fdbrpc/Platform.h"
#include "CoroFlow.h"
#include "flow/SignalSafeUnwind.h"

#define BOOST_DATE_TIME_NO_LIB
#include <boost/interprocess/managed_shared_memory.hpp>

#ifdef  __linux__
#include <execinfo.h>
#include <signal.h>
#ifdef ALLOC_INSTRUMENTATION
#include <cxxabi.h>
#endif
#endif

#ifndef WIN32
#include "versions.h"
#endif

#include "flow/SimpleOpt.h"

enum {
	OPT_CONNFILE, OPT_SEEDCONNFILE, OPT_SEEDCONNSTRING, OPT_ROLE, OPT_LISTEN, OPT_PUBLICADDR, OPT_DATAFOLDER, OPT_LOGFOLDER, OPT_PARENTPID, OPT_NEWCONSOLE, OPT_NOBOX, OPT_TESTFILE, OPT_RESTARTING, OPT_RANDOMSEED, OPT_KEY, OPT_MEMLIMIT, OPT_STORAGEMEMLIMIT, OPT_MACHINEID, OPT_DCID, OPT_MACHINE_CLASS, OPT_BUGGIFY, OPT_VERSION, OPT_CRASHONERROR, OPT_HELP, OPT_NETWORKIMPL, OPT_NOBUFSTDOUT, OPT_BUFSTDOUTERR, OPT_TRACECLOCK, OPT_NUMTESTERS, OPT_DEVHELP, OPT_ROLLSIZE, OPT_MAXLOGS, OPT_MAXLOGSSIZE, OPT_KNOB, OPT_TESTSERVERS, OPT_TEST_ON_SERVERS, OPT_METRICSCONNFILE, OPT_METRICSPREFIX,
	OPT_LOGGROUP, OPT_LOCALITY, OPT_IO_TRUST_SECONDS, OPT_IO_TRUST_WARN_ONLY, OPT_FILESYSTEM, OPT_KVFILE };

CSimpleOpt::SOption g_rgOptions[] = {
	{ OPT_CONNFILE,             "-C",                          SO_REQ_SEP },
	{ OPT_CONNFILE,             "--cluster_file",              SO_REQ_SEP },
	{ OPT_SEEDCONNFILE,         "--seed_cluster_file",         SO_REQ_SEP },
	{ OPT_SEEDCONNSTRING,       "--seed_connection_string",    SO_REQ_SEP },
	{ OPT_ROLE,                 "-r",                          SO_REQ_SEP },
	{ OPT_ROLE,                 "--role",                      SO_REQ_SEP },
	{ OPT_PUBLICADDR,           "-p",                          SO_REQ_SEP },
	{ OPT_PUBLICADDR,           "--public_address",            SO_REQ_SEP },
	{ OPT_LISTEN,               "-l",                          SO_REQ_SEP },
	{ OPT_LISTEN,               "--listen_address",            SO_REQ_SEP },
#ifdef __linux__
	{ OPT_FILESYSTEM,           "--data_filesystem",           SO_REQ_SEP },
#endif
	{ OPT_DATAFOLDER,           "-d",                          SO_REQ_SEP },
	{ OPT_DATAFOLDER,           "--datadir",                   SO_REQ_SEP },
	{ OPT_LOGFOLDER,            "-L",                          SO_REQ_SEP },
	{ OPT_LOGFOLDER,            "--logdir",                    SO_REQ_SEP },
	{ OPT_ROLLSIZE,             "-Rs",                         SO_REQ_SEP },
	{ OPT_ROLLSIZE,             "--logsize",                   SO_REQ_SEP },
	{ OPT_MAXLOGS,              "--maxlogs",                   SO_REQ_SEP },
	{ OPT_MAXLOGSSIZE,          "--maxlogssize",               SO_REQ_SEP },
	{ OPT_LOGGROUP,             "--loggroup",                  SO_REQ_SEP },
#ifdef _WIN32
	{ OPT_PARENTPID,            "--parentpid",                 SO_REQ_SEP },
	{ OPT_NEWCONSOLE,           "-n",                          SO_NONE },
	{ OPT_NEWCONSOLE,           "--newconsole",                SO_NONE },
	{ OPT_NOBOX,                "-q",                          SO_NONE },
	{ OPT_NOBOX,                "--no_dialog",                 SO_NONE },
#endif
	{ OPT_KVFILE,               "--kvfile",                    SO_REQ_SEP },
	{ OPT_TESTFILE,             "-f",                          SO_REQ_SEP },
	{ OPT_TESTFILE,             "--testfile",                  SO_REQ_SEP },
	{ OPT_RESTARTING,           "-R",                          SO_NONE },
	{ OPT_RESTARTING,           "--restarting",                SO_NONE },
	{ OPT_RANDOMSEED,           "-s",                          SO_REQ_SEP },
	{ OPT_RANDOMSEED,           "--seed",                      SO_REQ_SEP },
	{ OPT_KEY,                  "-k",                          SO_REQ_SEP },
	{ OPT_KEY,                  "--key",                       SO_REQ_SEP },
	{ OPT_MEMLIMIT,             "-m",                          SO_REQ_SEP },
	{ OPT_MEMLIMIT,             "--memory",                    SO_REQ_SEP },
	{ OPT_STORAGEMEMLIMIT,      "-M",                          SO_REQ_SEP },
	{ OPT_STORAGEMEMLIMIT,      "--storage_memory",            SO_REQ_SEP },
	{ OPT_MACHINEID,            "-i",                          SO_REQ_SEP },
	{ OPT_MACHINEID,            "--machine_id",                SO_REQ_SEP },
	{ OPT_DCID,                 "-a",                          SO_REQ_SEP },
	{ OPT_DCID,                 "--datacenter_id",             SO_REQ_SEP },
	{ OPT_MACHINE_CLASS,        "-c",                          SO_REQ_SEP },
	{ OPT_MACHINE_CLASS,        "--class",                     SO_REQ_SEP },
	{ OPT_BUGGIFY,              "-b",                          SO_REQ_SEP },
	{ OPT_BUGGIFY,              "--buggify",                   SO_REQ_SEP },
	{ OPT_VERSION,              "-v",                          SO_NONE },
	{ OPT_VERSION,              "--version",                   SO_NONE },
	{ OPT_CRASHONERROR,         "--crash",                     SO_NONE },
	{ OPT_NETWORKIMPL,          "-N",                          SO_REQ_SEP },
	{ OPT_NETWORKIMPL,          "--network",                   SO_REQ_SEP },
	{ OPT_NOBUFSTDOUT,          "--unbufferedout",             SO_NONE },
	{ OPT_BUFSTDOUTERR,         "--bufferedout",               SO_NONE },
	{ OPT_TRACECLOCK,           "--traceclock",                SO_REQ_SEP },
	{ OPT_NUMTESTERS,           "--num_testers",               SO_REQ_SEP },
	{ OPT_HELP,                 "-?",                          SO_NONE },
	{ OPT_HELP,                 "-h",                          SO_NONE },
	{ OPT_HELP,                 "--help",                      SO_NONE },
	{ OPT_DEVHELP,              "--dev-help",                  SO_NONE },
	{ OPT_KNOB,                 "--knob_",                     SO_REQ_SEP },
	{ OPT_LOCALITY,             "--locality_",                 SO_REQ_SEP },
	{ OPT_TESTSERVERS,          "--testservers",               SO_REQ_SEP },
	{ OPT_TEST_ON_SERVERS,      "--testonservers",             SO_NONE },
	{ OPT_METRICSCONNFILE,      "--metrics_cluster",           SO_REQ_SEP },
	{ OPT_METRICSPREFIX,        "--metrics_prefix",            SO_REQ_SEP },
	{ OPT_IO_TRUST_SECONDS,     "--io_trust_seconds",          SO_REQ_SEP },
	{ OPT_IO_TRUST_WARN_ONLY,   "--io_trust_warn_only",        SO_NONE },

	TLS_OPTION_FLAGS

	SO_END_OF_OPTIONS
};

GlobalCounters g_counters;

extern void dsltest();
extern void pingtest();
extern void copyTest();
extern void versionedMapTest();
extern void createTemplateDatabase();
// FIXME: this really belongs in a header somewhere since it is actually used.
extern uint32_t determinePublicIPAutomatically( ClusterConnectionString const& ccs );

extern const char* getHGVersion();

extern IRandom* trace_random;
extern void flushTraceFileVoid();

extern bool noUnseed;
extern const int MAX_CLUSTER_FILE_BYTES;

#ifdef ALLOC_INSTRUMENTATION
extern uint8_t *g_extra_memory;
#endif

bool enableFailures = true;

#define test_assert(x) if (!(x)) { cout << "Test failed: " #x << endl; return false; }

template <class X> vector<X> vec( X x ) { vector<X> v; v.push_back(x); return v; }
template <class X> vector<X> vec( X x, X y ) { vector<X> v; v.push_back(x); v.push_back(y); return v; }
template <class X> vector<X> vec( X x, X y, X z ) { vector<X> v; v.push_back(x); v.push_back(y); v.push_back(z); return v; }

//KeyRange keyRange( const Key& a, const Key& b ) { return std::make_pair(a,b); }

vector< Standalone<VectorRef<DebugEntryRef>> > debugEntries;
int64_t totalDebugEntriesSize = 0;

#if CENABLED(0, NOT_IN_CLEAN)
StringRef debugKey = LiteralStringRef( "" );
StringRef debugKey2 = LiteralStringRef( "\xff\xff\xff\xff" );

bool debugMutation( const char* context, Version version, MutationRef const& mutation ) {
	if ((mutation.type == mutation.SetValue || mutation.type == mutation.AddValue || mutation.type==mutation.DebugKey) && (mutation.param1 == debugKey || mutation.param1 == debugKey2))
		;//TraceEvent("MutationTracking").detail("At", context).detail("Version", version).detail("MutationType", "SetValue").detail("Key", printable(mutation.param1)).detail("Value", printable(mutation.param2));
	else if ((mutation.type == mutation.ClearRange || mutation.type == mutation.DebugKeyRange) && ((mutation.param1<=debugKey && mutation.param2>debugKey) || (mutation.param1<=debugKey2 && mutation.param2>debugKey2)))
		;//TraceEvent("MutationTracking").detail("At", context).detail("Version", version).detail("MutationType", "ClearRange").detail("KeyBegin", printable(mutation.param1)).detail("KeyEnd", printable(mutation.param2));
	else
		return false;
	const char* type =
		mutation.type == MutationRef::SetValue ? "SetValue" :
		mutation.type == MutationRef::ClearRange ? "ClearRange" :
		mutation.type == MutationRef::AddValue ? "AddValue" :
		mutation.type == MutationRef::DebugKeyRange ? "DebugKeyRange" :
		mutation.type == MutationRef::DebugKey ? "DebugKey" :
		"UnknownMutation";
	printf("DEBUGMUTATION:\t%.6f\t%s\t%s\t%lld\t%s\t%s\t%s\n", now(), g_network->getLocalAddress().toString().c_str(), context, version, type, printable(mutation.param1).c_str(), printable(mutation.param2).c_str());

	return true;
}

bool debugKeyRange( const char* context, Version version, KeyRangeRef const& keys ) {
	if (keys.contains(debugKey) || keys.contains(debugKey2)) {
		debugMutation(context, version, MutationRef(MutationRef::DebugKeyRange, keys.begin, keys.end) );
		//TraceEvent("MutationTracking").detail("At", context).detail("Version", version).detail("KeyBegin", printable(keys.begin)).detail("KeyEnd", printable(keys.end));
		return true;
	} else
		return false;
}

#elif CENABLED(0, NOT_IN_CLEAN)
bool debugMutation( const char* context, Version version, MutationRef const& mutation ) {
	if (!debugEntries.size() || debugEntries.back().size() >= 1000) {
		if (debugEntries.size()) totalDebugEntriesSize += debugEntries.back().arena().getSize() + sizeof(debugEntries.back());
		debugEntries.push_back(Standalone<VectorRef<DebugEntryRef>>());
		TraceEvent("DebugMutationBuffer").detail("Bytes", totalDebugEntriesSize);
	}
	auto& v = debugEntries.back();
	v.push_back_deep( v.arena(), DebugEntryRef(context, version, mutation) );

	return false;	// No auxiliary logging
}

bool debugKeyRange( const char* context, Version version, KeyRangeRef const& keys ) {
	return debugMutation( context, version, MutationRef(MutationRef::DebugKeyRange, keys.begin, keys.end) );
}

#else // Default implementation.
bool debugMutation( const char* context, Version version, MutationRef const& mutation ) { return false; }
bool debugKeyRange( const char* context, Version version, KeyRangeRef const& keys ) { return false; }
#endif

Future<Void> debugQueryServer( DebugQueryRequest const& req ) {
	Standalone<VectorRef<DebugEntryRef>> reply;

	for(auto v = debugEntries.begin(); v != debugEntries.end(); ++v)
		for(auto m = v->begin(); m != v->end(); ++m) {
			if (m->mutation.type == m->mutation.ClearRange || m->mutation.type == m->mutation.DebugKeyRange) {
				if (!KeyRangeRef(m->mutation.param1, m->mutation.param2).contains( req.search ))
					continue;
			} else if (m->mutation.type == m->mutation.SetValue) {
				if (m->mutation.param1 != req.search)
					continue;
			}
			reply.push_back( reply.arena(), *m );
		}

	req.reply.send(reply);
	return Void();
}

auto sortByTime = [](DebugEntryRef const& a, DebugEntryRef const& b) { return a.time < b.time; };

/*ACTOR Future<Void> debugSearchMutationCluster( ZookeeperInterface zk, Key key ) {
	state ZKWatch<ClusterControllerFullInterface> ccWatch(zk, LiteralStringRef("ClusterController"));
	state ClusterControllerFullInterface cc = wait( ccWatch.get() );

	ASSERT( ccWatch.getLastVersion() );

	Optional<vector<WorkerInterface>> workerList = wait( cc.getWorkers.tryGetReply( GetWorkersRequest() ) );
	if( !workerList.present() ) {
		printf("ERROR: CC interface not in ZK\n");
		return Void();
	}
	state vector<WorkerInterface> workers = workerList.get();

	state vector<Future<Standalone<VectorRef<DebugEntryRef>>>> replies( workers.size() );
	for(int w=0; w<workers.size(); w++) {
		DebugQueryRequest req;
		req.search = key;
		replies[w] = timeoutError(workers[w].debugQuery.getReply( req ), 5.0);
	}
	//state vector<Standalone<VectorRef<DebugEntryRef>>> result = wait( getAll( replies ) );
	Void _ = wait(waitForAllReady( replies ));
	state vector<Standalone<VectorRef<DebugEntryRef>>> result( workers.size() );
	for(int r=0; r<result.size(); r++) {
		if (replies[r].isError())
			printf("ERROR: Couldn't get results from '%s'\n", workers[r].debugQuery.getEndpoint().address.toString().c_str());
		else
			result[r] = replies[r].get();
	}
	ASSERT( result.size() == workers.size() );

	Standalone<VectorRef<DebugEntryRef>> all;
	for(int r=0; r<result.size(); r++)
		all.append( all.arena(), &result[r][0], result[r].size() );
	std::sort( all.begin(), all.end(), sortByTime );
	printf("\n\n");
	for(auto e = all.begin(); e != all.end(); ++e) {
		const char* type =
			e->mutation.type == MutationRef::SetValue ? "SetValue" :
			e->mutation.type == MutationRef::ClearRange ? "ClearRange" :
			e->mutation.type == MutationRef::DebugKeyRange ? "DebugKeyRange" :
			"UnknownMutation";
		printf("%.6f\t%s\t%s\t%lld\t%s\t%s\t%s\n", e->time, e->address.toString().c_str(), e->context.toString().c_str(), e->version, type, printable(e->mutation.param1).c_str(), printable(e->mutation.param2).c_str());
	}
	printf("\n\n");

	return Void();
}*/

#ifdef _WIN32
#include <sddl.h>

// It is your
//    responsibility to properly initialize the
//    structure and to free the structure's
//    lpSecurityDescriptor member when you have
//    finished using it. To free the structure's
//    lpSecurityDescriptor member, call the
//    LocalFree function.
BOOL CreatePermissiveReadWriteDACL(SECURITY_ATTRIBUTES * pSA)
{
	UNSTOPPABLE_ASSERT( pSA != NULL );

	TCHAR * szSD = TEXT("D:")        // Discretionary ACL
		TEXT("(A;OICI;GR;;;AU)")     // Allow read/write/execute to authenticated users
		TEXT("(A;OICI;GA;;;BA)");    // Allow full control to administrators

	return ConvertStringSecurityDescriptorToSecurityDescriptor(
			szSD,
			SDDL_REVISION_1,
			&(pSA->lpSecurityDescriptor),
			NULL);
}
#endif

class WorldReadablePermissions {
public:
	WorldReadablePermissions() {
#ifdef _WIN32
		sa.nLength = sizeof(SECURITY_ATTRIBUTES);
		sa.bInheritHandle = FALSE;
		if( !CreatePermissiveReadWriteDACL(&sa) ) {
			TraceEvent("Win32DACLCreationFail").GetLastError();
			throw platform_error();
		}
		permission.set_permissions( &sa );
#elif (defined(__linux__) || defined(__APPLE__))
		// There is nothing to do here, since the default permissions are fine
#else
		#error Port me!
#endif
	}

	virtual ~WorldReadablePermissions() {
#ifdef _WIN32
		LocalFree( sa.lpSecurityDescriptor );
#elif (defined(__linux__) || defined(__APPLE__))
		// There is nothing to do here, since the default permissions are fine
#else
		#error Port me!
#endif
	}

	boost::interprocess::permissions permission;

private:
	WorldReadablePermissions(const WorldReadablePermissions &rhs) {}
#ifdef _WIN32
	SECURITY_ATTRIBUTES sa;
#endif
};

UID getSharedMemoryMachineId() {
	UID *machineId = NULL;
	int numTries = 0;

	// Permissions object defaults to 0644 on *nix, but on windows defaults to allowing access to only the creator.
	// On windows, this means that we have to create an elaborate workaround for DACLs
	WorldReadablePermissions p;

	loop {
		try {
			// "0" is the default parameter "addr"
			boost::interprocess::managed_shared_memory segment(boost::interprocess::open_or_create, "fdbserver", 1000, 0, p.permission);
			machineId = segment.find_or_construct<UID>("machineId")(g_random->randomUniqueID());
			if (!machineId)
				criticalError(FDB_EXIT_ERROR, "SharedMemoryError", "Could not locate or create shared memory - 'machineId'");
			return *machineId;
		}
		catch (boost::interprocess::interprocess_exception &) {
			try {
				//If the shared memory already exists, open it read-only in case it was created by another user
				boost::interprocess::managed_shared_memory segment(boost::interprocess::open_read_only, "fdbserver");
				machineId = segment.find<UID>("machineId").first;
				if (!machineId)
					criticalError(FDB_EXIT_ERROR, "SharedMemoryError", "Could not locate shared memory - 'machineId'");
				return *machineId;
			}
			catch (boost::interprocess::interprocess_exception &ex) {
				//Retry in case the shared memory was deleted in between the call to open_or_create and open_read_only
				//Don't keep trying forever in case this is caused by some other problem
				if (++numTries == 10)
					criticalError(FDB_EXIT_ERROR, "SharedMemoryError", format("Could not open shared memory - %s", ex.what()).c_str());
			}
		}
	}
}


ACTOR void failAfter( Future<Void> trigger, ISimulator::ProcessInfo* m = g_simulator.getCurrentProcess() ) {
	Void _ = wait( trigger );
	if (enableFailures) {
		printf("Killing machine: %s at %f\n", m->address.toString().c_str(), now());
		g_simulator.killProcess( m, ISimulator::KillInstantly );
	}
}

void failAfter( Future<Void> trigger, Endpoint e ) {
	if (g_network == &g_simulator)
		failAfter( trigger, g_simulator.getProcess( e ) );
}

void testSerializationSpeed() {
	double tstart;
	double build = 0, serialize = 0, deserialize = 0, copy = 0, deallocate = 0;
	double bytes = 0;
	double testBegin = timer();
	for(int a=0; a<10000; a++) {
		{
			tstart = timer();

			Arena batchArena;
			VectorRef< CommitTransactionRef > batch;
			batch.resize( batchArena, 1000 );
			for(int t=0; t<batch.size(); t++) {
				CommitTransactionRef &tr = batch[t];
				tr.read_snapshot = 0;
				for(int i=0; i<2; i++)
					tr.mutations.push_back_deep( batchArena,
						MutationRef( MutationRef::SetValue, LiteralStringRef("KeyABCDE"), LiteralStringRef("SomeValu") ) );
				tr.mutations.push_back_deep( batchArena,
						MutationRef( MutationRef::ClearRange, LiteralStringRef("BeginKey"), LiteralStringRef("EndKeyAB") ));
			}

			build += timer()-tstart;

			tstart = timer();

			BinaryWriter wr( IncludeVersion() );
			wr << batch;

			bytes += wr.getLength();

			serialize += timer() - tstart;

			for(int i=0; i<1; i++) {
				tstart = timer();
				Arena arena;
				StringRef data( arena, StringRef( (const uint8_t*)wr.getData(), wr.getLength() ) );
				copy += timer() - tstart;

				tstart = timer();
				ArenaReader rd( arena, data, IncludeVersion() );
				VectorRef<CommitTransactionRef> batch2;
				rd >> arena >> batch2;

				deserialize += timer() - tstart;
			}

			tstart = timer();
		}
		deallocate += timer() - tstart;
	}
	double elapsed = (timer()-testBegin);
	printf("Test speed: %0.1f MB/sec (%0.0f/sec)\n", bytes/1e6/elapsed, 1000000/elapsed);
	printf("  Build: %0.1f MB/sec\n", bytes/1e6/build);
	printf("  Serialize: %0.1f MB/sec\n", bytes/1e6/serialize);
	printf("  Copy: %0.1f MB/sec\n", bytes/1e6/copy);
	printf("  Deserialize: %0.1f MB/sec\n", bytes/1e6/deserialize);
	printf("  Deallocate: %0.1f MB/sec\n", bytes/1e6/deallocate);
	printf("  Bytes: %0.1f MB\n", bytes/1e6);
	printf("\n");
}

std::string toHTML( const StringRef& binaryString ) {
	std::string s;

	for(int i=0; i<binaryString.size(); i++) {
		uint8_t c = binaryString[i];
		if (c == '<') s += "&lt;";
		else if (c == '>') s += "&gt;";
		else if (c == '&') s += "&amp;";
		else if (c == '"') s += "&quot;";
		else if (c == ' ') s += "&nbsp;";
		else if (c > 32 && c < 127) s += c;
		else s += format("<span class=\"binary\">[%02x]</span>", c);
	}

	return s;
}

ACTOR Future<Void> dumpDatabase( Database cx, std::string outputFilename, KeyRange range = allKeys ) {
	try {
		state Transaction tr( cx );
		loop {
			state FILE* output = fopen(outputFilename.c_str(), "wt");
			try {
				state KeySelectorRef iter = firstGreaterOrEqual( range.begin );
				state Arena arena;
				fprintf(output, "<html><head><style type=\"text/css\">.binary {color:red}</style></head><body>\n");
				Version ver = wait( tr.getReadVersion() );
				fprintf(output, "<h3>Database version: %lld</h3>", ver);

				loop {
					Standalone<RangeResultRef> results = wait(
						tr.getRange( iter, firstGreaterOrEqual( range.end ), 1000 ) );
					for(int r=0; r<results.size(); r++) {
						std::string key = toHTML( results[r].key ), value = toHTML( results[r].value );
						fprintf( output, "<p>%s <b>:=</b> %s</p>\n", key.c_str(), value.c_str() );
					}
					if (results.size() < 1000) break;
					iter = firstGreaterThan( KeyRef(arena, results[ results.size()-1 ].key) );
				}
				fprintf(output, "</body></html>");
				fclose(output);
				TraceEvent("DatabaseDumped").detail("Filename", outputFilename);
				return Void();
			} catch (Error& e) {
				fclose(output);
				Void _ = wait( tr.onError(e) );
			}
		}
	} catch (Error& e) {
		TraceEvent(SevError,"dumpDatabaseError").error(e).detail("Filename", outputFilename);
		throw;
	}
}

void memoryTest();
void skipListTest();

Future<Void> startSystemMonitor(std::string dataFolder, Optional<Standalone<StringRef>> zoneId, Optional<Standalone<StringRef>> machineId) {
	initializeSystemMonitorMachineState(SystemMonitorMachineState(dataFolder, zoneId, machineId, g_network->getLocalAddress().ip));

	systemMonitor();
	return recurring( &systemMonitor, 5.0, TaskFlushTrace );
}

void testIndexedSet();

#ifdef _WIN32
void parentWatcher(void *parentHandle) {
	HANDLE parent = (HANDLE)parentHandle;
	int signal = WaitForSingleObject( parent, INFINITE );
	CloseHandle( parentHandle );
	if( signal == WAIT_OBJECT_0 )
		criticalError( FDB_EXIT_SUCCESS, "ParentProcessExited", "Parent process exited" );
	TraceEvent(SevError, "ParentProcessWaitFailed").detail("RetCode", signal).GetLastError();
}
#endif

static void printVersion() {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("source version %s\n", getHGVersion());
	printf("protocol %llx\n", currentProtocolVersion);
}

static void printHelpTeaser( const char *name ) {
	fprintf(stderr, "Try `%s --help' for more information.\n", name);
}

static void printUsage( const char *name, bool devhelp ) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s -p ADDRESS [OPTIONS]\n\n", name);
	printf("  -p ADDRESS, --public_address ADDRESS\n"
		   "                 Public address, specified as `IP_ADDRESS:PORT' or `auto:PORT'.\n");
	printf("  -l ADDRESS, --listen_address ADDRESS\n"
		   "                 Listen address, specified as `IP_ADDRESS:PORT' (defaults to\n");
	printf("                 public address).\n");
	printf("  -C CONNFILE, --cluster_file CONNFILE\n"
		   "                 The path of a file containing the connection string for the\n"
		   "                 FoundationDB cluster. The default is first the value of the\n"
		   "                 FDB_CLUSTER_FILE environment variable, then `./fdb.cluster',\n"
		   "                 then `%s'.\n", platform::getDefaultClusterFilePath().c_str());
	printf("  --seed_cluster_file SEEDCONNFILE\n"
		   "                 The path of a seed cluster file which will be used to connect\n"
		   "                 if the -C cluster file does not exist. If the server connects\n"
		   "                 successfully using the seed file, then it copies the file to\n"
		   "                 the -C file location.\n");
	printf("  --seed_connection_string SEEDCONNSTRING\n"
		   "                 The path of a seed connection string which will be used to connect\n"
		   "                 if the -C cluster file does not exist. If the server connects\n"
		   "                 successfully using the seed string, then it copies the string to\n"
		   "                 the -C file location.\n");
#ifdef __linux__
	printf("  --data_filesystem PATH\n"
		   "                 Turns on validation that all data files are written to a drive\n"
		   "                 mounted at the specified PATH. This checks that the device at PATH\n"
		   "                 is currently mounted and that any data files get written to the\n"
		   "                 same device.\n");
#endif
	printf("  -d PATH, --datadir PATH\n"
		   "                 Store data files in the given folder (must be unique for each\n");
	printf("                 fdbserver instance on a given machine).\n");
	printf("  -L PATH, --logdir PATH\n"
		   "                 Store log files in the given folder (default is `.').\n");
	printf("  --logsize SIZE Roll over to a new log file after the current log file\n"
		   "                 exceeds SIZE bytes. The default value is 10MiB.\n");
	printf("  --maxlogs SIZE, --maxlogssize SIZE\n"
		   "                 Delete the oldest log file when the total size of all log\n"
		   "                 files exceeds SIZE bytes. If set to 0, old log files will not\n"
		   "                 be deleted. The default value is 100MiB.\n");
	printf("  -i ID, --machine_id ID\n"
		   "                 Machine identifier key (up to 16 hex characters). Defaults\n"
		   "                 to a random value shared by all fdbserver processes on this\n"
		   "                 machine.\n");
	printf("  -a ID, --datacenter_id ID\n"
		   "                 Data center identifier key (up to 16 hex characters).\n");
	printf("  -c CLASS, --class CLASS\n"
		   "                 Machine class (valid options are storage, transaction,\n"
		   "                 resolution, proxy, master, test, unset, stateless, log, router,\n"
		   "                 and cluster_controller).\n");
	printf(TLS_HELP);
	printf("  -v, --version  Print version information and exit.\n");
	printf("  -h, -?, --help Display this help and exit.\n");
	if( devhelp ) {
		printf("  -r ROLE, --role ROLE\n"
			   "                 Server role (valid options are fdbd, test, multitest,\n");
		printf("                 simulation, networktestclient, networktestserver,\n");
		printf("                 consistencycheck, kvfileintegritycheck, kvfilegeneratesums). The default is `fdbd'.\n");
#ifdef _WIN32
		printf("  -n, --newconsole\n"
			   "                 Create a new console.\n");
		printf("  -q, --no_dialog\n"
			   "                 Disable error dialog on crash.\n");
		printf("  --parentpid PID\n");
		printf("                 Specify a process after whose termination to exit.\n");
#endif
		printf("  -f TESTFILE, --testfile\n"
			   "                 Testfile to run, defaults to `tests/default.txt'.\n");
		printf("  -R, --restarting\n");
		printf("                 Restart a previous simulation that was cleanly shut down.\n");
		printf("  -s SEED, --seed SEED\n"
			   "                 Random seed.\n");
		printf("  -k KEY, --key KEY  Target key for search role.\n");
		printf("  -m SIZE, --memory SIZE\n"
			   "                 Memory limit. The default value is 8GiB. When specified\n"
			   "                 without a unit, MiB is assumed.\n");
		printf("  --kvfile FILE  Input file (SQLite database file) for use by the 'kvfilegeneratesums' and 'kvfileintegritycheck' roles.\n");
		printf("  -M SIZE, --storage_memory SIZE\n"
			   "                 Maximum amount of memory used for storage. The default\n"
			   "                 value is 1GiB. When specified without a unit, MB is\n"
			   "                 assumed.\n");
		printf("  -b [on,off], --buggify [on,off]\n"
			   "                 Sets Buggify system state, defaults to `off'.\n");
		printf("  --crash        Crash on serious errors instead of continuing.\n");
		printf("  -N NETWORKIMPL, --network NETWORKIMPL\n"
			   "                 Select network implementation, `net2' (default),\n");
		printf("                 `net2-threadpool'.\n");
		printf("  --unbufferedout\n");
		printf("                 Do not buffer stdout and stderr.\n");
		printf("  --bufferedout\n");
		printf("                 Buffer stdout and stderr.\n");
		printf("  --traceclock CLOCKIMPL\n");
		printf("                 Select clock source for trace files, `now' (default) or\n");
		printf("                 `realtime'.\n");
		printf("  --num_testers NUM\n");
		printf("                 A multitester will wait for NUM testers before starting\n");
		printf("                 (defaults to 1).\n");
		printf("  --testservers ADDRESSES\n");
		printf("                 The addresses of networktestservers\n");
		printf("                 specified as ADDRESS:PORT,ADDRESS:PORT...\n");
		printf("  --testonservers\n");
		printf("                 Testers are recruited on servers.\n");
		printf("  --metrics_cluster CONNFILE\n");
		printf("                 The cluster file designating where this process will\n");
		printf("                 store its metric data. By default metrics will be stored\n");
		printf("                 in the same database the process is participating in.\n");
		printf("  --metrics_prefix PREFIX\n");
		printf("                 The prefix where this process will store its metric data.\n");
		printf("                 Must be specified if using a different database for metrics.\n");
		printf("  --knob_KNOBNAME KNOBVALUE\n");
		printf("                 Changes a database knob. KNOBNAME should be lowercase.\n");
		printf("  --locality_LOCALITYKEY LOCALITYVALUE\n");
		printf("                 Define a locality key. LOCALITYKEY is case-insensitive though LOCALITYVALUE is not.\n");
		printf("  --io_trust_seconds SECONDS\n");
		printf("                 Sets the time in seconds that a read or write operation is allowed to take\n"
		       "                 before timing out with an error. If an operation times out, all future\n"
		       "                 operations on that file will fail with an error as well. Only has an effect\n"
		       "                 when using AsyncFileKAIO in Linux.\n");
		printf("  --io_trust_warn_only\n");
		printf("                 Instead of failing when an I/O operation exceeds io_trust_seconds, just\n"
		       "                 log a warning to the trace log. Has no effect if io_trust_seconds is unspecified.\n");
	} else {
		printf("  --dev-help     Display developer-specific help and exit.\n");
	}

	printf("\n"
		   "SIZE parameters may use one of the multiplicative suffixes B=1, KB=10^3,\n"
		   "KiB=2^10, MB=10^6, MiB=2^20, GB=10^9, GiB=2^30, TB=10^12, or TiB=2^40.\n");
}

extern bool g_crashOnError;

#if defined(ALLOC_INSTRUMENTATION) || defined(ALLOC_INSTRUMENTATION_STDOUT)
	void* operator new (std::size_t size) throw(std::bad_alloc) {
		void* p = malloc(size);
		if(!p)
			throw std::bad_alloc();
		recordAllocation( p, size );
		return p;
	}
	void operator delete (void* ptr) throw() {
		recordDeallocation( ptr );
		free( ptr );
	}

	//scalar, nothrow new and it matching delete
	void* operator new (std::size_t size,const std::nothrow_t&) throw() {
		void* p = malloc(size);
		recordAllocation( p, size );
		return p;
	}
	void operator delete (void* ptr, const std::nothrow_t&) throw() {
		recordDeallocation( ptr );
		free( ptr );
	}

	//array throwing new and matching delete[]
	void* operator new  [](std::size_t size) throw(std::bad_alloc) {
		void* p = malloc(size);
		if(!p)
			throw std::bad_alloc();
		recordAllocation( p, size );
		return p;
	}
	void operator delete[](void* ptr) throw() {
		recordDeallocation( ptr );
		free( ptr );
	}

	//array, nothrow new and matching delete[]
	void* operator new [](std::size_t size, const std::nothrow_t&) throw() {
		void* p = malloc(size);
		recordAllocation( p, size );
		return p;
	}
	void operator delete[](void* ptr, const std::nothrow_t&) throw() {
		recordDeallocation( ptr );
		free( ptr );
	}
#endif

Optional<bool> checkBuggifyOverride(const char *testFile) {
	std::ifstream ifs;
	ifs.open(testFile, std::ifstream::in);
	if (!ifs.good())
		return 0;

	std::string cline;

	while (ifs.good()) {
		getline(ifs, cline);
		std::string line = removeWhitespace(std::string(cline));
		if (!line.size() || line.find(';') == 0)
			continue;

		size_t found = line.find('=');
		if (found == std::string::npos)
			// hmmm, not good
			continue;
		std::string attrib = removeWhitespace(line.substr(0, found));
		std::string value = removeWhitespace(line.substr(found + 1));

		if (attrib == "buggify") {
			if( !strcmp( value.c_str(), "on" ) ) {
				ifs.close();
				return true;
			} else if( !strcmp( value.c_str(), "off" ) ) {
				ifs.close();
				return false;
			} else {
				fprintf(stderr, "ERROR: Unknown buggify override state `%s'\n", value.c_str());
				flushAndExit(FDB_EXIT_ERROR);
			}
		}
	}

	ifs.close();
	return Optional<bool>();
}

int main(int argc, char* argv[]) {
	try {
		platformInit();
		initSignalSafeUnwind();

#ifdef ALLOC_INSTRUMENTATION
		g_extra_memory = new uint8_t[1000000];
#endif
		registerCrashHandler();

		// Set default of line buffering standard out and error
		setvbuf(stdout, NULL, _IOLBF, BUFSIZ);
		setvbuf(stderr, NULL, _IOLBF, BUFSIZ);

		//Enables profiling on this thread (but does not start it)
		registerThreadForProfiling();
		
		std::string commandLine;
		for (int a = 0; a<argc; a++) {
			if (a) commandLine += ' ';
			commandLine += argv[a];
		}

		CSimpleOpt args(argc, argv, g_rgOptions, SO_O_EXACT);

		enum Role {
			Simulation,
			FDBD,
			Test,
			MultiTester,
			SkipListTest,
			SearchMutations,
			DSLTest,
			VersionedMapTest,
			CreateTemplateDatabase,
			NetworkTestClient,
			NetworkTestServer,
			KVFileIntegrityCheck,
			KVFileGenerateIOLogChecksums,
			ConsistencyCheck
		};
		std::string fileSystemPath = "", dataFolder, connFile = "", seedConnFile = "", seedConnString = "", logFolder = ".", metricsConnFile = "", metricsPrefix = "";
		std::string logGroup = "default";
		Role role = FDBD;
		uint32_t randomSeed = platform::getRandomSeed();

		const char *testFile = "tests/default.txt";
		std::string kvFile;
		std::string publicAddressStr, listenAddressStr = "public";
		std::string testServersStr;
		NetworkAddress publicAddress, listenAddress;
		const char *targetKey = NULL;
		uint64_t memLimit = 8LL << 30;
		uint64_t storageMemLimit = 1LL << 30;
		bool buggifyEnabled = false, machineIdOverride = false, restarting = false;
		Optional<Standalone<StringRef>> zoneId;
		Optional<Standalone<StringRef>> dcId;
		ProcessClass processClass = ProcessClass( ProcessClass::UnsetClass, ProcessClass::CommandLineSource );
		bool useNet2 = true;
		bool useThreadPool = false;
		uint64_t rollsize = TRACE_DEFAULT_ROLL_SIZE;
		uint64_t maxLogsSize = TRACE_DEFAULT_MAX_LOGS_SIZE;
		bool maxLogsSizeSet = false;
		int maxLogs = 0;
		bool maxLogsSet = false;
		std::vector<std::pair<std::string, std::string>> knobs;
		LocalityData localities;
		int minTesterCount = 1;
		bool testOnServers = false;

		Reference<TLSOptions> tlsOptions = Reference<TLSOptions>( new TLSOptions );
		std::string tlsCertPath, tlsKeyPath, tlsVerifyPeers;
		double fileIoTimeout = 0.0;
		bool fileIoWarnOnly = false;

		if( argc == 1 ) {
			printUsage(argv[0], false);
			flushAndExit(FDB_EXIT_ERROR);
		}

	#ifdef _WIN32
		// Windows needs a gentle nudge to format floats correctly
		//_set_output_format(_TWO_DIGIT_EXPONENT);
	#endif

		while (args.Next()) {
			if (args.LastError() == SO_ARG_INVALID_DATA) {
				fprintf(stderr, "ERROR: invalid argument to option `%s'\n", args.OptionText());
				printHelpTeaser(argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
			if (args.LastError() == SO_ARG_INVALID) {
				fprintf(stderr, "ERROR: argument given for option `%s'\n", args.OptionText());
				printHelpTeaser(argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
			if (args.LastError() == SO_ARG_MISSING) {
				fprintf(stderr, "ERROR: missing argument for option `%s'\n", args.OptionText());
				printHelpTeaser(argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
			if (args.LastError() == SO_OPT_INVALID) {
				fprintf(stderr, "ERROR: unknown option: `%s'\n", args.OptionText());
				printHelpTeaser(argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
			if (args.LastError() != SO_SUCCESS) {
				fprintf(stderr, "ERROR: error parsing options\n");
				printHelpTeaser(argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
			const char *sRole;
			Optional<uint64_t> ti;

			switch (args.OptionId()) {
				case OPT_HELP:
					printUsage(argv[0], false);
					flushAndExit(FDB_EXIT_SUCCESS);
					break;
				case OPT_DEVHELP:
					printUsage(argv[0], true);
					flushAndExit(FDB_EXIT_SUCCESS);
					break;
				case OPT_KNOB: {
					std::string syn = args.OptionSyntax();
					if (!StringRef(syn).startsWith(LiteralStringRef("--knob_"))) {
						fprintf(stderr, "ERROR: unable to parse knob option '%s'\n", syn.c_str());
						flushAndExit(FDB_EXIT_ERROR);
					}
					syn = syn.substr(7);
					knobs.push_back( std::make_pair( syn, args.OptionArg() ) );
					break;
					}
				case OPT_LOCALITY: {
					std::string syn = args.OptionSyntax();
					if (!StringRef(syn).startsWith(LiteralStringRef("--locality_"))) {
						fprintf(stderr, "ERROR: unable to parse locality key '%s'\n", syn.c_str());
						flushAndExit(FDB_EXIT_ERROR);
					}
					syn = syn.substr(11);
					std::transform(syn.begin(), syn.end(), syn.begin(), ::tolower);
					localities.set(Standalone<StringRef>(syn), Standalone<StringRef>(std::string(args.OptionArg())));
					break;
					}
				case OPT_VERSION:
					printVersion();
					flushAndExit(FDB_EXIT_SUCCESS);
					break;
				case OPT_NOBUFSTDOUT:
					setvbuf(stdout, NULL, _IONBF, 0);
					setvbuf(stderr, NULL, _IONBF, 0);
					break;
				case OPT_BUFSTDOUTERR:
					setvbuf(stdout, NULL, _IOFBF, BUFSIZ);
					setvbuf(stderr, NULL, _IOFBF, BUFSIZ);
					break;
				case OPT_ROLE:
					sRole = args.OptionArg();
					if (!strcmp(sRole, "fdbd")) role = FDBD;
					else if (!strcmp(sRole, "simulation")) role = Simulation;
					else if (!strcmp(sRole, "test")) role = Test;
					else if (!strcmp(sRole, "multitest")) role = MultiTester;
					else if (!strcmp(sRole, "skiplisttest")) role = SkipListTest;
					else if (!strcmp(sRole, "search")) role = SearchMutations;
					else if (!strcmp(sRole, "dsltest")) role = DSLTest;
					else if (!strcmp(sRole, "versionedmaptest")) role = VersionedMapTest;
					else if (!strcmp(sRole, "createtemplatedb")) role = CreateTemplateDatabase;
					else if (!strcmp(sRole, "networktestclient")) role = NetworkTestClient;
					else if (!strcmp(sRole, "networktestserver")) role = NetworkTestServer;
					else if (!strcmp(sRole, "kvfileintegritycheck")) role = KVFileIntegrityCheck;
					else if (!strcmp(sRole, "kvfilegeneratesums")) role = KVFileGenerateIOLogChecksums;
					else if (!strcmp(sRole, "consistencycheck")) role = ConsistencyCheck;
					else {
						fprintf(stderr, "ERROR: Unknown role `%s'\n", sRole);
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					break;
				case OPT_PUBLICADDR:
					publicAddressStr = args.OptionArg();
					break;
				case OPT_LISTEN:
					listenAddressStr = args.OptionArg();
					break;
				case OPT_CONNFILE:
					connFile = args.OptionArg();
					break;
				case OPT_LOGGROUP:
					logGroup = args.OptionArg();
					break;
				case OPT_SEEDCONNFILE:
					seedConnFile = args.OptionArg();
					break;
				case OPT_SEEDCONNSTRING:
					seedConnString = args.OptionArg();
					break;
	#ifdef __linux__
				case OPT_FILESYSTEM: {
					fileSystemPath = args.OptionArg();
					break;
				}
	#endif
				case OPT_DATAFOLDER:
					dataFolder = args.OptionArg();
					break;
				case OPT_LOGFOLDER:
					logFolder = args.OptionArg();
					break;
				case OPT_NETWORKIMPL: {
					const char* a = args.OptionArg();
					if (!strcmp(a, "net2")) useNet2 = true;
					else if (!strcmp(a, "net2-threadpool")) { useNet2 = true; useThreadPool = true; }
					else {
						fprintf(stderr, "ERROR: Unknown network implementation `%s'\n", a);
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					break;
				}
				case OPT_TRACECLOCK: {
					const char* a = args.OptionArg();
					if (!strcmp(a, "realtime")) g_trace_clock = TRACE_CLOCK_REALTIME;
					else if (!strcmp(a, "now")) g_trace_clock = TRACE_CLOCK_NOW;
					else {
						fprintf(stderr, "ERROR: Unknown clock source `%s'\n", a);
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					break;
				}
				case OPT_NUMTESTERS: {
					const char* a = args.OptionArg();
					if( !sscanf(a, "%d", &minTesterCount) ) {
						fprintf(stderr, "ERROR: Could not parse numtesters `%s'\n", a);
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					break;
				}
				case OPT_ROLLSIZE: {
					const char* a = args.OptionArg();
					ti = parse_with_suffix(a);
					if (!ti.present()) {
						fprintf(stderr, "ERROR: Could not parse logsize `%s'\n", a);
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					rollsize = ti.get();
					break;
				}
				case OPT_MAXLOGSSIZE: {
					const char *a = args.OptionArg();
					ti = parse_with_suffix(a);
					if (!ti.present()) {
						fprintf(stderr, "ERROR: Could not parse maxlogssize `%s'\n", a);
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					maxLogsSize = ti.get();
					maxLogsSizeSet = true;
					break;
				}
				case OPT_MAXLOGS: {
					const char *a = args.OptionArg();
					char *end;
					maxLogs = strtoull(a, &end, 10);
					if(*end) {
						fprintf(stderr, "ERROR: Unrecognized maximum number of logs `%s'\n", a);
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					maxLogsSet = true;
					break;
				}
	#ifdef _WIN32
				case OPT_PARENTPID: {
					auto pid_str = args.OptionArg();
					int parent_pid = atoi(pid_str);
					auto pHandle = OpenProcess( SYNCHRONIZE, FALSE, parent_pid );
					if( !pHandle ) {
						TraceEvent("ParentProcessOpenError").GetLastError();
						fprintf(stderr, "Could not open parent process at pid %d (error %d)", parent_pid, GetLastError());
						throw platform_error();
					}
					startThread(&parentWatcher, pHandle);
					break;
				}
				case OPT_NEWCONSOLE:
					FreeConsole();
					AllocConsole();
					freopen("CONIN$","rb",stdin);
					freopen("CONOUT$","wb",stdout);
					freopen("CONOUT$","wb",stderr);
					break;
				case OPT_NOBOX:
					SetErrorMode(SetErrorMode(0) | SEM_NOGPFAULTERRORBOX);
					break;
	#endif
				case OPT_TESTFILE:
					testFile = args.OptionArg();
					break;
				case OPT_KVFILE:
					kvFile = args.OptionArg();
					break;
				case OPT_RESTARTING:
					restarting = true;
					break;
				case OPT_RANDOMSEED: {
					char* end;
					randomSeed = (uint32_t)strtoul( args.OptionArg(), &end, 10 );
					if( *end ) {
						fprintf(stderr, "ERROR: Could not parse random seed `%s'\n", args.OptionArg());
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					break;
				}
				case OPT_MACHINEID: {
					zoneId = std::string(args.OptionArg());
					break;
				}
				case OPT_DCID: {
					dcId = std::string(args.OptionArg());
					break;
				}
				case OPT_MACHINE_CLASS:
					sRole = args.OptionArg();
					processClass = ProcessClass( sRole, ProcessClass::CommandLineSource );
					if (processClass == ProcessClass::InvalidClass) {
						fprintf(stderr, "ERROR: Unknown machine class `%s'\n", sRole);
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					break;
				case OPT_KEY:
					targetKey = args.OptionArg();
					break;
				case OPT_MEMLIMIT:
					ti = parse_with_suffix(args.OptionArg(), "MiB");
					if (!ti.present()) {
						fprintf(stderr, "ERROR: Could not parse memory limit from `%s'\n", args.OptionArg());
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					memLimit = ti.get();
					break;
				case OPT_STORAGEMEMLIMIT:
					ti = parse_with_suffix(args.OptionArg(), "MB");
					if (!ti.present()) {
						fprintf(stderr, "ERROR: Could not parse storage memory limit from `%s'\n", args.OptionArg());
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					storageMemLimit = ti.get();
					break;
				case OPT_BUGGIFY:
					if( !strcmp( args.OptionArg(), "on" ) )
						buggifyEnabled = true;
					else if( !strcmp( args.OptionArg(), "off" ) )
						buggifyEnabled = false;
					else {
						fprintf(stderr, "ERROR: Unknown buggify state `%s'\n", args.OptionArg());
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					break;
				case OPT_CRASHONERROR:
					g_crashOnError = true;
					break;
				case OPT_TESTSERVERS:
					testServersStr = args.OptionArg();
					break;
				case OPT_TEST_ON_SERVERS:
					testOnServers = true;
					break;
				case OPT_METRICSCONNFILE:
					metricsConnFile = args.OptionArg();
					break;
				case OPT_METRICSPREFIX:
					metricsPrefix = args.OptionArg();
					break;
				case OPT_IO_TRUST_SECONDS: {
					const char* a = args.OptionArg();
					if( !sscanf(a, "%lf", &fileIoTimeout) ) {
						fprintf(stderr, "ERROR: Could not parse io_trust_seconds `%s'\n", a);
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					break;
				}
				case OPT_IO_TRUST_WARN_ONLY:
					fileIoWarnOnly = true;
					break;
				case TLSOptions::OPT_TLS_PLUGIN:
					try {
						tlsOptions->set_plugin_name_or_path( args.OptionArg() );
					} catch (Error& e) {
						fprintf(stderr, "ERROR: cannot load TLS plugin `%s' (%s)\n", args.OptionArg(), e.what());
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					break;
				case TLSOptions::OPT_TLS_CERTIFICATES:
					tlsCertPath = args.OptionArg();
					break;
				case TLSOptions::OPT_TLS_KEY:
					tlsKeyPath = args.OptionArg();
					break;
				case TLSOptions::OPT_TLS_VERIFY_PEERS:
					tlsVerifyPeers = args.OptionArg();
					break;
			}
		}

		if (seedConnString.length() && seedConnFile.length()) {
			fprintf(stderr, "%s\n", "--seed_cluster_file and --seed_connection_string may not both be specified at once.");
			return FDB_EXIT_ERROR;
		}

		bool seedSpecified = seedConnFile.length() || seedConnString.length();

		if (seedSpecified && !connFile.length()){
			fprintf(stderr, "%s\n", "If -seed_cluster_file or --seed_connection_string is specified, -C must be specified as well.");
			return FDB_EXIT_ERROR;
		}

		if( metricsConnFile == connFile )
			metricsConnFile = "";

		if( metricsConnFile != "" &&  metricsPrefix == "" ) {
			fprintf(stderr, "If a metrics cluster file is specified, a metrics prefix is required.\n");
			return FDB_EXIT_ERROR;
		}

		bool autoPublicAddress = StringRef(publicAddressStr).startsWith(LiteralStringRef("auto:"));

		Reference<ClusterConnectionFile> connectionFile;
		if ( (role != Simulation && role != CreateTemplateDatabase && role != KVFileIntegrityCheck && role != KVFileGenerateIOLogChecksums) || autoPublicAddress ) {

				if (seedSpecified && !fileExists(connFile)){
					std::string connectionString = seedConnString.length() ? seedConnString : "";
					ClusterConnectionString ccs;
					if(seedConnFile.length()) {
						try {
							connectionString = readFileBytes(seedConnFile, MAX_CLUSTER_FILE_BYTES);
						}
						catch (Error& e) {
							fprintf(stderr, "%s\n", ClusterConnectionFile::getErrorString(std::make_pair(seedConnFile, false), e).c_str());
							throw;
						}
					}

					try {
						ccs = ClusterConnectionString(connectionString);
					}
					catch (Error& e) {
						fprintf(stderr, "%s\n", ClusterConnectionString::getErrorString(connectionString, e).c_str());
						throw;
					}
					connectionFile = Reference<ClusterConnectionFile>(new ClusterConnectionFile(connFile, ccs));
				}
				else {
					std::pair<std::string, bool> resolvedClusterFile;
					try {
						resolvedClusterFile = ClusterConnectionFile::lookupClusterFileName(connFile);
						connectionFile = Reference<ClusterConnectionFile>(new ClusterConnectionFile(resolvedClusterFile.first));
					} catch (Error& e) {
						fprintf(stderr, "%s\n", ClusterConnectionFile::getErrorString(resolvedClusterFile, e).c_str());
						throw;
					}
				}

			// failmon?
		}

		if (publicAddressStr != "") {
			if (autoPublicAddress) {
				try {
					NetworkAddress parsedAddress = NetworkAddress::parse("0.0.0.0:" + publicAddressStr.substr(5));
					auto publicIP = determinePublicIPAutomatically( connectionFile->getConnectionString() );
					publicAddress = NetworkAddress( publicIP, parsedAddress.port, true, parsedAddress.isTLS() );
				} catch (Error& e) {
					fprintf(stderr, "ERROR: could not determine public address automatically from `%s': %s\n", publicAddressStr.c_str(), e.what());
					throw;
				}
			} else {
				try {
					publicAddress = NetworkAddress::parse(publicAddressStr);
				} catch (Error&) {
					fprintf(stderr, "ERROR: Could not parse network address `%s' (specify as IP_ADDRESS:PORT)\n", publicAddressStr.c_str());
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
			}

			bool clusterIsTLS = connectionFile->getConnectionString().coordinators()[0].isTLS();

			// Decide whether or not to use TLS based on if we see any of the coordinators have TLS enabled.
			//  Note that we are not supporting mixed clusters, but are defaulting to using TLS based on
			//  the contents of the cluster file. Note that we look at all the servers in the cluster file
			//  and if ANY of them are TLS, we turn TLS on.
			if( !StringRef(publicAddressStr).endsWith(LiteralStringRef(":tls")) ) {
				publicAddress = NetworkAddress( publicAddress.ip, publicAddress.port, true, clusterIsTLS );
			} else if( publicAddress.isTLS() != clusterIsTLS ) {
				fprintf(stderr, "ERROR: public address must not specify TLS if coordinators are non-TLS\n");
				printHelpTeaser(argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
		}

		if( role == FDBD && publicAddress.ip == 0 ) {
			if (publicAddressStr == "")
				fprintf(stderr, "ERROR: The -p or --public_address option is required\n");
			else
				fprintf(stderr, "ERROR: cannot use 0.0.0.0 as a public ip address\n");
			printHelpTeaser(argv[0]);
			flushAndExit(FDB_EXIT_ERROR);
		}

		if(role == ConsistencyCheck) {
			if(publicAddressStr != "") {
				fprintf(stderr, "ERROR: Public address cannot be specified for consistency check processes\n");
				printHelpTeaser(argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
			auto publicIP = determinePublicIPAutomatically(connectionFile->getConnectionString());
			publicAddress = NetworkAddress(publicIP, ::getpid());
		}

		if (listenAddressStr == "public")
			listenAddress = publicAddress;
		else {
			try {
				listenAddress = NetworkAddress::parse(listenAddressStr);
			} catch (Error&) {
				fprintf(stderr, "ERROR: Could not parse network address `%s' (specify as IP_ADDRESS:PORT)\n", listenAddressStr.c_str());
				printHelpTeaser(argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
		}

		if (role == FDBD && !publicAddress.isValid()) {
			fprintf(stderr, "ERROR: Public address not specified\n");
			printHelpTeaser(argv[0]);
			flushAndExit(FDB_EXIT_ERROR);
		}

		if (role==Simulation)
			printf("Random seed is %u...\n", randomSeed);

		if( zoneId.present() )
			printf("ZoneId set to %s, dcId to %s\n", printable(zoneId).c_str(), printable(dcId).c_str());

		g_random = new DeterministicRandom(randomSeed);
		if (role == Simulation)
			trace_random = new DeterministicRandom(1);
		else
			trace_random = new DeterministicRandom(platform::getRandomSeed());
		if (role == Simulation)
			g_nondeterministic_random = new DeterministicRandom(2);
		else
			g_nondeterministic_random = new DeterministicRandom(platform::getRandomSeed());
		if (role == Simulation)
			g_debug_random = new DeterministicRandom(3);
		else
			g_debug_random = new DeterministicRandom(platform::getRandomSeed());

		if(role==Simulation) {
			Optional<bool> buggifyOverride = checkBuggifyOverride(testFile);
			if(buggifyOverride.present())
				buggifyEnabled = buggifyOverride.get();
		}
		enableBuggify( buggifyEnabled );

		delete FLOW_KNOBS;
		delete SERVER_KNOBS;
		delete CLIENT_KNOBS;
		FlowKnobs* flowKnobs = new FlowKnobs(true, role == Simulation);
		ClientKnobs* clientKnobs = new ClientKnobs(true);
		ServerKnobs* serverKnobs = new ServerKnobs(true, clientKnobs);
		FLOW_KNOBS = flowKnobs;
		SERVER_KNOBS = serverKnobs;
		CLIENT_KNOBS = clientKnobs;

		if (!serverKnobs->setKnob( "log_directory", logFolder )) ASSERT(false);

		for(auto k=knobs.begin(); k!=knobs.end(); ++k) {
			try {
				if (!flowKnobs->setKnob( k->first, k->second ) &&
					!clientKnobs->setKnob( k->first, k->second ) &&
					!serverKnobs->setKnob( k->first, k->second ))
				{
					fprintf(stderr, "Unrecognized knob option '%s'\n", k->first.c_str());
					flushAndExit(FDB_EXIT_ERROR);
				}
			} catch (Error& e) {
				if (e.code() == error_code_invalid_option_value) {
					fprintf(stderr, "Invalid value '%s' for option '%s'\n", k->second.c_str(), k->first.c_str());
					flushAndExit(FDB_EXIT_ERROR);
				}
				throw;
			}
		}

		if (role == SkipListTest) {
			skipListTest();
			flushAndExit(FDB_EXIT_SUCCESS);
		}

		if (role == DSLTest) {
			dsltest();
			flushAndExit(FDB_EXIT_SUCCESS);
		}

		if (role == VersionedMapTest) {
			versionedMapTest();
			flushAndExit(FDB_EXIT_SUCCESS);
		}

		if (role == SearchMutations && !targetKey) {
			fprintf(stderr, "ERROR: please specify a target key\n");
			printHelpTeaser(argv[0]);
			flushAndExit(FDB_EXIT_ERROR);
		}

		if (role == NetworkTestClient && !testServersStr.size() ) {
			fprintf(stderr, "ERROR: please specify --testservers\n");
			printHelpTeaser(argv[0]);
			flushAndExit(FDB_EXIT_ERROR);
		}

		Future<Void> listenError;

		// Interpret legacy "maxLogs" option in the most sensible and unsurprising way we can while eliminating its code path
		if (maxLogsSet) {
			if (maxLogsSizeSet) {
				// This is the case where both options are set and we must deconflict.
				auto maxLogsAsSize = maxLogs * rollsize;

				// If either was unlimited, then the safe option here is to take the larger one.
				//  This means that is one of the two options specified a limited amount of logging
				//  then the option that specified "unlimited" will be ignored.
				if( maxLogsSize == 0 || maxLogs == 0 )
					maxLogsSize = std::max(maxLogsSize, maxLogsAsSize);
				else
					maxLogsSize = std::min(maxLogsSize, maxLogs * rollsize);
			}
			else {
				maxLogsSize = maxLogs * rollsize;
			}
		}

		// Initialize the thread pool
		CoroThreadPool::init();
		// Ordinarily, this is done when the network is run. However, network thread should be set before TraceEvents are logged. This thread will eventually run the network, so call it now.
		TraceEvent::setNetworkThread(); 

		if (role == Simulation || role == CreateTemplateDatabase) {
			//startOldSimulator();
			startNewSimulator();
			openTraceFile(NetworkAddress(), rollsize, maxLogsSize, logFolder, "trace", logGroup);
		} else {
			g_network = newNet2(NetworkAddress(), useThreadPool, true);
			FlowTransport::createInstance(1);

			openTraceFile(publicAddress, rollsize, maxLogsSize, logFolder, "trace", logGroup);

			if ( tlsCertPath.size() )
				tlsOptions->set_cert_file( tlsCertPath );
			if ( tlsKeyPath.size() )
				tlsOptions->set_key_file( tlsKeyPath );
			if ( tlsVerifyPeers.size() )
				tlsOptions->set_verify_peers( tlsVerifyPeers );

			if (tlsOptions->get_policy())
				tlsOptions->register_network();

			if (role == FDBD || role == NetworkTestServer) {
				try {
					listenError = FlowTransport::transport().bind(publicAddress, listenAddress);
					if (listenError.isReady()) listenError.get();
				} catch (Error& e) {
					TraceEvent("BindError").error(e);
					fprintf(stderr, "Error initializing networking with public address %s and listen address %s\n", publicAddress.toString().c_str(), listenAddress.toString().c_str());
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
			}

			// Use a negative ioTimeout to indicate warn-only
			Net2FileSystem::newFileSystem(fileIoWarnOnly ? -fileIoTimeout : fileIoTimeout, fileSystemPath);
			g_network->initMetrics();
			FlowTransport::transport().initMetrics();
			initTraceEventMetrics();
		}

		double start = timer(), startNow = now();

		std::string cwd = "<unknown>";
		try {
			cwd = platform::getWorkingDirectory();
		} catch(Error &e) {
			// Allow for platform error by rethrowing all _other_ errors
			if( e.code() != error_code_platform_error )
				throw;
		}

		TraceEvent("ProgramStart")
			.detail("RandomSeed", randomSeed)
			.detail("SourceVersion", getHGVersion())
			.detail("Version", FDB_VT_VERSION )
			.detail("PackageName", FDB_VT_PACKAGE_NAME)
			.detail("FileSystem", fileSystemPath)
			.detail("DataFolder", dataFolder)
			.detail("WorkingDirectory", cwd)
			.detail("ClusterFile", connectionFile ? connectionFile->getFilename().c_str() : "")
			.detail("ConnectionString", connectionFile ? connectionFile->getConnectionString().toString() : "")
			.detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(NULL))
			.detail("CommandLine", commandLine)
			.detail("BuggifyEnabled", buggifyEnabled)
			.detail("MemoryLimit", memLimit)
			.trackLatest("ProgramStart");

		// Test for TraceEvent length limits
		/*std::string foo(4096, 'x');
		TraceEvent("TooLongDetail").detail("Contents", foo);

		TraceEvent("TooLongEvent")
			.detail("Contents1", foo)
			.detail("Contents2", foo)
			.detail("Contents3", foo)
			.detail("Contents4", foo)
			.detail("Contents5", foo)
			.detail("Contents6", foo)
			.detail("Contents7", foo)
			.detail("Contents8", foo)
			.detail("ExtraTest", 1776);*/

		Error::init();
		std::set_new_handler( &platform::outOfMemory );
		setMemoryQuota( memLimit );

		Future<Optional<Void>> f;

		Standalone<StringRef> machineId(getSharedMemoryMachineId().toString());

		if (!localities.isPresent(LocalityData::keyZoneId))
			localities.set(LocalityData::keyZoneId, zoneId.present() ? zoneId : machineId);

		if (!localities.isPresent(LocalityData::keyMachineId))
			localities.set(LocalityData::keyMachineId, machineId);

		if (!localities.isPresent(LocalityData::keyDcId) && dcId.present())
			localities.set(LocalityData::keyDcId, dcId);

		if (role == Simulation) {
			TraceEvent("Simulation").detail("TestFile", testFile);

			clientKnobs->trace();
			flowKnobs->trace();
			serverKnobs->trace();

			if (!dataFolder.size())
				dataFolder = "simfdb";

			std::vector<std::string> directories = platform::listDirectories( dataFolder );
			for(int i = 0; i < directories.size(); i++)
				if( directories[i].size() != 32 && directories[i] != "." && directories[i] != ".." && directories[i] != "backups") {
					TraceEvent(SevError, "IncompatibleDirectoryFound").detail("DataFolder", dataFolder).detail("SuspiciousFile", directories[i]);
					fprintf(stderr, "ERROR: Data folder `%s' had non fdb file `%s'; please use clean, fdb-only folder\n", dataFolder.c_str(), directories[i].c_str());
					flushAndExit(FDB_EXIT_ERROR);
				}
			std::vector<std::string> files = platform::listFiles( dataFolder );
			if( (files.size()>1 || (files.size()==1 && files[0] != "restartInfo.ini" )) && !restarting ) {
				TraceEvent(SevError, "IncompatibleFileFound").detail("DataFolder", dataFolder);
				fprintf(stderr, "ERROR: Data folder `%s' is non-empty; please use clean, fdb-only folder\n", dataFolder.c_str());
				flushAndExit(FDB_EXIT_ERROR);
			}
			else if ( files.empty() && restarting ) {
				TraceEvent(SevWarnAlways, "FileNotFound").detail("DataFolder", dataFolder);
				printf("ERROR: Data folder `%s' is empty, but restarting option selected. Run Phase 1 test first\n", dataFolder.c_str());
				flushAndExit(FDB_EXIT_ERROR);
			}

			if (!restarting) {
				platform::eraseDirectoryRecursive( dataFolder );
				platform::createDirectory( dataFolder );
			}

			setupAndRun( dataFolder, testFile, restarting, tlsOptions->enabled() );
			g_simulator.run();
		} else if (role == FDBD) {
			ASSERT( connectionFile );

			setupSlowTaskProfiler();

			if (!dataFolder.size())
				dataFolder = format("fdb/%d/", publicAddress.port);  // SOMEDAY: Better default

			vector<Future<Void>> actors;
			actors.push_back( listenError );

			actors.push_back( fdbd(connectionFile, localities, processClass, dataFolder, dataFolder, storageMemLimit, metricsConnFile, metricsPrefix) );
			//actors.push_back( recurring( []{}, .001 ) );  // for ASIO latency measurement

			f = stopAfter( waitForAll(actors) );
			g_network->run();
		} else if (role == MultiTester) {
			f = stopAfter( runTests( connectionFile, TEST_TYPE_FROM_FILE, testOnServers ? TEST_ON_SERVERS : TEST_ON_TESTERS, minTesterCount, testFile, StringRef(), localities ) );
			g_network->run();
		} else if (role == Test) {
			auto m = startSystemMonitor(dataFolder, zoneId, zoneId);
			f = stopAfter( runTests( connectionFile, TEST_TYPE_FROM_FILE, TEST_HERE, 1, testFile, StringRef(), localities ) );
			g_network->run();
		} else if (role == ConsistencyCheck) {
			setupSlowTaskProfiler();

			auto m = startSystemMonitor(dataFolder, zoneId, zoneId);
			f = stopAfter( runTests( connectionFile, TEST_TYPE_CONSISTENCY_CHECK, TEST_HERE, 1, testFile, StringRef(), localities ) );
			g_network->run();
		} else if (role == CreateTemplateDatabase) {
			createTemplateDatabase();
		} else if (role == NetworkTestClient) {
			f = stopAfter( networkTestClient( testServersStr ) );
			g_network->run();
		} else if (role == NetworkTestServer) {
			f = stopAfter( networkTestServer() );
			g_network->run();
		} else if (role == KVFileIntegrityCheck) {
			auto f = stopAfter( KVFileCheck(kvFile, true) );
			g_network->run();
		} else if (role == KVFileGenerateIOLogChecksums) {
			auto f = stopAfter( GenerateIOLogChecksumFile(kvFile) );
			g_network->run();
		}

		int rc = FDB_EXIT_SUCCESS;
		if(f.isValid() && f.isReady() && !f.isError() && !f.get().present()) {
			rc = FDB_EXIT_ERROR;
		}

		int unseed = noUnseed ? 0 : g_random->randomInt(0, 100001);
		TraceEvent("ElapsedTime").detail("SimTime", now()-startNow).detail("RealTime", timer()-start)
			.detail("RandomUnseed", unseed);

		if (role==Simulation){
			printf("Unseed: %d\n", unseed);
			printf("Elapsed: %f simsec, %f real seconds\n", now()-startNow, timer()-start);
			//cout << format("  %d endpoints left\n", transport().getEndpointCount());
		}

		//IFailureMonitor::failureMonitor().address_info.clear();

		// we should have shut down ALL actors associated with this machine; let's list all of the ones still live
		/*{
			auto living = Actor::all;
			printf("%d surviving actors:\n", living.size());
			for(auto a = living.begin(); a != living.end(); ++a)
				printf("  #%lld %s %p\n", (*a)->creationIndex, (*a)->getName(), (*a));
		}

		{
			auto living = DatabaseContext::all;
			printf("%d surviving DatabaseContexts:\n", living.size());
			for(auto a = living.begin(); a != living.end(); ++a)
				printf("  #%lld %p\n", (*a)->creationIndex, (*a));
		}

		{
			auto living = TransactionData::all;
			printf("%d surviving TransactionData(s):\n", living.size());
			for(auto a = living.begin(); a != living.end(); ++a)
				printf("  #%lld %p\n", (*a)->creationIndex, (*a));
		}*/

		/*cout << Actor::allActors.size() << " surviving actors:" << endl;
		std::map<std::string,int> actorCount;
		for(int i=0; i<Actor::allActors.size(); i++)
			++actorCount[Actor::allActors[i]->getName()];
		for(auto i = actorCount.rbegin(); !(i == actorCount.rend()); ++i)
			cout << "  " << i->second << " " << i->first << endl;*/
		//	cout << "  " << Actor::allActors[i]->getName() << endl;

		int total = 0;
		for(auto i = Error::errorCounts().begin(); i != Error::errorCounts().end(); ++i)
			total += i->second;
		if (total)
			printf("%d errors:\n", total);
		for(auto i = Error::errorCounts().begin(); i != Error::errorCounts().end(); ++i)
			if (i->second > 0)
				printf("  %d: %d %s\n", i->second, i->first, Error::fromCode(i->first).what());

		if (&g_simulator == g_network) {
			auto processes = g_simulator.getAllProcesses();
			for(auto i = processes.begin(); i != processes.end(); ++i)
				printf("%s %s: %0.3f Mclocks\n", (*i)->name, (*i)->address.toString().c_str(), (*i)->cpuTicks / 1e6);
		}
		if (role == Simulation) {
			unsigned long sevErrorEventsLogged = TraceEvent::CountEventsLoggedAt(SevError);
			if (sevErrorEventsLogged > 0) {
				printf("%lu SevError events logged\n", sevErrorEventsLogged);
				rc = FDB_EXIT_ERROR;
			}
		}

		//g_simulator.run();

		#ifdef ALLOC_INSTRUMENTATION
		{
			std::cout << "Page Counts: "
				<< FastAllocator<16>::pageCount << " "
				<< FastAllocator<32>::pageCount << " "
				<< FastAllocator<64>::pageCount << " "
				<< FastAllocator<128>::pageCount << " "
				<< FastAllocator<256>::pageCount << " "
				<< FastAllocator<512>::pageCount << " "
				<< FastAllocator<1024>::pageCount << " "
				<< FastAllocator<2048>::pageCount << " "
				<< FastAllocator<4096>::pageCount << std::endl;

			vector< std::pair<std::string, const char*> > typeNames;
			for( auto i = allocInstr.begin(); i != allocInstr.end(); ++i ) {
				std::string s;

#ifdef __linux__
				char *demangled = abi::__cxa_demangle(i->first, NULL, NULL, NULL);
				if (demangled) {
					s = demangled;
					if (StringRef(s).startsWith(LiteralStringRef("(anonymous namespace)::")))
						s = s.substr(LiteralStringRef("(anonymous namespace)::").size());
					free(demangled);
				} else
					s = i->first;
#else
				s = i->first;
				if (StringRef(s).startsWith(LiteralStringRef("class `anonymous namespace'::")))
					s = s.substr(LiteralStringRef("class `anonymous namespace'::").size());
				else if (StringRef(s).startsWith(LiteralStringRef("class ")))
					s = s.substr(LiteralStringRef("class ").size());
				else if (StringRef(s).startsWith(LiteralStringRef("struct ")))
					s = s.substr(LiteralStringRef("struct ").size());
#endif

				typeNames.push_back( std::make_pair(s, i->first) );
			}
			std::sort(typeNames.begin(), typeNames.end());
			for(int i=0; i<typeNames.size(); i++) {
				const char* n = typeNames[i].second;
				auto& f = allocInstr[n];
				printf("%+d\t%+d\t%d\t%d\t%s\n", f.allocCount, -f.deallocCount, f.allocCount-f.deallocCount, f.maxAllocated, typeNames[i].first.c_str());
			}

			// We're about to exit and clean up data structures, this will wreak havoc on allocation recording
			memSample_entered = true;
		}
		#endif
		//printf("\n%d tests passed; %d tests failed\n", passCount, failCount);
		flushAndExit(rc);
	} catch (Error& e) {
		fprintf(stderr, "Error: %s\n", e.what());
		TraceEvent(SevError, "MainError").error(e);
		//printf("\n%d tests passed; %d tests failed\n", passCount, failCount);
		flushAndExit(FDB_EXIT_MAIN_ERROR);
	} catch (std::exception& e) {
		fprintf(stderr, "std::exception: %s\n", e.what());
		TraceEvent(SevError, "MainError").error(unknown_error()).detail("std::exception", e.what());
		//printf("\n%d tests passed; %d tests failed\n", passCount, failCount);
		flushAndExit(FDB_EXIT_MAIN_EXCEPTION);
	}

	static_assert( LBLocalityData<StorageServerInterface>::Present, "Storage server interface should be load balanced" );
	static_assert( LBLocalityData<MasterProxyInterface>::Present, "Master proxy interface should be load balanced" );
	static_assert( LBLocalityData<TLogInterface>::Present, "TLog interface should be load balanced" );
	static_assert( !LBLocalityData<MasterInterface>::Present, "Master interface should not be load balanced" );
}
