/*
 * fdbserver.actor.cpp
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

// There's something in one of the files below that defines a macros
// a macro that makes boost interprocess break on Windows.
#define BOOST_DATE_TIME_NO_LIB

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iterator>
#include <sstream>
#include <vector>

#include <stdarg.h>
#include <stdio.h>
#include <time.h>

#include <boost/algorithm/string.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>

#include <fmt/printf.h>

#include "fdbclient/ActorLineageProfiler.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/versions.h"
#include "fdbclient/BuildFlags.h"
#include "fdbrpc/WellKnownEndpoints.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbclient/SimpleIni.h"
#include "fdbrpc/AsyncFileCached.actor.h"
#include "fdbrpc/IPAllowList.h"
#include "fdbrpc/FlowProcess.actor.h"
#include "fdbrpc/Net2FileSystem.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/FlowGrpc.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/ConflictSet.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/CoroFlow.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/NetworkTest.h"
#include "fdbserver/RemoteIKeyValueStore.actor.h"
#include "fdbserver/RestoreWorkerInterface.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/SimulatedCluster.h"
#include "fdbserver/Status.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/pubsub.h"
#include "fdbserver/OnDemandStore.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/ArgParseUtil.h"
#include "flow/DeterministicRandom.h"
#include "flow/Platform.h"
#include "flow/ProtocolVersion.h"
#include "SimpleOpt/SimpleOpt.h"
#include "flow/SystemMonitor.h"
#include "flow/TLSConfig.actor.h"
#include "fdbclient/Tracing.h"
#include "flow/WriteOnlySet.h"
#include "flow/UnitTest.h"
#include "flow/FaultInjection.h"
#include "flow/flow.h"
#include "flow/network.h"

#include "flow/swift.h"
#include "flow/swift_concurrency_hooks.h"

#if defined(__linux__) || defined(__FreeBSD__)
#include <execinfo.h>
#include <signal.h>
#if defined(__linux__)
#include <sys/prctl.h>
#elif defined(__FreeBSD__)
#include <sys/procctl.h>
#endif
#ifdef ALLOC_INSTRUMENTATION
#include <cxxabi.h>
#endif
#endif

#ifdef WIN32
#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#endif

#if __has_include("SwiftModules/FDBServer")
class MasterData;
#include "SwiftModules/FDBServer"
#define SWIFT_REVERSE_INTEROP_SUPPORTED
#endif

#if __has_include("SwiftModules/Flow")
#include "SwiftModules/Flow"
#endif

#include "flow/actorcompiler.h" // This must be the last #include.

// FIXME(swift): remove those
extern "C" void swiftCallMeFuture(void* _Nonnull opaqueResultPromisePtr) noexcept;

using namespace std::literals;

// clang-format off
enum {
	OPT_CONNFILE, OPT_SEEDCONNFILE, OPT_SEEDCONNSTRING, OPT_ROLE, OPT_LISTEN, OPT_PUBLICADDR, OPT_DATAFOLDER, OPT_LOGFOLDER, OPT_PARENTPID, OPT_TRACER, OPT_NEWCONSOLE,
	OPT_NOBOX, OPT_TESTFILE, OPT_RESTARTING, OPT_RESTORING, OPT_RANDOMSEED, OPT_KEY, OPT_MEMLIMIT, OPT_VMEMLIMIT, OPT_STORAGEMEMLIMIT, OPT_CACHEMEMLIMIT, OPT_MACHINEID,
	OPT_DCID, OPT_MACHINE_CLASS, OPT_BUGGIFY, OPT_VERSION, OPT_BUILD_FLAGS, OPT_CRASHONERROR, OPT_HELP, OPT_NETWORKIMPL, OPT_NOBUFSTDOUT, OPT_BUFSTDOUTERR,
	OPT_TRACECLOCK, OPT_NUMTESTERS, OPT_DEVHELP, OPT_PRINT_CODE_PROBES, OPT_ROLLSIZE, OPT_MAXLOGS, OPT_MAXLOGSSIZE, OPT_KNOB, OPT_UNITTESTPARAM, OPT_TESTSERVERS, OPT_TEST_ON_SERVERS, OPT_METRICSCONNFILE,
	OPT_METRICSPREFIX, OPT_LOGGROUP, OPT_LOCALITY, OPT_IO_TRUST_SECONDS, OPT_IO_TRUST_WARN_ONLY, OPT_FILESYSTEM, OPT_PROFILER_RSS_SIZE, OPT_KVFILE,
	OPT_TRACE_FORMAT, OPT_WHITELIST_BINPATH, OPT_BLOB_CREDENTIALS, OPT_PROXY, OPT_CONFIG_PATH, OPT_USE_TEST_CONFIG_DB, OPT_NO_CONFIG_DB, OPT_FAULT_INJECTION, OPT_PROFILER, OPT_PRINT_SIMTIME,
	OPT_FLOW_PROCESS_NAME, OPT_FLOW_PROCESS_ENDPOINT, OPT_IP_TRUSTED_MASK, OPT_KMS_CONN_DISCOVERY_URL_FILE, OPT_KMS_CONNECTOR_TYPE, OPT_KMS_REST_ALLOW_NOT_SECURE_CONECTION, OPT_KMS_CONN_VALIDATION_TOKEN_DETAILS,
	OPT_KMS_CONN_GET_ENCRYPTION_KEYS_ENDPOINT, OPT_KMS_CONN_GET_LATEST_ENCRYPTION_KEYS_ENDPOINT, OPT_KMS_CONN_GET_BLOB_METADATA_ENDPOINT, OPT_NEW_CLUSTER_KEY, OPT_AUTHZ_PUBLIC_KEY_FILE, OPT_USE_FUTURE_PROTOCOL_VERSION, OPT_CONSISTENCY_CHECK_URGENT_MODE
};

CSimpleOpt::SOption g_rgOptions[] = {
	{ OPT_CONNFILE,              "-C",                          SO_REQ_SEP },
	{ OPT_CONNFILE,              "--cluster-file",              SO_REQ_SEP },
	{ OPT_SEEDCONNFILE,          "--seed-cluster-file",         SO_REQ_SEP },
	{ OPT_SEEDCONNSTRING,        "--seed-connection-string",    SO_REQ_SEP },
	{ OPT_ROLE,                  "-r",                          SO_REQ_SEP },
	{ OPT_ROLE,                  "--role",                      SO_REQ_SEP },
	{ OPT_PUBLICADDR,            "-p",                          SO_REQ_SEP },
	{ OPT_PUBLICADDR,            "--public-address",            SO_REQ_SEP },
	{ OPT_LISTEN,                "-l",                          SO_REQ_SEP },
	{ OPT_LISTEN,                "--listen-address",            SO_REQ_SEP },
#ifdef __linux__
	{ OPT_FILESYSTEM,           "--data-filesystem",            SO_REQ_SEP },
	{ OPT_PROFILER_RSS_SIZE,    "--rsssize",                    SO_REQ_SEP },
#endif
	{ OPT_DATAFOLDER,            "-d",                          SO_REQ_SEP },
	{ OPT_DATAFOLDER,            "--datadir",                   SO_REQ_SEP },
	{ OPT_LOGFOLDER,             "-L",                          SO_REQ_SEP },
	{ OPT_LOGFOLDER,             "--logdir",                    SO_REQ_SEP },
	{ OPT_ROLLSIZE,              "-Rs",                         SO_REQ_SEP },
	{ OPT_ROLLSIZE,              "--logsize",                   SO_REQ_SEP },
	{ OPT_MAXLOGS,               "--maxlogs",                   SO_REQ_SEP },
	{ OPT_MAXLOGSSIZE,           "--maxlogssize",               SO_REQ_SEP },
	{ OPT_LOGGROUP,              "--loggroup",                  SO_REQ_SEP },
	{ OPT_PARENTPID,             "--parentpid",                 SO_REQ_SEP },
	{ OPT_TRACER,                "--tracer",                    SO_REQ_SEP },
#ifdef _WIN32
	{ OPT_NEWCONSOLE,            "-n",                          SO_NONE },
	{ OPT_NEWCONSOLE,            "--newconsole",                SO_NONE },
	{ OPT_NOBOX,                 "-q",                          SO_NONE },
	{ OPT_NOBOX,                 "--no-dialog",                 SO_NONE },
#endif
	{ OPT_KVFILE,                "--kvfile",                    SO_REQ_SEP },
	{ OPT_TESTFILE,              "-f",                          SO_REQ_SEP },
	{ OPT_TESTFILE,              "--testfile",                  SO_REQ_SEP },
	{ OPT_RESTARTING,            "-R",                          SO_NONE },
	{ OPT_RESTARTING,            "--restarting",                SO_NONE },
	{ OPT_RANDOMSEED,            "-s",                          SO_REQ_SEP },
	{ OPT_RANDOMSEED,            "--seed",                      SO_REQ_SEP },
	{ OPT_KEY,                   "-k",                          SO_REQ_SEP },
	{ OPT_KEY,                   "--key",                       SO_REQ_SEP },
	{ OPT_MEMLIMIT,              "-m",                          SO_REQ_SEP },
	{ OPT_MEMLIMIT,              "--memory",                    SO_REQ_SEP },
	{ OPT_VMEMLIMIT,             "--memory-vsize",              SO_REQ_SEP },
	{ OPT_STORAGEMEMLIMIT,       "-M",                          SO_REQ_SEP },
	{ OPT_STORAGEMEMLIMIT,       "--storage-memory",            SO_REQ_SEP },
	{ OPT_CACHEMEMLIMIT,         "--cache-memory",              SO_REQ_SEP },
	{ OPT_MACHINEID,             "-i",                          SO_REQ_SEP },
	{ OPT_MACHINEID,             "--machine-id",                SO_REQ_SEP },
	{ OPT_DCID,                  "-a",                          SO_REQ_SEP },
	{ OPT_DCID,                  "--datacenter-id",             SO_REQ_SEP },
	{ OPT_MACHINE_CLASS,         "-c",                          SO_REQ_SEP },
	{ OPT_MACHINE_CLASS,         "--class",                     SO_REQ_SEP },
	{ OPT_BUGGIFY,               "-b",                          SO_REQ_SEP },
	{ OPT_BUGGIFY,               "--buggify",                   SO_REQ_SEP },
	{ OPT_VERSION,               "-v",                          SO_NONE },
	{ OPT_VERSION,               "--version",                   SO_NONE },
	{ OPT_BUILD_FLAGS,           "--build-flags",               SO_NONE },
	{ OPT_CRASHONERROR,          "--crash",                     SO_NONE },
	{ OPT_NETWORKIMPL,           "-N",                          SO_REQ_SEP },
	{ OPT_NETWORKIMPL,           "--network",                   SO_REQ_SEP },
	{ OPT_NOBUFSTDOUT,           "--unbufferedout",             SO_NONE },
	{ OPT_BUFSTDOUTERR,          "--bufferedout",               SO_NONE },
	{ OPT_TRACECLOCK,            "--traceclock",                SO_REQ_SEP },
	{ OPT_NUMTESTERS,            "--num-testers",               SO_REQ_SEP },
	{ OPT_HELP,                  "-?",                          SO_NONE },
	{ OPT_HELP,                  "-h",                          SO_NONE },
	{ OPT_HELP,                  "--help",                      SO_NONE },
	{ OPT_DEVHELP,               "--dev-help",                  SO_NONE },
	{ OPT_PRINT_CODE_PROBES,     "--code-probes",               SO_REQ_SEP },
	{ OPT_KNOB,                  "--knob-",                     SO_REQ_SEP },
	{ OPT_UNITTESTPARAM,         "--test-",                     SO_REQ_SEP },
	{ OPT_LOCALITY,              "--locality-",                 SO_REQ_SEP },
	{ OPT_TESTSERVERS,           "--testservers",               SO_REQ_SEP },
	{ OPT_TEST_ON_SERVERS,       "--testonservers",             SO_NONE },
	{ OPT_METRICSCONNFILE,       "--metrics-cluster",           SO_REQ_SEP },
	{ OPT_METRICSPREFIX,         "--metrics-prefix",            SO_REQ_SEP },
	{ OPT_IO_TRUST_SECONDS,      "--io-trust-seconds",          SO_REQ_SEP },
	{ OPT_IO_TRUST_WARN_ONLY,    "--io-trust-warn-only",        SO_NONE },
	{ OPT_TRACE_FORMAT,          "--trace-format",              SO_REQ_SEP },
	{ OPT_WHITELIST_BINPATH,     "--whitelist-binpath",         SO_REQ_SEP },
	{ OPT_BLOB_CREDENTIALS,      "--blob-credentials",          SO_REQ_SEP },
	{ OPT_PROXY,                 "--proxy",                     SO_REQ_SEP },
	{ OPT_CONFIG_PATH,           "--config-path",               SO_REQ_SEP },
	{ OPT_USE_TEST_CONFIG_DB,    "--use-test-config-db",        SO_NONE },
	{ OPT_NO_CONFIG_DB,          "--no-config-db",              SO_NONE },
	{ OPT_FAULT_INJECTION,       "-fi",                         SO_REQ_SEP },
	{ OPT_FAULT_INJECTION,       "--fault-injection",           SO_REQ_SEP },
	{ OPT_PROFILER,	             "--profiler-",                 SO_REQ_SEP },
	{ OPT_PRINT_SIMTIME,         "--print-sim-time",             SO_NONE },
	{ OPT_FLOW_PROCESS_NAME,     "--process-name",              SO_REQ_SEP },
	{ OPT_FLOW_PROCESS_ENDPOINT, "--process-endpoint",          SO_REQ_SEP },
	{ OPT_IP_TRUSTED_MASK,       "--trusted-subnet-",           SO_REQ_SEP },
	{ OPT_NEW_CLUSTER_KEY,       "--new-cluster-key",           SO_REQ_SEP },
	{ OPT_AUTHZ_PUBLIC_KEY_FILE, "--authorization-public-key-file", SO_REQ_SEP },
	{ OPT_KMS_CONN_DISCOVERY_URL_FILE,           "--discover-kms-conn-url-file",            SO_REQ_SEP },
	{ OPT_KMS_CONNECTOR_TYPE,    "--kms-connector-type",                                    SO_REQ_SEP },
	{ OPT_KMS_REST_ALLOW_NOT_SECURE_CONECTION,  "--kms-rest-allow-not-secure-connection",      SO_NONE },
	{ OPT_KMS_CONN_VALIDATION_TOKEN_DETAILS,     "--kms-conn-validation-token-details",     SO_REQ_SEP },
	{ OPT_KMS_CONN_GET_ENCRYPTION_KEYS_ENDPOINT, "--kms-conn-get-encryption-keys-endpoint", SO_REQ_SEP },
	{ OPT_KMS_CONN_GET_LATEST_ENCRYPTION_KEYS_ENDPOINT, "--kms-conn-get-latest-encryption-keys-endpoint", SO_REQ_SEP },
	{ OPT_KMS_CONN_GET_BLOB_METADATA_ENDPOINT,   "--kms-conn-get-blob-metadata-endpoint",   SO_REQ_SEP },
	{ OPT_USE_FUTURE_PROTOCOL_VERSION, 			 "--use-future-protocol-version",			SO_REQ_SEP },
	{ OPT_CONSISTENCY_CHECK_URGENT_MODE, 		 "--consistency-check-urgent-mode",			SO_NONE },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

// clang-format on

extern void dsltest();
extern void pingtest();
extern void copyTest();
extern void versionedMapTest();
extern void createTemplateDatabase();

extern const char* getSourceVersion();

extern void flushTraceFileVoid();

extern const int MAX_CLUSTER_FILE_BYTES;

#ifdef ALLOC_INSTRUMENTATION
extern uint8_t* g_extra_memory;
#endif

bool enableFailures = true;

#define test_assert(x)                                                                                                 \
	if (!(x)) {                                                                                                        \
		std::cout << "Test failed: " #x << std::endl;                                                                  \
		return false;                                                                                                  \
	}

#ifdef _WIN32
#include <sddl.h>

// It is your
//    responsibility to properly initialize the
//    structure and to free the structure's
//    lpSecurityDescriptor member when you have
//    finished using it. To free the structure's
//    lpSecurityDescriptor member, call the
//    LocalFree function.
BOOL CreatePermissiveReadWriteDACL(SECURITY_ATTRIBUTES* pSA) {
	UNSTOPPABLE_ASSERT(pSA != nullptr);

	TCHAR* szSD = TEXT("D:") // Discretionary ACL
	    TEXT("(A;OICI;GR;;;AU)") // Allow read/write/execute to authenticated users
	    TEXT("(A;OICI;GA;;;BA)"); // Allow full control to administrators

	return ConvertStringSecurityDescriptorToSecurityDescriptor(
	    szSD, SDDL_REVISION_1, &(pSA->lpSecurityDescriptor), nullptr);
}
#endif

class WorldReadablePermissions {
public:
	WorldReadablePermissions() {
#ifdef _WIN32
		sa.nLength = sizeof(SECURITY_ATTRIBUTES);
		sa.bInheritHandle = FALSE;
		if (!CreatePermissiveReadWriteDACL(&sa)) {
			TraceEvent("Win32DACLCreationFail").GetLastError();
			throw platform_error();
		}
		permission.set_permissions(&sa);
#elif (defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__))
		// There is nothing to do here, since the default permissions are fine
#else
#error Port me!
#endif
	}

	virtual ~WorldReadablePermissions() {
#ifdef _WIN32
		LocalFree(sa.lpSecurityDescriptor);
#elif (defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__))
		// There is nothing to do here, since the default permissions are fine
#else
#error Port me!
#endif
	}

	boost::interprocess::permissions permission;

private:
	WorldReadablePermissions(const WorldReadablePermissions& rhs) {}
#ifdef _WIN32
	SECURITY_ATTRIBUTES sa;
#endif
};

UID getSharedMemoryMachineId() {
	// new UID to use if an existing one is not found
	UID newUID = deterministicRandom()->randomUniqueID();

#if DEBUG_DETERMINISM
	// Don't use shared memory if DEBUG_DETERMINISM is set
	return newUID;
#else
	UID* machineId = nullptr;
	int numTries = 0;

	// Permissions object defaults to 0644 on *nix, but on windows defaults to allowing access to only the creator.
	// On windows, this means that we have to create an elaborate workaround for DACLs
	WorldReadablePermissions p;
	std::string sharedMemoryIdentifier = "fdbserver_shared_memory_id";
	loop {
		try {
			// "0" is the default netPrefix "addr"
			boost::interprocess::managed_shared_memory segment(
			    boost::interprocess::open_or_create, sharedMemoryIdentifier.c_str(), 1000, 0, p.permission);
			machineId = segment.find_or_construct<UID>("machineId")(newUID);
			if (!machineId)
				criticalError(
				    FDB_EXIT_ERROR, "SharedMemoryError", "Could not locate or create shared memory - 'machineId'");
			return *machineId;
		} catch (boost::interprocess::interprocess_exception&) {
			try {
				// If the shared memory already exists, open it read-only in case it was created by another user
				boost::interprocess::managed_shared_memory segment(boost::interprocess::open_read_only,
				                                                   sharedMemoryIdentifier.c_str());
				machineId = segment.find<UID>("machineId").first;
				if (!machineId)
					criticalError(FDB_EXIT_ERROR, "SharedMemoryError", "Could not locate shared memory - 'machineId'");
				return *machineId;
			} catch (boost::interprocess::interprocess_exception& ex) {
				// Retry in case the shared memory was deleted in between the call to open_or_create and open_read_only
				// Don't keep trying forever in case this is caused by some other problem
				if (++numTries == 10)
					criticalError(FDB_EXIT_ERROR,
					              "SharedMemoryError",
					              format("Could not open shared memory - %s", ex.what()).c_str());
			}
		}
	}
#endif
}

ACTOR void failAfter(Future<Void> trigger, ISimulator::ProcessInfo* m = g_simulator->getCurrentProcess()) {
	wait(trigger);
	if (enableFailures) {
		printf("Killing machine: %s at %f\n", m->address.toString().c_str(), now());
		g_simulator->killProcess(m, ISimulator::KillType::KillInstantly);
	}
}

void failAfter(Future<Void> trigger, Endpoint e) {
	if (g_network == g_simulator)
		failAfter(trigger, g_simulator->getProcess(e));
}

#ifdef WITH_SWIFT
ACTOR void swiftTestRunner() {
	auto p = PromiseVoid();
	fdbserver_swift::swiftyTestRunner(p);
	wait(p.getFuture());

	flushAndExit(0);
}
#endif

ACTOR Future<Void> histogramReport() {
	loop {
		wait(delay(SERVER_KNOBS->HISTOGRAM_REPORT_INTERVAL));

		GetHistogramRegistry().logReport();
	}
}

void testSerializationSpeed() {
	double tstart;
	double build = 0, serialize = 0, deserialize = 0, copy = 0, deallocate = 0;
	double bytes = 0;
	double testBegin = timer();
	for (int a = 0; a < 10000; a++) {
		{
			tstart = timer();

			Arena batchArena;
			VectorRef<CommitTransactionRef> batch;
			batch.resize(batchArena, 1000);
			for (int t = 0; t < batch.size(); t++) {
				CommitTransactionRef& tr = batch[t];
				tr.read_snapshot = 0;
				for (int i = 0; i < 2; i++)
					tr.mutations.push_back_deep(batchArena,
					                            MutationRef(MutationRef::SetValue, "KeyABCDE"_sr, "SomeValu"_sr));
				tr.mutations.push_back_deep(batchArena,
				                            MutationRef(MutationRef::ClearRange, "BeginKey"_sr, "EndKeyAB"_sr));
			}

			build += timer() - tstart;

			tstart = timer();

			BinaryWriter wr(IncludeVersion());
			wr << batch;

			bytes += wr.getLength();

			serialize += timer() - tstart;

			for (int i = 0; i < 1; i++) {
				tstart = timer();
				Arena arena;
				StringRef data(arena, StringRef((const uint8_t*)wr.getData(), wr.getLength()));
				copy += timer() - tstart;

				tstart = timer();
				ArenaReader rd(arena, data, IncludeVersion());
				VectorRef<CommitTransactionRef> batch2;
				rd >> arena >> batch2;

				deserialize += timer() - tstart;
			}

			tstart = timer();
		}
		deallocate += timer() - tstart;
	}
	double elapsed = (timer() - testBegin);
	printf("Test speed: %0.1f MB/sec (%0.0f/sec)\n", bytes / 1e6 / elapsed, 1000000 / elapsed);
	printf("  Build: %0.1f MB/sec\n", bytes / 1e6 / build);
	printf("  Serialize: %0.1f MB/sec\n", bytes / 1e6 / serialize);
	printf("  Copy: %0.1f MB/sec\n", bytes / 1e6 / copy);
	printf("  Deserialize: %0.1f MB/sec\n", bytes / 1e6 / deserialize);
	printf("  Deallocate: %0.1f MB/sec\n", bytes / 1e6 / deallocate);
	printf("  Bytes: %0.1f MB\n", bytes / 1e6);
	printf("\n");
}

std::string toHTML(const StringRef& binaryString) {
	std::string s;

	for (int i = 0; i < binaryString.size(); i++) {
		uint8_t c = binaryString[i];
		if (c == '<')
			s += "&lt;";
		else if (c == '>')
			s += "&gt;";
		else if (c == '&')
			s += "&amp;";
		else if (c == '"')
			s += "&quot;";
		else if (c == ' ')
			s += "&nbsp;";
		else if (c > 32 && c < 127)
			s += c;
		else
			s += format("<span class=\"binary\">[%02x]</span>", c);
	}

	return s;
}

ACTOR Future<Void> dumpDatabase(Database cx, std::string outputFilename, KeyRange range = allKeys) {
	try {
		state Transaction tr(cx);
		loop {
			state FILE* output = fopen(outputFilename.c_str(), "wt");
			try {
				state KeySelectorRef iter = firstGreaterOrEqual(range.begin);
				state Arena arena;
				fprintf(output, "<html><head><style type=\"text/css\">.binary {color:red}</style></head><body>\n");
				Version ver = wait(tr.getReadVersion());
				fprintf(output, "<h3>Database version: %" PRId64 "</h3>", ver);

				loop {
					RangeResult results = wait(tr.getRange(iter, firstGreaterOrEqual(range.end), 1000));
					for (int r = 0; r < results.size(); r++) {
						std::string key = toHTML(results[r].key), value = toHTML(results[r].value);
						fprintf(output, "<p>%s <b>:=</b> %s</p>\n", key.c_str(), value.c_str());
					}
					if (results.size() < 1000)
						break;
					iter = firstGreaterThan(KeyRef(arena, results[results.size() - 1].key));
				}
				fprintf(output, "</body></html>");
				fclose(output);
				TraceEvent("DatabaseDumped").detail("Filename", outputFilename);
				return Void();
			} catch (Error& e) {
				fclose(output);
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevError, "DumpDatabaseError").error(e).detail("Filename", outputFilename);
		throw;
	}
}

void memoryTest();
void skipListTest();

Future<Void> startSystemMonitor(std::string dataFolder,
                                Optional<Standalone<StringRef>> dcId,
                                Optional<Standalone<StringRef>> zoneId,
                                Optional<Standalone<StringRef>> machineId,
                                Optional<Standalone<StringRef>> datahallId) {
	initializeSystemMonitorMachineState(SystemMonitorMachineState(
	    dataFolder, dcId, zoneId, machineId, datahallId, g_network->getLocalAddress().ip, FDB_VT_VERSION));

	systemMonitor();
	return recurring(&systemMonitor, SERVER_KNOBS->SYSTEM_MONITOR_FREQUENCY, TaskPriority::FlushTrace);
}

void testIndexedSet();

#ifdef _WIN32
void parentWatcher(void* parentHandle) {
	HANDLE parent = (HANDLE)parentHandle;
	int signal = WaitForSingleObject(parent, INFINITE);
	CloseHandle(parentHandle);
	if (signal == WAIT_OBJECT_0)
		criticalError(FDB_EXIT_SUCCESS, "ParentProcessExited", "Parent process exited");
	TraceEvent(SevError, "ParentProcessWaitFailed").detail("RetCode", signal).GetLastError();
}
#else
void* parentWatcher(void* arg) {
	int* parent_pid = (int*)arg;
	while (1) {
		sleep(1);
		if (getppid() != *parent_pid)
			criticalError(FDB_EXIT_SUCCESS, "ParentProcessExited", "Parent process exited");
	}
}
#endif

static void printBuildInformation() {
	printf("%s", jsonBuildInformation().c_str());
}

static void printVersion() {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("source version %s\n", getSourceVersion());
	printf("protocol %" PRIx64 "\n", currentProtocolVersion().version());
}

static void printHelpTeaser(const char* name) {
	fprintf(stderr, "Try `%s --help' for more information.\n", name);
}

static void printOptionUsage(std::string option, std::string description) {
	static const std::string OPTION_INDENT("  ");
	static const std::string DESCRIPTION_INDENT("                ");
	static const int WIDTH = 80;

	boost::algorithm::trim(option);
	boost::algorithm::trim(description);

	std::string result = OPTION_INDENT + option + "\n";

	std::stringstream sstream(description);
	if (sstream.eof()) {
		printf("%s", result.c_str());
		return;
	}

	std::string currWord;
	sstream >> currWord;

	std::string currLine(DESCRIPTION_INDENT + ' ' + currWord);
	int currLength = currLine.size();

	while (!sstream.eof()) {
		sstream >> currWord;

		if (currLength + static_cast<int>(currWord.size()) + 1 > WIDTH) {
			result += currLine + '\n';
			currLine = DESCRIPTION_INDENT + ' ' + currWord;
		} else {
			currLine += ' ' + currWord;
		}
		currLength = currLine.size();
	}
	result += currLine + '\n';

	printf("%s", result.c_str());
}

static void printUsage(const char* name, bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s -p ADDRESS [OPTIONS]\n\n", name);
	printOptionUsage("-p ADDRESS, --public-address ADDRESS",
	                 " Public address, specified as `IP_ADDRESS:PORT' or `auto:PORT'.");
	printOptionUsage("-l ADDRESS, --listen-address ADDRESS",
	                 " Listen address, specified as `IP_ADDRESS:PORT' (defaults to"
	                 " public address).");
	printOptionUsage("-C CONNFILE, --cluster-file CONNFILE",
	                 " The path of a file containing the connection string for the"
	                 " FoundationDB cluster. The default is first the value of the"
	                 " FDB_CLUSTER_FILE environment variable, then `./fdb.cluster',"
	                 " then `" +
	                     platform::getDefaultClusterFilePath() + "'.");
	printOptionUsage("--seed-cluster-file SEEDCONNFILE",
	                 " The path of a seed cluster file which will be used to connect"
	                 " if the -C cluster file does not exist. If the server connects"
	                 " successfully using the seed file, then it copies the file to"
	                 " the -C file location.");
	printOptionUsage("--seed-connection-string SEEDCONNSTRING",
	                 " The path of a seed connection string which will be used to connect"
	                 " if the -C cluster file does not exist. If the server connects"
	                 " successfully using the seed string, then it copies the string to"
	                 " the -C file location.");
#ifdef __linux__
	printOptionUsage("--data-filesystem PATH",
	                 " Turns on validation that all data files are written to a drive"
	                 " mounted at the specified PATH. This checks that the device at PATH"
	                 " is currently mounted and that any data files get written to the"
	                 " same device.");
#endif
	printOptionUsage("-d PATH, --datadir PATH",
	                 " Store data files in the given folder (must be unique for each"
	                 " fdbserver instance on a given machine).");
	printOptionUsage("-L PATH, --logdir PATH", " Store log files in the given folder (default is `.').");
	printOptionUsage("--logsize SIZE",
	                 "Roll over to a new log file after the current log file"
	                 " exceeds SIZE bytes. The default value is 10MiB.");
	printOptionUsage("--maxlogs SIZE, --maxlogssize SIZE",
	                 " Delete the oldest log file when the total size of all log"
	                 " files exceeds SIZE bytes. If set to 0, old log files will not"
	                 " be deleted. The default value is 100MiB.");
	printOptionUsage("--loggroup LOG_GROUP",
	                 " Sets the LogGroup field with the specified value for all"
	                 " events in the trace output (defaults to `default').");
	printOptionUsage("--trace-format FORMAT",
	                 " Select the format of the log files. xml (the default) and json"
	                 " are supported.");
	printOptionUsage("--tracer       TRACER",
	                 " Select a tracer for transaction tracing. Currently disabled"
	                 " (the default) and log_file are supported.");
	printOptionUsage("-i ID, --machine-id ID",
	                 " Machine and zone identifier key (up to 16 hex characters)."
	                 " Defaults to a random value shared by all fdbserver processes"
	                 " on this machine.");
	printOptionUsage("-a ID, --datacenter-id ID", " Data center identifier key (up to 16 hex characters).");
	printOptionUsage("--locality-LOCALITYKEY LOCALITYVALUE",
	                 " Define a locality key. LOCALITYKEY is case-insensitive though"
	                 " LOCALITYVALUE is not.");
	printOptionUsage("-m SIZE, --memory SIZE",
	                 " Resident memory limit. The default value is 8GiB. When specified"
	                 " without a unit, MiB is assumed.");
	printOptionUsage("--memory-vsize SIZE",
	                 " Virtual memory limit. The default value is unlimited. When specified"
	                 " without a unit, MiB is assumed.");
	printOptionUsage("-M SIZE, --storage-memory SIZE",
	                 " Maximum amount of memory used for storage. The default"
	                 " value is 1GiB. When specified without a unit, MB is"
	                 " assumed.");
	printOptionUsage("--cache-memory SIZE",
	                 " The amount of memory to use for caching disk pages."
	                 " The default value is 2GiB. When specified without a unit,"
	                 " MiB is assumed.");
	printOptionUsage("-c CLASS, --class CLASS",
	                 " Machine class (valid options are storage, transaction,"
	                 " resolution, grv_proxy, commit_proxy, master, test, unset, stateless, log, router,"
	                 " and cluster_controller).");
	printOptionUsage("--blob-credentials FILE",
	                 "File containing blob credentials in JSON format. Can be specified "
	                 "multiple times for multiple files. See fdbbackup usage for more details.");
	printOptionUsage("--proxy PROXY:PORT", "IP:port or host:port to proxy server for connecting to external network.");
	printOptionUsage("--profiler-",
	                 "Set an actor profiler option. Supported options are:\n"
	                 "  collector -- None or FluentD (FluentD requires collector_endpoint to be set)\n"
	                 "  collector_endpoint -- IP:PORT of the fluentd server\n"
	                 "  collector_protocol -- UDP or TCP (default is UDP)");
	printf("%s", TLS_HELP);
	printOptionUsage("-v, --version", "Print version information and exit.");
	printOptionUsage("-h, -?, --help", "Display this help and exit.");
	if (devhelp) {
		printf("  --build-flags  Print build information and exit.\n");
		printOptionUsage("-r ROLE, --role ROLE",
		                 " Server role (valid options are fdbd, test, multitest,"
		                 " simulation, networktestclient, networktestserver, restore"
		                 " consistencycheck, consistencycheckurgent, kvfileintegritycheck, kvfilegeneratesums, "
		                 "kvfiledump, unittests)."
		                 " The default is `fdbd'.");
#ifdef _WIN32
		printOptionUsage("-n, --newconsole", " Create a new console.");
		printOptionUsage("-q, --no-dialog", " Disable error dialog on crash.");
		printOptionUsage("--parentpid PID", " Specify a process after whose termination to exit.");
#endif
		printOptionUsage("-f TESTFILE, --testfile",
		                 " Testfile to run, defaults to `tests/default.txt'.  If role is `unittests', specifies which "
		                 "unit tests to run as a search prefix.");
		printOptionUsage("-R, --restarting", " Restart a previous simulation that was cleanly shut down.");
		printOptionUsage("-s SEED, --seed SEED", " Random seed.");
		printOptionUsage("-k KEY, --key KEY", "Target key for search role.");
		printOptionUsage("--kvfile FILE",
		                 "Input file (SQLite database file) for use by the 'kvfilegeneratesums', "
		                 "'kvfileintegritycheck' and 'kvfiledump' roles.");
		printOptionUsage("-b [on,off], --buggify [on,off]", " Sets Buggify system state, defaults to `off'.");
		printOptionUsage("-fi [on,off], --fault-injection [on,off]", " Sets fault injection, defaults to `on'.");
		printOptionUsage("--crash", "Crash on serious errors instead of continuing.");
		printOptionUsage("-N NETWORKIMPL, --network NETWORKIMPL",
		                 " Select network implementation, `net2' (default),"
		                 " `net2-threadpool'.");
		printOptionUsage("--unbufferedout", " Do not buffer stdout and stderr.");
		printOptionUsage("--bufferedout", " Buffer stdout and stderr.");
		printOptionUsage("--traceclock CLOCKIMPL",
		                 " Select clock source for trace files, `now' (default) or"
		                 " `realtime'.");
		printOptionUsage("--num-testers NUM",
		                 " A multitester will wait for NUM testers before starting"
		                 " (defaults to 1).");
		printOptionUsage("--test-PARAMNAME PARAMVALUE",
		                 " Set a UnitTest named parameter to the given value.  Names are case sensitive.");
#ifdef __linux__
		printOptionUsage("--rsssize SIZE",
		                 " Turns on automatic heap profiling when RSS memory size exceeds"
		                 " the given threshold. fdbserver needs to be compiled with"
		                 " USE_GPERFTOOLS flag in order to use this feature.");
#endif
		printOptionUsage("--testservers ADDRESSES",
		                 " The addresses of networktestservers"
		                 " specified as ADDRESS:PORT,ADDRESS:PORT...");
		printOptionUsage("--testonservers", " Testers are recruited on servers.");
		printOptionUsage("--metrics-cluster CONNFILE",
		                 " The cluster file designating where this process will"
		                 " store its metric data. By default metrics will be stored"
		                 " in the same database the process is participating in.");
		printOptionUsage("--metrics-prefix PREFIX",
		                 " The prefix where this process will store its metric data."
		                 " Must be specified if using a different database for metrics.");
		printOptionUsage("--knob-KNOBNAME KNOBVALUE", " Changes a database knob. KNOBNAME should be lowercase.");
		printOptionUsage("--io-trust-seconds SECONDS",
		                 " Sets the time in seconds that a read or write operation is allowed to take"
		                 " before timing out with an error. If an operation times out, all future"
		                 " operations on that file will fail with an error as well. Only has an effect"
		                 " when using AsyncFileKAIO in Linux.");
		printOptionUsage("--io-trust-warn-only",
		                 " Instead of failing when an I/O operation exceeds io_trust_seconds, just"
		                 " log a warning to the trace log. Has no effect if io_trust_seconds is unspecified.");
		printOptionUsage("--use-future-protocol-version [true,false]",
		                 " Run the process with a simulated future protocol version."
		                 " This option can be used testing purposes only!");
		printf("\n"
		       "The 'kvfiledump' role dump all key-values from kvfile to stdout in binary format:\n"
		       "{key length}{key binary}{value length}{value binary}, length is 4 bytes int\n"
		       "(little endianness). This role takes 3 environment variables as parameters:\n"
		       " - FDB_DUMP_STARTKEY: start key for the dump, default is empty\n"
		       " - FDB_DUMP_ENDKEY: end key for the dump, default is \"\\xff\\xff\"\n"
		       " - FDB_DUMP_DEBUG: print key-values to stderr in escaped format\n");

		printf(
		    "\n"
		    "The 'changedescription' role replaces the old cluster key in all coordinators' data file to the specified "
		    "new cluster key,\n"
		    "which is passed in by '--new-cluster-key'. In particular, cluster key means '[description]:[id]'.\n"
		    "'--datadir' is supposed to point to the top level directory of FDB's data, where subdirectories are for "
		    "each process's data.\n"
		    "The given cluster file passed in by '-C, --cluster-file' is considered to contain the old cluster key.\n"
		    "It is used before restoring a snapshotted cluster to let the cluster have a different cluster key.\n"
		    "Please make sure run it on every host in the cluster with the same '--new-cluster-key'.\n");

	} else {
		printOptionUsage("--dev-help", "Display developer-specific help and exit.");
	}

	printf("\n"
	       "SIZE parameters may use one of the multiplicative suffixes B=1, KB=10^3,\n"
	       "KiB=2^10, MB=10^6, MiB=2^20, GB=10^9, GiB=2^30, TB=10^12, or TiB=2^40.\n");
}

extern bool g_crashOnError;

#if defined(ALLOC_INSTRUMENTATION) || defined(ALLOC_INSTRUMENTATION_STDOUT)
void* operator new(std::size_t size) {
	void* p = malloc(size);
	if (!p)
		throw std::bad_alloc();
	recordAllocation(p, size);
	return p;
}
void operator delete(void* ptr) throw() {
	recordDeallocation(ptr);
	free(ptr);
}

// scalar, nothrow new and it matching delete
void* operator new(std::size_t size, const std::nothrow_t&) throw() {
	void* p = malloc(size);
	recordAllocation(p, size);
	return p;
}
void operator delete(void* ptr, const std::nothrow_t&) throw() {
	recordDeallocation(ptr);
	free(ptr);
}

// array throwing new and matching delete[]
void* operator new[](std::size_t size) {
	void* p = malloc(size);
	if (!p)
		throw std::bad_alloc();
	recordAllocation(p, size);
	return p;
}
void operator delete[](void* ptr) throw() {
	recordDeallocation(ptr);
	free(ptr);
}

// array, nothrow new and matching delete[]
void* operator new[](std::size_t size, const std::nothrow_t&) throw() {
	void* p = malloc(size);
	recordAllocation(p, size);
	return p;
}
void operator delete[](void* ptr, const std::nothrow_t&) throw() {
	recordDeallocation(ptr);
	free(ptr);
}
#endif

Optional<bool> checkBuggifyOverride(const char* testFile) {
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
			// Testspec uses `on` or `off` (without quotes).
			// TOML uses literal `true` and `false`.
			if (!strcmp(value.c_str(), "on") || !strcmp(value.c_str(), "true")) {
				ifs.close();
				return true;
			} else if (!strcmp(value.c_str(), "off") || !strcmp(value.c_str(), "false")) {
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

// Takes a vector of public and listen address strings given via command line, and returns vector of NetworkAddress
// objects.
std::pair<NetworkAddressList, NetworkAddressList> buildNetworkAddresses(
    IClusterConnectionRecord& connectionRecord,
    const std::vector<std::string>& publicAddressStrs,
    std::vector<std::string>& listenAddressStrs) {
	if (listenAddressStrs.size() > 0 && publicAddressStrs.size() != listenAddressStrs.size()) {
		fprintf(stderr,
		        "ERROR: Listen addresses (if provided) should be equal to the number of public addresses in order.\n");
		flushAndExit(FDB_EXIT_ERROR);
	}
	listenAddressStrs.resize(publicAddressStrs.size(), "public");

	if (publicAddressStrs.size() > 2) {
		fprintf(stderr, "ERROR: maximum 2 public/listen addresses are allowed\n");
		flushAndExit(FDB_EXIT_ERROR);
	}

	NetworkAddressList publicNetworkAddresses;
	NetworkAddressList listenNetworkAddresses;

	std::vector<Hostname>& hostnames = connectionRecord.getConnectionString().hostnames;
	const std::vector<NetworkAddress>& coords = connectionRecord.getConnectionString().coords;
	ASSERT(hostnames.size() + coords.size() > 0);

	for (int ii = 0; ii < publicAddressStrs.size(); ++ii) {
		const std::string& publicAddressStr = publicAddressStrs[ii];
		bool autoPublicAddress = StringRef(publicAddressStr).startsWith("auto:"_sr);
		NetworkAddress currentPublicAddress;
		if (autoPublicAddress) {
			try {
				const NetworkAddress& parsedAddress = NetworkAddress::parse("0.0.0.0:" + publicAddressStr.substr(5));
				const IPAddress publicIP = connectionRecord.getConnectionString().determineLocalSourceIP();
				currentPublicAddress = NetworkAddress(publicIP, parsedAddress.port, true, parsedAddress.isTLS());
			} catch (Error& e) {
				fprintf(stderr,
				        "ERROR: could not determine public address automatically from `%s': %s\n",
				        publicAddressStr.c_str(),
				        e.what());
				throw;
			}
		} else {
			try {
				currentPublicAddress = NetworkAddress::parse(publicAddressStr);
			} catch (Error&) {
				fprintf(stderr,
				        "ERROR: Could not parse network address `%s' (specify as IP_ADDRESS:PORT)\n",
				        publicAddressStr.c_str());
				throw;
			}
		}

		if (ii == 0) {
			publicNetworkAddresses.address = currentPublicAddress;
		} else {
			publicNetworkAddresses.secondaryAddress = currentPublicAddress;
		}

		if (!currentPublicAddress.isValid()) {
			fprintf(stderr, "ERROR: %s is not a valid IP address\n", currentPublicAddress.toString().c_str());
			flushAndExit(FDB_EXIT_ERROR);
		}

		const std::string& listenAddressStr = listenAddressStrs[ii];
		NetworkAddress currentListenAddress;
		if (listenAddressStr == "public") {
			currentListenAddress = currentPublicAddress;
		} else {
			try {
				currentListenAddress = NetworkAddress::parse(listenAddressStr);
			} catch (Error&) {
				fprintf(stderr,
				        "ERROR: Could not parse network address `%s' (specify as IP_ADDRESS:PORT)\n",
				        listenAddressStr.c_str());
				throw;
			}

			if (currentListenAddress.isTLS() != currentPublicAddress.isTLS()) {
				fprintf(stderr,
				        "ERROR: TLS state of listen address: %s is not equal to the TLS state of public address: %s.\n",
				        listenAddressStr.c_str(),
				        publicAddressStr.c_str());
				flushAndExit(FDB_EXIT_ERROR);
			}
		}

		if (ii == 0) {
			listenNetworkAddresses.address = currentListenAddress;
		} else {
			listenNetworkAddresses.secondaryAddress = currentListenAddress;
		}

		bool matchCoordinatorsTls = std::all_of(coords.begin(), coords.end(), [&](const NetworkAddress& address) {
			if (address.ip == currentPublicAddress.ip && address.port == currentPublicAddress.port) {
				return address.isTLS() == currentPublicAddress.isTLS();
			}
			return true;
		});
		// If true, further check hostnames.
		if (matchCoordinatorsTls) {
			matchCoordinatorsTls = std::all_of(hostnames.begin(), hostnames.end(), [&](Hostname& hostname) {
				Optional<NetworkAddress> resolvedAddress = hostname.resolveBlocking();
				if (resolvedAddress.present()) {
					NetworkAddress address = resolvedAddress.get();
					if (address.ip == currentPublicAddress.ip && address.port == currentPublicAddress.port) {
						return address.isTLS() == currentPublicAddress.isTLS();
					}
				}
				return true;
			});
		}
		if (!matchCoordinatorsTls) {
			fprintf(stderr,
			        "ERROR: TLS state of public address %s does not match in coordinator list.\n",
			        publicAddressStr.c_str());
			flushAndExit(FDB_EXIT_ERROR);
		}
	}

	if (publicNetworkAddresses.secondaryAddress.present() &&
	    publicNetworkAddresses.address.isTLS() == publicNetworkAddresses.secondaryAddress.get().isTLS()) {
		fprintf(stderr, "ERROR: only one public address of each TLS state is allowed.\n");
		flushAndExit(FDB_EXIT_ERROR);
	}

	return std::make_pair(publicNetworkAddresses, listenNetworkAddresses);
}

// moves files from 'dirSrc' to 'dirToMove' if their name contains 'role'
void restoreRoleFilesHelper(std::string dirSrc, std::string dirToMove, std::string role) {
	std::vector<std::string> returnFiles = platform::listFiles(dirSrc, "");
	for (const auto& fileEntry : returnFiles) {
		if (fileEntry != "fdb.cluster" && fileEntry.find(role) != std::string::npos) {
			// rename files
			TraceEvent("RenamingSnapFile")
			    .detail("Oldname", dirSrc + "/" + fileEntry)
			    .detail("Newname", dirToMove + "/" + fileEntry);
			renameFile(dirSrc + "/" + fileEntry, dirToMove + "/" + fileEntry);
		}
	}
}

namespace {
enum class ServerRole {
	ChangeClusterKey,
	ConsistencyCheck,
	ConsistencyCheckUrgent,
	CreateTemplateDatabase,
	DSLTest,
	FDBD,
	FlowProcess,
	KVFileGenerateIOLogChecksums,
	KVFileIntegrityCheck,
	KVFileDump,
	MultiTester,
	NetworkTestClient,
	NetworkTestServer,
	Restore,
	SearchMutations,
	Simulation,
	SkipListTest,
	Test,
	VersionedMapTest,
	UnitTests
};
struct CLIOptions {
	std::string commandLine;
	std::string fileSystemPath, dataFolder, connFile, seedConnFile, seedConnString,
	    logFolder = ".", metricsConnFile, metricsPrefix, newClusterKey, authzPublicKeyFile;
	std::string logGroup = "default";
	uint64_t rollsize = TRACE_DEFAULT_ROLL_SIZE;
	uint64_t maxLogsSize = TRACE_DEFAULT_MAX_LOGS_SIZE;
	bool maxLogsSizeSet = false;
	int maxLogs = 0;
	bool maxLogsSet = false;

	ServerRole role = ServerRole::FDBD;
	uint32_t randomSeed = platform::getRandomSeed();

	const char* testFile = "tests/default.txt";
	std::string kvFile;
	std::string testServersStr;
	std::string whitelistBinPaths;

	std::vector<std::string> publicAddressStrs, listenAddressStrs, grpcAddressStrs;
	NetworkAddressList publicAddresses, listenAddresses;

	const char* targetKey = nullptr;
	uint64_t memLimit =
	    8LL << 30; // Nice to maintain the same default value for memLimit and SERVER_KNOBS->SERVER_MEM_LIMIT and
	               // SERVER_KNOBS->COMMIT_BATCHES_MEM_BYTES_HARD_LIMIT
	uint64_t virtualMemLimit = 0; // unlimited
	uint64_t storageMemLimit = 1LL << 30;
	bool buggifyEnabled = false, faultInjectionEnabled = true, restarting = false;
	Optional<Standalone<StringRef>> zoneId;
	Optional<Standalone<StringRef>> dcId;
	ProcessClass processClass = ProcessClass(ProcessClass::UnsetClass, ProcessClass::CommandLineSource);
	bool useNet2 = true;
	bool useThreadPool = false;
	std::vector<std::pair<std::string, std::string>> knobs;
	std::map<std::string, std::string> manualKnobOverrides;
	LocalityData localities;
	int minTesterCount = 1;
	bool testOnServers = false;
	bool consistencyCheckUrgentMode = false;

	TLSConfig tlsConfig = TLSConfig(TLSEndpointType::SERVER);
	double fileIoTimeout = 0.0;
	bool fileIoWarnOnly = false;
	uint64_t rsssize = -1;
	std::vector<std::string> blobCredentials; // used for fast restore workers & backup workers
	Optional<std::string> proxy;
	const char* blobCredsFromENV = nullptr;

	std::string configPath;
	ConfigDBType configDBType{ ConfigDBType::PAXOS };

	Reference<IClusterConnectionRecord> connectionFile;
	Standalone<StringRef> machineId;
	UnitTestParameters testParams;

	std::map<std::string, std::string> profilerConfig;
	std::string flowProcessName;
	Endpoint flowProcessEndpoint;
	bool printSimTime = false;
	IPAllowList allowList;

	static CLIOptions parseArgs(int argc, char* argv[]) {
		CLIOptions opts;
		opts.parseArgsInternal(argc, argv);
		opts.parseEnvInternal();
		return opts;
	}

	// Determine publicAddresses and listenAddresses by calling buildNetworkAddresses().
	void buildNetwork(const char* name) {
		try {
			if (!publicAddressStrs.empty()) {
				std::tie(publicAddresses, listenAddresses) =
				    buildNetworkAddresses(*connectionFile, publicAddressStrs, listenAddressStrs);
			}
		} catch (Error&) {
			printHelpTeaser(name);
			flushAndExit(FDB_EXIT_ERROR);
		}

		for (auto& s : grpcAddressStrs) {
			fmt::printf("gRPC Endpoint: %s\n", s);
		}

		if (role == ServerRole::ConsistencyCheck || role == ServerRole::ConsistencyCheckUrgent) {
			if (!publicAddressStrs.empty()) {
				fprintf(stderr, "ERROR: Public address cannot be specified for consistency check processes\n");
				printHelpTeaser(name);
				flushAndExit(FDB_EXIT_ERROR);
			}
			auto publicIP = connectionFile->getConnectionString().determineLocalSourceIP();
			publicAddresses.address = NetworkAddress(publicIP, ::getpid());
		}
	}

private:
	CLIOptions() = default;

	void parseEnvInternal() {
		for (const std::string& knob : getEnvironmentKnobOptions()) {
			auto pos = knob.find_first_of("=");
			if (pos == std::string::npos) {
				fprintf(stderr,
				        "Error: malformed environment knob option: %s%s\n",
				        ENVIRONMENT_KNOB_OPTION_PREFIX,
				        knob.c_str());
				TraceEvent(SevWarnAlways, "MalformedEnvironmentVariableKnob")
				    .detail("Key", ENVIRONMENT_KNOB_OPTION_PREFIX + knob);
			} else {
				std::string k = knob.substr(0, pos);
				std::string v = knob.substr(pos + 1, knob.length());
				knobs.emplace_back(k, v);
				manualKnobOverrides[k] = v;
			}
		}
	}

	void parseArgsInternal(int argc, char* argv[]) {
		for (int a = 0; a < argc; a++) {
			if (a)
				commandLine += ' ';
			commandLine += argv[a];
		}

		CSimpleOpt args(argc, argv, g_rgOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);

		if (argc == 1) {
			printUsage(argv[0], false);
			flushAndExit(FDB_EXIT_ERROR);
		}

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
			const char* sRole;
			Optional<uint64_t> ti;
			std::string argStr;
			std::vector<std::string> tmpStrings;

			switch (args.OptionId()) {
			case OPT_HELP:
				printUsage(argv[0], false);
				flushAndExit(FDB_EXIT_SUCCESS);
				break;
			case OPT_DEVHELP:
				printUsage(argv[0], true);
				flushAndExit(FDB_EXIT_SUCCESS);
				break;
			case OPT_PRINT_CODE_PROBES:
				probe::ICodeProbe::printProbesJSON({ std::string(args.OptionArg()) });
				flushAndExit(FDB_EXIT_SUCCESS);
				break;
			case OPT_KNOB: {
				Optional<std::string> knobName = extractPrefixedArgument("--knob", args.OptionSyntax());
				if (!knobName.present()) {
					fprintf(stderr, "ERROR: unable to parse knob option '%s'\n", args.OptionSyntax());
					flushAndExit(FDB_EXIT_ERROR);
				}
				knobs.emplace_back(knobName.get(), args.OptionArg());
				manualKnobOverrides[knobName.get()] = args.OptionArg();
				break;
			}
			case OPT_PROFILER: {
				Optional<std::string> profilerArg = extractPrefixedArgument("--profiler", args.OptionSyntax());
				if (!profilerArg.present()) {
					fprintf(stderr, "ERROR: unable to parse profiler option '%s'\n", args.OptionSyntax());
					flushAndExit(FDB_EXIT_ERROR);
				}
				profilerConfig.emplace(profilerArg.get(), args.OptionArg());
				break;
			};
			case OPT_UNITTESTPARAM: {
				Optional<std::string> testArg = extractPrefixedArgument("--test", args.OptionSyntax());
				if (!testArg.present()) {
					fprintf(stderr, "ERROR: unable to parse unit test option '%s'\n", args.OptionSyntax());
					flushAndExit(FDB_EXIT_ERROR);
				}
				testParams.set(testArg.get(), args.OptionArg());
				break;
			}
			case OPT_LOCALITY: {
				Optional<std::string> localityKey = extractPrefixedArgument("--locality", args.OptionSyntax());
				if (!localityKey.present()) {
					fprintf(stderr, "ERROR: unable to parse locality key '%s'\n", args.OptionSyntax());
					flushAndExit(FDB_EXIT_ERROR);
				}
				Standalone<StringRef> key = StringRef(localityKey.get());
				std::transform(key.begin(), key.end(), mutateString(key), ::tolower);
				localities.set(key, Standalone<StringRef>(std::string(args.OptionArg())));
				break;
			}
			case OPT_IP_TRUSTED_MASK: {
				Optional<std::string> subnetKey = extractPrefixedArgument("--trusted-subnet", args.OptionSyntax());
				if (!subnetKey.present()) {
					fprintf(stderr, "ERROR: unable to parse locality key '%s'\n", args.OptionSyntax());
					flushAndExit(FDB_EXIT_ERROR);
				}
				allowList.addTrustedSubnet(args.OptionArg());
				break;
			}
			case OPT_VERSION:
				printVersion();
				flushAndExit(FDB_EXIT_SUCCESS);
				break;
			case OPT_BUILD_FLAGS:
				printBuildInformation();
				flushAndExit(FDB_EXIT_SUCCESS);
			case OPT_NOBUFSTDOUT:
				setvbuf(stdout, nullptr, _IONBF, 0);
				setvbuf(stderr, nullptr, _IONBF, 0);
				break;
			case OPT_BUFSTDOUTERR:
				setvbuf(stdout, nullptr, _IOFBF, BUFSIZ);
				setvbuf(stderr, nullptr, _IOFBF, BUFSIZ);
				break;
			case OPT_ROLE:
				sRole = args.OptionArg();
				if (!strcmp(sRole, "fdbd"))
					role = ServerRole::FDBD;
				else if (!strcmp(sRole, "simulation"))
					role = ServerRole::Simulation;
				else if (!strcmp(sRole, "test"))
					role = ServerRole::Test;
				else if (!strcmp(sRole, "multitest"))
					role = ServerRole::MultiTester;
				else if (!strcmp(sRole, "skiplisttest"))
					role = ServerRole::SkipListTest;
				else if (!strcmp(sRole, "search"))
					role = ServerRole::SearchMutations;
				else if (!strcmp(sRole, "dsltest"))
					role = ServerRole::DSLTest;
				else if (!strcmp(sRole, "versionedmaptest"))
					role = ServerRole::VersionedMapTest;
				else if (!strcmp(sRole, "createtemplatedb"))
					role = ServerRole::CreateTemplateDatabase;
				else if (!strcmp(sRole, "networktestclient"))
					role = ServerRole::NetworkTestClient;
				else if (!strcmp(sRole, "networktestserver"))
					role = ServerRole::NetworkTestServer;
				else if (!strcmp(sRole, "restore"))
					role = ServerRole::Restore;
				else if (!strcmp(sRole, "kvfileintegritycheck"))
					role = ServerRole::KVFileIntegrityCheck;
				else if (!strcmp(sRole, "kvfilegeneratesums"))
					role = ServerRole::KVFileGenerateIOLogChecksums;
				else if (!strcmp(sRole, "kvfiledump"))
					role = ServerRole::KVFileDump;
				else if (!strcmp(sRole, "consistencycheck"))
					role = ServerRole::ConsistencyCheck;
				else if (!strcmp(sRole, "consistencycheckurgent"))
					role = ServerRole::ConsistencyCheckUrgent;
				else if (!strcmp(sRole, "unittests"))
					role = ServerRole::UnitTests;
				else if (!strcmp(sRole, "flowprocess"))
					role = ServerRole::FlowProcess;
				else if (!strcmp(sRole, "changeclusterkey"))
					role = ServerRole::ChangeClusterKey;
				else {
					fprintf(stderr, "ERROR: Unknown role `%s'\n", sRole);
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
				break;
			case OPT_PUBLICADDR:
				argStr = args.OptionArg();
				boost::split(tmpStrings, argStr, [](char c) { return c == ','; });
				for (auto& addr : tmpStrings) {
					if (addr.ends_with(":grpc")) {
						grpcAddressStrs.push_back(addr.substr(0, addr.size() - std::string(":grpc").size()));
					} else {
						publicAddressStrs.push_back(addr);
					}
				}
				break;
			case OPT_LISTEN:
				argStr = args.OptionArg();
				boost::split(tmpStrings, argStr, [](char c) { return c == ','; });
				listenAddressStrs.insert(listenAddressStrs.end(), tmpStrings.begin(), tmpStrings.end());
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
			case OPT_PROFILER_RSS_SIZE: {
				const char* a = args.OptionArg();
				char* end;
				rsssize = strtoull(a, &end, 10);
				if (*end) {
					fprintf(stderr, "ERROR: Unrecognized memory size `%s'\n", a);
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
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
				if (!strcmp(a, "net2"))
					useNet2 = true;
				else if (!strcmp(a, "net2-threadpool")) {
					useNet2 = true;
					useThreadPool = true;
				} else {
					fprintf(stderr, "ERROR: Unknown network implementation `%s'\n", a);
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
				break;
			}
			case OPT_TRACECLOCK: {
				const char* a = args.OptionArg();
				if (!strcmp(a, "realtime"))
					g_trace_clock.store(TRACE_CLOCK_REALTIME);
				else if (!strcmp(a, "now"))
					g_trace_clock.store(TRACE_CLOCK_NOW);
				else {
					fprintf(stderr, "ERROR: Unknown clock source `%s'\n", a);
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
				break;
			}
			case OPT_NUMTESTERS: {
				const char* a = args.OptionArg();
				if (!sscanf(a, "%d", &minTesterCount)) {
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
				const char* a = args.OptionArg();
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
				const char* a = args.OptionArg();
				char* end;
				maxLogs = strtoull(a, &end, 10);
				if (*end) {
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
				auto pHandle = OpenProcess(SYNCHRONIZE, FALSE, parent_pid);
				if (!pHandle) {
					TraceEvent("ParentProcessOpenError").GetLastError();
					fprintf(stderr, "Could not open parent process at pid %d (error %d)", parent_pid, GetLastError());
					throw platform_error();
				}
				startThread(&parentWatcher, pHandle, 0, "fdb-parentwatch");
				break;
			}
			case OPT_NEWCONSOLE:
				FreeConsole();
				AllocConsole();
				freopen("CONIN$", "rb", stdin);
				freopen("CONOUT$", "wb", stdout);
				freopen("CONOUT$", "wb", stderr);
				break;
			case OPT_NOBOX:
				SetErrorMode(SetErrorMode(0) | SEM_NOGPFAULTERRORBOX);
				break;
#else
			case OPT_PARENTPID: {
				auto pid_str = args.OptionArg();
				int* parent_pid = new (int);
				*parent_pid = atoi(pid_str);
				startThread(&parentWatcher, parent_pid, 0, "fdb-parentwatch");
				break;
			}
#endif
			case OPT_TRACER: {
				std::string arg = args.OptionArg();
				std::string tracer;
				std::transform(arg.begin(), arg.end(), std::back_inserter(tracer), [](char c) { return tolower(c); });
				if (tracer == "none" || tracer == "disabled") {
					openTracer(TracerType::DISABLED);
				} else if (tracer == "logfile" || tracer == "file" || tracer == "log_file") {
					openTracer(TracerType::LOG_FILE);
				} else if (tracer == "network_lossy") {
					openTracer(TracerType::NETWORK_LOSSY);
				} else {
					fprintf(stderr, "ERROR: Unknown or unsupported tracer: `%s'", args.OptionArg());
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
				break;
			}
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
				randomSeed = (uint32_t)strtoul(args.OptionArg(), &end, 0);
				if (*end) {
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
				processClass = ProcessClass(sRole, ProcessClass::CommandLineSource);
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
			case OPT_VMEMLIMIT:
				ti = parse_with_suffix(args.OptionArg(), "MiB");
				if (!ti.present()) {
					fprintf(stderr, "ERROR: Could not parse virtual memory limit from `%s'\n", args.OptionArg());
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
				virtualMemLimit = ti.get();
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
			case OPT_CACHEMEMLIMIT:
				ti = parse_with_suffix(args.OptionArg(), "MiB");
				if (!ti.present()) {
					fprintf(stderr, "ERROR: Could not parse cache memory limit from `%s'\n", args.OptionArg());
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
				// SOMEDAY: ideally we'd have some better way to express that a knob should be elevated to formal
				// parameter
				knobs.emplace_back(
				    "page_cache_4k",
				    format("%lld", ti.get() / 4096 * 4096)); // The cache holds 4K pages, so we can truncate this to the
				                                             // next smaller multiple of 4K.
				break;
			case OPT_BUGGIFY:
				if (!strcmp(args.OptionArg(), "on"))
					buggifyEnabled = true;
				else if (!strcmp(args.OptionArg(), "off"))
					buggifyEnabled = false;
				else {
					fprintf(stderr, "ERROR: Unknown buggify state `%s'\n", args.OptionArg());
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
				break;
			case OPT_FAULT_INJECTION:
				if (!strcmp(args.OptionArg(), "on"))
					faultInjectionEnabled = true;
				else if (!strcmp(args.OptionArg(), "off"))
					faultInjectionEnabled = false;
				else {
					fprintf(stderr, "ERROR: Unknown fault injection state `%s'\n", args.OptionArg());
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
			case OPT_CONSISTENCY_CHECK_URGENT_MODE:
				consistencyCheckUrgentMode = true;
				break;
			case OPT_METRICSCONNFILE:
				metricsConnFile = args.OptionArg();
				break;
			case OPT_METRICSPREFIX:
				metricsPrefix = args.OptionArg();
				break;
			case OPT_IO_TRUST_SECONDS: {
				const char* a = args.OptionArg();
				if (!sscanf(a, "%lf", &fileIoTimeout)) {
					fprintf(stderr, "ERROR: Could not parse io_trust_seconds `%s'\n", a);
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
				break;
			}
			case OPT_IO_TRUST_WARN_ONLY:
				fileIoWarnOnly = true;
				break;
			case OPT_TRACE_FORMAT:
				if (!selectTraceFormatter(args.OptionArg())) {
					fprintf(stderr, "WARNING: Unrecognized trace format `%s'\n", args.OptionArg());
				}
				break;
			case OPT_WHITELIST_BINPATH:
				whitelistBinPaths = args.OptionArg();
				break;
			case OPT_BLOB_CREDENTIALS:
				// Add blob credential following backup agent example
				blobCredentials.push_back(args.OptionArg());
				break;
			case OPT_PROXY:
				proxy = args.OptionArg();
				if (!Hostname::isHostname(proxy.get()) && !NetworkAddress::parseOptional(proxy.get()).present()) {
					fprintf(stderr, "ERROR: proxy format should be either IP:port or host:port\n");
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
				break;
			case OPT_CONFIG_PATH:
				configPath = args.OptionArg();
				break;
			case OPT_USE_TEST_CONFIG_DB:
				configDBType = ConfigDBType::SIMPLE;
				break;
			case OPT_NO_CONFIG_DB:
				configDBType = ConfigDBType::DISABLED;
				break;
			case OPT_FLOW_PROCESS_NAME:
				flowProcessName = args.OptionArg();
				std::cout << flowProcessName << std::endl;
				break;
			case OPT_FLOW_PROCESS_ENDPOINT: {
				std::vector<std::string> strings;
				std::cout << args.OptionArg() << std::endl;
				boost::split(strings, args.OptionArg(), [](char c) { return c == ','; });
				for (auto& str : strings) {
					std::cout << str << " ";
				}
				std::cout << "\n";
				if (strings.size() != 3) {
					std::cerr << "Invalid argument, expected 3 elements in --process-endpoint got " << strings.size()
					          << std::endl;
					flushAndExit(FDB_EXIT_ERROR);
				}
				try {
					auto addr = NetworkAddress::parse(strings[0]);
					uint64_t fst = std::stoul(strings[1]);
					uint64_t snd = std::stoul(strings[2]);
					UID token(fst, snd);
					NetworkAddressList l;
					l.address = addr;
					flowProcessEndpoint = Endpoint(l, token);
					std::cout << "flowProcessEndpoint: " << flowProcessEndpoint.getPrimaryAddress().toString()
					          << ", token: " << flowProcessEndpoint.token.toString() << "\n";
				} catch (Error& e) {
					std::cerr << "Could not parse network address " << strings[0] << std::endl;
					flushAndExit(FDB_EXIT_ERROR);
				} catch (std::exception& e) {
					std::cerr << "Could not parse token " << strings[1] << "," << strings[2] << std::endl;
					flushAndExit(FDB_EXIT_ERROR);
				}
				break;
			}
			case OPT_PRINT_SIMTIME:
				printSimTime = true;
				break;

			case TLSConfig::OPT_TLS_PLUGIN:
				args.OptionArg();
				break;
			case TLSConfig::OPT_TLS_CERTIFICATES:
				tlsConfig.setCertificatePath(args.OptionArg());
				break;
			case TLSConfig::OPT_TLS_PASSWORD:
				tlsConfig.setPassword(args.OptionArg());
				break;
			case TLSConfig::OPT_TLS_CA_FILE:
				tlsConfig.setCAPath(args.OptionArg());
				break;
			case TLSConfig::OPT_TLS_KEY:
				tlsConfig.setKeyPath(args.OptionArg());
				break;
			case TLSConfig::OPT_TLS_VERIFY_PEERS:
				tlsConfig.addVerifyPeers(args.OptionArg());
				break;
			case TLSConfig::OPT_TLS_DISABLE_PLAINTEXT_CONNECTION:
				tlsConfig.setDisablePlainTextConnection(true);
				break;
			case OPT_KMS_CONN_DISCOVERY_URL_FILE: {
				knobs.emplace_back("rest_kms_connector_discover_kms_url_file", args.OptionArg());
				break;
			}
			case OPT_KMS_CONNECTOR_TYPE: {
				knobs.emplace_back("kms_connector_type", args.OptionArg());
				break;
			}
			case OPT_KMS_REST_ALLOW_NOT_SECURE_CONECTION: {
				TraceEvent(SevWarnAlways, "RESTKmsConnAllowNotSecureConnection");
				knobs.emplace_back("rest_kms_allow_not_secure_connection", "true");
				break;
			}
			case OPT_KMS_CONN_VALIDATION_TOKEN_DETAILS: {
				knobs.emplace_back("rest_kms_connector_validation_token_details", args.OptionArg());
				break;
			}
			case OPT_KMS_CONN_GET_ENCRYPTION_KEYS_ENDPOINT: {
				knobs.emplace_back("rest_kms_connector_get_encryption_keys_endpoint", args.OptionArg());
				break;
			}
			case OPT_KMS_CONN_GET_LATEST_ENCRYPTION_KEYS_ENDPOINT: {
				knobs.emplace_back("rest_kms_connector_get_latest_encryption_keys_endpoint", args.OptionArg());
				break;
			}
			case OPT_KMS_CONN_GET_BLOB_METADATA_ENDPOINT: {
				knobs.emplace_back("rest_kms_connector_get_blob_metadata_endpoint", args.OptionArg());
				break;
			}
			case OPT_NEW_CLUSTER_KEY: {
				newClusterKey = args.OptionArg();
				try {
					ClusterConnectionString ccs;
					// make sure the new cluster key is in valid format
					ccs.parseKey(newClusterKey);
				} catch (Error& e) {
					std::cerr << "Invalid cluster key(description:id) '" << newClusterKey << "' from --new-cluster-key"
					          << std::endl;
					flushAndExit(FDB_EXIT_ERROR);
				}
				break;
			}
			case OPT_AUTHZ_PUBLIC_KEY_FILE: {
				authzPublicKeyFile = args.OptionArg();
				break;
			}
			case OPT_USE_FUTURE_PROTOCOL_VERSION: {
				if (!strcmp(args.OptionArg(), "true")) {
					::useFutureProtocolVersion();
				}
				break;
			}
			}
		}
		// Sets up blob credentials, including one from the environment FDB_BLOB_CREDENTIALS.
		// Below is top-half of BackupTLSConfig::setupBlobCredentials().
		const char* blobCredsFromENV = getenv("FDB_BLOB_CREDENTIALS");
		if (blobCredsFromENV != nullptr) {
			StringRef t((uint8_t*)blobCredsFromENV, strlen(blobCredsFromENV));
			do {
				StringRef file = t.eat(":");
				if (file.size() != 0)
					blobCredentials.push_back(file.toString());
			} while (t.size() != 0);
		}

		// Sets up proxy from ENV if it is not set by arg.
		const char* proxyENV = getenv("FDB_PROXY");
		if (proxyENV != nullptr && !proxy.present()) {
			proxy = proxyENV;
			if (!Hostname::isHostname(proxy.get()) && !NetworkAddress::parseOptional(proxy.get()).present()) {
				fprintf(stderr, "ERROR: proxy format should be either IP:port or host:port\n");
				printHelpTeaser(argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
		}

		setThreadLocalDeterministicRandomSeed(randomSeed);

		try {
			ProfilerConfig::instance().reset(profilerConfig);
		} catch (ConfigError& e) {
			printf("Error setting up profiler: %s", e.description.c_str());
			flushAndExit(FDB_EXIT_ERROR);
		}

		if (seedConnString.length() && seedConnFile.length()) {
			fprintf(
			    stderr, "%s\n", "--seed-cluster-file and --seed-connection-string may not both be specified at once.");
			flushAndExit(FDB_EXIT_ERROR);
		}

		bool seedSpecified = seedConnFile.length() || seedConnString.length();

		if (seedSpecified && !connFile.length()) {
			fprintf(stderr,
			        "%s\n",
			        "If -seed-cluster-file or --seed-connection-string is specified, -C must be specified as well.");
			flushAndExit(FDB_EXIT_ERROR);
		}

		if (metricsConnFile == connFile)
			metricsConnFile = "";

		if (metricsConnFile != "" && metricsPrefix == "") {
			fprintf(stderr, "If a metrics cluster file is specified, a metrics prefix is required.\n");
			flushAndExit(FDB_EXIT_ERROR);
		}

		bool autoPublicAddress =
		    std::any_of(publicAddressStrs.begin(), publicAddressStrs.end(), [](const std::string& addr) {
			    return StringRef(addr).startsWith("auto:"_sr);
		    });
		if ((role != ServerRole::Simulation && role != ServerRole::CreateTemplateDatabase &&
		     role != ServerRole::KVFileIntegrityCheck && role != ServerRole::KVFileGenerateIOLogChecksums &&
		     role != ServerRole::KVFileDump && role != ServerRole::UnitTests) ||
		    autoPublicAddress) {

			if (seedSpecified && !fileExists(connFile)) {
				std::string connectionString = seedConnString.length() ? seedConnString : "";
				ClusterConnectionString ccs;
				if (seedConnFile.length()) {
					try {
						connectionString = readFileBytes(seedConnFile, MAX_CLUSTER_FILE_BYTES);
					} catch (Error& e) {
						fprintf(stderr,
						        "%s\n",
						        ClusterConnectionFile::getErrorString(std::make_pair(seedConnFile, false), e).c_str());
						throw;
					}
				}

				try {
					ccs = ClusterConnectionString(connectionString);
				} catch (Error& e) {
					fprintf(stderr, "%s\n", ClusterConnectionString::getErrorString(connectionString, e).c_str());
					throw;
				}
				connectionFile = makeReference<ClusterConnectionFile>(connFile, ccs);
			} else {
				std::pair<std::string, bool> resolvedClusterFile;
				try {
					resolvedClusterFile = ClusterConnectionFile::lookupClusterFileName(connFile);
					connectionFile = makeReference<ClusterConnectionFile>(resolvedClusterFile.first);
				} catch (Error& e) {
					fprintf(stderr, "%s\n", ClusterConnectionFile::getErrorString(resolvedClusterFile, e).c_str());
					throw;
				}
			}

			// failmon?
		}

		if (role == ServerRole::Simulation) {
			Optional<bool> buggifyOverride = checkBuggifyOverride(testFile);
			if (buggifyOverride.present())
				buggifyEnabled = buggifyOverride.get();
		}

		if (role == ServerRole::SearchMutations && !targetKey) {
			fprintf(stderr, "ERROR: please specify a target key\n");
			printHelpTeaser(argv[0]);
			flushAndExit(FDB_EXIT_ERROR);
		}

		if (role == ServerRole::NetworkTestClient && !testServersStr.size()) {
			fprintf(stderr, "ERROR: please specify --testservers\n");
			printHelpTeaser(argv[0]);
			flushAndExit(FDB_EXIT_ERROR);
		}

		if (role == ServerRole::ChangeClusterKey) {
			bool error = false;
			if (!newClusterKey.size()) {
				fprintf(stderr, "ERROR: please specify --new-cluster-key\n");
				error = true;
			} else if (connectionFile->getConnectionString().clusterKey() == newClusterKey) {
				fprintf(stderr, "ERROR: the new cluster key is the same as the old one\n");
				error = true;
			}
			if (error) {
				printHelpTeaser(argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
		}

		// Interpret legacy "maxLogs" option in the most sensible and unsurprising way we can while eliminating its code
		// path
		if (maxLogsSet) {
			if (maxLogsSizeSet) {
				// This is the case where both options are set and we must deconflict.
				auto maxLogsAsSize = maxLogs * rollsize;

				// If either was unlimited, then the safe option here is to take the larger one.
				//  This means that is one of the two options specified a limited amount of logging
				//  then the option that specified "unlimited" will be ignored.
				if (maxLogsSize == 0 || maxLogs == 0)
					maxLogsSize = std::max(maxLogsSize, maxLogsAsSize);
				else
					maxLogsSize = std::min(maxLogsSize, maxLogs * rollsize);
			} else {
				maxLogsSize = maxLogs * rollsize;
			}
		}
		if (!zoneId.present() &&
		    !(localities.isPresent(LocalityData::keyZoneId) && localities.isPresent(LocalityData::keyMachineId))) {
			machineId = getSharedMemoryMachineId().toString();
		}
		if (!localities.isPresent(LocalityData::keyZoneId))
			localities.set(LocalityData::keyZoneId, zoneId.present() ? zoneId : machineId);

		if (!localities.isPresent(LocalityData::keyMachineId))
			localities.set(LocalityData::keyMachineId, zoneId.present() ? zoneId : machineId);

		if (!localities.isPresent(LocalityData::keyDcId) && dcId.present())
			localities.set(LocalityData::keyDcId, dcId);
	}
};

// Returns true iff validation is successful
bool validateSimulationDataFiles(std::string const& dataFolder, bool isRestarting) {
	std::vector<std::string> files = platform::listFiles(dataFolder);
	if (!isRestarting) {
		for (const auto& file : files) {
			if (file != "restartInfo.ini" && file != getTestEncryptionFileName()) {
				TraceEvent(SevError, "IncompatibleFileFound").detail("DataFolder", dataFolder).detail("FileName", file);
				fprintf(stderr,
				        "ERROR: Data folder `%s' is non-empty; please use clean, fdb-only folder\n",
				        dataFolder.c_str());
				return false;
			}
		}
	} else if (isRestarting && files.empty()) {
		TraceEvent(SevWarnAlways, "FileNotFound").detail("DataFolder", dataFolder);
		printf("ERROR: Data folder `%s' is empty, but restarting option selected. Run Phase 1 test first\n",
		       dataFolder.c_str());
		return false;
	}

	return true;
}

} // namespace

int main(int argc, char* argv[]) {
	// TODO: Remove later, this is just to force the statics to be initialized
	// otherwise the unit test won't run
#ifdef ENABLE_SAMPLING
	ActorLineageSet _;
#endif
	try {
		platformInit();

#ifdef ALLOC_INSTRUMENTATION
		g_extra_memory = new uint8_t[1000000];
#endif
		registerCrashHandler();

		// Set default of line buffering standard out and error
		setvbuf(stdout, nullptr, _IOLBF, BUFSIZ);
		setvbuf(stderr, nullptr, _IOLBF, BUFSIZ);

		// Enables profiling on this thread (but does not start it)
		registerThreadForProfiling();

#ifdef _WIN32
		// Windows needs a gentle nudge to format floats correctly
		//_set_output_format(_TWO_DIGIT_EXPONENT);
#endif

		auto opts = CLIOptions::parseArgs(argc, argv);
		const auto role = opts.role;

		if (role == ServerRole::Simulation) {
			printf("Random seed is %u...\n", opts.randomSeed);
			bindDeterministicRandomToOpenssl();
		}

		if (opts.zoneId.present())
			printf("ZoneId set to %s, dcId to %s\n", printable(opts.zoneId).c_str(), printable(opts.dcId).c_str());

		if (opts.buggifyEnabled) {
			enableGeneralBuggify();
		} else {
			disableGeneralBuggify();
		}
		enableFaultInjection(opts.faultInjectionEnabled);

		IKnobCollection::setGlobalKnobCollection(IKnobCollection::Type::SERVER,
		                                         Randomize::True,
		                                         role == ServerRole::Simulation ? IsSimulated::True
		                                                                        : IsSimulated::False);
		auto& g_knobs = IKnobCollection::getMutableGlobalKnobCollection();
		g_knobs.setKnob("log_directory", KnobValue::create(opts.logFolder));
		g_knobs.setKnob("conn_file", KnobValue::create(opts.connFile));
		if (role != ServerRole::Simulation && opts.memLimit > 0) {
			g_knobs.setKnob("commit_batches_mem_bytes_hard_limit",
			                KnobValue::create(static_cast<int64_t>(opts.memLimit)));
		}

		IKnobCollection::setupKnobs(opts.knobs);
		g_knobs.setKnob("server_mem_limit", KnobValue::create(static_cast<int64_t>(opts.memLimit)));
		// Reinitialize knobs in order to update knobs that are dependent on explicitly set knobs
		g_knobs.initialize(Randomize::True, role == ServerRole::Simulation ? IsSimulated::True : IsSimulated::False);

		if (!SERVER_KNOBS->ALLOW_DANGEROUS_KNOBS) {
			if (SERVER_KNOBS->REMOTE_KV_STORE) {
				fprintf(stderr,
				        "ERROR : explicitly setting REMOTE_KV_STORE is dangerous! set ALLOW_DANGEROUS_KNOBS to "
				        "proceed anyways\n");
				flushAndExit(FDB_EXIT_ERROR);
			}
		}

		// evictionPolicyStringToEnum will throw an exception if the string is not recognized as a valid
		EvictablePageCache::evictionPolicyStringToEnum(FLOW_KNOBS->CACHE_EVICTION_POLICY);

		if (opts.memLimit > 0 && opts.virtualMemLimit > 0 && opts.memLimit > opts.virtualMemLimit) {
			fprintf(stderr, "ERROR : --memory-vsize has to be no less than --memory");
			flushAndExit(FDB_EXIT_ERROR);
		}

		if (opts.memLimit > 0 && opts.memLimit <= FLOW_KNOBS->PAGE_CACHE_4K) {
			fprintf(stderr, "ERROR: --memory has to be larger than --cache-memory\n");
			flushAndExit(FDB_EXIT_ERROR);
		}

		if (role == ServerRole::SkipListTest) {
			skipListTest();
			flushAndExit(FDB_EXIT_SUCCESS);
		}

		if (role == ServerRole::DSLTest) {
			dsltest();
			flushAndExit(FDB_EXIT_SUCCESS);
		}

		if (role == ServerRole::VersionedMapTest) {
			versionedMapTest();
			flushAndExit(FDB_EXIT_SUCCESS);
		}

		// Initialize the thread pool
		CoroThreadPool::init();
		// Ordinarily, this is done when the network is run. However, network thread should be set before TraceEvents
		// are logged. This thread will eventually run the network, so call it now.
		TraceEvent::setNetworkThread();

		std::vector<Future<Void>> listenErrors;

		if (role == ServerRole::Simulation || role == ServerRole::CreateTemplateDatabase) {
			// startOldSimulator();
			opts.buildNetwork(argv[0]);
			startNewSimulator(opts.printSimTime);

			if (SERVER_KNOBS->FLOW_WITH_SWIFT) {
				// TODO (Swift): Make it TraceEvent
				// printf("[%s:%d](%s) Installed Swift concurrency hooks: sim2 (g_network)\n",
				//        __FILE_NAME__,
				//        __LINE__,
				//        __FUNCTION__);
				installSwiftConcurrencyHooks(role == ServerRole::Simulation, g_network);
			}

			openTraceFile({}, opts.rollsize, opts.maxLogsSize, opts.logFolder, "trace", opts.logGroup);
			openTracer(TracerType(deterministicRandom()->randomInt(static_cast<int>(TracerType::DISABLED),
			                                                       static_cast<int>(TracerType::SIM_END))));
		} else {
			g_network = newNet2(opts.tlsConfig, opts.useThreadPool, true);

			if (SERVER_KNOBS->FLOW_WITH_SWIFT) {
				installSwiftConcurrencyHooks(role == ServerRole::Simulation, g_network);
				// TODO (Swift): Make it TraceEvent
				// printf("[%s:%d](%s) Installed Swift concurrency hooks: net2 (g_network)\n",
				//        __FILE_NAME__,
				//        __LINE__,
				//        __FUNCTION__);
			}

#if WITH_SWIFT
			// Set FDBSWIFTTEST env variable to execute some simple Swift/Flow interop tests.
			if (SERVER_KNOBS->FLOW_WITH_SWIFT && getenv("FDBSWIFTTEST")) {
				swiftTestRunner(); // spawns actor that will call Swift functions
			}
#endif

			g_network->addStopCallback(Net2FileSystem::stop);
			FlowTransport::createInstance(false, 1, WLTOKEN_RESERVED_COUNT, &opts.allowList);
			opts.buildNetwork(argv[0]);

			const bool expectsPublicAddress = (role == ServerRole::FDBD || role == ServerRole::NetworkTestServer ||
			                                   role == ServerRole::Restore || role == ServerRole::FlowProcess);
			if (opts.publicAddressStrs.empty()) {
				if (expectsPublicAddress) {
					fprintf(stderr, "ERROR: The -p or --public-address option is required\n");
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
			}

			openTraceFile(opts.publicAddresses.address,
			              opts.rollsize,
			              opts.maxLogsSize,
			              opts.logFolder,
			              "trace",
			              opts.logGroup,
			              /* identifier = */ "",
			              /* tracePartialFileSuffix = */ "",
			              InitializeTraceMetrics::True);

			g_network->initTLS();
			if (!opts.authzPublicKeyFile.empty()) {
				try {
					FlowTransport::transport().loadPublicKeyFile(opts.authzPublicKeyFile);
				} catch (Error& e) {
					TraceEvent("AuthzPublicKeySetLoadError").error(e);
				}
				FlowTransport::transport().watchPublicKeyFile(opts.authzPublicKeyFile);
			} else {
				TraceEvent(SevInfo, "AuthzPublicKeyFileNotSet");
			}
			if (FLOW_KNOBS->ALLOW_TOKENLESS_TENANT_ACCESS)
				TraceEvent(SevWarnAlways, "AuthzTokenlessAccessEnabled");

			if (expectsPublicAddress) {
				for (int ii = 0; ii < (opts.publicAddresses.secondaryAddress.present() ? 2 : 1); ++ii) {
					const NetworkAddress& publicAddress =
					    ii == 0 ? opts.publicAddresses.address : opts.publicAddresses.secondaryAddress.get();
					const NetworkAddress& listenAddress =
					    ii == 0 ? opts.listenAddresses.address : opts.listenAddresses.secondaryAddress.get();
					try {
						const Future<Void>& errorF = FlowTransport::transport().bind(publicAddress, listenAddress);
						listenErrors.push_back(errorF);
						if (errorF.isReady())
							errorF.get();
					} catch (Error& e) {
						TraceEvent("BindError").error(e);
						fprintf(stderr,
						        "Error initializing networking with public address %s and listen address %s (%s)\n",
						        publicAddress.toString().c_str(),
						        listenAddress.toString().c_str(),
						        e.what());
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
				}
			}

			// Use a negative ioTimeout to indicate warn-only
			Net2FileSystem::newFileSystem(opts.fileIoWarnOnly ? -opts.fileIoTimeout : opts.fileIoTimeout,
			                              opts.fileSystemPath);
			g_network->initMetrics();
			FlowTransport::transport().initMetrics();
		}

		double start = timer(), startNow = now();

		std::string cwd = "<unknown>";
		try {
			cwd = platform::getWorkingDirectory();
		} catch (Error& e) {
			// Allow for platform error by rethrowing all _other_ errors
			if (e.code() != error_code_platform_error)
				throw;
		}

		std::string environmentKnobOptions;
		for (const std::string& knobOption : getEnvironmentKnobOptions()) {
			environmentKnobOptions += knobOption + " ";
		}
		if (environmentKnobOptions.length()) {
			environmentKnobOptions.pop_back();
		}

		TraceEvent("ProgramStart")
		    .setMaxEventLength(12000)
		    .detail("RandomSeed", opts.randomSeed)
		    .detail("SourceVersion", getSourceVersion())
		    .detail("Version", FDB_VT_VERSION)
		    .detail("PackageName", FDB_VT_PACKAGE_NAME)
		    .detail("FileSystem", opts.fileSystemPath)
		    .detail("DataFolder", opts.dataFolder)
		    .detail("WorkingDirectory", cwd)
		    .detail("ClusterFile", opts.connectionFile ? opts.connectionFile->toString() : "")
		    .detail("ConnectionString",
		            opts.connectionFile ? opts.connectionFile->getConnectionString().toString() : "")
		    .detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(nullptr))
		    .setMaxFieldLength(10000)
		    .detail("EnvironmentKnobOptions", environmentKnobOptions.length() ? environmentKnobOptions : "none")
		    .detail("CommandLine", opts.commandLine)
		    .setMaxFieldLength(0)
		    .detail("BuggifyEnabled", opts.buggifyEnabled)
		    .detail("FaultInjectionEnabled", opts.faultInjectionEnabled)
		    .detail("MemoryLimit", opts.memLimit)
		    .detail("VirtualMemoryLimit", opts.virtualMemLimit)
		    .detail("ProtocolVersion", currentProtocolVersion())
		    .trackLatest("ProgramStart");

		Error::init();
		std::set_new_handler(&platform::outOfMemory);
		Future<Void> memoryUsageMonitor = startMemoryUsageMonitor(opts.memLimit);
		setMemoryQuota(opts.virtualMemLimit);

		Future<Optional<Void>> f;

		if (role == ServerRole::Simulation) {
			TraceEvent("Simulation").detail("TestFile", opts.testFile);

			auto histogramReportActor = histogramReport();

			CLIENT_KNOBS->trace();
			FLOW_KNOBS->trace();
			SERVER_KNOBS->trace();

			auto dataFolder = opts.dataFolder.size() ? opts.dataFolder : "simfdb";
			std::vector<std::string> directories = platform::listDirectories(dataFolder);
			const std::set<std::string> allowedDirectories = { ".",       "..",       "backups", "unittests",
				                                               "fdbblob", "bulkdump", "bulkload" };
			// bulkdump and bulkload folders are used by bulkloading and bulkdumping simulation tests

			for (const auto& dir : directories) {
				if (dir.size() != 32 && !allowedDirectories.contains(dir) && dir.find("snap") == std::string::npos) {

					TraceEvent(SevError, "IncompatibleDirectoryFound")
					    .detail("DataFolder", dataFolder)
					    .detail("SuspiciousFile", dir);

					fprintf(stderr,
					        "ERROR: Data folder `%s' had non fdb file `%s'; please use clean, fdb-only folder\n",
					        dataFolder.c_str(),
					        dir.c_str());

					flushAndExit(FDB_EXIT_ERROR);
				}
			}

			if (!validateSimulationDataFiles(dataFolder, opts.restarting)) {
				flushAndExit(FDB_EXIT_ERROR);
			}

			int isRestoring = 0;
			if (!opts.restarting) {
				platform::eraseDirectoryRecursive(dataFolder);
				platform::createDirectory(dataFolder);
			} else {
				CSimpleIni ini;
				ini.SetUnicode();
				std::string absDataFolder = abspath(dataFolder);
				ini.LoadFile(joinPath(absDataFolder, "restartInfo.ini").c_str());
				int backupFailed = true;
				const char* isRestoringStr = ini.GetValue("RESTORE", "isRestoring", nullptr);
				if (isRestoringStr) {
					isRestoring = atoi(isRestoringStr);
					const char* backupFailedStr = ini.GetValue("RESTORE", "BackupFailed", nullptr);
					if (isRestoring && backupFailedStr) {
						backupFailed = atoi(backupFailedStr);
					}
				}
				if (isRestoring && !backupFailed) {
					std::vector<std::string> returnList;
					std::string ext = "";
					returnList = platform::listDirectories(absDataFolder);
					std::string snapStr = ini.GetValue("RESTORE", "RestoreSnapUID");

					TraceEvent("RestoringDataFolder").detail("DataFolder", absDataFolder);
					TraceEvent("RestoreSnapUID").detail("UID", snapStr);

					// delete all files (except fdb.cluster) in non-snap directories
					for (const auto& dirEntry : returnList) {
						if (dirEntry == "." || dirEntry == "..") {
							continue;
						}
						if (dirEntry.find(snapStr) != std::string::npos) {
							continue;
						}

						std::string childf = absDataFolder + "/" + dirEntry;
						std::vector<std::string> returnFiles = platform::listFiles(childf, ext);
						for (const auto& fileEntry : returnFiles) {
							if (fileEntry != "fdb.cluster" && fileEntry != "fitness") {
								TraceEvent("DeletingNonSnapfiles").detail("FileBeingDeleted", childf + "/" + fileEntry);
								deleteFile(childf + "/" + fileEntry);
							}
						}
					}
					// cleanup unwanted and partial directories
					for (const auto& dirEntry : returnList) {
						if (dirEntry == "." || dirEntry == "..") {
							continue;
						}
						std::string dirSrc = absDataFolder + "/" + dirEntry;
						// delete snap directories which are not part of restoreSnapUID
						if (dirEntry.find(snapStr) == std::string::npos) {
							if (dirEntry.find("snap") != std::string::npos) {
								platform::eraseDirectoryRecursive(dirSrc);
							}
							continue;
						}
						// remove empty/partial snap directories
						std::vector<std::string> childrenList = platform::listFiles(dirSrc);
						if (childrenList.size() == 0) {
							TraceEvent("RemovingEmptySnapDirectory").detail("DirBeingDeleted", dirSrc);
							platform::eraseDirectoryRecursive(dirSrc);
							continue;
						}
					}
					// move snapshotted files to appropriate locations
					for (const auto& dirEntry : returnList) {
						if (dirEntry == "." || dirEntry == "..") {
							continue;
						}
						std::string dirSrc = absDataFolder + "/" + dirEntry;
						std::string origDir = dirEntry.substr(0, 32);
						std::string dirToMove = absDataFolder + "/" + origDir;
						if ((dirEntry.find("snap") != std::string::npos) &&
						    (dirEntry.find("tlog") != std::string::npos)) {
							// restore tlog files
							restoreRoleFilesHelper(dirSrc, dirToMove, "log");
						} else if ((dirEntry.find("snap") != std::string::npos) &&
						           (dirEntry.find("storage") != std::string::npos)) {
							// restore storage files
							restoreRoleFilesHelper(dirSrc, dirToMove, "storage");
						} else if ((dirEntry.find("snap") != std::string::npos) &&
						           (dirEntry.find("coord") != std::string::npos)) {
							// restore coordinator files
							restoreRoleFilesHelper(dirSrc, dirToMove, "coordination");
						}
					}
				}
				g_knobs.setKnob("enable_blob_granule_compression",
				                KnobValue::create(ini.GetBoolValue("META", "enableBlobGranuleEncryption", false)));
				g_knobs.setKnob("encrypt_header_auth_token_enabled",
				                KnobValue::create(ini.GetBoolValue("META", "encryptHeaderAuthTokenEnabled", false)));
				g_knobs.setKnob("encrypt_header_auth_token_algo",
				                KnobValue::create((int)ini.GetLongValue(
				                    "META", "encryptHeaderAuthTokenAlgo", FLOW_KNOBS->ENCRYPT_HEADER_AUTH_TOKEN_ALGO)));

				g_knobs.setKnob(
				    "shard_encode_location_metadata",
				    KnobValue::create(ini.GetBoolValue("META", "enableShardEncodeLocationMetadata", false)));
			}
			simulationSetupAndRun(
			    dataFolder, opts.testFile, opts.restarting, (isRestoring >= 1), opts.whitelistBinPaths);
			g_simulator->run();
		} else if (role == ServerRole::FDBD) {
			// Update the global blob credential files list so that both fast
			// restore workers and backup workers can access blob storage.
			std::vector<std::string>* pFiles =
			    (std::vector<std::string>*)g_network->global(INetwork::enBlobCredentialFiles);
			if (pFiles != nullptr) {
				for (auto& f : opts.blobCredentials) {
					pFiles->push_back(f);
				}
			}

			// Update proxy string
			Optional<std::string>* pProxy = (Optional<std::string>*)g_network->global(INetwork::enProxy);
			*pProxy = opts.proxy;

			// Call fast restore for the class FastRestoreClass. This is a short-cut to run fast restore in circus
			if (opts.processClass == ProcessClass::FastRestoreClass) {
				printf("Run as fast restore worker\n");
				ASSERT(opts.connectionFile);
				auto dataFolder = opts.dataFolder;
				if (!dataFolder.size())
					dataFolder = format("fdb/%d/", opts.publicAddresses.address.port); // SOMEDAY: Better default

				std::vector<Future<Void>> actors(listenErrors.begin(), listenErrors.end());
				actors.push_back(restoreWorker(opts.connectionFile, opts.localities, dataFolder));
				f = stopAfter(waitForAll(actors));
				printf("Fast restore worker started\n");
				g_network->run();
				printf("g_network->run() done\n");
			} else { // Call fdbd roles in conventional way
				ASSERT(opts.connectionFile);

				setupRunLoopProfiler();

				auto dataFolder = opts.dataFolder;
				if (!dataFolder.size())
					dataFolder = format("fdb/%d/", opts.publicAddresses.address.port); // SOMEDAY: Better default

				std::vector<Future<Void>> actors(listenErrors.begin(), listenErrors.end());
				actors.push_back(fdbd(opts.connectionFile,
				                      opts.localities,
				                      opts.processClass,
				                      dataFolder,
				                      dataFolder,
				                      opts.storageMemLimit,
				                      opts.metricsConnFile,
				                      opts.metricsPrefix,
				                      opts.rsssize,
				                      opts.whitelistBinPaths,
				                      opts.configPath,
				                      opts.manualKnobOverrides,
				                      opts.configDBType,
				                      opts.consistencyCheckUrgentMode));
				actors.push_back(histogramReport());
				// actors.push_back( recurring( []{}, .001 ) );  // for ASIO latency measurement
#ifdef FLOW_GRPC_ENABLED
				if (opts.grpcAddressStrs.size() > 0) {
					FlowGrpc::init(&opts.tlsConfig, NetworkAddress::parse(opts.grpcAddressStrs[0]));
					actors.push_back(GrpcServer::instance()->run());
				}
#endif
				f = stopAfter(waitForAll(actors));
				g_network->run();
			}
		} else if (role == ServerRole::MultiTester) {
			setupRunLoopProfiler();
			f = stopAfter(runTests(opts.connectionFile,
			                       TEST_TYPE_FROM_FILE,
			                       opts.testOnServers ? TEST_ON_SERVERS : TEST_ON_TESTERS,
			                       opts.minTesterCount,
			                       opts.testFile,
			                       StringRef(),
			                       opts.localities));
			g_network->run();
		} else if (role == ServerRole::Test) {
			TraceEvent("NonSimulationTest").detail("TestFile", opts.testFile);
			setupRunLoopProfiler();
			auto m =
			    startSystemMonitor(opts.dataFolder, opts.dcId, opts.zoneId, opts.zoneId, opts.localities.dataHallId());
			f = stopAfter(runTests(
			    opts.connectionFile, TEST_TYPE_FROM_FILE, TEST_HERE, 1, opts.testFile, StringRef(), opts.localities));
			g_network->run();
		} else if (role == ServerRole::ConsistencyCheck) {
			setupRunLoopProfiler();

			auto m =
			    startSystemMonitor(opts.dataFolder, opts.dcId, opts.zoneId, opts.zoneId, opts.localities.dataHallId());
			f = stopAfter(runTests(opts.connectionFile,
			                       TEST_TYPE_CONSISTENCY_CHECK,
			                       TEST_HERE,
			                       1,
			                       opts.testFile,
			                       StringRef(),
			                       opts.localities));
			g_network->run();
		} else if (role == ServerRole::ConsistencyCheckUrgent) {
			setupRunLoopProfiler();
			auto m =
			    startSystemMonitor(opts.dataFolder, opts.dcId, opts.zoneId, opts.zoneId, opts.localities.dataHallId());
			f = stopAfter(runTests(opts.connectionFile,
			                       TEST_TYPE_CONSISTENCY_CHECK_URGENT,
			                       TEST_ON_TESTERS,
			                       opts.minTesterCount,
			                       opts.testFile,
			                       StringRef(),
			                       opts.localities));
			g_network->run();
		} else if (role == ServerRole::UnitTests) {
			setupRunLoopProfiler();
			auto m =
			    startSystemMonitor(opts.dataFolder, opts.dcId, opts.zoneId, opts.zoneId, opts.localities.dataHallId());
			f = stopAfter(runTests(opts.connectionFile,
			                       TEST_TYPE_UNIT_TESTS,
			                       TEST_HERE,
			                       1,
			                       opts.testFile,
			                       StringRef(),
			                       opts.localities,
			                       opts.testParams));
			g_network->run();
		} else if (role == ServerRole::CreateTemplateDatabase) {
			createTemplateDatabase();
		} else if (role == ServerRole::NetworkTestClient) {
			f = stopAfter(networkTestClient(opts.testServersStr));
			g_network->run();
		} else if (role == ServerRole::NetworkTestServer) {
			f = stopAfter(networkTestServer());
			g_network->run();
		} else if (role == ServerRole::Restore) {
			f = stopAfter(restoreWorker(opts.connectionFile, opts.localities, opts.dataFolder));
			g_network->run();
		} else if (role == ServerRole::KVFileIntegrityCheck) {
			f = stopAfter(KVFileCheck(opts.kvFile, true));
			g_network->run();
		} else if (role == ServerRole::KVFileGenerateIOLogChecksums) {
			Optional<Void> result;
			try {
				GenerateIOLogChecksumFile(opts.kvFile);
				result = Void();
			} catch (Error& e) {
				fprintf(stderr, "Fatal Error: %s\n", e.what());
			}

			f = result;
		} else if (role == ServerRole::FlowProcess) {
			std::string traceFormat = getTraceFormatExtension();
			// close and reopen trace file with the correct process listen address to name the file
			closeTraceFile();
			// writer is not shutdown immediately, addref on it
			disposeTraceFileWriter();
			// use the same trace format as before
			selectTraceFormatter(traceFormat);
			// create the trace file with the correct process address
			openTraceFile(
			    g_network->getLocalAddress(), opts.rollsize, opts.maxLogsSize, opts.logFolder, "trace", opts.logGroup);
			auto m =
			    startSystemMonitor(opts.dataFolder, opts.dcId, opts.zoneId, opts.zoneId, opts.localities.dataHallId());
			TraceEvent(SevDebug, "StartingFlowProcess").detail("FlowProcessName", opts.flowProcessName);
#if defined(__linux__)
			prctl(PR_SET_PDEATHSIG, SIGTERM);
			if (getppid() == 1) /* parent already died before prctl */
				flushAndExit(FDB_EXIT_SUCCESS);
#elif defined(__FreeBSD__)
			const int sig = SIGTERM;
			procctl(P_PID, 0, PROC_PDEATHSIG_CTL, (void*)&sig);
			if (getppid() == 1) /* parent already died before procctl */
				flushAndExit(FDB_EXIT_SUCCESS);
#endif

			if (opts.flowProcessName == "KeyValueStoreProcess") {
				ProcessFactory<KeyValueStoreProcess>(opts.flowProcessName.c_str());
			}
			f = stopAfter(runFlowProcess(opts.flowProcessName, opts.flowProcessEndpoint));
			g_network->run();
		} else if (role == ServerRole::KVFileDump) {
			f = stopAfter(KVFileDump(opts.kvFile));
			g_network->run();
		} else if (role == ServerRole::ChangeClusterKey) {
			Key newClusterKey(opts.newClusterKey);
			Key oldClusterKey = opts.connectionFile->getConnectionString().clusterKey();
			f = stopAfter(coordChangeClusterKey(opts.dataFolder, newClusterKey, oldClusterKey));
			g_network->run();
		}

		int rc = FDB_EXIT_SUCCESS;
		if (f.isValid() && f.isReady() && !f.isError() && !f.get().present()) {
			rc = FDB_EXIT_ERROR;
		}

		int unseed = noUnseed ? -1 : deterministicRandom()->randomInt(0, 100001);
		TraceEvent("ElapsedTime")
		    .detail("SimTime", now() - startNow)
		    .detail("RealTime", timer() - start)
		    .detail("RandomUnseed", unseed);

		if (role == ServerRole::Simulation) {
			printf("Unseed: %d\n", unseed);
			printf("Elapsed: %f simsec, %f real seconds\n", now() - startNow, timer() - start);
		}

		// IFailureMonitor::failureMonitor().address_info.clear();

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

		/*cout << Actor::allActors.size() << " surviving actors:" << std::endl;
		std::map<std::string,int> actorCount;
		for(int i=0; i<Actor::allActors.size(); i++)
		    ++actorCount[Actor::allActors[i]->getName()];
		for(auto i = actorCount.rbegin(); !(i == actorCount.rend()); ++i)
		    std::cout << "  " << i->second << " " << i->first << std::endl;*/
		//	std::cout << "  " << Actor::allActors[i]->getName() << std::endl;

		if (role == ServerRole::Simulation) {
			unsigned long sevErrorEventsLogged = TraceEvent::CountEventsLoggedAt(SevError);
			if (sevErrorEventsLogged > 0) {
				printf("%lu SevError events logged\n", sevErrorEventsLogged);
				rc = FDB_EXIT_ERROR;
			}
		}

		// g_simulator->run();

#ifdef ALLOC_INSTRUMENTATION
		{
			std::cout << "Page Counts: " << FastAllocator<16>::pageCount << " " << FastAllocator<32>::pageCount << " "
			          << FastAllocator<64>::pageCount << " " << FastAllocator<128>::pageCount << " "
			          << FastAllocator<256>::pageCount << " " << FastAllocator<512>::pageCount << " "
			          << FastAllocator<1024>::pageCount << " " << FastAllocator<2048>::pageCount << " "
			          << FastAllocator<4096>::pageCount << " " << FastAllocator<8192>::pageCount << " "
			          << FastAllocator<16384>::pageCount << std::endl;

			std::vector<std::pair<std::string, const char*>> typeNames;
			for (auto i = allocInstr.begin(); i != allocInstr.end(); ++i) {
				std::string s;

#ifdef __linux__
				char* demangled = abi::__cxa_demangle(i->first, nullptr, nullptr, nullptr);
				if (demangled) {
					s = demangled;
					if (StringRef(s).startsWith("(anonymous namespace)::"_sr))
						s = s.substr("(anonymous namespace)::"_sr.size());
					free(demangled);
				} else
					s = i->first;
#else
				s = i->first;
				if (StringRef(s).startsWith("class `anonymous namespace'::"_sr))
					s = s.substr("class `anonymous namespace'::"_sr.size());
				else if (StringRef(s).startsWith("class "_sr))
					s = s.substr("class "_sr.size());
				else if (StringRef(s).startsWith("struct "_sr))
					s = s.substr("struct "_sr.size());
#endif

				typeNames.emplace_back(s, i->first);
			}
			std::sort(typeNames.begin(), typeNames.end());
			for (int i = 0; i < typeNames.size(); i++) {
				const char* n = typeNames[i].second;
				auto& f = allocInstr[n];
				printf("%+d\t%+d\t%d\t%d\t%s\n",
				       f.allocCount,
				       -f.deallocCount,
				       f.allocCount - f.deallocCount,
				       f.maxAllocated,
				       typeNames[i].first.c_str());
			}

			// We're about to exit and clean up data structures, this will wreak havoc on allocation recording
			memSample_entered = true;
		}
#endif
		// printf("\n%d tests passed; %d tests failed\n", passCount, failCount);
		flushAndExit(rc);
	} catch (Error& e) {
		fprintf(stderr, "Error: %s\n", e.what());
		TraceEvent(SevError, "MainError").error(e);
		// printf("\n%d tests passed; %d tests failed\n", passCount, failCount);
		flushAndExit(FDB_EXIT_MAIN_ERROR);
	} catch (boost::system::system_error& e) {
		ASSERT_WE_THINK(false); // boost errors shouldn't leak
		fprintf(stderr, "boost::system::system_error: %s (%d)", e.what(), e.code().value());
		TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		// printf("\n%d tests passed; %d tests failed\n", passCount, failCount);
		flushAndExit(FDB_EXIT_MAIN_EXCEPTION);
	} catch (std::exception& e) {
		fprintf(stderr, "std::exception: %s\n", e.what());
		TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		// printf("\n%d tests passed; %d tests failed\n", passCount, failCount);
		flushAndExit(FDB_EXIT_MAIN_EXCEPTION);
	}

	static_assert(LBLocalityData<StorageServerInterface>::Present, "Storage server interface should be load balanced");
	static_assert(LBLocalityData<CommitProxyInterface>::Present, "Commit proxy interface should be load balanced");
	static_assert(LBLocalityData<GrvProxyInterface>::Present, "GRV proxy interface should be load balanced");
	static_assert(LBLocalityData<TLogInterface>::Present, "TLog interface should be load balanced");
	static_assert(!LBLocalityData<MasterInterface>::Present, "Master interface should not be load balanced");
}
