/*
 * worker.actor.cpp
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

#include "flow/ActorCollection.h"
#include "flow/SystemMonitor.h"
#include "flow/TDMetric.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/MetricLogger.h"
#include "fdbserver/WorkerInterface.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/TesterInterface.h"  // for poisson()
#include "fdbserver/IDiskQueue.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbserver/ClusterRecruitmentInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbclient/FailureMonitorClient.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbclient/ClientWorkerInterface.h"
#include "flow/Profiler.h"

#ifdef __linux__
#ifdef USE_GPERFTOOLS
#include "gperftools/profiler.h"
#endif
#include <unistd.h>
#include <thread>
#include <execinfo.h>
#endif
#include "flow/actorcompiler.h"  // This must be the last #include.

#if CENABLED(0, NOT_IN_CLEAN)
extern IKeyValueStore* keyValueStoreCompressTestData(IKeyValueStore* store);
# define KV_STORE(filename,uid) keyValueStoreCompressTestData(keyValueStoreSQLite(filename,uid))
#elif CENABLED(0, NOT_IN_CLEAN)
# define KV_STORE(filename,uid) keyValueStoreSQLite(filename,uid)
#else
# define KV_STORE(filename,uid) keyValueStoreMemory(filename,uid)
#endif

ACTOR static Future<Void> extractClientInfo( Reference<AsyncVar<ServerDBInfo>> db, Reference<AsyncVar<ClientDBInfo>> info ) {
	loop {
		info->set( db->get().client );
		wait( db->onChange() );
	}
}

Database openDBOnServer( Reference<AsyncVar<ServerDBInfo>> const& db, int taskID, bool enableLocalityLoadBalance, bool lockAware ) {
	Reference<AsyncVar<ClientDBInfo>> info( new AsyncVar<ClientDBInfo> );
	return DatabaseContext::create( info, extractClientInfo(db, info), enableLocalityLoadBalance ? db->get().myLocality : LocalityData(), enableLocalityLoadBalance, taskID, lockAware );
}

struct ErrorInfo {
	Error error;
	const Role &role;
	UID id;
	ErrorInfo( Error e, const Role &role, UID id ) : error(e), role(role), id(id) {}
	template <class Ar> void serialize(Ar&) { ASSERT(false); }
};

Error checkIOTimeout(Error const &e) {
	// Convert all_errors to io_timeout if global timeout bool was set
	bool timeoutOccurred = (bool)g_network->global(INetwork::enASIOTimedOut);
	// In simulation, have to check global timed out flag for both this process and the machine process on which IO is done
	if(g_network->isSimulated() && !timeoutOccurred)
		timeoutOccurred = g_pSimulator->getCurrentProcess()->machine->machineProcess->global(INetwork::enASIOTimedOut);

	if(timeoutOccurred) {
		TEST(true); // Timeout occurred
		Error timeout = io_timeout();
		// Preserve injectedness of error
		if(e.isInjectedFault())
			timeout = timeout.asInjectedFault();
		return timeout;
	}
	return e;
}

ACTOR Future<Void> forwardError( PromiseStream<ErrorInfo> errors,
	Role role, UID id,
	Future<Void> process )
{
	try {
		wait(process);
		errors.send( ErrorInfo(success(), role, id) );
		return Void();
	} catch (Error& e) {
		errors.send( ErrorInfo(e, role, id) );
		return Void();
	}
}

ACTOR Future<Void> handleIOErrors( Future<Void> actor, IClosable* store, UID id, Future<Void> onClosed = Void() ) {
	state Future<ErrorOr<Void>> storeError = actor.isReady() ? Never() : errorOr( store->getError() );
	choose {
		when (state ErrorOr<Void> e = wait( errorOr(actor) )) {
			if (e.isError() && e.getError().code() == error_code_please_reboot) {
				// no need to wait.
			} else {
				wait(onClosed);
			}
			if(storeError.isReady()) throw storeError.get().getError();
			if (e.isError()) throw e.getError(); else return e.get();
		}
		when (ErrorOr<Void> e = wait( storeError )) {
			TraceEvent("WorkerTerminatingByIOError", id).error(e.getError(), true);
			actor.cancel();
			// file_not_found can occur due to attempting to open a partially deleted DiskQueue, which should not be reported SevError.
			if (e.getError().code() == error_code_file_not_found) {
				TEST(true); // Worker terminated with file_not_found error
				return Void();
			}
			throw e.getError();
		}
	}
}

ACTOR Future<Void> workerHandleErrors(FutureStream<ErrorInfo> errors) {
	loop choose {
		when( ErrorInfo _err = waitNext(errors) ) {
			ErrorInfo err = _err;
			bool ok =
				err.error.code() == error_code_success ||
				err.error.code() == error_code_please_reboot ||
				err.error.code() == error_code_actor_cancelled ||
				err.error.code() == error_code_coordinators_changed ||  // The worker server was cancelled
				err.error.code() == error_code_shutdown_in_progress;

			if (!ok) {
				err.error = checkIOTimeout(err.error);  // Possibly convert error to io_timeout
			}

			endRole(err.role, err.id, "Error", ok, err.error);

			if (err.error.code() == error_code_please_reboot || err.error.code() == error_code_io_timeout) throw err.error;
		}
	}
}

// Improve simulation code coverage by sometimes deferring the destruction of workerInterface (and therefore "endpoint not found" responses to clients
//		for an extra second, so that clients are more likely to see broken_promise errors
ACTOR template<class T> Future<Void> zombie(T workerInterface, Future<Void> worker) {
	try {
		wait(worker);
		if (BUGGIFY)
			wait(delay(1.0));
		return Void();
	} catch (Error& e) {
		throw;
	}
}

ACTOR Future<Void> loadedPonger( FutureStream<LoadedPingRequest> pings ) {
	state Standalone<StringRef> payloadBack = std::string( 20480, '.' );

	loop {
		LoadedPingRequest pong = waitNext( pings );
		LoadedReply rep;
		rep.payload = (pong.loadReply ? payloadBack : LiteralStringRef(""));
		rep.id = pong.id;
		pong.reply.send( rep );
	}
}

StringRef fileStoragePrefix = LiteralStringRef("storage-");
StringRef fileLogDataPrefix = LiteralStringRef("log-");
StringRef fileLogQueuePrefix = LiteralStringRef("logqueue-");
StringRef tlogQueueExtension = LiteralStringRef("fdq");

std::pair<KeyValueStoreType, std::string> bTreeV1Suffix  = std::make_pair( KeyValueStoreType::SSD_BTREE_V1, ".fdb" );
std::pair<KeyValueStoreType, std::string> bTreeV2Suffix = std::make_pair(KeyValueStoreType::SSD_BTREE_V2,   ".sqlite");
std::pair<KeyValueStoreType, std::string> memorySuffix = std::make_pair( KeyValueStoreType::MEMORY,         "-0.fdq" );
std::pair<KeyValueStoreType, std::string> redwoodSuffix = std::make_pair( KeyValueStoreType::SSD_REDWOOD_V1,   ".redwood" );

std::string validationFilename = "_validate";

std::string filenameFromSample( KeyValueStoreType storeType, std::string folder, std::string sample_filename ) {
	if( storeType == KeyValueStoreType::SSD_BTREE_V1 )
		return joinPath( folder, sample_filename );
	else if ( storeType == KeyValueStoreType::SSD_BTREE_V2 )
		return joinPath(folder, sample_filename);
	else if( storeType == KeyValueStoreType::MEMORY )
		return joinPath( folder, sample_filename.substr(0, sample_filename.size() - 5) );
	
	else if ( storeType == KeyValueStoreType::SSD_REDWOOD_V1 )
		return joinPath(folder, sample_filename);
	UNREACHABLE();
}

std::string filenameFromId( KeyValueStoreType storeType, std::string folder, std::string prefix, UID id ) {
	if( storeType == KeyValueStoreType::SSD_BTREE_V1)
		return joinPath( folder, prefix + id.toString() + ".fdb" );
	else if (storeType == KeyValueStoreType::SSD_BTREE_V2)
		return joinPath(folder, prefix + id.toString() + ".sqlite");
	else if( storeType == KeyValueStoreType::MEMORY )
		return joinPath( folder, prefix + id.toString() + "-" );
	else if (storeType == KeyValueStoreType::SSD_REDWOOD_V1)
		return joinPath(folder, prefix + id.toString() + ".redwood");

	UNREACHABLE();
}

struct DiskStore {
	enum COMPONENT { TLogData, Storage };

	UID storeID;
	std::string filename; // For KVStoreMemory just the base filename to be passed to IDiskQueue
	COMPONENT storedComponent;
	KeyValueStoreType storeType;
};

std::vector< DiskStore > getDiskStores( std::string folder, std::string suffix, KeyValueStoreType type) {
	std::vector< DiskStore > result;
	vector<std::string> files = platform::listFiles( folder, suffix );

	for( int idx = 0; idx < files.size(); idx++ ) {
		DiskStore store;
		store.storeType = type;

		StringRef prefix;
		if( StringRef( files[idx] ).startsWith( fileStoragePrefix ) ) {
			store.storedComponent = DiskStore::Storage;
			prefix = fileStoragePrefix;
		}
		else if( StringRef( files[idx] ).startsWith( fileLogDataPrefix ) ) {
			store.storedComponent = DiskStore::TLogData;
			prefix = fileLogDataPrefix;
		}
		else
			continue;

		store.storeID = UID::fromString( files[idx].substr( prefix.size(), 32 ) );
		store.filename = filenameFromSample( type, folder, files[idx] );
		result.push_back( store );
	}
	return result;
}

std::vector< DiskStore > getDiskStores( std::string folder ) {
	auto result = getDiskStores( folder, bTreeV1Suffix.second, bTreeV1Suffix.first);
	auto result1 = getDiskStores( folder, bTreeV2Suffix.second, bTreeV2Suffix.first);
	result.insert( result.end(), result1.begin(), result1.end() );
	auto result2 = getDiskStores( folder, memorySuffix.second, memorySuffix.first );
	result.insert( result.end(), result2.begin(), result2.end() );
	auto result3 = getDiskStores( folder, redwoodSuffix.second, redwoodSuffix.first);
	result.insert( result.end(), result3.begin(), result3.end() );
	return result;
}

ACTOR Future<Void> registrationClient( Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> ccInterface, WorkerInterface interf, Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo, ProcessClass initialClass) {
	// Keeps the cluster controller (as it may be re-elected) informed that this worker exists
	// The cluster controller uses waitFailureClient to find out if we die, and returns from registrationReply (requiring us to re-register)
	state Generation requestGeneration = 0;
	state ProcessClass processClass = initialClass;
	loop {
		Future<RegisterWorkerReply> registrationReply = ccInterface->get().present() ? brokenPromiseToNever( ccInterface->get().get().registerWorker.getReply( RegisterWorkerRequest(interf, initialClass, processClass, asyncPriorityInfo->get(), requestGeneration++) ) ) : Never();
		choose {
			when ( RegisterWorkerReply reply = wait( registrationReply )) {
				processClass = reply.processClass;	
				asyncPriorityInfo->set( reply.priorityInfo );
			}
			when ( wait( ccInterface->onChange() )) { }
		}
	}
}

#if defined(__linux__) && defined(USE_GPERFTOOLS)
//A set of threads that should be profiled
std::set<std::thread::id> profiledThreads;

//Returns whether or not a given thread should be profiled
int filter_in_thread(void *arg) {
	return profiledThreads.count(std::this_thread::get_id()) > 0 ? 1 : 0;
}
#endif

//Enables the calling thread to be profiled
void registerThreadForProfiling() {
#if defined(__linux__) && defined(USE_GPERFTOOLS)
	//Not sure if this is actually needed, but a call to backtrace was advised here:
	//http://groups.google.com/group/google-perftools/browse_thread/thread/0dfd74532e038eb8/2686d9f24ac4365f?pli=1
	profiledThreads.insert(std::this_thread::get_id());
	const int num_levels = 100;
	void *pc[num_levels];
	backtrace(pc, num_levels);
#endif
}

//Starts or stops the CPU profiler
void updateCpuProfiler(ProfilerRequest req) {
	switch (req.type) {
	case ProfilerRequest::Type::GPROF:
#if defined(__linux__) && defined(USE_GPERFTOOLS) && !defined(VALGRIND)
		switch (req.action) {
		case ProfilerRequest::Action::ENABLE: {
			const char *path = (const char*)req.outputFile.begin();
			ProfilerOptions *options = new ProfilerOptions();
			options->filter_in_thread = &filter_in_thread;
			options->filter_in_thread_arg = NULL;
			ProfilerStartWithOptions(path, options);
			free(workingDir);
			break;
		}
		case ProfilerRequest::Action::DISABLE:
			ProfilerStop();
			break;
		case ProfilerRequest::Action::RUN:
			ASSERT(false);  // User should have called runProfiler.
			break;
		}
#endif
		break;
	case ProfilerRequest::Type::FLOW:
		switch (req.action) {
		case ProfilerRequest::Action::ENABLE:
			startProfiling(g_network, {}, req.outputFile);
			break;
		case ProfilerRequest::Action::DISABLE:
			stopProfiling();
			break;
		case ProfilerRequest::Action::RUN:
			ASSERT(false);  // User should have called runProfiler.
			break;
		}
		break;
	}
}

ACTOR Future<Void> runProfiler(ProfilerRequest req) {
	if (req.action == ProfilerRequest::Action::RUN) {
		req.action = ProfilerRequest::Action::ENABLE;
		updateCpuProfiler(req);
		wait(delay(req.duration));
		req.action = ProfilerRequest::Action::DISABLE;
		updateCpuProfiler(req);
		return Void();
	} else {
		updateCpuProfiler(req);
		return Void();
	}
}

ACTOR Future<Void> storageServerRollbackRebooter( Future<Void> prevStorageServer, KeyValueStoreType storeType, std::string filename, UID id, LocalityData locality, Reference<AsyncVar<ServerDBInfo>> db, std::string folder, ActorCollection* filesClosed, int64_t memoryLimit, IKeyValueStore* store ) {
	loop {
		ErrorOr<Void> e = wait( errorOr( prevStorageServer) );
		if (!e.isError()) return Void();
		else if (e.getError().code() != error_code_please_reboot) throw e.getError();

		TraceEvent("StorageServerRequestedReboot", id);

		StorageServerInterface recruited;
		recruited.uniqueID = id;
		recruited.locality = locality;
		recruited.initEndpoints();

		DUMPTOKEN(recruited.getVersion);
		DUMPTOKEN(recruited.getValue);
		DUMPTOKEN(recruited.getKey);
		DUMPTOKEN(recruited.getKeyValues);
		DUMPTOKEN(recruited.getShardState);
		DUMPTOKEN(recruited.waitMetrics);
		DUMPTOKEN(recruited.splitMetrics);
		DUMPTOKEN(recruited.getPhysicalMetrics);
		DUMPTOKEN(recruited.waitFailure);
		DUMPTOKEN(recruited.getQueuingMetrics);
		DUMPTOKEN(recruited.getKeyValueStoreType);
		DUMPTOKEN(recruited.watchValue);

		prevStorageServer = storageServer( store, recruited, db, folder, Promise<Void>() );
		prevStorageServer = handleIOErrors(prevStorageServer, store, id, store->onClosed());
	}
}

// FIXME:  This will not work correctly in simulation as all workers would share the same roles map
std::set<std::pair<std::string, std::string>> g_roles;

Standalone<StringRef> roleString(std::set<std::pair<std::string, std::string>> roles, bool with_ids) {
	std:: string result;
	for(auto &r : roles) {
		if(!result.empty())
			result.append(",");
		result.append(r.first);
		if(with_ids) {
			result.append(":");
			result.append(r.second);
		}
	}
	return StringRef(result);
}

void startRole(const Role &role, UID roleId, UID workerId, std::map<std::string, std::string> details, std::string origination) {
	if(role.includeInTraceRoles) {
		addTraceRole(role.abbreviation);
	}

	TraceEvent ev("Role", roleId);
	ev.detail("As", role.roleName)
		.detail("Transition", "Begin")
		.detail("Origination", origination)
		.detail("OnWorker", workerId);
	for(auto it = details.begin(); it != details.end(); it++)
		ev.detail(it->first.c_str(), it->second);

	ev.trackLatest( (roleId.shortString() + ".Role" ).c_str() );

	// Update roles map, log Roles metrics
	g_roles.insert({role.roleName, roleId.shortString()});
	StringMetricHandle(LiteralStringRef("Roles")) = roleString(g_roles, false);
	StringMetricHandle(LiteralStringRef("RolesWithIDs")) = roleString(g_roles, true);
	if (g_network->isSimulated()) g_simulator.addRole(g_network->getLocalAddress(), role.roleName);
}

void endRole(const Role &role, UID id, std::string reason, bool ok, Error e) {
	{
		TraceEvent ev("Role", id);
		if(e.code() != invalid_error_code)
			ev.error(e, true);
		ev.detail("Transition", "End")
			.detail("As", role.roleName)
			.detail("Reason", reason);

		ev.trackLatest( (id.shortString() + ".Role").c_str() );
	}

	if(!ok) {
		std::string type = role.roleName + "Failed";

		TraceEvent err(SevError, type.c_str(), id);
		if(e.code() != invalid_error_code) {
			err.error(e, true);
		}
		err.detail("Reason", reason);
	}

	latestEventCache.clear( id.shortString() );

	// Update roles map, log Roles metrics
	g_roles.erase({role.roleName, id.shortString()});
	StringMetricHandle(LiteralStringRef("Roles")) = roleString(g_roles, false);
	StringMetricHandle(LiteralStringRef("RolesWithIDs")) = roleString(g_roles, true);
	if (g_network->isSimulated()) g_simulator.removeRole(g_network->getLocalAddress(), role.roleName);

	if(role.includeInTraceRoles) {
		removeTraceRole(role.abbreviation);
	}
}

ACTOR Future<Void> monitorServerDBInfo( Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> ccInterface, Reference<ClusterConnectionFile> connFile, LocalityData locality, Reference<AsyncVar<ServerDBInfo>> dbInfo ) {
	// Initially most of the serverDBInfo is not known, but we know our locality right away
	ServerDBInfo localInfo;
	localInfo.myLocality = locality;
	dbInfo->set(localInfo);

	state Optional<std::string> incorrectConnectionString;
	loop {
		GetServerDBInfoRequest req;
		req.knownServerInfoID = dbInfo->get().id;

		ClusterConnectionString fileConnectionString;
		if (connFile && !connFile->fileContentsUpToDate(fileConnectionString)) {
			req.issues = LiteralStringRef("incorrect_cluster_file_contents");
			std::string connectionString = connFile->getConnectionString().toString();
			if(connFile->canGetFilename()) {
				// Don't log a SevWarnAlways the first time to account for transient issues (e.g. someone else changing the file right before us)
				TraceEvent(incorrectConnectionString.present() && incorrectConnectionString.get() == connectionString ? SevWarnAlways : SevWarn, "IncorrectClusterFileContents")
					.detail("Filename", connFile->getFilename())
					.detail("ConnectionStringFromFile", fileConnectionString.toString())
					.detail("CurrentConnectionString", connectionString);
			}
			incorrectConnectionString = connectionString;
		}
		else {
			incorrectConnectionString = Optional<std::string>();
		}

		auto peers = FlowTransport::transport().getIncompatiblePeers();
		for(auto it = peers->begin(); it != peers->end();) {
			if( now() - it->second.second > SERVER_KNOBS->INCOMPATIBLE_PEER_DELAY_BEFORE_LOGGING ) {
				req.incompatiblePeers.push_back(it->first);
				it = peers->erase(it);
			} else {
				it++;
			}
		}

		choose {
			when( ServerDBInfo ni = wait( ccInterface->get().present() ? brokenPromiseToNever( ccInterface->get().get().getServerDBInfo.getReply( req ) ) : Never() ) ) {
				TraceEvent("GotServerDBInfoChange").detail("ChangeID", ni.id).detail("MasterID", ni.master.id());
				ServerDBInfo localInfo = ni;
				localInfo.myLocality = locality;
				dbInfo->set(localInfo);
			}
			when( wait( ccInterface->onChange() ) ) {
				if(ccInterface->get().present())
					TraceEvent("GotCCInterfaceChange").detail("CCID", ccInterface->get().get().id()).detail("CCMachine", ccInterface->get().get().getWorkers.getEndpoint().address);
			}
		}
	}
}

ACTOR Future<Void> workerServer( Reference<ClusterConnectionFile> connFile, Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> ccInterface, LocalityData locality,
	Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo, ProcessClass initialClass, std::string folder, int64_t memoryLimit, std::string metricsConnFile, std::string metricsPrefix, Promise<Void> recoveredDiskFiles) {
	state PromiseStream< ErrorInfo > errors;
	state Future<Void> handleErrors = workerHandleErrors( errors.getFuture() );  // Needs to be stopped last
	state ActorCollection errorForwarders(false);
	state Future<Void> loggingTrigger = Void();
	state double loggingDelay = SERVER_KNOBS->WORKER_LOGGING_INTERVAL;
	state ActorCollection filesClosed(true);
	state Promise<Void> stopping;
	state WorkerCache<InitializeStorageReply> storageCache;
	state Reference<AsyncVar<ServerDBInfo>> dbInfo( new AsyncVar<ServerDBInfo>(ServerDBInfo()) );
	state Future<Void> metricsLogger;
	state std::map<KeyValueStoreType::StoreType, std::pair<Future<Void>, PromiseStream<InitializeTLogRequest>>> sharedLogs;

	state WorkerInterface interf( locality );

	if(metricsPrefix.size() > 0) {
		if( metricsConnFile.size() > 0) {
			try {
				state Database db = Database::createDatabase(metricsConnFile, Database::API_VERSION_LATEST, locality);
				metricsLogger = runMetrics( db, KeyRef(metricsPrefix) );
			} catch(Error &e) {
				TraceEvent(SevWarnAlways, "TDMetricsBadClusterFile").error(e).detail("ConnFile", metricsConnFile);
			}
		} else {
			bool lockAware = metricsPrefix.size() && metricsPrefix[0] == '\xff';
			metricsLogger = runMetrics( openDBOnServer( dbInfo, TaskDefaultEndpoint, true, lockAware ), KeyRef(metricsPrefix) );
		}
	}

	errorForwarders.add( loadedPonger( interf.debugPing.getFuture() ) );
	errorForwarders.add( waitFailureServer( interf.waitFailure.getFuture() ) );
	errorForwarders.add( monitorServerDBInfo( ccInterface, connFile, locality, dbInfo ) );
	errorForwarders.add( testerServerCore( interf.testerInterface, connFile, dbInfo, locality ) );

	filesClosed.add(stopping.getFuture());

	initializeSystemMonitorMachineState(SystemMonitorMachineState(folder, locality.zoneId(), locality.machineId(), g_network->getLocalAddress().ip));

	{
		auto recruited = interf;  //ghetto! don't we all love a good #define
		DUMPTOKEN(recruited.clientInterface.reboot);
		DUMPTOKEN(recruited.clientInterface.profiler);
		DUMPTOKEN(recruited.tLog);
		DUMPTOKEN(recruited.master);
		DUMPTOKEN(recruited.masterProxy);
		DUMPTOKEN(recruited.resolver);
		DUMPTOKEN(recruited.storage);
		DUMPTOKEN(recruited.debugPing);
		DUMPTOKEN(recruited.coordinationPing);
		DUMPTOKEN(recruited.waitFailure);
		DUMPTOKEN(recruited.setMetricsRate);
		DUMPTOKEN(recruited.eventLogRequest);
		DUMPTOKEN(recruited.traceBatchDumpRequest);
	}

	try {
		std::vector<DiskStore> stores = getDiskStores( folder );
		bool validateDataFiles = deleteFile(joinPath(folder, validationFilename));
		std::vector<Future<Void>> recoveries;
		for( int f = 0; f < stores.size(); f++ ) {
			DiskStore s = stores[f];
			// FIXME: Error handling
			if( s.storedComponent == DiskStore::Storage ) {
				IKeyValueStore* kv = openKVStore(s.storeType, s.filename, s.storeID, memoryLimit, false, validateDataFiles);
				Future<Void> kvClosed = kv->onClosed();
				filesClosed.add( kvClosed );

				StorageServerInterface recruited;
				recruited.uniqueID = s.storeID;
				recruited.locality = locality;
				recruited.initEndpoints();

				std::map<std::string, std::string> details;
				details["StorageEngine"] = s.storeType.toString();
				startRole( Role::STORAGE_SERVER, recruited.id(), interf.id(), details, "Restored" );

				DUMPTOKEN(recruited.getVersion);
				DUMPTOKEN(recruited.getValue);
				DUMPTOKEN(recruited.getKey);
				DUMPTOKEN(recruited.getKeyValues);
				DUMPTOKEN(recruited.getShardState);
				DUMPTOKEN(recruited.waitMetrics);
				DUMPTOKEN(recruited.splitMetrics);
				DUMPTOKEN(recruited.getPhysicalMetrics);
				DUMPTOKEN(recruited.waitFailure);
				DUMPTOKEN(recruited.getQueuingMetrics);
				DUMPTOKEN(recruited.getKeyValueStoreType);
				DUMPTOKEN(recruited.watchValue);

				Promise<Void> recovery;
				Future<Void> f = storageServer( kv, recruited, dbInfo, folder, recovery );
				recoveries.push_back(recovery.getFuture());
				f = handleIOErrors( f, kv, s.storeID, kvClosed );
				f = storageServerRollbackRebooter( f, s.storeType, s.filename, recruited.id(), recruited.locality, dbInfo, folder, &filesClosed, memoryLimit, kv);
				errorForwarders.add( forwardError( errors, Role::STORAGE_SERVER, recruited.id(), f ) );
			} else if( s.storedComponent == DiskStore::TLogData ) {
				IKeyValueStore* kv = openKVStore( s.storeType, s.filename, s.storeID, memoryLimit, validateDataFiles );
				IDiskQueue* queue = openDiskQueue(
					joinPath( folder, fileLogQueuePrefix.toString() + s.storeID.toString() + "-"), tlogQueueExtension.toString(), s.storeID,  10*SERVER_KNOBS->TARGET_BYTES_PER_TLOG);
				filesClosed.add( kv->onClosed() );
				filesClosed.add( queue->onClosed() );

				std::map<std::string, std::string> details;
				details["StorageEngine"] = s.storeType.toString();
				startRole( Role::SHARED_TRANSACTION_LOG, s.storeID, interf.id(), details, "Restored" );

				Promise<Void> oldLog;
				Promise<Void> recovery;
				auto& logData = sharedLogs[s.storeType];
				Future<Void> tl = tLog( kv, queue, dbInfo, locality, !logData.first.isValid() || logData.first.isReady() ? logData.second : PromiseStream<InitializeTLogRequest>(), s.storeID, true, oldLog, recovery );
				recoveries.push_back(recovery.getFuture());

				tl = handleIOErrors( tl, kv, s.storeID );
				tl = handleIOErrors( tl, queue, s.storeID );
				if(!logData.first.isValid() || logData.first.isReady()) {
					logData.first = oldLog.getFuture() || tl;
				}
				errorForwarders.add( forwardError( errors, Role::SHARED_TRANSACTION_LOG, s.storeID, tl ) );
			}
		}

		std::map<std::string, std::string> details;
		details["Locality"] = locality.toString();
		details["DataFolder"] = folder;
		details["StoresPresent"] = format("%d", stores.size());
		startRole( Role::WORKER, interf.id(), interf.id(), details );

		wait(waitForAll(recoveries));
		recoveredDiskFiles.send(Void());

		errorForwarders.add( registrationClient( ccInterface, interf, asyncPriorityInfo, initialClass ) );

		TraceEvent("RecoveriesComplete", interf.id());

		loop choose {

			when( RebootRequest req = waitNext( interf.clientInterface.reboot.getFuture() ) ) {
				state RebootRequest rebootReq = req;
				if(rebootReq.checkData) {
					Reference<IAsyncFile> checkFile = wait( IAsyncFileSystem::filesystem()->open( joinPath(folder, validationFilename), IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE, 0600 ) );
					wait( checkFile->sync() );
				}

				if(g_network->isSimulated()) {
					TraceEvent("SimulatedReboot").detail("Deletion", rebootReq.deleteData );
					if( rebootReq.deleteData ) {
						throw please_reboot_delete();
					}
					throw please_reboot();
				}
				else {
					TraceEvent("ProcessReboot");
					ASSERT(!rebootReq.deleteData);
					flushAndExit(0);
				}
			}
			when( ProfilerRequest req = waitNext(interf.clientInterface.profiler.getFuture()) ) {
				state ProfilerRequest profilerReq = req;
				// There really isn't a great "filepath sanitizer" or "filepath escape" function available,
				// thus we instead enforce a different requirement.  One can only write to a file that's
				// beneath the working directory, and we remove the ability to do any symlink or ../..
				// tricks by resolving all paths through `abspath` first.
				try {
					std::string realLogDir = abspath(SERVER_KNOBS->LOG_DIRECTORY);
					std::string realOutPath = abspath(realLogDir + "/" + profilerReq.outputFile.toString());
					if (realLogDir.size() < realOutPath.size() &&
					    strncmp(realLogDir.c_str(), realOutPath.c_str(), realLogDir.size()) == 0) {
						profilerReq.outputFile = realOutPath;
						uncancellable(runProfiler(profilerReq));
						profilerReq.reply.send(Void());
					} else {
						profilerReq.reply.sendError(client_invalid_operation());
					}
				} catch (Error& e) {
					profilerReq.reply.sendError(e);
				}
			}
			when( RecruitMasterRequest req = waitNext(interf.master.getFuture()) ) {
				MasterInterface recruited;
				recruited.locality = locality;
				recruited.initEndpoints();

				startRole( Role::MASTER, recruited.id(), interf.id() );

				DUMPTOKEN( recruited.waitFailure );
				DUMPTOKEN( recruited.getRateInfo );
				DUMPTOKEN( recruited.tlogRejoin );
				DUMPTOKEN( recruited.changeCoordinators );
				DUMPTOKEN( recruited.getCommitVersion );

				//printf("Recruited as masterServer\n");
				Future<Void> masterProcess = masterServer( recruited, dbInfo, ServerCoordinators( connFile ), req.lifetime, req.forceRecovery );
				errorForwarders.add( zombie(recruited, forwardError( errors, Role::MASTER, recruited.id(), masterProcess )) );
				req.reply.send(recruited);
			}
			when( InitializeTLogRequest req = waitNext(interf.tLog.getFuture()) ) {
				auto& logData = sharedLogs[req.storeType];
				logData.second.send(req);
				if(!logData.first.isValid() || logData.first.isReady()) {
					UID logId = g_random->randomUniqueID();
					std::map<std::string, std::string> details;
					details["ForMaster"] = req.recruitmentID.shortString();
					details["StorageEngine"] = req.storeType.toString();

					//FIXME: start role for every tlog instance, rather that just for the shared actor, also use a different role type for the shared actor
					startRole( Role::SHARED_TRANSACTION_LOG, logId, interf.id(), details );

					std::string filename = filenameFromId( req.storeType, folder, fileLogDataPrefix.toString(), logId );
					IKeyValueStore* data = openKVStore( req.storeType, filename, logId, memoryLimit );
					IDiskQueue* queue = openDiskQueue( joinPath( folder, fileLogQueuePrefix.toString() + logId.toString() + "-" ), tlogQueueExtension.toString(), logId );
					filesClosed.add( data->onClosed() );
					filesClosed.add( queue->onClosed() );

					logData.first = tLog( data, queue, dbInfo, locality, logData.second, logId, false, Promise<Void>(), Promise<Void>() );
					logData.first = handleIOErrors( logData.first, data, logId );
					logData.first = handleIOErrors( logData.first, queue, logId );
					errorForwarders.add( forwardError( errors, Role::SHARED_TRANSACTION_LOG, logId, logData.first ) );
				}
			}
			when( InitializeStorageRequest req = waitNext(interf.storage.getFuture()) ) {
				if( !storageCache.exists( req.reqId ) ) {
					StorageServerInterface recruited(req.interfaceId);
					recruited.locality = locality;
					recruited.initEndpoints();

					std::map<std::string, std::string> details;
					details["StorageEngine"] = req.storeType.toString();
					startRole( Role::STORAGE_SERVER, recruited.id(), interf.id(), details );

					DUMPTOKEN(recruited.getVersion);
					DUMPTOKEN(recruited.getValue);
					DUMPTOKEN(recruited.getKey);
					DUMPTOKEN(recruited.getKeyValues);
					DUMPTOKEN(recruited.getShardState);
					DUMPTOKEN(recruited.waitMetrics);
					DUMPTOKEN(recruited.splitMetrics);
					DUMPTOKEN(recruited.getPhysicalMetrics);
					DUMPTOKEN(recruited.waitFailure);
					DUMPTOKEN(recruited.getQueuingMetrics);
					DUMPTOKEN(recruited.getKeyValueStoreType);
					DUMPTOKEN(recruited.watchValue);
					//printf("Recruited as storageServer\n");

					std::string filename = filenameFromId( req.storeType, folder, fileStoragePrefix.toString(), recruited.id() );
					IKeyValueStore* data = openKVStore( req.storeType, filename, recruited.id(), memoryLimit );
					Future<Void> kvClosed = data->onClosed();
					filesClosed.add( kvClosed );
					ReplyPromise<InitializeStorageReply> storageReady = req.reply;
					storageCache.set( req.reqId, storageReady.getFuture() );
					Future<Void> s = storageServer( data, recruited, req.seedTag, storageReady, dbInfo, folder );
					s = handleIOErrors(s, data, recruited.id(), kvClosed);
					s = storageCache.removeOnReady( req.reqId, s );
					s = storageServerRollbackRebooter( s, req.storeType, filename, recruited.id(), recruited.locality, dbInfo, folder, &filesClosed, memoryLimit, data );
					errorForwarders.add( forwardError( errors, Role::STORAGE_SERVER, recruited.id(), s ) );
				} else
					forwardPromise( req.reply, storageCache.get( req.reqId ) );
			}
			when( InitializeMasterProxyRequest req = waitNext(interf.masterProxy.getFuture()) ) {
				MasterProxyInterface recruited;
				recruited.locality = locality;
				recruited.initEndpoints();

				std::map<std::string, std::string> details;
				details["ForMaster"] = req.master.id().shortString();
				startRole( Role::MASTER_PROXY, recruited.id(), interf.id(), details );

				DUMPTOKEN(recruited.commit);
				DUMPTOKEN(recruited.getConsistentReadVersion);
				DUMPTOKEN(recruited.getKeyServersLocations);
				DUMPTOKEN(recruited.getStorageServerRejoinInfo);
				DUMPTOKEN(recruited.waitFailure);
				DUMPTOKEN(recruited.getRawCommittedVersion);
				DUMPTOKEN(recruited.txnState);

				//printf("Recruited as masterProxyServer\n");
				errorForwarders.add( zombie(recruited, forwardError( errors, Role::MASTER_PROXY, recruited.id(),
						masterProxyServer( recruited, req, dbInfo ) ) ) );
				req.reply.send(recruited);
			}
			when( InitializeResolverRequest req = waitNext(interf.resolver.getFuture()) ) {
				ResolverInterface recruited;
				recruited.locality = locality;
				recruited.initEndpoints();

				std::map<std::string, std::string> details;
				startRole( Role::RESOLVER, recruited.id(), interf.id(), details );

				DUMPTOKEN(recruited.resolve);
				DUMPTOKEN(recruited.metrics);
				DUMPTOKEN(recruited.split);
				DUMPTOKEN(recruited.waitFailure);

				errorForwarders.add( zombie(recruited, forwardError( errors, Role::RESOLVER, recruited.id(),
						resolver( recruited, req, dbInfo ) ) ) );
				req.reply.send(recruited);
			}
			when( InitializeLogRouterRequest req = waitNext(interf.logRouter.getFuture()) ) {
				TLogInterface recruited(locality);
				recruited.initEndpoints();

				std::map<std::string, std::string> details;
				startRole( Role::LOG_ROUTER, recruited.id(), interf.id(), details );

				DUMPTOKEN( recruited.peekMessages );
				DUMPTOKEN( recruited.popMessages );
				DUMPTOKEN( recruited.commit );
				DUMPTOKEN( recruited.lock );
				DUMPTOKEN( recruited.getQueuingMetrics );
				DUMPTOKEN( recruited.confirmRunning );

				errorForwarders.add( zombie(recruited, forwardError( errors, Role::LOG_ROUTER, recruited.id(), 
						logRouter( recruited, req, dbInfo ) ) ) );
				req.reply.send(recruited);
			}
			when( CoordinationPingMessage m = waitNext( interf.coordinationPing.getFuture() ) ) {
				TraceEvent("CoordinationPing", interf.id()).detail("CCID", m.clusterControllerId).detail("TimeStep", m.timeStep);
			}
			when( SetMetricsLogRateRequest req = waitNext(interf.setMetricsRate.getFuture()) ) {
				TraceEvent("LoggingRateChange", interf.id()).detail("OldDelay", loggingDelay).detail("NewLogPS", req.metricsLogsPerSecond);
				if( req.metricsLogsPerSecond != 0 ) {
					loggingDelay = 1.0 / req.metricsLogsPerSecond;
					loggingTrigger = Void();
				}
			}
			when( EventLogRequest req = waitNext(interf.eventLogRequest.getFuture()) ) {
				TraceEventFields e;
				if( req.getLastError )
					e = latestEventCache.getLatestError();
				else
					e = latestEventCache.get( req.eventName.toString() );
				req.reply.send(e);
			}
			when( TraceBatchDumpRequest req = waitNext(interf.traceBatchDumpRequest.getFuture()) ) {
				g_traceBatch.dump();
				req.reply.send(Void());
			}
			when( DiskStoreRequest req = waitNext(interf.diskStoreRequest.getFuture()) ) {
				Standalone<VectorRef<UID>> ids;
				for(DiskStore d : getDiskStores(folder)) {
					bool included = true;
					if(!req.includePartialStores) {
						if(d.storeType == KeyValueStoreType::SSD_BTREE_V1) {
							included = fileExists(d.filename + ".fdb-wal");
						}
						else if (d.storeType == KeyValueStoreType::SSD_BTREE_V2) {
							included = fileExists(d.filename + ".sqlite-wal");
						}
						else if (d.storeType == KeyValueStoreType::SSD_REDWOOD_V1) {
							included = fileExists(d.filename + "0.pagerlog") && fileExists(d.filename + "1.pagerlog");
						}
						else {
							ASSERT(d.storeType == KeyValueStoreType::MEMORY);
							included = fileExists(d.filename + "1.fdq");
						}
						if(d.storedComponent == DiskStore::COMPONENT::TLogData && included) {
							std::string basename = fileLogQueuePrefix.toString() + d.filename.substr(fileLogDataPrefix.size());
							included = fileExists(basename + "0.fdq") && fileExists(basename + "1.fdq");
						}
					}
					if(included) {
						ids.push_back(ids.arena(), d.storeID);
					}
				}
				req.reply.send(ids);
			}
			when( wait( loggingTrigger ) ) {
				systemMonitor();
				loggingTrigger = delay( loggingDelay, TaskFlushTrace );
			}
			when( wait( errorForwarders.getResult() ) ) {}
			when( wait( handleErrors ) ) {}
		}
	} catch (Error& err) {
		state Error e = err;
		bool ok = e.code() == error_code_please_reboot || e.code() == error_code_actor_cancelled || e.code() == error_code_please_reboot_delete;

		endRole(Role::WORKER, interf.id(), "WorkerError", ok, e);
		errorForwarders.clear(false);
		sharedLogs.clear();

		if (e.code() != error_code_actor_cancelled) { // We get cancelled e.g. when an entire simulation times out, but in that case we won't be restarted and don't need to wait for shutdown
			stopping.send(Void());
			wait( filesClosed.getResult() ); // Wait for complete shutdown of KV stores
			wait(delay(0.0)); //Unwind the callstack to make sure that IAsyncFile references are all gone
			TraceEvent(SevInfo, "WorkerShutdownComplete", interf.id());
		}

		throw e;
	}
}

ACTOR Future<Void> extractClusterInterface( Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> a, Reference<AsyncVar<Optional<ClusterInterface>>> b ) {
	loop {
		if(a->get().present())
			b->set(a->get().get().clientInterface);
		else
			b->set(Optional<ClusterInterface>());
		wait(a->onChange());
	}
}

static std::set<int> const& normalWorkerErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert( error_code_please_reboot );
		s.insert( error_code_please_reboot_delete );
	}
	return s;
}

ACTOR Future<Void> fileNotFoundToNever(Future<Void> f) {
	try {
		wait(f);
		return Void();
	}
	catch(Error &e) {
		if(e.code() == error_code_file_not_found) {
			TraceEvent(SevWarn, "ClusterCoordinatorFailed").error(e);
			return Never();
		}
		throw;
	}
}

ACTOR Future<Void> printTimeout() {
	wait( delay(5) );
	if( !g_network->isSimulated() ) {
		fprintf(stderr, "Warning: FDBD has not joined the cluster after 5 seconds.\n");
		fprintf(stderr, "  Check configuration and availability using the 'status' command with the fdbcli\n");
	}
	return Void();
}

ACTOR Future<Void> printOnFirstConnected( Reference<AsyncVar<Optional<ClusterInterface>>> ci ) {
	state Future<Void> timeoutFuture = printTimeout();
	loop {
		choose {
			when (wait( ci->get().present() ? IFailureMonitor::failureMonitor().onStateEqual( ci->get().get().openDatabase.getEndpoint(), FailureStatus(false) ) : Never() )) {
				printf("FDBD joined cluster.\n");
				TraceEvent("FDBDConnected");
				return Void();
			}
			when( wait(ci->onChange())) {}
		}
	}
}

ClusterControllerPriorityInfo getCCPriorityInfo(std::string filePath, ProcessClass processClass) {
	if (!fileExists(filePath))
		return ClusterControllerPriorityInfo(ProcessClass(processClass.classType(), ProcessClass::CommandLineSource).machineClassFitness(ProcessClass::ClusterController), false, ClusterControllerPriorityInfo::FitnessUnknown);
	std::string contents(readFileBytes(filePath, 1000));
	BinaryReader br(StringRef(contents), IncludeVersion());
	ClusterControllerPriorityInfo priorityInfo(ProcessClass::UnsetFit, false, ClusterControllerPriorityInfo::FitnessUnknown);
	br >> priorityInfo;
	if (!br.empty()) {
		if (g_network->isSimulated()) {
			ASSERT(false);
		}
		else {
			TraceEvent(SevWarnAlways, "FitnessFileCorrupted").detail("filePath", filePath);
			return ClusterControllerPriorityInfo(ProcessClass(processClass.classType(), ProcessClass::CommandLineSource).machineClassFitness(ProcessClass::ClusterController), false, ClusterControllerPriorityInfo::FitnessUnknown);
		}
	}
	return priorityInfo;
}

ACTOR Future<Void> monitorAndWriteCCPriorityInfo(std::string filePath, Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo) {
	loop {
		wait(asyncPriorityInfo->onChange());
		std::string contents(BinaryWriter::toValue(asyncPriorityInfo->get(), IncludeVersion()).toString());
		atomicReplace(filePath, contents, false);
	}
}

ACTOR Future<UID> createAndLockProcessIdFile(std::string folder) {
	state UID processIDUid;
	platform::createDirectory(folder);

	try {
		state std::string lockFilePath = joinPath(folder, "processId");
		state ErrorOr<Reference<IAsyncFile>> lockFile = wait(errorOr(IAsyncFileSystem::filesystem(g_network)->open(lockFilePath, IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_LOCK, 0600)));

		if (lockFile.isError() && lockFile.getError().code() == error_code_file_not_found && !fileExists(lockFilePath)) {
			Reference<IAsyncFile> _lockFile = wait(IAsyncFileSystem::filesystem()->open(lockFilePath, IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_LOCK | IAsyncFile::OPEN_READWRITE, 0600));
			lockFile = _lockFile;
			processIDUid = g_random->randomUniqueID();
			BinaryWriter wr(IncludeVersion());
			wr << processIDUid;
			wait(lockFile.get()->write(wr.getData(), wr.getLength(), 0));
			wait(lockFile.get()->sync());
		}
		else {
			if (lockFile.isError()) throw lockFile.getError(); // If we've failed to open the file, throw an exception

			int64_t fileSize = wait(lockFile.get()->size());
			state Key fileData = makeString(fileSize);
			int length = wait(lockFile.get()->read(mutateString(fileData), fileSize, 0));
			processIDUid = BinaryReader::fromStringRef<UID>(fileData, IncludeVersion());
		}
	}
	catch (Error& e) {
		if (e.code() != error_code_actor_cancelled) {
			if (!e.isInjectedFault())
				fprintf(stderr, "ERROR: error creating or opening process id file `%s'.\n", joinPath(folder, "processId").c_str());
			TraceEvent(SevError, "OpenProcessIdError").error(e);
		}
		throw;
	}
	return processIDUid;
}

ACTOR Future<Void> fdbd(
	Reference<ClusterConnectionFile> connFile,
	LocalityData localities,
	ProcessClass processClass,
	std::string dataFolder,
	std::string coordFolder,
	int64_t memoryLimit,
	std::string metricsConnFile,
	std::string metricsPrefix )
{
	try {

		ServerCoordinators coordinators( connFile );
		TraceEvent("StartingFDBD").detailext("ZoneID", localities.zoneId()).detailext("MachineId", localities.machineId()).detail("DiskPath", dataFolder).detail("CoordPath", coordFolder);

		// SOMEDAY: start the services on the machine in a staggered fashion in simulation?
		state vector<Future<Void>> v;
		// Endpoints should be registered first before any process trying to connect to it. So coordinationServer actor should be the first one executed before any other.
		if ( coordFolder.size() )
			v.push_back( fileNotFoundToNever( coordinationServer( coordFolder ) ) ); //SOMEDAY: remove the fileNotFound wrapper and make DiskQueue construction safe from errors setting up their files
		
		state UID processIDUid = wait(createAndLockProcessIdFile(dataFolder));
		localities.set(LocalityData::keyProcessId, processIDUid.toString());
		// Only one process can execute on a dataFolder from this point onwards

		std::string fitnessFilePath = joinPath(dataFolder, "fitness");
		Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> cc(new AsyncVar<Optional<ClusterControllerFullInterface>>);
		Reference<AsyncVar<Optional<ClusterInterface>>> ci(new AsyncVar<Optional<ClusterInterface>>);
		Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo(new AsyncVar<ClusterControllerPriorityInfo>(getCCPriorityInfo(fitnessFilePath, processClass)));
		Promise<Void> recoveredDiskFiles;

		v.push_back(reportErrors(monitorAndWriteCCPriorityInfo(fitnessFilePath, asyncPriorityInfo), "MonitorAndWriteCCPriorityInfo"));
		v.push_back( reportErrors( processClass == ProcessClass::TesterClass ? monitorLeader( connFile, cc ) : clusterController( connFile, cc , asyncPriorityInfo, recoveredDiskFiles.getFuture(), localities ), "ClusterController") );
		v.push_back( reportErrors(extractClusterInterface( cc, ci ), "ExtractClusterInterface") );
		v.push_back( reportErrors(failureMonitorClient( ci, true ), "FailureMonitorClient") );
		v.push_back( reportErrorsExcept(workerServer(connFile, cc, localities, asyncPriorityInfo, processClass, dataFolder, memoryLimit, metricsConnFile, metricsPrefix, recoveredDiskFiles), "WorkerServer", UID(), &normalWorkerErrors()) );
		state Future<Void> firstConnect = reportErrors( printOnFirstConnected(ci), "ClusterFirstConnectedError" );

		wait( quorum(v,1) );
		ASSERT(false);  // None of these actors should terminate normally
		throw internal_error();
	} catch(Error &e) {
		Error err = checkIOTimeout(e);
		throw err;
	}
}

const Role Role::WORKER("Worker", "WK", false);
const Role Role::STORAGE_SERVER("StorageServer", "SS");
const Role Role::TRANSACTION_LOG("TLog", "TL");
const Role Role::SHARED_TRANSACTION_LOG("SharedTLog", "SL", false);
const Role Role::MASTER_PROXY("MasterProxyServer", "MP");
const Role Role::MASTER("MasterServer", "MS");
const Role Role::RESOLVER("Resolver", "RV");
const Role Role::CLUSTER_CONTROLLER("ClusterController", "CC");
const Role Role::TESTER("Tester", "TS");
const Role Role::LOG_ROUTER("LogRouter", "LR");
