/*
 * Restore.actor.cpp
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
#include "fdbclient/SystemData.h"

// Backup agent header
#include "fdbclient/BackupAgent.actor.h"
//#include "FileBackupAgent.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/BackupContainer.h"

#include <ctime>
#include <climits>
#include "fdbrpc/IAsyncFile.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include <numeric>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <algorithm>

#include "flow/ActorCollection.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreWorkerInterface.actor.h"
#include "fdbserver/RestoreCommon.actor.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreLoader.actor.h"
#include "fdbserver/RestoreApplier.actor.h"
#include "fdbserver/RestoreMaster.actor.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

// NOTE: The initRestoreWorkerConfig function will reset the configuration params in simulation
// These configurations for restore workers will  be set in initRestoreWorkerConfig() later.

int ratio_loader_to_applier = 1; // the ratio of loader over applier. The loader number = total worker * (ratio /  (ratio + 1) )
int NUM_LOADERS = 120;
int NUM_APPLIERS = 40;
int MIN_NUM_WORKERS = NUM_LOADERS + NUM_APPLIERS; //10; // TODO: This can become a configuration param later
int FastRestore_Failure_Timeout = 3600; // seconds
double loadBatchSizeMB = 10 * 1024; // MB
double loadBatchSizeThresholdB = loadBatchSizeMB * 1024 * 1024;
double mutationVectorThreshold =  1 * 1024 * 1024; // Bytes // correctness passed when the value is 1
double transactionBatchSizeThreshold =  512; // Byte

int restoreStatusIndex = 0;

class RestoreConfig;
struct RestoreWorkerData; // Only declare the struct exist but we cannot use its field

void initRestoreWorkerConfig();

ACTOR Future<Void> handlerTerminateWorkerRequest(RestoreSimpleRequest req, Reference<RestoreWorkerData> self, RestoreWorkerInterface workerInterf, Database cx);
ACTOR Future<Void> monitorWorkerLiveness(Reference<RestoreWorkerData> self);
ACTOR Future<Void> handleRecruitRoleRequest(RestoreRecruitRoleRequest req, Reference<RestoreWorkerData> self, ActorCollection *actors, Database cx);
ACTOR Future<Void> collectRestoreWorkerInterface(Reference<RestoreWorkerData> self, Database cx, int min_num_workers = 2);
ACTOR Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> self);
ACTOR Future<Void> monitorleader(Reference<AsyncVar<RestoreWorkerInterface>> leader, Database cx, RestoreWorkerInterface myWorkerInterf);
ACTOR Future<Void> startRestoreWorkerLeader(Reference<RestoreWorkerData> self, RestoreWorkerInterface workerInterf, Database cx);
ACTOR Future<Void> handleRestoreSysInfoRequest(RestoreSysInfoRequest req, Reference<RestoreWorkerData> self);

bool debug_verbose = false;
void printGlobalNodeStatus(Reference<RestoreWorkerData>);

template<> Tuple Codec<ERestoreState>::pack(ERestoreState const &val); // { return Tuple().append(val); }
template<> ERestoreState Codec<ERestoreState>::unpack(Tuple const &val); // { return (ERestoreState)val.getInt(0); }


// DEBUG_FAST_RESTORE is not used right now!
#define DEBUG_FAST_RESTORE 1

#ifdef DEBUG_FAST_RESTORE
#define dbprintf_rs(fmt, args...)	printf(fmt, ## args);
#else
#define dbprintf_rs(fmt, args...)
#endif


// Each restore worker (a process) is assigned for a role.
// MAYBE Later: We will support multiple restore roles on a worker
struct RestoreWorkerData :  NonCopyable, public ReferenceCounted<RestoreWorkerData> {
	UID workerID;
	std::map<UID, RestoreWorkerInterface> workerInterfaces; // UID is worker's node id, RestoreWorkerInterface is worker's communication workerInterface

	// Restore Roles
	Optional<RestoreLoaderInterface> loaderInterf;
	Reference<RestoreLoaderData> loaderData;
	Optional<RestoreApplierInterface> applierInterf;
	Reference<RestoreApplierData> applierData;
	Reference<RestoreMasterData> masterData;

	uint32_t inProgressFlag = 0; // To avoid race between duplicate message delivery that invokes the same actor multiple times

	UID id() const { return workerID; };

	RestoreWorkerData() = default;

	~RestoreWorkerData() {
		printf("[Exit] Worker:%s RestoreWorkerData is deleted\n", workerID.toString().c_str());
	}

	std::string describeNode() {
		std::stringstream ss;
		ss << "RestoreWorker workerID:" << workerID.toString();
		return ss.str();
	}
};

// Remove the worker interface from restoreWorkerKey and remove its roles interfaces from their keys.
ACTOR Future<Void> handlerTerminateWorkerRequest(RestoreSimpleRequest req, Reference<RestoreWorkerData> self, RestoreWorkerInterface workerInterf, Database cx) {
	wait( runRYWTransaction( cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->clear(restoreWorkerKeyFor(workerInterf.id()));
		if ( self->loaderInterf.present() ) {
			tr->clear(restoreLoaderKeyFor(self->loaderInterf.get().id()));
		}
		if ( self->applierInterf.present() ) {
			tr->clear(restoreApplierKeyFor(self->applierInterf.get().id()));
		}
		return Void();
  	}) );

	printf("Node:%s finish restore, clear the interface keys for all roles on the worker (id:%s) and the worker itself. Then exit\n", self->describeNode().c_str(),  workerInterf.id().toString().c_str()); 
			req.reply.send( RestoreCommonReply(workerInterf.id()) );

	return Void();
 }

// Periodically send worker heartbeat to 
 ACTOR Future<Void> monitorWorkerLiveness(Reference<RestoreWorkerData> self) {
	ASSERT( !self->workerInterfaces.empty() );
	state int wIndex = 0;
	for (auto &workerInterf : self->workerInterfaces) {
		printf("[Worker:%d][UID:%s][Interf.NodeInfo:%s]\n", wIndex, workerInterf.first.toString().c_str(), workerInterf.second.id().toString().c_str());
		wIndex++;
	}

	state std::vector<Future<RestoreCommonReply>> cmdReplies;
	state std::map<UID, RestoreWorkerInterface>::iterator workerInterf;
	loop {
		wIndex = 0;
		for ( workerInterf = self->workerInterfaces.begin(); workerInterf !=  self->workerInterfaces.end(); workerInterf++)  {
			try {
				wait( delay(1.0) );
				cmdReplies.push_back( workerInterf->second.heartbeat.getReply(RestoreSimpleRequest()) );
				std::vector<RestoreCommonReply> reps = wait( timeoutError(getAll(cmdReplies), FastRestore_Failure_Timeout) );
				cmdReplies.clear();
				wIndex++;
			} catch (Error &e) {
				fprintf(stdout, "[ERROR] Node:%s, error. error code:%d, error message:%s\n", self->describeNode().c_str(),
							 e.code(), e.what());
				printf("[Heartbeat: Node may be down][Worker:%d][UID:%s][Interf.NodeInfo:%s]\n", wIndex,  workerInterf->first.toString().c_str(), workerInterf->second.id().toString().c_str());
			}
		}
		wait( delay(30.0) );
	}
 }

void initRestoreWorkerConfig() {
	ratio_loader_to_applier = 1; // the ratio of loader over applier. The loader number = total worker * (ratio /  (ratio + 1) )
	NUM_LOADERS = g_network->isSimulated() ? 3 : NUM_LOADERS;
	//NUM_APPLIERS = 1;
	NUM_APPLIERS = g_network->isSimulated() ? 3 : NUM_APPLIERS;
	MIN_NUM_WORKERS  = NUM_LOADERS + NUM_APPLIERS;
	FastRestore_Failure_Timeout = 3600; // seconds
	loadBatchSizeMB = g_network->isSimulated() ? 1 : loadBatchSizeMB; // MB
	loadBatchSizeThresholdB = loadBatchSizeMB * 1024 * 1024;
	mutationVectorThreshold = g_network->isSimulated() ? 100 : mutationVectorThreshold; // Bytes // correctness passed when the value is 1
	transactionBatchSizeThreshold = g_network->isSimulated() ? 512 : transactionBatchSizeThreshold; // Byte

	printf("Init RestoreWorkerConfig. min_num_workers:%d ratio_loader_to_applier:%d loadBatchSizeMB:%.2f loadBatchSizeThresholdB:%.2f transactionBatchSizeThreshold:%.2f\n",
			MIN_NUM_WORKERS, ratio_loader_to_applier, loadBatchSizeMB, loadBatchSizeThresholdB, transactionBatchSizeThreshold);
}

// Assume only 1 role on a restore worker.
// Future: Multiple roles in a restore worker
ACTOR Future<Void> handleRecruitRoleRequest(RestoreRecruitRoleRequest req, Reference<RestoreWorkerData> self, ActorCollection *actors, Database cx) {
	printf("[INFO][Worker] Node:%s get role %s\n", self->describeNode().c_str(),
			getRoleStr(req.role).c_str());

	// Already recruited a role
	if (self->loaderInterf.present()) {
		ASSERT( req.role == RestoreRole::Loader );
		req.reply.send(RestoreRecruitRoleReply(self->id(), RestoreRole::Loader, self->loaderInterf.get()));
		return Void();
	} else if (self->applierInterf.present()) {
		req.reply.send(RestoreRecruitRoleReply(self->id(), RestoreRole::Applier, self->applierInterf.get()));
		return Void();
	}

	if (req.role == RestoreRole::Loader) {
		ASSERT( !self->loaderInterf.present() );
		self->loaderInterf = RestoreLoaderInterface();
		self->loaderInterf.get().initEndpoints();
		RestoreLoaderInterface &recruited = self->loaderInterf.get();
		DUMPTOKEN(recruited.setApplierKeyRangeVectorRequest);
		DUMPTOKEN(recruited.loadRangeFile);
		DUMPTOKEN(recruited.loadLogFile);
		DUMPTOKEN(recruited.initVersionBatch);
		DUMPTOKEN(recruited.collectRestoreRoleInterfaces);
		DUMPTOKEN(recruited.finishRestore);
		self->loaderData = Reference<RestoreLoaderData>( new RestoreLoaderData(self->loaderInterf.get().id(),  req.nodeIndex) );
		actors->add( restoreLoaderCore(self->loaderData, self->loaderInterf.get(), cx) );
		req.reply.send(RestoreRecruitRoleReply(self->id(),  RestoreRole::Loader, self->loaderInterf.get()));
	} else if (req.role == RestoreRole::Applier) {
		ASSERT( !self->applierInterf.present() );
		self->applierInterf = RestoreApplierInterface();
		self->applierInterf.get().initEndpoints();
		RestoreApplierInterface &recruited = self->applierInterf.get();
		DUMPTOKEN(recruited.sendMutationVector);
		DUMPTOKEN(recruited.applyToDB);
		DUMPTOKEN(recruited.initVersionBatch);
		DUMPTOKEN(recruited.collectRestoreRoleInterfaces);
		DUMPTOKEN(recruited.finishRestore);
		self->applierData = Reference<RestoreApplierData>( new RestoreApplierData(self->applierInterf.get().id(), req.nodeIndex) );
		actors->add( restoreApplierCore(self->applierData, self->applierInterf.get(), cx) );
		req.reply.send(RestoreRecruitRoleReply(self->id(),  RestoreRole::Applier, self->applierInterf.get()));
	} else {
		TraceEvent(SevError, "FastRestore").detail("HandleRecruitRoleRequest", "UnknownRole"); //.detail("Request", req.printable());
	}

	return Void();
}

// Assume: Only update the local data if it (applierInterf) has not been set
ACTOR Future<Void> handleRestoreSysInfoRequest(RestoreSysInfoRequest req, Reference<RestoreWorkerData> self) {
	printf("handleRestoreSysInfoRequest, self->id:%s loaderData.isValid:%d\n",
		   self->id().toString().c_str(), self->loaderData.isValid());
	// Applier does not need to know appliers interfaces
	if ( !self->loaderData.isValid() ) {
		req.reply.send(RestoreCommonReply(self->id()));
		return Void();
	}
	// The loader has received the appliers interfaces
	if ( !self->loaderData->appliersInterf.empty() ) {
		req.reply.send(RestoreCommonReply(self->id()));
		return Void();
	}

	self->loaderData->appliersInterf = req.sysInfo.appliers;
	
	req.reply.send(RestoreCommonReply(self->id()) );
	return Void();
}


// Read restoreWorkersKeys from DB to get each restore worker's restore workerInterface and set it to self->workerInterfaces
// This is done before we assign restore roles for restore workers
 ACTOR Future<Void> collectRestoreWorkerInterface(Reference<RestoreWorkerData> self, Database cx, int min_num_workers) {
	state Transaction tr(cx);

	state vector<RestoreWorkerInterface> agents; // agents is cmdsInterf
	
	loop {
		try {
			self->workerInterfaces.clear();
			agents.clear();
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Standalone<RangeResultRef> agentValues = wait(tr.getRange(restoreWorkersKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!agentValues.more);
			// If agentValues.size() < min_num_workers, we should wait for coming workers to register their workerInterface before we read them once for all
			if(agentValues.size() >= min_num_workers) {
				for(auto& it : agentValues) {
					agents.push_back(BinaryReader::fromStringRef<RestoreWorkerInterface>(it.value, IncludeVersion()));
					// Save the RestoreWorkerInterface for the later operations
					self->workerInterfaces.insert(std::make_pair(agents.back().id(), agents.back()));
					printf("collectWorkerInterface, workerInterface id:%s\n", agents.back().id().toString().c_str());
				}
				break;
			}
			printf("%s:Wait for enough workers. Current num_workers:%d target num_workers:%d\n",
					self->describeNode().c_str(), agentValues.size(), min_num_workers);
			wait( delay(5.0) );
		} catch( Error &e ) {
			printf("[WARNING]%s: collectWorkerInterface transaction error:%s\n", self->describeNode().c_str(), e.what());
			wait( tr.onError(e) );
		}
	}
	ASSERT(agents.size() >= min_num_workers); // ASSUMPTION: We must have at least 1 loader and 1 applier

	TraceEvent("FastRestore").detail("CollectWorkerInterfaceNumWorkers", self->workerInterfaces.size());

	return Void();
 }

// RestoreWorker that has restore master role: Recruite a role for each worker
ACTOR Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> self)  {
	printf("%s:Start configuring roles for workers\n", self->describeNode().c_str());
	ASSERT( self->masterData.isValid() );

	// Set up the role, and the global status for each node
	int numNodes = self->workerInterfaces.size();
	state int numLoader = NUM_LOADERS; //numNodes * ratio_loader_to_applier / (ratio_loader_to_applier + 1);
	state int numApplier = NUM_APPLIERS; //numNodes - numLoader;
	if (numLoader <= 0 || numApplier <= 0) {
		ASSERT( numLoader > 0 ); // Quick check in correctness
		ASSERT( numApplier > 0 );
		fprintf(stderr, "[ERROR] not enough nodes for loader and applier. numLoader:%d, numApplier:%d, ratio_loader_to_applier:%d, numAgents:%d\n", numLoader, numApplier, ratio_loader_to_applier, numNodes);
	} else {
		printf("Node%s: Configure roles numWorkders:%d numLoader:%d numApplier:%d\n", self->describeNode().c_str(), numNodes, numLoader, numApplier);
	}

	// Assign a role to each worker
	state int nodeIndex = 0;
	state RestoreRole role;
	printf("Node:%s Start configuring roles for workers\n", self->describeNode().c_str());

	printf("numLoader:%d, numApplier:%d, self->workerInterfaces.size:%d\n", numLoader, numApplier, self->workerInterfaces.size());
	ASSERT( numLoader + numApplier <= self->workerInterfaces.size() ); // We assign 1 role per worker for now
	std::map<UID, RestoreRecruitRoleRequest> requests;
	for (auto &workerInterf : self->workerInterfaces)  {
		if ( nodeIndex >= 0 && nodeIndex < numApplier ) { 
			// [0, numApplier) are appliers
			role = RestoreRole::Applier;
		} else if ( nodeIndex >= numApplier && nodeIndex < numLoader + numApplier ) {
			// [numApplier, numApplier + numLoader) are loaders
			role = RestoreRole::Loader;
		}

		printf("Node:%s Set role (%s) to node (index=%d uid=%s)\n", self->describeNode().c_str(),
				getRoleStr(role).c_str(), nodeIndex,  workerInterf.first.toString().c_str());
		requests[workerInterf.first] = RestoreRecruitRoleRequest(role, nodeIndex);
		nodeIndex++;
	}
	state std::vector<RestoreRecruitRoleReply> replies;
	wait( getBatchReplies(&RestoreWorkerInterface::recruitRole, self->workerInterfaces, requests, &replies) );
	printf("TEST: RestoreRecruitRoleReply replies.size:%d\n", replies.size());
	for (auto& reply : replies) {
		printf("TEST: RestoreRecruitRoleReply reply:%s\n", reply.toString().c_str());
		if ( reply.role == RestoreRole::Applier ) {
			ASSERT_WE_THINK(reply.applier.present());
			self->masterData->appliersInterf[reply.applier.get().id()] = reply.applier.get();
		} else if ( reply.role == RestoreRole::Loader ) {
			ASSERT_WE_THINK(reply.loader.present());
			self->masterData->loadersInterf[reply.loader.get().id()] = reply.loader.get();
		} else {
			TraceEvent(SevError, "FastRestore").detail("RecruitRestoreRoles_InvalidRole", reply.role);
		}
	}
	printf("[RecruitRestoreRoles] Finished\n");

	return Void();
}

ACTOR Future<Void> distributeRestoreSysInfo(Reference<RestoreWorkerData> self)  {
	ASSERT( self->masterData.isValid() );
	ASSERT( !self->masterData->loadersInterf.empty() );
	RestoreSysInfo sysInfo(self->masterData->appliersInterf);
	std::vector<std::pair<UID, RestoreSysInfoRequest>> requests;
	for (auto &worker : self->workerInterfaces) {
		requests.push_back( std::make_pair(worker.first, RestoreSysInfoRequest(sysInfo)) );
	}
	printf("Master: distributeRestoreSysInfo\n");
	wait( sendBatchRequests(&RestoreWorkerInterface::updateRestoreSysInfo, self->workerInterfaces, requests) );

	TraceEvent("FastRestore").detail("DistributeRestoreSysInfo", "Finish");
	return Void();
}

// RestoreWorkerLeader is the worker that runs RestoreMaster role
ACTOR Future<Void> startRestoreWorkerLeader(Reference<RestoreWorkerData> self, RestoreWorkerInterface workerInterf, Database cx) {
	self->masterData = Reference<RestoreMasterData>(new RestoreMasterData());
	// We must wait for enough time to make sure all restore workers have registered their workerInterfaces into the DB
	printf("[INFO][Master] NodeID:%s Restore master waits for agents to register their workerKeys\n",
			workerInterf.id().toString().c_str());
	wait( delay(10.0) );
	printf("[INFO][Master]  NodeID:%s starts configuring roles for workers\n", workerInterf.id().toString().c_str());

	wait( collectRestoreWorkerInterface(self, cx, MIN_NUM_WORKERS) );

	state Future<Void> workersFailureMonitor = monitorWorkerLiveness(self);

	// recruitRestoreRoles must be after collectWorkerInterface
	wait( recruitRestoreRoles(self) );

	wait( distributeRestoreSysInfo(self) );

	wait( startRestoreMaster(self->masterData, cx) );

	return Void();
}

ACTOR Future<Void> startRestoreWorker(Reference<RestoreWorkerData> self, RestoreWorkerInterface interf, Database cx) {
	state double lastLoopTopTime;
	state ActorCollection actors(false); // Collect the main actor for each role
	
	loop {
		double loopTopTime = now();
		double elapsedTime = loopTopTime - lastLoopTopTime;
		if( elapsedTime > 0.050 ) {
			if (g_random->random01() < 0.01)
				TraceEvent(SevWarn, "SlowRestoreLoaderLoopx100").detail("NodeDesc", self->describeNode()).detail("Elapsed", elapsedTime);
		}
		lastLoopTopTime = loopTopTime;
		state std::string requestTypeStr = "[Init]";

		try {
			choose {
				when ( RestoreSimpleRequest req = waitNext(interf.heartbeat.getFuture()) ) {
					requestTypeStr = "heartbeat";
					actors.add( handleHeartbeat(req, interf.id()) );
				}
				when ( RestoreRecruitRoleRequest req = waitNext(interf.recruitRole.getFuture()) ) {
					requestTypeStr = "recruitRole";
					actors.add( handleRecruitRoleRequest(req, self, &actors, cx) );
				}
				when ( RestoreSysInfoRequest req = waitNext(interf.updateRestoreSysInfo.getFuture()) ) {
					requestTypeStr = "updateRestoreSysInfo";
					actors.add( handleRestoreSysInfoRequest(req, self) );
				}
				when ( RestoreSimpleRequest req = waitNext(interf.terminateWorker.getFuture()) ) {
					// Destroy the worker at the end of the restore
					// TODO: Cancel its own actors
					requestTypeStr = "terminateWorker";
					wait( handlerTerminateWorkerRequest(req, self, interf, cx) );
					return Void();
				}
			}
		} catch (Error &e) {
			fprintf(stdout, "[ERROR] RestoreWorker handle received request:%s error. error code:%d, error message:%s\n",
					requestTypeStr.c_str(), e.code(), e.what());
			if ( requestTypeStr.find("[Init]") != std::string::npos ) {
				printf("Exit due to error at requestType:%s", requestTypeStr.c_str());
				break;
			}
		}
	}

	return Void();
}

ACTOR Future<Void> _restoreWorker(Database cx, LocalityData locality) {
	state ActorCollection actors(false);
	state Future<Void> myWork = Never();
	state Reference<AsyncVar<RestoreWorkerInterface>> leader = Reference<AsyncVar<RestoreWorkerInterface>>( 
																new AsyncVar<RestoreWorkerInterface>() );

	state RestoreWorkerInterface myWorkerInterf;
	myWorkerInterf.initEndpoints();
	state Reference<RestoreWorkerData> self = Reference<RestoreWorkerData>(new RestoreWorkerData());
	self->workerID = myWorkerInterf.id();
	initRestoreWorkerConfig(); //TODO: Change to a global struct to store the restore configuration

	//actors.add( doRestoreWorker(leader, myWorkerInterf) );
	//actors.add( monitorleader(leader, cx, myWorkerInterf) );
	wait( monitorleader(leader, cx, myWorkerInterf) );

	printf("Wait for leader\n");
	wait(delay(1));
	if (leader->get() == myWorkerInterf) {
		// Restore master worker: doLeaderThings();
		myWork = startRestoreWorkerLeader(self, myWorkerInterf, cx);
	} else {
		// Restore normal worker (for RestoreLoader and RestoreApplier roles): doWorkerThings();
		myWork = startRestoreWorker(self, myWorkerInterf, cx);
	}

	wait(myWork);
	return Void();
}



// RestoreMaster is the leader
ACTOR Future<Void> monitorleader(Reference<AsyncVar<RestoreWorkerInterface>> leader, Database cx, RestoreWorkerInterface myWorkerInterf) {
 	state ReadYourWritesTransaction tr(cx);
	//state Future<Void> leaderWatch;
	state RestoreWorkerInterface leaderInterf;
	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> leaderValue = wait(tr.get(restoreLeaderKey));
			if(leaderValue.present()) {
				leaderInterf = BinaryReader::fromStringRef<RestoreWorkerInterface>(leaderValue.get(), IncludeVersion());
				// Register my interface as an worker
				tr.set(restoreWorkerKeyFor(myWorkerInterf.id()), restoreWorkerInterfaceValue(myWorkerInterf));
			} else {
				// Workers compete to be the leader
				tr.set(restoreLeaderKey, BinaryWriter::toValue(myWorkerInterf, IncludeVersion()));
				leaderInterf = myWorkerInterf;
			}
			wait( tr.commit() );
			leader->set(leaderInterf);
			break;
		} catch( Error &e ) {
			// We may have error commit_unknown_result, the commit may or may not succeed!
			// We must handle this error, otherwise, if the leader does not know its key has been registered, the leader will stuck here!
			printf("[INFO] NodeID:%s restoreWorker select leader error, error code:%d error info:%s\n",
					myWorkerInterf.id().toString().c_str(), e.code(), e.what());
			wait( tr.onError(e) );
		}
	}

	return Void();
}

ACTOR Future<Void> restoreWorker(Reference<ClusterConnectionFile> ccf, LocalityData locality) {
	Database cx = Database::createDatabase(ccf->getFilename(), Database::API_VERSION_LATEST,locality);
	wait(_restoreWorker(cx, locality));
	return Void();
}

