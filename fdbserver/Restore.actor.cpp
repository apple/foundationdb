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

// Forwaself declaration
void initRestoreWorkerConfig();

ACTOR Future<Void> handlerTerminateWorkerRequest(RestoreSimpleRequest req, Reference<RestoreWorkerData> self, RestoreWorkerInterface workerInterf, Database cx);
ACTOR Future<Void> monitorWorkerLiveness(Reference<RestoreWorkerData> self);
ACTOR Future<Void> commitRestoreRoleInterfaces(Reference<RestoreWorkerData> self, Database cx);
ACTOR Future<Void> handleRecruitRoleRequest(RestoreRecruitRoleRequest req, Reference<RestoreWorkerData> self, ActorCollection *actors, Database cx);
ACTOR Future<Void> collectRestoreWorkerInterface(Reference<RestoreWorkerData> self, Database cx, int min_num_workers = 2);
ACTOR Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> self);
ACTOR Future<Void> monitorleader(Reference<AsyncVar<RestoreWorkerInterface>> leader, Database cx, RestoreWorkerInterface myWorkerInterf);
ACTOR Future<Void> startRestoreWorkerLeader(Reference<RestoreWorkerData> self, RestoreWorkerInterface workerInterf, Database cx);

bool debug_verbose = true;
void printGlobalNodeStatus(Reference<RestoreWorkerData>);


const char *RestoreCommandEnumStr[] = {"Init",
		"Sample_Range_File", "Sample_Log_File", "Sample_File_Done",
		"Loader_Send_Sample_Mutation_To_Applier", "Loader_Send_Sample_Mutation_To_Applier_Done",
		"Calculate_Applier_KeyRange", "Get_Applier_KeyRange", "Get_Applier_KeyRange_Done",
		"Assign_Applier_KeyRange", "Assign_Applier_KeyRange_Done",
		"Assign_Loader_Range_File", "Assign_Loader_Log_File", "Assign_Loader_File_Done",
		"Loader_Send_Mutations_To_Applier", "Loader_Send_Mutations_To_Applier_Done",
		"Apply_Mutation_To_DB", "Apply_Mutation_To_DB_Skip",
		"Loader_Notify_Appler_To_Apply_Mutation",
		"Notify_Loader_ApplierKeyRange", "Notify_Loader_ApplierKeyRange_Done"
};

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
	std::map<UID, RestoreWorkerInterface> workers_workerInterface; // UID is worker's node id, RestoreWorkerInterface is worker's communication workerInterface

	// Restore Roles
	Optional<RestoreLoaderInterface> loaderInterf;
	Reference<RestoreLoaderData> loaderData;
	Optional<RestoreApplierInterface> applierInterf;
	Reference<RestoreApplierData> applierData;
	Reference<RestoreMasterData> masterData;

	CMDUID cmdID;

	uint32_t inProgressFlag = 0; // To avoid race between duplicate message delivery that invokes the same actor multiple times
	std::map<CMDUID, int> processedCmd;

	UID id() const { return workerID; };

	RestoreWorkerData() {
		workerID = UID();
	}

	~RestoreWorkerData() {
		printf("[Exit] Worker:%s RestoreWorkerData is deleted\n", workerID.toString().c_str());
	}

	std::string describeNode() {
		std::stringstream ss;
		ss << "RestoreWorker workerID:" << workerID.toString();
		return ss.str();
	}

	bool isCmdProcessed(CMDUID const &cmdID) {
		return processedCmd.find(cmdID) != processedCmd.end();
	}

	// Helper functions to set/clear the flag when a worker is in the middle of processing an actor.
	void setInProgressFlag(RestoreCommandEnum phaseEnum) {
		int phase = (int) phaseEnum;
		ASSERT(phase < 32);
		inProgressFlag |= (1UL << phase);
	}

	void clearInProgressFlag(RestoreCommandEnum phaseEnum) {
		int phase = (int) phaseEnum;
		ASSERT(phase < 32);
		inProgressFlag &= ~(1UL << phase);
	}

	bool isInProgress(RestoreCommandEnum phaseEnum) {
		int phase = (int) phaseEnum;
		ASSERT(phase < 32);
		return (inProgressFlag & (1UL << phase));
	}

};

// Remove the worker interface from restoreWorkerKey and remove its roles interfaces from their keys.
ACTOR Future<Void> handlerTerminateWorkerRequest(RestoreSimpleRequest req, Reference<RestoreWorkerData> self, RestoreWorkerInterface workerInterf, Database cx) {
 	state Transaction tr(cx);
	
	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.clear(restoreWorkerKeyFor(workerInterf.id()));
			if ( self->loaderInterf.present() ) {
				tr.clear(restoreLoaderKeyFor(self->loaderInterf.get().id()));
			}
			if ( self->applierInterf.present() ) {
				tr.clear(restoreApplierKeyFor(self->applierInterf.get().id()));
			}
			wait( tr.commit() ) ;
			printf("Node:%s finish restore, clear the interface keys for all roles on the worker (id:%s) and the worker itself. Then exit\n", self->describeNode().c_str(),  workerInterf.id().toString().c_str()); 
			req.reply.send( RestoreCommonReply(workerInterf.id(), req.cmdID) );
			break;
		} catch( Error &e ) {
			printf("[WARNING] Node:%s finishRestoreHandler() transaction error:%s\n", self->describeNode().c_str(), e.what());
			wait( tr.onError(e) );
		}
	};

	return Void();
 }

// Periodically send worker heartbeat to 
 ACTOR Future<Void> monitorWorkerLiveness(Reference<RestoreWorkerData> self) {
	ASSERT( !self->workers_workerInterface.empty() );
	state int wIndex = 0;
	for (auto &workerInterf : self->workers_workerInterface) {
		printf("[Worker:%d][UID:%s][Interf.NodeInfo:%s]\n", wIndex, workerInterf.first.toString().c_str(), workerInterf.second.id().toString().c_str());
		wIndex++;
	}

	state std::vector<Future<RestoreCommonReply>> cmdReplies;
	state std::map<UID, RestoreWorkerInterface>::iterator workerInterf;
	loop {
		wIndex = 0;
		self->cmdID.initPhase(RestoreCommandEnum::Heart_Beat);
		for ( workerInterf = self->workers_workerInterface.begin(); workerInterf !=  self->workers_workerInterface.end(); workerInterf++)  {
			self->cmdID.nextCmd();
			try {
				wait( delay(1.0) );
				cmdReplies.push_back( workerInterf->second.heartbeat.getReply(RestoreSimpleRequest(self->cmdID)) );
				std::vector<RestoreCommonReply> reps = wait( timeoutError(getAll(cmdReplies), FastRestore_Failure_Timeout) );
				cmdReplies.clear();
				wIndex++;
			} catch (Error &e) {
				fprintf(stdout, "[ERROR] Node:%s, Commands before cmdID:%s error. error code:%d, error message:%s\n", self->describeNode().c_str(),
							self->cmdID.toString().c_str(), e.code(), e.what());
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

	// Debug
	//loadBatchSizeThresholdB = 1;
	//transactionBatchSizeThreshold = 1;

	printf("Init RestoreWorkerConfig. min_num_workers:%d ratio_loader_to_applier:%d loadBatchSizeMB:%.2f loadBatchSizeThresholdB:%.2f transactionBatchSizeThreshold:%.2f\n",
			MIN_NUM_WORKERS, ratio_loader_to_applier, loadBatchSizeMB, loadBatchSizeThresholdB, transactionBatchSizeThreshold);
}


// Restore Worker
ACTOR Future<Void> commitRestoreRoleInterfaces(Reference<RestoreWorkerData> self, Database cx) {
	state ReadYourWritesTransaction tr(cx);
	// For now, we assume only one role per restore worker
	ASSERT( !(self->loaderInterf.present() && self->applierInterf.present()) );

	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			ASSERT( !(self->loaderInterf.present() && self->applierInterf.present()) ); 
			if ( self->loaderInterf.present() ) {
				tr.set( restoreLoaderKeyFor(self->loaderInterf.get().id()), restoreLoaderInterfaceValue(self->loaderInterf.get()) );
			}
			if ( self->applierInterf.present() ) {
				tr.set( restoreApplierKeyFor(self->applierInterf.get().id()), restoreApplierInterfaceValue(self->applierInterf.get()) );
			}
			wait (tr.commit() );
			break;
		} catch( Error &e ) {
			printf("[WARNING]%s: commitRestoreRoleInterfaces transaction error:%s\n", self->describeNode().c_str(), e.what());
			wait( tr.onError(e) );
		}
	}

	return Void();
} 

// Restore Worker
ACTOR Future<Void> handleRecruitRoleRequest(RestoreRecruitRoleRequest req, Reference<RestoreWorkerData> self, ActorCollection *actors, Database cx) {
	printf("[INFO][Worker] Node:%s get role %s\n", self->describeNode().c_str(),
			getRoleStr(req.role).c_str());

	while (self->isInProgress(RestoreCommandEnum::Recruit_Role_On_Worker)) {
		printf("[DEBUG] NODE:%s handleRecruitRoleRequest wait for 1s\n",  self->describeNode().c_str());
		wait(delay(1.0));
	}
	if ( self->isCmdProcessed(req.cmdID) ) {
		req.reply.send(RestoreCommonReply(self->id(), req.cmdID));
		return Void();
	}
	self->setInProgressFlag(RestoreCommandEnum::Recruit_Role_On_Worker);

	if (req.role == RestoreRole::Loader) {
		ASSERT( !self->loaderInterf.present() );
		self->loaderInterf = RestoreLoaderInterface();
		self->loaderInterf.get().initEndpoints();
		RestoreLoaderInterface &recruited = self->loaderInterf.get();
		DUMPTOKEN(recruited.sampleRangeFile);
		DUMPTOKEN(recruited.sampleLogFile);
		DUMPTOKEN(recruited.setApplierKeyRangeVectorRequest);
		DUMPTOKEN(recruited.loadRangeFile);
		DUMPTOKEN(recruited.loadLogFile);
		DUMPTOKEN(recruited.initVersionBatch);
		DUMPTOKEN(recruited.collectRestoreRoleInterfaces);
		DUMPTOKEN(recruited.finishRestore);
		self->loaderData = Reference<RestoreLoaderData>( new RestoreLoaderData(self->loaderInterf.get().id(),  req.nodeIndex) );
		actors->add( restoreLoaderCore(self->loaderData, self->loaderInterf.get(), cx) );
	} else if (req.role == RestoreRole::Applier) {
		ASSERT( !self->applierInterf.present() );
		self->applierInterf = RestoreApplierInterface();
		self->applierInterf.get().initEndpoints();
		RestoreApplierInterface &recruited = self->applierInterf.get();
		DUMPTOKEN(recruited.calculateApplierKeyRange);
		DUMPTOKEN(recruited.getApplierKeyRangeRequest);
		DUMPTOKEN(recruited.setApplierKeyRangeRequest);
		DUMPTOKEN(recruited.sendSampleMutationVector);
		DUMPTOKEN(recruited.sendMutationVector);
		DUMPTOKEN(recruited.applyToDB);
		DUMPTOKEN(recruited.initVersionBatch);
		DUMPTOKEN(recruited.collectRestoreRoleInterfaces);
		DUMPTOKEN(recruited.finishRestore);
		self->applierData = Reference<RestoreApplierData>( new RestoreApplierData(self->applierInterf.get().id(), req.nodeIndex) );
		actors->add( restoreApplierCore(self->applierData, self->applierInterf.get(), cx) );
	} else {
		TraceEvent(SevError, "FastRestore").detail("HandleRecruitRoleRequest", "UnknownRole"); //.detail("Request", req.printable());
	}

	wait( commitRestoreRoleInterfaces(self, cx) ); // Commit the interface after the interface is ready to accept requests
	req.reply.send(RestoreCommonReply(self->id(), req.cmdID));
	self->processedCmd[req.cmdID] = 1;
	self->clearInProgressFlag(RestoreCommandEnum::Recruit_Role_On_Worker);

	return Void();
}


// Read restoreWorkersKeys from DB to get each restore worker's restore workerInterface and set it to self->workers_workerInterface
// This is done before we assign restore roles for restore workers
 ACTOR Future<Void> collectRestoreWorkerInterface(Reference<RestoreWorkerData> self, Database cx, int min_num_workers) {
	state Transaction tr(cx);

	state vector<RestoreWorkerInterface> agents; // agents is cmdsInterf
	
	loop {
		try {
			self->workers_workerInterface.clear();
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
					self->workers_workerInterface.insert(std::make_pair(agents.back().id(), agents.back()));
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

	TraceEvent("FastRestore").detail("CollectWorkerInterfaceNumWorkers", self->workers_workerInterface.size());

	return Void();
 }


// Keep only k restore workers and remove redundant restore workers
ACTOR Future<Void> removeRedundantRestoreWorkers(Reference<RestoreWorkerData> self, Database cx)  {
	printf("%s:Start configuring roles for workers\n", self->describeNode().c_str());
	ASSERT( self->masterData.isValid() );

	// Set up the role, and the global status for each node
	int numNodes = self->workers_workerInterface.size();
	state int numLoader = NUM_LOADERS; //numNodes * ratio_loader_to_applier / (ratio_loader_to_applier + 1);
	int numApplier = NUM_APPLIERS; //numNodes - numLoader;
	state int numWorkers = numLoader + numApplier;

	if ( numNodes == numWorkers ) {
		return Void();
	} else if ( numNodes < numWorkers ) {
		fprintf(stderr, "actual number_of_workers:%d < expected number_of_workers:%d\n", numNodes, numWorkers);
	}

	state int nodeIndex = 0;
	state UID nodeID;
	
	loop {
		try {
			std::vector<Future<RestoreCommonReply>> cmdReplies;
			nodeIndex = 0;
			printf("Node:%s Start remove %d redundant restore worker\n", self->describeNode().c_str(), self->workers_workerInterface.size() - numWorkers);
			self->cmdID.initPhase(RestoreCommandEnum::Remove_Redundant_Worker);
			for (auto &workerInterf : self->workers_workerInterface)  {
				if ( nodeIndex < numWorkers ) {
					nodeIndex++;
					continue;
				}
				nodeID = workerInterf.first;
				self->cmdID.nextCmd();
				printf("[CMD:%s] Node:%s Remove restore worker(index=%d uid=%s)\n", self->cmdID.toString().c_str(), self->describeNode().c_str(),
						nodeIndex, nodeID.toString().c_str());
				cmdReplies.push_back( workerInterf.second.terminateWorker.getReply(RestoreSimpleRequest(self->cmdID)) );
				nodeIndex++;
			}
			std::vector<RestoreCommonReply> reps = wait( timeoutError(getAll(cmdReplies), FastRestore_Failure_Timeout) );
			// Get the updated key-value for restore worker interfaces
			self->workers_workerInterface.clear();
			wait( collectRestoreWorkerInterface(self, cx) );
			if ( self->workers_workerInterface.size() == NUM_LOADERS + NUM_APPLIERS ) {
				printf("[RemoveRedundantWorkers] Finished\n");
				break;
			} else {
				printf("Redo removeRedundantRestoreWorkers. workers_workerInterface.size:%d, NUM_LOADERS:%d NUM_APPLIERS:%d\n",
					self->workers_workerInterface.size(), NUM_LOADERS, NUM_APPLIERS);
			}
		} catch (Error &e) {
			// Handle the command reply timeout error
			fprintf(stdout, "[ERROR] Node:%s, Commands before cmdID:%s error. error code:%d, error message:%s\n", self->describeNode().c_str(),
						self->cmdID.toString().c_str(), e.code(), e.what());
			printf("Node:%s waits on replies time out. Current phase: removeRedundantRestoreWorkers, Retry all commands.\n", self->describeNode().c_str());
			wait( delay(5.0) );
			self->workers_workerInterface.clear();
			wait( collectRestoreWorkerInterface(self, cx) );
		}
	}

	return Void();
}


// RestoreWorker that has restore master role: Recruite a role for each worker
ACTOR Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> self)  {
	printf("%s:Start configuring roles for workers\n", self->describeNode().c_str());
	ASSERT( self->masterData.isValid() );

	// Set up the role, and the global status for each node
	int numNodes = self->workers_workerInterface.size();
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
	state UID nodeID;
	printf("Node:%s Start configuring roles for workers\n", self->describeNode().c_str());
	loop {
		try {
			std::vector<Future<RestoreCommonReply>> cmdReplies;
			self->cmdID.initPhase(RestoreCommandEnum::Recruit_Role_On_Worker);
			printf("numLoader:%d, numApplier:%d, self->workers_workerInterface.size:%d\n", numLoader, numApplier, self->workers_workerInterface.size());
			ASSERT( numLoader + numApplier == self->workers_workerInterface.size() ); // We assign 1 role per worker for now
			for (auto &workerInterf : self->workers_workerInterface)  {
				if ( nodeIndex < numLoader ) {
					role = RestoreRole::Loader;
				} else {
					role = RestoreRole::Applier;
				}
				nodeID = workerInterf.first;
				self->cmdID.nextCmd();
				printf("[CMD:%s] Node:%s Set role (%s) to node (index=%d uid=%s)\n", self->cmdID.toString().c_str(), self->describeNode().c_str(),
						getRoleStr(role).c_str(), nodeIndex, nodeID.toString().c_str());
				cmdReplies.push_back( workerInterf.second.recruitRole.getReply(RestoreRecruitRoleRequest(self->cmdID, role, nodeIndex)) );
				nodeIndex++;
			}
			std::vector<RestoreCommonReply> reps = wait( timeoutError(getAll(cmdReplies), FastRestore_Failure_Timeout) );
			printf("[RecruitRestoreRoles] Finished\n");
			break;
		} catch (Error &e) {
			// Handle the command reply timeout error
			fprintf(stdout, "[ERROR] Node:%s, Commands before cmdID:%s error. error code:%d, error message:%s\n", self->describeNode().c_str(),
						self->cmdID.toString().c_str(), e.code(), e.what());
			printf("Node:%s waits on replies time out. Current phase: Set_Role, Retry all commands.\n", self->describeNode().c_str());
		}
	}

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

	wait( removeRedundantRestoreWorkers(self, cx) );

	state Future<Void> workersFailureMonitor = monitorWorkerLiveness(self);

	// recruitRestoreRoles must be after collectWorkerInterface
	wait( recruitRestoreRoles(self) );

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
				when ( RestoreSimpleRequest req = waitNext(interf.terminateWorker.getFuture()) ) {
					// Destroy the worker at the end of the restore
					// TODO: Cancel its own actors
					requestTypeStr = "terminateWorker";
					wait( handlerTerminateWorkerRequest(req, self, interf, cx) );
					return Void();
				}
			}

		} catch (Error &e) {
			fprintf(stdout, "[ERROR] Loader handle received request:%s error. error code:%d, error message:%s\n",
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
			//leaderWatch = tr.watch(restoreLeaderKey);
			wait( tr.commit() );
			leader->set(leaderInterf);
			//wait( leaderWatch );
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

