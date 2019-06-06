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

#include <ctime>
#include <climits>
#include <numeric>
#include <algorithm>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/BackupContainer.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include "flow/ActorCollection.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreWorkerInterface.actor.h"
#include "fdbserver/RestoreCommon.actor.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreLoader.actor.h"
#include "fdbserver/RestoreApplier.actor.h"
#include "fdbserver/RestoreMaster.actor.h"

#include "flow/actorcompiler.h"  // This must be the last #include.


FastRestoreOpConfig opConfig;

int NUM_APPLIERS = 40;

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

template<> Tuple Codec<ERestoreState>::pack(ERestoreState const &val);
template<> ERestoreState Codec<ERestoreState>::unpack(Tuple const &val);

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
		return Void();
  	}) );

	TraceEvent("FastRestore").detail("HandleTerminateWorkerReq", self->id());

	return Void();
 }

// Assume only 1 role on a restore worker.
// Future: Multiple roles in a restore worker
ACTOR Future<Void> handleRecruitRoleRequest(RestoreRecruitRoleRequest req, Reference<RestoreWorkerData> self, ActorCollection *actors, Database cx) {
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
		DUMPTOKEN(recruited.initVersionBatch);
		DUMPTOKEN(recruited.collectRestoreRoleInterfaces);
		DUMPTOKEN(recruited.finishRestore);
		self->loaderData = Reference<RestoreLoaderData>( new RestoreLoaderData(self->loaderInterf.get().id(),  req.nodeIndex) );
		actors->add( restoreLoaderCore(self->loaderData, self->loaderInterf.get(), cx) );
		TraceEvent("FastRestore").detail("LoaderRecruited", self->loaderData->id());
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
		TraceEvent("FastRestore").detail("ApplierRecruited", self->applierData->id());
		req.reply.send(RestoreRecruitRoleReply(self->id(),  RestoreRole::Applier, self->applierInterf.get()));
	} else {
		TraceEvent(SevError, "FastRestore").detail("HandleRecruitRoleRequest", "UnknownRole"); //.detail("Request", req.printable());
	}

	return Void();
}

// Assume: Only update the local data if it (applierInterf) has not been set
ACTOR Future<Void> handleRestoreSysInfoRequest(RestoreSysInfoRequest req, Reference<RestoreWorkerData> self) {
	TraceEvent("FastRestore").detail("HandleRestoreSysInfoRequest", self->id());
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
				}
				break;
			}
			wait( delay(5.0) );
		} catch( Error &e ) {
			wait( tr.onError(e) );
		}
	}
	ASSERT(agents.size() >= min_num_workers); // ASSUMPTION: We must have at least 1 loader and 1 applier

	TraceEvent("FastRestore").detail("CollectWorkerInterfaceNumWorkers", self->workerInterfaces.size());

	return Void();
 }
 

// Periodically send worker heartbeat to 
 ACTOR Future<Void> monitorWorkerLiveness(Reference<RestoreWorkerData> self) {
	ASSERT( !self->workerInterfaces.empty() );

	state std::map<UID, RestoreWorkerInterface>::iterator workerInterf;
	loop {
		std::vector<std::pair<UID, RestoreSimpleRequest>> requests;
		for (auto& worker : self->workerInterfaces) {
			requests.push_back(std::make_pair(worker.first, RestoreSimpleRequest()));
		}
		wait( sendBatchRequests(&RestoreWorkerInterface::heartbeat, self->workerInterfaces, requests) );
		wait( delay(60.0) );
	}
 }

void initRestoreWorkerConfig() {
	opConfig.num_loaders = g_network->isSimulated() ? 3 : opConfig.num_loaders;
	opConfig.num_appliers = g_network->isSimulated() ? 3 : opConfig.num_appliers;
	opConfig.transactionBatchSizeThreshold = g_network->isSimulated() ? 512 : opConfig.transactionBatchSizeThreshold; // Byte
	TraceEvent("FastRestore").detail("InitOpConfig", "Result")
			.detail("NumLoaders", opConfig.num_loaders).detail("NumAppliers", opConfig.num_appliers)
			.detail("TxnBatchSize", opConfig.transactionBatchSizeThreshold);
}

// RestoreWorker that has restore master role: Recruite a role for each worker
ACTOR Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> self)  {
	TraceEvent("FastRestore").detail("RecruitRestoreRoles", self->workerInterfaces.size())
			.detail("NumLoaders", opConfig.num_loaders).detail("NumAppliers", opConfig.num_appliers);

	ASSERT( self->masterData.isValid() );
	ASSERT( opConfig.num_loaders > 0 && opConfig.num_appliers > 0 );
	ASSERT( opConfig.num_loaders + opConfig.num_appliers <= self->workerInterfaces.size() ); // We assign 1 role per worker for now
	
	// Assign a role to each worker
	state int nodeIndex = 0;
	state RestoreRole role;
	std::map<UID, RestoreRecruitRoleRequest> requests;
	for (auto &workerInterf : self->workerInterfaces)  {
		if ( nodeIndex >= 0 && nodeIndex < opConfig.num_appliers ) { 
			// [0, numApplier) are appliers
			role = RestoreRole::Applier;
		} else if ( nodeIndex >= opConfig.num_appliers && nodeIndex < opConfig.num_loaders + opConfig.num_appliers ) {
			// [numApplier, numApplier + numLoader) are loaders
			role = RestoreRole::Loader;
		}

		TraceEvent("FastRestore").detail("Role", getRoleStr(role)).detail("WorkerNode", workerInterf.first);
		requests[workerInterf.first] = RestoreRecruitRoleRequest(role, nodeIndex);
		nodeIndex++;
	}
	
	state std::vector<RestoreRecruitRoleReply> replies;
	wait( getBatchReplies(&RestoreWorkerInterface::recruitRole, self->workerInterfaces, requests, &replies) );
	for (auto& reply : replies) {
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
	TraceEvent("FastRestore").detail("RecruitRestoreRolesDone", self->workerInterfaces.size());

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
	
	TraceEvent("FastRestore").detail("DistributeRestoreSysInfo", self->workerInterfaces.size());
	wait( sendBatchRequests(&RestoreWorkerInterface::updateRestoreSysInfo, self->workerInterfaces, requests) );
	
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

	wait( collectRestoreWorkerInterface(self, cx, opConfig.num_loaders + opConfig.num_appliers) );

	// TODO: Needs to keep this monitor's future. May use actorCollection
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
	state Future<Void> exitRole = Never();
	
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
					requestTypeStr = "terminateWorker";
					exitRole = handlerTerminateWorkerRequest(req, self, interf, cx);
				}
				when ( wait(exitRole) ) {
					TraceEvent("FastRestore").detail("RestoreWorkerCore", "ExitRole").detail("NodeID", self->id());
					break;
				}
			}
		} catch (Error &e) {
			TraceEvent(SevWarn, "FastRestore").detail("RestoreWorkerError", e.what()).detail("RequestType", requestTypeStr);
			break;
			// if ( requestTypeStr.find("[Init]") != std::string::npos ) {
			// 	TraceEvent(SevError, "FastRestore").detail("RestoreWorkerUnexpectedExit", "RequestType_Init");
			// 	break;
			// }
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
	initRestoreWorkerConfig();

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

