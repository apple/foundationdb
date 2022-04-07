/*
 * RestoreWorker.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "fdbrpc/simulator.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include "flow/ActorCollection.h"
#include "fdbserver/RestoreWorker.actor.h"
#include "fdbserver/RestoreController.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

class RestoreConfigFR;
struct RestoreWorkerData; // Only declare the struct exist but we cannot use its field

ACTOR Future<Void> handlerTerminateWorkerRequest(RestoreSimpleRequest req,
                                                 Reference<RestoreWorkerData> self,
                                                 RestoreWorkerInterface workerInterf,
                                                 Database cx);
ACTOR Future<Void> monitorWorkerLiveness(Reference<RestoreWorkerData> self);
void handleRecruitRoleRequest(RestoreRecruitRoleRequest req,
                              Reference<RestoreWorkerData> self,
                              ActorCollection* actors,
                              Database cx);
ACTOR Future<Void> collectRestoreWorkerInterface(Reference<RestoreWorkerData> self,
                                                 Database cx,
                                                 int min_num_workers = 2);
ACTOR Future<Void> monitorleader(Reference<AsyncVar<RestoreWorkerInterface>> leader,
                                 Database cx,
                                 RestoreWorkerInterface myWorkerInterf);
ACTOR Future<Void> startRestoreWorkerLeader(Reference<RestoreWorkerData> self,
                                            RestoreWorkerInterface workerInterf,
                                            Database cx);

// Remove the worker interface from restoreWorkerKey and remove its roles interfaces from their keys.
ACTOR Future<Void> handlerTerminateWorkerRequest(RestoreSimpleRequest req,
                                                 Reference<RestoreWorkerData> self,
                                                 RestoreWorkerInterface workerInterf,
                                                 Database cx) {
	wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->clear(restoreWorkerKeyFor(workerInterf.id()));
		return Void();
	}));

	TraceEvent("FastRestoreWorker").detail("HandleTerminateWorkerReq", self->id());

	return Void();
}

// Assume only 1 role on a restore worker.
// Future: Multiple roles in a restore worker
void handleRecruitRoleRequest(RestoreRecruitRoleRequest req,
                              Reference<RestoreWorkerData> self,
                              ActorCollection* actors,
                              Database cx) {
	// Future: Allow multiple restore roles on a restore worker. The design should easily allow this.
	ASSERT(!self->loaderInterf.present() || !self->applierInterf.present()); // Only one role per worker for now
	// Already recruited a role
	if (self->loaderInterf.present()) {
		ASSERT(req.role == RestoreRole::Loader);
		req.reply.send(RestoreRecruitRoleReply(self->id(), RestoreRole::Loader, self->loaderInterf.get()));
		return;
	} else if (self->applierInterf.present()) {
		req.reply.send(RestoreRecruitRoleReply(self->id(), RestoreRole::Applier, self->applierInterf.get()));
		return;
	}

	if (req.role == RestoreRole::Loader) {
		ASSERT(!self->loaderInterf.present());
		self->controllerInterf = req.ci;
		self->loaderInterf = RestoreLoaderInterface();
		self->loaderInterf.get().initEndpoints();
		RestoreLoaderInterface& recruited = self->loaderInterf.get();
		DUMPTOKEN(recruited.heartbeat);
		DUMPTOKEN(recruited.updateRestoreSysInfo);
		DUMPTOKEN(recruited.initVersionBatch);
		DUMPTOKEN(recruited.loadFile);
		DUMPTOKEN(recruited.sendMutations);
		DUMPTOKEN(recruited.initVersionBatch);
		DUMPTOKEN(recruited.finishVersionBatch);
		DUMPTOKEN(recruited.collectRestoreRoleInterfaces);
		DUMPTOKEN(recruited.finishRestore);
		actors->add(restoreLoaderCore(self->loaderInterf.get(), req.nodeIndex, cx, req.ci));
		TraceEvent("FastRestoreWorker").detail("RecruitedLoaderNodeIndex", req.nodeIndex);
		req.reply.send(
		    RestoreRecruitRoleReply(self->loaderInterf.get().id(), RestoreRole::Loader, self->loaderInterf.get()));
	} else if (req.role == RestoreRole::Applier) {
		ASSERT(!self->applierInterf.present());
		self->controllerInterf = req.ci;
		self->applierInterf = RestoreApplierInterface();
		self->applierInterf.get().initEndpoints();
		RestoreApplierInterface& recruited = self->applierInterf.get();
		DUMPTOKEN(recruited.heartbeat);
		DUMPTOKEN(recruited.sendMutationVector);
		DUMPTOKEN(recruited.applyToDB);
		DUMPTOKEN(recruited.initVersionBatch);
		DUMPTOKEN(recruited.collectRestoreRoleInterfaces);
		DUMPTOKEN(recruited.finishRestore);
		actors->add(restoreApplierCore(self->applierInterf.get(), req.nodeIndex, cx));
		TraceEvent("FastRestoreWorker").detail("RecruitedApplierNodeIndex", req.nodeIndex);
		req.reply.send(
		    RestoreRecruitRoleReply(self->applierInterf.get().id(), RestoreRole::Applier, self->applierInterf.get()));
	} else {
		TraceEvent(SevError, "FastRestoreWorkerHandleRecruitRoleRequestUnknownRole").detail("Request", req.toString());
	}

	return;
}

// Read restoreWorkersKeys from DB to get each restore worker's workerInterface and set it to self->workerInterfaces;
// This is done before we assign restore roles for restore workers.
ACTOR Future<Void> collectRestoreWorkerInterface(Reference<RestoreWorkerData> self, Database cx, int min_num_workers) {
	state Transaction tr(cx);
	state std::vector<RestoreWorkerInterface> agents; // agents is cmdsInterf

	loop {
		try {
			self->workerInterfaces.clear();
			agents.clear();
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeResult agentValues = wait(tr.getRange(restoreWorkersKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!agentValues.more);
			// If agentValues.size() < min_num_workers, we should wait for coming workers to register their
			// workerInterface before we read them once for all
			if (agentValues.size() >= min_num_workers) {
				for (auto& it : agentValues) {
					agents.push_back(BinaryReader::fromStringRef<RestoreWorkerInterface>(it.value, IncludeVersion()));
					// Save the RestoreWorkerInterface for the later operations
					self->workerInterfaces.insert(std::make_pair(agents.back().id(), agents.back()));
				}
				break;
			}
			TraceEvent("FastRestoreWorker")
			    .suppressFor(10.0)
			    .detail("NotEnoughWorkers", agentValues.size())
			    .detail("MinWorkers", min_num_workers);
			wait(delay(5.0));
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	ASSERT(agents.size() >= min_num_workers); // ASSUMPTION: We must have at least 1 loader and 1 applier

	TraceEvent("FastRestoreWorker").detail("CollectWorkerInterfaceNumWorkers", self->workerInterfaces.size());

	return Void();
}

// Periodically send worker heartbeat to
ACTOR Future<Void> monitorWorkerLiveness(Reference<RestoreWorkerData> self) {
	ASSERT(!self->workerInterfaces.empty());

	state std::map<UID, RestoreWorkerInterface>::iterator workerInterf;
	loop {
		std::vector<std::pair<UID, RestoreSimpleRequest>> requests;
		for (auto& worker : self->workerInterfaces) {
			requests.emplace_back(worker.first, RestoreSimpleRequest());
		}
		wait(sendBatchRequests(&RestoreWorkerInterface::heartbeat, self->workerInterfaces, requests));
		wait(delay(60.0));
	}
}

// RestoreWorkerLeader is the worker that runs RestoreController role
ACTOR Future<Void> startRestoreWorkerLeader(Reference<RestoreWorkerData> self,
                                            RestoreWorkerInterface workerInterf,
                                            Database cx) {
	// We must wait for enough time to make sure all restore workers have registered their workerInterfaces into the DB
	TraceEvent("FastRestoreWorker")
	    .detail("Controller", workerInterf.id())
	    .detail("WaitForRestoreWorkerInterfaces",
	            SERVER_KNOBS->FASTRESTORE_NUM_LOADERS + SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS);
	wait(delay(10.0));
	TraceEvent("FastRestoreWorker")
	    .detail("Controller", workerInterf.id())
	    .detail("CollectRestoreWorkerInterfaces",
	            SERVER_KNOBS->FASTRESTORE_NUM_LOADERS + SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS);

	wait(collectRestoreWorkerInterface(
	    self, cx, SERVER_KNOBS->FASTRESTORE_NUM_LOADERS + SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS));

	// TODO: Needs to keep this monitor's future. May use actorCollection
	state Future<Void> workersFailureMonitor = monitorWorkerLiveness(self);

	RestoreControllerInterface recruited;
	DUMPTOKEN(recruited.samples);

	self->controllerInterf = recruited;
	wait(startRestoreController(self, cx) || workersFailureMonitor);

	return Void();
}

ACTOR Future<Void> startRestoreWorker(Reference<RestoreWorkerData> self, RestoreWorkerInterface interf, Database cx) {
	state double lastLoopTopTime;
	state ActorCollection actors(false); // Collect the main actor for each role
	state Future<Void> exitRole = Never();

	loop {
		double loopTopTime = now();
		double elapsedTime = loopTopTime - lastLoopTopTime;
		if (elapsedTime > 0.050) {
			if (deterministicRandom()->random01() < 0.01)
				TraceEvent(SevWarn, "SlowRestoreWorkerLoopx100")
				    .detail("NodeDesc", self->describeNode())
				    .detail("Elapsed", elapsedTime);
		}
		lastLoopTopTime = loopTopTime;
		state std::string requestTypeStr = "[Init]";

		try {
			choose {
				when(RestoreSimpleRequest req = waitNext(interf.heartbeat.getFuture())) {
					requestTypeStr = "heartbeat";
					actors.add(handleHeartbeat(req, interf.id()));
				}
				when(RestoreRecruitRoleRequest req = waitNext(interf.recruitRole.getFuture())) {
					requestTypeStr = "recruitRole";
					handleRecruitRoleRequest(req, self, &actors, cx);
				}
				when(RestoreSimpleRequest req = waitNext(interf.terminateWorker.getFuture())) {
					// Destroy the worker at the end of the restore
					requestTypeStr = "terminateWorker";
					exitRole = handlerTerminateWorkerRequest(req, self, interf, cx);
				}
				when(wait(exitRole)) {
					TraceEvent("FastRestoreWorkerCoreExitRole", self->id());
					break;
				}
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "FastRestoreWorkerError").errorUnsuppressed(e).detail("RequestType", requestTypeStr);
			break;
		}
	}

	return Void();
}

ACTOR static Future<Void> waitOnRestoreRequests(Database cx, UID nodeID = UID()) {
	state ReadYourWritesTransaction tr(cx);
	state Optional<Value> numRequests;

	// wait for the restoreRequestTriggerKey to be set by the client/test workload
	TraceEvent("FastRestoreWaitOnRestoreRequest", nodeID).log();
	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> _numRequests = wait(tr.get(restoreRequestTriggerKey));
			numRequests = _numRequests;
			if (!numRequests.present()) {
				state Future<Void> watchForRestoreRequest = tr.watch(restoreRequestTriggerKey);
				wait(tr.commit());
				TraceEvent(SevInfo, "FastRestoreWaitOnRestoreRequestTriggerKey", nodeID).log();
				wait(watchForRestoreRequest);
				TraceEvent(SevInfo, "FastRestoreDetectRestoreRequestTriggerKeyChanged", nodeID).log();
			} else {
				TraceEvent(SevInfo, "FastRestoreRestoreRequestTriggerKey", nodeID)
				    .detail("TriggerKey", numRequests.get().toString());
				break;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return Void();
}

// RestoreController is the leader
ACTOR Future<Void> monitorleader(Reference<AsyncVar<RestoreWorkerInterface>> leader,
                                 Database cx,
                                 RestoreWorkerInterface myWorkerInterf) {
	wait(delay(SERVER_KNOBS->FASTRESTORE_MONITOR_LEADER_DELAY));
	TraceEvent("FastRestoreWorker", myWorkerInterf.id()).detail("MonitorLeader", "StartLeaderElection");
	state int count = 0;
	state RestoreWorkerInterface leaderInterf;
	state ReadYourWritesTransaction tr(cx); // MX: Somewhere here program gets stuck
	loop {
		try {
			count++;
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> leaderValue = wait(tr.get(restoreLeaderKey));
			TraceEvent(SevInfo, "FastRestoreLeaderElection")
			    .detail("Round", count)
			    .detail("LeaderExisted", leaderValue.present());
			if (leaderValue.present()) {
				leaderInterf = BinaryReader::fromStringRef<RestoreWorkerInterface>(leaderValue.get(), IncludeVersion());
				// Register my interface as an worker if I am not the leader
				if (leaderInterf != myWorkerInterf) {
					tr.set(restoreWorkerKeyFor(myWorkerInterf.id()), restoreWorkerInterfaceValue(myWorkerInterf));
				}
			} else {
				// Workers compete to be the leader
				tr.set(restoreLeaderKey,
				       BinaryWriter::toValue(myWorkerInterf,
				                             IncludeVersion(ProtocolVersion::withRestoreWorkerInterfaceValue())));
				leaderInterf = myWorkerInterf;
			}
			wait(tr.commit());
			leader->set(leaderInterf);
			break;
		} catch (Error& e) {
			TraceEvent(SevInfo, "FastRestoreLeaderElection").detail("ErrorCode", e.code()).detail("Error", e.what());
			wait(tr.onError(e));
		}
	}

	TraceEvent("FastRestoreWorker", myWorkerInterf.id())
	    .detail("MonitorLeader", "FinishLeaderElection")
	    .detail("Leader", leaderInterf.id())
	    .detail("IamLeader", leaderInterf == myWorkerInterf);
	return Void();
}

ACTOR Future<Void> _restoreWorker(Database cx, LocalityData locality) {
	state ActorCollection actors(false);
	state Future<Void> myWork = Never();
	state Reference<AsyncVar<RestoreWorkerInterface>> leader = makeReference<AsyncVar<RestoreWorkerInterface>>();
	state RestoreWorkerInterface myWorkerInterf;
	state Reference<RestoreWorkerData> self = makeReference<RestoreWorkerData>();

	myWorkerInterf.initEndpoints();
	self->workerID = myWorkerInterf.id();

	// Protect restore worker from being killed in simulation;
	// Future: Remove the protection once restore can tolerate failure
	if (g_network->isSimulated()) {
		auto addresses = g_simulator.getProcessByAddress(myWorkerInterf.address())->addresses;

		g_simulator.protectedAddresses.insert(addresses.address);
		if (addresses.secondaryAddress.present()) {
			g_simulator.protectedAddresses.insert(addresses.secondaryAddress.get());
		}
		ISimulator::ProcessInfo* p = g_simulator.getProcessByAddress(myWorkerInterf.address());
		TraceEvent("ProtectRestoreWorker")
		    .detail("Address", addresses.toString())
		    .detail("IsReliable", p->isReliable())
		    .detail("ReliableInfo", p->getReliableInfo())
		    .backtrace();
		ASSERT(p->isReliable());
	}

	TraceEvent("FastRestoreWorkerKnobs", myWorkerInterf.id())
	    .detail("FailureTimeout", SERVER_KNOBS->FASTRESTORE_FAILURE_TIMEOUT)
	    .detail("HeartBeat", SERVER_KNOBS->FASTRESTORE_HEARTBEAT_INTERVAL)
	    .detail("SamplePercentage", SERVER_KNOBS->FASTRESTORE_SAMPLING_PERCENT)
	    .detail("NumLoaders", SERVER_KNOBS->FASTRESTORE_NUM_LOADERS)
	    .detail("NumAppliers", SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS)
	    .detail("TxnBatchSize", SERVER_KNOBS->FASTRESTORE_TXN_BATCH_MAX_BYTES)
	    .detail("VersionBatchSize", SERVER_KNOBS->FASTRESTORE_VERSIONBATCH_MAX_BYTES);

	wait(waitOnRestoreRequests(cx, myWorkerInterf.id()));

	wait(monitorleader(leader, cx, myWorkerInterf));

	TraceEvent("FastRestoreWorker", myWorkerInterf.id()).detail("LeaderElection", "WaitForLeader");
	if (leader->get() == myWorkerInterf) {
		// Restore controller worker: doLeaderThings();
		myWork = startRestoreWorkerLeader(self, myWorkerInterf, cx);
	} else {
		// Restore normal worker (for RestoreLoader and RestoreApplier roles): doWorkerThings();
		myWork = startRestoreWorker(self, myWorkerInterf, cx);
	}

	wait(myWork);
	return Void();
}

ACTOR Future<Void> restoreWorker(Reference<IClusterConnectionRecord> connRecord,
                                 LocalityData locality,
                                 std::string coordFolder) {
	try {
		Database cx = Database::createDatabase(connRecord, Database::API_VERSION_LATEST, IsInternal::True, locality);
		wait(reportErrors(_restoreWorker(cx, locality), "RestoreWorker"));
	} catch (Error& e) {
		TraceEvent("FastRestoreWorker").detail("Error", e.what());
		throw e;
	}

	return Void();
}
