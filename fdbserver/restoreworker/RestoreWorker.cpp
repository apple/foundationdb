/*
 * RestoreWorker.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/BackupAgent.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/BackupContainer.h"
#include "flow/ApiVersion.h"
#include "flow/IAsyncFile.h"
#include "fdbrpc/simulator.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include "flow/ActorCollection.h"
#include "RestoreWorker.h"
#include "RestoreLoader.h"
#include "RestoreApplier.h"
#include "RestoreController.h"
#include "fdbrpc/SimulatorProcessInfo.h"

#include "flow/CoroUtils.h"

class RestoreConfigFR;
struct RestoreWorkerData; // Only declare the struct exist but we cannot use its field

Future<Void> handlerTerminateWorkerRequest(RestoreSimpleRequest req,
                                           Reference<RestoreWorkerData> self,
                                           RestoreWorkerInterface workerInterf,
                                           Database cx);
Future<Void> monitorWorkerLiveness(Reference<RestoreWorkerData> self);
void handleRecruitRoleRequest(RestoreRecruitRoleRequest req,
                              Reference<RestoreWorkerData> self,
                              ActorCollection* actors,
                              Database cx);
Future<Void> collectRestoreWorkerInterface(Reference<RestoreWorkerData> self, Database cx, int min_num_workers = 2);
Future<Void> monitorleader(Reference<AsyncVar<RestoreWorkerInterface>> leader,
                           Database cx,
                           RestoreWorkerInterface myWorkerInterf);
Future<Void> startRestoreWorkerLeader(Reference<RestoreWorkerData> self,
                                      RestoreWorkerInterface workerInterf,
                                      Database cx);

// Remove the worker interface from restoreWorkerKey and remove its roles interfaces from their keys.
Future<Void> handlerTerminateWorkerRequest(RestoreSimpleRequest req,
                                           Reference<RestoreWorkerData> self,
                                           RestoreWorkerInterface workerInterf,
                                           Database cx) {
	ReadYourWritesTransaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.clear(restoreWorkerKeyFor(workerInterf.id()));
			co_await tr.commit();
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}

	TraceEvent("FastRestoreWorker").detail("HandleTerminateWorkerReq", self->id());
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
Future<Void> collectRestoreWorkerInterface(Reference<RestoreWorkerData> self, Database cx, int min_num_workers) {
	Transaction tr(cx);
	std::vector<RestoreWorkerInterface> agents; // agents is cmdsInterf

	while (true) {
		Error err;
		try {
			self->workerInterfaces.clear();
			agents.clear();
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeResult agentValues = co_await tr.getRange(restoreWorkersKeys, CLIENT_KNOBS->TOO_MANY);
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
			co_await delay(5.0);
			continue;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
	ASSERT(agents.size() >= min_num_workers); // ASSUMPTION: We must have at least 1 loader and 1 applier

	TraceEvent("FastRestoreWorker").detail("CollectWorkerInterfaceNumWorkers", self->workerInterfaces.size());
}

// Periodically send worker heartbeat to
Future<Void> monitorWorkerLiveness(Reference<RestoreWorkerData> self) {
	ASSERT(!self->workerInterfaces.empty());

	while (true) {
		std::vector<std::pair<UID, RestoreSimpleRequest>> requests;
		for (auto& worker : self->workerInterfaces) {
			requests.emplace_back(worker.first, RestoreSimpleRequest());
		}
		co_await sendBatchRequests(&RestoreWorkerInterface::heartbeat, self->workerInterfaces, requests);
		co_await delay(60.0);
	}
}

// RestoreWorkerLeader is the worker that runs RestoreController role
Future<Void> startRestoreWorkerLeader(Reference<RestoreWorkerData> self,
                                      RestoreWorkerInterface workerInterf,
                                      Database cx) {
	// We must wait for enough time to make sure all restore workers have registered their workerInterfaces into the DB
	TraceEvent("FastRestoreWorker")
	    .detail("Controller", workerInterf.id())
	    .detail("WaitForRestoreWorkerInterfaces",
	            SERVER_KNOBS->FASTRESTORE_NUM_LOADERS + SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS);
	co_await delay(10.0);
	TraceEvent("FastRestoreWorker")
	    .detail("Controller", workerInterf.id())
	    .detail("CollectRestoreWorkerInterfaces",
	            SERVER_KNOBS->FASTRESTORE_NUM_LOADERS + SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS);

	co_await collectRestoreWorkerInterface(
	    self, cx, SERVER_KNOBS->FASTRESTORE_NUM_LOADERS + SERVER_KNOBS->FASTRESTORE_NUM_APPLIERS);

	// TODO: Needs to keep this monitor's future. May use actorCollection
	Future<Void> workersFailureMonitor = monitorWorkerLiveness(self);

	RestoreControllerInterface recruited;
	DUMPTOKEN(recruited.samples);

	self->controllerInterf = recruited;
	co_await (startRestoreController(self, cx) || workersFailureMonitor);
}

Future<Void> startRestoreWorker(Reference<RestoreWorkerData> self, RestoreWorkerInterface interf, Database cx) {
	double lastLoopTopTime = now();
	ActorCollection actors(false); // Collect the main actor for each role
	Future<Void> exitRole = Never();

	while (true) {
		double loopTopTime = now();
		double elapsedTime = loopTopTime - lastLoopTopTime;
		if (elapsedTime > 0.050) {
			if (deterministicRandom()->random01() < 0.01)
				TraceEvent(SevWarn, "SlowRestoreWorkerLoopx100")
				    .detail("NodeDesc", self->describeNode())
				    .detail("Elapsed", elapsedTime);
		}
		lastLoopTopTime = loopTopTime;
		std::string requestTypeStr = "[Init]";

		try {
			auto res = co_await race(interf.heartbeat.getFuture(),
			                         interf.recruitRole.getFuture(),
			                         interf.terminateWorker.getFuture(),
			                         exitRole);
			if (res.index() == 0) {
				RestoreSimpleRequest req = std::get<0>(std::move(res));

				requestTypeStr = "heartbeat";
				actors.add(handleHeartbeat(req, interf.id()));
			} else if (res.index() == 1) {
				RestoreRecruitRoleRequest req = std::get<1>(std::move(res));

				requestTypeStr = "recruitRole";
				handleRecruitRoleRequest(req, self, &actors, cx);
			} else if (res.index() == 2) {
				RestoreSimpleRequest req = std::get<2>(std::move(res));

				// Destroy the worker at the end of the restore
				requestTypeStr = "terminateWorker";
				exitRole = handlerTerminateWorkerRequest(req, self, interf, cx);
			} else if (res.index() == 3) {
				TraceEvent("FastRestoreWorkerCoreExitRole", self->id());
				break;
			} else {
				UNREACHABLE();
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "FastRestoreWorkerError").errorUnsuppressed(e).detail("RequestType", requestTypeStr);
			break;
		}
	}
}

static Future<Void> waitOnRestoreRequests(Database cx, UID nodeID = UID()) {
	ReadYourWritesTransaction tr(cx);
	Optional<Value> numRequests;

	// wait for the restoreRequestTriggerKey to be set by the client/test workload
	TraceEvent("FastRestoreWaitOnRestoreRequest", nodeID).log();
	while (true) {
		Error err;
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			numRequests = co_await tr.get(restoreRequestTriggerKey);
			if (!numRequests.present()) {
				Future<Void> watchForRestoreRequest = tr.watch(restoreRequestTriggerKey);
				co_await tr.commit();
				TraceEvent(SevInfo, "FastRestoreWaitOnRestoreRequestTriggerKey", nodeID).log();
				co_await watchForRestoreRequest;
				TraceEvent(SevInfo, "FastRestoreDetectRestoreRequestTriggerKeyChanged", nodeID).log();
				continue;
			} else {
				TraceEvent(SevInfo, "FastRestoreRestoreRequestTriggerKey", nodeID)
				    .detail("TriggerKey", numRequests.get().toString());
				break;
			}
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// RestoreController is the leader
Future<Void> monitorleader(Reference<AsyncVar<RestoreWorkerInterface>> leader,
                           Database cx,
                           RestoreWorkerInterface myWorkerInterf) {
	co_await delay(SERVER_KNOBS->FASTRESTORE_MONITOR_LEADER_DELAY);
	TraceEvent("FastRestoreWorker", myWorkerInterf.id()).detail("MonitorLeader", "StartLeaderElection");
	int count = 0;
	RestoreWorkerInterface leaderInterf;
	ReadYourWritesTransaction tr(cx); // MX: Somewhere here program gets stuck
	while (true) {
		Error err;
		try {
			count++;
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> leaderValue = co_await tr.get(restoreLeaderKey);
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
			co_await tr.commit();
			leader->set(leaderInterf);
			break;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent(SevInfo, "FastRestoreLeaderElection").detail("ErrorCode", err.code()).detail("Error", err.what());
		co_await tr.onError(err);
	}

	TraceEvent("FastRestoreWorker", myWorkerInterf.id())
	    .detail("MonitorLeader", "FinishLeaderElection")
	    .detail("Leader", leaderInterf.id())
	    .detail("IamLeader", leaderInterf == myWorkerInterf);
}

Future<Void> _restoreWorker(Database cx, LocalityData locality) {
	Future<Void> myWork = Never();
	auto leader = makeReference<AsyncVar<RestoreWorkerInterface>>();
	RestoreWorkerInterface myWorkerInterf;
	auto self = makeReference<RestoreWorkerData>();

	myWorkerInterf.initEndpoints();
	self->workerID = myWorkerInterf.id();

	// Protect restore worker from being killed in simulation;
	// Future: Remove the protection once restore can tolerate failure
	if (g_network->isSimulated()) {
		auto addresses = g_simulator->getProcessByAddress(myWorkerInterf.address())->addresses;

		g_simulator->protectedAddresses.insert(addresses.address);
		if (addresses.secondaryAddress.present()) {
			g_simulator->protectedAddresses.insert(addresses.secondaryAddress.get());
		}
		ISimulator::ProcessInfo* p = g_simulator->getProcessByAddress(myWorkerInterf.address());
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

	co_await waitOnRestoreRequests(cx, myWorkerInterf.id());

	co_await monitorleader(leader, cx, myWorkerInterf);

	TraceEvent("FastRestoreWorker", myWorkerInterf.id()).detail("LeaderElection", "WaitForLeader");
	if (leader->get() == myWorkerInterf) {
		// Restore controller worker: doLeaderThings();
		myWork = startRestoreWorkerLeader(self, myWorkerInterf, cx);
	} else {
		// Restore normal worker (for RestoreLoader and RestoreApplier roles): doWorkerThings();
		myWork = startRestoreWorker(self, myWorkerInterf, cx);
	}

	co_await myWork;
}

Future<Void> restoreWorker(Reference<IClusterConnectionRecord> connRecord,
                           LocalityData locality,
                           std::string coordFolder) {
	try {
		Database cx = Database::createDatabase(connRecord, ApiVersion::LATEST_VERSION, IsInternal::True, locality);
		co_await reportErrors(_restoreWorker(cx, locality), "RestoreWorker");
	} catch (Error& e) {
		TraceEvent("FastRestoreWorker").detail("Error", e.what());
		throw e;
	}
}
