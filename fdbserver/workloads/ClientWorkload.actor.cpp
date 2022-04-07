/*
 * ClientWorkload.actor.cpp
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

#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"

#include <fmt/format.h>

#include "flow/actorcompiler.h" // has to be last include

class WorkloadProcessState {
	IPAddress childAddress;
	std::string processName;
	Future<Void> init;

	WorkloadProcessState(int clientId) : clientId(clientId) {
		auto self = g_simulator.getCurrentProcess();
		if (self->address.isV6()) {
			childAddress =
			    IPAddress::parse(fmt::format("2001:fdb1:fdb2:fdb3:fdb4:fdb5:fdb6:{:04x}", clientId + 2)).get();
		} else {
			childAddress = IPAddress::parse(fmt::format("192.168.0.{}", clientId + 2)).get();
		}
		//TraceEvent("TestClientStart", id).detail("ClusterFileLocation", child->ccr->getLocation()).log();
		processName = fmt::format("TestClient{}", clientId);
		Standalone<StringRef> newZoneId(deterministicRandom()->randomUniqueID().toString());
		auto locality = LocalityData(Optional<Standalone<StringRef>>(), newZoneId, newZoneId, self->locality.dcId());
		auto dataFolder = joinPath(popPath(self->dataFolder), deterministicRandom()->randomUniqueID().toString());
		platform::createDirectory(dataFolder);
		TraceEvent("StartingClientForWorkload", id).detail("Name", processName).detail("Address", childAddress);
		childProcess = g_simulator.newProcess(processName.c_str(),
		                                      childAddress,
		                                      1,
		                                      self->address.isTLS(),
		                                      1,
		                                      locality,
		                                      ProcessClass(ProcessClass::TesterClass, ProcessClass::AutoSource),
		                                      dataFolder.c_str(),
		                                      self->coordinationFolder,
		                                      self->protocolVersion);
		childProcess->excludeFromRestarts = true;
		init = doInitialize(this);
	}

	~WorkloadProcessState() {
		TraceEvent("ShutdownClientForWorkload", id).log();
		g_simulator.destroyProcess(childProcess);
	}

	static std::vector<std::pair<WorkloadProcessState*, int>>& states() {
		static std::vector<std::pair<WorkloadProcessState*, int>> res;
		return res;
	}

	ACTOR static Future<Void> doInitialize(WorkloadProcessState* self) {
		state ISimulator::ProcessInfo* parent = g_simulator.getCurrentProcess();
		wait(g_simulator.onProcess(self->childProcess, TaskPriority::DefaultYield));
		try {
			FlowTransport::createInstance(true, 1, WLTOKEN_RESERVED_COUNT);
			Sim2FileSystem::newFileSystem();
			auto addr = g_simulator.getCurrentProcess()->address;
			FlowTransport::transport().setLocalAddress(addr);
			TraceEvent("WorkloadProcessInitialized", self->id);
		} catch (...) {
			ASSERT(false);
		}
		wait(g_simulator.onProcess(parent, TaskPriority::DefaultYield));
		return Void();
	}

public:
	static WorkloadProcessState* instance(int clientId) {
		states().resize(std::max(states().size(), size_t(clientId + 1)), std::make_pair(nullptr, 0));
		auto& res = states()[clientId];
		if (res.first == nullptr) {
			res = std::make_pair(new WorkloadProcessState(clientId), 0);
		}
		++res.second;
		return res.first;
	}
	static void done(WorkloadProcessState* processState) {
		auto& p = states()[processState->clientId];
		ASSERT(p.first == processState);
		if (--p.second == 0) {
			// delete p.first;
			// p.first = nullptr;
		}
	}

	Future<Void> initialized() const { return init; }

	UID id = deterministicRandom()->randomUniqueID();
	int clientId;
	ISimulator::ProcessInfo* childProcess;
};

struct WorkloadProcess {
	WorkloadProcessState* processState;
	UID id = deterministicRandom()->randomUniqueID();
	Database cx;
	Future<Void> databaseOpened;
	Reference<TestWorkload> child;
	std::string desc;

	void createDatabase(ClientWorkload::CreateWorkload const& childCreator, WorkloadContext const& wcx) {
		try {
			child = childCreator(wcx);
			TraceEvent("ClientWorkloadOpenDatabase", id).detail("ClusterFileLocation", child->ccr->getLocation());
			cx = Database::createDatabase(child->ccr, -1);
			desc = child->description();
		} catch (Error&) {
			throw;
		} catch (...) {
			ASSERT(false);
		}
	}

	ACTOR static Future<Void> openDatabase(WorkloadProcess* self,
	                                       ClientWorkload::CreateWorkload childCreator,
	                                       WorkloadContext wcx) {
		state ISimulator::ProcessInfo* parent = g_simulator.getCurrentProcess();
		state Optional<Error> err;
		wcx.dbInfo = Reference<AsyncVar<struct ServerDBInfo> const>();
		wait(self->processState->initialized());
		wait(g_simulator.onProcess(self->childProcess(), TaskPriority::DefaultYield));
		try {
			self->createDatabase(childCreator, wcx);
		} catch (Error& e) {
			ASSERT(e.code() != error_code_actor_cancelled);
			err = e;
		}
		wait(g_simulator.onProcess(parent, TaskPriority::DefaultYield));
		if (err.present()) {
			throw err.get();
		}
		return Void();
	}

	ISimulator::ProcessInfo* childProcess() { return processState->childProcess; }

	int clientId() const { return processState->clientId; }

	WorkloadProcess(ClientWorkload::CreateWorkload const& childCreator, WorkloadContext const& wcx)
	  : processState(WorkloadProcessState::instance(wcx.clientId)) {
		TraceEvent("StartingClinetWorkload", id).detail("OnClientProcess", processState->id);
		databaseOpened = openDatabase(this, childCreator, wcx);
	}

	ACTOR static void destroy(WorkloadProcess* self) {
		state ISimulator::ProcessInfo* parent = g_simulator.getCurrentProcess();
		wait(g_simulator.onProcess(self->childProcess(), TaskPriority::DefaultYield));
		delete self;
		wait(g_simulator.onProcess(parent, TaskPriority::DefaultYield));
	}

	std::string description() { return desc; }

	ACTOR template <class Ret, class Fun>
	Future<Ret> runActor(WorkloadProcess* self, Optional<TenantName> defaultTenant, Fun f) {
		state Optional<Error> err;
		state Ret res;
		state ISimulator::ProcessInfo* parent = g_simulator.getCurrentProcess();
		wait(self->databaseOpened);
		wait(g_simulator.onProcess(self->childProcess(), TaskPriority::DefaultYield));
		self->cx->defaultTenant = defaultTenant;
		try {
			Ret r = wait(f(self->cx));
			res = r;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				ASSERT(false);
				throw;
			}
			err = e;
		}
		wait(g_simulator.onProcess(parent, TaskPriority::DefaultYield));
		if (err.present()) {
			throw err.get();
		}
		return res;
	}

	~WorkloadProcess() { WorkloadProcessState::done(processState); }
};

ClientWorkload::ClientWorkload(CreateWorkload const& childCreator, WorkloadContext const& wcx)
  : TestWorkload(wcx), impl(new WorkloadProcess(childCreator, wcx)) {}

ClientWorkload::~ClientWorkload() {
	WorkloadProcess::destroy(impl);
}

std::string ClientWorkload::description() const {
	return impl->description();
}

Future<Void> ClientWorkload::initialized() {
	return impl->databaseOpened;
}

Future<Void> ClientWorkload::setup(Database const& cx) {
	return impl->runActor<Void>(impl, cx->defaultTenant, [this](Database const& db) { return impl->child->setup(db); });
}
Future<Void> ClientWorkload::start(Database const& cx) {
	return impl->runActor<Void>(impl, cx->defaultTenant, [this](Database const& db) { return impl->child->start(db); });
}
Future<bool> ClientWorkload::check(Database const& cx) {
	return impl->runActor<bool>(impl, cx->defaultTenant, [this](Database const& db) { return impl->child->check(db); });
}
Future<std::vector<PerfMetric>> ClientWorkload::getMetrics() {
	return impl->runActor<std::vector<PerfMetric>>(
	    impl, Optional<TenantName>(), [this](Database const& db) { return impl->child->getMetrics(); });
}
void ClientWorkload::getMetrics(std::vector<PerfMetric>& m) {
	ASSERT(false);
}

double ClientWorkload::getCheckTimeout() const {
	return impl->child->getCheckTimeout();
}
