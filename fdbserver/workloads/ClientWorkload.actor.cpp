/*
 * ClientWorkload.actor.cpp
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

#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"

#include <fmt/format.h>

#include "flow/ApiVersion.h"
#include "flow/actorcompiler.h" // has to be last include

class WorkloadProcessState {
	IPAddress childAddress;
	std::string processName;
	Future<Void> processActor;
	Promise<Void> init;

	WorkloadProcessState(int clientId) : clientId(clientId) { processActor = processStart(this); }

	~WorkloadProcessState() {
		TraceEvent("ShutdownClientForWorkload", id).log();
		g_simulator->destroyProcess(childProcess);
	}

	ACTOR static Future<Void> initializationDone(WorkloadProcessState* self, ISimulator::ProcessInfo* parent) {
		wait(g_simulator->onProcess(parent, TaskPriority::DefaultYield));
		self->init.send(Void());
		wait(Never());
		ASSERT(false); // does not happen
		return Void();
	}

	ACTOR static Future<Void> processStart(WorkloadProcessState* self) {
		state ISimulator::ProcessInfo* parent = g_simulator->getCurrentProcess();
		state std::vector<Future<Void>> futures;
		if (parent->address.isV6()) {
			self->childAddress =
			    IPAddress::parse(fmt::format("2001:fdb1:fdb2:fdb3:fdb4:fdb5:fdb6:{:04x}", self->clientId + 2)).get();
		} else {
			self->childAddress = IPAddress::parse(fmt::format("192.168.0.{}", self->clientId + 2)).get();
		}
		self->processName = fmt::format("TestClient{}", self->clientId);
		Standalone<StringRef> newZoneId(deterministicRandom()->randomUniqueID().toString());
		auto locality = LocalityData(Optional<Standalone<StringRef>>(), newZoneId, newZoneId, parent->locality.dcId());
		auto dataFolder = joinPath(popPath(parent->dataFolder), deterministicRandom()->randomUniqueID().toString());
		platform::createDirectory(dataFolder);
		TraceEvent("StartingClientWorkloadProcess", self->id)
		    .detail("Name", self->processName)
		    .detail("Address", self->childAddress);
		self->childProcess = g_simulator->newProcess(self->processName.c_str(),
		                                             self->childAddress,
		                                             1,
		                                             parent->address.isTLS(),
		                                             1,
		                                             locality,
		                                             ProcessClass(ProcessClass::TesterClass, ProcessClass::AutoSource),
		                                             dataFolder.c_str(),
		                                             parent->coordinationFolder.c_str(),
		                                             parent->protocolVersion,
		                                             false);
		self->childProcess->excludeFromRestarts = true;
		wait(g_simulator->onProcess(self->childProcess, TaskPriority::DefaultYield));
		try {
			FlowTransport::createInstance(true, 1, WLTOKEN_RESERVED_COUNT);
			Sim2FileSystem::newFileSystem();
			auto addr = g_simulator->getCurrentProcess()->address;
			futures.push_back(FlowTransport::transport().bind(addr, addr));
			futures.push_back(success((self->childProcess->onShutdown())));
			TraceEvent("ClientWorkloadProcessInitialized", self->id).log();
			futures.push_back(initializationDone(self, parent));
			wait(waitForAny(futures));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				return Void();
			}
			ASSERT(false);
		}
		ASSERT(false);
		return Void();
	}

	static std::vector<WorkloadProcessState*>& states() {
		static std::vector<WorkloadProcessState*> res;
		return res;
	}

public:
	static WorkloadProcessState* instance(int clientId) {
		states().resize(std::max(states().size(), size_t(clientId + 1)), nullptr);
		auto& res = states()[clientId];
		if (res == nullptr) {
			res = new WorkloadProcessState(clientId);
		}
		return res;
	}

	Future<Void> initialized() const { return init.getFuture(); }

	UID id = deterministicRandom()->randomUniqueID();
	int clientId;
	ISimulator::ProcessInfo* childProcess;
};

struct WorkloadProcess {
	WorkloadProcessState* processState;
	WorkloadContext childWorkloadContext;
	UID id = deterministicRandom()->randomUniqueID();
	Database cx;
	Future<Void> databaseOpened;
	Reference<TestWorkload> child;
	std::string desc;

	void createDatabase(ClientWorkload::CreateWorkload const& childCreator, WorkloadContext const& wcx) {
		try {
			child = childCreator(wcx);
			TraceEvent("ClientWorkloadOpenDatabase", id).detail("ClusterFileLocation", child->ccr->getLocation());
			cx = Database::createDatabase(child->ccr, ApiVersion::LATEST_VERSION);
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
		state ISimulator::ProcessInfo* parent = g_simulator->getCurrentProcess();
		state Optional<Error> err;
		wcx.dbInfo = Reference<AsyncVar<struct ServerDBInfo> const>();
		wait(self->processState->initialized());
		wait(g_simulator->onProcess(self->childProcess(), TaskPriority::DefaultYield));
		try {
			self->createDatabase(childCreator, wcx);
		} catch (Error& e) {
			ASSERT(e.code() != error_code_actor_cancelled);
			err = e;
		}
		wait(g_simulator->onProcess(parent, TaskPriority::DefaultYield));
		if (err.present()) {
			throw err.get();
		}
		return Void();
	}

	ISimulator::ProcessInfo* childProcess() { return processState->childProcess; }

	int clientId() const { return processState->clientId; }

	WorkloadProcess(ClientWorkload::CreateWorkload const& childCreator, WorkloadContext const& wcx)
	  : processState(WorkloadProcessState::instance(wcx.clientId)) {
		TraceEvent("StartingClientWorkload", id).detail("OnClientProcess", processState->id);
		childWorkloadContext.clientCount = wcx.clientCount;
		childWorkloadContext.clientId = wcx.clientId;
		childWorkloadContext.ccr = wcx.ccr;
		childWorkloadContext.options = wcx.options;
		childWorkloadContext.sharedRandomNumber = wcx.sharedRandomNumber;
		databaseOpened = openDatabase(this, childCreator, childWorkloadContext);
	}

	ACTOR static void destroy(WorkloadProcess* self) {
		state ISimulator::ProcessInfo* parent = g_simulator->getCurrentProcess();
		wait(g_simulator->onProcess(self->childProcess(), TaskPriority::DefaultYield));
		TraceEvent("DeleteWorkloadProcess").backtrace();
		delete self;
		wait(g_simulator->onProcess(parent, TaskPriority::DefaultYield));
	}

	std::string description() { return desc; }

	// This actor will keep a reference to a future alive, switch to another process and then return. If the future
	// count of `f` is 1, this will cause the future to be destroyed in the process `process`
	ACTOR template <class T>
	static void cancelChild(ISimulator::ProcessInfo* process, Future<T> f) {
		wait(g_simulator->onProcess(process, TaskPriority::DefaultYield));
	}

	ACTOR template <class Ret, class Fun>
	Future<Ret> runActor(WorkloadProcess* self, Optional<TenantName> defaultTenant, Fun f) {
		state Optional<Error> err;
		state Ret res;
		state Future<Ret> fut;
		state ISimulator::ProcessInfo* parent = g_simulator->getCurrentProcess();
		wait(self->databaseOpened);
		wait(g_simulator->onProcess(self->childProcess(), TaskPriority::DefaultYield));
		self->cx->defaultTenant = defaultTenant;
		try {
			fut = f(self->cx);
			Ret r = wait(fut);
			res = r;
		} catch (Error& e) {
			// if we're getting cancelled, we could run in the scope of the parent process, but we're not allowed to
			// cancel `fut` in any other process than the child process. So we're going to pass the future to an
			// uncancellable actor (it has to be uncancellable because if we got cancelled here we can't wait on
			// anything) which will then destroy the future on the child process.
			cancelChild(self->childProcess(), fut);
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			err = e;
		}
		fut = Future<Ret>();
		wait(g_simulator->onProcess(parent, TaskPriority::DefaultYield));
		if (err.present()) {
			throw err.get();
		}
		return res;
	}
};

ClientWorkload::ClientWorkload(CreateWorkload const& childCreator, WorkloadContext const& wcx)
  : TestWorkload(wcx), impl(new WorkloadProcess(childCreator, wcx)) {}

ClientWorkload::~ClientWorkload() {
	TraceEvent(SevDebug, "DestroyClientWorkload").backtrace();
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
