/*
 * ClientWorkload.cpp
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

#include "fdbserver/core/ServerDBInfo.actor.h"
#include "fdbserver/tester/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"

#include <fmt/format.h>
#include <type_traits>

#include "flow/ApiVersion.h"

class WorkloadProcessState {
	IPAddress childAddress;
	std::string processName;
	Future<Void> processActor;
	Promise<Void> init;

	WorkloadProcessState(int clientId) : clientId(clientId) { processActor = processStart(); }

	~WorkloadProcessState() {
		TraceEvent("ShutdownClientForWorkload", id).log();
		g_simulator->destroyProcess(childProcess);
	}

	Future<Void> initializationDone(ISimulator::ProcessInfo* parent) {
		co_await g_simulator->onProcess(parent, TaskPriority::DefaultYield);
		init.send(Void());
		co_await Future<Void>(Never());
		ASSERT(false); // does not happen
	}

	Future<Void> processStart() {
		ISimulator::ProcessInfo* parent = g_simulator->getCurrentProcess();
		std::vector<Future<Void>> futures;
		if (parent->address.isV6()) {
			childAddress =
			    IPAddress::parse(fmt::format("2001:fdb1:fdb2:fdb3:fdb4:fdb5:fdb6:{:04x}", clientId + 2)).get();
		} else {
			childAddress = IPAddress::parse(fmt::format("192.168.0.{}", clientId + 2)).get();
		}
		processName = fmt::format("TestClient{}", clientId);
		Standalone<StringRef> newZoneId(deterministicRandom()->randomUniqueID().toString());
		auto locality = LocalityData(Optional<Standalone<StringRef>>(), newZoneId, newZoneId, parent->locality.dcId());
		auto dataFolder = joinPath(popPath(parent->dataFolder), deterministicRandom()->randomUniqueID().toString());
		platform::createDirectory(dataFolder);
		TraceEvent("StartingClientWorkloadProcess", id).detail("Name", processName).detail("Address", childAddress);
		childProcess = g_simulator->newProcess(processName.c_str(),
		                                       childAddress,
		                                       1,
		                                       parent->address.isTLS(),
		                                       1,
		                                       locality,
		                                       ProcessClass(ProcessClass::TesterClass, ProcessClass::AutoSource),
		                                       dataFolder.c_str(),
		                                       parent->coordinationFolder.c_str(),
		                                       parent->protocolVersion,
		                                       false);
		childProcess->excludeFromRestarts = true;
		co_await g_simulator->onProcess(childProcess, TaskPriority::DefaultYield);
		try {
			FlowTransport::createInstance(true, 1, WLTOKEN_RESERVED_COUNT);
			Sim2FileSystem::newFileSystem();
			auto addr = g_simulator->getCurrentProcess()->address;
			futures.push_back(FlowTransport::transport().bind(addr, addr));
			futures.push_back(success(childProcess->onShutdown()));
			TraceEvent("ClientWorkloadProcessInitialized", id).log();
			futures.push_back(initializationDone(parent));
			co_await waitForAny(futures);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				co_return;
			}
			ASSERT(false);
		}
		ASSERT(false);
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

	Future<Void> openDatabase(ClientWorkload::CreateWorkload childCreator, WorkloadContext wcx) {
		ISimulator::ProcessInfo* parent = g_simulator->getCurrentProcess();
		Optional<Error> err;
		wcx.dbInfo = Reference<AsyncVar<struct ServerDBInfo> const>();
		co_await processState->initialized();
		co_await g_simulator->onProcess(childProcess(), TaskPriority::DefaultYield);
		try {
			createDatabase(childCreator, wcx);
		} catch (Error& e) {
			ASSERT(e.code() != error_code_actor_cancelled);
			err = e;
		}
		co_await g_simulator->onProcess(parent, TaskPriority::DefaultYield);
		if (err.present()) {
			throw err.get();
		}
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
		databaseOpened = openDatabase(childCreator, childWorkloadContext);
	}

	Future<Void> destroy() {
		ISimulator::ProcessInfo* parent = g_simulator->getCurrentProcess();
		co_await g_simulator->onProcess(childProcess(), TaskPriority::DefaultYield);
		TraceEvent("DeleteWorkloadProcess").backtrace();
		delete this;
		co_await g_simulator->onProcess(parent, TaskPriority::DefaultYield);
	}

	std::string description() { return desc; }

	// This actor will keep a reference to a future alive, switch to another process and then return. If the future
	// count of `f` is 1, this will cause the future to be destroyed in the process `process`
	template <class T>
	static Future<Void> cancelChild(ISimulator::ProcessInfo* process, Future<T> f) {
		co_await g_simulator->onProcess(process, TaskPriority::DefaultYield);
	}

	template <class Ret, class Fun>
	Future<Ret> runActor(Fun f) {
		Optional<Error> err;
		using ResultHolder = std::conditional_t<std::is_same_v<Ret, Void>, bool, Ret>;
		[[maybe_unused]] ResultHolder res{};
		Future<Ret> fut;
		ISimulator::ProcessInfo* parent = g_simulator->getCurrentProcess();
		co_await databaseOpened;
		co_await g_simulator->onProcess(childProcess(), TaskPriority::DefaultYield);
		try {
			fut = f(cx);
			if constexpr (std::is_same_v<Ret, Void>) {
				co_await fut;
			} else {
				res = co_await fut;
			}
		} catch (Error& e) {
			// if we're getting cancelled, we could run in the scope of the parent process, but we're not allowed to
			// cancel `fut` in any other process than the child process. So we're going to pass the future to an
			// uncancellable actor (it has to be uncancellable because if we got cancelled here we can't wait on
			// anything) which will then destroy the future on the child process.
			cancelChild(childProcess(), fut);
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			err = e;
		}
		fut = Future<Ret>();
		co_await g_simulator->onProcess(parent, TaskPriority::DefaultYield);
		if (err.present()) {
			throw err.get();
		}
		if constexpr (std::is_same_v<Ret, Void>) {
		} else {
			co_return res;
		}
	}
};

ClientWorkload::ClientWorkload(CreateWorkload const& childCreator, WorkloadContext const& wcx)
  : TestWorkload(wcx), impl(new WorkloadProcess(childCreator, wcx)) {}

ClientWorkload::~ClientWorkload() {
	TraceEvent(SevDebug, "DestroyClientWorkload").backtrace();
	impl->destroy();
}

std::string ClientWorkload::description() const {
	return impl->description();
}

Future<Void> ClientWorkload::initialized() {
	return impl->databaseOpened;
}

Future<Void> ClientWorkload::setup(Database const& cx) {
	return impl->runActor<Void>([this](Database const& db) { return impl->child->setup(db); });
}
Future<Void> ClientWorkload::start(Database const& cx) {
	return impl->runActor<Void>([this](Database const& db) { return impl->child->start(db); });
}
Future<bool> ClientWorkload::check(Database const& cx) {
	return impl->runActor<bool>([this](Database const& db) { return impl->child->check(db); });
}
Future<std::vector<PerfMetric>> ClientWorkload::getMetrics() {
	return impl->runActor<std::vector<PerfMetric>>([this](Database const& db) { return impl->child->getMetrics(); });
}
void ClientWorkload::getMetrics(std::vector<PerfMetric>& m) {
	ASSERT(false);
}

double ClientWorkload::getCheckTimeout() const {
	return impl->child->getCheckTimeout();
}
