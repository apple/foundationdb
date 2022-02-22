/*
 * workloads.actor.h
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

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"

#include <fmt/format.h>

#include "flow/actorcompiler.h" // has to be last include

struct ClientWorkloadImpl {
	Reference<TestWorkload> child;
	ISimulator::ProcessInfo* self = g_pSimulator->getCurrentProcess();
	ISimulator::ProcessInfo* childProcess = nullptr;
	IPAddress childAddress;
	std::string processName;
	Database cx;
	Future<Void> databaseOpened;

	ClientWorkloadImpl(Reference<TestWorkload> const& child) : child(child) {
		if (self->address.isV6()) {
			childAddress =
			    IPAddress::parse(fmt::format("2001:fdb1:fdb2:fdb3:fdb4:fdb5:fdb6:{:40x}", child->clientId + 2)).get();
		} else {
			childAddress = IPAddress::parse(fmt::format("192.168.0.{}", child->clientId + 2)).get();
		}
		processName = fmt::format("TestClient{}", child->clientId);
		childProcess = g_simulator.newProcess(processName.c_str(),
		                                      childAddress,
		                                      0,
		                                      self->address.isTLS(),
		                                      1,
		                                      self->locality,
		                                      ProcessClass(ProcessClass::TesterClass, ProcessClass::AutoSource),
		                                      self->dataFolder,
		                                      self->coordinationFolder,
		                                      self->protocolVersion);
		databaseOpened = openDatabase(this);
	}

	~ClientWorkloadImpl() {
		g_simulator.destroyProcess(childProcess);
	}


	ACTOR static Future<Void> openDatabase(ClientWorkloadImpl* self) {
		wait(g_simulator.onProcess(self->childProcess));
		self->cx = Database::createDatabase(self->child->ccr, -1);
		wait(g_simulator.onProcess(self->self));
		return Void();
	}

	ACTOR template<class Ret, class Fun> Future<Ret> runActor(ClientWorkloadImpl* self, Fun f) {
		state Optional<Error> err;
		state Ret res;
		wait(g_simulator.onProcess(self->childProcess));
		wait(self->databaseOpened);
		try {
			Ret r = wait(f(self->cx));
			res = r;
		} catch(Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			err = e;
		}
		wait(g_simulator.onProcess(self->self));
		if (err.present()) {
			throw err.get();
		}
		return res;
	}

};

ClientWorkload::ClientWorkload(Reference<TestWorkload> const& child, WorkloadContext const& wcx)
  : TestWorkload(wcx), impl(new ClientWorkloadImpl(child)) {}

std::string ClientWorkload::description() const {
	return impl->child->description();
}
Future<Void> ClientWorkload::setup(Database const& cx) {
	return impl->runActor<Void>(impl, [this](Database const& db) {
		return impl->child->setup(db);
	});
}
Future<Void> ClientWorkload::start(Database const& cx) {
	return impl->runActor<Void>(impl, [this](Database const& db) {
		return impl->child->start(db);
	});
}
Future<bool> ClientWorkload::check(Database const& cx) {
	return impl->runActor<bool>(impl, [this](Database const& db) {
		return impl->child->check(db);
	});
}
void ClientWorkload::getMetrics(std::vector<PerfMetric>& m) {
	return impl->child->getMetrics(m);
}

double ClientWorkload::getCheckTimeout() const {
	return impl->child->getCheckTimeout();
}
