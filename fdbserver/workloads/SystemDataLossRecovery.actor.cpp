/*
 *SystemDataLossRecovery.actor.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
std::string printValue(const ErrorOr<Optional<Value>>& value) {
	if (value.isError()) {
		return value.getError().name();
	}
	return value.get().present() ? value.get().get().toString() : "Value Not Found.";
}
} // namespace

struct SystemDataLossRecoveryWorkload : TestWorkload {
	const bool enabled;
	bool pass;

	SystemDataLossRecoveryWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(!clientId), pass(true) {}

	void validationFailed(ErrorOr<Optional<Value>> expectedValue, ErrorOr<Optional<Value>> actualValue) {
		TraceEvent(SevError, "TestFailed")
		    .detail("ExpectedValue", printValue(expectedValue))
		    .detail("ActualValue", printValue(actualValue));
		pass = false;
	}

	std::string description() const override { return "SystemDataLossRecovery"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (!enabled) {
			return Void();
		}
		return _start(this, cx);
	}

	ACTOR Future<Void> _start(SystemDataLossRecoveryWorkload* self, Database cx) {
		wait(self->recoverFromSystemDataLoss(self, cx));
		return Void();
	}

	ACTOR Future<Void> recoverFromSystemDataLoss(SystemDataLossRecoveryWorkload* self, Database cx) {
		state Key key = "TestKey"_sr;
		state Key endKey = "TestKey0"_sr;
		state Value oldValue = "TestValue"_sr;
		state Value newValue = "TestNewValue"_sr;

		wait(self->writeAndVerify(self, cx, key, oldValue));
		TraceEvent("InitialDataReady");

		// Move [\xff, \xff\xff) to team: {ssi}.
		state StorageServerInterface ssi = wait(self->moveToRandomServer(self, cx, systemKeys));

		// Kill team {ssi}, and expect read to timeout.
		self->killProcess(self, ssi.address());

		// Read \xff/keyServers/, and expect the read to time out.
		wait(self->readAndVerify(self, cx, keyServersKeys.begin, timed_out()));

		// Repair system data.
		wait(repairSystemData(cx->getConnectionRecord()));

		// Read \xff/keyServers/, and expect the read to succeed.
		state Transaction tr(cx);
		loop {
			try {
				Optional<Value> res = wait(tr.get(keyServersKeys.begin));
				ASSERT(res.present());
				break;
			} catch (Error& e) {
				TraceEvent("ReadKeyServersKeysFailed").error(e);
				wait(tr.onError(e));
			}
		}

		TraceEvent("ReadKeyServersKeys");

		// Remove the failed server.
		wait(self->exclude(cx, ssi.address()));
		int ignore = wait(setDDMode(cx, 1));

		return Void();
	}

	// Move keyrange to a ramdom single storage server.
	ACTOR Future<StorageServerInterface> moveToRandomServer(SystemDataLossRecoveryWorkload* self,
	                                                        Database cx,
	                                                        KeyRangeRef shard) {
		TraceEvent("TestMoveShardBegin").detail("Begin", shard.begin).detail("End", shard.end);

        state int i = 1;
		loop {
			try {
				state Optional<StorageServerInterface> ssi = wait(self->getRandomStorageServer(cx));
				ASSERT(ssi.present());

				// Move [key, endKey) to team: {address}.
				std::vector<NetworkAddress> addresses;
				addresses.push_back(ssi.get().address());
				wait(moveShard(cx->getConnectionRecord(), shard, addresses, UID(1, i++)));
				break;
			} catch (Error& e) {
				if (e.code() != error_code_destination_servers_not_found && e.code() != error_code_movekeys_conflict) {
					throw e;
				}
			}
		}

		state Transaction txn(cx);
		loop {
			try {
				Standalone<VectorRef<const char*>> adds = wait(txn.getAddressesForKey(shard.begin));
				ASSERT(adds.size() == 1);
				ASSERT(adds[0] == ssi.get().address().toString());
				break;
			} catch (Error& e) {
				wait(txn.onError(e));
			}
		}

		TraceEvent("TestMoveShardEnd")
		    .detail("Begin", shard.begin)
		    .detail("End", shard.end)
		    .detail("Address", ssi.get().address());

		return ssi.get();
	}

	// Find a random storage server.
	ACTOR Future<Optional<StorageServerInterface>> getRandomStorageServer(Database cx) {
		loop {
			std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
			if (!interfs.empty()) {
				const auto& interf = interfs[deterministicRandom()->randomInt(0, interfs.size())];
				if (g_simulator.protectedAddresses.count(interf.address()) == 0 && !interf.isTss()) {
					return interf;
				}
			}
		}
	}

	ACTOR Future<Void> readAndVerify(SystemDataLossRecoveryWorkload* self,
	                                 Database cx,
	                                 Key key,
	                                 ErrorOr<Optional<Value>> expectedValue) {
		state Transaction tr(cx);

		loop {
			try {
				state Optional<Value> res = wait(timeoutError(tr.get(key), 30.0));
				const bool equal = !expectedValue.isError() && res == expectedValue.get();
				if (!equal) {
					self->validationFailed(expectedValue, ErrorOr<Optional<Value>>(res));
				}
				break;
			} catch (Error& e) {
				if (expectedValue.isError() && expectedValue.getError().code() == e.code()) {
					break;
				}
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<Void> writeAndVerify(SystemDataLossRecoveryWorkload* self,
	                                  Database cx,
	                                  Key key,
	                                  Optional<Value> value) {
		state Transaction tr(cx);
		loop {
			try {
				if (value.present()) {
					tr.set(key, value.get());
				} else {
					tr.clear(key);
				}
				wait(timeoutError(tr.commit(), 30.0));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		wait(self->readAndVerify(self, cx, key, value));

		return Void();
	}

	ACTOR Future<Void> exclude(Database cx, NetworkAddress addr) {
		state Transaction tr(cx);
		state std::vector<AddressExclusion> servers;
		servers.push_back(AddressExclusion(addr.ip, addr.port));
		loop {
			try {
				excludeServers(tr, servers, true);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		// Wait until all data are moved out of servers.
		std::set<NetworkAddress> inProgress = wait(checkForExcludingServers(cx, servers, true));
		ASSERT(inProgress.empty());

		TraceEvent("ExcludedFailedServer").detail("Address", addr.toString());
		return Void();
	}

	void killProcess(SystemDataLossRecoveryWorkload* self, const NetworkAddress& addr) {
		ISimulator::ProcessInfo* process = g_simulator.getProcessByAddress(addr);
		ASSERT(process->addresses.contains(addr));
		g_simulator.killProcess(process, ISimulator::KillInstantly);
		TraceEvent("TestTeamKilled").detail("Address", addr);
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SystemDataLossRecoveryWorkload> SystemDataLossRecoveryWorkloadFactory("SystemDataLossRecovery");