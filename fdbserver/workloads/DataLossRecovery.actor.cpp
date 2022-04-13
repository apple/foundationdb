/*
 * DataLossRecovery.actor.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/FDBOptions.g.h"
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

struct DataLossRecoveryWorkload : TestWorkload {
	FlowLock startMoveKeysParallelismLock;
	FlowLock finishMoveKeysParallelismLock;
	const bool enabled;
	bool pass;
	NetworkAddress addr;

	DataLossRecoveryWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), startMoveKeysParallelismLock(1), finishMoveKeysParallelismLock(1), enabled(!clientId),
	    pass(true) {}

	void validationFailed(ErrorOr<Optional<Value>> expectedValue, ErrorOr<Optional<Value>> actualValue) {
		TraceEvent(SevError, "TestFailed")
		    .detail("ExpectedValue", printValue(expectedValue))
		    .detail("ActualValue", printValue(actualValue));
		pass = false;
	}

	std::string description() const override { return "DataLossRecovery"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (!enabled) {
			return Void();
		}
		return _start(this, cx);
	}

	ACTOR Future<Void> _start(DataLossRecoveryWorkload* self, Database cx) {
		state Key key = "TestKey"_sr;
		state Key endKey = "TestKey0"_sr;
		state Value oldValue = "TestValue"_sr;
		state Value newValue = "TestNewValue"_sr;

		wait(self->writeAndVerify(self, cx, key, oldValue));

		// Move [key, endKey) to team: {address}.
		state NetworkAddress address = wait(self->disableDDAndMoveShard(self, cx, KeyRangeRef(key, endKey)));
		wait(self->readAndVerify(self, cx, key, oldValue));

		// Kill team {address}, and expect read to timeout.
		self->killProcess(self, address);
		wait(self->readAndVerify(self, cx, key, timed_out()));

		// Reenable DD and exclude address as fail, so that [key, endKey) will be dropped and moved to a new team.
		// Expect read to return 'value not found'.
		int ignore = wait(setDDMode(cx, 1));
		wait(self->exclude(cx, address));
		wait(self->readAndVerify(self, cx, key, Optional<Value>()));

		// Write will scceed.
		wait(self->writeAndVerify(self, cx, key, newValue));

		return Void();
	}

	ACTOR Future<Void> readAndVerify(DataLossRecoveryWorkload* self,
	                                 Database cx,
	                                 Key key,
	                                 ErrorOr<Optional<Value>> expectedValue) {
		state Transaction tr(cx);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
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

	ACTOR Future<Void> writeAndVerify(DataLossRecoveryWorkload* self, Database cx, Key key, Optional<Value> value) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
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

	// Move keys to a random selected team consisting of a single SS, after disabling DD, so that keys won't be
	// kept in the new team until DD is enabled.
	// Returns the address of the single SS of the new team.
	ACTOR Future<NetworkAddress> disableDDAndMoveShard(DataLossRecoveryWorkload* self, Database cx, KeyRange keys) {
		// Disable DD to avoid DD undoing of our move.
		state int ignore = wait(setDDMode(cx, 0));
		state NetworkAddress addr;

		// Pick a random SS as the dest, keys will reside on a single server after the move.
		state std::vector<UID> dest;
		while (dest.empty()) {
			std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
			if (!interfs.empty()) {
				const auto& interf = interfs[deterministicRandom()->randomInt(0, interfs.size())];
				if (g_simulator.protectedAddresses.count(interf.address()) == 0) {
					dest.push_back(interf.uniqueID);
					addr = interf.address();
				}
			}
		}

		state UID owner = deterministicRandom()->randomUniqueID();
		state DDEnabledState ddEnabledState;

		state Transaction tr(cx);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				BinaryWriter wrMyOwner(Unversioned());
				wrMyOwner << owner;
				tr.set(moveKeysLockOwnerKey, wrMyOwner.toValue());
				wait(tr.commit());

				MoveKeysLock moveKeysLock;
				moveKeysLock.myOwner = owner;

				wait(moveKeys(cx,
				              keys,
				              dest,
				              dest,
				              moveKeysLock,
				              Promise<Void>(),
				              &self->startMoveKeysParallelismLock,
				              &self->finishMoveKeysParallelismLock,
				              false,
				              UID(), // for logging only
				              &ddEnabledState));
				break;
			} catch (Error& e) {
				if (e.code() == error_code_movekeys_conflict) {
					// Conflict on moveKeysLocks with the current running DD is expected, just retry.
					tr.reset();
				} else {
					wait(tr.onError(e));
				}
			}
		}

		TraceEvent("TestKeyMoved").detail("NewTeam", describe(dest)).detail("Address", addr.toString());

		state Transaction validateTr(cx);
		loop {
			try {
				validateTr.setOption(FDBTransactionOptions::RAW_ACCESS);
				Standalone<VectorRef<const char*>> addresses = wait(validateTr.getAddressesForKey(keys.begin));
				// The move function is not what we are testing here, crash the test if the move fails.
				ASSERT(addresses.size() == 1);
				ASSERT(std::string(addresses[0]) == addr.toString());
				break;
			} catch (Error& e) {
				wait(validateTr.onError(e));
			}
		}

		return addr;
	}

	void killProcess(DataLossRecoveryWorkload* self, const NetworkAddress& addr) {
		ISimulator::ProcessInfo* process = g_simulator.getProcessByAddress(addr);
		ASSERT(process->addresses.contains(addr));
		g_simulator.killProcess(process, ISimulator::KillInstantly);
		TraceEvent("TestTeamKilled").detail("Address", addr);
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<DataLossRecoveryWorkload> DataLossRecoveryWorkloadFactory("DataLossRecovery");