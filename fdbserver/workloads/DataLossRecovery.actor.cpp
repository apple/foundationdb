/*
 *DataLossRecovery.actor.cpp
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
std::string printValue(const Optional<Value>& value) {
	return value.present() ? value.get().toString() : "ValueNotFound";
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

	void validationFailed(KeyRef key, Optional<Value>& expectedValue, Optional<Value>& actualValue) {
		TraceEvent(SevError, "TestFailed")
		    .detail("Key", key.toString())
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

		state Optional<StorageServerInterface> ssi = wait(self->getRandomStorageServer(cx));
		ASSERT(ssi.present());

		// Move [key, endKey) to team: {address}.
		state std::vector<NetworkAddress> addresses;
		addresses.push_back(ssi.get().address());
		std::cout << "Moving." << std::endl;
		wait(moveShard(cx->getConnectionFile(), KeyRangeRef(key, endKey), addresses));

		Transaction tr(cx);
		Standalone<VectorRef<const char*>> adds = wait(tr.getAddressesForKey(key));
		ASSERT(adds.size() == 1);
		ASSERT(adds[0] == ssi.get().address().toString());
		std::cout << "Moved to: " << adds[0] << "(" << ssi.get().uniqueID.toString() << ")" << std::endl;

		wait(self->readAndVerify(self, cx, key, oldValue));

		// Kill team {address}, and expect read to timeout.
		self->killProcess(self, ssi.get().address());
		wait(self->readAndVerify(self, cx, key, "Timeout"_sr));

		// Reenable DD and exclude address as fail, so that [key, endKey) will be dropped and moved to a new team.
		// Expect read to return 'value not found'.
		int ignore = wait(setDDMode(cx, 1));
		wait(self->exclude(self, cx, key, ssi.get().address()));
		wait(self->readAndVerify(self, cx, key, Optional<Value>()));

		// Write will scceed.
		wait(self->writeAndVerify(self, cx, key, newValue));

		// **********************************************************************

		state Optional<StorageServerInterface> ssi2 = wait(self->getRandomStorageServer(cx));
		ASSERT(ssi2.present());
		ASSERT(ssi2 != ssi);

		// Move [\xff, \xff\xff) to team: {ssi}.
		addresses.clear();
		addresses.push_back(ssi2.get().address());
		std::cout << "Moving." << std::endl;
		wait(moveShard(cx->getConnectionFile(), systemKeys, addresses));

		// Kill team {address}, and expect read to timeout.
		self->killProcess(self, ssi2.get().address());
		std::cout << "Killed process: " << ssi2.get().address().toString() << "(" << ssi2.get().toString() << ")"
		          << std::endl;
		wait(self->readAndVerify(self, cx, keyServersKey(key), "Timeout"_sr));
		std::cout << "Verified reading metadata timeout" << std::endl;

		wait(self->readAndVerify(self, cx, key, newValue));
		std::cout << "Read" << std::endl;
		// Write will scceed.
		wait(self->writeAndVerify(self, cx, key, oldValue));
		std::cout << "Write" << std::endl;
		wait(forceRecovery(cx->getConnectionFile(), LiteralStringRef("1")));

		// Write will scceed.
		wait(self->writeAndVerify(self, cx, key, oldValue));
		std::cout << "Write2" << std::endl;

		Optional<Value> res = wait(self->read(self, cx, keyServersKey(key)));
		ASSERT(res.present());
		std::cout << "Read3" << std::endl;

		return Void();
	}

	ACTOR Future<Optional<StorageServerInterface>> getRandomStorageServer(Database cx) {
		loop {
			std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
			if (!interfs.empty()) {
				const auto& interf = interfs[random() % interfs.size()];
				if (g_simulator.protectedAddresses.count(interf.address()) == 0) {
					return interf;
				}
			}
		}
	}

	ACTOR Future<Optional<Value>> read(DataLossRecoveryWorkload* self, Database cx, Key key) {
		state Transaction tr(cx);

		loop {
			tr.reset();
			try {
				state Optional<Value> res = wait(timeout(tr.get(key), 10.0, Optional<Value>("Timeout"_sr)));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return res;
	}

	ACTOR Future<Void> readAndVerify(DataLossRecoveryWorkload* self,
	                                 Database cx,
	                                 Key key,
	                                 Optional<Value> expectedValue) {
		state Transaction tr(cx);

		loop {
			tr.reset();
			try {
				state Optional<Value> res = wait(timeout(tr.get(key), 10.0, Optional<Value>("Timeout"_sr)));
				if (res != expectedValue) {
					self->validationFailed(key, expectedValue, res);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<Void> writeAndVerify(DataLossRecoveryWorkload* self, Database cx, Key key, Optional<Value> value) {
		state Transaction tr(cx);
		loop {
			tr.reset();
			try {
				if (value.present()) {
					tr.set(key, value.get());
				} else {
					tr.clear(key);
				}
				wait(timeout(tr.commit(), 10.0, Void()));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		wait(self->readAndVerify(self, cx, key, value));

		return Void();
	}

	ACTOR Future<Void> exclude(DataLossRecoveryWorkload* self, Database cx, Key key, NetworkAddress addr) {
		state Transaction tr(cx);
		Standalone<VectorRef<const char*>> addresses = wait(tr.getAddressesForKey(key));
		state std::vector<AddressExclusion> servers;
		servers.push_back(AddressExclusion(addr.ip, addr.port));
		loop {
			tr.reset();
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

		TraceEvent("ExcludedTestKey").detail("Address", addr.toString());
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
				const auto& interf = interfs[random() % interfs.size()];
				if (g_simulator.protectedAddresses.count(interf.address()) == 0) {
					dest.push_back(interf.uniqueID);
					self->addr = interf.address();
					addr = interf.address();
				}
			}
		}

		state UID owner = deterministicRandom()->randomUniqueID();
		state DDEnabledState ddEnabledState;

		state Transaction tr(cx);

		loop {
			tr.reset();
			try {
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
				if (e.code() != error_code_movekeys_conflict) {
					throw e;
				}
			}
		}

		TraceEvent("TestKeyMoved").detail("NewTeam", describe(dest)).detail("Address", addr.toString());

		tr.reset();
		Standalone<VectorRef<const char*>> addresses = wait(tr.getAddressesForKey(keys.begin));

		// The move function is not what we are testing here, crash the test if the move fails.
		ASSERT(addresses.size() == 1);
		ASSERT(std::string(addresses[0]) == addr.toString());

		return addr;
	}

	void killProcess(DataLossRecoveryWorkload* self, const NetworkAddress& addr) {
		ISimulator::ProcessInfo* process = g_simulator.getProcessByAddress(addr);
		ASSERT(process->addresses.contains(addr));
		g_simulator.killProcess(process, ISimulator::KillInstantly);
		TraceEvent("TestTeamKilled").detail("Address", addr.toString());
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<DataLossRecoveryWorkload> DataLossRecoveryWorkloadFactory("DataLossRecovery");