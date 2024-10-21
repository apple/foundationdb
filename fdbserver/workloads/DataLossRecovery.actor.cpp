/*
 * DataLossRecovery.actor.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/Knobs.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "fdbrpc/SimulatorProcessInfo.h"
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
	static constexpr auto NAME = "DataLossRecovery";
	FlowLock startMoveKeysParallelismLock;
	FlowLock finishMoveKeysParallelismLock;
	const bool enabled;
	bool pass;
	NetworkAddress addr;

	DataLossRecoveryWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), startMoveKeysParallelismLock(5), finishMoveKeysParallelismLock(5), enabled(!clientId),
	    pass(true) {}

	void validationFailed(ErrorOr<Optional<Value>> expectedValue, ErrorOr<Optional<Value>> actualValue) {
		TraceEvent(SevError, "TestFailed")
		    .detail("ExpectedValue", printValue(expectedValue))
		    .detail("ActualValue", printValue(actualValue));
		pass = false;
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomMoveKeys", "Attrition" });
	}

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

		TraceEvent("DataLossRecovery").detail("Phase", "Starting");
		wait(self->writeAndVerify(self, cx, key, oldValue));

		TraceEvent("DataLossRecovery").detail("Phase", "InitialWrites");
		// Move [key, endKey) to team: {address}.
		state NetworkAddress address = wait(self->disableDDAndMoveShard(self, cx, KeyRangeRef(key, endKey)));
		TraceEvent("DataLossRecovery").detail("Phase", "Moved");
		wait(self->readAndVerify(self, cx, key, oldValue));
		TraceEvent("DataLossRecovery").detail("Phase", "ReadAfterMove");

		// Kill team {address}, and expect read to timeout.
		self->killProcess(self, address);
		TraceEvent("DataLossRecovery").detail("Phase", "KilledProcess");
		wait(self->readAndVerify(self, cx, key, timed_out()));
		TraceEvent("DataLossRecovery").detail("Phase", "VerifiedReadTimeout");

		// Reenable DD and exclude address as fail, so that [key, endKey) will be dropped and moved to a new team.
		// Expect read to return 'value not found'.
		wait(success(setDDMode(cx, 1)));
		wait(self->exclude(cx, address));
		TraceEvent("DataLossRecovery").detail("Phase", "Excluded");
		wait(self->readAndVerify(self, cx, key, Optional<Value>()));
		TraceEvent("DataLossRecovery").detail("Phase", "VerifiedDataDropped");

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
				// add timeout to read so test fails faster if something goes wrong
				state Optional<Value> res = wait(timeoutError(tr.get(key), 90.0));
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
				if (value.present()) {
					tr.set(key, value.get());
				} else {
					tr.clear(key);
				}
				wait(tr.commit());
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
				wait(excludeServers(&tr, servers, true));
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
		wait(success(setDDMode(cx, 0)));
		TraceEvent("DataLossRecovery").detail("Phase", "DisabledDD");
		state NetworkAddress addr;

		// Pick a random SS as the dest, keys will reside on a single server after the move.
		state std::vector<UID> dest;
		while (dest.empty()) {
			state std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
			if (!interfs.empty()) {
				state StorageServerInterface interf = interfs[deterministicRandom()->randomInt(0, interfs.size())];
				if (!g_simulator->protectedAddresses.contains(interf.address())) {
					// We need to avoid selecting a storage server that is already dead at this point, otherwise
					// the test will hang. This is achieved by sending a GetStorageMetrics RPC. This is a necessary
					// check for this test because DD has been disabled and the proper mechanism that removes bad
					// storage servers are not taking place in the scope of this function.
					state Future<ErrorOr<GetStorageMetricsReply>> metricsRequest = interf.getStorageMetrics.tryGetReply(
					    GetStorageMetricsRequest(), TaskPriority::DataDistributionLaunch);

					state ErrorOr<GetStorageMetricsReply> rep = wait(metricsRequest);
					if (rep.isError()) {
						// Delay 1s to avoid tight spin loop in case all options fail
						wait(delay(1.0));
						continue;
					}
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

				TraceEvent("DataLossRecovery").detail("Phase", "StartMoveKeys");
				std::unique_ptr<MoveKeysParams> params;
				if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
					UID dataMoveId = newDataMoveId(deterministicRandom()->randomUInt64(),
					                               AssignEmptyRange(false),
					                               DataMoveType::PHYSICAL,
					                               DataMovementReason::TEAM_HEALTHY,
					                               UnassignShard(false));
					params = std::make_unique<MoveKeysParams>(dataMoveId,
					                                          std::vector<KeyRange>{ keys },
					                                          dest,
					                                          dest,
					                                          moveKeysLock,
					                                          Promise<Void>(),
					                                          &self->startMoveKeysParallelismLock,
					                                          &self->finishMoveKeysParallelismLock,
					                                          false,
					                                          UID(), // for logging only
					                                          &ddEnabledState,
					                                          CancelConflictingDataMoves::True,
					                                          Optional<BulkLoadState>());
				} else {
					UID dataMoveId = newDataMoveId(deterministicRandom()->randomUInt64(),
					                               AssignEmptyRange(false),
					                               DataMoveType::LOGICAL,
					                               DataMovementReason::TEAM_HEALTHY,
					                               UnassignShard(false));
					params = std::make_unique<MoveKeysParams>(dataMoveId,
					                                          keys,
					                                          dest,
					                                          dest,
					                                          moveKeysLock,
					                                          Promise<Void>(),
					                                          &self->startMoveKeysParallelismLock,
					                                          &self->finishMoveKeysParallelismLock,
					                                          false,
					                                          UID(), // for logging only
					                                          &ddEnabledState,
					                                          CancelConflictingDataMoves::True,
					                                          Optional<BulkLoadState>());
				}
				wait(moveKeys(cx, *params));
				break;
			} catch (Error& e) {
				TraceEvent("DataLossRecovery").error(e).detail("Phase", "MoveRangeError");
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
		ISimulator::ProcessInfo* process = g_simulator->getProcessByAddress(addr);
		ASSERT(process->addresses.contains(addr));
		g_simulator->killProcess(process, ISimulator::KillType::KillInstantly);
		TraceEvent("TestTeamKilled").detail("Address", addr);
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<DataLossRecoveryWorkload> DataLossRecoveryWorkloadFactory;
