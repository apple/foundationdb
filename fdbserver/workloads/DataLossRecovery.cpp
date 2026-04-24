/*
 * DataLossRecovery.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbserver/core/MoveKeys.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/core/Knobs.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/tester/workloads.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "fdbrpc/SimulatorProcessInfo.h"

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

	explicit DataLossRecoveryWorkload(WorkloadContext const& wcx)
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
		return _start(cx);
	}

	Future<Void> _start(Database cx) {
		Key key = "TestKey"_sr;
		Key endKey = "TestKey0"_sr;
		Value oldValue = "TestValue"_sr;
		Value newValue = "TestNewValue"_sr;

		TraceEvent("DataLossRecovery").detail("Phase", "Starting");
		co_await writeAndVerify(this, cx, key, oldValue);

		TraceEvent("DataLossRecovery").detail("Phase", "InitialWrites");
		// Move [key, endKey) to team: {address}.
		NetworkAddress address = co_await disableDDAndMoveShard(this, cx, KeyRangeRef(key, endKey));
		TraceEvent("DataLossRecovery").detail("Phase", "Moved");
		co_await readAndVerify(this, cx, key, oldValue);
		TraceEvent("DataLossRecovery").detail("Phase", "ReadAfterMove");

		// Kill team {address}, and expect read to timeout.
		killProcess(this, address);
		TraceEvent("DataLossRecovery").detail("Phase", "KilledProcess");
		co_await readAndVerify(this, cx, key, timed_out());
		TraceEvent("DataLossRecovery").detail("Phase", "VerifiedReadTimeout");

		// Reenable DD and exclude address as fail, so that [key, endKey) will be dropped and moved to a new team.
		// Expect read to return 'value not found'.
		co_await setDDMode(cx, 1);
		co_await exclude(cx, address);
		TraceEvent("DataLossRecovery").detail("Phase", "Excluded");
		co_await readAndVerify(this, cx, key, Optional<Value>());
		TraceEvent("DataLossRecovery").detail("Phase", "VerifiedDataDropped");

		// Write will scceed.
		co_await writeAndVerify(this, cx, key, newValue);
	}

	Future<Void> readAndVerify(DataLossRecoveryWorkload* self,
	                           Database cx,
	                           Key key,
	                           ErrorOr<Optional<Value>> expectedValue) {
		Transaction tr(cx);

		while (true) {
			Error err;
			try {
				// add timeout to read so test fails faster if something goes wrong
				Optional<Value> res = co_await timeoutError(tr.get(key), 90.0);
				const bool equal = !expectedValue.isError() && res == expectedValue.get();
				if (!equal) {
					self->validationFailed(expectedValue, ErrorOr<Optional<Value>>(res));
				}
				break;
			} catch (Error& e) {
				err = e;
			}
			if (expectedValue.isError() && expectedValue.getError().code() == err.code()) {
				break;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> writeAndVerify(DataLossRecoveryWorkload* self, Database cx, Key key, Optional<Value> value) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				if (value.present()) {
					tr.set(key, value.get());
				} else {
					tr.clear(key);
				}
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		co_await self->readAndVerify(self, cx, key, value);
	}

	Future<Void> exclude(Database cx, NetworkAddress addr) {
		Transaction tr(cx);
		std::vector<AddressExclusion> servers;
		servers.push_back(AddressExclusion(addr.ip, addr.port));
		while (true) {
			Error err;
			try {
				co_await excludeServers(&tr, servers, true);
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		// Wait until all data are moved out of servers.
		std::set<NetworkAddress> inProgress = co_await checkForExcludingServers(cx, servers, true);
		ASSERT(inProgress.empty());

		TraceEvent("ExcludedFailedServer").detail("Address", addr.toString());
	}

	// Move keys to a random selected team consisting of a single SS, after disabling DD, so that keys won't be
	// kept in the new team until DD is enabled.
	// Returns the address of the single SS of the new team.
	Future<NetworkAddress> disableDDAndMoveShard(DataLossRecoveryWorkload* self, Database cx, KeyRange keys) {
		// Disable DD to avoid DD undoing of our move.
		co_await setDDMode(cx, 0);
		TraceEvent("DataLossRecovery").detail("Phase", "DisabledDD");
		NetworkAddress addr;

		// Pick a random SS as the dest, keys will reside on a single server after the move.
		std::vector<UID> dest;
		while (dest.empty()) {
			std::vector<StorageServerInterface> interfs = co_await getStorageServers(cx);
			if (!interfs.empty()) {
				StorageServerInterface interf = interfs[deterministicRandom()->randomInt(0, interfs.size())];
				if (!g_simulator->protectedAddresses.contains(interf.address())) {
					// We need to avoid selecting a storage server that is already dead at this point, otherwise
					// the test will hang. This is achieved by sending a GetStorageMetrics RPC. This is a necessary
					// check for this test because DD has been disabled and the proper mechanism that removes bad
					// storage servers are not taking place in the scope of this function.
					Future<ErrorOr<GetStorageMetricsReply>> metricsRequest = interf.getStorageMetrics.tryGetReply(
					    GetStorageMetricsRequest(), TaskPriority::DataDistributionLaunch);

					ErrorOr<GetStorageMetricsReply> rep = co_await metricsRequest;
					if (rep.isError()) {
						// Delay 1s to avoid tight spin loop in case all options fail
						co_await delay(1.0);
						continue;
					}
					dest.push_back(interf.uniqueID);
					addr = interf.address();
				}
			}
		}

		UID owner = deterministicRandom()->randomUniqueID();
		DDEnabledState ddEnabledState;

		Transaction tr(cx);

		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				BinaryWriter wrMyOwner(Unversioned());
				wrMyOwner << owner;
				tr.set(moveKeysLockOwnerKey, wrMyOwner.toValue());
				co_await tr.commit();

				MoveKeysLock moveKeysLock;
				moveKeysLock.myOwner = owner;

				TraceEvent("DataLossRecovery").detail("Phase", "StartMoveKeys");
				std::unique_ptr<MoveKeysParams> params;
				if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
					UID dataMoveId = newDataMoveId(deterministicRandom()->randomUInt64(),
					                               AssignEmptyRange(false),
					                               deterministicRandom()->random01() <
					                                       SERVER_KNOBS->DD_PHYSICAL_SHARD_MOVE_PROBABILITY
					                                   ? DataMoveType::PHYSICAL
					                                   : DataMoveType::LOGICAL,
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
					                                          Optional<BulkLoadTaskState>());
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
					                                          Optional<BulkLoadTaskState>());
				}
				co_await moveKeys(cx, *params);
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("DataLossRecovery").error(err).detail("Phase", "MoveRangeError");
			if (err.code() == error_code_movekeys_conflict) {
				// Conflict on moveKeysLocks with the current running DD is expected, just retry.
				tr.reset();
			} else {
				co_await tr.onError(err);
			}
		}

		TraceEvent("TestKeyMoved").detail("NewTeam", describe(dest)).detail("Address", addr.toString());

		Transaction validateTr(cx);
		while (true) {
			Error err;
			try {
				Standalone<VectorRef<const char*>> addresses = co_await validateTr.getAddressesForKey(keys.begin);
				// The move function is not what we are testing here, crash the test if the move fails.
				ASSERT(addresses.size() == 1);
				ASSERT(std::string(addresses[0]) == addr.toString());
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await validateTr.onError(err);
		}

		co_return addr;
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
