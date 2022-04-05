/*
 *PhysicalShardMove.actor.cpp
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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/ServerCheckpoint.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include <cstdint>
#include <limits>

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
std::string printValue(const ErrorOr<Optional<Value>>& value) {
	if (value.isError()) {
		return value.getError().name();
	}
	return value.get().present() ? value.get().get().toString() : "Value Not Found.";
}
} // namespace

struct SSCheckpointWorkload : TestWorkload {
	FlowLock startMoveKeysParallelismLock;
	FlowLock finishMoveKeysParallelismLock;
	const bool enabled;
	bool pass;

	SSCheckpointWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(!clientId), pass(true) {}

	void validationFailed(ErrorOr<Optional<Value>> expectedValue, ErrorOr<Optional<Value>> actualValue) {
		TraceEvent(SevError, "TestFailed")
		    .detail("ExpectedValue", printValue(expectedValue))
		    .detail("ActualValue", printValue(actualValue));
		pass = false;
	}

	std::string description() const override { return "SSCheckpoint"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (!enabled) {
			return Void();
		}
		return _start(this, cx);
	}

	ACTOR Future<Void> _start(SSCheckpointWorkload* self, Database cx) {
		int ignore = wait(setDDMode(cx, 0));
		state Key keyA = "TestKeyA"_sr;
		state Key keyB = "TestKeyB"_sr;
		state Key keyC = "TestKeyC"_sr;
		state Value testValue = "TestValue"_sr;

		Version ignore1 = wait(self->writeAndVerify(self, cx, keyA, testValue));
		Version ignore2 = wait(self->writeAndVerify(self, cx, keyB, testValue));
		Version ignore3 = wait(self->writeAndVerify(self, cx, keyC, testValue));

		TraceEvent("TestValueWritten").log();

		state std::unordered_set<UID> excludes;
		state int teamSize = 3;
		state std::vector<UID> teamA = wait(self->moveShard(self, cx, KeyRangeRef(keyA, keyB), teamSize, &excludes));
		// state std::vector<UID> teamB = wait(self->moveShard(self, cx, KeyRangeRef(keyB, keyC), teamSize, &excludes));

		wait(self->readAndVerify(self, cx, keyA, testValue));
		wait(self->readAndVerify(self, cx, keyB, testValue));
		wait(self->readAndVerify(self, cx, keyC, testValue));
		TraceEvent("TestValueVerified").log();

		int ignore = wait(setDDMode(cx, 1));
		return Void();
	}

	ACTOR Future<Void> readAndVerify(SSCheckpointWorkload* self,
	                                 Database cx,
	                                 Key key,
	                                 ErrorOr<Optional<Value>> expectedValue) {
		state Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

		loop {
			try {
				state Version readVersion = wait(tr.getReadVersion());
				state Optional<Value> res = wait(timeoutError(tr.get(key), 30.0));
				const bool equal = !expectedValue.isError() && res == expectedValue.get();
				if (!equal) {
					self->validationFailed(expectedValue, ErrorOr<Optional<Value>>(res));
				}
				break;
			} catch (Error& e) {
				TraceEvent("TestReadError").errorUnsuppressed(e);
				if (expectedValue.isError() && expectedValue.getError().code() == e.code()) {
					break;
				}
				wait(tr.onError(e));
			}
		}

		TraceEvent("TestReadSuccess").detail("Version", readVersion);

		return Void();
	}

	ACTOR Future<Version> writeAndVerify(SSCheckpointWorkload* self, Database cx, Key key, Optional<Value> value) {
		state Transaction tr(cx);
		state Version version;
		loop {
			state UID debugID = deterministicRandom()->randomUniqueID();
			try {
				// tr.setOption(FDBTransactionOptions::DEBUG_DUMP);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.debugTransaction(debugID);
				if (value.present()) {
					tr.set(key, value.get());
				} else {
					tr.clear(key);
				}
				wait(timeoutError(tr.commit(), 30.0));
				version = tr.getCommittedVersion();
				break;
			} catch (Error& e) {
				TraceEvent("TestCommitError").errorUnsuppressed(e);
				wait(tr.onError(e));
			}
		}

		TraceEvent("TestCommitSuccess").detail("CommitVersion", tr.getCommittedVersion()).detail("DebugID", debugID);

		wait(self->readAndVerify(self, cx, key, value));

		return version;
	}

	// Move keys to a random selected team consisting of a single SS, after disabling DD, so that keys won't be
	// kept in the new team until DD is enabled.
	// Returns the address of the single SS of the new team.
	ACTOR Future<std::vector<UID>> moveShard(SSCheckpointWorkload* self,
	                                         Database cx,
	                                         KeyRange keys,
	                                         int teamSize,
	                                         std::unordered_set<UID>* excludes) {
		// Disable DD to avoid DD undoing of our move.
		int ignore = wait(setDDMode(cx, 0));
		state std::vector<UID> dests;

		// Pick a random SS as the dest, keys will reside on a single server after the move.
		std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
		ASSERT(interfs.size() > teamSize);
		while (dests.size() < teamSize) {
			const auto& interf = interfs[deterministicRandom()->randomInt(0, interfs.size())];
			if (excludes->count(interf.uniqueID) == 0) {
				dests.push_back(interf.uniqueID);
				excludes->insert(interf.uniqueID);
			}
		}

		state UID owner = deterministicRandom()->randomUniqueID();
		// state Key ownerKey = "\xff/moveKeysLock/Owner"_sr;
		state DDEnabledState ddEnabledState;

		state Transaction tr(cx);

		loop {
			try {
				// BinaryWriter wrMyOwner(Unversioned());
				// wrMyOwner << owner;
				// tr.set(ownerKey, wrMyOwner.toValue());
				TraceEvent("TestMoveShard").detail("Range", keys.toString());
				// DEBUG_MUTATION("TestMoveShard", self->commitVersion, m, pProxyCommitData->dbgid).detail("To", tags);

				state MoveKeysLock moveKeysLock = wait(takeMoveKeysLock(cx, owner));

				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state RangeResult dataMoves = wait(tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY));
				Version readVersion = wait(tr.getReadVersion());
				TraceEvent("TestMoveShardReadDataMoves")
				    .detail("DataMoves", dataMoves.size())
				    .detail("ReadVersion", readVersion);
				// wait(tr.commit());
				state int i = 0;
				for (; i < dataMoves.size(); ++i) {
					UID dataMoveID = decodeDataMoveKey(dataMoves[i].key);
					state DataMoveMetaData dataMove = decodeDataMoveValue(dataMoves[i].value);
					ASSERT(dataMoveID == dataMove.id);
					TraceEvent("TestCancelDataMoveBegin").detail("DataMove", dataMove.toString());
					wait(cleanUpDataMove(cx, dataMoveID, moveKeysLock, dataMove.range, true, &ddEnabledState));
					TraceEvent("TestCancelDataMoveEnd").detail("DataMove", dataMove.toString());
				}

				wait(moveKeys(cx,
				              keys,
				              dests,
				              dests,
				              moveKeysLock,
				              Promise<Void>(),
				              &self->startMoveKeysParallelismLock,
				              &self->finishMoveKeysParallelismLock,
				              false,
				              deterministicRandom()->randomUniqueID(), // for logging only
				              deterministicRandom()->randomUniqueID(),
							  true,
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

		TraceEvent("TestMoveShardComplete").detail("Range", keys.toString()).detail("NewTeam", describe(dests));

		return dests;
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SSCheckpointWorkload> SSCheckpointWorkloadFactory("SSCheckpointWorkload");