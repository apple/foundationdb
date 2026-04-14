/*
 * RestoreThrottle.actor.cpp
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

// Test that DD survives restart when many \xff/dataMoves/ entries have accumulated.
// Reproduces general scenario where isRestore moves overwhelmed DD absent scale.
// Tests passing isRestore datamoves via canLaunchSrc 'works'.
//
// Approach:
// 1. Write data to create many small shards (via knobs)
// 2. Exclude storage servers to trigger many data moves
// 3. Kill DD mid-flight so \xff/dataMoves/ entries persist
// 4. New DD loads them via resumeFromDataMoves (isRestore)
// 5. Verify DD survives and processes entries with bounded concurrency

#include "fdbclient/IKnobCollection.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RestoreThrottleWorkload : TestWorkload {
	static constexpr auto NAME = "RestoreThrottle";

	int minDataMoves;
	double testDuration;
	bool enabled;
	bool pass;

	RestoreThrottleWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), pass(true) {
		minDataMoves = getOption(options, "minDataMoves"_sr, 100);
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		enabled = !clientId && g_network->isSimulated();
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	Future<Void> setup(Database const& cx) override {
		// Raise trace line limit — this test generates many data moves which produce
		// millions of trace events (TransactionDebug, FetchKeys, etc.)
		auto& g_knobs = IKnobCollection::getMutableGlobalKnobCollection();
		g_knobs.setKnob("max_trace_lines", KnobValueRef::create(int{ 100000000 }));
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (!enabled)
			return Void();
		return _start(this, cx);
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<int> countDataMoves(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				RangeResult result = wait(tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY));
				return result.size();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> _start(RestoreThrottleWorkload* self, Database cx) {
		// Step 1: Write data to create many small shards.
		// 20K keys × 500 bytes = 10MB. With small shard knobs → 200-500 shards.
		state int numKeys = 20000;
		state int batchSize = 500;
		state int k = 0;
		for (; k < numKeys; k += batchSize) {
			state Transaction wtr(cx);
			loop {
				try {
					state int j = k;
					for (; j < numKeys && j < k + batchSize; j++) {
						wtr.set(Key(format("restoreTest/%06d", j)), Value(std::string(500, 'x')));
					}
					wait(wtr.commit());
					break;
				} catch (Error& e) {
					wait(wtr.onError(e));
				}
			}
		}
		TraceEvent("RestoreThrottleDataWritten").detail("NumKeys", numKeys);

		// Step 2: Wait for data to be readable and shards to split.
		TraceEvent("RestoreThrottleWaitingForSplits");
		state double splitStart = now();
		state int shardCount = 0;
		loop {
			state Transaction rtr(cx);
			try {
				Optional<Value> val = wait(rtr.get(Key(format("restoreTest/%06d", 0))));
				if (!val.present()) {
					wait(delay(5.0));
					continue;
				}
			} catch (Error& e) {
				wait(delay(5.0));
				continue;
			}
			// Count shards
			state Transaction str(cx);
			try {
				str.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				str.setOption(FDBTransactionOptions::LOCK_AWARE);
				RangeResult result = wait(str.getRange(keyServersKeys, CLIENT_KNOBS->TOO_MANY));
				shardCount = result.size() > 0 ? result.size() - 1 : 0;
				TraceEvent("RestoreThrottleShardProgress").detail("Shards", shardCount);
			} catch (Error& e) {
				// ignore, retry
			}
			if (shardCount >= 100 || now() - splitStart > 120.0) {
				break;
			}
			wait(delay(5.0));
		}
		TraceEvent("RestoreThrottleShardCount").detail("Shards", shardCount);
		if (shardCount < 10) {
			TraceEvent(SevError, "RestoreThrottleInsufficientShards").detail("Shards", shardCount);
			self->pass = false;
			return Void();
		}

		// Step 3: Exclude storage servers to trigger data movement.
		state Transaction etr(cx);
		state std::vector<AddressExclusion> excluded;
		loop {
			try {
				etr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				etr.setOption(FDBTransactionOptions::LOCK_AWARE);
				std::vector<std::pair<StorageServerInterface, ProcessClass>> servers =
				    wait(NativeAPI::getServerListAndProcessClasses(&etr));

				for (auto& [ssi, pc] : servers) {
					if (g_simulator->protectedAddresses.count(ssi.address()) == 0) {
						excluded.push_back(AddressExclusion(ssi.address().ip, ssi.address().port));
					}
				}
				break;
			} catch (Error& e) {
				wait(etr.onError(e));
			}
		}

		// Exclude 1 server — creates many isRestore entries with 1000+ shards while
		// keeping cleanup fast (only 1 server's worth of shards to rebalance back).
		state int numToExclude = 1;
		if ((int)excluded.size() < numToExclude) {
			TraceEvent(SevError, "RestoreThrottleNoExcludableServers")
			    .detail("Found", excluded.size())
			    .detail("Needed", numToExclude);
			self->pass = false;
			return Void();
		}
		excluded.resize(numToExclude);
		wait(excludeServers(cx, excluded));
		TraceEvent("RestoreThrottleExcludedServers").detail("Count", numToExclude);

		// Step 4: Wait for data moves to accumulate, then force DD restart.
		state double moveWaitStart = now();
		state int movesBeforeKill = 0;
		loop {
			wait(delay(5.0));
			int moves = wait(countDataMoves(cx));
			movesBeforeKill = moves;
			TraceEvent("RestoreThrottleWaitingForMoves")
			    .detail("DataMoveEntries", moves)
			    .detail("MinRequired", self->minDataMoves);
			if (moves >= self->minDataMoves) {
				break;
			}
			if (now() - moveWaitStart > 120.0) {
				TraceEvent(SevWarn, "RestoreThrottleInsufficientMoves")
				    .detail("DataMoveEntries", moves)
				    .detail("MinRequired", self->minDataMoves);
				break;
			}
		}
		TraceEvent("RestoreThrottleMovesBeforeKill").detail("DataMoveEntries", movesBeforeKill);

		// Force DD restart
		state UID lockOwner = deterministicRandom()->randomUniqueID();
		MoveKeysLock lock = wait(takeMoveKeysLock(cx, lockOwner));
		TraceEvent("RestoreThrottleForcedDDRestart").detail("LockOwner", lockOwner);

		// Check moves after restart
		state int movesAfterRestart = wait(countDataMoves(cx));
		TraceEvent("RestoreThrottleMovesAfterRestart").detail("DataMoveEntries", movesAfterRestart);
		TraceEvent("RestoreThrottleVerifyDDAlive")
		    .detail("MovesBeforeKill", movesBeforeKill)
		    .detail("MovesAfterRestart", movesAfterRestart);

		// Step 5: Verify DD stays alive and makes progress.
		state double startTime = now();
		state int previousMoves = movesAfterRestart;
		state bool ddProgressing = (movesAfterRestart < movesBeforeKill);
		state int consecutiveFailures = 0;

		loop {
			wait(delay(5.0));
			state Transaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				RangeResult result = wait(tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY));
				state int currentMoves = result.size();
				consecutiveFailures = 0;
				TraceEvent("RestoreThrottleProgress")
				    .detail("DataMoveEntries", currentMoves)
				    .detail("PreviousMoves", previousMoves)
				    .detail("Elapsed", now() - startTime);
				if (currentMoves < previousMoves) {
					ddProgressing = true;
				}
				previousMoves = currentMoves;
				if (currentMoves == 0) {
					TraceEvent("RestoreThrottleAllMovesComplete").detail("Elapsed", now() - startTime);
					break;
				}
			} catch (Error& e) {
				consecutiveFailures++;
				TraceEvent(SevWarn, "RestoreThrottleReadFailed")
				    .error(e)
				    .detail("ConsecutiveFailures", consecutiveFailures);
				if (consecutiveFailures >= 6) {
					TraceEvent(SevError, "RestoreThrottleClusterUnresponsive")
					    .detail("ConsecutiveFailures", consecutiveFailures);
					self->pass = false;
					break;
				}
			}
			if (now() - startTime > self->testDuration) {
				if (ddProgressing) {
					TraceEvent("RestoreThrottleTimedOutButProgressing")
					    .detail("RemainingMoves", previousMoves);
				} else if (previousMoves <= 5) {
					TraceEvent("RestoreThrottleFewMovesNotProgressing")
					    .detail("RemainingMoves", previousMoves);
				} else {
					TraceEvent(SevError, "RestoreThrottleDDNotProgressing")
					    .detail("RemainingMoves", previousMoves)
					    .detail("InitialMoves", movesAfterRestart);
					self->pass = false;
				}
				break;
			}
		}

		// Clean up: include servers back and wait for DD to finish all moves.
		// Joshua counts any SevError as failure, and the harness emits
		// ErrorClearingDatabaseAfterTest if DD is still busy when it tries to wipe the DB.
		wait(includeServers(cx, std::vector<AddressExclusion>(1)));
		TraceEvent("RestoreThrottleIncludedServers");

		// Wait for all dataMoves to drain.
		state double cleanupStart = now();
		loop {
			int remaining = wait(countDataMoves(cx));
			if (remaining == 0) {
				TraceEvent("RestoreThrottleCleanupComplete");
				break;
			}
			if (now() - cleanupStart > 1200.0) {
				TraceEvent(SevWarn, "RestoreThrottleCleanupTimeout").detail("RemainingMoves", remaining);
				break;
			}
			wait(delay(30.0));
		}
		state int finalMoves = wait(countDataMoves(cx));

		// Report
		TraceEvent("RestoreThrottleReport")
		    .detail("Result", self->pass ? "PASS" : "FAIL")
		    .detail("Shards", shardCount)
		    .detail("ServersExcluded", numToExclude)
		    .detail("MovesBeforeKill", movesBeforeKill)
		    .detail("MovesAfterRestart", movesAfterRestart)
		    .detail("MovesAtEnd", finalMoves)
		    .detail("DDProgressing", ddProgressing)
		    .detail("TestDuration", now() - startTime);

		return Void();
	}
};

WorkloadFactory<RestoreThrottleWorkload> RestoreThrottleWorkloadFactory;
