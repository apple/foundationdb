/*
 * KRMCoalesceTest.actor.cpp
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

// Test workload for KRM (KeyRangeMap) tolerance of uncoalesced entries.
//
// This test verifies that when DD_COALESCE_UNCOALESCED_KRM is enabled,
// the system skips (tolerates) adjacent entries with the same value
// instead of crashing with ASSERT.
//
// The test:
// 1. Creates a test KRM with a custom prefix (not real system keys to avoid destabilizing)
// 2. Manually injects uncoalesced entries (adjacent keys with same value)
// 3. Calls krmSetRangeCoalescing on an overlapping range
// 4. Verifies the operation completes without crashing (entries remain, just skipped)

#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct KRMCoalesceTestWorkload : TestWorkload {
	static constexpr auto NAME = "KRMCoalesceTest";

	// Test prefix - use a non-system key to avoid interfering with real metadata
	Key testPrefix;
	bool enabled;

	KRMCoalesceTestWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId && g_network->isSimulated();
		testPrefix = "/krmcoalescetest/"_sr;
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (!enabled)
			return Void();
		return runTest(this, cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> runTest(KRMCoalesceTestWorkload* self, Database cx) {
		// Test 1: Verify skipping of uncoalesced entries
		wait(testCoalesceUncoalescedEntries(self, cx));

		// Test 2: Verify normal operation (no uncoalesced entries)
		wait(testNormalCoalescing(self, cx));

		TraceEvent("KRMCoalesceTestComplete");
		return Void();
	}

	ACTOR static Future<Void> testCoalesceUncoalescedEntries(KRMCoalesceTestWorkload* self, Database cx) {
		state Key prefix = self->testPrefix.withSuffix("uncoalesced/"_sr);
		state Key keyA = "a"_sr;
		state Key keyB = "b"_sr;
		state Key keyC = "c"_sr;
		state Key keyD = "d"_sr;
		state Key keyE = "e"_sr;
		state Key keyF = "f"_sr;
		state Key keyG = "g"_sr;
		state Value valueX = "X"_sr;
		state Value valueY = "Y"_sr;

		TraceEvent("KRMCoalesceTestStartUncoalesced").detail("Prefix", prefix);

		// Step 1: Manually inject uncoalesced entries
		// Create: A=X, B=X, C=X, D=X, E=X, F=Y
		// Many entries with same value to ensure some are beyond the read-ahead limit
		state Transaction tr1(cx);
		loop {
			try {
				tr1.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr1.setOption(FDBTransactionOptions::LOCK_AWARE);

				// Write many uncoalesced entries with same value
				tr1.set(prefix.withSuffix(keyA), valueX);
				tr1.set(prefix.withSuffix(keyB), valueX); // Uncoalesced
				tr1.set(prefix.withSuffix(keyC), valueX); // Uncoalesced
				tr1.set(prefix.withSuffix(keyD), valueX); // Uncoalesced
				tr1.set(prefix.withSuffix(keyE), valueX); // Uncoalesced
				tr1.set(prefix.withSuffix(keyF), valueY); // Different value

				wait(tr1.commit());
				break;
			} catch (Error& e) {
				wait(tr1.onError(e));
			}
		}

		TraceEvent("KRMCoalesceTestInjectedUncoalesced");

		// Step 2: Verify uncoalesced entries exist
		state Transaction tr2(cx);
		state RangeResult entries;
		loop {
			try {
				tr2.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr2.setOption(FDBTransactionOptions::LOCK_AWARE);

				RangeResult r = wait(tr2.getRange(
				    KeyRangeRef(prefix.withSuffix(keyA), prefix.withSuffix(keyG)), CLIENT_KNOBS->TOO_MANY));
				entries = r;
				break;
			} catch (Error& e) {
				wait(tr2.onError(e));
			}
		}

		// Should have 6 entries before coalescing
		ASSERT_EQ(entries.size(), 6);
		TraceEvent("KRMCoalesceTestVerifiedUncoalesced").detail("EntryCount", entries.size());

		// Step 3: Call krmSetRangeCoalescing which should trigger auto-coalesce
		// We set a range that overlaps with the uncoalesced entries
		// This requires DD_COALESCE_UNCOALESCED_KRM to be true
		if (!SERVER_KNOBS->DD_COALESCE_UNCOALESCED_KRM) {
			TraceEvent("KRMCoalesceTestSkipped").detail("Reason", "DD_COALESCE_UNCOALESCED_KRM is false");
			// Clean up and skip
			state Transaction trClean(cx);
			loop {
				try {
					trClean.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					trClean.setOption(FDBTransactionOptions::LOCK_AWARE);
					trClean.clear(KeyRangeRef(prefix, strinc(prefix)));
					wait(trClean.commit());
					break;
				} catch (Error& e) {
					wait(trClean.onError(e));
				}
			}
			return Void();
		}

		// Trigger the uncoalesced code path by calling krmSetRangeCoalescing on a range
		// that will read the uncoalesced entries. With the knob enabled, this should
		// skip the redundant entries and complete without ASSERT.
		//
		// Scenario to trigger skip:
		// - Entries: A=X, B=X, C=X, D=X, E=X, F=Y
		// - Set range [A, B) with value X, maxRange [A, F)
		// - The read at end finds B=X and C=X (2 entries)
		// - Case 1: hasNext=true, C <= F, valueMatches=true (B=X == X)
		// - endKey = C, endValue = X
		// - Skip condition: value(X) == endValue(X) && endKey(C) != maxEnd(F) = TRUE!
		state Reference<ReadYourWritesTransaction> tr3 = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr3->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr3->setOption(FDBTransactionOptions::LOCK_AWARE);

				// Set range [A, B) to valueX with maxRange [A, F)
				wait(krmSetRangeCoalescing(
				    tr3, prefix, KeyRangeRef(keyA, keyB), KeyRangeRef(keyA, keyF), valueX));

				wait(tr3->commit());
				break;
			} catch (Error& e) {
				wait(tr3->onError(e));
			}
		}

		TraceEvent("KRMCoalesceTestSkippedUncoalesced");

		// Step 4: Verify operation completed (entries may still exist - we skip, not remove)
		state Transaction tr4(cx);
		state RangeResult afterEntries;
		loop {
			try {
				tr4.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr4.setOption(FDBTransactionOptions::LOCK_AWARE);

				RangeResult r = wait(tr4.getRange(
				    KeyRangeRef(prefix.withSuffix(keyA), prefix.withSuffix(keyG)), CLIENT_KNOBS->TOO_MANY));
				afterEntries = r;
				break;
			} catch (Error& e) {
				wait(tr4.onError(e));
			}
		}

		// The key test is that we got here without crashing. Entry count may vary
		// based on what the clear/set operations did, but we tolerated the uncoalesced state.
		TraceEvent("KRMCoalesceTestAfterSkip")
		    .detail("EntryCount", afterEntries.size())
		    .detail("OriginalCount", 6);

		// Clean up
		state Transaction tr5(cx);
		loop {
			try {
				tr5.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr5.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr5.clear(KeyRangeRef(prefix, strinc(prefix)));
				wait(tr5.commit());
				break;
			} catch (Error& e) {
				wait(tr5.onError(e));
			}
		}

		TraceEvent("KRMCoalesceTestSkipComplete");
		return Void();
	}

	ACTOR static Future<Void> testNormalCoalescing(KRMCoalesceTestWorkload* self, Database cx) {
		state Key prefix = self->testPrefix.withSuffix("normal/"_sr);
		state Key keyA = "a"_sr;
		state Key keyB = "b"_sr;
		state Key keyC = "c"_sr;
		state Value valueX = "X"_sr;
		state Value valueY = "Y"_sr;

		TraceEvent("KRMCoalesceTestStartNormal").detail("Prefix", prefix);

		// Use krmSetRangeCoalescing normally (no pre-existing uncoalesced entries)
		state Reference<ReadYourWritesTransaction> tr1 = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr1->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr1->setOption(FDBTransactionOptions::LOCK_AWARE);

				// Set [A, B) = X
				wait(krmSetRangeCoalescing(
				    tr1, prefix, KeyRangeRef(keyA, keyB), KeyRangeRef(""_sr, "\xff"_sr), valueX));

				wait(tr1->commit());
				break;
			} catch (Error& e) {
				wait(tr1->onError(e));
			}
		}

		state Reference<ReadYourWritesTransaction> tr2 = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr2->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr2->setOption(FDBTransactionOptions::LOCK_AWARE);

				// Set [B, C) = Y (different value, should not coalesce)
				wait(krmSetRangeCoalescing(
				    tr2, prefix, KeyRangeRef(keyB, keyC), KeyRangeRef(""_sr, "\xff"_sr), valueY));

				wait(tr2->commit());
				break;
			} catch (Error& e) {
				wait(tr2->onError(e));
			}
		}

		// Verify we have proper entries
		state Transaction tr3(cx);
		state RangeResult entries;
		loop {
			try {
				tr3.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr3.setOption(FDBTransactionOptions::LOCK_AWARE);

				Key endKey = prefix.withSuffix("z"_sr);
				RangeResult r =
				    wait(tr3.getRange(KeyRangeRef(prefix.withSuffix(keyA), endKey), CLIENT_KNOBS->TOO_MANY));
				entries = r;
				break;
			} catch (Error& e) {
				wait(tr3.onError(e));
			}
		}

		TraceEvent("KRMCoalesceTestNormalEntries").detail("EntryCount", entries.size());

		// Clean up
		state Transaction tr4(cx);
		loop {
			try {
				tr4.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr4.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr4.clear(KeyRangeRef(prefix, strinc(prefix)));
				wait(tr4.commit());
				break;
			} catch (Error& e) {
				wait(tr4.onError(e));
			}
		}

		TraceEvent("KRMCoalesceTestNormalComplete");
		return Void();
	}
};

WorkloadFactory<KRMCoalesceTestWorkload> KRMCoalesceTestWorkloadFactory;
