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
// This test verifies that when DD_TOLERATE_UNCOALESCED_KRM is enabled,
// krmSetRangeCoalescing warns about adjacent entries with the same value
// and still applies the caller's mutation, rather than crashing with ASSERT.
//
// The test:
// 1. Creates a test KRM with a custom prefix
// 2. Manually injects uncoalesced entries (adjacent keys with same value)
// 3. Calls krmSetRangeCoalescing on an overlapping range
// 4. Verifies the operation completes without crashing AND the data is correct

#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct KRMCoalesceTestWorkload : TestWorkload {
	static constexpr auto NAME = "KRMCoalesceTest";

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
		wait(testCoalesceUncoalescedEntries(self, cx));
		wait(testNormalCoalescing(self, cx));
		TraceEvent("KRMCoalesceTestComplete");
		return Void();
	}

	// Helper to read all entries under a prefix in range [startSuffix, endSuffix)
	ACTOR static Future<RangeResult> readEntries(Database cx, Key prefix, Key startSuffix, Key endSuffix) {
		state Transaction tr(cx);
		loop {
			try {
				RangeResult r = wait(tr.getRange(
				    KeyRangeRef(prefix.withSuffix(startSuffix), prefix.withSuffix(endSuffix)), CLIENT_KNOBS->TOO_MANY));
				return r;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
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

		if (!SERVER_KNOBS->DD_TOLERATE_UNCOALESCED_KRM) {
			TraceEvent("KRMCoalesceTestSkipped").detail("Reason", "DD_TOLERATE_UNCOALESCED_KRM is false");
			return Void();
		}

		// Step 1: Inject uncoalesced entries
		// Create: prefix+A=X, prefix+B=X, prefix+C=X, prefix+D=X, prefix+E=X, prefix+F=Y
		// B through E are uncoalesced (same value X as A)
		state Transaction tr1(cx);
		loop {
			try {
				tr1.set(prefix.withSuffix(keyA), valueX);
				tr1.set(prefix.withSuffix(keyB), valueX);
				tr1.set(prefix.withSuffix(keyC), valueX);
				tr1.set(prefix.withSuffix(keyD), valueX);
				tr1.set(prefix.withSuffix(keyE), valueX);
				tr1.set(prefix.withSuffix(keyF), valueY);
				wait(tr1.commit());
				break;
			} catch (Error& e) {
				wait(tr1.onError(e));
			}
		}

		// Step 2: Verify 6 entries exist
		RangeResult beforeEntries = wait(readEntries(cx, prefix, keyA, keyG));
		ASSERT_EQ(beforeEntries.size(), 6);
		TraceEvent("KRMCoalesceTestInjectedUncoalesced").detail("EntryCount", beforeEntries.size());

		// Step 3: Call krmSetRangeCoalescing — triggers tolerance path
		// Set range [A, B) to valueX with maxRange [A, F)
		// The function reads entries near B, finds B=X and C=X (uncoalesced),
		// hits value(X)==endValue(X) && endKey(C)!=maxEnd(F), logs warning,
		// then performs: clear([A,C)), set(A,X), set(C,X)
		state Reference<ReadYourWritesTransaction> tr3 = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				wait(krmSetRangeCoalescing(tr3, prefix, KeyRangeRef(keyA, keyB), KeyRangeRef(keyA, keyF), valueX));
				wait(tr3->commit());
				break;
			} catch (Error& e) {
				wait(tr3->onError(e));
			}
		}

		TraceEvent("KRMCoalesceTestToleratedUncoalesced");

		// Step 4: Verify correctness — the mutation was applied, not silently dropped
		RangeResult afterEntries = wait(readEntries(cx, prefix, keyA, keyG));
		TraceEvent("KRMCoalesceTestAfterTolerate")
		    .detail("EntryCount", afterEntries.size())
		    .detail("OriginalCount", 6);

		// Verify A=X is present (the mutation was applied)
		ASSERT(afterEntries.size() >= 1);
		ASSERT(afterEntries[0].key == prefix.withSuffix(keyA));
		ASSERT(afterEntries[0].value == valueX);

		// Verify F=Y is still present (untouched by our operation)
		bool foundF = false;
		for (int i = 0; i < afterEntries.size(); i++) {
			if (afterEntries[i].key == prefix.withSuffix(keyF)) {
				ASSERT(afterEntries[i].value == valueY);
				foundF = true;
			}
		}
		ASSERT(foundF);

		// B should have been cleared (it was in the clear range [A, C))
		for (int i = 0; i < afterEntries.size(); i++) {
			ASSERT(afterEntries[i].key != prefix.withSuffix(keyB));
		}

		// Clean up
		state Transaction tr5(cx);
		loop {
			try {
				tr5.clear(KeyRangeRef(prefix, strinc(prefix)));
				wait(tr5.commit());
				break;
			} catch (Error& e) {
				wait(tr5.onError(e));
			}
		}

		TraceEvent("KRMCoalesceTestUncoalescedComplete");
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

		// Set [A, B) = X
		state Reference<ReadYourWritesTransaction> tr1 = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				wait(
				    krmSetRangeCoalescing(tr1, prefix, KeyRangeRef(keyA, keyB), KeyRangeRef(""_sr, "\xff"_sr), valueX));
				wait(tr1->commit());
				break;
			} catch (Error& e) {
				wait(tr1->onError(e));
			}
		}

		// Set [B, C) = Y (different value, should not coalesce with A)
		state Reference<ReadYourWritesTransaction> tr2 = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				wait(
				    krmSetRangeCoalescing(tr2, prefix, KeyRangeRef(keyB, keyC), KeyRangeRef(""_sr, "\xff"_sr), valueY));
				wait(tr2->commit());
				break;
			} catch (Error& e) {
				wait(tr2->onError(e));
			}
		}

		// Verify entries
		RangeResult entries = wait(readEntries(cx, prefix, keyA, "z"_sr));
		TraceEvent("KRMCoalesceTestNormalEntries").detail("EntryCount", entries.size());

		// Should have at least A=X and B=Y (plus possible end sentinel)
		ASSERT(entries.size() >= 2);
		ASSERT(entries[0].key == prefix.withSuffix(keyA));
		ASSERT(entries[0].value == valueX);
		ASSERT(entries[1].key == prefix.withSuffix(keyB));
		ASSERT(entries[1].value == valueY);

		// Clean up
		state Transaction tr4(cx);
		loop {
			try {
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
