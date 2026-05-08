/*
 * KRMCoalescingFragmentation.cpp
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

#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/tester/workloads.h"

// Reproduces the krmSetRangeCoalescing fragmentation bug from
// removeOldDestinations() in MoveKeys.cpp.
//
// When multiple krmSetRangeCoalescing calls on the same KRM prefix run
// concurrently in one transaction (via waitForAll), each call's snapshot
// reads see the committed DB state but not each other's writes. The
// coalescing logic extends clear ranges beyond the requested boundaries,
// and the extended clears overlap and erase each other's boundary sets.
// The result is adjacent entries with identical values (fragmentation).
//
// This test:
//   1. Sets up KRM state mimicking a server with alternating owned/unowned ranges
//   2. Fires concurrent krmSetRangeCoalescing calls (the removeOldDestinations pattern)
//   3. Verifies adjacent same-value entries were created
//   4. Triggers the ASSERT(value != endValue || endKey == maxWithPrefix.end) by
//      calling krmSetRangeCoalescing with range.end between the fragmented pair

struct KRMCoalescingFragmentationWorkload : TestWorkload {
	static constexpr auto NAME = "KRMCoalescingFragmentation";

	Key testPrefix;
	bool fragmentationCreated;
	bool assertionFired;

	explicit KRMCoalescingFragmentationWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), testPrefix("KRMFragTest/"_sr), fragmentationCreated(false), assertionFired(false) {}

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return setupInitialState(cx);
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return triggerFragmentation(cx);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0)
			return true;
		TraceEvent("KRMFragTestCheck")
		    .detail("FragmentationCreated", fragmentationCreated)
		    .detail("AssertionFired", assertionFired);
		return fragmentationCreated && assertionFired;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

private:
	// Establish initial KRM state mimicking a server with alternating
	// owned/unowned sub-ranges:
	//
	//   "" -> "1"    "d" -> ""    "j" -> "1"    "\xff\xff" -> ""
	//
	// Server owns ["","d") and ["j","\xff\xff") with a gap at ["d","j").
	// The "" entry at "d" is what causes both gap actors to extend their
	// coalescing through it, creating the overlapping clears.
	//
	// Uses allKeys = ["", "\xff\xff") as maxRange and serverKeysTrue/False
	// values, matching exactly how removeOldDestinations is called in
	// startMoveKeys (MoveKeys.cpp:797).
	Future<Void> setupInitialState(Database cx) {
		auto tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			Error err;
			try {
				co_await krmSetRange(tr, testPrefix, allKeys, serverKeysTrue);
				co_await tr->commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			Error err;
			try {
				co_await krmSetRange(tr, testPrefix, KeyRangeRef("d"_sr, "j"_sr), serverKeysFalse);
				co_await tr->commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			Error err;
			try {
				RangeResult result = co_await krmGetRanges(tr, testPrefix, allKeys);
				ASSERT(result.size() == 4);
				ASSERT(result[0].key == allKeys.begin && result[0].value == serverKeysTrue);
				ASSERT(result[1].key == "d"_sr && result[1].value == serverKeysFalse);
				ASSERT(result[2].key == "j"_sr && result[2].value == serverKeysTrue);
				ASSERT(result[3].key == allKeys.end);
				TraceEvent("KRMFragTestSetupDone").detail("Entries", result.size());
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}
	}

	// Reproduce the removeOldDestinations pattern, then trigger the assert.
	Future<Void> triggerFragmentation(Database cx) {
		// Step 1: Two concurrent krmSetRangeCoalescing calls clearing gaps
		// ["c","e") and ["g","k") on the same prefix with value serverKeysFalse.
		//
		// This is exactly the pattern in removeOldDestinations (MoveKeys.cpp:787-804):
		// each gap between source shards gets its own krmSetRangeCoalescing call,
		// all fired concurrently via waitForAll on the same RYW transaction.
		//
		// Each call's coalescing extends through the existing "" entry at "d":
		//
		//   Gap 1 ["c","e"): end query finds "d"->""  matching value "",
		//     extends endKey right to "j" (Case 1). Clears ["c","j").
		//
		//   Gap 2 ["g","k"): begin query finds "d"->"" matching value "",
		//     extends beginKey left to "d". Clears ["d","k").
		//
		// The clears overlap: Gap 1 sets "j"->"1" inside Gap 2's clear,
		// Gap 2 sets "d"->""  inside Gap 1's clear. One erases the other.
		auto tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			Error err;
			try {
				std::vector<Future<Void>> actors;
				actors.push_back(krmSetRangeCoalescing(
				    tr, testPrefix, KeyRangeRef("c"_sr, "e"_sr), allKeys, serverKeysFalse));
				actors.push_back(krmSetRangeCoalescing(
				    tr, testPrefix, KeyRangeRef("g"_sr, "k"_sr), allKeys, serverKeysFalse));
				co_await waitForAll(actors);
				co_await tr->commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		// Step 2: Read back and find the fragmented pair.
		// Depending on execution order, we get one of:
		//   Order 1: "" -> "1", "c" -> "", "d" -> "", "k" -> "1", "\xff\xff" -> ""
		//                       ^^^^^^^^^^^^^^^^ fragmented
		//   Order 2: "" -> "1", "c" -> "", "j" -> "1", "k" -> "1", "\xff\xff" -> ""
		//                                  ^^^^^^^^^^^^^^^^^^ fragmented
		Key fragKey1;
		Key fragKey2;
		Value fragValue;

		tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			Error err;
			try {
				RangeResult result = co_await krmGetRanges(tr, testPrefix, allKeys);
				TraceEvent evt("KRMFragTestResult");
				evt.detail("NumEntries", result.size());
				for (int i = 0; i < result.size(); i++) {
					evt.detail(format("Key%d", i), result[i].key);
					evt.detail(format("Val%d", i), result[i].value);
				}

				for (int i = 1; i < result.size(); i++) {
					if (result[i].value == result[i - 1].value) {
						fragKey1 = result[i - 1].key;
						fragKey2 = result[i].key;
						fragValue = result[i].value;
						fragmentationCreated = true;
						TraceEvent(SevWarn, "KRMFragTestBugReproduced")
						    .detail("Key1", fragKey1)
						    .detail("Key2", fragKey2)
						    .detail("Value", fragValue);
						break;
					}
				}

				if (!fragmentationCreated) {
					TraceEvent(SevWarn, "KRMFragTestNoFragmentation")
					    .detail("Message",
					            "Concurrent krmSetRangeCoalescing did not produce fragmentation - bug may be fixed");
				}
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}

		if (!fragmentationCreated)
			co_return;

		// Step 3: Trigger ASSERT(value != endValue || endKey == maxWithPrefix.end)
		// in krmSetRangeCoalescing_ (KeyRangeMap.cpp:290).
		//
		// We call krmSetRangeCoalescing with:
		//   - range.end landing between the two fragmented keys
		//   - value equal to the fragmented entries' value
		//   - maxRange = allKeys (matching DD's usage)
		//
		// The end query reads both fragmented entries. Case 1 sets:
		//   endKey = fragKey2, endValue = fragKey2's value = fragValue
		// Since value == endValue and endKey != maxWithPrefix.end, the ASSERT fires.
		//
		// Example with Order 1 fragmentation ("c"->""  "d"->""):
		//   krmSetRangeCoalescing(tr, prefix, ["a", "c\x00"), allKeys, "")
		//   End query for "c\x00": lastLessOrEqual -> "c"->""
		//                          next entry      -> "d"->""
		//   Case 1: endKey="d", endValue=""
		//   ASSERT("" != "" || "d" == "\xff\xff") -> ASSERT(false) -> internal_error
		Key triggerEnd = keyAfter(fragKey1);
		ASSERT(triggerEnd < fragKey2);

		tr = makeReference<ReadYourWritesTransaction>(cx);
		try {
			co_await krmSetRangeCoalescing(tr, testPrefix, KeyRangeRef("a"_sr, triggerEnd), allKeys, fragValue);

			TraceEvent(SevWarn, "KRMFragTestAssertDidNotFire")
			    .detail("TriggerEnd", triggerEnd)
			    .detail("FragValue", fragValue);
		} catch (Error& e) {
			if (e.code() == error_code_internal_error) {
				assertionFired = true;
				TraceEvent("KRMFragTestAssertTriggered")
				    .detail("FragKey1", fragKey1)
				    .detail("FragKey2", fragKey2)
				    .detail("FragValue", fragValue)
				    .detail("TriggerEnd", triggerEnd);
			} else {
				throw;
			}
		}
	}
};

WorkloadFactory<KRMCoalescingFragmentationWorkload> KRMCoalescingFragmentationWorkloadFactory;
