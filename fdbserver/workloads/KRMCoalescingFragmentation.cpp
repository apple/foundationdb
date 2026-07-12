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
#include "fdbserver/core/MoveKeys.h"
#include "fdbserver/tester/workloads.h"

// Regression test for the krmSetRangeCoalescing fragmentation bug in
// removeOldDestinations() (MoveKeys.cpp).
//
// Calls the real removeOldDestinations with a test-only KRM prefix (in user
// keyspace, not system keyspace) so the test exercises the actual production
// code path without disturbing any system metadata.
//
// On UNFIXED code (concurrent waitForAll): produces adjacent same-value entries.
// On FIXED code (sequential co_await): produces a correctly coalesced map.

struct KRMCoalescingFragmentationWorkload : TestWorkload {
	static constexpr auto NAME = "KRMCoalescingFragmentation";

	Key testPrefix;
	bool success;

	explicit KRMCoalescingFragmentationWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), testPrefix("KRMFragTest/"_sr), success(false) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return runTest(cx);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0)
			return true;
		return success;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

private:
	Future<Void> runTest(Database cx) {
		// Step 1: Establish initial KRM state with alternating values.
		//   "" -> "1"    "d" -> ""    "j" -> "1"    "\xff\xff" -> ""
		{
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
		}
		{
			auto tr = makeReference<ReadYourWritesTransaction>(cx);
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
		}

		// Verify setup.
		{
			auto tr = makeReference<ReadYourWritesTransaction>(cx);
			loop {
				Error err;
				try {
					RangeResult result = co_await krmGetRanges(tr, testPrefix, allKeys);
					if (result.back().key < allKeys.end) {
						tr->reset();
						continue;
					}
					TraceEvent evt("KRMFragTestSetupVerify");
					evt.detail("NumEntries", result.size());
					for (int i = 0; i < result.size(); i++) {
						evt.detail(format("Key%d", i), result[i].key);
						evt.detail(format("Val%d", i), result[i].value);
					}
					ASSERT(result.size() == 4);
					ASSERT(result[0].value == serverKeysTrue);
					ASSERT(result[1].key == "d"_sr && result[1].value == serverKeysFalse);
					ASSERT(result[2].key == "j"_sr && result[2].value == serverKeysTrue);
					TraceEvent("KRMFragTestSetupDone").detail("Entries", result.size());
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr->onError(err);
			}
		}

		// Step 2: Call removeOldDestinations with the multi-gap pattern.
		// Shards to keep: ["a","c"), ["e","g"), ["k","m")
		// Gaps to clear: ["c","e") and ["g","k")
		// currentKeys: ["a","m")
		//
		// The existing "" entry at "d" causes both gap clears to extend their
		// coalescing through it, producing overlapping clears on unfixed code.
		{
			Arena arena;
			VectorRef<KeyRangeRef> shards;
			shards.push_back(arena, KeyRangeRef("a"_sr, "c"_sr));
			shards.push_back(arena, KeyRangeRef("e"_sr, "g"_sr));
			shards.push_back(arena, KeyRangeRef("k"_sr, "m"_sr));
			KeyRangeRef currentKeys("a"_sr, "m"_sr);

			auto tr = makeReference<ReadYourWritesTransaction>(cx);
			loop {
				Error err;
				try {
					co_await removeOldDestinations(tr, testPrefix, shards, currentKeys);
					co_await tr->commit();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr->onError(err);
			}
		}

		// Step 3: Read back and verify the exact expected KRM state.
		//
		// Sequential coalescing should produce:
		//   "" -> "1"   (server owns ["", "c"))
		//   "c" -> ""   (gaps ["c","e") and ["g","k") coalesced with existing "" region ["d","j"))
		//   "k" -> "1"  (server owns ["k", ...))
		//   <terminator>
		//
		// Gap 1 ["c","e") coalesces right through "d"->"" to "j", then Gap 2 ["g","k")
		// coalesces left through the now-extended "" region back to "c".
		{
			auto tr = makeReference<ReadYourWritesTransaction>(cx);
			loop {
				Error err;
				try {
					RangeResult result = co_await krmGetRanges(tr, testPrefix, allKeys);
					if (result.back().key < allKeys.end) {
						tr->reset();
						continue;
					}

					TraceEvent evt("KRMFragTestResult");
					evt.detail("NumEntries", result.size());
					for (int i = 0; i < result.size(); i++) {
						evt.detail(format("Key%d", i), result[i].key);
						evt.detail(format("Val%d", i), result[i].value);
					}

					bool fragmented = false;
					for (int i = 1; i < (int)result.size() - 1; i++) {
						if (result[i].value == result[i - 1].value) {
							fragmented = true;
							TraceEvent(SevError, "KRMFragTestFragmentationDetected")
							    .detail("Key1", result[i - 1].key)
							    .detail("Key2", result[i].key)
							    .detail("Value", result[i].value);
						}
					}

					if (fragmented) {
						TraceEvent(SevError, "KRMFragTestFailed")
						    .detail("Message", "removeOldDestinations produced fragmented KRM entries");
						break;
					}

					// Verify the exact expected transitions: the gaps were cleared and
					// coalesced, and the kept-shard boundaries are correct.
					ASSERT_EQ(result.size(), 4);
					ASSERT(result[0].key.empty() && result[0].value == serverKeysTrue);
					ASSERT(result[1].key == "c"_sr && result[1].value == serverKeysFalse);
					ASSERT(result[2].key == "k"_sr && result[2].value == serverKeysTrue);

					success = true;
					TraceEvent("KRMFragTestPassed").detail("Entries", result.size());
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr->onError(err);
			}
		}
	}
};

WorkloadFactory<KRMCoalescingFragmentationWorkload> KRMCoalescingFragmentationWorkloadFactory;
