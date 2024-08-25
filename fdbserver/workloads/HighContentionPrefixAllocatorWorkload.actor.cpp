/*
 * HighContentionPrefixAllocatorWorkload.actor.cpp
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

#include "fdbclient/HighContentionPrefixAllocator.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// This workload tests the basic contract of the high contention allocator
struct HighContentionPrefixAllocatorWorkload : TestWorkload {
	static constexpr auto NAME = "HighContentionPrefixAllocator";

	Subspace allocatorSubspace;
	HighContentionPrefixAllocator allocator;
	int numRounds;
	int maxTransactionsPerRound;
	int maxAllocationsPerTransaction;

	int expectedPrefixes = 0;
	std::set<Key> allocatedPrefixes;

	HighContentionPrefixAllocatorWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), allocatorSubspace("test_subspace"_sr), allocator(allocatorSubspace) {
		numRounds = getOption(options, "numRounds"_sr, 100);
		maxTransactionsPerRound = getOption(options, "maxTransactionsPerRound"_sr, 20);
		maxAllocationsPerTransaction = getOption(options, "maxAllocationsPerTransaction"_sr, 20);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	ACTOR static Future<Void> runAllocationTransaction(Database cx, HighContentionPrefixAllocatorWorkload* self) {
		state Reference<ReadYourWritesTransaction> tr = cx->createTransaction();

		state int numAllocations = deterministicRandom()->randomInt(1, self->maxAllocationsPerTransaction + 1);
		self->expectedPrefixes += numAllocations;

		loop {
			try {
				state std::vector<Future<Key>> futures;
				for (int i = 0; i < numAllocations; ++i) {
					futures.push_back(self->allocator.allocate(tr));
				}

				wait(waitForAll(futures));
				wait(tr->commit());

				for (auto f : futures) {
					Key prefix = f.get();

					// There should be no previously allocated prefix that is prefixed by our newly allocated one
					auto itr = self->allocatedPrefixes.lower_bound(prefix);
					if (itr != self->allocatedPrefixes.end() && itr->startsWith(prefix)) {
						TraceEvent(SevError, "HighContentionAllocationWorkloadFailure")
						    .detail("Reason", "Prefix collision")
						    .detail("AllocatedPrefix", prefix)
						    .detail("PreviousPrefix", *itr);

						ASSERT(false);
					}

					// There should be no previously allocated prefix that is a prefix of our newly allocated one
					if (itr != self->allocatedPrefixes.begin()) {
						--itr;

						if (prefix.startsWith(*itr)) {
							TraceEvent(SevError, "HighContentionAllocationWorkloadFailure")
							    .detail("Reason", "Prefix collision")
							    .detail("AllocatedPrefix", prefix)
							    .detail("PreviousPrefix", *itr);

							ASSERT(false);
						}
					}

					// This is technically redundant, but the prefix should not have been allocated previously
					ASSERT(self->allocatedPrefixes.insert(f.get()).second);
				}

				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		return Void();
	}

	ACTOR static Future<Void> runTest(Database cx, HighContentionPrefixAllocatorWorkload* self) {
		state int roundNum = 0;
		for (; roundNum < self->numRounds; ++roundNum) {
			std::vector<Future<Void>> futures;
			int numTransactions = deterministicRandom()->randomInt(1, self->maxTransactionsPerRound + 1);
			for (int i = 0; i < numTransactions; ++i) {
				futures.push_back(runAllocationTransaction(cx, self));
			}

			wait(waitForAll(futures));
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override { return runTest(cx, this); }

	ACTOR static Future<bool> _check(Database cx, HighContentionPrefixAllocatorWorkload* self) {
		if (self->expectedPrefixes != self->allocatedPrefixes.size()) {
			TraceEvent(SevError, "HighContentionAllocationWorkloadFailure")
			    .detail("Reason", "Incorrect Number of Prefixes Allocated")
			    .detail("NumAllocated", self->allocatedPrefixes.size())
			    .detail("Expected", self->expectedPrefixes);

			return false;
		}

		state Reference<ReadYourWritesTransaction> tr = cx->createTransaction();
		loop {
			try {
				state Key k1 = wait(tr->getKey(firstGreaterOrEqual(""_sr)));
				Key k2 = wait(tr->getKey(lastLessThan("\xff"_sr)));
				if (!k1.startsWith(self->allocatorSubspace.key()) || !k2.startsWith(self->allocatorSubspace.key())) {
					TraceEvent(SevError, "HighContentionAllocationWorkloadFailure")
					    .detail("Reason", "Keys written outside allocator subspace")
					    .detail("MinKey", k1)
					    .detail("MaxKey", k2);

					return false;
				}
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		return true;
	}
	Future<bool> check(Database const& cx) override { return _check(cx, this); }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};
WorkloadFactory<HighContentionPrefixAllocatorWorkload> HighContentionPrefixAllocatorWorkload;
