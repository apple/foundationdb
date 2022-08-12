/*
 * BlobRangesWorkload.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/Util.h"
#include "flow/serialize.h"
#include <cstring>
#include <limits>

#include "flow/actorcompiler.h" // This must be the last #include.

// FIXME: need to do multiple changes per commit to properly exercise future change feed logic
// A workload specifically designed to stress the blob range management of the blob manager + blob worker
struct BlobRangesWorkload : TestWorkload {
	// test settings
	double testDuration;
	int operationsPerSecond;
	int targetRanges;
	bool sequential;
	int sequentialGap;

	Future<Void> client;

	int32_t nextKey;

	std::vector<KeyRange> inactiveRanges;
	std::vector<KeyRange> activeRanges;

	BlobRangesWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 30.0);
		// TODO change back!
		operationsPerSecond = getOption(options, "opsPerSecond"_sr, deterministicRandom()->randomInt(1, 100));
		operationsPerSecond /= clientCount;
		if (operationsPerSecond <= 0) {
			operationsPerSecond = 1;
		}

		int64_t rand = wcx.sharedRandomNumber;
		targetRanges = deterministicRandom()->randomExp(1, 1 + rand % 10);
		targetRanges *= (0.8 + (deterministicRandom()->random01() * 0.4));
		targetRanges /= clientCount;
		if (targetRanges <= 0) {
			targetRanges = 1;
		}
		rand /= 10;

		sequential = rand % 2;
		rand /= 2;

		sequentialGap = 1 + rand % 2;
		rand /= 2;

		nextKey = 1000000 * clientId;

		TraceEvent("BlobRangesWorkloadInit").detail("TargetRanges", targetRanges);
	}

	std::string description() const override { return "BlobRangesWorkload"; }
	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	std::string newKey() {
		if (sequential) {
			nextKey += sequentialGap;
			return format("%08x", nextKey);
		} else {
			return deterministicRandom()->randomUniqueID().toString();
		}
	}

	// FIXME: switch these to new apis instead of manipulating system keys
	ACTOR Future<Void> setRange(Database cx, KeyRange range, bool active) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->set(blobRangeChangeKey, deterministicRandom()->randomUniqueID().toString());
				wait(krmSetRange(
				    tr, blobRangeKeys.begin, range, active ? LiteralStringRef("1") : LiteralStringRef("0")));
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR Future<Void> registerNewRange(Database cx, BlobRangesWorkload* self) {
		std::string nextRangeKey = self->newKey();
		state KeyRange range(KeyRangeRef(StringRef(nextRangeKey), strinc(StringRef(nextRangeKey))));
		// TODO REMOVE
		fmt::print("Registering new range [{0} - {1})\n", range.begin.printable(), range.end.printable());

		// don't put in active ranges until AFTER set range command succeeds, to avoid checking a range that maybe
		// wasn't initialized
		wait(self->setRange(cx, range, true));

		// TODO REMOVE
		fmt::print("Registered new range [{0} - {1})\n", range.begin.printable(), range.end.printable());

		self->activeRanges.push_back(range);
		return Void();
	}

	ACTOR Future<Void> unregisterRandomRange(Database cx, BlobRangesWorkload* self) {
		// FIXME: sometimes hard purge instead of unregistering?
		int randomRangeIdx = deterministicRandom()->randomInt(0, self->activeRanges.size());
		state KeyRange range = self->activeRanges[randomRangeIdx];
		// remove range from active BEFORE committing txn but add to remove AFTER, to avoid checking a range that could
		// potentially be in both states
		swapAndPop(&self->activeRanges, randomRangeIdx);

		fmt::print("Unregistering new range [{0} - {1})\n", range.begin.printable(), range.end.printable());

		wait(self->setRange(cx, range, false));

		fmt::print("Unregistered new range [{0} - {1})\n", range.begin.printable(), range.end.printable());

		self->inactiveRanges.push_back(range);

		return Void();
	}

	ACTOR Future<Void> _setup(Database cx, BlobRangesWorkload* self) {
		// create initial target ranges
		TraceEvent("BlobRangesSetup").detail("InitialRanges", self->targetRanges).log();
		// set up blob granules
		wait(success(ManagementAPI::changeConfig(cx.getReference(), "blob_granules_enabled=1", true)));

		state int i;
		std::vector<Future<Void>> createInitialRanges;
		for (i = 0; i < self->targetRanges; i++) {
			wait(self->registerNewRange(cx, self));
		}
		TraceEvent("BlobRangesSetupComplete");
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		client = blobRangesClient(cx->clone(), this);
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		client = Future<Void>();
		return _check(cx, this);
	}

	ACTOR Future<Void> checkRange(Database cx, BlobRangesWorkload* self, KeyRange range, bool isActive) {
		// Check that a read completes for the range. If not loop around and try again
		state Transaction tr(cx);
		loop {
			state bool completed;
			loop {
				try {
					wait(success(tr.readBlobGranules(range, 0, {})));
					completed = true;
					break;
				} catch (Error& e) {
					fmt::print("Error checking: {0}\n", e.name());
					if (e.code() == error_code_blob_granule_transaction_too_old) {
						completed = false;
						break;
					}
					wait(tr.onError(e));
				}
			}
			if (completed == isActive) {
				return Void();
			}

			fmt::print("CHECK: {0} range [{1} - {2}) failed!\n",
			           isActive ? "Active" : "Inactive",
			           range.begin.printable(),
			           range.end.printable());

			wait(delay(1.0));
		}
	}

	ACTOR Future<bool> _check(Database cx, BlobRangesWorkload* self) {
		TraceEvent("BlobRangesCheck")
		    .detail("ActiveRanges", self->activeRanges.size())
		    .detail("InactiveRanges", self->inactiveRanges.size())
		    .log();
		fmt::print(
		    "Checking {0} active and {1} inactive ranges\n", self->activeRanges.size(), self->inactiveRanges.size());
		state std::vector<Future<Void>> checks;
		for (int i = 0; i < self->activeRanges.size(); i++) {
			checks.push_back(self->checkRange(cx, self, self->activeRanges[i], true));
		}
		// FIXME: re-enable once new blob commands refactor is merged
		/*for (int i = 0; i < self->inactiveRanges.size(); i++) {
		    checks.push_back(self->checkRange(cx, self, self->inactiveRanges[i], false));
		}*/
		wait(waitForAll(checks));
		TraceEvent("BlobRangesCheckComplete");
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> blobRangesClient(Database cx, BlobRangesWorkload* self) {
		state double last = now();
		loop {
			state Future<Void> waitNextOp = poisson(&last, 1.0 / self->operationsPerSecond);

			if (self->activeRanges.empty() || deterministicRandom()->coinflip()) {
				wait(self->registerNewRange(cx, self));
			} else {
				wait(self->unregisterRandomRange(cx, self));
			}

			wait(waitNextOp);
		}
	}
};

WorkloadFactory<BlobRangesWorkload> BlobRangesWorkloadFactory("BlobRanges");
