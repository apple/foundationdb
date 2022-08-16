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
#include "fdbclient/FDBTypes.h"
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
// A workload specifically designed to stress the blob range management of the blob manager + blob worker, and test the
// blob database api functions
struct BlobRangesWorkload : TestWorkload {
	// test settings
	double testDuration;
	int operationsPerSecond;
	int targetRanges;
	bool sequential;
	int sequentialGap;

	Future<Void> client;
	Future<Void> unitClient;
	bool stopUnitClient;

	int32_t nextKey;

	std::vector<KeyRange> inactiveRanges;
	std::vector<KeyRange> activeRanges;

	BlobRangesWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 30.0);
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

		nextKey = 10000000 * clientId;

		stopUnitClient = false;

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

	ACTOR Future<bool> setRange(Database cx, KeyRange range, bool active) {
		if (active) {
			bool success = wait(cx->blobbifyRange(range));
			return success;
		} else {
			bool success = wait(cx->unblobbifyRange(range));
			return success;
		}
	}

	ACTOR Future<Void> registerNewRange(Database cx, BlobRangesWorkload* self) {
		std::string nextRangeKey = "R_" + self->newKey();
		state KeyRange range(KeyRangeRef(StringRef(nextRangeKey), strinc(StringRef(nextRangeKey))));
		// TODO REMOVE
		fmt::print("Registering new range [{0} - {1})\n", range.begin.printable(), range.end.printable());

		// don't put in active ranges until AFTER set range command succeeds, to avoid checking a range that maybe
		// wasn't initialized
		bool success = wait(self->setRange(cx, range, true));
		ASSERT(success);

		// TODO REMOVE
		fmt::print("Registered new range [{0} - {1})\n", range.begin.printable(), range.end.printable());

		self->activeRanges.push_back(range);
		return Void();
	}

	ACTOR Future<Void> unregisterRandomRange(Database cx, BlobRangesWorkload* self) {
		int randomRangeIdx = deterministicRandom()->randomInt(0, self->activeRanges.size());
		state KeyRange range = self->activeRanges[randomRangeIdx];
		// remove range from active BEFORE committing txn but add to remove AFTER, to avoid checking a range that could
		// potentially be in both states
		swapAndPop(&self->activeRanges, randomRangeIdx);

		fmt::print("Unregistering new range [{0} - {1})\n", range.begin.printable(), range.end.printable());

		if (deterministicRandom()->coinflip()) {
			fmt::print("Force purging range before un-registering: [{0} - {1})\n",
			           range.begin.printable(),
			           range.end.printable());
			Key purgeKey = wait(cx->purgeBlobGranules(range, 1, {}, true));
			wait(cx->waitPurgeGranulesComplete(purgeKey));
		}
		bool success = wait(self->setRange(cx, range, false));
		ASSERT(success);

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
		/*if (clientId == 0) {
		    unitClient = blobRangesUnitTests(cx->clone(), this);
		} else {
		    unitClient = Future<Void>(Void());
		}*/
		// TODO CHANGE BACK!!
		unitClient = Future<Void>(Void());
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		client = Future<Void>();
		stopUnitClient = true;
		return _check(cx, this);
	}

	ACTOR Future<bool> isRangeActive(Database cx, KeyRange range) {
		Version v = wait(cx->verifyBlobRange(range, {}));
		return v != invalidVersion;
	}

	ACTOR Future<Void> checkRange(Database cx, BlobRangesWorkload* self, KeyRange range, bool isActive) {
		// Check that a read completes for the range. If not loop around and try again
		loop {
			bool completed = wait(self->isRangeActive(cx, range));

			if (completed == isActive) {
				break;
			}

			fmt::print("CHECK: {0} range [{1} - {2}) failed!\n",
			           isActive ? "Active" : "Inactive",
			           range.begin.printable(),
			           range.end.printable());

			wait(delay(1.0));
		}

		Standalone<VectorRef<KeyRangeRef>> blobRanges = wait(cx->listBlobbifiedRanges(range, 1000000));
		if (isActive) {
			ASSERT(blobRanges.size() == 1);
			ASSERT(blobRanges[0].begin <= range.begin);
			ASSERT(blobRanges[0].end >= range.end);
		} else {
			ASSERT(blobRanges.empty());
		}

		state Transaction tr(cx);
		loop {
			try {
				Standalone<VectorRef<KeyRangeRef>> granules = wait(tr.getBlobGranuleRanges(range, 1000000));
				if (isActive) {
					ASSERT(granules.size() >= 1);
					ASSERT(granules.front().begin <= range.begin);
					ASSERT(granules.back().end >= range.end);
					for (int i = 0; i < granules.size() - 1; i++) {
						ASSERT(granules[i].end == granules[i + 1].begin);
					}
				} else {
					//  TODO REMOVE
					if (!granules.empty()) {
						fmt::print("Granules for [{0} - {1}) not empty! ({2}):\n",
						           range.begin.printable(),
						           range.end.printable(),
						           granules.size());
						for (auto& it : granules) {
							fmt::print("  [{0} - {1})\n", it.begin.printable(), it.end.printable());
						}
					}
					ASSERT(granules.empty());
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
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

		// FIXME: re-enable! if we don't force purge there are weird races that cause granules to technically still
		// exist
		/*for (int i = 0; i < self->inactiveRanges.size(); i++) {
		    checks.push_back(self->checkRange(cx, self, self->inactiveRanges[i], false));
		}*/
		wait(waitForAll(checks));
		wait(self->unitClient);
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

	enum UnitTestTypes {
		VERIFY_RANGE_UNIT,
		VERIFY_RANGE_GAP_UNIT,
		RANGES_MISALIGNED,
		BLOBBIFY_IDEMPOTENT,
		RE_BLOBBIFY,
		OP_COUNT = 5 /* keep this last */
	};

	ACTOR Future<Void> verifyRangeUnit(Database cx, BlobRangesWorkload* self, KeyRange range) {
		state KeyRange activeRange(KeyRangeRef(range.begin.withSuffix("A"_sr), range.begin.withSuffix("B"_sr)));
		state Key middleKey = range.begin.withSuffix("AF"_sr);

		bool setSuccess = wait(self->setRange(cx, activeRange, true));
		ASSERT(setSuccess);
		wait(self->checkRange(cx, self, activeRange, true));

		bool success1 = wait(self->isRangeActive(cx, KeyRangeRef(activeRange.begin, middleKey)));
		ASSERT(success1);

		bool success2 = wait(self->isRangeActive(cx, KeyRangeRef(middleKey, activeRange.end)));
		ASSERT(success2);

		bool fail1 = wait(self->isRangeActive(cx, range));
		ASSERT(!fail1);

		bool fail2 = wait(self->isRangeActive(cx, KeyRangeRef(range.begin, activeRange.begin)));
		ASSERT(!fail2);

		bool fail3 = wait(self->isRangeActive(cx, KeyRangeRef(activeRange.end, range.end)));
		ASSERT(!fail3);

		bool fail4 = wait(self->isRangeActive(cx, KeyRangeRef(range.begin, middleKey)));
		ASSERT(!fail4);

		bool fail5 = wait(self->isRangeActive(cx, KeyRangeRef(middleKey, range.end)));
		ASSERT(!fail5);

		bool fail6 = wait(self->isRangeActive(cx, KeyRangeRef(range.begin, activeRange.end)));
		ASSERT(!fail6);

		bool fail7 = wait(self->isRangeActive(cx, KeyRangeRef(activeRange.begin, range.end)));
		ASSERT(!fail7);

		return Void();
	}

	ACTOR Future<Void> verifyRangeGapUnit(Database cx, BlobRangesWorkload* self, KeyRange range) {
		state std::vector<Key> boundaries;
		boundaries.push_back(range.begin);
		state int rangeCount = deterministicRandom()->randomExp(3, 6) + 1;
		for (int i = 0; i < rangeCount - 1; i++) {
			std::string suffix = format("%04x", i);
			boundaries.push_back(range.begin.withSuffix(suffix));
		}
		boundaries.push_back(range.end);

		ASSERT(boundaries.size() - 1 == rangeCount);

		state int rangeToNotBlobbify = deterministicRandom()->randomInt(0, rangeCount);
		state int i;
		for (i = 0; i < rangeCount; i++) {
			state KeyRange subRange(KeyRangeRef(boundaries[i], boundaries[i + 1]));
			if (i != rangeToNotBlobbify) {
				bool setSuccess = wait(self->setRange(cx, subRange, true));
				ASSERT(setSuccess);
				wait(self->checkRange(cx, self, subRange, true));
			} else {
				wait(self->checkRange(cx, self, subRange, false));
			}
		}

		bool success = wait(self->isRangeActive(cx, range));
		ASSERT(!success);

		return Void();
	}

	ACTOR Future<Void> rangesMisalignedUnit(Database cx, BlobRangesWorkload* self, KeyRange range) {
		// FIXME: parts of this don't work yet
		bool setSuccess = wait(self->setRange(cx, range, true));
		ASSERT(setSuccess);
		state KeyRange subRange(KeyRangeRef(range.begin.withSuffix("A"_sr), range.begin.withSuffix("B"_sr)));

		// getBlobGranules and getBlobRanges on subRange - should return actual granules instead of clipped to subRange
		Standalone<VectorRef<KeyRangeRef>> blobRanges = wait(cx->listBlobbifiedRanges(range, 1000000));
		ASSERT(blobRanges.size() == 1);
		ASSERT(blobRanges[0] == range);

		state Transaction tr(cx);
		loop {
			try {
				Standalone<VectorRef<KeyRangeRef>> granules = wait(tr.getBlobGranuleRanges(range, 1000000));
				ASSERT(granules.size() == 1);
				ASSERT(granules[0] == range);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		Key purgeKey = wait(cx->purgeBlobGranules(subRange, 1, {}, false));
		wait(cx->waitPurgeGranulesComplete(purgeKey));

		// should still be active after normal purge
		bool success1 = wait(self->isRangeActive(cx, subRange));
		ASSERT(success1);

		bool success2 = wait(self->isRangeActive(cx, range));
		ASSERT(success2);

		Key forcePurgeKey = wait(cx->purgeBlobGranules(subRange, 1, {}, false));
		wait(cx->waitPurgeGranulesComplete(forcePurgeKey));

		// should NOT still be active after force purge
		bool fail1 = wait(self->isRangeActive(cx, subRange));
		ASSERT(!fail1);

		bool fail2 = wait(self->isRangeActive(cx, range));
		ASSERT(!fail2);

		// getBlobGranules should return nothing here after purge

		tr.reset();
		loop {
			try {
				Standalone<VectorRef<KeyRangeRef>> granules2 = wait(tr.getBlobGranuleRanges(range, 1000000));
				ASSERT(granules2.empty());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<Void> blobbifyIdempotentUnit(Database cx, BlobRangesWorkload* self, KeyRange range) {
		state KeyRange activeRange(KeyRangeRef(range.begin.withSuffix("A"_sr), range.begin.withSuffix("B"_sr)));
		state Key middleKey = range.begin.withSuffix("AF"_sr);
		state Key middleKey2 = range.begin.withSuffix("AG"_sr);

		bool success = wait(self->setRange(cx, activeRange, true));
		ASSERT(success);
		wait(self->checkRange(cx, self, activeRange, true));

		// check that re-blobbifying same range is successful
		bool retrySuccess = wait(self->setRange(cx, activeRange, true));
		ASSERT(retrySuccess);
		wait(self->checkRange(cx, self, activeRange, true));

		// check that blobbifying range that overlaps but does not match existing blob range fails
		bool fail1 = wait(self->setRange(cx, range, true));
		ASSERT(!fail1);

		bool fail2 = wait(self->setRange(cx, KeyRangeRef(range.begin, activeRange.end), true));
		ASSERT(!fail2);

		bool fail3 = wait(self->setRange(cx, KeyRangeRef(activeRange.begin, range.end), true));
		ASSERT(!fail3);

		bool fail4 = wait(self->setRange(cx, KeyRangeRef(range.begin, middleKey), true));
		ASSERT(!fail4);

		bool fail5 = wait(self->setRange(cx, KeyRangeRef(middleKey, range.end), true));
		ASSERT(!fail5);

		bool fail6 = wait(self->setRange(cx, KeyRangeRef(activeRange.begin, middleKey), true));
		ASSERT(!fail6);

		bool fail7 = wait(self->setRange(cx, KeyRangeRef(middleKey, activeRange.end), true));
		ASSERT(!fail7);

		bool fail8 = wait(self->setRange(cx, KeyRangeRef(middleKey, middleKey2), true));
		ASSERT(!fail8);

		Standalone<VectorRef<KeyRangeRef>> blobRanges = wait(cx->listBlobbifiedRanges(range, 1000000));
		ASSERT(blobRanges.size() == 1);
		ASSERT(blobRanges[0] == activeRange);

		state Transaction tr(cx);
		loop {
			try {
				Standalone<VectorRef<KeyRangeRef>> granules = wait(tr.getBlobGranuleRanges(range, 1000000));
				ASSERT(granules.size() == 1);
				ASSERT(granules[0] == activeRange);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<Void> reBlobbifyUnit(Database cx, BlobRangesWorkload* self, KeyRange range) {
		bool setSuccess = wait(self->setRange(cx, range, true));
		ASSERT(setSuccess);
		wait(self->checkRange(cx, self, range, true));

		// force purge range
		Key purgeKey = wait(cx->purgeBlobGranules(range, 1, {}, true));
		wait(cx->waitPurgeGranulesComplete(purgeKey));
		wait(self->checkRange(cx, self, range, false));

		bool unsetSuccess = wait(self->setRange(cx, range, false));
		ASSERT(unsetSuccess);
		wait(self->checkRange(cx, self, range, false));

		bool reSetSuccess = wait(self->setRange(cx, range, true));
		ASSERT(reSetSuccess);
		wait(self->checkRange(cx, self, range, true));

		return Void();
	}

	ACTOR Future<Void> blobRangesUnitTests(Database cx, BlobRangesWorkload* self) {
		loop {
			if (self->stopUnitClient) {
				return Void();
			}
			std::string nextRangeKey = "U_" + self->newKey();
			state KeyRange range(KeyRangeRef(StringRef(nextRangeKey), strinc(StringRef(nextRangeKey))));
			int op = deterministicRandom()->random01();
			if (op == VERIFY_RANGE_UNIT) {
				wait(self->verifyRangeUnit(cx, self, range));
			} else if (op == VERIFY_RANGE_GAP_UNIT) {
				wait(self->verifyRangeGapUnit(cx, self, range));
			} else if (op == RANGES_MISALIGNED) {
				wait(self->rangesMisalignedUnit(cx, self, range));
			} else if (op == BLOBBIFY_IDEMPOTENT) {
				wait(self->blobbifyIdempotentUnit(cx, self, range));
			} else if (op == RE_BLOBBIFY) {
				wait(self->reBlobbifyUnit(cx, self, range));
			} else {
				ASSERT(false);
			}

			// tear down range at end
			Key purgeKey = wait(cx->purgeBlobGranules(range, 1, {}, true));
			wait(cx->waitPurgeGranulesComplete(purgeKey));
			bool success = wait(self->setRange(cx, range, false));
			ASSERT(success);

			wait(delay(1.0));
		}
	}
};

WorkloadFactory<BlobRangesWorkload> BlobRangesWorkloadFactory("BlobRanges");
