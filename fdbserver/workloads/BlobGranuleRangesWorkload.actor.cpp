/*
 * BlobGranuleRangesWorkload.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/TesterInterface.h"
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

#define BGRW_DEBUG true

// FIXME: need to do multiple changes per commit to properly exercise future change feed logic
// A workload specifically designed to stress the blob range management of the blob manager + blob worker, and test the
// blob database api functions
struct BlobGranuleRangesWorkload : TestWorkload {
	static constexpr auto NAME = "BlobGranuleRanges";
	// test settings
	double testDuration;
	int operationsPerSecond;
	int targetRanges;
	bool sequential;
	int sequentialGap;

	Future<Void> client;
	Future<Void> unitClient;
	bool stopUnitClient;
	Optional<TenantName> tenantName;
	Optional<Reference<Tenant>> tenant;

	int32_t nextKey;

	std::vector<KeyRange> inactiveRanges;
	std::vector<KeyRange> activeRanges;

	BlobGranuleRangesWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
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
		tenantName = StringRef("bgrwTenant" + std::to_string(clientId));

		TraceEvent("BlobGranuleRangesWorkloadInit").detail("TargetRanges", targetRanges);
	}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	std::string newKey() {
		if (sequential) {
			nextKey += sequentialGap;
			return format("%08x", nextKey);
		} else {
			return deterministicRandom()->randomUniqueID().toString();
		}
	}

	ACTOR Future<Void> registerNewRange(Database cx,
	                                    BlobGranuleRangesWorkload* self,
	                                    Optional<Reference<Tenant>> alternateTenant) {
		std::string nextRangeKey = "R_" + self->newKey();
		state KeyRange range(KeyRangeRef(StringRef(nextRangeKey), strinc(StringRef(nextRangeKey))));
		if (BGRW_DEBUG) {
			fmt::print("Registering new range [{0} - {1})\n", range.begin.printable(), range.end.printable());
		}

		// don't put in active ranges until AFTER set range command succeeds, to avoid checking a range that maybe
		// wasn't initialized
		bool success = wait(cx->blobbifyRange(range, alternateTenant.present() ? alternateTenant : self->tenant));
		ASSERT(success);

		if (BGRW_DEBUG) {
			fmt::print("Registered new range [{0} - {1})\n", range.begin.printable(), range.end.printable());
		}

		self->activeRanges.push_back(range);
		return Void();
	}

	ACTOR Future<Key> versionedForcePurge(Database cx, KeyRange range, Optional<Reference<Tenant>> tenant) {
		Version rv = deterministicRandom()->coinflip() ? latestVersion : 1;
		Key purgeKey = wait(cx->purgeBlobGranules(range, rv, tenant, true));

		return purgeKey;
	}

	ACTOR Future<Void> unregisterRandomRange(Database cx, BlobGranuleRangesWorkload* self) {
		int randomRangeIdx = deterministicRandom()->randomInt(0, self->activeRanges.size());
		state KeyRange range = self->activeRanges[randomRangeIdx];
		// remove range from active BEFORE committing txn but add to remove AFTER, to avoid checking a range that could
		// potentially be in either state
		swapAndPop(&self->activeRanges, randomRangeIdx);

		if (BGRW_DEBUG) {
			fmt::print("Unregistering new range [{0} - {1})\n", range.begin.printable(), range.end.printable());
		}

		if (deterministicRandom()->coinflip()) {
			if (BGRW_DEBUG) {
				fmt::print("Force purging range before un-registering: [{0} - {1})\n",
				           range.begin.printable(),
				           range.end.printable());
			}
			Key purgeKey = wait(self->versionedForcePurge(cx, range, self->tenant));
			wait(cx->waitPurgeGranulesComplete(purgeKey));
		}
		bool success = wait(cx->unblobbifyRange(range, self->tenant));
		ASSERT(success);

		if (BGRW_DEBUG) {
			fmt::print("Unregistered new range [{0} - {1})\n", range.begin.printable(), range.end.printable());
		}

		self->inactiveRanges.push_back(range);

		return Void();
	}

	ACTOR Future<TenantMapEntry> setupTenant(Database cx, TenantName name) {
		if (BGRW_DEBUG) {
			fmt::print("Creating tenant: {0}\n", name.printable());
		}

		Optional<TenantMapEntry> entry = wait(TenantAPI::createTenant(cx.getReference(), name));
		ASSERT(entry.present());

		if (BGRW_DEBUG) {
			fmt::print("Created tenant {0}: {1}\n", name.printable(), entry.get().prefix.printable());
		}

		return entry.get();
	}

	ACTOR Future<Void> _setup(Database cx, BlobGranuleRangesWorkload* self) {
		// create initial target ranges
		TraceEvent("BlobGranuleRangesSetup").detail("InitialRanges", self->targetRanges).log();

		// set up blob granules
		wait(success(ManagementAPI::changeConfig(cx.getReference(), "blob_granules_enabled=1", true)));

		if (cx->clientInfo->get().tenantMode != TenantMode::REQUIRED && deterministicRandom()->coinflip()) {
			self->tenantName.reset();
		}

		if (self->tenantName.present()) {
			wait(success(self->setupTenant(cx, self->tenantName.get())));

			self->tenant = makeReference<Tenant>(cx, self->tenantName.get());

			try {
				wait(self->registerNewRange(cx, self, makeReference<Tenant>(cx, "BogusTenant"_sr)));
				ASSERT(false);
			} catch (Error& e) {
				if (e.code() != error_code_tenant_not_found) {
					throw e;
				}
			}
		}

		state int i;
		std::vector<Future<Void>> createInitialRanges;
		for (i = 0; i < self->targetRanges; i++) {
			wait(self->registerNewRange(cx, self, {}));
		}
		TraceEvent("BlobGranuleRangesSetupComplete");
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		client = blobGranuleRangesClient(cx->clone(), this);
		if (clientId == 0) {
			unitClient = blobGranuleRangesUnitTests(cx->clone(), this);
		} else {
			unitClient = Future<Void>(Void());
		}
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		client = Future<Void>();
		stopUnitClient = true;
		return _check(cx, this);
	}

	ACTOR Future<bool> isRangeActive(Database cx, KeyRange range, Optional<Reference<Tenant>> tenant) {
		Optional<Version> rv;
		if (deterministicRandom()->coinflip()) {
			rv = latestVersion;
		}
		state Version v = wait(cx->verifyBlobRange(range, rv, tenant));
		return v != invalidVersion;
	}

	ACTOR Future<Void> checkRange(Database cx,
	                              BlobGranuleRangesWorkload* self,
	                              KeyRange range,
	                              bool isActive,
	                              bool strict) {
		// Check that a read completes for the range. If not loop around and try again
		loop {
			bool completed = wait(self->isRangeActive(cx, range, self->tenant));

			if (completed == isActive) {
				break;
			}

			if (BGRW_DEBUG) {
				fmt::print("CHECK: {0} range [{1} - {2}) failed!\n",
				           isActive ? "Active" : "Inactive",
				           range.begin.printable(),
				           range.end.printable());
			}

			wait(delay(1.0));
		}

		Standalone<VectorRef<KeyRangeRef>> blobRanges = wait(cx->listBlobbifiedRanges(range, 1000000, self->tenant));
		if (isActive) {
			ASSERT(blobRanges.size() == 1);
			if (strict) {
				ASSERT_EQ(blobRanges[0].begin, range.begin);
				ASSERT_EQ(blobRanges[0].end, range.end);
			} else {
				ASSERT_LE(blobRanges[0].begin, range.begin);
				ASSERT_GE(blobRanges[0].end, range.end);
			}
		} else {
			ASSERT(blobRanges.empty());
		}

		state Transaction tr(cx, self->tenant);
		loop {
			try {
				Standalone<VectorRef<KeyRangeRef>> granules = wait(tr.getBlobGranuleRanges(range, 1000000));
				if (isActive) {
					ASSERT(granules.size() >= 1);
					if (strict) {
						ASSERT_EQ(granules.front().begin, range.begin);
						ASSERT_EQ(granules.back().end, range.end);
					} else {
						ASSERT_LE(granules.front().begin, range.begin);
						ASSERT_GE(granules.back().end, range.end);
					}
					for (int i = 0; i < granules.size() - 1; i++) {
						ASSERT(granules[i].end == granules[i + 1].begin);
					}
				} else {
					if (BGRW_DEBUG) {
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

	ACTOR Future<bool> _check(Database cx, BlobGranuleRangesWorkload* self) {
		if (deterministicRandom()->coinflip()) {
			cx->internal = IsInternal::False;
		}
		TraceEvent("BlobGranuleRangesCheck")
		    .detail("ActiveRanges", self->activeRanges.size())
		    .detail("InactiveRanges", self->inactiveRanges.size())
		    .log();
		if (BGRW_DEBUG) {
			fmt::print("Checking {0} active and {1} inactive ranges\n",
			           self->activeRanges.size(),
			           self->inactiveRanges.size());
		}
		state std::vector<Future<Void>> checks;
		for (int i = 0; i < self->activeRanges.size(); i++) {
			checks.push_back(self->checkRange(cx, self, self->activeRanges[i], true, true));
		}

		// FIXME: re-enable! if we don't force purge there are weird races that cause granules to technically still
		// exist
		/*for (int i = 0; i < self->inactiveRanges.size(); i++) {
		    checks.push_back(self->checkRange(cx, self, self->inactiveRanges[i], false, false));
		}*/
		wait(waitForAll(checks));
		wait(self->unitClient);
		TraceEvent("BlobGranuleRangesCheckComplete");
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> blobGranuleRangesClient(Database cx, BlobGranuleRangesWorkload* self) {
		if (deterministicRandom()->coinflip()) {
			cx->internal = IsInternal::False;
		}
		state double last = now();
		loop {
			state Future<Void> waitNextOp = poisson(&last, 1.0 / self->operationsPerSecond);

			if (self->activeRanges.empty() || deterministicRandom()->coinflip()) {
				wait(self->registerNewRange(cx, self, {}));
			} else {
				wait(self->unregisterRandomRange(cx, self));
			}

			wait(waitNextOp);
		}
	}

	ACTOR Future<Void> tearDownRangeAfterUnit(Database cx, BlobGranuleRangesWorkload* self, KeyRange range) {
		if (BGRW_DEBUG) {
			fmt::print("Tearing down [{0} - {1}) after unit!\n", range.begin.printable(), range.end.printable());
		}

		// tear down range at end
		Key purgeKey = wait(self->versionedForcePurge(cx, range, self->tenant));
		wait(cx->waitPurgeGranulesComplete(purgeKey));
		bool success = wait(cx->unblobbifyRange(range, self->tenant));
		ASSERT(success);

		if (BGRW_DEBUG) {
			fmt::print("Range [{0} - {1}) torn down.\n", range.begin.printable(), range.end.printable());
		}

		return Void();
	}

	ACTOR Future<Void> verifyRangeUnit(Database cx, BlobGranuleRangesWorkload* self, KeyRange range) {
		state KeyRange activeRange(KeyRangeRef(range.begin.withSuffix("A"_sr), range.begin.withSuffix("B"_sr)));
		state Key middleKey = range.begin.withSuffix("AF"_sr);

		if (BGRW_DEBUG) {
			fmt::print("VerifyRangeUnit: [{0} - {1})\n", range.begin.printable(), range.end.printable());
		}
		bool setSuccess = wait(cx->blobbifyRange(activeRange, self->tenant));
		ASSERT(setSuccess);
		wait(self->checkRange(cx, self, activeRange, true, true));

		bool success1 = wait(self->isRangeActive(cx, KeyRangeRef(activeRange.begin, middleKey), self->tenant));
		ASSERT(success1);

		bool success2 = wait(self->isRangeActive(cx, KeyRangeRef(middleKey, activeRange.end), self->tenant));
		ASSERT(success2);

		bool fail1 = wait(self->isRangeActive(cx, range, self->tenant));
		ASSERT(!fail1);

		bool fail2 = wait(self->isRangeActive(cx, KeyRangeRef(range.begin, activeRange.begin), self->tenant));
		ASSERT(!fail2);

		bool fail3 = wait(self->isRangeActive(cx, KeyRangeRef(activeRange.end, range.end), self->tenant));
		ASSERT(!fail3);

		bool fail4 = wait(self->isRangeActive(cx, KeyRangeRef(range.begin, middleKey), self->tenant));
		ASSERT(!fail4);

		bool fail5 = wait(self->isRangeActive(cx, KeyRangeRef(middleKey, range.end), self->tenant));
		ASSERT(!fail5);

		bool fail6 = wait(self->isRangeActive(cx, KeyRangeRef(range.begin, activeRange.end), self->tenant));
		ASSERT(!fail6);

		bool fail7 = wait(self->isRangeActive(cx, KeyRangeRef(activeRange.begin, range.end), self->tenant));
		ASSERT(!fail7);

		wait(self->tearDownRangeAfterUnit(cx, self, activeRange));

		return Void();
	}

	ACTOR Future<Void> verifyRangeGapUnit(Database cx, BlobGranuleRangesWorkload* self, KeyRange range) {
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
				bool setSuccess = wait(cx->blobbifyRange(subRange, self->tenant));
				ASSERT(setSuccess);
				wait(self->checkRange(cx, self, subRange, true, true));
			} else {
				wait(self->checkRange(cx, self, subRange, false, false));
			}
		}

		bool success = wait(self->isRangeActive(cx, range, self->tenant));
		ASSERT(!success);

		if (rangeToNotBlobbify != 0) {
			wait(self->tearDownRangeAfterUnit(cx, self, KeyRangeRef(boundaries[0], boundaries[rangeToNotBlobbify])));
		}
		if (rangeToNotBlobbify != rangeCount - 1) {
			wait(self->tearDownRangeAfterUnit(
			    cx, self, KeyRangeRef(boundaries[rangeToNotBlobbify + 1], boundaries.back())));
		}

		return Void();
	}

	ACTOR Future<Void> checkRangesMisaligned(Database cx,
	                                         BlobGranuleRangesWorkload* self,
	                                         KeyRange expectedRange,
	                                         KeyRange queryRange) {
		Standalone<VectorRef<KeyRangeRef>> blobRanges =
		    wait(cx->listBlobbifiedRanges(queryRange, 1000000, self->tenant));
		ASSERT(blobRanges.size() == 1);
		ASSERT(blobRanges[0] == expectedRange);

		state Transaction tr(cx, self->tenant);
		loop {
			try {
				Standalone<VectorRef<KeyRangeRef>> granules = wait(tr.getBlobGranuleRanges(queryRange, 1000000));
				ASSERT(granules.size() == 1);
				ASSERT(granules[0] == expectedRange);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<Void> rangesMisalignedUnit(Database cx, BlobGranuleRangesWorkload* self, KeyRange range) {
		bool setSuccess = wait(cx->blobbifyRange(range, self->tenant));
		ASSERT(setSuccess);
		state KeyRange subRange(KeyRangeRef(range.begin.withSuffix("A"_sr), range.begin.withSuffix("B"_sr)));

		// validate range set up correctly
		wait(self->checkRange(cx, self, range, true, true));
		wait(self->checkRangesMisaligned(cx, self, range, range));

		// getBlobGranules and getBlobRanges on sub ranges- should return actual granule/range instead of clipped
		wait(self->checkRange(cx, self, subRange, true, false));
		wait(self->checkRangesMisaligned(cx, self, range, subRange));
		wait(self->checkRangesMisaligned(cx, self, range, KeyRangeRef(range.begin, subRange.end)));
		wait(self->checkRangesMisaligned(cx, self, range, KeyRangeRef(subRange.begin, range.end)));

		try {
			wait(success(cx->purgeBlobGranules(subRange, 1, self->tenant, false)));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			ASSERT(e.code() == error_code_unsupported_operation);
		}

		try {
			wait(success(cx->purgeBlobGranules(subRange, 1, self->tenant, true)));
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			ASSERT(e.code() == error_code_unsupported_operation);
		}

		// ensure range still there after unaligned purges
		wait(self->checkRange(cx, self, range, true, true));
		wait(self->checkRangesMisaligned(cx, self, range, range));

		wait(self->tearDownRangeAfterUnit(cx, self, range));
		return Void();
	}

	ACTOR Future<Void> blobbifyIdempotentUnit(Database cx, BlobGranuleRangesWorkload* self, KeyRange range) {
		state KeyRange activeRange(KeyRangeRef(range.begin.withSuffix("A"_sr), range.begin.withSuffix("B"_sr)));
		state Key middleKey = range.begin.withSuffix("AF"_sr);
		state Key middleKey2 = range.begin.withSuffix("AG"_sr);

		if (BGRW_DEBUG) {
			fmt::print("IdempotentUnit: [{0} - {1})\n", range.begin.printable(), range.end.printable());
		}

		// unblobbifying range that already doesn't exist should be no-op
		if (deterministicRandom()->coinflip()) {
			bool unblobbifyStartSuccess = wait(cx->blobbifyRange(activeRange, self->tenant));
			ASSERT(unblobbifyStartSuccess);
		}

		bool success = wait(cx->blobbifyRange(activeRange, self->tenant));
		ASSERT(success);
		wait(self->checkRange(cx, self, activeRange, true, true));

		// check that re-blobbifying same range is successful
		bool retrySuccess = wait(cx->blobbifyRange(activeRange, self->tenant));
		ASSERT(retrySuccess);
		wait(self->checkRange(cx, self, activeRange, true, true));

		// check that blobbifying range that overlaps but does not match existing blob range fails
		bool fail1 = wait(cx->blobbifyRange(range, self->tenant));
		ASSERT(!fail1);

		bool fail2 = wait(cx->blobbifyRange(KeyRangeRef(range.begin, activeRange.end), self->tenant));
		ASSERT(!fail2);

		bool fail3 = wait(cx->blobbifyRange(KeyRangeRef(activeRange.begin, range.end), self->tenant));
		ASSERT(!fail3);

		bool fail4 = wait(cx->blobbifyRange(KeyRangeRef(range.begin, middleKey), self->tenant));
		ASSERT(!fail4);

		bool fail5 = wait(cx->blobbifyRange(KeyRangeRef(middleKey, range.end), self->tenant));
		ASSERT(!fail5);

		bool fail6 = wait(cx->blobbifyRange(KeyRangeRef(activeRange.begin, middleKey), self->tenant));
		ASSERT(!fail6);

		bool fail7 = wait(cx->blobbifyRange(KeyRangeRef(middleKey, activeRange.end), self->tenant));

		ASSERT(!fail7);
		bool fail8 = wait(cx->blobbifyRange(KeyRangeRef(middleKey, middleKey2), self->tenant));
		ASSERT(!fail8);

		{
			Standalone<VectorRef<KeyRangeRef>> blobRanges =
			    wait(cx->listBlobbifiedRanges(range, 1000000, self->tenant));
			ASSERT(blobRanges.size() == 1);
			ASSERT(blobRanges[0] == activeRange);

			state Transaction tr(cx, self->tenant);
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

			state Version purgeVersion = deterministicRandom()->coinflip() ? latestVersion : 1;
			state KeyRangeRef purgeRange = deterministicRandom()->coinflip() ? activeRange : range;
			Key purgeKey = wait(cx->purgeBlobGranules(purgeRange, purgeVersion, self->tenant, true));
			wait(cx->waitPurgeGranulesComplete(purgeKey));

			if (deterministicRandom()->coinflip()) {
				// force purge again and ensure it is idempotent
				Key purgeKeyAgain = wait(cx->purgeBlobGranules(purgeRange, purgeVersion, self->tenant, true));
				wait(cx->waitPurgeGranulesComplete(purgeKeyAgain));
			}
		}

		// Check that the blob range is still listed
		{
			Standalone<VectorRef<KeyRangeRef>> blobRanges =
			    wait(cx->listBlobbifiedRanges(range, 1000000, self->tenant));
			ASSERT(blobRanges.size() == 1);
			ASSERT(blobRanges[0] == activeRange);

			// range that overlaps but does not completely include activeRange should fail to unblobbify
			bool unblobbifyFail1 = wait(cx->unblobbifyRange(KeyRangeRef(activeRange.begin, middleKey), self->tenant));
			ASSERT(!unblobbifyFail1);

			bool unblobbifyFail2 = wait(cx->unblobbifyRange(KeyRangeRef(middleKey, activeRange.end), self->tenant));
			ASSERT(!unblobbifyFail2);

			bool unblobbifyFail3 = wait(cx->unblobbifyRange(KeyRangeRef(activeRange.begin, middleKey), self->tenant));
			ASSERT(!unblobbifyFail3);

			bool unblobbifyFail4 = wait(cx->unblobbifyRange(KeyRangeRef(middleKey, activeRange.end), self->tenant));
			ASSERT(!unblobbifyFail4);

			bool unblobbifyFail5 = wait(cx->unblobbifyRange(KeyRangeRef(middleKey, middleKey2), self->tenant));
			ASSERT(!unblobbifyFail5);

			// several combinations of unblobbifying should work here, any that completely contain activeRange
			state int unblobbifyMethod = deterministicRandom()->randomInt(0, 4);
			if (unblobbifyMethod == 0) {
				bool unblobbifySuccess0 = wait(cx->unblobbifyRange(range, self->tenant));
				ASSERT(unblobbifySuccess0);
			} else if (unblobbifyMethod == 1) {
				bool unblobbifySuccess1 =
				    wait(cx->unblobbifyRange(KeyRangeRef(range.begin, activeRange.end), self->tenant));
				ASSERT(unblobbifySuccess1);
			} else if (unblobbifyMethod == 2) {
				bool unblobbifySuccess2 =
				    wait(cx->unblobbifyRange(KeyRangeRef(activeRange.begin, range.end), self->tenant));
				ASSERT(unblobbifySuccess2);
			} else if (unblobbifyMethod == 3) {
				bool unblobbifySuccess3 = wait(cx->unblobbifyRange(activeRange, self->tenant));
				ASSERT(unblobbifySuccess3);
			} else {
				ASSERT(false);
			}

			// unblobbify should be idempotent
			bool unblobbifySuccessAgain = wait(cx->unblobbifyRange(activeRange, self->tenant));
			ASSERT(unblobbifySuccessAgain);
		}

		return Void();
	}

	ACTOR Future<Void> reBlobbifyUnit(Database cx, BlobGranuleRangesWorkload* self, KeyRange range) {
		bool setSuccess = wait(cx->blobbifyRange(range, self->tenant));
		ASSERT(setSuccess);
		wait(self->checkRange(cx, self, range, true, true));

		// force purge range
		Key purgeKey = wait(self->versionedForcePurge(cx, range, self->tenant));
		wait(cx->waitPurgeGranulesComplete(purgeKey));
		wait(self->checkRange(cx, self, range, false, false));

		bool unsetSuccess = wait(cx->unblobbifyRange(range, self->tenant));
		ASSERT(unsetSuccess);
		wait(self->checkRange(cx, self, range, false, false));

		bool reSetSuccess = wait(cx->blobbifyRange(range, self->tenant));
		ASSERT(reSetSuccess);
		wait(self->checkRange(cx, self, range, true, true));

		wait(self->tearDownRangeAfterUnit(cx, self, range));

		return Void();
	}

	ACTOR Future<Void> adjacentPurge(Database cx, BlobGranuleRangesWorkload* self, KeyRange range) {
		// Create 2 adjacent blobbified regions.
		Key midKey = range.begin.withSuffix("mid"_sr);
		state KeyRange range1(KeyRangeRef(range.begin, midKey));
		state KeyRange range2(KeyRangeRef(midKey, range.end));

		state bool setSuccess = false;
		wait(store(setSuccess, cx->blobbifyRange(range1, self->tenant)));
		ASSERT(setSuccess);
		wait(self->checkRange(cx, self, range1, true, true));
		wait(store(setSuccess, cx->blobbifyRange(range2, self->tenant)));
		ASSERT(setSuccess);
		wait(self->checkRange(cx, self, range2, true, true));

		// force purge range
		state Key purgeKey;
		wait(store(purgeKey, self->versionedForcePurge(cx, range1, self->tenant)));
		wait(cx->waitPurgeGranulesComplete(purgeKey));
		wait(store(purgeKey, self->versionedForcePurge(cx, range2, self->tenant)));
		wait(cx->waitPurgeGranulesComplete(purgeKey));

		bool unsetSuccess = wait(cx->unblobbifyRange(range, self->tenant));
		ASSERT(unsetSuccess);

		wait(self->tearDownRangeAfterUnit(cx, self, range));

		return Void();
	}

	ACTOR Future<Void> blobbifyBlockingUnit(Database cx, BlobGranuleRangesWorkload* self, KeyRange range) {
		bool setSuccess = wait(cx->blobbifyRangeBlocking(range, self->tenant));
		ASSERT(setSuccess);
		bool verifySuccess = wait(self->isRangeActive(cx, range, self->tenant));
		ASSERT(verifySuccess);

		return Void();
	}

	// create tenant, set up a blob range, force purge blob range, and then delete tenant
	ACTOR Future<Void> deleteTenantUnit(Database cx, BlobGranuleRangesWorkload* self, KeyRange range) {
		if (!self->tenantName.present()) {
			return Void();
		}

		state Standalone<StringRef> newTenantName =
		    self->tenantName.get().withSuffix("_" + deterministicRandom()->randomUniqueID().toString());
		wait(success(self->setupTenant(cx, newTenantName)));
		state Reference<Tenant> newTenant = makeReference<Tenant>(cx, newTenantName);
		wait(newTenant->ready());

		bool setSuccess = wait(cx->blobbifyRangeBlocking(range, newTenant));
		ASSERT(setSuccess);

		Key purgeKey = wait(cx->purgeBlobGranules(range, 1, newTenant, true));
		wait(cx->waitPurgeGranulesComplete(purgeKey));

		bool unblobbifySuccess = wait(cx->unblobbifyRange(range, newTenant));
		ASSERT(unblobbifySuccess);

		wait(TenantAPI::deleteTenant(cx.getReference(), newTenantName));

		return Void();
	}

	// Simulate having many consecutive ranges within one subrange registered, and unblobbifying some prefix of them
	// (potentially the whole range)
	ACTOR Future<Void> unblobbifyManyUnit(Database cx, BlobGranuleRangesWorkload* self, KeyRange range) {
		// Create 2 adjacent blobbified regions.
		state int ranges = deterministicRandom()->randomInt(2, 10);
		state int prefixToUnblobbify = deterministicRandom()->randomInt(1, ranges + 1);

		state int i;
		for (i = 0; i < ranges; i++) {
			state KeyRange subRange(KeyRangeRef(range.begin.withSuffix("_" + std::to_string(i)),
			                                    range.begin.withSuffix("_" + std::to_string(i + 1))));
			bool success = wait(cx->blobbifyRange(subRange, self->tenant));
			ASSERT(success);
			wait(self->checkRange(cx, self, subRange, true, true));
		}

		// sometimes start purge and unblobbify at logical range start, other times at first subrange start
		Key startKey = range.begin;
		if (deterministicRandom()->coinflip()) {
			startKey = startKey.withSuffix("_0"_sr);
		}

		// if purge is full range, sometimes end it at logical range end, other times at subrange boundary
		state Key endKey = range.end;
		if (prefixToUnblobbify < ranges || deterministicRandom()->coinflip()) {
			endKey = range.begin.withSuffix("_" + std::to_string(prefixToUnblobbify));
		}

		state KeyRange unblobbifyRange(KeyRangeRef(startKey, endKey));

		state Key purgeKey;
		wait(store(purgeKey, self->versionedForcePurge(cx, unblobbifyRange, self->tenant)));
		wait(cx->waitPurgeGranulesComplete(purgeKey));

		bool unsetSuccess = wait(cx->unblobbifyRange(unblobbifyRange, self->tenant));
		ASSERT(unsetSuccess);

		// check that all ranges after the unblobbified boundary are still valid
		for (i = prefixToUnblobbify; i < ranges; i++) {
			KeyRange checkRange(KeyRangeRef(range.begin.withSuffix("_" + std::to_string(i)),
			                                range.begin.withSuffix("_" + std::to_string(i + 1))));
			wait(self->checkRange(cx, self, checkRange, true, true));
		}

		// tear down the part of the test that we didn't already do
		wait(self->tearDownRangeAfterUnit(cx, self, KeyRangeRef(endKey, range.end)));

		return Void();
	}

	enum UnitTestTypes {
		VERIFY_RANGE_UNIT,
		VERIFY_RANGE_GAP_UNIT,
		RANGES_MISALIGNED,
		BLOBBIFY_IDEMPOTENT,
		RE_BLOBBIFY,
		ADJACENT_PURGE,
		BLOBBIFY_BLOCKING_UNIT,
		DELETE_TENANT_UNIT,
		UNBLOBBIFY_MANY_UNIT,
		OP_COUNT = 9 /* keep this last */
	};

	ACTOR Future<Void> blobGranuleRangesUnitTests(Database cx, BlobGranuleRangesWorkload* self) {
		if (deterministicRandom()->coinflip()) {
			cx->internal = IsInternal::False;
		}
		loop {
			if (self->stopUnitClient) {
				return Void();
			}
			std::set<UnitTestTypes> excludedTypes;
			excludedTypes.insert(OP_COUNT);

			// FIXME: fix bugs and enable these tests!
			excludedTypes.insert(RE_BLOBBIFY); // TODO - fix is non-trivial, is desired behavior eventually

			std::string nextRangeKey = "U_" + self->newKey();
			state KeyRange range(KeyRangeRef(StringRef(nextRangeKey), strinc(StringRef(nextRangeKey))));
			// prevent infinite loop
			int loopTries = 1000;
			int op = OP_COUNT;
			loop {
				op = deterministicRandom()->randomInt(0, OP_COUNT);
				if (!excludedTypes.count((UnitTestTypes)op)) {
					break;
				}
				loopTries--;
				ASSERT(loopTries >= 0);
			}
			if (BGRW_DEBUG) {
				fmt::print(
				    "Selected range [{0} - {1}) for unit {2}.\n", range.begin.printable(), range.end.printable(), op);
			}

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
			} else if (op == ADJACENT_PURGE) {
				wait(self->adjacentPurge(cx, self, range));
			} else if (op == BLOBBIFY_BLOCKING_UNIT) {
				wait(self->blobbifyBlockingUnit(cx, self, range));
			} else if (op == DELETE_TENANT_UNIT) {
				wait(self->deleteTenantUnit(cx, self, range));
			} else if (op == UNBLOBBIFY_MANY_UNIT) {
				wait(self->unblobbifyManyUnit(cx, self, range));
			} else {
				ASSERT(false);
			}

			wait(delay(1.0));
		}
	}
};

WorkloadFactory<BlobGranuleRangesWorkload> BlobGranuleRangesWorkloadFactory;
