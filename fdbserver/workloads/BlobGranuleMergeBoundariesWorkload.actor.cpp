/*
 * BlobGranuleMergeBoundariesWorkload.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/Util.h"
#include <cstring>
#include <limits>

#include "flow/actorcompiler.h" // This must be the last #include.

/*
 * Verifies that, depending on the specified functionality for splitting a tuple prefix with
 * bg_key_tuple_truncate_offset=1 Writes several normal granules' worth of data to one tuple prefix, and validates the
 * desired configuration of splitting within a tuple prefix being allowed/disallowed.
 */
struct BlobGranuleMergeBoundariesWorkload : TestWorkload {
	static constexpr auto NAME = "BlobGranuleMergeBoundaries";
	int targetGranules;
	bool initAfter;
	int nodeCount;
	int targetValueLen;

	Optional<TenantName> tenantName;
	Optional<Reference<Tenant>> tenant;

	BlobGranuleMergeBoundariesWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		targetGranules = 3 + sharedRandomNumber % 6;
		sharedRandomNumber /= 6;
		initAfter = (sharedRandomNumber % 4) == 0;
		sharedRandomNumber /= 4;
		targetValueLen = 100 * (1 + sharedRandomNumber % 10);
		sharedRandomNumber /= 10;

		int64_t targetBytes = targetGranules * SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES;
		targetBytes = std::max<int64_t>(
		    1000000, targetBytes); // write at least 1 MB to avoid very small granule/byte sample issues
		nodeCount = (int)(targetBytes / targetValueLen);

		tenantName = "bgMergeBoundsTenant"_sr;

		// FIXME: maybe enable for completeness at some point? We probably will never convert non-empty ranges to blob
		// after 71.3
		initAfter = false;

		TraceEvent("BlobGranuleMergeBoundariesWorkloadInit")
		    .detail("TargetGranules", targetGranules)
		    .detail("InitAfter", initAfter)
		    .detail("TargetValSize", targetValueLen)
		    .detail("TargetBytes", targetBytes)
		    .detail("GranuleSize", SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES)
		    .detail("NodeCount", nodeCount);
	}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	ACTOR Future<Void> setUpBlobRange(Database cx, BlobGranuleMergeBoundariesWorkload* self) {
		bool success = wait(cx->blobbifyRange(normalKeys, self->tenant));
		ASSERT(success);
		return Void();
	}

	// Functions required by `bulkSetup()`
	// key is always a 2-tuple with the same first element and a different last element
	Key keyForIndex(int n) { return Tuple::makeTuple(7, n).pack(); }
	Value value(int n) {
		// FIXME: shared with BlobGranuleCorrectnessWorkload
		int valLen = deterministicRandom()->randomInt(1, 2 * targetValueLen);
		valLen = std::max(10, valLen);
		std::string v(valLen, 'z');
		auto valFormatted = format("%08x", n);
		ASSERT(valFormatted.size() <= v.size());

		for (int i = 0; i < valFormatted.size(); i++) {
			v[i] = valFormatted[i];
		}
		// copy into an arena
		// TODO do this in original arena? a bit more efficient that way
		Arena a;
		return Standalone<StringRef>(StringRef(a, v), a);
	}
	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(keyForIndex(n), value(n)); }

	ACTOR Future<Void> _setup(Database cx, BlobGranuleMergeBoundariesWorkload* self) {
		if (self->clientId != 0) {
			return Void();
		}
		TraceEvent("BlobGranuleMergeBoundariesInit")
		    .detail("TargetGranules", self->targetGranules)
		    .detail("InitAfter", self->initAfter);

		// set up blob granules
		wait(success(ManagementAPI::changeConfig(cx.getReference(), "blob_granules_enabled=1", true)));

		Optional<TenantMapEntry> entry = wait(TenantAPI::createTenant(cx.getReference(), self->tenantName.get()));
		ASSERT(entry.present());
		self->tenant = makeReference<Tenant>(cx, self->tenantName.get());

		if (!self->initAfter) {
			wait(self->setUpBlobRange(cx, self));

			TraceEvent("BlobGranuleMergeBoundariesSetupVerifying");
			loop {
				Version checkVersion = wait(cx->verifyBlobRange(normalKeys, latestVersion, self->tenant));
				if (checkVersion != -1) {
					break;
				}

				TraceEvent("BlobGranuleMergeBoundariesSetupVerifyRetrying");

				wait(delay(1.0));
			}
		}

		TraceEvent("BlobGranuleMergeBoundariesLoading");

		// we only have one client and bulk setup divides the writes amongst them, so multiply node count by client
		// count
		wait(bulkSetup(cx,
		               self,
		               self->nodeCount * self->clientCount,
		               Promise<double>(),
		               true,
		               0.0,
		               1e12,
		               std::vector<uint64_t>(),
		               Promise<std::vector<std::pair<uint64_t, double>>>(),
		               0,
		               0.1,
		               0,
		               0,
		               { self->tenant.get() }));

		TraceEvent("BlobGranuleMergeBoundariesLoadingComplete");

		if (self->initAfter) {
			wait(self->setUpBlobRange(cx, self));
		}

		TraceEvent("BlobGranuleMergeBoundariesSetupComplete");
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		// no test phase
		return Void();
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }

	ACTOR Future<bool> _check(Database cx, BlobGranuleMergeBoundariesWorkload* self) {
		if (self->clientId != 0) {
			return true;
		}
		state Key tuplePrefix = Tuple::makeTuple(7).pack();
		// FIXME: checking normalKeys finds another empty granule, that's metadata overhead we should fix at some point
		state KeyRange tupleRange(KeyRangeRef(tuplePrefix, strinc(tuplePrefix)));
		TraceEvent("BlobGranuleMergeBoundariesCheckStart").detail("Range", tupleRange);
		loop {
			Version checkVersion = wait(cx->verifyBlobRange(tupleRange, latestVersion, self->tenant));
			if (checkVersion != -1) {
				TraceEvent("BlobGranuleMergeBoundariesCheckRead").detail("CheckVersion", checkVersion);
				break;
			}

			TraceEvent("BlobGranuleMergeBoundariesCheckRetrying");

			wait(delay(1.0));
		}

		state Transaction tr(cx, self->tenant);
		loop {
			try {
				Standalone<VectorRef<KeyRangeRef>> granules = wait(tr.getBlobGranuleRanges(tupleRange, 1000000));
				TraceEvent("BlobGranuleMergeBoundariesCheckGranules")
				    .detail("GranuleCount", granules.size())
				    .detail("EnableSplitTruncated", SERVER_KNOBS->BG_ENABLE_SPLIT_TRUNCATED)
				    .detail("TruncateOffset", SERVER_KNOBS->BG_KEY_TUPLE_TRUNCATE_OFFSET);
				if (SERVER_KNOBS->BG_ENABLE_SPLIT_TRUNCATED) {
					// test the test to ensure in the case where this knob wasn't set, we would be producing multiple
					// granules
					// FIXME: sometimes behind granule resnapshotting means we still only have one granule so we can't
					// assert > 1
					ASSERT(granules.size() >= 1);
				} else {
					ASSERT(granules.size() == 1);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		TraceEvent("BlobGranuleMergeBoundariesCheckDone");

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<BlobGranuleMergeBoundariesWorkload> BlobGranuleMergeBoundariesWorkloadFactory;
