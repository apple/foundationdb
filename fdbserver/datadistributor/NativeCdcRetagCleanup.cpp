/*
 * NativeCdcRetagCleanup.cpp
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

#include <cmath>
#include <vector>

#include "NativeCdcRetagCleanup.h"
#include "fdbserver/core/NativeCdcMetadata.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/Knobs.h"
#include "flow/CodeProbe.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

namespace {

class NativeCdcCleanupProgress {
	Optional<Value> assignmentChange;
	Key next = cdcStreamKeys.begin;
	bool cycleComplete = false;
	bool sawPending = false;
	bool rescan = false;

	bool sameGeneration(ValueRef generation) const {
		return assignmentChange.present() && assignmentChange.get() == generation;
	}

public:
	bool needsScan(ValueRef generation) const {
		return !sameGeneration(generation) || !cycleComplete || sawPending || rescan;
	}

	Key begin() const { return cycleComplete ? Key(cdcStreamKeys.begin) : next; }

	void scanned(Value generation, Key nextBegin, bool lastPage, bool pagePending) {
		const bool continuing = assignmentChange.present() && !cycleComplete;
		sawPending = (continuing && sawPending) || pagePending;
		rescan = continuing && (rescan || !sameGeneration(generation));
		assignmentChange = std::move(generation);
		next = std::move(nextBegin);
		cycleComplete = lastPage;
	}

	// Churn requires another traversal, not a restart that can starve later pages.
	void changed() { rescan = true; }
};

class NativeCdcRetagCleanup {
	Database cx;
	MoveKeysLock lock;
	const DDEnabledState* ddEnabledState;
	NativeCdcCleanupProgress cleanupProgress;

	Future<bool> finishPendingPage() {
		constexpr int cleanupPageSize = 100;
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				const Optional<Value> change = co_await tr.get(cdcProxyAssignmentChangeKey);
				const Value generation = change.present() ? change.get() : Value();
				if (!cleanupProgress.needsScan(generation)) {
					co_return false;
				}
				const Key begin = cleanupProgress.begin();
				const RangeResult page = co_await tr.getRange(KeyRangeRef(begin, cdcStreamKeys.end), cleanupPageSize);
				std::vector<Future<Optional<NativeCdcTagState>>> reads;
				for (const auto& row : page) {
					reads.push_back(readNativeCdcTagState(&tr, decodeCDCStreamKey(row.key)));
				}
				const std::vector<Optional<NativeCdcTagState>> states = co_await getAll(reads);
				int finished = 0;
				bool pagePending = false;
				for (const auto& state : states) {
					if (!state.present()) {
						// Incomplete ownership/metadata is not evidence that all transitions have drained.
						pagePending = true;
						continue;
					}
					pagePending = pagePending || state.get().pending;
					if (state.get().pending && (co_await finishNativeCdcRetag(&tr, state.get()))) {
						++finished;
					}
				}
				if (finished == 0) {
					cleanupProgress.scanned(generation,
					                        page.more ? keyAfter(page.back().key) : Key(cdcStreamKeys.end),
					                        !page.more,
					                        pagePending);
					co_return false;
				}
				co_await checkMoveKeysLock(&tr, lock, ddEnabledState);
				co_await tr.commit();
				cleanupProgress.scanned(generation,
				                        page.more ? keyAfter(page.back().key) : Key(cdcStreamKeys.end),
				                        !page.more,
				                        pagePending);
				cleanupProgress.changed();
				CODE_PROBE(true, "Native CDC DD finishes acknowledged tag transitions");
				TraceEvent("NativeCdcTagTransitionsFinished", lock.myOwner).detail("Streams", finished);
				co_return true;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

public:
	NativeCdcRetagCleanup(Database cx, MoveKeysLock lock, const DDEnabledState* ddEnabledState)
	  : cx(cx), lock(lock), ddEnabledState(ddEnabledState) {}

	Future<Void> run(Future<Void> initialized) {
		co_await initialized;
		while (true) {
			try {
				co_await finishPendingPage();
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled || e.code() == error_code_broken_promise ||
				    e.code() == error_code_movekeys_conflict) {
					throw;
				}
				TraceEvent(SevWarn, "NativeCdcRetagCleanupError", lock.myOwner).error(e);
			}
			const double interval = SERVER_KNOBS->NATIVE_CDC_RETAG_CLEANUP_INTERVAL;
			co_await delay(std::isfinite(interval) && interval > 0 ? interval : 30.0, TaskPriority::DataDistribution);
		}
	}
};

TEST_CASE("/NativeCDC/RetagCleanup/Paging") {
	NativeCdcCleanupProgress progress;
	const Value firstGeneration = "first"_sr;
	const Value secondGeneration = "second"_sr;
	const Key nextPage = keyAfter(cdcStreamKeyFor(100));
	ASSERT(progress.needsScan(firstGeneration));
	ASSERT_EQ(progress.begin(), cdcStreamKeys.begin);
	progress.scanned(firstGeneration, nextPage, false, false);
	ASSERT(progress.needsScan(firstGeneration));
	ASSERT_EQ(progress.begin(), nextPage);
	progress.scanned(firstGeneration, cdcStreamKeys.end, true, true);
	// Acknowledgements do not change the assignment generation, so pending cycles must repeat.
	ASSERT(progress.needsScan(firstGeneration));
	ASSERT_EQ(progress.begin(), cdcStreamKeys.begin);
	progress.scanned(firstGeneration, cdcStreamKeys.end, true, false);
	ASSERT(!progress.needsScan(firstGeneration));
	ASSERT(progress.needsScan(secondGeneration));
	ASSERT_EQ(progress.begin(), cdcStreamKeys.begin);
	progress.changed();
	ASSERT(progress.needsScan(firstGeneration));
	return Void();
}

TEST_CASE("/NativeCDC/RetagCleanup/ProgressAcrossChurn") {
	NativeCdcCleanupProgress progress;
	const Value firstGeneration = "first"_sr;
	const Value secondGeneration = "second"_sr;
	const Value thirdGeneration = "third"_sr;
	const Key secondPage = keyAfter(cdcStreamKeyFor(100));
	const Key thirdPage = keyAfter(cdcStreamKeyFor(200));
	progress.scanned(firstGeneration, secondPage, false, true);
	progress.changed(); // A successful cleanup on the first page changes the generation.
	ASSERT(progress.needsScan(secondGeneration));
	ASSERT_EQ(progress.begin(), secondPage);
	progress.scanned(secondGeneration, thirdPage, false, true);
	progress.changed();
	ASSERT(progress.needsScan(thirdGeneration));
	ASSERT_EQ(progress.begin(), thirdPage);
	progress.scanned(thirdGeneration, cdcStreamKeys.end, true, false);
	ASSERT(progress.needsScan(thirdGeneration));
	ASSERT_EQ(progress.begin(), cdcStreamKeys.begin);
	progress.scanned(thirdGeneration, cdcStreamKeys.end, true, false);
	ASSERT(!progress.needsScan(thirdGeneration));
	return Void();
}

} // namespace

Future<Void> nativeCdcRetagCleanup(Database cx,
                                   MoveKeysLock lock,
                                   const DDEnabledState* ddEnabledState,
                                   Future<Void> initialized) {
	NativeCdcRetagCleanup cleanup(cx, lock, ddEnabledState);
	co_await cleanup.run(initialized);
}

void forceLinkNativeCdcRetagCleanupTests() {}
