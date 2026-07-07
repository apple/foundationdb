/*
 * WatchCommitTimeout.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "flow/Coroutines.h"
#include "flow/Error.h"

// Regression test for the watch / commit-error contract:
//
//   "If the transaction used to create a watch encounters an error during
//    commit, the watch must eventually resolve (with an error) rather than
//    hang forever."
//
// A transaction timeout (FDB_TR_OPTION_TIMEOUT) fires through the transaction's
// reset promise. If it fires while the commit is in flight, the commit fails with
// transaction_timed_out. Previously the native commit actor was cancelled before
// it could propagate that error to the transaction's watches, so the watch future
// was orphaned: it never fired and never errored, and any caller awaiting it hung
// forever.
//
// This workload repeatedly arms a watch on a transaction with a short timeout and
// commits. Whenever the commit times out, it waits up to watchResolveBound seconds
// for the watch to become ready; an unresolved watch increments orphanedWatches.
// check() returns false if any were seen. The workload does NOT assert anything
// about HOW the watch resolves — under cluster recovery the storage-server-side
// watch actor can resolve it with its own error (transaction_too_old,
// process_behind, etc.), or even with success (Void), before the commit-error
// propagation path reaches the promise. All such outcomes satisfy the "no orphan"
// contract this test is designed to verify.
struct WatchCommitTimeoutWorkload : TestWorkload {
	static constexpr auto NAME = "WatchCommitTimeout";

	double testDuration;
	int64_t timeoutMs;
	double watchResolveBound;
	int actorsPerClient;
	std::vector<Future<Void>> clients;

	PerfIntCounter commitErrors, commitSuccesses, orphanedWatches;

	explicit WatchCommitTimeoutWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), commitErrors("CommitErrors"), commitSuccesses("CommitSuccesses"),
	    orphanedWatches("OrphanedWatches") {
		testDuration = getOption(options, "testDuration"_sr, 30.0);
		// Small timeout so it frequently fires while the commit is still in flight.
		timeoutMs = getOption(options, "timeoutMs"_sr, (int64_t)30);
		// How long a watch is allowed to stay pending after its commit has failed
		// before we declare it orphaned. Well above any legitimate delivery latency.
		watchResolveBound = getOption(options, "watchResolveBound"_sr, 60.0);
		actorsPerClient = getOption(options, "actorsPerClient"_sr, 5);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		for (int c = 0; c < actorsPerClient; c++) {
			clients.push_back(worker(cx, this, c));
		}
		return waitForAll(clients);
	}

	Future<bool> check(Database const& cx) override {
		TraceEvent("WatchCommitTimeoutSummary")
		    .detail("CommitErrors", commitErrors.getValue())
		    .detail("CommitSuccesses", commitSuccesses.getValue())
		    .detail("OrphanedWatches", orphanedWatches.getValue());
		return orphanedWatches.getValue() == 0;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(commitErrors.getMetric());
		m.push_back(commitSuccesses.getMetric());
		m.push_back(orphanedWatches.getMetric());
	}

	// Key written by the transaction. Kept separate from the watched key so the watch
	// can only become ready via error propagation, not via an observed key change.
	Key setKeyFor(int clientIdx, uint64_t iteration) const {
		return Key(StringRef(
		    format("watchCommitTimeout/s/%03d-%03d-%012llx", clientId, clientIdx, (unsigned long long)iteration)));
	}

	// Key watched by the transaction. Never written, so it only resolves when cancelled
	// with a commit error (or explicitly cancelled on success).
	Key watchKeyFor(int clientIdx, uint64_t iteration) const {
		return Key(StringRef(
		    format("watchCommitTimeout/w/%03d-%03d-%012llx", clientId, clientIdx, (unsigned long long)iteration)));
	}

	Future<Void> worker(Database cx, WatchCommitTimeoutWorkload* self, int clientIdx) {
		double endTime = now() + self->testDuration;
		uint64_t iteration = 0;
		try {
			while (now() < endTime) {
				Key setKey = self->setKeyFor(clientIdx, iteration);
				Key watchKey = self->watchKeyFor(clientIdx, iteration);
				++iteration;

				ReadYourWritesTransaction tr(cx);
				// Arm a transaction timeout so the commit below can be cancelled by it.
				tr.setOption(FDBTransactionOptions::TIMEOUT, StringRef((uint8_t*)&self->timeoutMs, sizeof(int64_t)));
				tr.set(setKey, "v"_sr);
				Future<Void> watch = tr.watch(watchKey);

				// co_await is not permitted inside a catch handler; capture the commit
				// error here so the watch wait below can run outside any catch block.
				Optional<Error> commitError;
				try {
					co_await tr.commit();
				} catch (Error& e) {
					if (e.code() == error_code_actor_cancelled) {
						throw;
					}
					if (e.code() != error_code_transaction_timed_out) {
						// Transient error unrelated to the bug under test; skip iteration.
						watch.cancel();
						continue;
					}
					commitError = e;
				}

				if (!commitError.present()) {
					// Commit succeeded: cancel the watch (watchKey was never written,
					// so it won't fire naturally) and move on.
					++self->commitSuccesses;
					watch.cancel();
					continue;
				}

				++self->commitErrors;
				// Contract: a commit error must resolve the watch (error OR success;
				// the SS-side watch actor can race with the commit-error propagation
				// path and resolve the watch with Void() under cluster recovery).
				// Bound the wait so a regression (orphaned watch) is detected here
				// rather than hanging forever.
				try {
					co_await (watch || delay(self->watchResolveBound));
				} catch (Error& e2) {
					if (e2.code() == error_code_actor_cancelled) {
						throw;
					}
					// Any other error is a legitimate watch resolution; fall through
					// to the orphan check.
				}

				if (!watch.isReady()) {
					++self->orphanedWatches;
					TraceEvent(SevError, "WatchOrphanedAfterCommitError")
					    .error(commitError.get())
					    .detail("WatchKey", watchKey)
					    .detail("ResolveBoundSec", self->watchResolveBound);
				}
				// Watch resolved (or was just declared orphan): the "no orphan"
				// contract — every watch on a failed commit eventually resolves —
				// is the actual property under test, and check() enforces it via
				// orphanedWatches==0. We don't assert a specific outcome on the
				// watch here because the resolution can come from either the
				// commit-error propagation path or the SS-side watch actor under
				// cluster recovery.
				watch.cancel();
			}
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				TraceEvent(SevError, "WatchCommitTimeoutWorkerError").error(e).detail("ClientIdx", clientIdx);
			}
			throw;
		}
	}
};

WorkloadFactory<WatchCommitTimeoutWorkload> WatchCommitTimeoutWorkloadFactory;
