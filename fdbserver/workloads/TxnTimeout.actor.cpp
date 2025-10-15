#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Buggify.h"
#include "flow/Error.h"
#include "flow/Optional.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct TxnTimeout : TestWorkload {
	static constexpr auto NAME = "TxnTimeout";

	bool enabled{ false };
	double testDuration{ 0.0 };
	int actorsPerClient{ 0 };
	int nodeCountPerClientPerActor{ 0 };
	double txnMinSecDuration{ 0 };

	int txnsTotal{ 0 };
	int txnsSucceeded{ 0 };
	int txnsFailed{ 0 };

	TxnTimeout(const WorkloadContext& wctx) : TestWorkload(wctx) {
		testDuration = getOption(options, "testDuration"_sr, 120);
		actorsPerClient = getOption(options, "actorsPerClient"_sr, 1);
		nodeCountPerClientPerActor = getOption(options, "nodeCountPerClientPerActor"_sr, 100);
		txnMinSecDuration = getOption(options, "txnMinSecDuration"_sr, 5);
	}

	Future<Void> setup(const Database& db) override {
		TraceEvent("TxnTimeoutSetup")
		    .detail("Enabled", enabled)
		    .detail("TestDuration", testDuration)
		    .detail("ActorsPerClient", actorsPerClient)
		    .detail("NodeCountPerClientPerActor", nodeCountPerClientPerActor)
		    .detail("TxnTimeoutThreshold", txnMinSecDuration);
		return Void();
	}

	Future<Void> start(const Database& db) override {
		if (!g_network->isSimulated() || isGeneralBuggifyEnabled()) {
			return Void();
		}
		return timeout(reportErrors(workload(this, db), "TxnTimeoutError"), testDuration, Void());
	}

	Future<bool> check(const Database& db) override {
		if (!g_network->isSimulated() || isGeneralBuggifyEnabled()) {
			return true;
		}
		if (txnsFailed > 0 || txnsSucceeded == 0 || txnsSucceeded != txnsTotal) {
			TraceEvent(SevError, "TxnTimeoutCheckErrorFailures")
			    .detail("TxnsSucceeded", txnsSucceeded)
			    .detail("TxnsFailed", txnsFailed)
			    .detail("TxnsTotal", txnsTotal);
		}
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	ACTOR static Future<Void> testData(TxnTimeout* self, Database db) {
		TraceEvent("TxnTimeoutTestDataStart");
		state RangeResult keyRanges;
		state Transaction tr(db);
		TraceEvent("TxnTimeoutTestDataMid1");
		loop {
			try {
				state RangeResult keyRanges_ =
				    wait(tr.getRange(KeyRangeRef{ "foo"_sr, strinc("foo"_sr) }, CLIENT_KNOBS->TOO_MANY));
				keyRanges = keyRanges_;
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		TraceEvent("TxnTimeoutTestDataMid2");
		for (auto& kv : keyRanges) {
			TraceEvent("TxnTimeoutTestData").detail("Key", kv.key);
		}
		TraceEvent("TxnTimeoutTestDataEnd");
		return Void();
	}

	ACTOR static Future<Void> fill(TxnTimeout* self, Database db, int actorIdx) {
		TraceEvent("TxnTimeoutFillStart").detail("ActorIdx", actorIdx);
		state int nodeIdx = 0;
		loop {
			if (nodeIdx == self->nodeCountPerClientPerActor) {
				TraceEvent("TxnTimeoutFillEnd").detail("ActorIdx", actorIdx);
				return Void();
			}
			state Transaction tr(db);
			loop {
				try {
					state std::string key = std::format("foo_client{}_actor{}_node{}",
					                                    std::to_string(self->clientId),
					                                    std::to_string(actorIdx),
					                                    std::to_string(nodeIdx));
					state std::string val = "0";
					tr.set(key, val);
					wait(tr.commit());
					nodeIdx++;
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}

	ACTOR static Future<Void> fillForAllActors(TxnTimeout* self, Database db) {
		TraceEvent("TxnTimeoutFillForAllActorsStart");
		std::vector<Future<Void>> fills;
		for (int actorIdx = 0; actorIdx < self->actorsPerClient; ++actorIdx) {
			fills.push_back(fill(self, db, actorIdx));
		}
		wait(waitForAll(fills));
		TraceEvent("TxnTimeoutFillForAllActorsEnd");
		return Void();
	}

	// Serially (via async api) makes txns
	// These txns intentionally wait upto TIMEOUT limit picked in knob
	// I should dump that value in a trace event at test start time
	// Then: for every client, I should know: num txns, total time it took, fail count (couldn't go upto TIMEOUT i.e.
	// got too_old too early)
	// Maybe the test should disable any fault injection and do buggify = false? Goal is to
	// ensure we are within timeout bounds.. to that end, no txn should fail?
	// TODO: add more client workloads.. e.g. read snapshot, write only, read-write mixed.. for now let's do read only
	// TODO: make the workload fail for too_old cases
	// THINK: hard code testing for 10 seconds?
	// ADD: table scan use-case, i think if i tr.get *after* 5 seconds of creating the txn, then it should start showing
	// too_old issue
	// TODO: improve check by only checking txns for failures if we know that txn start -> commit is less than 10s
	// (assuming 10s timeout). generally i pin at 7, but it's possible initial read let's say takes 11, then there's no
	// wait start->commit is under 10, so failure is expected. in practice however, given i am disabling fault injection
	// and buggify, read should never be that long. perhaps i can enable those faults and with this improved/accurate
	// checking, still get test to pass 100K times?
	ACTOR static Future<Void> txnClient(TxnTimeout* self, Database db, int actorIdx) {
		TraceEvent("TxnTimeoutTxnClientStart");
		state int nodeIdx = 0;
		state double tStart = now();
		loop {
			if (nodeIdx == self->nodeCountPerClientPerActor) {
				nodeIdx = 0; // reset
			}
			if (now() - tStart > self->testDuration * 0.8) {
				TraceEvent("TxnTimeoutTxnClientDurationOver");
				break;
			}
			TraceEvent("TxnTimeoutTxnClient1a");
			state Transaction tr(db);
			state Version rv1 = 0;
			self->txnsTotal++;
			loop {
				try {
					state std::string key = std::format("foo_client{}_actor{}_node{}",
					                                    std::to_string(self->clientId),
					                                    std::to_string(actorIdx),
					                                    std::to_string(nodeIdx));
					TraceEvent("TxnTimeoutTxnClient1b");
					state double readStartT = now();
					state Version rv1_ = wait(tr.getReadVersion());
					rv1 = rv1_; // todo: there's syntatic sugar for this somewhere i remember
					state Optional<Value> val = wait(tr.get(StringRef(key)));
					state double readDuration = now() - readStartT;
					TraceEvent("TxnTimeoutTxnClient2");
					if (self->txnMinSecDuration > readDuration) {
						wait(delay(self->txnMinSecDuration - readDuration));
					}
					const int newVal = std::stoi(val.get().toString()) + 1;
					tr.set(key, std::to_string(newVal));
					state double latency = now() - tStart;
					TraceEvent("TxnTimeoutTxnClient3");
					wait(tr.commit());
					self->txnsSucceeded++;
					TraceEvent("TxnTimeoutTxnClient4")
					    .detail("Key", key)
					    .detail("Val", val.get())
					    .detail("Latency", latency);
					nodeIdx++;
					break;
				} catch (Error& e) {
					state bool okError = e.code() == error_code_future_version ||
					                     e.code() == error_code_commit_unknown_result ||
					                     e.code() == error_code_process_behind;
					TraceEvent("TxnTimeoutTxnClientError")
					    .detail("RecoveryState", self->dbInfo->get().recoveryState)
					    .errorUnsuppressed(e);

					wait(tr.onError(e));

					Version rv2 = wait(tr.getReadVersion());
					// two cases: 1) recovery 100M bump, or 2) stale rv issue (attach graphs in pr)
					const bool rvDeltaHigh = (rv2 - rv1) > SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS;
					if (okError || rvDeltaHigh) {
						TraceEvent("TxnTimeoutRecoveryNotBumpingFailed").detail("RV1", rv1).detail("RV2", rv2);
					} else {
						self->txnsFailed++;
						TraceEvent("TxnTimeoutNoRecoveryBumpingFailed").detail("RV1", rv1).detail("RV2", rv2);
					}
				}
			}
		}
		return Void();
	}

	ACTOR Future<Void> workload(TxnTimeout* self, Database db) {
		TraceEvent("TxnTimeoutWorkloadStart");

		// Phase 1: fill
		TraceEvent("TxnTimeoutWorkloadFillStart");
		wait(fillForAllActors(self, db));
		TraceEvent("TxnTimeoutWorkloadFillEnd");
		wait(testData(self, db));

		// Phase 2: timeout check
		state std::vector<Future<Void>> txnClients;
		for (int actorIdx = 0; actorIdx < self->actorsPerClient; ++actorIdx) {
			txnClients.emplace_back(txnClient(self, db, actorIdx));
		}

		wait(waitForAll(txnClients));

		TraceEvent("TxnTimeoutWorkloadMetrics")
		    .detail("TxnsSucceeded", self->txnsSucceeded)
		    .detail("TxnsFailed", self->txnsFailed)
		    .detail("TxnsTotal", self->txnsTotal);

		TraceEvent("TxnTimeoutWorkloadEnd");
		return Void();
	}
};

WorkloadFactory<TxnTimeout> TxnTimeoutWorkloadFactory;
