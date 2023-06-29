/**
 * TooManyGrvBatchers.actor.cpp
 */

#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // must be last include

class TooManyGrvBatchersWorkload : public TestWorkload {
	Database cx;

	static TransactionTag getTag(int index) { return TransactionTag(format("tag/%06d", index)); }

	ACTOR static Future<Void> getReadVersion(Database cx, int index) {
		loop {
			try {
				state Transaction tr(cx);
				tr.setOption(FDBTransactionOptions::AUTO_THROTTLE_TAG, getTag(index));
				wait(success(tr.getReadVersion()));
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> grvFetcher(Database cx, int index) {
		loop {
			wait(getReadVersion(cx, index));
			wait(delayJittered(1.0));
		}
	}

	ACTOR static Future<Void> runManyGrvBatchers(Database cx, int numActors) {
		std::vector<Future<Void>> actors;
		for (int i = 0; i < numActors; ++i) {
			actors.push_back(grvFetcher(cx, i));
		}
		try {
			wait(waitForAll(actors));
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_too_many_grv_batchers);
			return Void();
		}
		ASSERT(false);
		return Void();
	}

	ACTOR static Future<Void> _start(Database cx) {
		state int numActors = 2 * CLIENT_KNOBS->MAX_GRV_BATCHERS;

		wait(delay(1.0));

		// Run too many GRV batchers, get an error
		wait(runManyGrvBatchers(cx, numActors));

		// wait long enough for idle GRV batchers to expire
		wait(delay(CLIENT_KNOBS->GRV_BATCHER_EXPIRATION_TIMEOUT + CLIENT_KNOBS->GRV_BATCHER_CLEANING_INTERVAL * 2));

		// Now getting a read version with a new tag should succeed
		wait(getReadVersion(cx, numActors));

		return Void();
	}

public:
	static constexpr auto NAME = "TooManyGrvBatchers";
	explicit TooManyGrvBatchersWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}
	Future<Void> setup(Database const& cx) override {
		this->cx = cx->clone();
		this->cx->debugUseTag = false;
		return Void();
	}
	Future<Void> start(Database const&) override { return clientId ? Void() : _start(this->cx); }
	Future<bool> check(Database const&) override { return true; }
	void getMetrics(std::vector<PerfMetric>&) override {}
};

WorkloadFactory<TooManyGrvBatchersWorkload> TooManyGrvBatchersWorkloadFactory;
