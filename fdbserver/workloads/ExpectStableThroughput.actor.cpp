/**
 * ExpectStableThroughput.actor.cpp
 */

#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last include

class ExpectStableThroughputWorkload : public TestWorkload {
	uint64_t totalCost{ 0 };
	double throttledDuration{ 0.0 };
	int tagThrottledErrors{ 0 };
	double testDuration;
	uint64_t expectedThroughputPagesRate;
	TransactionTag throttlingTag;
	double errorTolerance;
	Key testKey;
	int numActors;

	ACTOR static Future<Void> runTransaction(ExpectStableThroughputWorkload* self, Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::AUTO_THROTTLE_TAG, self->throttlingTag);
				wait(success(tr.get(self->testKey)));
				self->totalCost += tr.getTotalCost();
				self->throttledDuration += tr.getTagThrottledDuration();
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_proxy_tag_throttled) {
					++self->tagThrottledErrors;
				}
				wait(tr.onError(e));
			}
		}
	}

  ACTOR static Future<Void> runClient(ExpectStableThroughputWorkload* self, Database cx) {
    loop {
      wait(runTransaction(self, cx));
    }
  }

public:
	static constexpr auto NAME = "ExpectStableThroughput";
	explicit ExpectStableThroughputWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 1200.0);
		throttlingTag = getOption(options, "throttlingTag"_sr, "testTag"_sr);
		expectedThroughputPagesRate = getOption(options, "expectedThroughputPagesRate"_sr, 1);
		errorTolerance = getOption(options, "errorTolerance"_sr, 0.2);
		testKey = getOption(options, "testKey"_sr, "testKey"_sr);
		numActors = getOption(options, "numActors"_sr, 100);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
	  return Void();
		}
		std::vector<Future<Void>> clients;
		for (int i = 0; i < numActors; ++i) {
	  clients.push_back(runClient(this, cx));
		}
		return success(timeout(waitForAll(clients), testDuration));
	}

	Future<bool> check(Database const& cx) override {
		if (clientId) {
	  return true;
		}
		auto const expectedTotalCost =
		    testDuration * expectedThroughputPagesRate * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
		TraceEvent("CheckingStableThroughput")
		    .detail("ExpectedTotalCost", expectedTotalCost)
		    .detail("ErrorTolerance", errorTolerance)
		    .detail("TotalCost", totalCost)
		    .detail("ThrottledDuration", throttledDuration)
		    .detail("TagThrottledErrors", tagThrottledErrors);
		bool const passed = (static_cast<double>(expectedTotalCost) * (1.0 - errorTolerance) <= totalCost) &&
		                    (totalCost <= static_cast<double>(expectedTotalCost) * (1.0 + errorTolerance));
		ASSERT(passed);
		return passed;
	}
	void getMetrics(std::vector<PerfMetric>&) override {}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.emplace("Attrition"); }
};

WorkloadFactory<ExpectStableThroughputWorkload> ExpectStableThroughputWorkloadFactory;
