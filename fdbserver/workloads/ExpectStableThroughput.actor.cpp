/**
 * ExpectStableThroughput.actor.cpp
 */

#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last include

// This workload is meant to be run with the ThroughputQuotaWorklaod.
// The ThroughputQuotaWorkload sets a total quota, and then this workload runs
// with tagged transactions for a long duration, attempting to achieve a higher
// throughput than the specified quota. The check phase of this workload then
// verifies that the achieved throughput is near the total quota.
//
// TODO:
//   - Test write workloads
//   - Randomize the number of operations per transaction
//   - Test multi-page operations
class ExpectStableThroughputWorkload : public TestWorkload {
	// Metrics:
	uint64_t totalCost{ 0 };
	double throttledDuration{ 0.0 };
	int tagThrottledErrors{ 0 };

	// Parameters:
	double testDuration;
	uint64_t expectedThroughputPagesRate;
	TransactionTag throttlingTag;
	double errorTolerance;
	Key keyPrefix;
	int numActors;
	double attemptedTransactionRatePerActor;
	double warmupTime;
	int opsPerTransaction;

	double startTime;

	void finishTransaction(Transaction const& tr) {
		if (now() > startTime + warmupTime) {
			totalCost += tr.getTotalCost();
			throttledDuration += tr.getTagThrottledDuration();
		}
	}

	Key getKey(int index) { return Key(format("%06d", index)).withPrefix(keyPrefix); }

	ACTOR static Future<Void> runTransaction(ExpectStableThroughputWorkload* self, Database cx) {
		state Transaction tr(cx);
		state std::vector<Future<Void>> futures;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::AUTO_THROTTLE_TAG, self->throttlingTag);
				futures.clear();
				futures.reserve(self->opsPerTransaction);
				for (int i = 0; i < self->opsPerTransaction; ++i) {
					futures.push_back(success(tr.get(self->getKey(i))));
				}
				wait(waitForAll(futures));
				self->finishTransaction(tr);
				return Void();
			} catch (Error& e) {
				self->finishTransaction(tr);
				if (e.code() == error_code_proxy_tag_throttled && now() > self->startTime + self->warmupTime) {
					++self->tagThrottledErrors;
				}
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> runClient(ExpectStableThroughputWorkload* self, Database cx) {
		loop {
			wait(delay(1 / self->attemptedTransactionRatePerActor) && runTransaction(self, cx));
		}
	}

public:
	static constexpr auto NAME = "ExpectStableThroughput";
	explicit ExpectStableThroughputWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 1200.0);
		throttlingTag = getOption(options, "throttlingTag"_sr, "testTag"_sr);
		expectedThroughputPagesRate = getOption(options, "expectedThroughputPagesRate"_sr, 1);
		errorTolerance = getOption(options, "errorTolerance"_sr, 0.2);
		keyPrefix = getOption(options, "keyPrefix"_sr, "testKey"_sr);
		numActors = getOption(options, "numActors"_sr, 100);
		attemptedTransactionRatePerActor = getOption(options, "attemptedTransactionRatePerActor"_sr, 0.4);
		warmupTime = getOption(options, "warmupTime"_sr, 10.0);
		opsPerTransaction = getOption(options, "opsPerTransaction"_sr, 5);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0 || !SERVER_KNOBS->GLOBAL_TAG_THROTTLING) {
			return Void();
		}
		startTime = now();
		std::vector<Future<Void>> clients;
		for (int i = 0; i < numActors; ++i) {
			clients.push_back(runClient(this, cx));
		}
		return success(timeout(waitForAll(clients), warmupTime + testDuration));
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0 || !SERVER_KNOBS->GLOBAL_TAG_THROTTLING) {
			return true;
		}
		auto const expectedTotalCost =
		    testDuration * expectedThroughputPagesRate * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
		bool const passed = (static_cast<double>(expectedTotalCost) * (1.0 - errorTolerance) <= totalCost) &&
		                    (totalCost <= static_cast<double>(expectedTotalCost) * (1.0 + errorTolerance));
		auto const severity = passed ? SevInfo : SevError;
		TraceEvent(severity, "CheckingStableThroughput")
		    .detail("ExpectedTotalCost", expectedTotalCost)
		    .detail("ErrorTolerance", errorTolerance)
		    .detail("TotalCost", totalCost);
		return passed;
	}
	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("Total Cost", totalCost, Averaged::False);
		m.emplace_back("TagThrottled Errors", tagThrottledErrors, Averaged::False);
		m.emplace_back("Total Throttling Duration", throttledDuration, Averaged::False);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert("Attrition");
		out.insert("RandomClogging");
	}
};

WorkloadFactory<ExpectStableThroughputWorkload> ExpectStableThroughputWorkloadFactory;
