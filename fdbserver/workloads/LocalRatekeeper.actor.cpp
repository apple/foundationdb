#include "workloads.actor.h"
#include <fdbserver/Knobs.h>
#include <flow/actorcompiler.h>

namespace {

ACTOR Future<StorageServerInterface> getRandomStrage(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			Standalone<RangeResultRef> range = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
			if (range.size() > 0) {
				auto idx = g_random->randomInt(0, range.size());
				return decodeServerListValue(range[idx].value);
			} else {
				wait(delay(1.0));
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

struct LocalRatekeeperWorkload : TestWorkload {

	double startAfter = 0.0;
	double blockWritesFor;
	bool testFailed = false;

	LocalRatekeeperWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		startAfter = getOption(options, LiteralStringRef("startAfter"), startAfter);
		blockWritesFor = getOption(options, LiteralStringRef("blockWritesFor"),
								   double(SERVER_KNOBS->STORAGE_DURABILITY_LAG_HARD_MAX)/double(1e6));
	}
	virtual std::string description() { return "LocalRatekeeperWorkload"; }

	ACTOR static Future<Void> testStorage(LocalRatekeeperWorkload* self, Database cx, StorageServerInterface ssi) {
		state Transaction tr(cx);
		state std::vector<Future<GetValueReply>> requests;
		requests.reserve(100);
		loop {
			state StorageQueuingMetricsReply metrics = wait(ssi.getQueuingMetrics.getReply(StorageQueuingMetricsRequest{}));
			auto durabilityLag = metrics.version - metrics.durableVersion;
			double expectedRateLimit = 1.0;
			if (durabilityLag >= SERVER_KNOBS->STORAGE_DURABILITY_LAG_HARD_MAX) {
				expectedRateLimit = 0.0;
			} else if (durabilityLag > SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX) {
				expectedRateLimit = double(durabilityLag) / double(SERVER_KNOBS->STORAGE_DURABILITY_LAG_HARD_MAX);
			}
			if (expectedRateLimit < metrics.localRateLimit - 0.01 || expectedRateLimit > metrics.localRateLimit + 0.01) {
				self->testFailed = true;
				TraceEvent(SevError, "StorageRateLimitTooFarOff")
					.detail("Storage", ssi.id())
					.detail("Expected", expectedRateLimit)
					.detail("Actual", metrics.localRateLimit);
			}
			tr.reset();
			Version readVersion = wait(tr.getReadVersion());
			requests.clear();
			// we send 100 requests to this storage node and count how many of those get rejected
			for (int i = 0; i < 100; ++i) {
				GetValueRequest req;
				req.version = readVersion;
				// we don't care about the value
				req.key = LiteralStringRef("/lkfs");
				requests.emplace_back(ssi.getValue.getReply(req));
			}
			wait(waitForAllReady(requests));
			int failedRequests = 0;
			int errors = 0;
			for (const auto& resp : requests) {
				if (resp.isError()) {
					self->testFailed = true;
					++errors;
					TraceEvent(SevError, "LoadBalancedResponseReturnedError")
						.error(resp.getError());
				} else if (resp.get().error.present() && resp.get().error.get().code() == error_code_future_version) {
					++failedRequests;
				}
				if (errors > 9) {
					break;
				}
			}
			TraceEvent("RejectedVersions")
				.detail("NumRejected", failedRequests);
			if (self->testFailed) {
				return Void();
			}
			wait(delay(5.0));
		}
	}

	ACTOR static Future<Void> _start(LocalRatekeeperWorkload* self, Database cx) {
		wait(delay(self->startAfter));
		state StorageServerInterface ssi = wait(getRandomStrage(cx));
		g_simulator.disableFor(format("%s/updateStorage", ssi.id().toString().c_str()), self->blockWritesFor);
		state Future<Void> done = delay(self->blockWritesFor);
		// not much will happen until the storage goes over the soft limit
		wait(delay(double(SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX/1e6)));
		wait(testStorage(self, cx, ssi) || done);
		return Void();
	}

	virtual Future<Void> start(Database const& cx) {
		// we run this only on one client
		if (clientId != 0 || !g_network->isSimulated()) {
			return Void();
		}
		return _start(this, cx);
	}
	virtual Future<bool> check(Database const& cx) { return !testFailed; }
	virtual void getMetrics(vector<PerfMetric>& m) {}
};

} // namespace

WorkloadFactory<LocalRatekeeperWorkload> CycleWorkloadFactory("LocalRatekeeper");
