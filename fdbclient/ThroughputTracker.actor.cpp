/**
 * ThroughputTracker.actor.cpp
 */

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ThroughputTracker.h"
#include "fdbrpc/LoadBalance.actor.h"
#include "flow/actorcompiler.h" // must be last include

class ThroughputTrackerImpl {
public:
	ACTOR static Future<Void> run(ThroughputTracker* self, DatabaseContext* cx) {
		state Future<Void> timer = Never();
		loop {
			try {
				ReportThroughputRequest req(std::move(self->throughput));
				self->throughput.clear();
				timer = delayJittered(CLIENT_KNOBS->CLIENT_THROUGHPUT_REPORT_INTERVAL);
				wait(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies::False),
				                      &GrvProxyInterface::reportThroughput,
				                      std::move(req),
				                      TaskPriority::DefaultPromiseEndpoint,
				                      AtMostOnce::True) ||
				     timer);
				if (timer.isReady()) {
					CODE_PROBE(true, "ReportThroughputRequest timed out");
					TraceEvent(SevWarn, "ReportThroughputRequestTimedOut");
				}
			} catch (Error& e) {
				if (e.code() == error_code_request_maybe_delivered) {
					CODE_PROBE(true, "ReportThroughputRequest maybe delivered");
					TraceEvent(SevWarn, "ReportThroughputRequestMaybeDelivered");
				} else {
					TraceEvent(SevWarnAlways, "ThroughputTrackerFailedWithError").error(e);
					throw e;
				}
			}
		}
	}
}; // class ThroughputTrackersImpl

Future<Void> ThroughputTracker::run(DatabaseContext& cx) {
	return ThroughputTrackerImpl::run(this, &cx);
}

void ThroughputTracker::addCost(ThrottlingId const& throttlingId, uint64_t cost) {
	throughput[throttlingId] += cost;
}
