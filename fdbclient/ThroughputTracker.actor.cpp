/**
 * ThroughputTracker.actor.cpp
 */

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ThroughputTracker.h"
#include "fdbrpc/LoadBalance.actor.h"
#include "flow/actorcompiler.h" // must be last include

class ThroughputTrackerImpl {
public:
	ACTOR static Future<Void> reporterActor(ThroughputTracker* self, Database cx) {
		loop {
			ReportThroughputRequest req(std::move(self->throughput));
			self->throughput.clear();
			wait(basicLoadBalance(
			    cx->getGrvProxies(UseProvisionalProxies::False), &GrvProxyInterface::reportThroughput, std::move(req)));
			wait(delay(1.0));
		}
	}
}; // class ThroughputTrackersImpl

ThroughputTracker::ThroughputTracker(Database cx) {
	reporter = ThroughputTrackerImpl::reporterActor(this, cx);
}

void ThroughputTracker::addCost(ThrottlingId const& throttlingId, int64_t cost) {
	throughput[throttlingId] += cost;
}
