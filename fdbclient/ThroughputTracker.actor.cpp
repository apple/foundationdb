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
		loop {
			ReportThroughputRequest req(std::move(self->throughput));
			self->throughput.clear();
			wait(basicLoadBalance(
			    cx->getGrvProxies(UseProvisionalProxies::False), &GrvProxyInterface::reportThroughput, std::move(req)));
			wait(delay(1.0));
		}
	}
}; // class ThroughputTrackersImpl

Future<Void> ThroughputTracker::run(DatabaseContext& cx) {
	return ThroughputTrackerImpl::run(this, &cx);
}

void ThroughputTracker::addCost(ThrottlingId const& throttlingId, uint64_t cost) {
	throughput[throttlingId] += cost;
}
