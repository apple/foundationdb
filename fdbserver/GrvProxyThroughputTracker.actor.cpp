/**
 * GrvProxyThroughputTracker.actor.cpp
 */

#include "fdbserver/GrvProxyThroughputTracker.h"
#include "flow/actorcompiler.h" // must be last include

class GrvProxyThroughputTrackerImpl {
public:
	ACTOR static Future<Void> run(GrvProxyThroughputTracker* self, FutureStream<ReportThroughputRequest> stream) {
		loop {
			ReportThroughputRequest req = waitNext(stream);
			for (auto const& [throttlingId, clientThroughput] : req.throughput) {
				self->throughput[throttlingId] += clientThroughput;
			}
		}
	}
}; // class GrvProxyThroughputTrackerImpl

GrvProxyThroughputTracker::GrvProxyThroughputTracker(FutureStream<ReportThroughputRequest> stream) {
	actor = GrvProxyThroughputTrackerImpl::run(this, stream);
}

ThrottlingIdMap<int64_t> GrvProxyThroughputTracker::getAndClearThroughput() {
  auto const result = std::move(throughput);
  throughput.clear();
  return result;
}
