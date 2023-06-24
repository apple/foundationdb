/**
 * GrvProxyThroughputTracker.h
 */

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/ThrottlingId.h"

// The GrvProxyThroughputTracker class is responsible for aggregating
// per-throttlingId throughput statistics from all clients. These
// statistics are periodically sent to the ratekeeper.
class GrvProxyThroughputTracker {
  friend class GrvProxyThroughputTrackerImpl;

  ThrottlingIdMap<int64_t> throughput;
  Future<Void> actor;

public:
  GrvProxyThroughputTracker(FutureStream<ReportThroughputRequest>);
  ThrottlingIdMap<int64_t> getAndClearThroughput();
};
