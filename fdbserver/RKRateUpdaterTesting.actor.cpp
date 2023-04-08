/**
 * RKRateUpdaterTesting.actor.cpp
 */

#include "fdbrpc/Locality.h"
#include "fdbserver/IRKMetricsTracker.h"
#include "fdbserver/IRKRateServer.h"
#include "fdbserver/IRKRateUpdater.h"
#include "fdbserver/TagThrottler.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

void checkApproximatelyEqual(double a, double b) {
	ASSERT(a < b + 0.01 || a < b * 1.05);
	ASSERT(b < a + 0.01 || b < a * 1.05);
}

ACTOR Future<StorageQueueInfo> getMockStorageQueueInfo(UID id,
                                                       LocalityData locality,
                                                       int64_t storageQueueBytes,
                                                       double inputBytesPerSecond) {
	state int iterations = 10000;
	state StorageQueueInfo ss(id, locality);
	state StorageQueuingMetricsReply reply;
	state Smoother smoothTotalDurableBytes(10.0); // unused

	ss.acceptingRequests = true;
	reply.instanceID = 0;
	reply.bytesInput = storageQueueBytes;
	reply.bytesDurable = 0;
	reply.storageBytes.total = 100e9;
	reply.storageBytes.available = 100e9;
	reply.storageBytes.free = 100e9;
	reply.version = std::max(5.0, storageQueueBytes / inputBytesPerSecond);
	reply.durableVersion = 0;
	ss.update(reply, smoothTotalDurableBytes);

	while (iterations--) {
		wait(delay(0.01));
		reply.bytesInput += (inputBytesPerSecond / 100);
		reply.bytesDurable += (inputBytesPerSecond / 100);
		reply.version += 10000;
		reply.durableVersion += 10000;
		ss.update(reply, smoothTotalDurableBytes);
	}

	checkApproximatelyEqual(ss.getSmoothInputBytesRate(), inputBytesPerSecond);
	checkApproximatelyEqual(ss.getVerySmoothDurableBytesRate(), inputBytesPerSecond);
	checkApproximatelyEqual(ss.getSmoothFreeSpace(), 100e9);
	checkApproximatelyEqual(ss.getSmoothTotalSpace(), 100e9);
	checkApproximatelyEqual(ss.getStorageQueueBytes(), storageQueueBytes);

	return ss;
}

} // namespace

TEST_CASE("/fdbserver/RKRateUpdater/Simple") {
	MockRKMetricsTracker metricsTracker;
	MockRKRateServer rateServer(1000.0);
	MockTagThrottler tagThrottler;
	MockRKConfigurationMonitor configurationMonitor(1);
	MockRKRecoveryTracker recoveryTracker;
	Deque<double> actualTpsHistory;
	Deque<std::pair<double, Version>> blobWorkerVersionHistory;
	double blobWorkerTime{ 0.0 };
	double unblockedAssignmentTime{ 0.0 };

	RatekeeperLimits limits(TransactionPriority::DEFAULT, "", 1000e6, 100e6, 1000e6, 100e6, 1e6, 5e6, 300.0);
	RKRateUpdater rateUpdater(UID(), limits);

	rateUpdater.update(metricsTracker,
	                   rateServer,
	                   tagThrottler,
	                   configurationMonitor,
	                   recoveryTracker,
	                   actualTpsHistory,
	                   false,
	                   blobWorkerVersionHistory,
	                   blobWorkerTime,
	                   unblockedAssignmentTime);

	checkApproximatelyEqual(rateUpdater.getTpsLimit(), SERVER_KNOBS->RATEKEEPER_DEFAULT_LIMIT);
	return Void();
}

// Currently, a workload of 1000 transactions per second is using up half of the storage queue
// spring bytes (950MB SQ, with a 1GB target and 100MB of spring). The rate updater estimates
// that the cluster can handle double the current transaction rate, or 2000 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/HighSQ") {
	StorageQueueInfo ss = wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 950e6, 1e6));

	MockRKMetricsTracker metricsTracker;
	MockRKRateServer rateServer(1000.0);
	MockTagThrottler tagThrottler;
	MockRKConfigurationMonitor configurationMonitor(1);
	MockRKRecoveryTracker recoveryTracker;
	Deque<double> actualTpsHistory;
	Deque<std::pair<double, Version>> blobWorkerVersionHistory;
	double blobWorkerTime{ 0.0 };
	double unblockedAssignmentTime{ 0.0 };

	metricsTracker.updateStorageQueueInfo(ss);

	RatekeeperLimits limits(TransactionPriority::DEFAULT, "", 1000e6, 100e6, 1000e6, 100e6, 1e6, 5e6, 300.0);
	RKRateUpdater rateUpdater(UID(), limits);

	rateUpdater.update(metricsTracker,
	                   rateServer,
	                   tagThrottler,
	                   configurationMonitor,
	                   recoveryTracker,
	                   actualTpsHistory,
	                   false,
	                   blobWorkerVersionHistory,
	                   blobWorkerTime,
	                   unblockedAssignmentTime);

	checkApproximatelyEqual(rateUpdater.getTpsLimit(), 2000.0);
	return Void();
}
