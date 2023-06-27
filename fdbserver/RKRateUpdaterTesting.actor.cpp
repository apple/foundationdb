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

// WARNING: If any of these test* constants are changed, make sure to update
// comments that refer to these values in the test cases below.

double const testActualTps = 1000.0;
// Corresponds to TARGET_BYTES_PER_STORAGE_SERVER, TARGET_BYTES_PER_TLOG,
// TARGET_BYTES_PER_STORAGE_SERVER_BATCH, and TARGET_BYTES_PER_TLOG_BATCH:
constexpr int64_t testTargetQueueBytes = 1000e6;
// Corresponds to SPRING_BYTES_STORAGE_SERVER, SPRING_BYTES_TLOG,
// SPRING_BYTES_STORAGE_SERVER_BATCH, and SPRING_BYTES_TLOG_BATCH:
constexpr int64_t testSpringBytes = 100e6;
// Corresponds to TARGET_DURABILITY_LAG_VERSIONS, MAX_TL_SS_VERSION_DIFFERENCE,
// TARGET_DURABILTY_LAG_VERSIONS_BATCH, and MAX_TL_SS_VERSION_DIFFERENCE_BATCH:
constexpr int64_t testTargetVersionDifference = 2e9;
constexpr int64_t testTotalSpace = 100e9;
constexpr int64_t testGenerateMockInfoIterations = 3e3;
double const testInputBytesPerSecond = 1e6;

bool checkApproximatelyEqual(double a, double b, double errorBound = 0.05) {
	if ((a > b + 0.01 && a > b * (1 + errorBound)) || (b > a + 0.01 && b > a * (1 + errorBound))) {
		TraceEvent(SevError, "CheckApproximatelyEqualFailure")
		    .detail("A", a)
		    .detail("B", b)
		    .detail("ErrorBound", errorBound);
		return false;
	} else {
		return true;
	}
}

ACTOR Future<TLogQueueInfo> getMockTLogQueueInfo(UID id,
                                                 int64_t queueBytes,
                                                 int64_t availableSpace = testTotalSpace,
                                                 int64_t totalSpace = testTotalSpace,
                                                 Version startVersion = 0) {
	state int iterations = testGenerateMockInfoIterations;
	state TLogQueueInfo result(id);
	state TLogQueuingMetricsReply reply;
	state Smoother smoothTotalDurableBytes(10.0); // unused

	reply.bytesInput = queueBytes;
	reply.instanceID = 0;
	reply.bytesDurable = 0;
	reply.storageBytes.total = totalSpace;
	reply.storageBytes.available = availableSpace;
	reply.storageBytes.free = availableSpace;
	reply.storageBytes.used = totalSpace - availableSpace;
	reply.v = startVersion;
	result.update(reply, smoothTotalDurableBytes);

	// Each iteration simulates 10ms of work on a tlog server.
	// We iterate many times in order for smoothers to catch up
	// to their desired values.
	while (iterations--) {
		// Use orderedDelay to prevent buggification
		wait(orderedDelay(0.1));

		reply.bytesInput += testInputBytesPerSecond / 10;
		reply.bytesDurable += testInputBytesPerSecond / 10;
		reply.v += 100000;
		result.update(reply, smoothTotalDurableBytes);
	}

	// Validate that result statistics approximately match desired values:
	ASSERT(checkApproximatelyEqual(result.getSmoothFreeSpace(), availableSpace, /*errorBound=*/0.01));
	ASSERT(checkApproximatelyEqual(result.getSmoothInputBytesRate(), testInputBytesPerSecond, /*errorBound=*/0.01));
	ASSERT(
	    checkApproximatelyEqual(result.getVerySmoothDurableBytesRate(), testInputBytesPerSecond, /*errorBound=*/0.01));
	ASSERT(checkApproximatelyEqual(result.getSmoothTotalSpace(), totalSpace, /*errorBound=*/0.01));

	return result;
}

ACTOR Future<StorageQueueInfo> getMockStorageQueueInfo(UID id,
                                                       LocalityData locality,
                                                       int64_t storageQueueBytes,
                                                       int64_t targetNonDurableVersionsLag = 5e6,
                                                       int64_t availableSpace = testTotalSpace,
                                                       int64_t totalSpace = testTotalSpace) {
	state int iterations = testGenerateMockInfoIterations;
	state StorageQueueInfo ss(id, locality);
	state StorageQueuingMetricsReply reply;
	state Smoother smoothTotalDurableBytes(10.0); // unused

	ss.acceptingRequests = true;
	reply.instanceID = 0;
	reply.bytesInput = storageQueueBytes;
	reply.bytesDurable = 0;
	reply.storageBytes.total = totalSpace;
	reply.storageBytes.available = availableSpace;
	reply.storageBytes.free = availableSpace;
	reply.storageBytes.used = totalSpace - availableSpace;
	reply.version = std::max<Version>(
	    targetNonDurableVersionsLag, SERVER_KNOBS->VERSIONS_PER_SECOND * (storageQueueBytes / testInputBytesPerSecond));
	reply.durableVersion = 0;
	ss.update(reply, smoothTotalDurableBytes);

	// Each iteration simulates 10ms of work on a storage server.
	// We iterate many times in order for smoothers to catch up
	// to their desired values.
	while (iterations--) {
		// Use orderedDelay to prevent buggification
		wait(orderedDelay(0.1));

		reply.bytesInput += (testInputBytesPerSecond / 10);
		reply.bytesDurable += (testInputBytesPerSecond / 10);
		reply.version += 100000;
		reply.durableVersion += 100000;
		ss.update(reply, smoothTotalDurableBytes);
	}

	// Validate that ss statistics approximately match desired values:
	ASSERT(checkApproximatelyEqual(ss.getSmoothInputBytesRate(), testInputBytesPerSecond, /*errorBound=*/0.01));
	ASSERT(checkApproximatelyEqual(ss.getVerySmoothDurableBytesRate(), testInputBytesPerSecond, /*errorBound=*/0.01));
	ASSERT(checkApproximatelyEqual(ss.getSmoothFreeSpace(), availableSpace, /*errorBound=*/0.01));
	ASSERT(checkApproximatelyEqual(ss.getSmoothTotalSpace(), totalSpace, /*errorBound=*/0.01));
	ASSERT(checkApproximatelyEqual(ss.getStorageQueueBytes(), storageQueueBytes, /*errorBound=*/0.01));
	ASSERT(checkApproximatelyEqual(
	    ss.getDurabilityLag(),
	    std::max<int64_t>(targetNonDurableVersionsLag,
	                      SERVER_KNOBS->VERSIONS_PER_SECOND * (storageQueueBytes / testInputBytesPerSecond)),
	    /*errorBound=*/0.01));

	return ss;
}

struct RKRateUpdaterTestEnvironment {
	MockRKMetricsTracker metricsTracker;
	MockRKRateServer rateServer;
	StubTagThrottler tagThrottler;
	MockRKConfigurationMonitor configurationMonitor;
	MockRKRecoveryTracker recoveryTracker;
	Deque<double> actualTpsHistory;
	MockRKBlobMonitor blobMonitor;

	RKRateUpdater rateUpdater;

	RKRateUpdaterTestEnvironment(int storageTeamSize = 1)
	  : rateServer(testActualTps), configurationMonitor(storageTeamSize),
	    rateUpdater(UID{},
	                RatekeeperLimits(TransactionPriority::DEFAULT,
	                                 "",
	                                 testTargetQueueBytes,
	                                 testSpringBytes,
	                                 testTargetQueueBytes,
	                                 testSpringBytes,
	                                 testTargetVersionDifference,
	                                 testTargetVersionDifference,
	                                 SERVER_KNOBS->TARGET_BW_LAG)) {
		for (int i = 0; i < SERVER_KNOBS->NEEDED_TPS_HISTORY_SAMPLES; ++i) {
			actualTpsHistory.push_back(testActualTps);
		}
	}

	void update() {
		rateUpdater.update(metricsTracker,
		                   rateServer,
		                   tagThrottler,
		                   configurationMonitor,
		                   recoveryTracker,
		                   actualTpsHistory,
		                   blobMonitor);
	}
};

struct RateAndReason {
	double tpsLimit;
	limitReason_t limitReason;
	RateAndReason(double tpsLimit, limitReason_t limitReason) : tpsLimit(tpsLimit), limitReason(limitReason) {}
};

ACTOR static Future<RateAndReason> testIgnoreWorstZones(int numBadStorageServers) {
	state std::vector<Future<StorageQueueInfo>> ssFutures;
	int smallStorageQueueBytes = testTargetQueueBytes - 5 * testSpringBytes;
	int largeStorageQueueBytes = testTargetQueueBytes + 5 * testSpringBytes;

	ssFutures.reserve(10);
	for (int i = 0; i < 10; ++i) {
		LocalityData locality({}, format("zone%d", i), {}, {});
		int64_t storageQueueBytes = (i < numBadStorageServers) ? largeStorageQueueBytes : smallStorageQueueBytes;
		ssFutures.push_back(getMockStorageQueueInfo(UID(i, i), locality, storageQueueBytes));
	}
	wait(waitForAll(ssFutures));
	RKRateUpdaterTestEnvironment env(/*storageTeamSize=*/3);
	for (auto const& ssFuture : ssFutures) {
		env.metricsTracker.updateStorageQueueInfo(ssFuture.get());
	}
	env.update();
	return RateAndReason(env.rateUpdater.getTpsLimit(), env.rateUpdater.getLimitReason());
}

} // namespace

// No processes are reporting any metrics to the rate updater. The default ratekeeper limit
// is applied.
TEST_CASE("/fdbserver/RKRateUpdater/Simple") {
	RKRateUpdaterTestEnvironment env;
	env.update();
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), SERVER_KNOBS->RATEKEEPER_DEFAULT_LIMIT));
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::unlimited);
	return Void();
}

// Currently, a workload of 1000 transactions per second is using up half of the storage queue
// spring bytes (950MB SQ, with a 1GB target and 100MB of spring). The rate updater estimates
// that the cluster can handle double the current transaction rate, or 2000 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/HighSQ") {
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, testTargetQueueBytes - testSpringBytes / 2));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_write_queue_size);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 2 * testActualTps));
	return Void();
}

// Currently, a workload of 1000 transactions per second is exceeding the target storage queue
// size by half of the spring bytes limit (1050MB SQ, with a 1GB target and 100MB of spring).
// The rate updater estimates that the cluster can handle 2/3 of the current transaction rate,
// or ~667 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/HighSQ2") {
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, testTargetQueueBytes + testSpringBytes / 2));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_write_queue_size);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), testActualTps * 2 / 3));
	return Void();
}

// Currently, a workload of 1000 transactions per second is exceeding the sum of the target
// storage queue bytes and spring bytes. The rate updater applies the maximum possible throttling
// based on storage queue, limiting throughput to half the current transaction rate, or 500
// transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/HighSQ3") {
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, testTargetQueueBytes + 5 * testSpringBytes));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_write_queue_size);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), testActualTps / 2));
	return Void();
}

// For the one storage process emitting metrics, storage queue is below the target bytes minus
// the spring bytes. Therefore, throttling is enforced to ensure that at the current write rate
// per transaction, an MVCC window worth of writes does not cause storage queue to rise above
// the target bytes minus spring bytes.
TEST_CASE("/fdbserver/RKRateUpdater/StorageWriteBandwidthMVCC") {
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, testTargetQueueBytes - 5 * testSpringBytes));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_write_bandwidth_mvcc);
	ASSERT_GT(env.rateUpdater.getTpsLimit(), testActualTps);
	return Void();
}

// The current 1000 transaction per second workload is saturating the storage queue of
// MAX_MACHINES_FALLING_BEHIND servers, but not saturating the storage queue of the other storage
// servers in different zones. The rate updater does not throttle based on the worst
// storage servers' queues.
TEST_CASE("/fdbserver/RKRateUpdater/IgnoreWorstZones") {
	RateAndReason rateAndReason = wait(testIgnoreWorstZones(SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND));

	// Even though some storage servers won't allow more than the current transaction rate, the
	// rate updater will still allow more than the current transaction rate, because these storage
	// servers' zones are ignored.
	ASSERT_GT(rateAndReason.tpsLimit, testActualTps);

	// Even though the storage servers with high storage queue are ignored, we still report write
	// queue size as the limiting reason.
	// TODO: Should this behaviour be changed?
	ASSERT_EQ(rateAndReason.limitReason, limitReason_t::storage_server_write_queue_size);
	return Void();
}

// The current 1000 transaction per second workload is saturating the storage queue of
// (MAX_MACHINES_FALLING_BEHIND + 1) servers, but not saturating the storage queue of the other
// storage servers in different zones. The rate updater throttles based on the worst storage
// servers' queues.
TEST_CASE("/fdbserver/RKRateUpdater/DontIgnoreWorstZones") {
	RateAndReason rateAndReason = wait(testIgnoreWorstZones(SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND + 1));

	// The storage queues with high storage queue cannot be ignored, so the
	// rate updater should throttle to less than the current transaction rate.
	ASSERT_LT(rateAndReason.tpsLimit, testActualTps);
	ASSERT_EQ(rateAndReason.limitReason, limitReason_t::storage_server_write_queue_size);
	return Void();
}

// The durability lag on the single storage server exceeds the configured durability lag limit.
// Therefore, the rate updated throttles based on storage server durability lag.
TEST_CASE("/fdbserver/RKRateUpdater/HighNDV") {
	StorageQueueInfo ss = wait(getMockStorageQueueInfo(
	    UID(1, 1), LocalityData{}, testTargetQueueBytes - 5 * testSpringBytes, 2 * testTargetVersionDifference));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_durability_lag);
	return Void();
}

// The rate updater was unable to fetch the list of storage servers. Therefore, the tps limit
// is set to 0.
TEST_CASE("/fdbserver/RKRateUpdater/ServerListFetchFailed") {
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.failSSListFetch();
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_list_fetch_failed);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 0.0));
	return Void();
}

// Though the storage queue is only 300MB (less than the threshold for throttling on storage queue alone),
// the storage server only has 300MB of space to spare before hitting the MIN_AVAILABLE_SPACE threshold.
// As a result, the rate updater throttles at the current transaction rate of 1000 TPS.
TEST_CASE("/fdbserver/RKRateUpdater/SSFreeSpace") {
	int64_t totalSpace = 1e9;
	int64_t storageQueueBytes = 3 * testSpringBytes;
	int64_t availableSpace = SERVER_KNOBS->MIN_AVAILABLE_SPACE + storageQueueBytes;
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, storageQueueBytes, 5e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_min_free_space);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), testActualTps));
	return Void();
}

// Though the storage queue is only 600MB (less than the threshold for throttling on storage queue alone),
// the storage server only has 300MB of space to spare before hitting the MIN_AVAILABLE_SPACE threshold.
// As a result, the rate updater throttles at half the current transaction rate, or 500 TPS.
TEST_CASE("/fdbserver/RKRateUpdater/SSFreeSpace2") {
	int64_t totalSpace = 1e9;
	int64_t availableSpace = SERVER_KNOBS->MIN_AVAILABLE_SPACE + 3 * testSpringBytes;
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 6 * testSpringBytes, 5e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_min_free_space);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), testActualTps / 2));
	return Void();
}

// Though the storage queue is only 300MB (less than the threshold for throttling on storage queue alone),
// the storage server only has 300MB of space to spare before hitting the MIN_AVAILABLE_SPACE_RATIO threshold.
// As a result, the rate updater throttles at the current transaction rate of 1000 TPS.
TEST_CASE("/fdbserver/RKRateUpdater/SSFreeSpaceRatio") {
	int64_t totalSpace = 1e15;
	int64_t storageQueueBytes = 3 * testSpringBytes;
	int64_t availableSpace = (totalSpace * SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO) + storageQueueBytes;
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, storageQueueBytes, 5e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_min_free_space_ratio);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), testActualTps));
	return Void();
}

// Though the storage queue is only 600MB (less than the threshold for throttling on storage queue alone),
// the storage server only has 300MB of space to spare before hitting the MIN_AVAILABLE_SPACE_RATIO threshold.
// As a result, the rate updater throttles at half the current transaction rate, or 500 TPS.
TEST_CASE("/fdbserver/RKRateUpdater/SSFreeSpaceRatio2") {
	int64_t totalSpace = 1e15;
	int64_t availableSpace = (totalSpace * SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO) + 3 * testSpringBytes;
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 6 * testSpringBytes, 5e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_min_free_space_ratio);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), testActualTps / 2));
	return Void();
}

// Currently, a workload of 1000 transactions per second is using up half of the tlog queue
// spring bytes (950MB queue, with a 1GB target and 100MB of spring). The rate updater estimates
// that the cluster can handle double the current transaction rate, or 2000 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogQueue") {
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), testTargetQueueBytes - testSpringBytes / 2));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_write_queue);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 2 * testActualTps));
	return Void();
}

// Currently, a workload of 1000 transactions per second is exceeding the target tlog queue size
// by half of the spring bytes limit (1050MB queue, with a 1GB target and 100MB of spring).
// The rate updater estimates that the cluster can handle 2/3 of the current transaction rate,
// or ~667 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogQueue2") {
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), testTargetQueueBytes + testSpringBytes / 2));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_write_queue);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), testActualTps * 2 / 3));
	return Void();
}

// Currently, a workload of 1000 transactions per second is exceeding the sum of the target
// tlog queue bytes and spring bytes. The rate updater applies the maximum possible throttling based on
// tlog queue, limiting throughput to half the current transaction rate, or 500 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogQueue3") {
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), testTargetQueueBytes + 5 * testSpringBytes));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_write_queue);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), testActualTps / 2));
	return Void();
}

// For the one tlog process emitting metrics, the queue is below the target bytes minus
// the spring bytes. Therefore, throttling is enforced to ensure that at the current write
// rate per transaction, an MVCC window worth of writes does not cause tlog queue to rise
// above the target bytes minues spring bytes.
TEST_CASE("/fdbserver/RKRateUpdater/TLogWriteBandwidthMVCC") {
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), testTargetQueueBytes - 5 * testSpringBytes));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_mvcc_write_bandwidth);
	ASSERT_GT(env.rateUpdater.getTpsLimit(), testActualTps);
	return Void();
}

// The tlog queue plus currently used disk space add to leave only MIN_AVAILABLE_SPACE
// bytes left on the tlog disk. The rate updater reacts by throttling at the current
// transaction rate of 1000 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogFreeSpace") {
	int64_t totalSpace = 1e9;
	int64_t tlogQueueBytes = 3 * testSpringBytes;
	int64_t availableSpace = SERVER_KNOBS->MIN_AVAILABLE_SPACE + tlogQueueBytes;
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), tlogQueueBytes, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_min_free_space);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), testActualTps));
	return Void();
}

// The tlog queue plus currently used disk space add to leave less than MIN_AVAILABLE_SPACE / 2
// bytes on disk. In response, the rate updater throttles throughput to 0.
TEST_CASE("/fdbserver/RKRateUpdater/TLogFreeSpace2") {
	int64_t totalSpace = 1e9;
	int64_t tlogQueueBytes = (SERVER_KNOBS->MIN_AVAILABLE_SPACE * 3) / 2;
	int64_t availableSpace = SERVER_KNOBS->MIN_AVAILABLE_SPACE + tlogQueueBytes / 2;
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), tlogQueueBytes, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_min_free_space);
	// When simulation is sped up, ratekeeper limit is artificially increased
	if (!(g_network->isSimulated() && g_simulator->speedUpSimulation)) {
		ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 0.0));
	}
	return Void();
}

// The tlog queue plus currently used disk space add to leave only the available
// space ratio of total disk space. The rate updater reacts by throttling at the current
// transaction rate of 1000 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogFreeSpaceRatio") {
	int64_t totalSpace = 1e15;
	int64_t tlogQueueBytes = 3 * testSpringBytes;
	int64_t availableSpace = (totalSpace * SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO) + tlogQueueBytes;
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), tlogQueueBytes, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_min_free_space_ratio);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), testActualTps));
	return Void();
}

// The tlog queue plus currently used disk space add to exceed the minimum available disk
// space ratio of total disk space plus spring bytes. In response, the rate updater throttles
// at half the current transaction rate, or 500 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogFreeSpaceRatio2") {
	int64_t totalSpace = 1e15;
	int64_t tlogQueueBytes = 6 * testSpringBytes;
	int64_t availableSpace = (totalSpace * SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO) + 3 * testSpringBytes;
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), tlogQueueBytes, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_min_free_space_ratio);
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), testActualTps / 2));
	return Void();
}

// The tlog is 4e9 versions ahead of the storage server, but the target max version difference is only
// 2e9. Therefore, the rate updater throttles based on the storage server readable version being behind.
TEST_CASE("/fdbserver/RKRateUpdater/StorageReadableBehind") {
	state Future<StorageQueueInfo> ssFuture =
	    getMockStorageQueueInfo(UID(1, 1), LocalityData{}, testTargetQueueBytes - 5 * testSpringBytes);
	state Future<TLogQueueInfo> tlFuture = getMockTLogQueueInfo(UID(1, 1),
	                                                            testTargetQueueBytes - 5 * testSpringBytes,
	                                                            testTotalSpace,
	                                                            testTotalSpace,
	                                                            2 * testTargetVersionDifference);
	wait(success(ssFuture) && success(tlFuture));
	RKRateUpdaterTestEnvironment env;
	env.metricsTracker.updateStorageQueueInfo(ssFuture.get());
	env.metricsTracker.updateTLogQueueInfo(tlFuture.get());
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_readable_behind);
	ASSERT_LT(env.rateUpdater.getTpsLimit(), testActualTps);
	return Void();
}

// Blob worker is updating its versions at half the speed of the GRV proxy.
// After TARGET_BW_LAG*4 seconds, the lag is equal to TARGET_BW_LAG*2.
// At this point, the ratekeeper throttles to a rate BW_LAG_DECREASE_AMOUNT
// times the blob worker rate.
TEST_CASE("/fdbserver/RKRateUpdater/BlobWorkerLag1") {
	state RKRateUpdaterTestEnvironment env;
	state int64_t i = 0;

	env.blobMonitor.addRange();
	env.configurationMonitor.enableBlobGranules();

	for (; i < 4 * SERVER_KNOBS->TARGET_BW_LAG; ++i) {
		wait(delay(1.0));
		env.blobMonitor.setCurrentVersion(SERVER_KNOBS->VERSIONS_PER_SECOND * i / 2);
		env.rateServer.updateProxy(UID(1, 1), SERVER_KNOBS->VERSIONS_PER_SECOND * i, testActualTps);
		env.update();
	}
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::blob_worker_lag);
	// FIXME: Figure out how to lower the error bound on this test
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(),
	                               (testActualTps / 2) * SERVER_KNOBS->BW_LAG_DECREASE_AMOUNT,
	                               /*errorBound=*/0.2));
	return Void();
}

// Blob worker is updating its versions at half the speed of the GRV proxy.
// After TARGET_BW_LAG * 1.5 seconds, the lag is equal to TARGET_BW_LAG * 0.75.
// At this point, the ratekeeper throttles to a rate BW_LAG_INCREASE_AMOUNT
// times the blob worker rate.
TEST_CASE("/fdbserver/RKRateUpdater/BlobWorkerLag2") {
	state RKRateUpdaterTestEnvironment env;
	state int64_t i = 0;

	env.blobMonitor.addRange();
	env.configurationMonitor.enableBlobGranules();

	for (; i < 3 * SERVER_KNOBS->TARGET_BW_LAG / 2; ++i) {
		wait(delay(1.0));
		env.blobMonitor.setCurrentVersion(SERVER_KNOBS->VERSIONS_PER_SECOND * i / 2);
		env.rateServer.updateProxy(UID(1, 1), SERVER_KNOBS->VERSIONS_PER_SECOND * i, testActualTps);
		env.update();
	}
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::blob_worker_lag);
	// FIXME: Figure out how to lower the error bound on this test
	ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(),
	                               (testActualTps / 2) * SERVER_KNOBS->BW_LAG_INCREASE_AMOUNT,
	                               /*errorBound=*/0.2));
	return Void();
}

// Blob worker is updating its versions at half the speed of the GRV proxy.
// After TARGET_BW_LAG * 8 seconds, the lag is equal to TARGET_BW_LAG * 4.
// At this point, the ratekeeper throttles to 0 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/BlobWorkerLag3") {
	state RKRateUpdaterTestEnvironment env;
	state int64_t i = 0;

	env.blobMonitor.addRange();
	env.configurationMonitor.enableBlobGranules();

	for (; i < 8 * SERVER_KNOBS->TARGET_BW_LAG; ++i) {
		wait(delay(1.0));
		env.blobMonitor.setCurrentVersion(SERVER_KNOBS->VERSIONS_PER_SECOND * i / 2);
		env.rateServer.updateProxy(UID(1, 1), SERVER_KNOBS->VERSIONS_PER_SECOND * i, testActualTps);
		if (IRKRateUpdater::requireSmallBlobVersionLag()) {
			return Void();
		}
		env.update();
	}
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::blob_worker_lag);
	// When simulation is sped up, ratekeeper limit is artificially increased
	if (!(g_network->isSimulated() && g_simulator->speedUpSimulation)) {
		ASSERT(checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 0.0));
	}
	return Void();
}
