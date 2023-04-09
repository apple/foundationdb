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

ACTOR Future<TLogQueueInfo> getMockTLogQueueInfo(UID id,
                                                 int64_t queueBytes,
                                                 int64_t inputBytesPerSecond,
                                                 int64_t availableSpace = 100e9,
                                                 int64_t totalSpace = 100e9) {
	state int iterations = 10000;
	state TLogQueueInfo result(id);
	state TLogQueuingMetricsReply reply;
	state Smoother smoothTotalDurableBytes(10.0); // unused

	reply.bytesInput = queueBytes;
	reply.bytesDurable = 0;
	reply.storageBytes.total = totalSpace;
	reply.storageBytes.available = availableSpace;
	reply.storageBytes.free = availableSpace;
	reply.storageBytes.used = totalSpace - availableSpace;
	reply.v = 0;
	result.update(reply, smoothTotalDurableBytes);

	while (iterations--) {
		wait(delay(0.01));
		reply.bytesInput += inputBytesPerSecond / 100;
		reply.bytesDurable += inputBytesPerSecond / 100;
		reply.v += 1000;
		result.update(reply, smoothTotalDurableBytes);
	}

	return result;
}

ACTOR Future<StorageQueueInfo> getMockStorageQueueInfo(UID id,
                                                       LocalityData locality,
                                                       int64_t storageQueueBytes,
                                                       double inputBytesPerSecond,
                                                       int64_t targetNonDurableVersionsLag = 5e6,
                                                       int64_t availableSpace = 100e9,
                                                       int64_t totalSpace = 100e9) {
	state int iterations = 10000;
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
	reply.version = std::max<Version>(targetNonDurableVersionsLag, (1e6 * storageQueueBytes) / inputBytesPerSecond);
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
	checkApproximatelyEqual(ss.getSmoothFreeSpace(), availableSpace);
	checkApproximatelyEqual(ss.getSmoothTotalSpace(), totalSpace);
	checkApproximatelyEqual(ss.getStorageQueueBytes(), storageQueueBytes);
	checkApproximatelyEqual(
	    ss.getDurabilityLag(),
	    std::max<int64_t>(targetNonDurableVersionsLag, (1e6 * storageQueueBytes) / inputBytesPerSecond));

	return ss;
}

struct RKRateUpdaterTestEnvironment {
	MockRKMetricsTracker metricsTracker;
	MockRKRateServer rateServer;
	MockTagThrottler tagThrottler;
	MockRKConfigurationMonitor configurationMonitor;
	MockRKRecoveryTracker recoveryTracker;
	Deque<double> actualTpsHistory;
	Deque<std::pair<double, Version>> blobWorkerVersionHistory;
	double blobWorkerTime{ 0.0 };
	double unblockedAssignmentTime{ 0.0 };

	RKRateUpdater rateUpdater;

	RKRateUpdaterTestEnvironment(double actualTps, int storageTeamSize)
	  : rateServer(actualTps), configurationMonitor(storageTeamSize),
	    rateUpdater(UID{},
	                RatekeeperLimits(TransactionPriority::DEFAULT, "", 1000e6, 100e6, 1000e6, 100e6, 2e9, 2e9, 300.0)) {
		for (int i = 0; i <= SERVER_KNOBS->NEEDED_TPS_HISTORY_SAMPLES; ++i) {
			actualTpsHistory.push_back(actualTps);
		}
	}

	void update() {
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
	}
};

} // namespace

// No processes are reporting any metrics to the rate updater. The default ratekeeper limit
// is applied.
TEST_CASE("/fdbserver/RKRateUpdater/Simple") {
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.update();
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), SERVER_KNOBS->RATEKEEPER_DEFAULT_LIMIT);
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::unlimited);
	return Void();
}

// Currently, a workload of 1000 transactions per second is using up half of the storage queue
// spring bytes (950MB SQ, with a 1GB target and 100MB of spring). The rate updater estimates
// that the cluster can handle double the current transaction rate, or 2000 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/HighSQ") {
	StorageQueueInfo ss = wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 950e6, 1e6));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 2000.0);
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_write_queue_size);
	return Void();
}

// Currently, a workload of 1000 transactions per second is exceeding the target storage queue
// size by half of the spring bytes limit (1050MB SQ, with a 1GB target and 100MB of spring).
// The rate updater estimates that the cluster can handle 2/3 of the current transaction rate,
// or ~667 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/HighSQ2") {
	StorageQueueInfo ss = wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 1050e6, 1e6));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 2000.0 / 3);
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_write_queue_size);
	return Void();
}

// Currently, a workload of 1000 transactions per second is exceeding the sum of the target
// storage queue bytes and spring bytes. The rate updater applies the maximum possible throttling
// based on storage queue, limiting throughput to half the current transaction rate, or 500
// transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/HighSQ3") {
	StorageQueueInfo ss = wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 1500e6, 1e6));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 500.0);
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_write_queue_size);
	return Void();
}

// For the one storage process emitting metrics, storage queue is below the target bytes minus
// the spring bytes. Therefore, throttling is enforced to ensure that at the current write rate
// per transaction, an MVCC window worth of writes does not cause storage queue to rise above
// the target bytes minus spring bytes.
TEST_CASE("/fdbserver/RKRateUpdater/StorageWriteBandwidthMVCC") {
	StorageQueueInfo ss = wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 500e6, 1e6));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_write_bandwidth_mvcc);
	ASSERT_GT(env.rateUpdater.getTpsLimit(), 1000.0);
	return Void();
}

// The current 1000 transaction per second workload is saturating the storage queue of one server,
// but not saturating the storage queue of the other storage server in a different zone.
// If SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND > 0, the rate updated does not throttle based
// on the worst storage server's queue.
TEST_CASE("/fdbserver/RKRateUpdater/IgnoreWorstZone") {
	state LocalityData locality1({}, "zone1"_sr, {}, {});
	state LocalityData locality2({}, "zone2"_sr, {}, {});
	state std::vector<Future<StorageQueueInfo>> ssFutures;

	if (SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND == 0) {
		return Void();
	}

	ssFutures.reserve(2);
	ssFutures.push_back(getMockStorageQueueInfo(UID(1, 1), locality1, 500e6, 1e6));
	ssFutures.push_back(getMockStorageQueueInfo(UID(2, 2), locality2, 1500e6, 1e6));
	wait(waitForAll(ssFutures));
	RKRateUpdaterTestEnvironment env(1000.0, 2);
	env.metricsTracker.updateStorageQueueInfo(ssFutures[0].get());
	env.metricsTracker.updateStorageQueueInfo(ssFutures[1].get());
	env.update();

	// Even though 1 storage server won't allow more that the current transaction rate, the
	// rate updater will still allow more than the current transaction rate, because this storage
	// server's zone is ignored.
	ASSERT_GT(env.rateUpdater.getTpsLimit(), 1000.0);

	// Even though the storage server with high storage queue is ignored, we still report write
	// queue size as the limiting reason.
	// TODO: Should this behaviour be changed?
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_write_queue_size);
	return Void();
}

// The durability lag on the single storage server exceeds the configured durability lag limit.
// Therefore, the rate updated throttles based on storage server durability lag.
TEST_CASE("/fdbserver/RKRateUpdater/HighNDV") {
	StorageQueueInfo ss = wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 500e6, 1e6, 3e9));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_durability_lag);
	return Void();
}

// The rate updater was unable to fetch the list of storage servers. Therefore, the tps limit
// is set to 0.
TEST_CASE("/fdbserver/RKRateUpdater/ServerListFetchFailed") {
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.failSSListFetch();
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_list_fetch_failed);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 0.0);
	return Void();
}

// Though the storage queue is only 300MB (less than the threshold for throttling on storage queue alone),
// the storage server only has 300MB of space to spare before hitting the MIN_AVAILABLE_SPACE threshold.
// As a result, the rate updater throttles at the current transaction rate of 1000 TPS.
TEST_CASE("/fdbserver/RKRateUpdater/SSFreeSpace") {
	int64_t totalSpace = 1e9;
	int64_t availableSpace = SERVER_KNOBS->MIN_AVAILABLE_SPACE + 300e6;
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 300e6, 1e6, 5e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_min_free_space);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 1000.0);
	return Void();
}

// Though the storage queue is only 600MB (less than the threshold for throttling on storage queue alone),
// the storage server only has 300MB of space to spare before hitting the MIN_AVAILABLE_SPACE threshold.
// As a result, the rate updater throttles at half the current transaction rate, or 500 TPS.
TEST_CASE("/fdbserver/RKRateUpdater/SSFreeSpace2") {
	int64_t totalSpace = 1e9;
	int64_t availableSpace = SERVER_KNOBS->MIN_AVAILABLE_SPACE + 300e6;
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 600e6, 1e6, 5e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_min_free_space);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 500.0);
	return Void();
}

// Though the storage queue is only 300MB (less than the threshold for throttling on storage queue alone),
// the storage server only has 300MB of space to spare before hitting the MIN_AVAILABLE_SPACE_RATIO threshold.
// As a result, the rate updater throttles at the current transaction rate of 1000 TPS.
TEST_CASE("/fdbserver/RKRateUpdater/SSFreeSpaceRatio") {
	int64_t totalSpace = 1e15;
	int64_t availableSpace = (totalSpace * SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO) + 300e6;
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 300e6, 1e6, 5e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_min_free_space_ratio);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 1000.0);
	return Void();
}

// Though the storage queue is only 600MB (less than the threshold for throttling on storage queue alone),
// the storage server only has 300MB of space to spare before hitting the MIN_AVAILABLE_SPACE_RATIO threshold.
// As a result, the rate updater throttles at half the current transaction rate, or 500 TPS.
TEST_CASE("/fdbserver/RKRateUpdater/SSFreeSpaceRatio2") {
	int64_t totalSpace = 1e15;
	int64_t availableSpace = (totalSpace * SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO) + 300e6;
	StorageQueueInfo ss =
	    wait(getMockStorageQueueInfo(UID(1, 1), LocalityData{}, 600e6, 1e6, 5e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateStorageQueueInfo(ss);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::storage_server_min_free_space_ratio);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 500.0);
	return Void();
}

// Currently, a workload of 1000 transactions per second is using up half of the tlog queue
// spring bytes (950MB queue, with a 1GB target and 100MB of spring). The rate updater estimates
// that the cluster can handle double the current transaction rate, or 2000 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogQueue") {
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), 950e6, 1e6));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_write_queue);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 2000.0);
	return Void();
}

// Currently, a workload of 1000 transactions per second is exceeding the target tlog queue size
// by half of the spring bytes limit (1050MB queue, with a 1GB target and 100MB of spring).
// The rate updater estimates that the cluster can handle 2/3 of the current transaction rate,
// or ~667 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogQueue2") {
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), 1050e6, 1e6));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_write_queue);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 2000.0 / 3);
	return Void();
}

// Currently, a workload of 1000 transactions per second is exceeding the sum of the target
// tlog queue bytes and spring bytes. The rate updater applies the maximum possible throttling based on
// tlog queue, limiting throughput to half the current transaction rate, or 500 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogQueue3") {
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), 1500e6, 1e6));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_write_queue);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 500.0);
	return Void();
}

// For the one tlog process emitting metrics, the queue is below the target bytes minus
// the spring bytes. Therefore, throttling is enforced to ensure that at the current write
// rate per transaction, an MVCC window worth of writes does not cause tlog queue to rise
// above the target bytes minues spring bytes.
TEST_CASE("/fdbserver/RKRateUpdater/TLogWriteBandwidthMVCC") {
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), 500e6, 1e6));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_mvcc_write_bandwidth);
	ASSERT_GT(env.rateUpdater.getTpsLimit(), 1000.0);
	return Void();
}

// The tlog queue plus currently used disk space add to leave only MIN_AVAILABLE_SPACE
// bytes left on the tlog disk. The rate updater reacts by throttling at the current
// transaction rate of 1000 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogFreeSpace") {
	int64_t totalSpace = 1e9;
	int64_t availableSpace = SERVER_KNOBS->MIN_AVAILABLE_SPACE + 300e6;
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), 300e6, 1e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_min_free_space);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 1000.0);
	return Void();
}

// The tlog queue plus currently used disk space add to leave than MIN_AVAILABLE_SPACE / 2
// bytes on disk. In response, the rate updater throttles throughput to 0.
TEST_CASE("/fdbserver/RKRateUpdater/TLogFreeSpace2") {
	int64_t totalSpace = 1e9;
	int64_t availableSpace = SERVER_KNOBS->MIN_AVAILABLE_SPACE + 300e6;
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), 600e6, 1e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_min_free_space);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 0.0);
	return Void();
}

// The tlog queue plus currently used disk space add to leave only the available
// space ratio of total disk space. The rate updater reacts by throttling at the current
// transaction rate of 1000 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogFreeSpaceRatio") {
	int64_t totalSpace = 1e15;
	int64_t availableSpace = (totalSpace * SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO) + 300e6;
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), 300e6, 1e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_min_free_space_ratio);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 1000.0);
	return Void();
}

// The tlog queue plus currently used disk space add to exceed the minimum available disk
// space ratio of total disk space plus spring bytes. In response, the rate updater throttles
// at half the current transaction rate, or 500 transactions per second.
TEST_CASE("/fdbserver/RKRateUpdater/TLogFreeSpaceRatio2") {
	int64_t totalSpace = 1e15;
	int64_t availableSpace = (totalSpace * SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO) + 300e6;
	TLogQueueInfo tl = wait(getMockTLogQueueInfo(UID(1, 1), 600e6, 1e6, availableSpace, totalSpace));
	RKRateUpdaterTestEnvironment env(1000.0, 1);
	env.metricsTracker.updateTLogQueueInfo(tl);
	env.update();
	ASSERT_EQ(env.rateUpdater.getLimitReason(), limitReason_t::log_server_min_free_space_ratio);
	checkApproximatelyEqual(env.rateUpdater.getTpsLimit(), 500.0);
	return Void();
}
