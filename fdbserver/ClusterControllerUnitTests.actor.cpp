#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbserver/ClusterController.actor.h"
#include "fdbserver/CoordinationInterface.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

class ClusterControllerTesting {
public:
	// Tests `ClusterController::updateWorkerHealth()` can update `ClusterController::workerHealth`
	// based on `UpdateWorkerHealth` request correctly.
	ACTOR static Future<Void> updateWorkerHealth() {
		// Create a testing ClusterController. Most of the internal states do not matter in this test.
		state ClusterController data(ClusterControllerFullInterface(),
		                             LocalityData(),
		                             ServerCoordinators(Reference<IClusterConnectionRecord>(
		                                 new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
		                             makeReference<AsyncVar<Optional<UID>>>());
		state NetworkAddress workerAddress(IPAddress(0x01010101), 1);
		state NetworkAddress badPeer1(IPAddress(0x02020202), 1);
		state NetworkAddress badPeer2(IPAddress(0x03030303), 1);
		state NetworkAddress badPeer3(IPAddress(0x04040404), 1);

		// Create a `UpdateWorkerHealthRequest` with two bad peers, and they should appear in the
		// `workerAddress`'s degradedPeers.
		{
			UpdateWorkerHealthRequest req;
			req.address = workerAddress;
			req.degradedPeers.push_back(badPeer1);
			req.degradedPeers.push_back(badPeer2);
			req.disconnectedPeers.push_back(badPeer1);
			req.disconnectedPeers.push_back(badPeer2);
			data.updateWorkerHealth(req);
			ASSERT(data.workerHealth.find(workerAddress) != data.workerHealth.end());
			auto& health = data.workerHealth[workerAddress];
			ASSERT_EQ(health.degradedPeers.size(), 2);
			ASSERT(health.degradedPeers.find(badPeer1) != health.degradedPeers.end());
			ASSERT_EQ(health.degradedPeers[badPeer1].startTime, health.degradedPeers[badPeer1].lastRefreshTime);
			ASSERT(health.degradedPeers.find(badPeer2) != health.degradedPeers.end());
			ASSERT_EQ(health.degradedPeers[badPeer2].startTime, health.degradedPeers[badPeer2].lastRefreshTime);
			ASSERT_EQ(health.disconnectedPeers.size(), 2);
			ASSERT(health.disconnectedPeers.find(badPeer1) != health.disconnectedPeers.end());
			ASSERT_EQ(health.disconnectedPeers[badPeer1].startTime, health.disconnectedPeers[badPeer1].lastRefreshTime);
			ASSERT(health.disconnectedPeers.find(badPeer2) != health.disconnectedPeers.end());
			ASSERT_EQ(health.disconnectedPeers[badPeer2].startTime, health.disconnectedPeers[badPeer2].lastRefreshTime);
		}

		// Create a `UpdateWorkerHealthRequest` with two bad peers, one from the previous test and a new one.
		// The one from the previous test should have lastRefreshTime updated.
		// The other one from the previous test not included in this test should not be removed.
		state double previousStartTime;
		state double previousRefreshTime;
		{
			// Make the time to move so that now() guarantees to return a larger value than before.
			wait(delay(0.001));
			UpdateWorkerHealthRequest req;
			req.address = workerAddress;
			req.degradedPeers.push_back(badPeer1);
			req.degradedPeers.push_back(badPeer3);
			req.disconnectedPeers.push_back(badPeer1);
			req.disconnectedPeers.push_back(badPeer3);
			data.updateWorkerHealth(req);
			ASSERT(data.workerHealth.find(workerAddress) != data.workerHealth.end());
			auto& health = data.workerHealth[workerAddress];
			ASSERT_EQ(health.degradedPeers.size(), 3);
			ASSERT(health.degradedPeers.find(badPeer1) != health.degradedPeers.end());
			ASSERT_LT(health.degradedPeers[badPeer1].startTime, health.degradedPeers[badPeer1].lastRefreshTime);
			ASSERT(health.degradedPeers.find(badPeer2) != health.degradedPeers.end());
			ASSERT_EQ(health.degradedPeers[badPeer2].startTime, health.degradedPeers[badPeer2].lastRefreshTime);
			ASSERT_EQ(health.degradedPeers[badPeer2].startTime, health.degradedPeers[badPeer1].startTime);
			ASSERT(health.degradedPeers.find(badPeer3) != health.degradedPeers.end());
			ASSERT_EQ(health.degradedPeers[badPeer3].startTime, health.degradedPeers[badPeer3].lastRefreshTime);
			ASSERT_EQ(health.disconnectedPeers.size(), 3);
			ASSERT(health.disconnectedPeers.find(badPeer1) != health.disconnectedPeers.end());
			ASSERT_LT(health.disconnectedPeers[badPeer1].startTime, health.disconnectedPeers[badPeer1].lastRefreshTime);
			ASSERT(health.disconnectedPeers.find(badPeer2) != health.disconnectedPeers.end());
			ASSERT_EQ(health.disconnectedPeers[badPeer2].startTime, health.disconnectedPeers[badPeer2].lastRefreshTime);
			ASSERT_EQ(health.disconnectedPeers[badPeer2].startTime, health.disconnectedPeers[badPeer1].startTime);
			ASSERT(health.disconnectedPeers.find(badPeer3) != health.disconnectedPeers.end());
			ASSERT_EQ(health.disconnectedPeers[badPeer3].startTime, health.disconnectedPeers[badPeer3].lastRefreshTime);

			previousStartTime = health.degradedPeers[badPeer3].startTime;
			previousRefreshTime = health.degradedPeers[badPeer3].lastRefreshTime;
		}

		// Create a `UpdateWorkerHealthRequest` with empty `degradedPeers`, which should not remove the worker
		// from `workerHealth`.
		{
			wait(delay(0.001));
			UpdateWorkerHealthRequest req;
			req.address = workerAddress;
			data.updateWorkerHealth(req);
			ASSERT(data.workerHealth.find(workerAddress) != data.workerHealth.end());
			auto& health = data.workerHealth[workerAddress];
			ASSERT_EQ(health.degradedPeers.size(), 3);
			ASSERT(health.degradedPeers.find(badPeer3) != health.degradedPeers.end());
			ASSERT_EQ(health.degradedPeers[badPeer3].startTime, previousStartTime);
			ASSERT_EQ(health.degradedPeers[badPeer3].lastRefreshTime, previousRefreshTime);
			ASSERT_EQ(health.disconnectedPeers.size(), 3);
			ASSERT(health.disconnectedPeers.find(badPeer3) != health.disconnectedPeers.end());
			ASSERT_EQ(health.disconnectedPeers[badPeer3].startTime, previousStartTime);
			ASSERT_EQ(health.disconnectedPeers[badPeer3].lastRefreshTime, previousRefreshTime);
		}

		return Void();
	}

	static void updateRecoveredWorkers() {
		// Create a testing ClusterController. Most of the internal states do not matter in this test.
		ClusterController data(ClusterControllerFullInterface(),
		                       LocalityData(),
		                       ServerCoordinators(Reference<IClusterConnectionRecord>(
		                           new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
		                       makeReference<AsyncVar<Optional<UID>>>());
		NetworkAddress worker1(IPAddress(0x01010101), 1);
		NetworkAddress worker2(IPAddress(0x11111111), 1);
		NetworkAddress badPeer1(IPAddress(0x02020202), 1);
		NetworkAddress badPeer2(IPAddress(0x03030303), 1);
		NetworkAddress disconnectedPeer3(IPAddress(0x04040404), 1);

		// Create following test scenario:
		// 	 worker1 -> badPeer1 active
		// 	 worker1 -> badPeer2 recovered
		//   worker1 -> disconnectedPeer3 active
		// 	 worker2 -> badPeer2 recovered
		//   worker2 -> disconnectedPeer3 recovered
		data.workerHealth[worker1].degradedPeers[badPeer1] = {
			now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1, now()
		};
		data.workerHealth[worker1].degradedPeers[badPeer2] = {
			now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1,
			now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1
		};
		data.workerHealth[worker1].degradedPeers[disconnectedPeer3] = {
			now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1, now()
		};
		data.workerHealth[worker2].degradedPeers[badPeer2] = {
			now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1,
			now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1
		};
		data.workerHealth[worker2].degradedPeers[disconnectedPeer3] = {
			now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1,
			now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1
		};
		data.updateRecoveredWorkers();

		ASSERT_EQ(data.workerHealth.size(), 1);
		ASSERT(data.workerHealth.find(worker1) != data.workerHealth.end());
		ASSERT(data.workerHealth[worker1].degradedPeers.find(badPeer1) !=
		       data.workerHealth[worker1].degradedPeers.end());
		ASSERT(data.workerHealth[worker1].degradedPeers.find(badPeer2) ==
		       data.workerHealth[worker1].degradedPeers.end());
		ASSERT(data.workerHealth[worker1].degradedPeers.find(disconnectedPeer3) !=
		       data.workerHealth[worker1].degradedPeers.end());
		ASSERT(data.workerHealth.find(worker2) == data.workerHealth.end());
	}

	static void getDegradationInfo() {
		// Create a testing ClusterController. Most of the internal states do not matter in this test.
		ClusterController data(ClusterControllerFullInterface(),
		                       LocalityData(),
		                       ServerCoordinators(Reference<IClusterConnectionRecord>(
		                           new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
		                       makeReference<AsyncVar<Optional<UID>>>());
		NetworkAddress worker(IPAddress(0x01010101), 1);
		NetworkAddress badPeer1(IPAddress(0x02020202), 1);
		NetworkAddress badPeer2(IPAddress(0x03030303), 1);
		NetworkAddress badPeer3(IPAddress(0x04040404), 1);
		NetworkAddress badPeer4(IPAddress(0x05050505), 1);

		// Test that a reported degraded link should stay for sometime before being considered as a degraded
		// link by cluster controller.
		{
			data.workerHealth[worker].degradedPeers[badPeer1] = { now(), now() };
			data.workerHealth[worker].disconnectedPeers[badPeer2] = { now(), now() };
			ASSERT(data.getDegradationInfo().degradedServers.empty());
			data.workerHealth.clear();
		}

		// Test that when there is only one reported degraded link, getDegradationInfo can return correct
		// degraded server.
		{
			data.workerHealth[worker].degradedPeers[badPeer1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			auto degradationInfo = data.getDegradationInfo();
			ASSERT(degradationInfo.degradedServers.size() == 1);
			ASSERT(degradationInfo.degradedServers.find(badPeer1) != degradationInfo.degradedServers.end());
			ASSERT(degradationInfo.disconnectedServers.empty());
			data.workerHealth.clear();
		}

		// Test that when there is only one reported disconnected link, getDegradationInfo can return correct
		// degraded server.
		{
			data.workerHealth[worker].disconnectedPeers[badPeer1] = {
				now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1, now()
			};
			auto degradationInfo = data.getDegradationInfo();
			ASSERT(degradationInfo.disconnectedServers.size() == 1);
			ASSERT(degradationInfo.disconnectedServers.find(badPeer1) != degradationInfo.disconnectedServers.end());
			ASSERT(degradationInfo.degradedServers.empty());
			data.workerHealth.clear();
		}

		// Test that if both A complains B and B compalins A, only one of the server will be chosen as degraded
		// server.
		{
			data.workerHealth[worker].degradedPeers[badPeer1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[badPeer1].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[worker].disconnectedPeers[badPeer2] = {
				now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1, now()
			};
			data.workerHealth[badPeer2].disconnectedPeers[worker] = {
				now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1, now()
			};
			auto degradationInfo = data.getDegradationInfo();
			ASSERT(degradationInfo.degradedServers.size() == 1);
			ASSERT(degradationInfo.degradedServers.find(worker) != degradationInfo.degradedServers.end() ||
			       degradationInfo.degradedServers.find(badPeer1) != degradationInfo.degradedServers.end());
			ASSERT(degradationInfo.disconnectedServers.size() == 1);
			ASSERT(degradationInfo.disconnectedServers.find(worker) != degradationInfo.disconnectedServers.end() ||
			       degradationInfo.disconnectedServers.find(badPeer2) != degradationInfo.disconnectedServers.end());
			data.workerHealth.clear();
		}

		// Test that if B complains A and C complains A, A is selected as degraded server instead of B or C.
		{
			ASSERT(SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE < 4);

			// test for both degraded peers and disconnected peers.
			data.workerHealth[worker].degradedPeers[badPeer1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[badPeer1].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[worker].degradedPeers[badPeer2] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[badPeer2].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[worker].disconnectedPeers[badPeer3] = {
				now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1, now()
			};
			data.workerHealth[badPeer3].disconnectedPeers[worker] = {
				now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1, now()
			};
			data.workerHealth[worker].disconnectedPeers[badPeer4] = {
				now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1, now()
			};
			data.workerHealth[badPeer4].disconnectedPeers[worker] = {
				now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1, now()
			};
			auto degradationInfo = data.getDegradationInfo();
			ASSERT(degradationInfo.degradedServers.size() == 1);
			ASSERT(degradationInfo.degradedServers.find(worker) != degradationInfo.degradedServers.end());
			ASSERT(degradationInfo.disconnectedServers.size() == 1);
			ASSERT(degradationInfo.disconnectedServers.find(worker) != degradationInfo.disconnectedServers.end());
			data.workerHealth.clear();
		}

		// Test that if the number of complainers exceeds the threshold, no degraded server is returned.
		{
			ASSERT(SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE < 4);
			data.workerHealth[badPeer1].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[badPeer2].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[badPeer3].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[badPeer4].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			ASSERT(data.getDegradationInfo().degradedServers.empty());
			data.workerHealth.clear();
		}

		// Test that CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE doesn't affect disconnectedServers calculation.
		{
			ASSERT(SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE < 4);
			data.workerHealth[badPeer1].disconnectedPeers[worker] = {
				now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1, now()
			};
			data.workerHealth[badPeer2].disconnectedPeers[worker] = {
				now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1, now()
			};
			data.workerHealth[badPeer3].disconnectedPeers[worker] = {
				now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1, now()
			};
			data.workerHealth[badPeer4].disconnectedPeers[worker] = {
				now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1, now()
			};
			ASSERT(data.getDegradationInfo().disconnectedServers.size() == 1);
			ASSERT(data.getDegradationInfo().disconnectedServers.find(worker) !=
			       data.getDegradationInfo().disconnectedServers.end());
			data.workerHealth.clear();
		}

		// Test that if the degradation is reported both ways between A and other 4 servers, no degraded server
		// is returned.
		{
			ASSERT(SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE < 4);
			data.workerHealth[worker].degradedPeers[badPeer1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[badPeer1].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[worker].degradedPeers[badPeer2] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[badPeer2].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[worker].degradedPeers[badPeer3] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[badPeer3].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[worker].degradedPeers[badPeer4] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			data.workerHealth[badPeer4].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
				                                                  now() };
			ASSERT(data.getDegradationInfo().degradedServers.empty());
			data.workerHealth.clear();
		}
	}

	static void recentRecoveryCountDueToHealth() {
		// Create a testing ClusterController. Most of the internal states do not matter in this test.
		ClusterController data(ClusterControllerFullInterface(),
		                       LocalityData(),
		                       ServerCoordinators(Reference<IClusterConnectionRecord>(
		                           new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
		                       makeReference<AsyncVar<Optional<UID>>>());

		ASSERT_EQ(data.recentRecoveryCountDueToHealth(), 0);

		data.recentHealthTriggeredRecoveryTime.push(now() - SERVER_KNOBS->CC_TRACKING_HEALTH_RECOVERY_INTERVAL - 1);
		ASSERT_EQ(data.recentRecoveryCountDueToHealth(), 0);

		data.recentHealthTriggeredRecoveryTime.push(now() - SERVER_KNOBS->CC_TRACKING_HEALTH_RECOVERY_INTERVAL + 1);
		ASSERT_EQ(data.recentRecoveryCountDueToHealth(), 1);

		data.recentHealthTriggeredRecoveryTime.push(now());
		ASSERT_EQ(data.recentRecoveryCountDueToHealth(), 2);
	}

	static void shouldTriggerRecoveryDueToDegradedServers() {
		// Create a testing ClusterController. Most of the internal states do not matter in this test.
		ClusterController data(ClusterControllerFullInterface(),
		                       LocalityData(),
		                       ServerCoordinators(Reference<IClusterConnectionRecord>(
		                           new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
		                       makeReference<AsyncVar<Optional<UID>>>());
		NetworkAddress master(IPAddress(0x01010101), 1);
		NetworkAddress tlog(IPAddress(0x02020202), 1);
		NetworkAddress satelliteTlog(IPAddress(0x03030303), 1);
		NetworkAddress remoteTlog(IPAddress(0x04040404), 1);
		NetworkAddress logRouter(IPAddress(0x05050505), 1);
		NetworkAddress backup(IPAddress(0x06060606), 1);
		NetworkAddress proxy(IPAddress(0x07070707), 1);
		NetworkAddress resolver(IPAddress(0x08080808), 1);
		NetworkAddress clusterController(IPAddress(0x09090909), 1);
		UID testUID(1, 2);

		// Create a ServerDBInfo using above addresses.
		ServerDBInfo testDbInfo;
		testDbInfo.clusterInterface.changeCoordinators =
		    RequestStream<struct ChangeCoordinatorsRequest>(Endpoint({ clusterController }, UID(1, 2)));

		MasterInterface mInterface;
		mInterface.getCommitVersion = RequestStream<struct GetCommitVersionRequest>(Endpoint({ master }, UID(1, 2)));
		testDbInfo.master = mInterface;

		TLogInterface localTLogInterf;
		localTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ tlog }, testUID));
		TLogInterface localLogRouterInterf;
		localLogRouterInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ logRouter }, testUID));
		BackupInterface backupInterf;
		backupInterf.waitFailure = RequestStream<ReplyPromise<Void>>(Endpoint({ backup }, testUID));
		TLogSet localTLogSet;
		localTLogSet.isLocal = true;
		localTLogSet.tLogs.push_back(OptionalInterface(localTLogInterf));
		localTLogSet.logRouters.push_back(OptionalInterface(localLogRouterInterf));
		localTLogSet.backupWorkers.push_back(OptionalInterface(backupInterf));
		testDbInfo.logSystemConfig.tLogs.push_back(localTLogSet);

		TLogInterface sateTLogInterf;
		sateTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ satelliteTlog }, testUID));
		TLogSet sateTLogSet;
		sateTLogSet.isLocal = true;
		sateTLogSet.locality = tagLocalitySatellite;
		sateTLogSet.tLogs.push_back(OptionalInterface(sateTLogInterf));
		testDbInfo.logSystemConfig.tLogs.push_back(sateTLogSet);

		TLogInterface remoteTLogInterf;
		remoteTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ remoteTlog }, testUID));
		TLogSet remoteTLogSet;
		remoteTLogSet.isLocal = false;
		remoteTLogSet.tLogs.push_back(OptionalInterface(remoteTLogInterf));
		testDbInfo.logSystemConfig.tLogs.push_back(remoteTLogSet);

		GrvProxyInterface proxyInterf;
		proxyInterf.getConsistentReadVersion =
		    PublicRequestStream<struct GetReadVersionRequest>(Endpoint({ proxy }, testUID));
		testDbInfo.client.grvProxies.push_back(proxyInterf);

		ResolverInterface resolverInterf;
		resolverInterf.resolve = RequestStream<struct ResolveTransactionBatchRequest>(Endpoint({ resolver }, testUID));
		testDbInfo.resolvers.push_back(resolverInterf);

		testDbInfo.recoveryState = RecoveryState::ACCEPTING_COMMITS;

		// No recovery when no degraded servers.
		data.db.serverInfo->set(testDbInfo);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());

		// Trigger recovery when master is degraded.
		data.degradationInfo.degradedServers.insert(master);
		ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(master);
		ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();

		// Trigger recovery when primary TLog is degraded.
		data.degradationInfo.degradedServers.insert(tlog);
		ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(tlog);
		ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();

		// No recovery when satellite Tlog is degraded.
		data.degradationInfo.degradedServers.insert(satelliteTlog);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(satelliteTlog);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();

		// No recovery when remote tlog is degraded.
		data.degradationInfo.degradedServers.insert(remoteTlog);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(remoteTlog);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();

		// No recovery when log router is degraded.
		data.degradationInfo.degradedServers.insert(logRouter);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(logRouter);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();

		// No recovery when backup worker is degraded.
		data.degradationInfo.degradedServers.insert(backup);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(backup);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();

		// Trigger recovery when proxy is degraded.
		data.degradationInfo.degradedServers.insert(proxy);
		ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(proxy);
		ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();

		// Trigger recovery when resolver is degraded.
		data.degradationInfo.degradedServers.insert(resolver);
		ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(resolver);
		ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();
	}

	static void shouldTriggerFailoverDueToDegradedServers() {
		// Create a testing ClusterController. Most of the internal states do not matter in this test.
		ClusterController data(ClusterControllerFullInterface(),
		                       LocalityData(),
		                       ServerCoordinators(Reference<IClusterConnectionRecord>(
		                           new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
		                       makeReference<AsyncVar<Optional<UID>>>());
		NetworkAddress master(IPAddress(0x01010101), 1);
		NetworkAddress tlog(IPAddress(0x02020202), 1);
		NetworkAddress satelliteTlog(IPAddress(0x03030303), 1);
		NetworkAddress remoteTlog(IPAddress(0x04040404), 1);
		NetworkAddress logRouter(IPAddress(0x05050505), 1);
		NetworkAddress backup(IPAddress(0x06060606), 1);
		NetworkAddress proxy(IPAddress(0x07070707), 1);
		NetworkAddress proxy2(IPAddress(0x08080808), 1);
		NetworkAddress resolver(IPAddress(0x09090909), 1);
		NetworkAddress clusterController(IPAddress(0x10101010), 1);
		UID testUID(1, 2);

		data.db.config.usableRegions = 2;

		// Create a ServerDBInfo using above addresses.
		ServerDBInfo testDbInfo;
		testDbInfo.clusterInterface.changeCoordinators =
		    RequestStream<struct ChangeCoordinatorsRequest>(Endpoint({ clusterController }, UID(1, 2)));

		TLogInterface localTLogInterf;
		localTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ tlog }, testUID));
		TLogInterface localLogRouterInterf;
		localLogRouterInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ logRouter }, testUID));
		BackupInterface backupInterf;
		backupInterf.waitFailure = RequestStream<ReplyPromise<Void>>(Endpoint({ backup }, testUID));
		TLogSet localTLogSet;
		localTLogSet.isLocal = true;
		localTLogSet.tLogs.push_back(OptionalInterface(localTLogInterf));
		localTLogSet.logRouters.push_back(OptionalInterface(localLogRouterInterf));
		localTLogSet.backupWorkers.push_back(OptionalInterface(backupInterf));
		testDbInfo.logSystemConfig.tLogs.push_back(localTLogSet);

		TLogInterface sateTLogInterf;
		sateTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ satelliteTlog }, testUID));
		TLogSet sateTLogSet;
		sateTLogSet.isLocal = true;
		sateTLogSet.locality = tagLocalitySatellite;
		sateTLogSet.tLogs.push_back(OptionalInterface(sateTLogInterf));
		testDbInfo.logSystemConfig.tLogs.push_back(sateTLogSet);

		TLogInterface remoteTLogInterf;
		remoteTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ remoteTlog }, testUID));
		TLogSet remoteTLogSet;
		remoteTLogSet.isLocal = false;
		remoteTLogSet.tLogs.push_back(OptionalInterface(remoteTLogInterf));
		testDbInfo.logSystemConfig.tLogs.push_back(remoteTLogSet);

		GrvProxyInterface grvProxyInterf;
		grvProxyInterf.getConsistentReadVersion =
		    PublicRequestStream<struct GetReadVersionRequest>(Endpoint({ proxy }, testUID));
		testDbInfo.client.grvProxies.push_back(grvProxyInterf);

		CommitProxyInterface commitProxyInterf;
		commitProxyInterf.commit = PublicRequestStream<struct CommitTransactionRequest>(Endpoint({ proxy2 }, testUID));
		testDbInfo.client.commitProxies.push_back(commitProxyInterf);

		ResolverInterface resolverInterf;
		resolverInterf.resolve = RequestStream<struct ResolveTransactionBatchRequest>(Endpoint({ resolver }, testUID));
		testDbInfo.resolvers.push_back(resolverInterf);

		testDbInfo.recoveryState = RecoveryState::ACCEPTING_COMMITS;

		// No failover when no degraded servers.
		data.db.serverInfo->set(testDbInfo);
		ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());

		// No failover when small number of degraded servers
		data.degradationInfo.degradedServers.insert(master);
		ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(master);
		ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();

		// Trigger failover when enough servers in the txn system are degraded.
		data.degradationInfo.degradedServers.insert(master);
		data.degradationInfo.degradedServers.insert(tlog);
		data.degradationInfo.degradedServers.insert(proxy);
		data.degradationInfo.degradedServers.insert(proxy2);
		data.degradationInfo.degradedServers.insert(resolver);
		ASSERT(data.shouldTriggerFailoverDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(master);
		data.degradationInfo.disconnectedServers.insert(tlog);
		data.degradationInfo.disconnectedServers.insert(proxy);
		data.degradationInfo.disconnectedServers.insert(proxy2);
		data.degradationInfo.disconnectedServers.insert(resolver);
		ASSERT(data.shouldTriggerFailoverDueToDegradedServers());

		// No failover when usable region is 1.
		data.db.config.usableRegions = 1;
		ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
		data.db.config.usableRegions = 2;

		// No failover when remote is also degraded.
		data.degradationInfo.degradedServers.insert(remoteTlog);
		ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(remoteTlog);
		ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();

		// No failover when some are not from transaction system
		data.degradationInfo.degradedServers.insert(NetworkAddress(IPAddress(0x13131313), 1));
		data.degradationInfo.degradedServers.insert(NetworkAddress(IPAddress(0x13131313), 2));
		data.degradationInfo.degradedServers.insert(NetworkAddress(IPAddress(0x13131313), 3));
		data.degradationInfo.degradedServers.insert(NetworkAddress(IPAddress(0x13131313), 4));
		data.degradationInfo.degradedServers.insert(NetworkAddress(IPAddress(0x13131313), 5));
		ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();

		// Trigger failover when satellite is degraded.
		data.degradationInfo.degradedSatellite = true;
		ASSERT(data.shouldTriggerFailoverDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();

		// No failover when satellite is degraded, but remote is not healthy.
		data.degradationInfo.degradedSatellite = true;
		data.degradationInfo.degradedServers.insert(remoteTlog);
		ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
		data.degradationInfo.disconnectedServers.insert(remoteTlog);
		ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();
	}
};

TEST_CASE("/fdbserver/clustercontroller/updateWorkerHealth") {
	wait(ClusterControllerTesting::updateWorkerHealth());
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/updateRecoveredWorkers") {
	ClusterControllerTesting::updateRecoveredWorkers();
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/getDegradationInfo") {
	ClusterControllerTesting::getDegradationInfo();
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/recentRecoveryCountDueToHealth") {
	ClusterControllerTesting::recentRecoveryCountDueToHealth();
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/shouldTriggerRecoveryDueToDegradedServers") {
	ClusterControllerTesting::shouldTriggerRecoveryDueToDegradedServers();
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/shouldTriggerFailoverDueToDegradedServers") {
	ClusterControllerTesting::shouldTriggerFailoverDueToDegradedServers();
	return Void();
}
