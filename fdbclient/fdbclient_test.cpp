/*
 * fdbclient_test.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/Status.h"
#include "fdbclient/WellKnownEndpoints.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbrpc/Net2FileSystem.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/simulator.h"
#include "flow/IConnection.h"
#include "flow/TLSConfig.h"
#include "flow/UnitTest.h"
#include "flow/UnitTestRunner.h"
#include "flow/genericactors.h"

AsyncResult<Optional<StatusObject>> clientCoordinatorsStatusFetcher(Reference<IClusterConnectionRecord> connRecord,
                                                                    bool* quorumReachable,
                                                                    int* coordinatorsFaultTolerance,
                                                                    double statusDeadline);

namespace {

class ScopedMockCoordinatorDNS {
public:
	ScopedMockCoordinatorDNS(std::string host, std::string service)
	  : host(std::move(host)), service(std::move(service)) {}
	~ScopedMockCoordinatorDNS() { INetworkConnections::net()->removeMockTCPEndpoint(host, service); }

	void setAddress(NetworkAddress address) {
		INetworkConnections::net()->removeMockTCPEndpoint(host, service);
		INetworkConnections::net()->addMockTCPEndpoint(host, service, { address });
	}

private:
	std::string host;
	std::string service;
};

TEST_CASE("/fdbclient/status/mixedCoordinatorReachability") {
	if (!g_network->isSimulated()) {
		co_return;
	}

	// The standalone test runner has no other coordinator endpoints on this process.
	PublicRequestStream<GetLeaderRequest> leaderRequests;
	leaderRequests.makeWellKnownEndpoint(WLTOKEN_CLIENTLEADERREG_GETLEADER, TaskPriority::Coordination);
	PublicRequestStream<ProtocolInfoRequest> protocolRequests;
	protocolRequests.makeWellKnownEndpoint(WLTOKEN_PROTOCOL_INFO, TaskPriority::DefaultEndpoint);

	const NetworkAddress reachable = FlowTransport::transport().getLocalAddress();
	const NetworkAddress unreachable = NetworkAddress::parse("198.18.0.1:1");
	const std::string host = "coordinator-status-test";
	const std::string service = "1";
	const std::string hostname = host + ":" + service;
	ScopedMockCoordinatorDNS dns(host, service);

	for (bool hostnameReachable : { true, false }) {
		dns.setAddress(hostnameReachable ? reachable : unreachable);
		auto connRecord = makeReference<ClusterConnectionMemoryRecord>(ClusterConnectionString(
		    "test:test@" + unreachable.toString() + "," + hostname + "," + reachable.toString()));
		bool quorumReachable = false;
		int faultTolerance = 0;
		auto statusFuture = clientCoordinatorsStatusFetcher(connRecord, &quorumReachable, &faultTolerance, now() + 2.0);

		const int expectedRequests = hostnameReachable ? 2 : 1;
		for (int i = 0; i < expectedRequests; ++i) {
			GetLeaderRequest req = co_await timeoutError(waitAndForward(leaderRequests.getFuture()), 1.0);
			ASSERT(req.key == "test:test"_sr);
			ASSERT(req.knownLeader == UID());
			req.reply.send(Optional<LeaderInfo>(LeaderInfo(UID(1, 2))));
		}
		for (int i = 0; i < expectedRequests; ++i) {
			ProtocolInfoRequest req = co_await timeoutError(waitAndForward(protocolRequests.getFuture()), 1.0);
			req.reply.send(ProtocolInfoReply{ g_network->protocolVersion() });
		}

		Optional<StatusObject> status = co_await statusFuture;
		ASSERT(status.present());
		ASSERT(quorumReachable == hostnameReachable);
		ASSERT_EQ(faultTolerance, hostnameReachable ? 0 : -1);
		const auto& coordinators = status.get().at("coordinators").get_array();
		ASSERT_EQ(coordinators.size(), 3);
		const std::vector<std::pair<std::string, bool>> expected = { { hostname, hostnameReachable },
			                                                         { unreachable.toString(), false },
			                                                         { reachable.toString(), true } };
		for (int i = 0; i < expected.size(); ++i) {
			const auto& coordinator = coordinators[i].get_obj();
			ASSERT_EQ(coordinator.at("address").get_str(), expected[i].first);
			ASSERT_EQ(coordinator.at("reachable").get_bool(), expected[i].second);
			ASSERT_EQ(coordinator.count("protocol") != 0, expected[i].second);
		}
	}
	co_return;
}

Future<Void> initializeSimulation() {
	resetClientKnobs(Randomize::True, IsSimulated::True);
	return startUnitTestSimulator(WLTOKEN_RESERVED_COUNT);
}

void initializeNetwork() {
	resetClientKnobs(Randomize::False, IsSimulated::False);
	g_network = newNet2(TLSConfig());
	g_network->addStopCallback(Net2FileSystem::stop);
	Net2FileSystem::newFileSystem();
	FlowTransport::createInstance(true, 1, WLTOKEN_RESERVED_COUNT);
	const NetworkAddress address = NetworkAddress::parse("127.0.0.1:0");
	FlowTransport::transport().bind(address, address);
}
} // namespace

int main(int argc, char** argv) {
	return runUnitTests(
	    argc,
	    argv,
	    UnitTestRunnerConfig(
	        "fdbclient",
	        initializeSimulation,
	        initializeNetwork,
	        { "/fdbclient/MonitorLeader/PartialResolve", "/backup/containers/url", "/backup/containers_list" }));
}
