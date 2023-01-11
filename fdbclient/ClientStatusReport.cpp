/*
 * ClientStatusReport.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "fdbclient/json_spirit/json_spirit_writer_template.h"
#include <set>

namespace {

class ClientReportGenerator {
public:
	ClientReportGenerator(DatabaseContext& cx) : cx(cx) {}

	Standalone<StringRef> generateReport() {
		if (cx.isError()) {
			statusObj["InitializationError"] = cx.deferredError.code();
		} else {
			reportCoordinators(cx.connectionRecord->get());
			reportClientInfo(cx.clientInfo->get());
			if (cx.coordinator->get().present()) {
				statusObj["CurrentCoordinator"] = cx.coordinator->get().get().getAddressString();
			}
			reportConnections();
		}
		return StringRef(json_spirit::write_string(json_spirit::mValue(statusObj)));
	}

private:
	void reportCoordinators(Reference<IClusterConnectionRecord> connRecord) {
		ClusterConnectionString cs = connRecord->getConnectionString();
		json_spirit::mArray coordArray;
		for (const auto& hostName : cs.hostnames) {
			coordArray.push_back(hostName.toString());
		}
		for (const auto& addr : cs.coords) {
			coordArray.push_back(addr.toString());
			serverAddresses.insert(addr);
		}
		statusObj["Coordinators"] = coordArray;
	}

	void reportClientInfo(const ClientDBInfo& clientInfo) {
		statusObj["ClusterID"] = clientInfo.clusterId.toString();
		json_spirit::mArray grvProxyArr;
		for (auto& grvProxy : clientInfo.grvProxies) {
			serverAddresses.insert(grvProxy.address());
			grvProxyArr.push_back(grvProxy.address().toString());
		}
		statusObj["GrvProxies"] = grvProxyArr;
		json_spirit::mArray commitProxyArr;
		for (auto& commitProxy : clientInfo.commitProxies) {
			serverAddresses.insert(commitProxy.address());
			commitProxyArr.push_back(commitProxy.address().toString());
		}
		statusObj["CommitProxies"] = commitProxyArr;
	}

	void reportConnections() {
		json_spirit::mArray connectionArr;
		for (auto& addr : serverAddresses) {
			connectionArr.push_back(connectionStatusReport(addr));
		}
		statusObj["Connections"] = connectionArr;
	}

	json_spirit::mObject connectionStatusReport(const NetworkAddress& address) {
		json_spirit::mObject connStatus;
		connStatus["Address"] = address.toString();

		auto& peers = FlowTransport::transport().getAllPeers();
		auto peerIter = peers.find(address);

		FlowTransport::transport().healthMonitor()->getRecentClosedPeers();
		bool failed = IFailureMonitor::failureMonitor().getState(address).isFailed();
		if (failed) {
			connStatus["Status"] = "failed";
		} else if (peerIter == peers.end()) {
			connStatus["Status"] = "disconnected";
		} else {
			connStatus["Status"] = peerIter->second->connected ? "connected" : "connecting";
		}
		if (peerIter != peers.end()) {
			auto peer = peerIter->second;
			double currTime = now();
			connStatus["ConnectFailedCount"] = peer->connectFailedCount;
			connStatus["Compatible"] = peer->compatible;
			connStatus["LastConnectTime"] = currTime - peer->lastConnectTime;
			connStatus["PingCount"] = peer->pingLatencies.getPopulationSize();
			connStatus["PingTimeoutCount"] = peer->timeoutCount;
			auto lastLoggedTime = (peer->lastLoggedTime > 0.0) ? peer->lastLoggedTime : peer->lastConnectTime;
			connStatus["BytesSampleTime"] = currTime - lastLoggedTime;
			connStatus["BytesReceived"] = peer->bytesReceived;
			connStatus["BytesSent"] = peer->bytesSent;
			auto protocolVersion = peer->protocolVersion->get();
			if (protocolVersion.present()) {
				connStatus["ProtocolVersion"] = format("%llx", protocolVersion.get().version());
			}
		}

		return connStatus;
	}

	DatabaseContext& cx;
	json_spirit::mObject statusObj;
	std::set<NetworkAddress> serverAddresses;
};

} // namespace

// Get client-side status information
Standalone<StringRef> DatabaseContext::getClientStatus() {
	ClientReportGenerator generator(*this);
	return generator.generateReport();
}