/*
 * HealthMonitor.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbrpc/HealthMonitor.h"

void HealthMonitor::reportPeerClosed(const NetworkAddress& peerAddress) {
	purgeOutdatedHistory();
	peerClosedHistory.push_back(std::make_pair(now(), peerAddress));
	peerClosedNum[peerAddress] += 1;
}

void HealthMonitor::purgeOutdatedHistory() {
	for (auto it : peerClosedHistory) {
		if (it.first < now() - FLOW_KNOBS->HEALTH_MONITOR_CLIENT_REQUEST_INTERVAL_SECS) {
			peerClosedNum[it.second] -= 1;
			ASSERT(peerClosedNum[it.second] >= 0);
			peerClosedHistory.pop_front();
		} else {
			break;
		}
	}
}

const std::deque<std::pair<double, NetworkAddress>>& HealthMonitor::getPeerClosedHistory() {
	purgeOutdatedHistory();
	return peerClosedHistory;
}

std::map<NetworkAddress, bool> HealthMonitor::getPeerStatus() {
	purgeOutdatedHistory();
	std::map<NetworkAddress, bool> result;
	for (const auto& peer : FlowTransport::transport().getPeers()) {
		result[peer] = IFailureMonitor::failureMonitor().getState(peer).isAvailable();
	}
	return result;
}

bool HealthMonitor::tooManyConnectionsClosed(const NetworkAddress& peerAddress) {
	purgeOutdatedHistory();
	std::string history;
	for (const auto& peer : peerClosedHistory) {
		history += peer.second.toString() + " " + std::to_string(peer.first) + ", ";
	}
	return peerClosedNum[peerAddress] > FLOW_KNOBS->HEALTH_MONITOR_CONNECTION_MAX_CLOSED;
}
