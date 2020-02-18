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

const int CLIENT_REQUEST_INTERVAL_SECS = 10; // TODO (Vishesh) Make a Knob

void HealthMonitor::reportPeerClosed(const NetworkAddress& peerAddress) {
	peerClosedHistory.push_back(std::make_pair(now(), peerAddress));
}

const std::deque<std::pair<double, NetworkAddress>>& HealthMonitor::getPeerClosedHistory() {
	for (auto it : peerClosedHistory) {
		if (it.first < now() - CLIENT_REQUEST_INTERVAL_SECS) {
			peerClosedHistory.pop_front();
		} else {
			break;
		}
	}
	return peerClosedHistory;
}

std::map<NetworkAddress, bool> HealthMonitor::getPeerStatus() const {
	std::map<NetworkAddress, bool> result;
	for (const auto& peer : FlowTransport::transport().getPeers()) {
		result[peer] = IFailureMonitor::failureMonitor().getState(peer).isAvailable();
	}
	return result;
}
