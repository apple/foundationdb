/*
 * HealthMonitor.h
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

#ifndef FDBRPC_HEALTH_MONITOR_H
#define FDBRPC_HEALTH_MONITOR_H

#include <deque>
#include <unordered_map>

#include <flow/flow.h>

class HealthMonitor {
public:
	void reportPeerClosed(const NetworkAddress& peerAddress);
	bool tooManyConnectionsClosed(const NetworkAddress& peerAddress);
	int closedConnectionsCount(const NetworkAddress& peerAddress);

private:
	void purgeOutdatedHistory();

	std::deque<std::pair<double, NetworkAddress>> peerClosedHistory;
	std::unordered_map<NetworkAddress, int> peerClosedNum;
};

#endif // FDBRPC_HEALTH_MONITOR_H
