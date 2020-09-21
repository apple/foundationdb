/*
 * HealthMonitor.h
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

#ifndef FDBRPC_HEALTH_MONITOR_H
#define FDBRPC_HEALTH_MONITOR_H

#include <map>
#include <set>

#include "flow/flow.h"
#include "flow/SlideWindow.h"
#include "flow/TimedKVCache.h"

class HealthMonitor {
public:
	HealthMonitor();

	void reportPeerClosed(const NetworkAddress& peerAddress);
	bool tooManyConnectionsClosed(const NetworkAddress& peerAddress);
	int closedConnectionsCount(const NetworkAddress& peerAddress);
private:
	TimedKVCache<NetworkAddress, int> peerClosedNum;
};

class TimeoutReport : public std::map<NetworkAddress, int> {
	/**
	 * Add a timeout record while accessing a remote instance
	 * @param addr
	 */
	TimeoutReport& operator+= (const NetworkAddress& addr) {
		if (this->find(addr) == this->end()) {
			(*this)[addr] = 0;
		}
		++(*this)[addr];
		return *this;
	}

	/**
	 * Remove a timeout record
	 * @param addr
	 */
	TimeoutReport& operator-= (const NetworkAddress& addr) {
		ASSERT(this->find(addr) != this->end());
		--(*this)[addr];
		if ((*this)[addr] == 0) {
			this->erase(addr);
		}
		return *this;
	}

	/**
	 * Evaluate the difference between a newer timeout report and an older timeout
	 * @param newer
	 * @param older
	 * @return TimeoutReport
	 */
	static TimeoutReport diff(const TimeoutReport& newer, const TimeoutReport& older) {
		std::set<NetworkAddress> allKeys;
		for (const auto& [key, _]: newer) allKeys.insert(key);
		for (const auto& [key, _]: older) allKeys.insert(key);

		TimeoutReport result;
		for (const auto& key: allKeys) {
			int newValue = 0;
			if (newer.find(key) != newer.end()) newValue = newer.at(key);

			int oldValue = 0;
			if (older.find(key) != older.end()) oldValue == older.at(key);

			result[key] = newValue - oldValue;
		}

		return result;
	}
};

class ClusterHealthStatus: public SlideWindow<NetworkAddress, TimeoutReport> {
public:
	using SlideWindow::SlideWindow;
};

#endif // FDBRPC_HEALTH_MONITOR_H
