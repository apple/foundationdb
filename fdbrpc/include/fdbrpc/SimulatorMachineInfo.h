/*
 * SimulatorMachineInfo.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBRPC_SIMULATORMACHINEINFO_H
#define FDBRPC_SIMULATORMACHINEINFO_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "flow/Optional.h"

namespace simulator {

struct ProcessInfo;

// A set of data associated with a simulated machine
struct MachineInfo {
	ProcessInfo* machineProcess;
	std::vector<ProcessInfo*> processes;

	// A map from filename to file handle for all open files on a machine
	std::map<std::string, UnsafeWeakFutureReference<IAsyncFile>> openFiles;

	std::set<std::string> deletingOrClosingFiles;
	std::set<std::string> closingFiles;
	Optional<Standalone<StringRef>> machineId;

	const uint16_t remotePortStart;
	std::vector<uint16_t> usedRemotePorts;

	MachineInfo() : machineProcess(nullptr), remotePortStart(1000) {}

	short getRandomPort() {
		for (uint16_t i = remotePortStart; i < 60000; i++) {
			if (std::find(usedRemotePorts.begin(), usedRemotePorts.end(), i) == usedRemotePorts.end()) {
				TraceEvent(SevDebug, "RandomPortOpened").detail("PortNum", i);
				usedRemotePorts.push_back(i);
				return i;
			}
		}
		UNREACHABLE();
	}

	void removeRemotePort(uint16_t port) {
		if (port < remotePortStart)
			return;
		auto pos = std::find(usedRemotePorts.begin(), usedRemotePorts.end(), port);
		if (pos != usedRemotePorts.end()) {
			usedRemotePorts.erase(pos);
		}
	}
};

} // namespace simulator

#endif // FDBRPC_SIMULATORMACHINEINFO_H
