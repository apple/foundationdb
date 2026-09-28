/*
 * SimulatorMachineInfo.h
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

#ifndef FDBRPC_SIMULATORMACHINEINFO_H
#define FDBRPC_SIMULATORMACHINEINFO_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "flow/Optional.h"
#include "flow/IAsyncFile.h"
#include "flow/network.h"

namespace simulator {

struct ProcessInfo;

// A set of data associated with a simulated machine
struct MachineInfo {
	ProcessInfo* machineProcess;
	std::vector<ProcessInfo*> processes;

	struct OpenFile {
		UnsafeWeakFutureReference<IAsyncFile> file;
		NetworkAddress openedBy;
	};

	// Every handle open on a path, at most one per process.
	//
	// An AsyncFileNonDurable's operations complete on Sim2 tasks belonging to the process that
	// opened it, so a handle may only be reused by that process: resuming another process's waiter
	// on it migrates that process's coroutine chain onto the opener. Processes on a machine share a
	// disk, not open file objects.
	//
	// All of a path's handles stay listed here, because machine-wide operations act on the disk
	// rather than on one process: killing a machine must corrupt every in-flight write to a path,
	// and deleting a file must invalidate every handle to it.
	//
	// A path drops out entirely once its last handle does, never lingering as an empty vector, so a
	// key present here means some process still holds that path open.
	std::map<std::string, std::vector<OpenFile>> openFiles;

	// openedBy's handle on filename, or nullptr if it has none.
	OpenFile* getOpenFile(std::string const& filename, NetworkAddress const& openedBy) {
		auto itr = openFiles.find(filename);
		if (itr == openFiles.end()) {
			return nullptr;
		}
		for (auto& handle : itr->second) {
			if (handle.openedBy == openedBy) {
				return &handle;
			}
		}
		return nullptr;
	}

	void eraseOpenFile(std::string const& filename, NetworkAddress const& openedBy) {
		auto itr = openFiles.find(filename);
		if (itr == openFiles.end()) {
			return;
		}
		std::erase_if(itr->second, [&openedBy](OpenFile const& handle) { return handle.openedBy == openedBy; });
		if (itr->second.empty()) {
			openFiles.erase(itr);
		}
	}

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
