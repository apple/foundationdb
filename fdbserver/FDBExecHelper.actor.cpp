/*
 * FDBExecHelper.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#if !defined(_WIN32) && !defined(__APPLE__) && !defined(__INTEL_COMPILER)
#ifndef BOOST_SYSTEM_NO_LIB
#define BOOST_SYSTEM_NO_LIB
#endif
#ifndef BOOST_DATE_TIME_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#endif
#ifndef BOOST_REGEX_NO_LIB
#define BOOST_REGEX_NO_LIB
#endif
#include <boost/process.hpp>
#endif
#include <boost/algorithm/string.hpp>

#include "flow/TLSConfig.actor.h"
#include "flow/Trace.h"
#include "flow/Platform.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/network.h"
#include "fdbrpc/FlowProcess.actor.h"
#include "fdbrpc/Net2FileSystem.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/WellKnownEndpoints.h"
#include "fdbclient/versions.h"
#include "fdbserver/CoroFlow.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RemoteIKeyValueStore.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

ExecCmdValueString::ExecCmdValueString(StringRef pCmdValueString) {
	cmdValueString = pCmdValueString;
	parseCmdValue();
}

void ExecCmdValueString::setCmdValueString(StringRef pCmdValueString) {
	// reset everything
	binaryPath = StringRef();

	// set the new cmdValueString
	cmdValueString = pCmdValueString;

	// parse it out
	parseCmdValue();
}

StringRef ExecCmdValueString::getCmdValueString() const {
	return cmdValueString.toString();
}

StringRef ExecCmdValueString::getBinaryPath() const {
	return binaryPath;
}

VectorRef<StringRef> ExecCmdValueString::getBinaryArgs() const {
	return binaryArgs;
}

void ExecCmdValueString::parseCmdValue() {
	StringRef param = this->cmdValueString;
	// get the binary path
	this->binaryPath = param.eat(" "_sr);

	// no arguments provided
	if (param == StringRef()) {
		return;
	}

	// extract the arguments
	while (param != StringRef()) {
		StringRef token = param.eat(" "_sr);
		this->binaryArgs.push_back(this->binaryArgs.arena(), token);
	}
	return;
}

void ExecCmdValueString::dbgPrint() const {
	auto te = TraceEvent("ExecCmdValueString");

	te.detail("CmdValueString", cmdValueString.toString());
	te.detail("BinaryPath", binaryPath.toString());

	int i = 0;
	for (auto elem : binaryArgs) {
		te.detail(format("Arg", ++i).c_str(), elem.toString());
	}
	return;
}

ACTOR void destroyChildProcess(Future<Void> parentSSClosed, ISimulator::ProcessInfo* childInfo, std::string message) {
	// This code path should be bug free
	wait(parentSSClosed);
	TraceEvent(SevDebug, message.c_str()).log();
	// This one is root cause for most failures, make sure it's okay to destroy
	g_simulator->destroyProcess(childInfo);
	// Explicitly reset the connection with the child process in case re-spawn very quickly
	FlowTransport::transport().resetConnection(childInfo->address);
}

ACTOR Future<int> spawnSimulated(std::vector<std::string> paramList,
                                 double maxWaitTime,
                                 bool isSync,
                                 double maxSimDelayTime,
                                 IClosable* parent) {
	state ISimulator::ProcessInfo* self = g_simulator->getCurrentProcess();
	state ISimulator::ProcessInfo* child;

	state std::string role;
	state std::string addr;
	state std::string flowProcessName;
	state Endpoint parentProcessEndpoint;
	state int i = 0;
	// fdbserver -r flowprocess --process-name ikvs --process-endpoint ip:port,token,id
	for (; i < paramList.size(); i++) {
		if (paramList.size() > i + 1) {
			// temporary args parser that only supports the flowprocess role
			if (paramList[i] == "-r") {
				role = paramList[i + 1];
			} else if (paramList[i] == "-p" || paramList[i] == "--public_address") {
				addr = paramList[i + 1];
			} else if (paramList[i] == "--process-name") {
				flowProcessName = paramList[i + 1];
			} else if (paramList[i] == "--process-endpoint") {
				state std::vector<std::string> addressArray;
				boost::split(addressArray, paramList[i + 1], [](char c) { return c == ','; });
				if (addressArray.size() != 3) {
					std::cerr << "Invalid argument, expected 3 elements in --process-endpoint got "
					          << addressArray.size() << std::endl;
					flushAndExit(FDB_EXIT_ERROR);
				}
				try {
					auto addr = NetworkAddress::parse(addressArray[0]);
					uint64_t fst = std::stoul(addressArray[1]);
					uint64_t snd = std::stoul(addressArray[2]);
					UID token(fst, snd);
					NetworkAddressList l;
					l.address = addr;
					parentProcessEndpoint = Endpoint(l, token);
				} catch (Error& e) {
					std::cerr << "Could not parse network address " << addressArray[0] << std::endl;
					flushAndExit(FDB_EXIT_ERROR);
				}
			}
		}
	}
	state int result = 0;
	child = g_simulator->newProcess(
	    "remote flow process",
	    self->address.ip,
	    0,
	    self->address.isTLS(),
	    self->addresses.secondaryAddress.present() ? 2 : 1,
	    self->locality,
	    ProcessClass(ProcessClass::UnsetClass, ProcessClass::AutoSource),
	    self->dataFolder.c_str(),
	    self->coordinationFolder.c_str(), // do we need to customize this coordination folder path?
	    self->protocolVersion,
	    false);
	wait(g_simulator->onProcess(child));
	state Future<ISimulator::KillType> onShutdown = child->onShutdown();
	state Future<ISimulator::KillType> parentShutdown = self->onShutdown();
	state Future<Void> flowProcessF;

	try {
		TraceEvent(SevDebug, "SpawnedChildProcess")
		    .detail("Child", child->toString())
		    .detail("Parent", self->toString());
		std::string role = "";
		std::string addr = "";
		for (int i = 0; i < paramList.size(); i++) {
			if (paramList.size() > i + 1 && paramList[i] == "-r") {
				role = paramList[i + 1];
			}
		}
		if (role == "flowprocess" && !parentShutdown.isReady()) {
			self->childs.push_back(child);
			state Future<Void> parentSSClosed = parent->onClosed();
			FlowTransport::createInstance(false, 1, WLTOKEN_RESERVED_COUNT);
			FlowTransport::transport().bind(child->address, child->address);
			Sim2FileSystem::newFileSystem();
			ProcessFactory<KeyValueStoreProcess>(flowProcessName.c_str());
			flowProcessF = runFlowProcess(flowProcessName, parentProcessEndpoint);

			choose {
				when(wait(flowProcessF)) {
					TraceEvent(SevDebug, "ChildProcessKilled").log();
					wait(g_simulator->onProcess(self));
					TraceEvent(SevDebug, "BackOnParentProcess").detail("Result", std::to_string(result));
					destroyChildProcess(parentSSClosed, child, "StorageServerReceivedClosedMessage");
				}
				when(wait(success(onShutdown))) {
					ASSERT(false);
					// In prod, we use prctl to bind parent and child processes to die together
					// In simulation, we simply disable killing parent or child processes as we cannot use the same
					// mechanism here
				}
				when(wait(success(parentShutdown))) {
					ASSERT(false);
					// Parent process is not killed, see above
				}
			}
		} else {
			ASSERT(false);
		}
	} catch (Error& e) {
		TraceEvent(SevError, "RemoteIKVSDied").errorUnsuppressed(e);
		result = -1;
	}

	return result;
}

#if defined(_WIN32) || defined(__APPLE__) || defined(__INTEL_COMPILER)
ACTOR Future<int> spawnProcess(std::string binPath,
                               std::vector<std::string> paramList,
                               double maxWaitTime,
                               bool isSync,
                               double maxSimDelayTime,
                               IClosable* parent) {
	if (g_network->isSimulated() && getExecPath() == binPath) {
		int res = wait(spawnSimulated(paramList, maxWaitTime, isSync, maxSimDelayTime, parent));
		return res;
	}
	wait(delay(0.0));
	return 0;
}
#else

static auto fork_child(const std::string& path, std::vector<char*>& paramList) {
	int pipefd[2];
	if (pipe(pipefd) != 0) {
		return std::make_pair(-1, Optional<int>{});
	}
	auto readFD = pipefd[0];
	auto writeFD = pipefd[1];
	pid_t pid = fork();
	if (pid == -1) {
		close(readFD);
		close(writeFD);
		return std::make_pair(-1, Optional<int>{});
	}
	if (pid == 0) {
		close(readFD);
		dup2(writeFD, 1); // stdout
		dup2(writeFD, 2); // stderr
		close(writeFD);
		execv(&path[0], &paramList[0]);
		_exit(EXIT_FAILURE);
	}
	close(writeFD);
	return std::make_pair(pid, Optional<int>{ readFD });
}

static void setupTraceWithOutput(TraceEvent& event, size_t bytesRead, char* outputBuffer) {
	// get some errors printed for spawned process
	std::cout << "Output bytesRead: " << bytesRead << std::endl;
	std::cout << "output buffer: " << std::string_view(outputBuffer, bytesRead) << std::endl;
	if (bytesRead == 0)
		return;
	ASSERT(bytesRead <= SERVER_KNOBS->MAX_FORKED_PROCESS_OUTPUT);
	auto extraBytesNeeded = std::max<int>(bytesRead - event.getMaxFieldLength(), 0);
	event.setMaxFieldLength(event.getMaxFieldLength() + extraBytesNeeded);
	event.setMaxEventLength(event.getMaxEventLength() + extraBytesNeeded);
	outputBuffer[bytesRead - 1] = '\0';
	event.detail("Output", std::string(outputBuffer));
}

ACTOR Future<int> spawnProcess(std::string path,
                               std::vector<std::string> args,
                               double maxWaitTime,
                               bool isSync,
                               double maxSimDelayTime,
                               IClosable* parent) {
	if (g_network->isSimulated() && getExecPath() == path) {
		int res = wait(spawnSimulated(args, maxWaitTime, isSync, maxSimDelayTime, parent));
		return res;
	}
	// for async calls in simulator, always delay by a deterministic amount of time and then
	// do the call synchronously, otherwise the predictability of the simulator breaks
	if (!isSync && g_network->isSimulated()) {
		double snapDelay = std::max(maxSimDelayTime - 1, 0.0);
		// add some randomness
		snapDelay += deterministicRandom()->random01();
		TraceEvent("SnapDelaySpawnProcess").detail("SnapDelay", snapDelay);
		wait(delay(snapDelay));
	}

	std::vector<char*> paramList;
	paramList.reserve(args.size());
	for (int i = 0; i < args.size(); i++) {
		paramList.push_back(&args[i][0]);
	}
	paramList.push_back(nullptr);

	state std::string allArgs;
	for (int i = 0; i < args.size(); i++) {
		if (i > 0)
			allArgs += " ";
		allArgs += args[i];
	}

	state std::pair<pid_t, Optional<int>> pidAndReadFD = fork_child(path, paramList);
	state pid_t pid = pidAndReadFD.first;
	state Optional<int> readFD = pidAndReadFD.second;
	if (pid == -1) {
		TraceEvent(SevWarnAlways, "SpawnProcessFailure")
		    .detail("Reason", "Command failed to spawn")
		    .detail("Cmd", path)
		    .detail("Args", allArgs);
		return -1;
	} else if (pid > 0) {
		state int status = -1;
		state double runTime = 0;
		state Arena arena;
		state char* outputBuffer = new (arena) char[SERVER_KNOBS->MAX_FORKED_PROCESS_OUTPUT];
		state size_t bytesRead = 0;
		int flags = fcntl(readFD.get(), F_GETFL, 0);
		fcntl(readFD.get(), F_SETFL, flags | O_NONBLOCK);
		while (true) {
			if (maxWaitTime >= 0 && runTime > maxWaitTime) {
				// timing out

				TraceEvent(SevWarnAlways, "SpawnProcessFailure")
				    .detail("Reason", "Command failed, timeout")
				    .detail("Cmd", path)
				    .detail("Args", allArgs);
				return -1;
			}
			int err = waitpid(pid, &status, WNOHANG);
			loop {
				int bytes =
				    read(readFD.get(), &outputBuffer[bytesRead], SERVER_KNOBS->MAX_FORKED_PROCESS_OUTPUT - bytesRead);
				if (bytes < 0 && errno == EAGAIN)
					break;
				else if (bytes < 0)
					throw internal_error();
				else if (bytes == 0)
					break;
				bytesRead += bytes;
			}
			if (err < 0) {
				TraceEvent event(SevWarnAlways, "SpawnProcessFailure");
				setupTraceWithOutput(event, bytesRead, outputBuffer);
				event.detail("Reason", "Command failed")
				    .detail("Cmd", path)
				    .detail("Args", allArgs)
				    .detail("Errno", WIFEXITED(status) ? WEXITSTATUS(status) : -1);
				return -1;
			} else if (err == 0) {
				// child process has not completed yet
				if (isSync || g_network->isSimulated()) {
					// synchronously sleep
					threadSleep(0.1);
				} else {
					// yield for other actors to run
					wait(delay(0.1));
				}
				runTime += 0.1;
			} else {
				// child process completed
				if (!(WIFEXITED(status) && WEXITSTATUS(status) == 0)) {
					TraceEvent event(SevWarnAlways, "SpawnProcessFailure");
					setupTraceWithOutput(event, bytesRead, outputBuffer);
					event.detail("Reason", "Command failed")
					    .detail("Cmd", path)
					    .detail("Args", allArgs)
					    .detail("Errno", WIFEXITED(status) ? WEXITSTATUS(status) : -1);
					return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
				}
				TraceEvent event("SpawnProcessCommandStatus");
				setupTraceWithOutput(event, bytesRead, outputBuffer);
				event.detail("Cmd", path)
				    .detail("Args", allArgs)
				    .detail("Errno", WIFEXITED(status) ? WEXITSTATUS(status) : 0);
				return 0;
			}
		}
	}
	return -1;
}
#endif

ACTOR Future<int> execHelper(ExecCmdValueString* execArg, UID snapUID, std::string folder, std::string role) {
	state Standalone<StringRef> uidStr(snapUID.toString());
	state int err = 0;
	state Future<int> cmdErr;
	state double maxWaitTime = SERVER_KNOBS->SNAP_CREATE_MAX_TIMEOUT;
	if (!g_network->isSimulated()) {
		// get bin path
		auto snapBin = execArg->getBinaryPath();
		std::vector<std::string> paramList;
		paramList.push_back(snapBin.toString());
		// get user passed arguments
		auto listArgs = execArg->getBinaryArgs();
		for (auto elem : listArgs) {
			paramList.push_back(elem.toString());
		}
		// get additional arguments
		paramList.push_back("--path");
		paramList.push_back(folder);
		const char* version = FDB_VT_VERSION;
		paramList.push_back("--version");
		paramList.push_back(version);
		paramList.push_back("--role");
		paramList.push_back(role);
		paramList.push_back("--uid");
		paramList.push_back(uidStr.toString());
		cmdErr = spawnProcess(snapBin.toString(), paramList, maxWaitTime, false /*isSync*/, 0);
		wait(success(cmdErr));
		err = cmdErr.get();
	} else {
		// copy the files
		state std::string folderFrom = folder + "/.";
		state std::string folderTo = folder + "-snap-" + uidStr.toString() + "-" + role;
		std::vector<std::string> paramList;
		std::string mkdirBin = "/bin/mkdir";
		paramList.push_back(mkdirBin);
		paramList.push_back(folderTo);
		cmdErr = spawnProcess(mkdirBin, paramList, maxWaitTime, false /*isSync*/, 10.0);
		wait(success(cmdErr));
		err = cmdErr.get();
		if (err == 0) {
			std::vector<std::string> paramList;
			std::string cpBin = "/bin/cp";
			paramList.push_back(cpBin);
			paramList.push_back("-a");
			paramList.push_back(folderFrom);
			paramList.push_back(folderTo);
			cmdErr = spawnProcess(cpBin, paramList, maxWaitTime, true /*isSync*/, 1.0);
			wait(success(cmdErr));
			err = cmdErr.get();
		}
	}
	return err;
}

struct StorageVersionInfo {
	Version version;
	Version durableVersion;
};

// storage nodes get snapshotted through the worker interface which does not have context about version information,
// following info is gathered at worker level to facilitate printing of version info during storage snapshots.
typedef std::map<UID, StorageVersionInfo> UidStorageVersionInfo;

std::map<NetworkAddress, UidStorageVersionInfo> workerStorageVersionInfo;

void setDataVersion(UID uid, Version version) {
	NetworkAddress addr = g_network->getLocalAddress();
	workerStorageVersionInfo[addr][uid].version = version;
}

void setDataDurableVersion(UID uid, Version durableVersion) {
	NetworkAddress addr = g_network->getLocalAddress();
	workerStorageVersionInfo[addr][uid].durableVersion = durableVersion;
}

void printStorageVersionInfo() {
	NetworkAddress addr = g_network->getLocalAddress();
	for (auto itr = workerStorageVersionInfo[addr].begin(); itr != workerStorageVersionInfo[addr].end(); itr++) {
		TraceEvent("StorageVersionInfo")
		    .detail("UID", itr->first)
		    .detail("Version", itr->second.version)
		    .detail("DurableVersion", itr->second.durableVersion);
	}
}
