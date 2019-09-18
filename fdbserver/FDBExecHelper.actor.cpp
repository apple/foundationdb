#if !defined(_WIN32) && !defined(__APPLE__)
#define BOOST_SYSTEM_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#define BOOST_REGEX_NO_LIB
#include <boost/process.hpp>
#endif
#include "fdbserver/FDBExecHelper.actor.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#if defined(CMAKE_BUILD) || !defined(_WIN32)
#include "versions.h"
#endif
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

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

StringRef ExecCmdValueString::getCmdValueString() {
	return cmdValueString.toString();
}

StringRef ExecCmdValueString::getBinaryPath() {
	return binaryPath;
}

VectorRef<StringRef> ExecCmdValueString::getBinaryArgs() {
	return binaryArgs;
}

void ExecCmdValueString::parseCmdValue() {
	StringRef param = this->cmdValueString;
	// get the binary path
	this->binaryPath = param.eat(LiteralStringRef(" "));

	// no arguments provided
	if (param == StringRef()) {
		return;
	}

	// extract the arguments
	while (param != StringRef()) {
		StringRef token = param.eat(LiteralStringRef(" "));
		this->binaryArgs.push_back(this->binaryArgs.arena(), token);
	}
	return;
}

void ExecCmdValueString::dbgPrint() {
	auto te = TraceEvent("ExecCmdValueString");

	te.detail("CmdValueString", cmdValueString.toString());
	te.detail("BinaryPath", binaryPath.toString());

	int i = 0;
	for (auto elem : binaryArgs) {
		te.detail(format("Arg", ++i).c_str(), elem.toString());
	}
	return;
}

#if defined(_WIN32) || defined(__APPLE__)
ACTOR Future<int> spawnProcess(std::string binPath, std::vector<std::string> paramList, double maxWaitTime, bool isSync, double maxSimDelayTime)
{
	wait(delay(0.0));
	return 0;
}
#else
ACTOR Future<int> spawnProcess(std::string binPath, std::vector<std::string> paramList, double maxWaitTime, bool isSync, double maxSimDelayTime)
{
	state std::string argsString;
	for (auto const& elem : paramList) {
		argsString += elem + ",";
	}
	TraceEvent("SpawnProcess").detail("Cmd", binPath).detail("Args", argsString);

	state int err = 0;
	state double runTime = 0;
	state boost::process::child c(binPath, boost::process::args(paramList),
								  boost::process::std_err > boost::process::null);

	// for async calls in simulator, always delay by a deterinistic amount of time and do the call
	// synchronously, otherwise the predictability of the simulator breaks
	if (!isSync && g_network->isSimulated()) {
		double snapDelay = std::max(maxSimDelayTime - 1, 0.0);
		// add some randomness
		snapDelay += deterministicRandom()->random01();
		TraceEvent("SnapDelaySpawnProcess")
			.detail("SnapDelay", snapDelay);
		wait(delay(snapDelay));
	}

	if (!isSync && !g_network->isSimulated()) {
		while (c.running() && runTime <= maxWaitTime) {
			wait(delay(0.1));
			runTime += 0.1;
		}
	} else {
		if (g_network->isSimulated()) {
			// to keep the simulator deterministic, wait till the process exits,
			// hence giving a large wait time
			c.wait_for(std::chrono::hours(24));
			ASSERT(!c.running());
		} else {
			int maxWaitTimeInt = static_cast<int>(maxWaitTime + 1.0);
			c.wait_for(std::chrono::seconds(maxWaitTimeInt));
		}
	}

	if (c.running()) {
		TraceEvent(SevWarnAlways, "ChildTermination")
				.detail("Cmd", binPath)
				.detail("Args", argsString);
		c.terminate();
		err = -1;
		if (!c.wait_for(std::chrono::seconds(1))) {
			TraceEvent(SevWarnAlways, "SpawnProcessFailedToExit")
				.detail("Cmd", binPath)
				.detail("Args", argsString);
		}
	} else {
		err = c.exit_code();
	}
	TraceEvent("SpawnProcess")
		.detail("Cmd", binPath)
		.detail("Error", err);
	return err;
}
#endif

ACTOR Future<int> execHelper(ExecCmdValueString* execArg, UID snapUID, std::string folder, std::string role) {
	state Standalone<StringRef> uidStr = snapUID.toString();
	state int err = 0;
	state Future<int> cmdErr;
	state double maxWaitTime = SERVER_KNOBS->SNAP_CREATE_MAX_TIMEOUT;
	if (!g_network->isSimulated()) {
		// get bin path
		auto snapBin = execArg->getBinaryPath();
		std::vector<std::string> paramList;
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
		state std::string folderTo = folder + "-snap-" + uidStr.toString();
		double maxSimDelayTime = 10.0;
		folderTo = folder + "-snap-" + uidStr.toString() + "-" + role;
		std::vector<std::string> paramList;
		std::string mkdirBin = "/bin/mkdir";
		paramList.push_back(folderTo);
		cmdErr = spawnProcess(mkdirBin, paramList, maxWaitTime, false /*isSync*/, maxSimDelayTime);
		wait(success(cmdErr));
		err = cmdErr.get();
		if (err == 0) {
			std::vector<std::string> paramList;
			std::string cpBin = "/bin/cp";
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

std::map<NetworkAddress, std::set<UID>> execOpsInProgress;

bool isExecOpInProgress(UID execUID) {
	NetworkAddress addr = g_network->getLocalAddress();
	return (execOpsInProgress[addr].find(execUID) != execOpsInProgress[addr].end());
}

void setExecOpInProgress(UID execUID) {
	NetworkAddress addr = g_network->getLocalAddress();
	ASSERT(execOpsInProgress[addr].find(execUID) == execOpsInProgress[addr].end());
	execOpsInProgress[addr].insert(execUID);
	return;
}

void clearExecOpInProgress(UID execUID) {
	NetworkAddress addr = g_network->getLocalAddress();
	ASSERT(execOpsInProgress[addr].find(execUID) != execOpsInProgress[addr].end());
	execOpsInProgress[addr].erase(execUID);
	return;
}

std::map<NetworkAddress, std::set<UID>> tLogsAlive;

void registerTLog(UID uid) {
	NetworkAddress addr = g_network->getLocalAddress();
	tLogsAlive[addr].insert(uid);
}
void unregisterTLog(UID uid) {
	NetworkAddress addr = g_network->getLocalAddress();
	if (tLogsAlive[addr].find(uid) != tLogsAlive[addr].end()) {
		tLogsAlive[addr].erase(uid);
	}
}
bool isTLogInSameNode() {
	NetworkAddress addr = g_network->getLocalAddress();
	return tLogsAlive[addr].size() >= 1;
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
