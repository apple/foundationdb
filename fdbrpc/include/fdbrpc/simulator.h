/*
 * simulator.h
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

#ifndef FLOW_SIMULATOR_H
#define FLOW_SIMULATOR_H
#pragma once
#include <algorithm>
#include <string>
#include <random>
#include <limits>

#include <boost/unordered_set.hpp>

#include "flow/flow.h"
#include "flow/Histogram.h"
#include "flow/ChaosMetrics.h"
#include "flow/ProtocolVersion.h"
#include "flow/WipedString.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Locality.h"
#include "flow/IAsyncFile.h"
#include "flow/TDMetric.actor.h"
#include "fdbrpc/HTTP.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbrpc/TokenSign.h"
#include "fdbrpc/SimulatorKillType.h"

enum ClogMode { ClogDefault, ClogAll, ClogSend, ClogReceive };

struct ValidationData {
	// global validation that missing refreshed feeds were previously destroyed
	std::unordered_set<std::string> allDestroyedChangeFeedIDs;
};

namespace simulator {
struct ProcessInfo;
struct MachineInfo;
} // namespace simulator

constexpr double DISABLE_CONNECTION_FAILURE_FOREVER = 1e6;

// Flip a random bit in the data for error injection ONLY in simulation.
extern void flip_bit(StringRef data, const char* file, int line);

extern Severity getBitFlipSeverityType();

#define INJECT_BIT_FLIP(data) flip_bit(data, __FILE__, __LINE__)

class ISimulator : public INetwork {

public:
	using KillType = simulator::KillType;
	using ProcessInfo = simulator::ProcessInfo;
	using MachineInfo = simulator::MachineInfo;

	// Order matters! all modes >= 2 are fault injection modes
	enum TSSMode { Disabled, EnabledNormal, EnabledAddDelay, EnabledDropMutations };

	enum class BackupAgentType { NoBackupAgents, WaitForType, BackupToFile, BackupToDB };
	enum class ExtraDatabaseMode { Disabled, LocalOrSingle, Single, Local, Multiple };

	static ExtraDatabaseMode stringToExtraDatabaseMode(std::string databaseMode) {
		if (databaseMode == "Disabled") {
			return ExtraDatabaseMode::Disabled;
		} else if (databaseMode == "LocalOrSingle") {
			return ExtraDatabaseMode::LocalOrSingle;
		} else if (databaseMode == "Single") {
			return ExtraDatabaseMode::Single;
		} else if (databaseMode == "Local") {
			return ExtraDatabaseMode::Local;
		} else if (databaseMode == "Multiple") {
			return ExtraDatabaseMode::Multiple;
		} else {
			TraceEvent(SevError, "UnknownExtraDatabaseMode").detail("DatabaseMode", databaseMode);
			ASSERT(false);
			throw internal_error();
		}
	};

	ProcessInfo* getProcess(Endpoint const& endpoint) { return getProcessByAddress(endpoint.getPrimaryAddress()); }
	ProcessInfo* getCurrentProcess() { return currentProcess; }
	ProcessInfo const* getCurrentProcess() const { return currentProcess; }

	// onProcess: wait for the process to be scheduled by the runloop; a task will be created for the process.
	virtual Future<Void> onProcess(ISimulator::ProcessInfo* process, TaskPriority taskID = TaskPriority::Zero) = 0;
	virtual Future<Void> onMachine(ISimulator::ProcessInfo* process, TaskPriority taskID = TaskPriority::Zero) = 0;

	virtual ProcessInfo* newProcess(const char* name,
	                                IPAddress ip,
	                                uint16_t port,
	                                bool sslEnabled,
	                                uint16_t listenPerProcess,
	                                LocalityData locality,
	                                ProcessClass startingClass,
	                                const char* dataFolder,
	                                const char* coordinationFolder,
	                                ProtocolVersion protocol,
	                                bool drProcess) = 0;
	virtual void killProcess(ProcessInfo* machine, KillType) = 0;
	virtual void rebootProcess(Optional<Standalone<StringRef>> zoneId, bool allProcesses) = 0;
	virtual void rebootProcess(ProcessInfo* process, KillType kt) = 0;
	virtual void killInterface(NetworkAddress address, KillType) = 0;
	virtual bool killMachine(Optional<Standalone<StringRef>> machineId,
	                         KillType kt,
	                         bool forceKill = false,
	                         KillType* ktFinal = nullptr) = 0;
	virtual bool killZone(Optional<Standalone<StringRef>> zoneId,
	                      KillType kt,
	                      bool forceKill = false,
	                      KillType* ktFinal = nullptr) = 0;
	virtual bool killDataCenter(Optional<Standalone<StringRef>> dcId,
	                            KillType kt,
	                            bool forceKill = false,
	                            KillType* ktFinal = nullptr) = 0;
	virtual bool killDataHall(Optional<Standalone<StringRef>> dcId,
	                          KillType kt,
	                          bool forceKill = false,
	                          KillType* ktFinal = nullptr) = 0;
	virtual bool killAll(KillType kt, bool forceKill = false, KillType* ktFinal = nullptr) = 0;
	// virtual KillType getMachineKillState( UID zoneID ) = 0;
	virtual void processInjectBlobFault(ProcessInfo* machine, double failureRate) = 0;
	virtual void processStopInjectBlobFault(ProcessInfo* machine) = 0;
	virtual bool canKillProcesses(std::vector<ProcessInfo*> const& availableProcesses,
	                              std::vector<ProcessInfo*> const& deadProcesses,
	                              KillType kt,
	                              KillType* newKillType) const = 0;
	virtual bool isAvailable() const = 0;
	virtual std::vector<AddressExclusion> getAllAddressesInDCToExclude(Optional<Standalone<StringRef>> dcId) const = 0;
	virtual bool datacenterDead(Optional<Standalone<StringRef>> dcId) const = 0;
	virtual void displayWorkers() const;
	ProtocolVersion protocolVersion() const override = 0;
	void addRole(NetworkAddress const& address, std::string const& role) {
		roleAddresses[address][role]++;
		TraceEvent("RoleAdd")
		    .detail("Address", address)
		    .detail("Role", role)
		    .detail("NumRoles", roleAddresses[address].size())
		    .detail("Value", roleAddresses[address][role]);
	}

	void removeRole(NetworkAddress const& address, std::string const& role) {
		auto addressIt = roleAddresses.find(address);
		if (addressIt != roleAddresses.end()) {
			auto rolesIt = addressIt->second.find(role);
			if (rolesIt != addressIt->second.end()) {
				if (rolesIt->second > 1) {
					rolesIt->second--;
					TraceEvent("RoleRemove")
					    .detail("Address", address)
					    .detail("Role", role)
					    .detail("NumRoles", addressIt->second.size())
					    .detail("Value", rolesIt->second)
					    .detail("Result", "Decremented Role");
				} else {
					addressIt->second.erase(rolesIt);
					if (addressIt->second.size()) {
						TraceEvent("RoleRemove")
						    .detail("Address", address)
						    .detail("Role", role)
						    .detail("NumRoles", addressIt->second.size())
						    .detail("Value", 0)
						    .detail("Result", "Removed Role");
					} else {
						roleAddresses.erase(addressIt);
						TraceEvent("RoleRemove")
						    .detail("Address", address)
						    .detail("Role", role)
						    .detail("NumRoles", 0)
						    .detail("Value", 0)
						    .detail("Result", "Removed Address");
					}
				}
			} else {
				TraceEvent(SevWarn, "RoleRemove")
				    .detail("Address", address)
				    .detail("Role", role)
				    .detail("Result", "Role Missing");
			}
		} else {
			TraceEvent(SevWarn, "RoleRemove")
			    .detail("Address", address)
			    .detail("Role", role)
			    .detail("Result", "Address Missing");
		}
	}

	std::string getRoles(NetworkAddress const& address, bool skipWorkers = true) const {
		auto addressIt = roleAddresses.find(address);
		std::string roleText;
		if (addressIt != roleAddresses.end()) {
			for (auto& roleIt : addressIt->second) {
				if ((!skipWorkers) || (roleIt.first != "Worker"))
					roleText += roleIt.first + ((roleIt.second > 1) ? format("-%d ", roleIt.second) : " ");
			}
		}
		if (roleText.empty())
			roleText = "[unset]";
		return roleText;
	}

	bool hasRole(NetworkAddress const& address, std::string const& role) const {
		auto addressIt = roleAddresses.find(address);
		if (addressIt != roleAddresses.end()) {
			auto rolesIt = addressIt->second.find(role);
			if (rolesIt != addressIt->second.end()) {
				return true;
			}
		}
		return false;
	}

	void clearAddress(NetworkAddress const& address) {
		clearedAddresses[address]++;
		TraceEvent("ClearAddress").detail("Address", address).detail("Value", clearedAddresses[address]);
	}
	bool isCleared(NetworkAddress const& address) const {
		return clearedAddresses.find(address) != clearedAddresses.end();
	}

	void switchCluster(NetworkAddress const& address) { switchedCluster[address] = !switchedCluster[address]; }
	bool hasSwitchedCluster(NetworkAddress const& address) const {
		return switchedCluster.find(address) != switchedCluster.end() ? switchedCluster.at(address) : false;
	}
	void toggleGlobalSwitchCluster() { globalSwitchedCluster = !globalSwitchedCluster; }
	bool globalHasSwitchedCluster() const { return globalSwitchedCluster; }

	void excludeAddress(NetworkAddress const& address) {
		excludedAddresses[address]++;
		TraceEvent("ExcludeAddress").detail("Address", address).detail("Value", excludedAddresses[address]);
	}

	void includeAddress(NetworkAddress const& address) {
		auto addressIt = excludedAddresses.find(address);
		if (addressIt != excludedAddresses.end()) {
			if (addressIt->second > 1) {
				addressIt->second--;
				TraceEvent("IncludeAddress")
				    .detail("Address", address)
				    .detail("Value", addressIt->second)
				    .detail("Result", "Decremented");
			} else {
				excludedAddresses.erase(addressIt);
				TraceEvent("IncludeAddress").detail("Address", address).detail("Value", 0).detail("Result", "Removed");
			}
		} else {
			TraceEvent(SevWarn, "IncludeAddress").detail("Address", address).detail("Result", "Missing");
		}
	}
	void includeAllAddresses() {
		TraceEvent("IncludeAddressAll").detail("AddressTotal", excludedAddresses.size());
		excludedAddresses.clear();
	}
	bool isExcluded(NetworkAddress const& address) const {
		return excludedAddresses.find(address) != excludedAddresses.end();
	}

	void disableSwapToMachine(Optional<Standalone<StringRef>> zoneId) { swapsDisabled.insert(zoneId); }
	void enableSwapToMachine(Optional<Standalone<StringRef>> zoneId) {
		swapsDisabled.erase(zoneId);
		allSwapsDisabled = false;
	}
	bool canSwapToMachine(Optional<Standalone<StringRef>> zoneId) const {
		return swapsDisabled.count(zoneId) == 0 && !allSwapsDisabled && extraDatabases.empty();
	}
	void enableSwapsToAll() {
		swapsDisabled.clear();
		allSwapsDisabled = false;
	}
	void disableSwapsToAll() {
		swapsDisabled.clear();
		allSwapsDisabled = true;
	}

	virtual void clogInterface(const IPAddress& ip, double seconds, ClogMode mode = ClogDefault) = 0;
	virtual void clogPair(const IPAddress& from, const IPAddress& to, double seconds) = 0;
	virtual void unclogPair(const IPAddress& from, const IPAddress& to) = 0;
	virtual void disconnectPair(const IPAddress& from, const IPAddress& to, double seconds) = 0;
	virtual void reconnectPair(const IPAddress& from, const IPAddress& to) = 0;
	virtual std::vector<ProcessInfo*> getAllProcesses() const = 0;
	virtual ProcessInfo* getProcessByAddress(NetworkAddress const& address) = 0;
	virtual MachineInfo* getMachineByNetworkAddress(NetworkAddress const& address) = 0;
	virtual MachineInfo* getMachineById(Optional<Standalone<StringRef>> const& machineId) = 0;
	void run() override {}
	virtual void destroyProcess(ProcessInfo* p) = 0;
	virtual void destroyMachine(Optional<Standalone<StringRef>> const& machineId) = 0;

	virtual void addSimHTTPProcess(Reference<HTTP::SimServerContext> serverContext) = 0;
	virtual void removeSimHTTPProcess() = 0;
	virtual Future<Void> registerSimHTTPServer(std::string hostname,
	                                           std::string service,
	                                           Reference<HTTP::IRequestHandler> requestHandler) = 0;

	int desiredCoordinators;
	int physicalDatacenters;
	int processesPerMachine;
	int listenersPerProcess;

	// We won't kill machines in this set, but we might reboot
	// them.  This is a conservative mechanism to prevent the
	// simulator from killing off important processes and rendering
	// the cluster unrecoverable, e.g. a quorum of coordinators.
	std::set<NetworkAddress> protectedAddresses;

	std::map<NetworkAddress, ProcessInfo*> currentlyRebootingProcesses;
	std::vector<std::string> extraDatabases;
	Reference<IReplicationPolicy> storagePolicy;
	Reference<IReplicationPolicy> tLogPolicy;
	int32_t tLogWriteAntiQuorum;
	Optional<Standalone<StringRef>> primaryDcId;
	Reference<IReplicationPolicy> remoteTLogPolicy;
	int32_t usableRegions;
	bool quiesced = false;
	std::string disablePrimary;
	std::string disableRemote;
	std::string originalRegions;
	std::string startingDisabledConfiguration;
	bool allowLogSetKills;
	Optional<Standalone<StringRef>> remoteDcId;
	bool hasSatelliteReplication;
	Reference<IReplicationPolicy> satelliteTLogPolicy;
	Reference<IReplicationPolicy> satelliteTLogPolicyFallback;
	int32_t satelliteTLogWriteAntiQuorum;
	int32_t satelliteTLogWriteAntiQuorumFallback;
	std::vector<Optional<Standalone<StringRef>>> primarySatelliteDcIds;
	std::vector<Optional<Standalone<StringRef>>> remoteSatelliteDcIds;
	TSSMode tssMode;
	std::map<NetworkAddress, bool> corruptWorkerMap;
	ConfigDBType configDBType;
	bool blobGranulesEnabled;

	// Used by workloads that perform reconfigurations
	int testerCount;
	std::string connectionString;

	bool isStopped;
	double lastConnectionFailure;
	double connectionFailuresDisableDuration;
	bool speedUpSimulation;
	double connectionFailureEnableTime; // Last time connection failure is enabled.
	bool disableTLogRecoveryFinish;
	BackupAgentType backupAgents;
	BackupAgentType drAgents;
	bool willRestart = false;
	bool restarted = false;
	bool isConsistencyChecked = false;
	ValidationData validationData;

	bool hasDiffProtocolProcess; // true if simulator is testing a process with a different version
	bool setDiffProtocol; // true if a process with a different protocol version has been started

	bool allowStorageMigrationTypeChange = false;
	double injectTargetedSSRestartTime = std::numeric_limits<double>::max();
	double injectSSDelayTime = std::numeric_limits<double>::max();
	double injectTargetedBMRestartTime = std::numeric_limits<double>::max();
	double injectTargetedBWRestartTime = std::numeric_limits<double>::max();

	enum SimConsistencyScanState {
		DisabledStart = 0,
		Enabling = 1,
		Enabled = 2,
		Enabled_InjectCorruption = 3,
		Enabled_FoundCorruption = 4,
		Complete = 5,
		DisabledEnd = 6
	};
	SimConsistencyScanState consistencyScanState = SimConsistencyScanState::DisabledStart;

	// check that validates that the state transition is valid
	bool updateConsistencyScanState(SimConsistencyScanState expectedCurrent, SimConsistencyScanState desired) {
		if (consistencyScanState == expectedCurrent && desired > consistencyScanState) {
			consistencyScanState = desired;

			if (desired == SimConsistencyScanState::Enabled_FoundCorruption) {
				// reset other metadata
				consistencyScanInjectedCorruptionType = {};
				consistencyScanCorruptRequestKey = {};
				consistencyScanCorruptor = {};
			}

			return true;
		}
		return false;
	}

	// Inject corruption only in consistency scan reads to ensure scan finds it.
	enum SimConsistencyScanCorruptionType { FlipMoreFlag = 0, AddToEmpty = 1, RemoveLastRow = 2, ChangeFirstValue = 3 };
	Optional<SimConsistencyScanCorruptionType> consistencyScanInjectedCorruptionType;
	Optional<UID> consistencyScanInjectedCorruptionDestination;
	Optional<bool> doInjectConsistencyScanCorruption;
	Optional<Standalone<StringRef>> consistencyScanCorruptRequestKey;
	Optional<std::pair<UID, NetworkAddress>> consistencyScanCorruptor;

	std::unordered_map<Standalone<StringRef>, PrivateKey> authKeys;

	std::set<std::pair<std::string, unsigned>> corruptedBlocks;

	// Valdiate at-rest encryption guarantees. If enabled, tests should inject a known 'marker' in Key and/or Values
	// inserted into FDB by the workload. On shutdown, all test generated files (under simfdb/) are scanned to find if
	// 'plaintext marker' is present.
	Optional<std::string> dataAtRestPlaintextMarker;

	std::unordered_map<std::string, Reference<HTTP::SimRegisteredHandlerContext>> httpHandlers;
	std::vector<std::pair<ProcessInfo*, Reference<HTTP::SimServerContext>>> httpServerProcesses;
	std::set<IPAddress> httpServerIps;
	int nextHTTPPort = 5000;
	bool httpProtected = false;

	flowGlobalType global(int id) const final;
	void setGlobal(size_t id, flowGlobalType v) final;

	void disableFor(const std::string& desc, double time);

	double checkDisabled(const std::string& desc) const;

	// generate authz token for use in simulation environment
	WipedString makeToken(int64_t tenantId, uint64_t ttlSecondsFromNow);

	static thread_local ProcessInfo* currentProcess;

	bool checkInjectedCorruption();

	ISimulator();
	virtual ~ISimulator();

	bool allowBitFlipInjection = false;
	std::map<std::string, int> bitFlipInjections;
	void enableBitFlipInjection() { allowBitFlipInjection = true; }
	bool isBitFlipInjectionEnabled() { return allowBitFlipInjection && !speedUpSimulation; }
	void disableBitFlipInjection() { allowBitFlipInjection = false; }
	void addBitFlipInjectionStats(const char* file, int line);
	bool isBitFlipInjected(const char* file, int line);
	bool isBitFlipInjected() { return !bitFlipInjections.empty(); }

protected:
	Mutex mutex;

private:
	std::set<Optional<Standalone<StringRef>>> swapsDisabled;
	std::map<NetworkAddress, int> excludedAddresses;
	std::map<NetworkAddress, int> clearedAddresses;
	std::map<NetworkAddress, bool> switchedCluster;
	bool globalSwitchedCluster = false;
	std::map<NetworkAddress, std::map<std::string, int>> roleAddresses;
	std::map<std::string, double> disabledMap;
	bool allSwapsDisabled;
};

extern ISimulator* g_simulator;

void startNewSimulator(bool printSimTime);

// Parameters used to simulate disk performance
struct DiskParameters : ReferenceCounted<DiskParameters> {
	double nextOperation;
	int64_t iops;
	int64_t bandwidth;

	DiskParameters(int64_t iops, int64_t bandwidth) : nextOperation(0), iops(iops), bandwidth(bandwidth) {}
};

// Simulates delays for performing operations on disk
extern Future<Void> waitUntilDiskReady(Reference<DiskParameters> parameters, int64_t size, bool sync = false);

// Enables connection failures, i.e., clogging, in simulation
void enableConnectionFailures(std::string const& context);

// Disables connection failures, i.e., clogging, in simulation
void disableConnectionFailures(std::string const& context);

class Sim2FileSystem : public IAsyncFileSystem {
public:
	// Opens a file for asynchronous I/O
	Future<Reference<class IAsyncFile>> open(const std::string& filename, int64_t flags, int64_t mode) override;

	// Deletes the given file. If mustBeDurable, returns only when the file is guaranteed to be deleted even after a
	// power failure.
	Future<Void> deleteFile(const std::string& filename, bool mustBeDurable) override;

	Future<std::time_t> lastWriteTime(const std::string& filename) override;

#ifdef ENABLE_SAMPLING
	ActorLineageSet& getActorLineageSet() override;
#endif

	Future<Void> renameFile(std::string const& from, std::string const& to) override;

	Sim2FileSystem() {}

	~Sim2FileSystem() override {}

	static void newFileSystem();

#ifdef ENABLE_SAMPLING
	ActorLineageSet actorLineageSet;
#endif
};

#endif
