/*
 * simulator.h
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

#ifndef FLOW_SIMULATOR_H
#define FLOW_SIMULATOR_H
#include "flow/ProtocolVersion.h"
#include <string>
#pragma once

#include "flow/flow.h"
#include "flow/Histogram.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/TDMetric.actor.h"
#include <random>
#include "fdbrpc/ReplicationPolicy.h"

enum ClogMode { ClogDefault, ClogAll, ClogSend, ClogReceive };

class ISimulator : public INetwork {
public:
	// Order matters!
	enum KillType {
		KillInstantly,
		InjectFaults,
		FailDisk,
		RebootAndDelete,
		RebootProcessAndDelete,
		Reboot,
		RebootProcess,
		None
	};

	// Order matters! all modes >= 2 are fault injection modes
	enum TSSMode { Disabled, EnabledNormal, EnabledAddDelay, EnabledDropMutations };

	enum class BackupAgentType { NoBackupAgents, WaitForType, BackupToFile, BackupToDB };

	// Subclasses may subclass ProcessInfo as well
	struct MachineInfo;

	struct ProcessInfo : NonCopyable {
		const char* name;
		const char* coordinationFolder;
		const char* dataFolder;
		MachineInfo* machine;
		NetworkAddressList addresses;
		NetworkAddress address;
		LocalityData locality;
		ProcessClass startingClass;
		TDMetricCollection tdmetrics;
		ChaosMetrics chaosMetrics;
		HistogramRegistry histograms;
		std::map<NetworkAddress, Reference<IListener>> listenerMap;
		std::map<NetworkAddress, Reference<IUDPSocket>> boundUDPSockets;
		bool failed;
		bool excluded;
		bool cleared;
		bool rebooting;
		std::vector<flowGlobalType> globals;

		INetworkConnections* network;

		uint64_t fault_injection_r;
		double fault_injection_p1, fault_injection_p2;
		bool failedDisk;

		UID uid;

		ProtocolVersion protocolVersion;

		ProcessInfo(const char* name,
		            LocalityData locality,
		            ProcessClass startingClass,
		            NetworkAddressList addresses,
		            INetworkConnections* net,
		            const char* dataFolder,
		            const char* coordinationFolder)
		  : name(name), coordinationFolder(coordinationFolder), dataFolder(dataFolder), machine(nullptr),
		    addresses(addresses), address(addresses.address), locality(locality), startingClass(startingClass),
		    failed(false), excluded(false), cleared(false), rebooting(false), network(net), fault_injection_r(0),
		    fault_injection_p1(0), fault_injection_p2(0), failedDisk(false) {
			uid = deterministicRandom()->randomUniqueID();
		}

		Future<KillType> onShutdown() { return shutdownSignal.getFuture(); }

		bool isReliable() const {
			return !failed && fault_injection_p1 == 0 && fault_injection_p2 == 0 && !failedDisk &&
			       (!machine || (machine->machineProcess->fault_injection_p1 == 0 &&
			                     machine->machineProcess->fault_injection_p2 == 0));
		}
		bool isAvailable() const { return !isExcluded() && isReliable(); }
		bool isExcluded() const { return excluded; }
		bool isCleared() const { return cleared; }
		std::string getReliableInfo() const {
			std::stringstream ss;
			ss << "failed:" << failed << " fault_injection_p1:" << fault_injection_p1
			   << " fault_injection_p2:" << fault_injection_p2;
			return ss.str();
		}

		// Return true if the class type is suitable for stateful roles, such as tLog and StorageServer.
		bool isAvailableClass() const {
			switch (startingClass._class) {
			case ProcessClass::UnsetClass:
				return true;
			case ProcessClass::StorageClass:
				return true;
			case ProcessClass::TransactionClass:
				return true;
			case ProcessClass::ResolutionClass:
				return false;
			case ProcessClass::CommitProxyClass:
				return false;
			case ProcessClass::GrvProxyClass:
				return false;
			case ProcessClass::MasterClass:
				return false;
			case ProcessClass::TesterClass:
				return false;
			case ProcessClass::StatelessClass:
				return false;
			case ProcessClass::LogClass:
				return true;
			case ProcessClass::LogRouterClass:
				return false;
			case ProcessClass::ClusterControllerClass:
				return false;
			case ProcessClass::DataDistributorClass:
				return false;
			case ProcessClass::RatekeeperClass:
				return false;
			case ProcessClass::BlobManagerClass:
				return false;
			case ProcessClass::StorageCacheClass:
				return false;
			case ProcessClass::BackupClass:
				return false;
			case ProcessClass::EncryptKeyProxyClass:
				return false;
			default:
				return false;
			}
		}

		Reference<IListener> getListener(const NetworkAddress& addr) const {
			auto listener = listenerMap.find(addr);
			ASSERT(listener != listenerMap.end());
			return listener->second;
		}

		inline flowGlobalType global(int id) const { return (globals.size() > id) ? globals[id] : nullptr; };
		inline void setGlobal(size_t id, flowGlobalType v) {
			globals.resize(std::max(globals.size(), id + 1));
			globals[id] = v;
		};

		std::string toString() const {
			return format(
			    "name: %s address: %s zone: %s datahall: %s class: %s excluded: %d cleared: %d",
			    name,
			    formatIpPort(addresses.address.ip, addresses.address.port).c_str(),
			    (locality.zoneId().present() ? locality.zoneId().get().printable().c_str() : "[unset]"),
			    (locality.dataHallId().present() ? locality.dataHallId().get().printable().c_str() : "[unset]"),
			    startingClass.toString().c_str(),
			    excluded,
			    cleared);
		}

		// Members not for external use
		Promise<KillType> shutdownSignal;
	};

	// A set of data associated with a simulated machine
	struct MachineInfo {
		ProcessInfo* machineProcess;
		std::vector<ProcessInfo*> processes;

		// A map from filename to file handle for all open files on a machine
		std::map<std::string, UnsafeWeakFutureReference<IAsyncFile>> openFiles;

		std::set<std::string> deletingFiles;
		std::set<std::string> closingFiles;
		Optional<Standalone<StringRef>> machineId;

		MachineInfo() : machineProcess(nullptr) {}
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
	                                ProtocolVersion protocol) = 0;
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
	// virtual KillType getMachineKillState( UID zoneID ) = 0;
	virtual bool canKillProcesses(std::vector<ProcessInfo*> const& availableProcesses,
	                              std::vector<ProcessInfo*> const& deadProcesses,
	                              KillType kt,
	                              KillType* newKillType) const = 0;
	virtual bool isAvailable() const = 0;
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

	void clearAddress(NetworkAddress const& address) {
		clearedAddresses[address]++;
		TraceEvent("ClearAddress").detail("Address", address).detail("Value", clearedAddresses[address]);
	}
	bool isCleared(NetworkAddress const& address) const {
		return clearedAddresses.find(address) != clearedAddresses.end();
	}

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
		return swapsDisabled.count(zoneId) == 0 && !allSwapsDisabled && !extraDB;
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
	virtual std::vector<ProcessInfo*> getAllProcesses() const = 0;
	virtual ProcessInfo* getProcessByAddress(NetworkAddress const& address) = 0;
	virtual MachineInfo* getMachineByNetworkAddress(NetworkAddress const& address) = 0;
	virtual MachineInfo* getMachineById(Optional<Standalone<StringRef>> const& machineId) = 0;
	void run() override {}
	virtual void destroyProcess(ProcessInfo* p) = 0;
	virtual void destroyMachine(Optional<Standalone<StringRef>> const& machineId) = 0;

	int desiredCoordinators;
	int physicalDatacenters;
	int processesPerMachine;
	int listenersPerProcess;
	std::set<NetworkAddress> protectedAddresses;
	std::map<NetworkAddress, ProcessInfo*> currentlyRebootingProcesses;
	std::unique_ptr<class ClusterConnectionString> extraDB;
	Reference<IReplicationPolicy> storagePolicy;
	Reference<IReplicationPolicy> tLogPolicy;
	int32_t tLogWriteAntiQuorum;
	Optional<Standalone<StringRef>> primaryDcId;
	Reference<IReplicationPolicy> remoteTLogPolicy;
	int32_t usableRegions;
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

	// Used by workloads that perform reconfigurations
	int testerCount;
	std::string connectionString;

	bool isStopped;
	double lastConnectionFailure;
	double connectionFailuresDisableDuration;
	bool speedUpSimulation;
	BackupAgentType backupAgents;
	BackupAgentType drAgents;
	bool restarted = false;

	bool hasDiffProtocolProcess; // true if simulator is testing a process with a different version
	bool setDiffProtocol; // true if a process with a different protocol version has been started

	bool allowStorageMigrationTypeChange = false;

	flowGlobalType global(int id) const final { return getCurrentProcess()->global(id); };
	void setGlobal(size_t id, flowGlobalType v) final { getCurrentProcess()->setGlobal(id, v); };

	void disableFor(const std::string& desc, double time) { disabledMap[desc] = time; }

	double checkDisabled(const std::string& desc) const {
		auto iter = disabledMap.find(desc);
		if (iter != disabledMap.end()) {
			return iter->second;
		}
		return 0;
	}

	static thread_local ProcessInfo* currentProcess;

	bool checkInjectedCorruption() {
		auto iter = corruptWorkerMap.find(currentProcess->address);
		if (iter != corruptWorkerMap.end())
			return iter->second;
		return false;
	}

	ISimulator();
	virtual ~ISimulator();

protected:
	Mutex mutex;

private:
	std::set<Optional<Standalone<StringRef>>> swapsDisabled;
	std::map<NetworkAddress, int> excludedAddresses;
	std::map<NetworkAddress, int> clearedAddresses;
	std::map<NetworkAddress, std::map<std::string, int>> roleAddresses;
	std::map<std::string, double> disabledMap;
	bool allSwapsDisabled;
};

// Quickly make existing code work that expects g_simulator to be of class type (not a pointer)
extern ISimulator* g_pSimulator;
#define g_simulator (*g_pSimulator)

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
