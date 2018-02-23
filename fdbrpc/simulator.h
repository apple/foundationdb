/*
 * simulator.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/flow.h"
#include "FailureMonitor.h"
#include "Locality.h"
#include "IAsyncFile.h"
#include "flow/TDMetric.actor.h"
#include <random>
#include "fdbrpc/ReplicationPolicy.h"

enum ClogMode { ClogDefault, ClogAll, ClogSend, ClogReceive };

class ISimulator : public INetwork {
public:
	ISimulator() : killedMachines(0), killableMachines(0), machinesNeededForProgress(3), neededDatacenters(1), killableDatacenters(0), killedDatacenters(0), maxCoordinatorsInDatacenter(0), desiredCoordinators(1), processesPerMachine(0), isStopped(false), lastConnectionFailure(0), connectionFailuresDisableDuration(0), speedUpSimulation(false), allSwapsDisabled(false), backupAgents(WaitForType), extraDB(NULL) {}

	// Order matters!
	enum KillType { None, KillInstantly, InjectFaults, RebootAndDelete, Reboot, RebootProcessAndDelete, RebootProcess };

	enum BackupAgentType { NoBackupAgents, WaitForType, BackupToFile, BackupToDB };

	// Subclasses may subclass ProcessInfo as well
	struct MachineInfo;

	struct ProcessInfo : NonCopyable {
		const char* name;
		const char* coordinationFolder;
		const char* dataFolder;
		MachineInfo* machine;
		NetworkAddress address;
		LocalityData	locality;
		ProcessClass startingClass;
		TDMetricCollection tdmetrics;
		Reference<IListener> listener;
		bool failed;
		bool excluded;
		bool cleared;
		int64_t cpuTicks;
		bool rebooting;
		std::vector<flowGlobalType> globals;

		INetworkConnections *network;

		uint64_t fault_injection_r;
		double fault_injection_p1, fault_injection_p2;

		ProcessInfo(const char* name, LocalityData locality, ProcessClass startingClass, NetworkAddress address,
					INetworkConnections *net, const char* dataFolder, const char* coordinationFolder )
			: name(name), locality(locality), startingClass(startingClass), address(address), dataFolder(dataFolder),
				network(net), coordinationFolder(coordinationFolder), failed(false), excluded(false), cpuTicks(0),
				rebooting(false), fault_injection_p1(0), fault_injection_p2(0),
				fault_injection_r(0), machine(0), cleared(false) {}

		Future<KillType> onShutdown() { return shutdownSignal.getFuture(); }

		bool isReliable() const { return !failed && fault_injection_p1 == 0 && fault_injection_p2 == 0; }
		bool isAvailable() const { return !isExcluded() && isReliable(); }
		bool isExcluded() const { return !excluded; }
		bool isCleared() const { return !cleared; }

		// Returns true if the class represents an acceptable worker
		bool isAvailableClass() const {
			switch (startingClass._class) {
				case ProcessClass::UnsetClass: return true;
				case ProcessClass::StorageClass: return true;
				case ProcessClass::TransactionClass: return true;
				case ProcessClass::ResolutionClass: return false;
				case ProcessClass::ProxyClass: return false;
				case ProcessClass::MasterClass: return false;
				case ProcessClass::TesterClass: return false;
				case ProcessClass::StatelessClass: return false;
				case ProcessClass::LogClass: return true;
				case ProcessClass::ClusterControllerClass: return false;
				default: return false;
			}
		}

		inline flowGlobalType global(int id) { return (globals.size() > id) ? globals[id] : NULL; };
		inline void setGlobal(size_t id, flowGlobalType v) { globals.resize(std::max(globals.size(),id+1)); globals[id] = v; };

		std::string toString() const {
			return format("name: %s  address: %d.%d.%d.%d:%d  zone: %s  datahall: %s  class: %s  coord: %s data: %s  excluded: %d cleared: %d",
			name, (address.ip>>24)&0xff, (address.ip>>16)&0xff, (address.ip>>8)&0xff, address.ip&0xff, address.port, (locality.zoneId().present() ? locality.zoneId().get().printable().c_str() : "[unset]"), (locality.dataHallId().present() ? locality.dataHallId().get().printable().c_str() : "[unset]"), startingClass.toString().c_str(), coordinationFolder, dataFolder, excluded, cleared); }

		// Members not for external use
		Promise<KillType> shutdownSignal;
	};

	struct MachineInfo {
		ProcessInfo* machineProcess;
		std::vector<ProcessInfo*> processes;
		std::map<std::string, Future<Reference<IAsyncFile>>> openFiles;
		std::set<std::string> deletingFiles;
		std::set<std::string> closingFiles;
		Optional<Standalone<StringRef>>	zoneId;

		MachineInfo() : machineProcess(0) {}
	};

	template <class Func>
	ProcessInfo* asNewProcess( const char* name, uint32_t ip, uint16_t port, LocalityData locality, ProcessClass startingClass,
							   Func func, const char* dataFolder, const char* coordinationFolder ) {
		ProcessInfo* m = newProcess(name, ip, port, locality, startingClass, dataFolder, coordinationFolder);
//		ProcessInfo* m = newProcess(name, ip, port, zoneId, machineId, dcId, startingClass, dataFolder, coordinationFolder);
		std::swap(m, currentProcess);
		try {
			func();
		} catch (Error& e) {
			TraceEvent(SevError, "NewMachineError").error(e);
			killProcess(currentProcess, KillInstantly);
		} catch (...) {
			TraceEvent(SevError, "NewMachineError").error(unknown_error());
			killProcess(currentProcess, KillInstantly);
		}
		std::swap(m, currentProcess);
		return m;
	}

	ProcessInfo* getProcess( Endpoint const& endpoint ) { return getProcessByAddress(endpoint.address); }
	ProcessInfo* getCurrentProcess() { return currentProcess; }
	virtual Future<Void> onProcess( ISimulator::ProcessInfo *process, int taskID = -1 ) = 0;
	virtual Future<Void> onMachine( ISimulator::ProcessInfo *process, int taskID = -1 ) = 0;

	virtual ProcessInfo* newProcess(const char* name, uint32_t ip, uint16_t port, LocalityData locality, ProcessClass startingClass, const char* dataFolder, const char* coordinationFolder) = 0;
	virtual void killProcess( ProcessInfo* machine, KillType ) = 0;
	virtual void rebootProcess(Optional<Standalone<StringRef>> zoneId, bool allProcesses ) = 0;
	virtual void rebootProcess( ProcessInfo* process, KillType kt ) = 0;
	virtual void killInterface( NetworkAddress address, KillType ) = 0;
	virtual bool killMachine(Optional<Standalone<StringRef>> zoneId, KillType, bool killIsSafe = false, bool forceKill = false, KillType* ktFinal = NULL) = 0;
	virtual bool killDataCenter(Optional<Standalone<StringRef>> dcId, KillType kt, KillType* ktFinal = NULL) = 0;
	//virtual KillType getMachineKillState( UID zoneID ) = 0;
	virtual bool canKillProcesses(std::vector<ProcessInfo*> const& availableProcesses, std::vector<ProcessInfo*> const& deadProcesses, KillType kt, KillType* newKillType) const = 0;
	virtual bool isAvailable() const = 0;
	virtual void displayWorkers() const;

	virtual void addRole(NetworkAddress const& address, std::string const& role) {
		roleAddresses[address][role] ++;
		TraceEvent("RoleAdd").detail("Address", address).detail("Role", role).detail("Roles", roleAddresses[address].size()).detail("Value", roleAddresses[address][role]);
	}

	virtual void removeRole(NetworkAddress const& address, std::string const& role) {
		auto addressIt = roleAddresses.find(address);
		if (addressIt != roleAddresses.end()) {
			auto rolesIt = addressIt->second.find(role);
			if (rolesIt != addressIt->second.end()) {
				if (rolesIt->second > 1) {
					rolesIt->second --;
					TraceEvent("RoleRemove").detail("Address", address).detail("Role", role).detail("Roles", addressIt->second.size()).detail("Value", rolesIt->second).detail("Result", "Decremented Role");
				}
				else {
					addressIt->second.erase(rolesIt);
					if (addressIt->second.size()) {
						TraceEvent("RoleRemove").detail("Address", address).detail("Role", role).detail("Roles", addressIt->second.size()).detail("Value", 0).detail("Result", "Removed Role");
					}
					else {
						roleAddresses.erase(addressIt);
						TraceEvent("RoleRemove").detail("Address", address).detail("Role", role).detail("Roles", 0).detail("Value", 0).detail("Result", "Removed Address");
					}
				}
			}
			else {
				TraceEvent(SevWarn,"RoleRemove").detail("Address", address).detail("Role", role).detail("Result", "Role Missing");
			}
		}
		else {
			TraceEvent(SevWarn,"RoleRemove").detail("Address", address).detail("Role", role).detail("Result", "Address Missing");
		}
	}

	virtual std::string getRoles(NetworkAddress const& address, bool skipWorkers = true) const {
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

	virtual void clearAddress(NetworkAddress const& address) {
		clearedAddresses[address]++;
		TraceEvent("ClearAddress").detail("Address", address).detail("Value", clearedAddresses[address]);
	}
	virtual bool isCleared(NetworkAddress const& address) const {
		return clearedAddresses.find(address) != clearedAddresses.end();
	}

	virtual void excludeAddress(NetworkAddress const& address) {
		excludedAddresses[address]++;
		TraceEvent("ExcludeAddress").detail("Address", address).detail("Value", excludedAddresses[address]);
	}

	virtual void includeAddress(NetworkAddress const& address) {
		auto addressIt = excludedAddresses.find(address);
		if (addressIt != excludedAddresses.end()) {
			if (addressIt->second > 1) {
				addressIt->second --;
				TraceEvent("IncludeAddress").detail("Address", address).detail("Value", addressIt->second).detail("Result", "Decremented");
			}
			else {
				excludedAddresses.erase(addressIt);
				TraceEvent("IncludeAddress").detail("Address", address).detail("Value", 0).detail("Result", "Removed");
			}
		}
		else {
			TraceEvent(SevWarn,"IncludeAddress").detail("Address", address).detail("Result", "Missing");
		}
	}
	virtual void includeAllAddresses() {
		TraceEvent("IncludeAddressAll").detail("AddressTotal", excludedAddresses.size());
		excludedAddresses.clear();
	}
	virtual bool isExcluded(NetworkAddress const& address) const {
		return excludedAddresses.find(address) != excludedAddresses.end();
	}

	virtual void disableSwapToMachine(Optional<Standalone<StringRef>> zoneId ) {
		swapsDisabled.insert(zoneId);
	}
	virtual void enableSwapToMachine(Optional<Standalone<StringRef>> zoneId ) {
		swapsDisabled.erase(zoneId);
		allSwapsDisabled = false;
	}
	virtual bool canSwapToMachine(Optional<Standalone<StringRef>> zoneId ) {
		return swapsDisabled.count( zoneId ) == 0 && !allSwapsDisabled && !extraDB;
	}
	virtual void enableSwapsToAll() {
		swapsDisabled.clear();
		allSwapsDisabled = false;
	}
	virtual void disableSwapsToAll() {
		swapsDisabled.clear();
		allSwapsDisabled = true;
	}

	virtual void clogInterface( uint32_t ip, double seconds, ClogMode mode = ClogDefault ) = 0;
	virtual void clogPair( uint32_t from, uint32_t to, double seconds ) = 0;
	virtual std::vector<ProcessInfo*> getAllProcesses() const = 0;
	virtual ProcessInfo* getProcessByAddress( NetworkAddress const& address ) = 0;
	virtual MachineInfo* getMachineByNetworkAddress(NetworkAddress const& address) = 0;
	virtual MachineInfo* getMachineById(Optional<Standalone<StringRef>> const& zoneId) = 0;
	virtual void run() {}
	virtual void destroyProcess( ProcessInfo *p ) = 0;
	virtual void destroyMachine(Optional<Standalone<StringRef>> const& zoneId ) = 0;

	// These are here for reasoning about whether it is possible to kill machines (or delete their data)
	//  and maintain the durability of the database.
	int killedMachines;
	int killableMachines;
	int machinesNeededForProgress;
	int desiredCoordinators;
	int neededDatacenters;
	int killedDatacenters;
	int killableDatacenters;
	int physicalDatacenters;
	int maxCoordinatorsInDatacenter;
	int processesPerMachine;
	std::set<NetworkAddress> protectedAddresses;
	std::map<NetworkAddress, ProcessInfo*> currentlyRebootingProcesses;
	class ClusterConnectionString* extraDB;
	IRepPolicyRef storagePolicy;
	IRepPolicyRef	tLogPolicy;
	int						tLogWriteAntiQuorum;

	//Used by workloads that perform reconfigurations
	int testerCount;
	std::string connectionString;

	bool isStopped;
	double lastConnectionFailure;
	double connectionFailuresDisableDuration;
	bool speedUpSimulation;
	BackupAgentType backupAgents;

	virtual flowGlobalType global(int id) { return getCurrentProcess()->global(id); };
	virtual void setGlobal(size_t id, flowGlobalType v) { getCurrentProcess()->setGlobal(id,v); };

protected:
	static thread_local ProcessInfo* currentProcess;
	Mutex mutex;

private:
	std::set<Optional<Standalone<StringRef>>> swapsDisabled;
	std::map<NetworkAddress, int> excludedAddresses;
	std::map<NetworkAddress, int> clearedAddresses;
	std::map<NetworkAddress, std::map<std::string, int>> roleAddresses;
	bool allSwapsDisabled;
};

// Quickly make existing code work that expects g_simulator to be of class type (not a pointer)
extern ISimulator* g_pSimulator;
#define g_simulator (*g_pSimulator)

void startNewSimulator();

//Parameters used to simulate disk performance
struct DiskParameters : ReferenceCounted<DiskParameters> {
	double nextOperation;
	int64_t iops;
	int64_t bandwidth;

	DiskParameters(int64_t iops, int64_t bandwidth) : nextOperation(0), iops(iops), bandwidth(bandwidth) { }
};

//Simulates delays for performing operations on disk
extern Future<Void> waitUntilDiskReady(Reference<DiskParameters> parameters, int64_t size, bool sync = false);


class Sim2FileSystem : public IAsyncFileSystem {
public:
	virtual Future< Reference<class IAsyncFile> > open( std::string filename, int64_t flags, int64_t mode );
	// Opens a file for asynchronous I/O

	virtual Future< Void > deleteFile( std::string filename, bool mustBeDurable );
	// Deletes the given file.  If mustBeDurable, returns only when the file is guaranteed to be deleted even after a power failure.

	Sim2FileSystem() {}

	virtual ~Sim2FileSystem() {}

	static void newFileSystem();
};

#endif
