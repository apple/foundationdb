#ifndef FDBRPC_SIMULATOR_PROCESSINFO_H
#define FDBRPC_SIMULATOR_PROCESSINFO_H

#include <map>
#include <string>

#include "flow/NetworkAddress.h"
#include "flow/IConnection.h"
#include "flow/IUDPSocket.h"

#include "fdbrpc/SimulatorMachineInfo.h"
#include "fdbrpc/SimulatorKillType.h"

struct MachineInfo;

namespace simulator {

struct ProcessInfo : NonCopyable {
	std::string name;
	std::string coordinationFolder;
	std::string dataFolder;
	MachineInfo* machine;
	NetworkAddressList addresses;
	NetworkAddress address;
	LocalityData locality;
	ProcessClass startingClass;
	TDMetricCollection tdmetrics;
	MetricCollection metrics;
	ChaosMetrics chaosMetrics;
	HistogramRegistry histograms;
	std::map<NetworkAddress, Reference<IListener>> listenerMap;
	std::map<NetworkAddress, Reference<IUDPSocket>> boundUDPSockets;
	bool failed;
	bool excluded;
	bool cleared;
	bool rebooting;
	bool drProcess;
	std::vector<flowGlobalType> globals;

	INetworkConnections* network;

	uint64_t fault_injection_r;
	double fault_injection_p1, fault_injection_p2;
	bool failedDisk;

	UID uid;

	ProtocolVersion protocolVersion;
	bool excludeFromRestarts = false;

	std::vector<ProcessInfo*> childs;

	ProcessInfo(const char* name,
	            LocalityData locality,
	            ProcessClass startingClass,
	            NetworkAddressList addresses,
	            INetworkConnections* net,
	            const char* dataFolder,
	            const char* coordinationFolder)
	  : name(name), coordinationFolder(coordinationFolder), dataFolder(dataFolder), machine(nullptr),
	    addresses(addresses), address(addresses.address), locality(locality), startingClass(startingClass),
	    failed(false), excluded(false), cleared(false), rebooting(false), drProcess(false), network(net),
	    fault_injection_r(0), fault_injection_p1(0), fault_injection_p2(0), failedDisk(false) {
		uid = deterministicRandom()->randomUniqueID();
	}

	Future<KillType> onShutdown() { return shutdownSignal.getFuture(); }

	bool isSpawnedKVProcess() const {
		// SOMEDAY: use a separate bool may be better?
		return name == "remote flow process";
	}
	bool isReliable() const {
		return !failed && fault_injection_p1 == 0 && fault_injection_p2 == 0 && !failedDisk &&
		       (!machine ||
		        (machine->machineProcess->fault_injection_p1 == 0 && machine->machineProcess->fault_injection_p2 == 0));
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
	std::vector<ProcessInfo*> const& getChilds() const { return childs; }

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
		case ProcessClass::ConsistencyScanClass:
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
		return format("name: %s address: %s zone: %s datahall: %s class: %s excluded: %d cleared: %d",
		              name.c_str(),
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

} // namespace simulator

#endif // FDBRPC_SIMULATOR_PROCESSINFO_H