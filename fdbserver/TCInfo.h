/*
 * TCInfo.h
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

#pragma once

#include "fdbserver/DDTeamCollection.h"

class TCTeamInfo;
class TCMachineInfo;
class TCMachineTeamInfo;

class TCServerInfo : public ReferenceCounted<TCServerInfo> {
	friend class TCServerInfoImpl;

public:
	UID id;
	Version addedVersion; // Read version when this Server is added
	DDTeamCollection* collection;
	StorageServerInterface lastKnownInterface;
	ProcessClass lastKnownClass;
	std::vector<Reference<TCTeamInfo>> teams;
	Reference<TCMachineInfo> machine;
	Future<Void> tracker;
	int64_t dataInFlightToServer;
	ErrorOr<GetStorageMetricsReply> serverMetrics;
	Promise<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged;
	Future<std::pair<StorageServerInterface, ProcessClass>> onInterfaceChanged;
	Promise<Void> removed;
	Future<Void> onRemoved;
	Future<Void> onTSSPairRemoved;
	Promise<Void> killTss;
	Promise<Void> wakeUpTracker;
	bool inDesiredDC;
	LocalityEntry localityEntry;
	Promise<Void> updated;
	AsyncVar<bool> wrongStoreTypeToRemove;
	AsyncVar<bool> ssVersionTooFarBehind;
	// A storage server's StoreType does not change.
	// To change storeType for an ip:port, we destroy the old one and create a new one.
	KeyValueStoreType storeType; // Storage engine type

	TCServerInfo(StorageServerInterface ssi,
	             DDTeamCollection* collection,
	             ProcessClass processClass,
	             bool inDesiredDC,
	             Reference<LocalitySet> storageServerSet,
	             Version addedVersion = 0)
	  : id(ssi.id()), addedVersion(addedVersion), collection(collection), lastKnownInterface(ssi),
	    lastKnownClass(processClass), dataInFlightToServer(0), onInterfaceChanged(interfaceChanged.getFuture()),
	    onRemoved(removed.getFuture()), onTSSPairRemoved(Never()), inDesiredDC(inDesiredDC),
	    storeType(KeyValueStoreType::END) {

		if (!ssi.isTss()) {
			localityEntry = ((LocalityMap<UID>*)storageServerSet.getPtr())->add(ssi.locality, &id);
		}
	}

	bool isCorrectStoreType(KeyValueStoreType configStoreType) const {
		// A new storage server's store type may not be set immediately.
		// If a storage server does not reply its storeType, it will be tracked by failure monitor and removed.
		return (storeType == configStoreType || storeType == KeyValueStoreType::END);
	}

	Future<Void> updateServerMetrics();

	static Future<Void> updateServerMetrics(Reference<TCServerInfo> server);

	Future<Void> serverMetricsPolling();

	~TCServerInfo();
};

class TCMachineInfo : public ReferenceCounted<TCMachineInfo> {
	TCMachineInfo() = default;

public:
	std::vector<Reference<TCServerInfo>> serversOnMachine; // SOMEDAY: change from vector to set
	Standalone<StringRef> machineID;
	std::vector<Reference<TCMachineTeamInfo>> machineTeams; // SOMEDAY: split good and bad machine teams.
	LocalityEntry localityEntry;

	Reference<TCMachineInfo> clone() const {
		auto result = Reference<TCMachineInfo>(new TCMachineInfo);
		result->serversOnMachine = serversOnMachine;
		result->machineID = machineID;
		result->machineTeams = machineTeams;
		result->localityEntry = localityEntry;
		return result;
	}

	explicit TCMachineInfo(Reference<TCServerInfo> server, const LocalityEntry& entry) : localityEntry(entry) {
		ASSERT(serversOnMachine.empty());
		serversOnMachine.push_back(server);

		LocalityData& locality = server->lastKnownInterface.locality;
		ASSERT(locality.zoneId().present());
		machineID = locality.zoneId().get();
	}

	std::string getServersIDStr() const {
		std::stringstream ss;
		if (serversOnMachine.empty())
			return "[unset]";

		for (const auto& server : serversOnMachine) {
			ss << server->id.toString() << " ";
		}

		return std::move(ss).str();
	}
};

// TeamCollection's machine team information
class TCMachineTeamInfo : public ReferenceCounted<TCMachineTeamInfo> {
public:
	std::vector<Reference<TCMachineInfo>> machines;
	std::vector<Standalone<StringRef>> machineIDs;
	std::vector<Reference<TCTeamInfo>> serverTeams;
	UID id;

	explicit TCMachineTeamInfo(std::vector<Reference<TCMachineInfo>> const& machines)
	  : machines(machines), id(deterministicRandom()->randomUniqueID()) {
		machineIDs.reserve(machines.size());
		for (int i = 0; i < machines.size(); i++) {
			machineIDs.push_back(machines[i]->machineID);
		}
		sort(machineIDs.begin(), machineIDs.end());
	}

	int size() const {
		ASSERT(machines.size() == machineIDs.size());
		return machineIDs.size();
	}

	std::string getMachineIDsStr() const {
		std::stringstream ss;

		if (machineIDs.empty())
			return "[unset]";

		for (const auto& id : machineIDs) {
			ss << id.contents().toString() << " ";
		}

		return std::move(ss).str();
	}

	bool operator==(TCMachineTeamInfo& rhs) const { return this->machineIDs == rhs.machineIDs; }
};

// TeamCollection's server team info.
class TCTeamInfo final : public ReferenceCounted<TCTeamInfo>, public IDataDistributionTeam {
	friend class TCTeamInfoImpl;
	std::vector<Reference<TCServerInfo>> servers;
	std::vector<UID> serverIDs;
	bool healthy;
	bool wrongConfiguration; // True if any of the servers in the team have the wrong configuration
	int priority;
	UID id;

public:
	Reference<TCMachineTeamInfo> machineTeam;
	Future<Void> tracker;

	explicit TCTeamInfo(std::vector<Reference<TCServerInfo>> const& servers)
	  : servers(servers), healthy(true), wrongConfiguration(false), priority(SERVER_KNOBS->PRIORITY_TEAM_HEALTHY),
	    id(deterministicRandom()->randomUniqueID()) {
		if (servers.empty()) {
			TraceEvent(SevInfo, "ConstructTCTeamFromEmptyServers").log();
		}
		serverIDs.reserve(servers.size());
		for (int i = 0; i < servers.size(); i++) {
			serverIDs.push_back(servers[i]->id);
		}
	}

	std::string getTeamID() const override { return id.shortString(); }

	std::vector<StorageServerInterface> getLastKnownServerInterfaces() const override {
		std::vector<StorageServerInterface> v;
		v.reserve(servers.size());
		for (const auto& server : servers) {
			v.push_back(server->lastKnownInterface);
		}
		return v;
	}
	int size() const override {
		ASSERT(servers.size() == serverIDs.size());
		return servers.size();
	}
	std::vector<UID> const& getServerIDs() const override { return serverIDs; }
	const std::vector<Reference<TCServerInfo>>& getServers() const { return servers; }

	std::string getServerIDsStr() const {
		std::stringstream ss;

		if (serverIDs.empty())
			return "[unset]";

		for (const auto& id : serverIDs) {
			ss << id.toString() << " ";
		}

		return std::move(ss).str();
	}

	void addDataInFlightToTeam(int64_t delta) override {
		for (int i = 0; i < servers.size(); i++)
			servers[i]->dataInFlightToServer += delta;
	}
	int64_t getDataInFlightToTeam() const override {
		int64_t dataInFlight = 0.0;
		for (int i = 0; i < servers.size(); i++)
			dataInFlight += servers[i]->dataInFlightToServer;
		return dataInFlight;
	}

	int64_t getLoadBytes(bool includeInFlight = true, double inflightPenalty = 1.0) const override {
		int64_t physicalBytes = getLoadAverage();
		double minAvailableSpaceRatio = getMinAvailableSpaceRatio(includeInFlight);
		int64_t inFlightBytes = includeInFlight ? getDataInFlightToTeam() / servers.size() : 0;
		double availableSpaceMultiplier =
		    SERVER_KNOBS->AVAILABLE_SPACE_RATIO_CUTOFF /
		    (std::max(std::min(SERVER_KNOBS->AVAILABLE_SPACE_RATIO_CUTOFF, minAvailableSpaceRatio), 0.000001));
		if (servers.size() > 2) {
			// make sure in triple replication the penalty is high enough that you will always avoid a team with a
			// member at 20% free space
			availableSpaceMultiplier = availableSpaceMultiplier * availableSpaceMultiplier;
		}

		if (minAvailableSpaceRatio < SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO) {
			TraceEvent(SevWarn, "DiskNearCapacity")
			    .suppressFor(1.0)
			    .detail("AvailableSpaceRatio", minAvailableSpaceRatio);
		}

		return (physicalBytes + (inflightPenalty * inFlightBytes)) * availableSpaceMultiplier;
	}

	int64_t getMinAvailableSpace(bool includeInFlight = true) const override {
		int64_t minAvailableSpace = std::numeric_limits<int64_t>::max();
		for (const auto& server : servers) {
			if (server->serverMetrics.present()) {
				auto& replyValue = server->serverMetrics.get();

				ASSERT(replyValue.available.bytes >= 0);
				ASSERT(replyValue.capacity.bytes >= 0);

				int64_t bytesAvailable = replyValue.available.bytes;
				if (includeInFlight) {
					bytesAvailable -= server->dataInFlightToServer;
				}

				minAvailableSpace = std::min(bytesAvailable, minAvailableSpace);
			}
		}

		return minAvailableSpace; // Could be negative
	}

	double getMinAvailableSpaceRatio(bool includeInFlight = true) const override {
		double minRatio = 1.0;
		for (const auto& server : servers) {
			if (server->serverMetrics.present()) {
				auto& replyValue = server->serverMetrics.get();

				ASSERT(replyValue.available.bytes >= 0);
				ASSERT(replyValue.capacity.bytes >= 0);

				int64_t bytesAvailable = replyValue.available.bytes;
				if (includeInFlight) {
					bytesAvailable = std::max((int64_t)0, bytesAvailable - server->dataInFlightToServer);
				}

				if (replyValue.capacity.bytes == 0)
					minRatio = 0;
				else
					minRatio = std::min(minRatio, ((double)bytesAvailable) / replyValue.capacity.bytes);
			}
		}

		return minRatio;
	}

	bool hasHealthyAvailableSpace(double minRatio) const override {
		return getMinAvailableSpaceRatio() >= minRatio && getMinAvailableSpace() > SERVER_KNOBS->MIN_AVAILABLE_SPACE;
	}

	Future<Void> updateStorageMetrics() override;

	bool isOptimal() const override {
		for (const auto& server : servers) {
			if (server->lastKnownClass.machineClassFitness(ProcessClass::Storage) > ProcessClass::UnsetFit) {
				return false;
			}
		}
		return true;
	}

	bool isWrongConfiguration() const override { return wrongConfiguration; }
	void setWrongConfiguration(bool wrongConfiguration) override { this->wrongConfiguration = wrongConfiguration; }
	bool isHealthy() const override { return healthy; }
	void setHealthy(bool h) override { healthy = h; }
	int getPriority() const override { return priority; }
	void setPriority(int p) override { priority = p; }
	void addref() override { ReferenceCounted<TCTeamInfo>::addref(); }
	void delref() override { ReferenceCounted<TCTeamInfo>::delref(); }

	bool hasServer(const UID& server) {
		return std::find(serverIDs.begin(), serverIDs.end(), server) != serverIDs.end();
	}

	void addServers(const std::vector<UID>& servers) override {
		serverIDs.reserve(servers.size());
		for (int i = 0; i < servers.size(); i++) {
			serverIDs.push_back(servers[i]);
		}
	}

private:
	// Calculate an "average" of the metrics replies that we received.  Penalize teams from which we did not receive all
	// replies.
	int64_t getLoadAverage() const {
		int64_t bytesSum = 0;
		int added = 0;
		for (int i = 0; i < servers.size(); i++)
			if (servers[i]->serverMetrics.present()) {
				added++;
				bytesSum += servers[i]->serverMetrics.get().load.bytes;
			}

		if (added < servers.size())
			bytesSum *= 2;

		return added == 0 ? 0 : bytesSum / added;
	}
};
