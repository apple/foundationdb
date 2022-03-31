/*
 * TCInfo.h
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

#pragma once

#include "fdbserver/DDTeamCollection.h"

class TCTeamInfo;
class TCMachineInfo;
class TCMachineTeamInfo;

class TCServerInfo : public ReferenceCounted<TCServerInfo> {
	friend class TCServerInfoImpl;
	friend class DDTeamCollectionUnitTest;
	UID id;
	bool inDesiredDC;
	DDTeamCollection* collection;
	Future<Void> tracker;

	// TODO: Remove?
	[[maybe_unused]] Version addedVersion; // Read version when this Server is added

	StorageServerInterface lastKnownInterface;
	ProcessClass lastKnownClass;

	// A storage server's StoreType does not change.
	// To change storeType for an ip:port, we destroy the old one and create a new one.
	KeyValueStoreType storeType; // Storage engine type

	int64_t dataInFlightToServer;
	std::vector<Reference<TCTeamInfo>> teams;
	ErrorOr<GetStorageMetricsReply> metrics;

	GetStorageMetricsReply const& getMetrics() const { return metrics.get(); }

	void setMetrics(GetStorageMetricsReply serverMetrics) { this->metrics = serverMetrics; }
	void markTeamUnhealthy(int teamIndex);

public:
	Reference<TCMachineInfo> machine;
	Promise<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged;
	Future<std::pair<StorageServerInterface, ProcessClass>> onInterfaceChanged;
	Promise<Void> removed;
	Future<Void> onRemoved;
	Future<Void> onTSSPairRemoved;
	Promise<Void> killTss;
	Promise<Void> wakeUpTracker;
	LocalityEntry localityEntry;
	Promise<Void> updated;
	AsyncVar<bool> wrongStoreTypeToRemove;
	AsyncVar<bool> ssVersionTooFarBehind;

	TCServerInfo(StorageServerInterface ssi,
	             DDTeamCollection* collection,
	             ProcessClass processClass,
	             bool inDesiredDC,
	             Reference<LocalitySet> storageServerSet,
	             Version addedVersion = 0);

	UID const& getId() const { return id; }
	bool isInDesiredDC() const { return inDesiredDC; }
	void updateInDesiredDC(std::vector<Optional<Key>> const& includedDCs);
	void setTracker(Future<Void> tracker) { this->tracker = tracker; }
	void updateLastKnown(StorageServerInterface const&, ProcessClass);
	StorageServerInterface const& getLastKnownInterface() const { return lastKnownInterface; }
	ProcessClass const& getLastKnownClass() const { return lastKnownClass; }
	Future<Void> updateStoreType();
	KeyValueStoreType getStoreType() const { return storeType; }
	int64_t getDataInFlightToServer() const { return dataInFlightToServer; }
	void incrementDataInFlightToServer(int64_t bytes) { dataInFlightToServer += bytes; }
	void cancel();
	std::vector<Reference<TCTeamInfo>> const& getTeams() const { return teams; }
	void addTeam(Reference<TCTeamInfo> team) { teams.push_back(team); }
	void removeTeamsContainingServer(UID removedServer);
	void removeTeam(Reference<TCTeamInfo>);
	bool metricsPresent() const { return metrics.present(); }

	bool isCorrectStoreType(KeyValueStoreType configStoreType) const {
		// A new storage server's store type may not be set immediately.
		// If a storage server does not reply its storeType, it will be tracked by failure monitor and removed.
		return (storeType == configStoreType || storeType == KeyValueStoreType::END);
	}
	bool isWigglePausedServer() const;

	std::pair<int64_t, int64_t> spaceBytes(bool includeInFlight = true) const;
	int64_t loadBytes() const;
	bool hasHealthyAvailableSpace(double minAvailableSpaceRatio) const;

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

	Reference<TCMachineInfo> clone() const;

	explicit TCMachineInfo(Reference<TCServerInfo> server, const LocalityEntry& entry);

	std::string getServersIDStr() const;
};

// TeamCollection's machine team information
class TCMachineTeamInfo : public ReferenceCounted<TCMachineTeamInfo> {
	UID _id;
	std::vector<Reference<TCMachineInfo>> machines;
	std::vector<Standalone<StringRef>> machineIDs;
	std::vector<Reference<TCTeamInfo>> serverTeams;

public:
	explicit TCMachineTeamInfo(std::vector<Reference<TCMachineInfo>> const& machines);

	int size() const {
		ASSERT_EQ(machines.size(), machineIDs.size());
		return machineIDs.size();
	}

	UID id() const { return _id; }
	std::vector<Reference<TCMachineInfo>> const& getMachines() const { return machines; }
	std::vector<Standalone<StringRef>> const& getMachineIDs() const { return machineIDs; }
	std::vector<Reference<TCTeamInfo>> const& getServerTeams() const { return serverTeams; }
	void addServerTeam(Reference<TCTeamInfo> team) { serverTeams.push_back(team); }
	bool matches(std::vector<Standalone<StringRef>> const& sortedMachineIDs);
	std::string getMachineIDsStr() const;
	bool containsMachine(Standalone<StringRef> machineID) const {
		return std::count(machineIDs.begin(), machineIDs.end(), machineID);
	}

	// Returns true iff team is found
	bool removeServerTeam(Reference<TCTeamInfo> team);

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

	explicit TCTeamInfo(std::vector<Reference<TCServerInfo>> const& servers);

	std::string getTeamID() const override { return id.shortString(); }

	std::vector<StorageServerInterface> getLastKnownServerInterfaces() const override;

	int size() const override {
		ASSERT(servers.size() == serverIDs.size());
		return servers.size();
	}

	std::vector<UID> const& getServerIDs() const override { return serverIDs; }

	const std::vector<Reference<TCServerInfo>>& getServers() const { return servers; }

	std::string getServerIDsStr() const;

	void addDataInFlightToTeam(int64_t delta) override;

	int64_t getDataInFlightToTeam() const override;

	int64_t getLoadBytes(bool includeInFlight = true, double inflightPenalty = 1.0) const override;

	int64_t getMinAvailableSpace(bool includeInFlight = true) const override;

	double getMinAvailableSpaceRatio(bool includeInFlight = true) const override;

	bool hasHealthyAvailableSpace(double minRatio) const override;

	Future<Void> updateStorageMetrics() override;

	bool isOptimal() const override;

	bool isWrongConfiguration() const override { return wrongConfiguration; }
	void setWrongConfiguration(bool wrongConfiguration) override { this->wrongConfiguration = wrongConfiguration; }
	bool isHealthy() const override { return healthy; }
	void setHealthy(bool h) override { healthy = h; }
	int getPriority() const override { return priority; }
	void setPriority(int p) override { priority = p; }
	void addref() const override { ReferenceCounted<TCTeamInfo>::addref(); }
	void delref() const override { ReferenceCounted<TCTeamInfo>::delref(); }

	bool hasServer(const UID& server) const;
	bool hasWigglePausedServer() const;

	void addServers(const std::vector<UID>& servers) override;

private:
	// Calculate an "average" of the metrics replies that we received.  Penalize teams from which we did not receive all
	// replies.
	int64_t getLoadAverage() const;

	bool allServersHaveHealthyAvailableSpace() const;
};
