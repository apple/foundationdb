/*
 * TCTeamInfo.actor.cpp
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

#include "TCTeamInfo.h"

TCTeamInfo::TCTeamInfo(vector<Reference<TCServerInfo>> const& servers)
  : servers(servers), healthy(true), priority(SERVER_KNOBS->PRIORITY_TEAM_HEALTHY), wrongConfiguration(false),
    id(deterministicRandom()->randomUniqueID()) {
	if (servers.empty()) {
		TraceEvent(SevInfo, "ConstructTCTeamFromEmptyServers");
	}
	serverIDs.reserve(servers.size());
	for (const auto& server : servers) {
		serverIDs.push_back(server->getID());
	}
}

std::string TCTeamInfo::getTeamID() const {
	return id.shortString();
}

std::vector<StorageServerInterface> TCTeamInfo::getLastKnownServerInterfaces() const {
	vector<StorageServerInterface> v;
	v.reserve(servers.size());
	for (const auto& server : servers) {
		v.push_back(server->lastKnownInterface);
	}
	return v;
}

int TCTeamInfo::size() const {
	ASSERT(servers.size() == serverIDs.size());
	return servers.size();
}

vector<UID> const& TCTeamInfo::getServerIDs() const {
	return serverIDs;
}
const vector<Reference<TCServerInfo>>& TCTeamInfo::getServers() const {
	return servers;
}

std::string TCTeamInfo::getServerIDsStr() const {
	std::string result;

	if (serverIDs.empty()) return "[unset]";

	for (const auto& id : serverIDs) {
		result += id.toString() + " ";
	}

	return result;
}

void TCTeamInfo::addDataInFlightToTeam(int64_t delta) {
	for (int i = 0; i < servers.size(); i++) servers[i]->dataInFlightToServer += delta;
}

int64_t TCTeamInfo::getDataInFlightToTeam() const {
	int64_t dataInFlight = 0.0;
	for (int i = 0; i < servers.size(); i++) dataInFlight += servers[i]->dataInFlightToServer;
	return dataInFlight;
}

int64_t TCTeamInfo::getLoadBytes(bool includeInFlight, double inflightPenalty) const {
	int64_t physicalBytes = getLoadAverage();
	double minAvailableSpaceRatio = getMinAvailableSpaceRatio(includeInFlight);
	int64_t inFlightBytes = includeInFlight ? getDataInFlightToTeam() / servers.size() : 0;
	double availableSpaceMultiplier =
	    SERVER_KNOBS->AVAILABLE_SPACE_RATIO_CUTOFF /
	    (std::max(std::min(SERVER_KNOBS->AVAILABLE_SPACE_RATIO_CUTOFF, minAvailableSpaceRatio), 0.000001));
	if (servers.size() > 2) {
		// make sure in triple replication the penalty is high enough that you will always avoid a team with a member at
		// 20% free space
		availableSpaceMultiplier = availableSpaceMultiplier * availableSpaceMultiplier;
	}

	if (minAvailableSpaceRatio < SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO) {
		TraceEvent(SevWarn, "DiskNearCapacity").suppressFor(1.0).detail("AvailableSpaceRatio", minAvailableSpaceRatio);
	}

	return (physicalBytes + (inflightPenalty * inFlightBytes)) * availableSpaceMultiplier;
}

int64_t TCTeamInfo::getMinAvailableSpace(bool includeInFlight) const {
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

double TCTeamInfo::getMinAvailableSpaceRatio(bool includeInFlight) const {
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

bool TCTeamInfo::hasHealthyAvailableSpace(double minRatio) const {
	return getMinAvailableSpaceRatio() >= minRatio && getMinAvailableSpace() > SERVER_KNOBS->MIN_AVAILABLE_SPACE;
}

Future<Void> TCTeamInfo::updateStorageMetrics() {
	std::vector<Future<Void>> updates;
	for (int i = 0; i < servers.size(); i++) updates.push_back(servers[i]->updateServerMetrics());
	return waitForAll(updates);
}

bool TCTeamInfo::isOptimal() const {
	for (const auto& server : servers) {
		if (server->lastKnownClass.machineClassFitness(ProcessClass::Storage) > ProcessClass::UnsetFit) {
			return false;
		}
	}
	return true;
}

bool TCTeamInfo::isWrongConfiguration() const {
	return wrongConfiguration;
}
void TCTeamInfo::setWrongConfiguration(bool wrongConfiguration) {
	this->wrongConfiguration = wrongConfiguration;
}
bool TCTeamInfo::isHealthy() const {
	return healthy;
}
void TCTeamInfo::setHealthy(bool h) {
	healthy = h;
}
int TCTeamInfo::getPriority() const {
	return priority;
}
void TCTeamInfo::setPriority(int p) {
	priority = p;
}
void TCTeamInfo::addref() {
	ReferenceCounted<TCTeamInfo>::addref();
}
void TCTeamInfo::delref() {
	ReferenceCounted<TCTeamInfo>::delref();
}

void TCTeamInfo::addServers(const vector<UID>& servers) {
	serverIDs.reserve(servers.size());
	for (int i = 0; i < servers.size(); i++) {
		serverIDs.push_back(servers[i]);
	}
}

// Calculate an "average" of the metrics replies that we received.  Penalize teams from which we did not receive all
// replies.
int64_t TCTeamInfo::getLoadAverage() const {
	int64_t bytesSum = 0;
	int added = 0;
	for (const auto& server : servers) {
		if (server->serverMetrics.present()) {
			added++;
			bytesSum += server->serverMetrics.get().load.bytes;
		}
	}

	if (added < servers.size()) bytesSum *= 2;

	return added == 0 ? 0 : bytesSum / added;
}

void TCTeamInfo::setTracker(Future<Void> &&tracker) {
	this->tracker = std::move(tracker);
}

void TCTeamInfo::cancelTracker() {
	tracker.cancel();
}

Reference<TCMachineTeamInfo> const& TCTeamInfo::getMachineTeam() const {
	return machineTeam;
}

void TCTeamInfo::setMachineTeam(Reference<TCMachineTeamInfo> const& machineTeam) {
	this->machineTeam = machineTeam;
}
