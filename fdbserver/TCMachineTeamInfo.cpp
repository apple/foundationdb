/*
 * TCMachineTeamInfo.cpp
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

#include "TCMachineTeamInfo.h"

TCMachineTeamInfo::TCMachineTeamInfo(vector<Reference<TCMachineInfo>> const& machines)
  : machines(machines), id(deterministicRandom()->randomUniqueID()) {
	machineIDs.reserve(machines.size());
	for (int i = 0; i < machines.size(); i++) {
		machineIDs.push_back(machines[i]->getID());
	}
	sort(machineIDs.begin(), machineIDs.end());
}

int TCMachineTeamInfo::size() const {
	ASSERT(machines.size() == machineIDs.size());
	return machineIDs.size();
}

std::string TCMachineTeamInfo::getMachineIDsStr() const {
	if (machineIDs.empty()) return "[unset]";

	std::string result;

	for (const auto& id : machineIDs) {
		result += id.contents().toString() + " ";
	}

	return result;
}

bool TCMachineTeamInfo::operator==(const TCMachineTeamInfo& rhs) const {
	return this->machineIDs == rhs.machineIDs;
}

std::vector<Reference<TCTeamInfo>> const& TCMachineTeamInfo::getServerTeams() const {
	return serverTeams;
}

void TCMachineTeamInfo::addServerTeam(Reference<TCTeamInfo> const& team) {
	serverTeams.push_back(team);
}

void TCMachineTeamInfo::removeServerTeam(Reference<TCTeamInfo> const& team) {
	for (int t = 0; t < serverTeams.size(); ++t) {
		if (serverTeams[t] == team) {
			serverTeams[t--] = serverTeams.back();
			serverTeams.pop_back();
			return;
		}
	}
	ASSERT_WE_THINK(false);
}

UID TCMachineTeamInfo::getID() const {
	return id;
}

std::vector<Reference<TCMachineInfo>> const &TCMachineTeamInfo::getMachines() const {
	return machines;
}

std::vector<Standalone<StringRef>> const &TCMachineTeamInfo::getMachineIDs() const {
	return machineIDs;
}
