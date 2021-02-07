/*
 * TCMachineTeamInfo.h
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

#include "fdbserver/TCMachineInfo.h"
#include "fdbserver/TCTeamInfo.h"
#include "flow/FastRef.h"

#include <vector>

struct TCMachineInfo;
struct TCTeamInfo;

class TCMachineTeamInfo : public ReferenceCounted<TCMachineTeamInfo> {
	std::vector<Reference<TCTeamInfo>> serverTeams;
	UID id;

public:
	std::vector<Reference<TCMachineInfo>> machines;
	std::vector<Standalone<StringRef>> machineIDs;

public:
	UID getID() const;
	std::vector<Reference<TCTeamInfo>> const& getServerTeams() const;
	void addServerTeam(Reference<TCTeamInfo> const& team);
	void removeServerTeam(Reference<TCTeamInfo> const& team);
	explicit TCMachineTeamInfo(vector<Reference<TCMachineInfo>> const& machines);
	int size() const;
	std::string getMachineIDsStr() const;
	bool operator==(const TCMachineTeamInfo& rhs) const;
};
