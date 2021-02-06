/*
 * TCMachineInfo.h
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

#include "fdbrpc/ReplicationTypes.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"

struct TCServerInfo;
struct TCMachineTeamInfo;

class TCMachineInfo : public ReferenceCounted<TCMachineInfo> {
public:
	std::vector<Reference<TCServerInfo>> serversOnMachine; // SOMEDAY: change from vector to set
	Standalone<StringRef> machineID;
	std::vector<Reference<TCMachineTeamInfo>> machineTeams; // SOMEDAY: split good and bad machine teams.
	LocalityEntry localityEntry;

	explicit TCMachineInfo(Reference<TCServerInfo> server, const LocalityEntry& entry);
	std::string getServersIDStr() const;
};
