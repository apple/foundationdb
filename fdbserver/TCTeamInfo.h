/*
 * TCTeamInfo.h
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

#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/TCMachineTeamInfo.h"
#include "fdbserver/TCServerInfo.h"
#include "flow/FastRef.h"
#include "flow/flow.h"

#include <vector>

struct TCServerInfo;
struct TCMachineTeamInfo;

class TCTeamInfo final : public ReferenceCounted<TCTeamInfo>, public IDataDistributionTeam {
	std::vector<Reference<TCServerInfo>> servers;
	std::vector<UID> serverIDs;
	bool healthy;
	bool wrongConfiguration; // True if any of the servers in the team have the wrong configuration
	int priority;
	UID id;
	Future<Void> tracker;

public:
	Reference<TCMachineTeamInfo> machineTeam; // only needed for sanity check?
	explicit TCTeamInfo(vector<Reference<TCServerInfo>> const& servers);
	std::string getTeamID() const override;

	std::vector<StorageServerInterface> getLastKnownServerInterfaces() const override;
	int size() const override;
	std::vector<UID> const& getServerIDs() const override;
	const std::vector<Reference<TCServerInfo>>& getServers() const;

	std::string getServerIDsStr() const;

	void setTracker(Future<Void> &&tracker);
	void cancelTracker();
	void addDataInFlightToTeam(int64_t delta) override;
	int64_t getDataInFlightToTeam() const override;
	int64_t getLoadBytes(bool includeInFlight = true, double inflightPenalty = 1.0) const override;
	int64_t getMinAvailableSpace(bool includeInFlight = true) const override;
	double getMinAvailableSpaceRatio(bool includeInFlight = true) const override;
	bool hasHealthyAvailableSpace(double minRatio) const override;
	Future<Void> updateStorageMetrics() override;
	bool isOptimal() const override;
	bool isWrongConfiguration() const override;
	void setWrongConfiguration(bool wrongConfiguration) override;
	bool isHealthy() const override;
	void setHealthy(bool h) override;
	int getPriority() const override;
	void setPriority(int p) override;
	void addref() override;
	void delref() override;
	void addServers(const vector<UID>& servers) override;

private:
	int64_t getLoadAverage() const;
};
