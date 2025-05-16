/*
 * DataDistributionTeam.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/StorageServerInterface.h"

struct GetTeamRequest;
namespace data_distribution {
// DD evaluate the metrics of server teams and will increase the count if corresponding metrics is within a eligible
// range
class EligibilityCounter {
public:
	// The type value are used to do bit operations to get combined type. Ex. combineType = LOW_CPU | LOW_DISK_UTIL .
	// When adding more types, the value should be 2^n, like 4, 8, 16...
	enum Type {
		NONE = 0, // don't care about eligibility
		LOW_CPU = 1,
		LOW_DISK_UTIL = 2
	};

	// set the count of type to 0
	void reset(Type type);

	// return the minimal count of a combined eligible type
	int getCount(int combinedType) const;

	// increase the count of type
	void increase(Type type);

	// return combinedType that can be used as input to getCount().
	static int fromGetTeamRequest(GetTeamRequest const&);

private:
	std::unordered_map<Type, int> type_count;
};

} // namespace data_distribution

struct IDataDistributionTeam {
	virtual std::vector<StorageServerInterface> getLastKnownServerInterfaces() const = 0;
	virtual int size() const = 0;
	virtual std::vector<UID> const& getServerIDs() const = 0;
	virtual void addDataInFlightToTeam(int64_t delta) = 0;
	virtual void addReadInFlightToTeam(int64_t delta) = 0;
	virtual int64_t getDataInFlightToTeam() const = 0;
	virtual Optional<int64_t> getLongestStorageQueueSize() const = 0;
	virtual Optional<int> getMaxOngoingBulkLoadTaskCount() const = 0;
	virtual int64_t getLoadBytes(bool includeInFlight = true, double inflightPenalty = 1.0) const = 0;
	virtual int64_t getReadInFlightToTeam() const = 0;
	virtual double getReadLoad(bool includeInFlight = true, double inflightPenalty = 1.0) const = 0;
	virtual double getAverageCPU() const = 0;
	virtual bool hasLowerCpu(double cpuThreshold) const = 0;
	virtual int64_t getMinAvailableSpace(bool includeInFlight = true) const = 0;
	virtual double getMinAvailableSpaceRatio(bool includeInFlight = true) const = 0;
	virtual bool hasHealthyAvailableSpace(double minRatio) const = 0;
	virtual Future<Void> updateStorageMetrics() = 0;
	virtual void addref() const = 0;
	virtual void delref() const = 0;
	virtual bool isHealthy() const = 0;
	virtual void setHealthy(bool) = 0;
	virtual int getPriority() const = 0;
	virtual void setPriority(int) = 0;
	virtual bool isOptimal() const = 0;
	virtual bool isWrongConfiguration() const = 0;
	virtual void setWrongConfiguration(bool) = 0;
	virtual void addServers(const std::vector<UID>& servers) = 0;
	virtual std::string getTeamID() const = 0;

	std::string getDesc() const {
		const auto& servers = getLastKnownServerInterfaces();
		std::string s = format("TeamID %s; ", getTeamID().c_str());
		s += format("Size %d; ", servers.size());
		for (int i = 0; i < servers.size(); i++) {
			if (i)
				s += ", ";
			s += servers[i].address().toString() + " " + servers[i].id().shortString();
		}
		return s;
	}
};

FDB_BOOLEAN_PARAM(WantNewServers);
FDB_BOOLEAN_PARAM(WantTrueBest);
FDB_BOOLEAN_PARAM(WantTrueBestIfMoveout);
FDB_BOOLEAN_PARAM(PreferLowerDiskUtil);
FDB_BOOLEAN_PARAM(TeamMustHaveShards);
FDB_BOOLEAN_PARAM(ForReadBalance);
FDB_BOOLEAN_PARAM(PreferLowerReadUtil);
FDB_BOOLEAN_PARAM(FindTeamByServers);
FDB_BOOLEAN_PARAM(PreferWithinShardLimit);
FDB_BOOLEAN_PARAM(FindTeamForBulkLoad);

class TeamSelect {
public:
	enum Value : int8_t {
		ANY = 0, // Any other situations except for the next two
		WANT_COMPLETE_SRCS, // Try best to select a healthy team consists of servers in completeSources
		WANT_TRUE_BEST, // Ask for the most or least utilized team in the cluster
	};
	TeamSelect() : value(ANY) {}
	TeamSelect(Value v) : value(v) {}
	std::string toString() const {
		switch (value) {
		case WANT_COMPLETE_SRCS:
			return "Want_Complete_Srcs";
		case WANT_TRUE_BEST:
			return "Want_True_Best";
		case ANY:
			return "Any";
		default:
			ASSERT(false);
		}
		return "";
	}

	bool operator==(const TeamSelect& tmpTeamSelect) { return value == tmpTeamSelect.value; }

private:
	Value value;
};

struct GetTeamRequest {
	TeamSelect teamSelect;
	bool preferLowerDiskUtil; // if true, lower utilized team has higher score
	bool teamMustHaveShards;
	bool forReadBalance;
	bool preferLowerReadUtil; // only make sense when forReadBalance is true
	bool preferWithinShardLimit;
	double inflightPenalty;
	bool findTeamByServers;
	bool findTeamForBulkLoad;
	Optional<KeyRange> keys;
	bool storageQueueAware = false;
	bool wantTrueBestIfMoveout = false;

	// completeSources have all shards in the key range being considered for movement, src have at least 1 shard in the
	// key range for movement. From the point of set, completeSources is the Intersection set of several <server_lists>,
	// while src is the Union set of them. E.g. keyRange = [Shard_1, Shard_2), and Shard_1 is located at {Server_1,
	// Server_2, Server_3}, Shard_2 is located at {Server_2, Server_3, Server_4}. completeSources = {Server_2,
	// Server_3}, src = {Server_1, Server_2, Server_3, Server_4}
	std::vector<UID> completeSources;
	std::vector<UID> src;

	Promise<std::pair<Optional<Reference<IDataDistributionTeam>>, bool>> reply;

	typedef Reference<IDataDistributionTeam> TeamRef;

	GetTeamRequest() {}
	GetTeamRequest(TeamSelect teamSelectRequest,
	               PreferLowerDiskUtil preferLowerDiskUtil,
	               TeamMustHaveShards teamMustHaveShards,
	               PreferLowerReadUtil preferLowerReadUtil,
	               PreferWithinShardLimit preferWithinShardLimit,
	               ForReadBalance forReadBalance = ForReadBalance::False,
	               double inflightPenalty = 1.0,
	               Optional<KeyRange> keys = Optional<KeyRange>())
	  : teamSelect(teamSelectRequest), storageQueueAware(false), preferLowerDiskUtil(preferLowerDiskUtil),
	    teamMustHaveShards(teamMustHaveShards), forReadBalance(forReadBalance),
	    preferLowerReadUtil(preferLowerReadUtil), preferWithinShardLimit(preferWithinShardLimit),
	    inflightPenalty(inflightPenalty), findTeamByServers(FindTeamByServers::False),
	    findTeamForBulkLoad(FindTeamForBulkLoad::False), keys(keys), wantTrueBestIfMoveout(false) {}
	GetTeamRequest(std::vector<UID> servers)
	  : teamSelect(TeamSelect::WANT_COMPLETE_SRCS), storageQueueAware(false),
	    preferLowerDiskUtil(PreferLowerDiskUtil::False), teamMustHaveShards(TeamMustHaveShards::False),
	    forReadBalance(ForReadBalance::False), preferLowerReadUtil(PreferLowerReadUtil::False),
	    preferWithinShardLimit(PreferWithinShardLimit::False), inflightPenalty(1.0),
	    findTeamByServers(FindTeamByServers::True), findTeamForBulkLoad(FindTeamForBulkLoad::False),
	    src(std::move(servers)), wantTrueBestIfMoveout(false) {}

	// return true if a.score < b.score
	[[nodiscard]] bool lessCompare(TeamRef a, TeamRef b, int64_t aLoadBytes, int64_t bLoadBytes) const {
		int res = 0;
		if (forReadBalance) {
			res = preferLowerReadUtil ? greaterReadLoad(a, b) : lessReadLoad(a, b);
		}
		return res == 0 ? lessCompareByLoad(aLoadBytes, bLoadBytes) : res < 0;
	}

	std::string getDesc() const {
		std::stringstream ss;

		ss << "TeamSelect:" << teamSelect.toString() << " StorageQueueAware:" << storageQueueAware
		   << " WantTrueBestIfMoveout:" << wantTrueBestIfMoveout << " PreferLowerDiskUtil:" << preferLowerDiskUtil
		   << " PreferLowerReadUtil:" << preferLowerReadUtil << " PreferWithinShardLimit:" << preferWithinShardLimit
		   << " teamMustHaveShards:" << teamMustHaveShards << " forReadBalance:" << forReadBalance
		   << " inflightPenalty:" << inflightPenalty << " findTeamByServers:" << findTeamByServers << ";";
		ss << "CompleteSources:";
		for (const auto& cs : completeSources) {
			ss << cs.toString() << ",";
		}

		return std::move(ss).str();
	}

private:
	// return true if preferHigherUtil && aLoadBytes <= bLoadBytes (higher load bytes has larger score)
	// or preferLowerUtil && aLoadBytes > bLoadBytes
	bool lessCompareByLoad(int64_t aLoadBytes, int64_t bLoadBytes) const {
		bool lessLoad = aLoadBytes <= bLoadBytes;
		return preferLowerDiskUtil ? !lessLoad : lessLoad;
	}

	// return -1 if a.readload > b.readload
	static int greaterReadLoad(TeamRef a, TeamRef b) {
		auto r1 = a->getReadLoad(true), r2 = b->getReadLoad(true);
		return r1 == r2 ? 0 : (r1 > r2 ? -1 : 1);
	}
	// return -1 if a.readload < b.readload
	static int lessReadLoad(TeamRef a, TeamRef b) {
		auto r1 = a->getReadLoad(false), r2 = b->getReadLoad(false);
		return r1 == r2 ? 0 : (r1 < r2 ? -1 : 1);
	}
};
