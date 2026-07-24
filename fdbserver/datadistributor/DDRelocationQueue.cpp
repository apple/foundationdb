/*
 * DataDistributionQueue.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include <algorithm>
#include <functional>
#include <limits>
#include <numeric>
#include <utility>
#include <vector>

#include "DataDistributionTeam.h"
#include "flow/ActorCollection.h"
#include "flow/Buggify.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/sim_validation.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/SystemData.h"
#include "DataDistribution.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/MoveKeys.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbrpc/simulator.h"
#include "DDTxnProcessor.h"
#include "flow/DebugTrace.h"
#include "DDRelocationQueue.h"
#include "TCInfo.h"
#include "flow/CoroUtils.h"
#include "flow/genericactors.actor.h"
#include "flow/SimpleCounter.h"

#define WORK_FULL_UTILIZATION 10000 // This is not a knob; it is a fixed point scaling factor!

using ITeamRef = Reference<IDataDistributionTeam>;
using SrcDestTeamPair = std::pair<ITeamRef, ITeamRef>;

inline bool isDataMovementForReadBalancing(DataMovementReason reason) {
	return reason == DataMovementReason::REBALANCE_READ_OVERUTIL_TEAM ||
	       reason == DataMovementReason::REBALANCE_READ_UNDERUTIL_TEAM;
}

inline bool isDataMovementForMountainChopper(DataMovementReason reason) {
	return reason == DataMovementReason::REBALANCE_OVERUTILIZED_TEAM ||
	       reason == DataMovementReason::REBALANCE_READ_OVERUTIL_TEAM;
}

// FIXME: Always use DataMovementReason to invoke these functions.
inline bool isValleyFillerPriority(int priority) {
	return priority == SERVER_KNOBS->PRIORITY_REBALANCE_UNDERUTILIZED_TEAM ||
	       priority == SERVER_KNOBS->PRIORITY_REBALANCE_READ_UNDERUTIL_TEAM;
}

inline bool isDataMovementForValleyFiller(DataMovementReason reason) {
	return reason == DataMovementReason::REBALANCE_UNDERUTILIZED_TEAM ||
	       reason == DataMovementReason::REBALANCE_READ_UNDERUTIL_TEAM;
}

RelocateData::RelocateData()
  : priority(-1), boundaryPriority(-1), healthPriority(-1), reason(RelocateReason::OTHER), startTime(-1),
    dataMoveId(anonymousShardId), workFactor(0), wantsNewServers(false), cancellable(false),
    interval("QueuedRelocation") {};

RelocateData::RelocateData(RelocateShard const& rs)
  : parent_range(rs.getParentRange()), keys(rs.keys), priority(rs.priority),
    boundaryPriority(rs.retryIntent.present() ? rs.retryIntent.get().boundaryPriority
                                              : (isBoundaryPriority(rs.priority) ? rs.priority : -1)),
    healthPriority(rs.retryIntent.present() ? rs.retryIntent.get().healthPriority
                                            : (isHealthPriority(rs.priority) ? rs.priority : -1)),
    reason(rs.reason), dmReason(rs.moveReason), startTime(now()),
    randomId(rs.traceId.isValid() ? rs.traceId : deterministicRandom()->randomUniqueID()), dataMoveId(rs.dataMoveId),
    workFactor(0),
    wantsNewServers(rs.retryIntent.present() ? rs.retryIntent.get().wantsNewServers
                                             : (isDataMovementForMountainChopper(rs.moveReason) ||
                                                isDataMovementForValleyFiller(rs.moveReason) ||
                                                rs.moveReason == DataMovementReason::SPLIT_SHARD ||
                                                rs.moveReason == DataMovementReason::TEAM_REDUNDANT ||
                                                rs.moveReason == DataMovementReason::REBALANCE_STORAGE_QUEUE)),
    cancellable(true), interval("QueuedRelocation", randomId), dataMove(rs.dataMove) {
	if (dataMove != nullptr) {
		this->src.insert(this->src.end(), dataMove->meta.src.begin(), dataMove->meta.src.end());
	}
}

bool RelocateData::isRestore() const {
	return this->dataMove != nullptr;
}

// Note: C++ standard library uses the Compare operator, uniqueness is determined by !comp(a, b) && !comp(b, a).
// So operator == and != is not used by std::set<RelocateData, std::greater<RelocateData>>
bool RelocateData::operator>(const RelocateData& rhs) const {
	if (priority != rhs.priority) {
		return priority > rhs.priority;
	} else if (startTime != rhs.startTime) {
		return startTime < rhs.startTime;
	} else if (randomId != rhs.randomId) {
		return randomId > rhs.randomId;
	} else if (keys.begin != rhs.keys.begin) {
		return keys.begin < rhs.keys.begin;
	} else {
		return keys.end < rhs.keys.end;
	}
}

bool RelocateData::operator==(const RelocateData& rhs) const {
	return priority == rhs.priority && boundaryPriority == rhs.boundaryPriority &&
	       healthPriority == rhs.healthPriority && reason == rhs.reason && keys == rhs.keys &&
	       startTime == rhs.startTime && workFactor == rhs.workFactor && src == rhs.src &&
	       completeSources == rhs.completeSources && wantsNewServers == rhs.wantsNewServers && randomId == rhs.randomId;
}
bool RelocateData::operator!=(const RelocateData& rhs) const {
	return !(*this == rhs);
}
Optional<KeyRange> RelocateData::getParentRange() const {
	return parent_range;
}

static RelocateShard makeDestinationFailureRetry(RelocateData const& rd, UID retryTraceId) {
	RelocateShard retry(rd.keys, rd.dmReason, rd.reason, retryTraceId);
	retry.priority = rd.priority;
	retry.retryIntent =
	    RelocateShard::RetryRelocationIntent{ rd.boundaryPriority, rd.healthPriority, rd.wantsNewServers };
	if (rd.getParentRange().present()) {
		retry.setParentRange(rd.getParentRange().get());
	}
	return retry;
}

static bool shouldRetryDestinationTeamFailure(bool doBulkLoading, RelocateData const&) {
	return !doBulkLoading;
}

static bool shouldYieldDestinationFailureRetry(RelocateData const& retry, RelocateData const& queued) {
	bool isSplit = retry.reason == RelocateReason::SIZE_SPLIT || retry.reason == RelocateReason::WRITE_SPLIT;
	return isSplit && retry.keys != queued.keys && retry.keys.contains(queued.keys);
}

class ParallelTCInfo final : public ReferenceCounted<ParallelTCInfo>, public IDataDistributionTeam {
	std::vector<Reference<IDataDistributionTeam>> teams;
	std::vector<UID> tempServerIDs;

	template <typename NUM>
	NUM sum(std::function<NUM(IDataDistributionTeam const&)> func) const {
		NUM result = 0;
		for (const auto& team : teams) {
			result += func(*team);
		}
		return result;
	}

	template <class T>
	std::vector<T> collect(std::function<std::vector<T>(IDataDistributionTeam const&)> func) const {
		std::vector<T> result;

		for (const auto& team : teams) {
			std::vector<T> newItems = func(*team);
			result.insert(result.end(), newItems.begin(), newItems.end());
		}
		return result;
	}

	bool any(std::function<bool(IDataDistributionTeam const&)> func) const {
		for (const auto& team : teams) {
			if (func(*team)) {
				return true;
			}
		}
		return false;
	}

public:
	ParallelTCInfo() = default;
	explicit ParallelTCInfo(ParallelTCInfo const& info) : teams(info.teams), tempServerIDs(info.tempServerIDs) {};

	void addTeam(Reference<IDataDistributionTeam> team) { teams.push_back(team); }

	void clear() { teams.clear(); }

	bool all(std::function<bool(IDataDistributionTeam const&)> func) const {
		return !any([func](IDataDistributionTeam const& team) { return !func(team); });
	}

	std::vector<StorageServerInterface> getLastKnownServerInterfaces() const override {
		return collect<StorageServerInterface>(
		    [](IDataDistributionTeam const& team) { return team.getLastKnownServerInterfaces(); });
	}

	int size() const override {
		int totalSize = 0;
		for (auto it = teams.begin(); it != teams.end(); it++) {
			totalSize += (*it)->size();
		}
		return totalSize;
	}

	std::vector<UID> const& getServerIDs() const override {
		static std::vector<UID> tempServerIDs;
		tempServerIDs.clear();
		for (const auto& team : teams) {
			std::vector<UID> const& childIDs = team->getServerIDs();
			tempServerIDs.insert(tempServerIDs.end(), childIDs.begin(), childIDs.end());
		}
		return tempServerIDs;
	}

	void addDataInFlightToTeam(int64_t delta) override {
		for (auto& team : teams) {
			team->addDataInFlightToTeam(delta);
		}
	}

	void addReadInFlightToTeam(int64_t delta) override {
		for (auto& team : teams) {
			team->addReadInFlightToTeam(delta);
		}
	}

	int64_t getDataInFlightToTeam() const override {
		return sum<int64_t>([](IDataDistributionTeam const& team) { return team.getDataInFlightToTeam(); });
	}

	int64_t getLoadBytes(bool includeInFlight = true, double inflightPenalty = 1.0) const override {
		return sum<int64_t>([includeInFlight, inflightPenalty](IDataDistributionTeam const& team) {
			return team.getLoadBytes(includeInFlight, inflightPenalty);
		});
	}

	int64_t getReadInFlightToTeam() const override {
		return sum<int64_t>([](IDataDistributionTeam const& team) { return team.getReadInFlightToTeam(); });
	}

	double getReadLoad(bool includeInFlight = true, double inflightPenalty = 1.0) const override {
		return sum<double>([includeInFlight, inflightPenalty](IDataDistributionTeam const& team) {
			return team.getReadLoad(includeInFlight, inflightPenalty);
		});
	}

	double getAverageCPU() const override {
		return sum<double>([](IDataDistributionTeam const& team) { return team.getAverageCPU(); }) / teams.size();
	}

	bool hasLowerCpu(double cpuThreshold) const override {
		return all([cpuThreshold](IDataDistributionTeam const& team) { return team.hasLowerCpu(cpuThreshold); });
	}

	Optional<int64_t> getLongestStorageQueueSize() const override {
		int64_t maxQueueSize = 0;
		for (const auto& team : teams) {
			Optional<int64_t> queueSize = team->getLongestStorageQueueSize();
			if (!queueSize.present()) {
				return Optional<int64_t>();
			}
			maxQueueSize = std::max(maxQueueSize, queueSize.get());
		}
		return maxQueueSize;
	}

	Optional<int> getMaxOngoingBulkLoadTaskCount() const override {
		int maxOngoingBulkLoadTaskCount = 0;
		for (const auto& team : teams) {
			Optional<int> ongoingBulkLoadTaskCount = team->getMaxOngoingBulkLoadTaskCount();
			if (!ongoingBulkLoadTaskCount.present()) {
				// If a SS tracker cannot get the metrics from the SS, it is possible that this SS has some healthy
				// issue. So, return an empty result to avoid choosing this server.
				return Optional<int>();
			}
			maxOngoingBulkLoadTaskCount = std::max(maxOngoingBulkLoadTaskCount, ongoingBulkLoadTaskCount.get());
		}
		return maxOngoingBulkLoadTaskCount;
	}

	int64_t getMinAvailableSpace(bool includeInFlight = true) const override {
		int64_t result = std::numeric_limits<int64_t>::max();
		for (const auto& team : teams) {
			result = std::min(result, team->getMinAvailableSpace(includeInFlight));
		}
		return result;
	}

	double getMinAvailableSpaceRatio(bool includeInFlight = true) const override {
		double result = std::numeric_limits<double>::max();
		for (const auto& team : teams) {
			result = std::min(result, team->getMinAvailableSpaceRatio(includeInFlight));
		}
		return result;
	}

	bool hasHealthyAvailableSpace(double minRatio) const override {
		return all([minRatio](IDataDistributionTeam const& team) { return team.hasHealthyAvailableSpace(minRatio); });
	}

	Future<Void> updateStorageMetrics() override {
		std::vector<Future<Void>> futures;

		for (auto& team : teams) {
			futures.push_back(team->updateStorageMetrics());
		}
		return waitForAll(futures);
	}

	bool isOptimal() const override {
		return all([](IDataDistributionTeam const& team) { return team.isOptimal(); });
	}

	bool isWrongConfiguration() const override {
		return any([](IDataDistributionTeam const& team) { return team.isWrongConfiguration(); });
	}
	void setWrongConfiguration(bool wrongConfiguration) override {
		for (auto it = teams.begin(); it != teams.end(); it++) {
			(*it)->setWrongConfiguration(wrongConfiguration);
		}
	}

	bool isHealthy() const override {
		return all([](IDataDistributionTeam const& team) { return team.isHealthy(); });
	}

	void setHealthy(bool h) override {
		for (auto it = teams.begin(); it != teams.end(); it++) {
			(*it)->setHealthy(h);
		}
	}

	int getPriority() const override {
		int priority = 0;
		for (auto it = teams.begin(); it != teams.end(); it++) {
			priority = std::max(priority, (*it)->getPriority());
		}
		return priority;
	}

	void setPriority(int p) override {
		for (auto it = teams.begin(); it != teams.end(); it++) {
			(*it)->setPriority(p);
		}
	}
	void addref() const override { ReferenceCounted<ParallelTCInfo>::addref(); }
	void delref() const override { ReferenceCounted<ParallelTCInfo>::delref(); }

	void addServers(const std::vector<UID>& servers) override {
		ASSERT(!teams.empty());
		teams[0]->addServers(servers);
	}

	std::string getTeamID() const override {
		std::string id;
		for (int i = 0; i < teams.size(); i++) {
			auto const& team = teams[i];
			id += (i == teams.size() - 1) ? team->getTeamID() : format("%s, ", team->getTeamID().c_str());
		}
		return id;
	}
};

bool Busyness::canLaunch(int prio, int work) const {
	ASSERT(prio > 0 && prio < 1000);
	return ledger[prio / 100] <= WORK_FULL_UTILIZATION - work; // allow for rounding errors in double division
}

void Busyness::addWork(int prio, int work) {
	ASSERT(prio > 0 && prio < 1000);
	for (int i = 0; i <= (prio / 100); i++)
		ledger[i] += work;
}

void Busyness::removeWork(int prio, int work) {
	addWork(prio, -work);
}

std::string Busyness::toString() {
	std::string result;
	for (int i = 1; i < ledger.size();) {
		int j = i + 1;
		while (j < ledger.size() && ledger[i] == ledger[j])
			j++;
		if (i != 1)
			result += ", ";
		result += i + 1 == j ? format("%03d", i * 100) : format("%03d/%03d", i * 100, (j - 1) * 100);
		result += format("=%1.02f (%d/%d)", (float)ledger[i] / WORK_FULL_UTILIZATION, ledger[i], WORK_FULL_UTILIZATION);
		i = j;
	}
	return result;
}

double adjustRelocationParallelismForSrc(double srcParallelism) {
	double res = srcParallelism;
	if (SERVER_KNOBS->ENABLE_CONSERVATIVE_RELOCATION_WHEN_REPLICA_CONSISTENCY_CHECK &&
	    SERVER_KNOBS->ENABLE_REPLICA_CONSISTENCY_CHECK_ON_DATA_MOVEMENT &&
	    srcParallelism >= 1.0 + SERVER_KNOBS->DATAMOVE_CONSISTENCY_CHECK_REQUIRED_REPLICAS) {
		// DATAMOVE_CONSISTENCY_CHECK_REQUIRED_REPLICAS is the number of extra replicas that the destination
		// servers will read from the source team.
		res = res / (1.0 + SERVER_KNOBS->DATAMOVE_CONSISTENCY_CHECK_REQUIRED_REPLICAS);
	}
	ASSERT(res > 0);
	return res;
}

// find the "workFactor" for this, were it launched now
int getSrcWorkFactor(RelocateData const& relocation, int singleRegionTeamSize) {
	// RELOCATION_PARALLELISM_PER_SOURCE_SERVER is the number of concurrent replications that can be launched on a
	// single storage server at a time, given the team size is 1 --- only this storage server is available to serve
	// fetchKey read requests from the dest team.
	// The real parallelism is adjusted by the number of source servers of a source team that can serve
	// fetchKey requests.
	// When ENABLE_REPLICA_CONSISTENCY_CHECK_ON_DATA_MOVEMENT is enabled, the fetchKeys on
	// destination servers will read from DATAMOVE_CONSISTENCY_CHECK_REQUIRED_REPLICAS + 1 replicas from the source team
	// (suppose the team size is large enough). As a result it is possible that the source team can be overloaded by the
	// fetchKey read requests. This is especially true when the shard split data movements are launched. So, we
	// introduce ENABLE_CONSERVATIVE_RELOCATION_WHEN_REPLICA_CONSISTENCY_CHECK knob to adjust the relocation parallelism
	// accordingly. The adjustment is to reduce the relocation parallelism by a factor of
	// (1 + DATAMOVE_CONSISTENCY_CHECK_REQUIRED_REPLICAS).
	if (relocation.bulkLoadTask.present()) {
		return 0;
	} else if (relocation.healthPriority == SERVER_KNOBS->PRIORITY_TEAM_1_LEFT ||
	           relocation.healthPriority == SERVER_KNOBS->PRIORITY_TEAM_0_LEFT) {
		return WORK_FULL_UTILIZATION /
		       adjustRelocationParallelismForSrc(SERVER_KNOBS->RELOCATION_PARALLELISM_PER_SOURCE_SERVER);
	} else if (relocation.healthPriority == SERVER_KNOBS->PRIORITY_TEAM_2_LEFT) {
		return WORK_FULL_UTILIZATION /
		       adjustRelocationParallelismForSrc(2 * SERVER_KNOBS->RELOCATION_PARALLELISM_PER_SOURCE_SERVER);
	} else if (relocation.healthPriority == SERVER_KNOBS->PRIORITY_PERPETUAL_STORAGE_WIGGLE) {
		// we want to set PRIORITY_PERPETUAL_STORAGE_WIGGLE to a reasonably large value
		// to make this parallelism take effect
		return WORK_FULL_UTILIZATION /
		       adjustRelocationParallelismForSrc(SERVER_KNOBS->WIGGLING_RELOCATION_PARALLELISM_PER_SOURCE_SERVER);
	} else if (relocation.priority == SERVER_KNOBS->PRIORITY_MERGE_SHARD) {
		return WORK_FULL_UTILIZATION /
		       adjustRelocationParallelismForSrc(SERVER_KNOBS->MERGE_RELOCATION_PARALLELISM_PER_TEAM);
	} else { // for now we assume that any message at a lower priority can best be assumed to have a full team left
		     // for work
		return WORK_FULL_UTILIZATION /
		       adjustRelocationParallelismForSrc(singleRegionTeamSize *
		                                         SERVER_KNOBS->RELOCATION_PARALLELISM_PER_SOURCE_SERVER);
	}
}

int getDestWorkFactor() {
	// Work of moving a shard is even across destination servers
	return WORK_FULL_UTILIZATION / SERVER_KNOBS->RELOCATION_PARALLELISM_PER_DEST_SERVER;
}

// Data movement's resource control: Do not overload servers used for the RelocateData
// return true if servers are not too busy to launch the relocation
// This ensure source servers will not be overloaded.
bool canLaunchSrc(RelocateData& relocation,
                  int teamSize,
                  int singleRegionTeamSize,
                  std::map<UID, Busyness>& busymap,
                  std::vector<RelocateData> cancellableRelocations) {
	// assert this has not already been launched
	ASSERT(relocation.workFactor == 0);
	ASSERT(!relocation.src.empty());
	ASSERT(teamSize >= singleRegionTeamSize);

	if (relocation.bulkLoadTask.present()) {
		// workFactor for bulk load task on source is always 0, therefore, we can safely launch
		// the data move with a bulk load task
		return true;
	}

	// find the "workFactor" for this, were it launched now
	int workFactor = getSrcWorkFactor(relocation, singleRegionTeamSize);
	int neededServers = std::min<int>(relocation.src.size(), teamSize - singleRegionTeamSize + 1);
	if (SERVER_KNOBS->USE_OLD_NEEDED_SERVERS) {
		neededServers = std::max(1, (int)relocation.src.size() - teamSize + 1);
	}
	// see if each of the SS can launch this task
	for (int i = 0; i < relocation.src.size(); i++) {
		// For each source server for this relocation, copy and modify its busyness to reflect work that WOULD be
		// cancelled
		auto busyCopy = busymap[relocation.src[i]];
		for (int j = 0; j < cancellableRelocations.size(); j++) {
			auto& servers = cancellableRelocations[j].src;
			if (std::count(servers.begin(), servers.end(), relocation.src[i]))
				busyCopy.removeWork(cancellableRelocations[j].priority, cancellableRelocations[j].workFactor);
		}
		// Use this modified busyness to check if this relocation could be launched
		if (busyCopy.canLaunch(relocation.priority, workFactor)) {
			--neededServers;
			if (neededServers == 0)
				return true;
		}
	}

	return false;
}

// candidateTeams is a vector containing one team per datacenter, the team(s) DD is planning on moving the shard to.
bool canLaunchDest(const std::vector<std::pair<Reference<IDataDistributionTeam>, bool>>& candidateTeams,
                   int priority,
                   std::map<UID, Busyness>& busymapDest) {
	// fail switch if this is causing issues
	if (SERVER_KNOBS->RELOCATION_PARALLELISM_PER_DEST_SERVER <= 0) {
		return true;
	}
	int workFactor = getDestWorkFactor();
	for (auto& [team, _] : candidateTeams) {
		for (UID id : team->getServerIDs()) {
			if (!busymapDest[id].canLaunch(priority, workFactor)) {
				return false;
			}
		}
	}
	return true;
}

// update busyness for each server
void launch(RelocateData& relocation, std::map<UID, Busyness>& busymap, int singleRegionTeamSize) {
	// if we are here this means that we can launch and should adjust all the work the servers can do
	relocation.workFactor = getSrcWorkFactor(relocation, singleRegionTeamSize);
	for (int i = 0; i < relocation.src.size(); i++)
		busymap[relocation.src[i]].addWork(relocation.priority, relocation.workFactor);
}

void launchDest(RelocateData& relocation,
                const std::vector<std::pair<Reference<IDataDistributionTeam>, bool>>& candidateTeams,
                std::map<UID, Busyness>& destBusymap) {
	ASSERT(relocation.completeDests.empty());
	int destWorkFactor = getDestWorkFactor();
	for (auto& [team, _] : candidateTeams) {
		for (UID id : team->getServerIDs()) {
			relocation.completeDests.push_back(id);
			destBusymap[id].addWork(relocation.priority, destWorkFactor);
		}
	}
}
void completeDest(RelocateData const& relocation, std::map<UID, Busyness>& destBusymap) {
	int destWorkFactor = getDestWorkFactor();
	for (UID id : relocation.completeDests) {
		destBusymap[id].removeWork(relocation.priority, destWorkFactor);
	}
}

static void completeOwnedDest(RelocateData const& relocation,
                              std::map<UID, Busyness>& destBusymap,
                              bool& ownsDestBusyness) {
	if (ownsDestBusyness) {
		completeDest(relocation, destBusymap);
		ownsDestBusyness = false;
	}
}

static void resetDestinationsForRetry(RelocateData& relocation,
                                      ParallelTCInfo& healthyDestinations,
                                      std::map<UID, Busyness>& destBusymap,
                                      bool& ownsDestBusyness) {
	completeOwnedDest(relocation, destBusymap, ownsDestBusyness);
	relocation.completeDests.clear();
	healthyDestinations.clear();
}

void complete(RelocateData const& relocation, std::map<UID, Busyness>& busymap, std::map<UID, Busyness>& destBusymap) {
	ASSERT(relocation.bulkLoadTask.present() || relocation.workFactor > 0);
	for (int i = 0; i < relocation.src.size(); i++)
		busymap[relocation.src[i]].removeWork(relocation.priority, relocation.workFactor);

	completeDest(relocation, destBusymap);
}

// Cancels in-flight data moves intersecting with range.
Future<Void> cancelDataMove(class DDQueue* self, KeyRange range, const DDEnabledState* ddEnabledState);

Future<Void> dataDistributionRelocator(class DDQueue* self,
                                       RelocateData rd,
                                       Future<Void> prevCleanup,
                                       const DDEnabledState* ddEnabledState);

Future<Void> getSourceServersForRange(DDQueue* self,
                                      RelocateData input,
                                      PromiseStream<RelocateData> output,
                                      Reference<FlowLock> fetchLock) {

	// FIXME: is the merge case needed
	if (input.priority == SERVER_KNOBS->PRIORITY_MERGE_SHARD) {
		co_await delay(0.5, TaskPriority::DataDistributionVeryLow);
	} else {
		co_await delay(0.0001, TaskPriority::DataDistributionLaunch);
	}

	co_await fetchLock->take(TaskPriority::DataDistributionLaunch);
	FlowLock::Releaser releaser(*fetchLock);

	IDDTxnProcessor::SourceServers res = co_await self->txnProcessor->getSourceServersForRange(input.keys);
	input.src = std::move(res.srcServers);
	input.completeSources = std::move(res.completeSources);
	output.send(input);
}

DDQueue::DDQueue(DDQueueInitParams const& params)
  : IDDRelocationQueue(), distributorId(params.id), lock(params.lock), txnProcessor(params.db),
    teamCollections(params.teamCollections), shardsAffectedByTeamFailure(params.shardsAffectedByTeamFailure),
    physicalShardCollection(params.physicalShardCollection), bulkLoadTaskCollection(params.bulkLoadTaskCollection),
    getAverageShardBytes(params.getAverageShardBytes),
    startMoveKeysParallelismLock(SERVER_KNOBS->DD_MOVE_KEYS_PARALLELISM),
    finishMoveKeysParallelismLock(SERVER_KNOBS->DD_MOVE_KEYS_PARALLELISM),
    cleanUpDataMoveParallelismLock(SERVER_KNOBS->DD_MOVE_KEYS_PARALLELISM),
    fetchSourceLock(new FlowLock(SERVER_KNOBS->DD_FETCH_SOURCE_PARALLELISM)), activeRelocations(0),
    queuedRelocations(0), pendingGateRelocations(0), bytesWritten(0), teamSize(params.teamSize),
    singleRegionTeamSize(params.singleRegionTeamSize), pipelineFull(new AsyncVar<bool>(false)),
    output(params.relocationProducer), input(params.relocationConsumer), getShardMetrics(params.getShardMetrics),
    getTopKMetrics(params.getTopKMetrics), lastInterval(0), suppressIntervals(0),
    rawProcessingUnhealthy(new AsyncVar<bool>(false)), rawProcessingWiggle(new AsyncVar<bool>(false)),
    unhealthyRelocations(0), movedKeyServersEventHolder(makeReference<EventCacheHolder>("MovedKeyServers")),
    moveReusePhysicalShard(0), moveCreateNewPhysicalShard(0),
    retryFindDstReasonCount(static_cast<int>(RetryFindDstReason::NumberOfTypes), 0),
    moveBytesRate(SERVER_KNOBS->DD_TRACE_MOVE_BYTES_AVERAGE_INTERVAL) {}

void DDQueue::updatePipelineFull() {
	if (pipelineSize() >= SERVER_KNOBS->DD_MAX_PIPELINE_MOVES && !pipelineFull->get()) {
		pipelineFull->set(true);
		TraceEvent("DDPipelineFullSet", distributorId)
		    .suppressFor(30.0)
		    .detail("PipelineSize", pipelineSize())
		    .detail("PendingGateRelocations", pendingGateRelocations)
		    .detail("PipelineLimit", SERVER_KNOBS->DD_MAX_PIPELINE_MOVES);
		CODE_PROBE(true, "DD Pipeline Full");
	} else if (pipelineSize() < SERVER_KNOBS->DD_MAX_PIPELINE_MOVES && pipelineFull->get()) {
		pipelineFull->set(false);
		TraceEvent("DDPipelineFullCleared", distributorId)
		    .suppressFor(30.0)
		    .detail("PipelineSize", pipelineSize())
		    .detail("PendingGateRelocations", pendingGateRelocations)
		    .detail("PipelineLimit", SERVER_KNOBS->DD_MAX_PIPELINE_MOVES);
	}
}

void DDQueue::startRelocation(int priority, int healthPriority) {
	// Although PRIORITY_TEAM_REDUNDANT has lower priority than split and merge shard movement,
	// we must count it into unhealthyRelocations; because team removers relies on unhealthyRelocations to
	// ensure a team remover will not start before the previous one finishes removing a team and move away data
	// NOTE: split and merge shard have higher priority. If they have to wait for unhealthyRelocations = 0,
	// deadlock may happen: split/merge shard waits for unhealthyRelocations, while blocks team_redundant.
	if (healthPriority == SERVER_KNOBS->PRIORITY_POPULATE_REGION ||
	    healthPriority == SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY ||
	    healthPriority == SERVER_KNOBS->PRIORITY_TEAM_2_LEFT || healthPriority == SERVER_KNOBS->PRIORITY_TEAM_1_LEFT ||
	    healthPriority == SERVER_KNOBS->PRIORITY_TEAM_0_LEFT ||
	    healthPriority == SERVER_KNOBS->PRIORITY_TEAM_REDUNDANT) {
		unhealthyRelocations++;
		rawProcessingUnhealthy->set(true);
	}
	if (healthPriority == SERVER_KNOBS->PRIORITY_PERPETUAL_STORAGE_WIGGLE) {
		rawProcessingWiggle->set(true);
	}
	priority_relocations[priority]++;
	updatePipelineFull();
}

void DDQueue::finishRelocation(int priority, int healthPriority) {
	if (healthPriority == SERVER_KNOBS->PRIORITY_POPULATE_REGION ||
	    healthPriority == SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY ||
	    healthPriority == SERVER_KNOBS->PRIORITY_TEAM_2_LEFT || healthPriority == SERVER_KNOBS->PRIORITY_TEAM_1_LEFT ||
	    healthPriority == SERVER_KNOBS->PRIORITY_TEAM_0_LEFT ||
	    healthPriority == SERVER_KNOBS->PRIORITY_TEAM_REDUNDANT) {
		unhealthyRelocations--;
		ASSERT(unhealthyRelocations >= 0);
		if (unhealthyRelocations == 0) {
			rawProcessingUnhealthy->set(false);
		}
	}
	priority_relocations[priority]--;
	updatePipelineFull();
	if (priority_relocations[SERVER_KNOBS->PRIORITY_PERPETUAL_STORAGE_WIGGLE] == 0) {
		rawProcessingWiggle->set(false);
	}
}

void DDQueue::validate() {
	if (EXPENSIVE_VALIDATION) {
		for (auto it = fetchingSourcesQueue.begin(); it != fetchingSourcesQueue.end(); ++it) {
			// relocates in the fetching queue do not have src servers yet.
			if (!it->src.empty()) {
				TraceEvent(SevError, "DDQueueValidateError1")
				    .detail("Problem", "relocates in the fetching queue do not have src servers yet");
			}

			// relocates in the fetching queue do not have a work factor yet.
			if (it->workFactor != 0.0)
				TraceEvent(SevError, "DDQueueValidateError2")
				    .detail("Problem", "relocates in the fetching queue do not have a work factor yet");

			// relocates in the fetching queue are in the queueMap.
			auto range = queueMap.rangeContaining(it->keys.begin);
			if (range.value() != *it || range.range() != it->keys)
				TraceEvent(SevError, "DDQueueValidateError3")
				    .detail("Problem", "relocates in the fetching queue are in the queueMap");
		}

		/*
		for( auto it = queue.begin(); it != queue.end(); ++it ) {
		    for( auto rdit = it->second.begin(); rdit != it->second.end(); ++rdit ) {
		        // relocates in the queue are in the queueMap exactly.
		        auto range = queueMap.rangeContaining( rdit->keys.begin );
		        if( range.value() != *rdit || range.range() != rdit->keys )
		            TraceEvent(SevError, "DDQueueValidateError4").detail("Problem", "relocates in the queue are in the queueMap exactly")
		            .detail("RangeBegin", range.range().begin)
		            .detail("RangeEnd", range.range().end)
		            .detail("RelocateBegin2", range.value().keys.begin)
		            .detail("RelocateEnd2", range.value().keys.end)
		            .detail("RelocateStart", range.value().startTime)
		            .detail("MapStart", rdit->startTime)
		            .detail("RelocateWork", range.value().workFactor)
		            .detail("MapWork", rdit->workFactor)
		            .detail("RelocateSrc", range.value().src.size())
		            .detail("MapSrc", rdit->src.size())
		            .detail("RelocatePrio", range.value().priority)
		            .detail("MapPrio", rdit->priority);

		        // relocates in the queue have src servers
		        if( !rdit->src.size() )
		            TraceEvent(SevError, "DDQueueValidateError5").detail("Problem", "relocates in the queue have src servers");

		        // relocates in the queue do not have a work factor yet.
		        if( rdit->workFactor != 0.0 )
		            TraceEvent(SevError, "DDQueueValidateError6").detail("Problem", "relocates in the queue do not have a work factor yet");

		        bool contains = false;
		        for( int i = 0; i < rdit->src.size(); i++ ) {
		            if( rdit->src[i] == it->first ) {
		                contains = true;
		                break;
		            }
		        }
		        if( !contains )
		            TraceEvent(SevError, "DDQueueValidateError7").detail("Problem", "queued relocate data does not include ss under which its filed");
		    }
		}*/

		auto inFlightRanges = inFlight.ranges();
		for (auto it = inFlightRanges.begin(); it != inFlightRanges.end(); ++it) {
			for (int i = 0; i < it->value().src.size(); i++) {
				// each server in the inFlight map is in the busymap
				if (!busymap.contains(it->value().src[i]))
					TraceEvent(SevError, "DDQueueValidateError8")
					    .detail("Problem", "each server in the inFlight map is in the busymap");

				// relocate data that is inFlight is not also in the queue
				if (queue[it->value().src[i]].contains(it->value()))
					TraceEvent(SevError, "DDQueueValidateError9")
					    .detail("Problem", "relocate data that is inFlight is not also in the queue");
			}

			for (int i = 0; i < it->value().completeDests.size(); i++) {
				// each server in the inFlight map is in the dest busymap
				if (!destBusymap.contains(it->value().completeDests[i]))
					TraceEvent(SevError, "DDQueueValidateError10")
					    .detail("Problem", "each server in the inFlight map is in the destBusymap");
			}

			// in flight relocates have source servers
			if (it->value().startTime != -1 && it->value().src.empty()) {
				TraceEvent(SevError, "DDQueueValidateError11")
				    .detail("Problem", "in flight relocates have source servers");
			}

			if (inFlightActors.liveActorAt(it->range().begin)) {
				// the key range in the inFlight map matches the key range in the RelocateData message
				if (it->value().keys != it->range()) {
					TraceEvent(SevError, "DDQueueValidateError12")
					    .detail("Problem",
					            "the key range in the inFlight map matches the key range in the RelocateData message");
				}
			} else if (it->value().cancellable) {
				TraceEvent(SevError, "DDQueueValidateError13")
				    .detail("Problem", "key range is cancellable but not in flight!")
				    .detail("Range", it->range());
			}
		}

		for (auto it = busymap.begin(); it != busymap.end(); ++it) {
			for (int i = 0; i < it->second.ledger.size() - 1; i++) {
				if (it->second.ledger[i] < it->second.ledger[i + 1]) {
					TraceEvent(SevError, "DDQueueValidateError14")
					    .detail("Problem", "ascending ledger problem")
					    .detail("LedgerLevel", i)
					    .detail("LedgerValueA", it->second.ledger[i])
					    .detail("LedgerValueB", it->second.ledger[i + 1]);
				}
				if (it->second.ledger[i] < 0.0) {
					TraceEvent(SevError, "DDQueueValidateError15")
					    .detail("Problem", "negative ascending problem")
					    .detail("LedgerLevel", i)
					    .detail("LedgerValue", it->second.ledger[i]);
				}
			}
		}

		for (auto it = destBusymap.begin(); it != destBusymap.end(); ++it) {
			for (int i = 0; i < it->second.ledger.size() - 1; i++) {
				if (it->second.ledger[i] < it->second.ledger[i + 1]) {
					TraceEvent(SevError, "DDQueueValidateError16")
					    .detail("Problem", "ascending ledger problem")
					    .detail("LedgerLevel", i)
					    .detail("LedgerValueA", it->second.ledger[i])
					    .detail("LedgerValueB", it->second.ledger[i + 1]);
				}
				if (it->second.ledger[i] < 0.0) {
					TraceEvent(SevError, "DDQueueValidateError17")
					    .detail("Problem", "negative ascending problem")
					    .detail("LedgerLevel", i)
					    .detail("LedgerValue", it->second.ledger[i]);
				}
			}
		}

		std::set<RelocateData, std::greater<RelocateData>> queuedRelocationsMatch;
		for (auto it = queue.begin(); it != queue.end(); ++it)
			queuedRelocationsMatch.insert(it->second.begin(), it->second.end());
		ASSERT(queuedRelocations == queuedRelocationsMatch.size() + fetchingSourcesQueue.size());

		int testActive = 0;
		for (auto it = priority_relocations.begin(); it != priority_relocations.end(); ++it)
			testActive += it->second;
		ASSERT(activeRelocations + queuedRelocations == testActive);
	}
}

void DDQueue::processRelocationComplete(const RelocateData& done) {
	activeRelocations--;
	TraceEvent(SevVerbose, "InFlightRelocationChange")
	    .detail("Complete", done.dataMoveId)
	    .detail("IsRestore", done.isRestore())
	    .detail("Total", activeRelocations);
	finishRelocation(done.priority, done.healthPriority);
	fetchKeysComplete.erase(done);
	if (g_network->isSimulated() && debug_isCheckRelocationDuration() && now() - done.startTime > 60) {
		TraceEvent(SevWarnAlways, "RelocationDurationTooLong").detail("Duration", now() - done.startTime);
		debug_setCheckRelocationDuration(false);
	}
}

void DDQueue::queueRelocation(RelocateShard rs, std::set<UID>& serversToLaunchFrom) {
	//TraceEvent("QueueRelocationBegin").detail("Begin", rd.keys.begin).detail("End", rd.keys.end);

	// remove all items from both queues that are fully contained in the new relocation (i.e. will be overwritten)
	bool destinationFailureRetry = rs.retryIntent.present();
	RelocateData rd(rs);
	if (destinationFailureRetry) {
		auto ranges = queueMap.intersectingRanges(rd.keys);
		for (auto r = ranges.begin(); r != ranges.end(); ++r) {
			RelocateData const& queued = r->value();
			if (!shouldYieldDestinationFailureRetry(rd, queued)) {
				continue;
			}

			bool active = fetchingSourcesQueue.contains(queued);
			if (!active && !queued.src.empty()) {
				auto sourceQueue = queue.find(queued.src.front());
				active = sourceQueue != queue.end() && sourceQueue->second.contains(queued);
			}
			if (active) {
				TraceEvent(SevInfo, "DestinationFailureRetryYieldedToQueuedSplit", distributorId)
				    .detail("RetryRange", rd.keys)
				    .detail("RetryTraceID", rd.randomId)
				    .detail("QueuedRange", queued.keys)
				    .detail("QueuedTraceID", queued.randomId);
				return;
			}
		}
	}
	bool hasHealthPriority = RelocateData::isHealthPriority(rd.priority);
	bool hasBoundaryPriority = RelocateData::isBoundaryPriority(rd.priority);

	auto ranges = queueMap.intersectingRanges(rd.keys);
	for (auto r = ranges.begin(); r != ranges.end(); ++r) {
		RelocateData& rrs = r->value();

		auto fetchingSourcesItr = fetchingSourcesQueue.find(rrs);
		bool foundActiveFetching = fetchingSourcesItr != fetchingSourcesQueue.end();
		std::set<RelocateData, std::greater<RelocateData>>* firstQueue;
		std::set<RelocateData, std::greater<RelocateData>>::iterator firstRelocationItr;
		bool foundActiveRelocation = false;

		if (!foundActiveFetching && !rrs.src.empty()) {
			firstQueue = &queue[rrs.src[0]];
			firstRelocationItr = firstQueue->find(rrs);
			foundActiveRelocation = firstRelocationItr != firstQueue->end();
		}

		// If there is a queued job that wants data relocation which we are about to cancel/modify,
		//  make sure that we keep the relocation intent for the job that we queue up
		if (foundActiveFetching || foundActiveRelocation) {
			rd.wantsNewServers |= rrs.wantsNewServers;
			rd.startTime = std::min(rd.startTime, rrs.startTime);
			if (!hasHealthPriority) {
				rd.healthPriority = std::max(rd.healthPriority, rrs.healthPriority);
			}
			if (!hasBoundaryPriority) {
				rd.boundaryPriority = std::max(rd.boundaryPriority, rrs.boundaryPriority);
			}
			rd.priority = std::max(rd.priority, std::max(rd.boundaryPriority, rd.healthPriority));
		}

		if (rd.keys.contains(rrs.keys)) {
			if (foundActiveFetching)
				fetchingSourcesQueue.erase(fetchingSourcesItr);
			else if (foundActiveRelocation) {
				firstQueue->erase(firstRelocationItr);
				for (int i = 1; i < rrs.src.size(); i++)
					queue[rrs.src[i]].erase(rrs);
			}
		}

		if (foundActiveFetching || foundActiveRelocation) {
			serversToLaunchFrom.insert(rrs.src.begin(), rrs.src.end());
			/*TraceEvent(rrs.interval.end(), mi.id()).detail("Result","Cancelled")
			    .detail("WasFetching", foundActiveFetching).detail("Contained", rd.keys.contains( rrs.keys ));*/
			queuedRelocations--;
			TraceEvent(SevVerbose, "QueuedRelocationsChanged")
			    .detail("DataMoveID", rrs.dataMoveId)
			    .detail("RandomID", rrs.randomId)
			    .detail("Total", queuedRelocations);
			finishRelocation(rrs.priority, rrs.healthPriority);
		}
	}

	// determine the final state of the relocations map
	auto affectedQueuedItems = queueMap.getAffectedRangesAfterInsertion(rd.keys, rd);

	// put the new request into the global map of requests (modifies the ranges already present)
	queueMap.insert(rd.keys, rd);

	// cancel all the getSourceServers actors that intersect the new range that we will be getting
	getSourceActors.cancel(KeyRangeRef(affectedQueuedItems.front().begin, affectedQueuedItems.back().end));

	// update fetchingSourcesQueue and the per-server queue based on truncated ranges after insertion, (re-)launch
	// getSourceServers
	auto queueMapItr = queueMap.rangeContaining(affectedQueuedItems[0].begin);

	// Put off erasing elements from fetchingSourcesQueue
	std::set<RelocateData, std::greater<RelocateData>> delayDelete;
	for (int r = 0; r < affectedQueuedItems.size(); ++r, ++queueMapItr) {
		// ASSERT(queueMapItr->value() == queueMap.rangeContaining(affectedQueuedItems[r].begin)->value());
		RelocateData& rrs = queueMapItr->value();

		if (rrs.src.empty() && (rrs.keys == rd.keys || fetchingSourcesQueue.contains(rrs))) {
			if (rrs.keys != rd.keys) {
				delayDelete.insert(rrs);
			}

			rrs.keys = affectedQueuedItems[r];
			rrs.interval = TraceInterval("QueuedRelocation", rrs.randomId); // inherit the old randomId

			DebugRelocationTraceEvent(rrs.interval.begin(), distributorId)
			    .detail("KeyBegin", rrs.keys.begin)
			    .detail("KeyEnd", rrs.keys.end)
			    .detail("Priority", rrs.priority)
			    .detail("WantsNewServers", rrs.wantsNewServers);

			queuedRelocations++;
			TraceEvent(SevVerbose, "QueuedRelocationsChanged")
			    .detail("DataMoveID", rrs.dataMoveId)
			    .detail("RandomID", rrs.randomId)
			    .detail("Total", queuedRelocations);
			startRelocation(rrs.priority, rrs.healthPriority);

			fetchingSourcesQueue.insert(rrs);
			getSourceActors.insert(rrs.keys,
			                       getSourceServersForRange(this, rrs, fetchSourceServersComplete, fetchSourceLock));
		} else {
			RelocateData newData(rrs);
			newData.keys = affectedQueuedItems[r];
			ASSERT(!rrs.src.empty() || rrs.startTime == -1);

			bool foundActiveRelocation = false;
			for (int i = 0; i < rrs.src.size(); i++) {
				auto& serverQueue = queue[rrs.src[i]];

				if (serverQueue.erase(rrs) > 0) {
					if (!foundActiveRelocation) {
						newData.interval = TraceInterval("QueuedRelocation", rrs.randomId); // inherit the old randomId

						DebugRelocationTraceEvent(newData.interval.begin(), distributorId)
						    .detail("KeyBegin", newData.keys.begin)
						    .detail("KeyEnd", newData.keys.end)
						    .detail("Priority", newData.priority)
						    .detail("WantsNewServers", newData.wantsNewServers);

						queuedRelocations++;
						TraceEvent(SevVerbose, "QueuedRelocationsChanged")
						    .detail("DataMoveID", newData.dataMoveId)
						    .detail("RandomID", newData.randomId)
						    .detail("Total", queuedRelocations);
						startRelocation(newData.priority, newData.healthPriority);
						foundActiveRelocation = true;
					}

					serverQueue.insert(newData);
				} else {
					break;
				}
			}

			// We update the keys of a relocation even if it is "dead" since it helps validate()
			rrs.keys = affectedQueuedItems[r];
			rrs.interval = newData.interval;
		}
	}

	for (const auto& it : delayDelete) {
		fetchingSourcesQueue.erase(it);
	}
	DebugRelocationTraceEvent("ReceivedRelocateShard", distributorId)
	    .detail("KeyBegin", rd.keys.begin)
	    .detail("KeyEnd", rd.keys.end)
	    .detail("Priority", rd.priority)
	    .detail("AffectedRanges", affectedQueuedItems.size());
}

void DDQueue::completeSourceFetch(const RelocateData& results) {
	ASSERT(fetchingSourcesQueue.contains(results));

	// logRelocation( results, "GotSourceServers" );

	fetchingSourcesQueue.erase(results);
	queueMap.insert(results.keys, results);
	for (int i = 0; i < results.src.size(); i++) {
		queue[results.src[i]].insert(results);
	}
	updateLastAsSource(results.src);
	serverCounter.increaseForTeam(results.src, results.reason, ServerCounter::CountType::QueuedSource);
}

void DDQueue::logRelocation(const RelocateData& rd, const char* title) {
	std::string busyString;
	for (int i = 0; i < rd.src.size() && i < teamSize * 2; i++)
		busyString += describe(rd.src[i]) + " - (" + busymap[rd.src[i]].toString() + "); ";

	TraceEvent(title, distributorId)
	    .detail("KeyBegin", rd.keys.begin)
	    .detail("KeyEnd", rd.keys.end)
	    .detail("Priority", rd.priority)
	    .detail("WorkFactor", rd.workFactor)
	    .detail("SourceServerCount", rd.src.size())
	    .detail("SourceServers", describe(rd.src, teamSize * 2))
	    .detail("SourceBusyness", busyString);
}

void DDQueue::launchQueuedWork(KeyRange keys, const DDEnabledState* ddEnabledState) {
	// combine all queued work in the key range and check to see if there is anything to launch
	std::set<RelocateData, std::greater<RelocateData>> combined;
	auto f = queueMap.intersectingRanges(keys);
	for (auto it = f.begin(); it != f.end(); ++it) {
		if (!it->value().src.empty() && queue[it->value().src[0]].contains(it->value()))
			combined.insert(it->value());
	}
	launchQueuedWork(combined, ddEnabledState);
}

void DDQueue::launchQueuedWork(const std::set<UID>& serversToLaunchFrom, const DDEnabledState* ddEnabledState) {
	// combine all work from the source servers to see if there is anything new to launch
	std::set<RelocateData, std::greater<RelocateData>> combined;
	for (auto id : serversToLaunchFrom) {
		auto& queuedWork = queue[id];
		auto it = queuedWork.begin();
		for (int j = 0; j < teamSize && it != queuedWork.end(); j++) {
			combined.insert(*it);
			++it;
		}
	}
	launchQueuedWork(combined, ddEnabledState);
}

void DDQueue::launchQueuedWork(RelocateData launchData, const DDEnabledState* ddEnabledState) {
	// check a single RelocateData to see if it can be launched
	std::set<RelocateData, std::greater<RelocateData>> combined;
	combined.insert(launchData);
	launchQueuedWork(combined, ddEnabledState);
}

DataMoveType newDataMoveType(bool doBulkLoading) {
	DataMoveType type = DataMoveType::LOGICAL;
	if (deterministicRandom()->random01() < SERVER_KNOBS->DD_PHYSICAL_SHARD_MOVE_PROBABILITY) {
		type = DataMoveType::PHYSICAL;
	}
	if (type != DataMoveType::PHYSICAL && SERVER_KNOBS->ENABLE_PHYSICAL_SHARD_MOVE_EXPERIMENT) {
		type = DataMoveType::PHYSICAL_EXP;
	}
	if (doBulkLoading) {
		if (type == DataMoveType::LOGICAL) {
			type = DataMoveType::LOGICAL_BULKLOAD;
		} else if (type == DataMoveType::PHYSICAL || type == DataMoveType::PHYSICAL_EXP) {
			type = DataMoveType::PHYSICAL_BULKLOAD;
		} else {
			UNREACHABLE();
		}
	}
	return type;
}

bool runPendingBulkLoadTaskWithRelocateData(DDQueue* self, RelocateData& rd) {
	bool doBulkLoading = false;
	Optional<DDBulkLoadEngineTask> task = self->bulkLoadTaskCollection->getTaskByRange(rd.keys);
	if (task.present() && task.get().coreState.onAnyPhase({ BulkLoadPhase::Triggered, BulkLoadPhase::Running })) {
		rd.bulkLoadTask = task.get();
		doBulkLoading = true;
	}
	if (doBulkLoading) {
		try {
			self->bulkLoadTaskCollection->startTask(rd.bulkLoadTask.get().coreState);
		} catch (Error& e) {
			ASSERT_WE_THINK(e.code() == error_code_bulkload_task_outdated);
			if (e.code() == error_code_bulkload_task_outdated) {
				TraceEvent(SevError, "DDBulkLoadTaskOutdatedWhenStartRelocator", self->distributorId)
				    .detail("NewDataMoveID", rd.dataMoveId)
				    .detail("NewDataMovePriority", rd.priority)
				    .detail("NewDataMoveRange", rd.keys)
				    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
				    .detail("TaskJobID", rd.bulkLoadTask.get().coreState.getJobId())
				    .detail("TaskRange", rd.bulkLoadTask.get().coreState.getRange())
				    .detail("TaskDataMoveID",
				            rd.bulkLoadTask.get().coreState.getDataMoveId().present()
				                ? rd.bulkLoadTask.get().coreState.getDataMoveId().get().toString()
				                : "");
				throw movekeys_conflict();
			} else {
				throw e;
			}
		}
	}
	return doBulkLoading;
}

// For each relocateData rd in the queue, check if there exist inflight relocate data whose keyrange is overlapped
// with rd. If there exist, cancel them by cancelling their actors and reducing the src servers' busyness of those
// canceled inflight relocateData. Launch the relocation for the rd.
void DDQueue::launchQueuedWork(std::set<RelocateData, std::greater<RelocateData>> combined,
                               const DDEnabledState* ddEnabledState) {
	[[maybe_unused]] int startedHere = 0;
	double startTime = now();
	// kick off relocators from items in the queue as need be
	for (auto it = combined.begin(); it != combined.end(); it++) {
		RelocateData rd(*it);
		// A restored move can be held behind the pipeline gate while shard-encoded metadata is disabled.
		// Restart DD before mutating the queue so the move is cancelled by the rollback path.
		if (rd.isRestore() && !SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
			throw dd_config_changed();
		}

		// If having a bulk load task overlapping the rd range,
		// attach bulk load task to the input rd if rd is not a data move
		// for unhealthy. Make the bulk load task visible on the global task map
		bool doBulkLoading = runPendingBulkLoadTaskWithRelocateData(this, rd);
		if (doBulkLoading) {
			TraceEvent(bulkLoadVerboseEventSev(), "DDBulkLoadTaskLaunchingDataMove", this->distributorId)
			    .detail("NewDataMoveId", rd.dataMoveId)
			    .detail("NewDataMovePriority", rd.priority)
			    .detail("NewDataMoveRange", rd.keys)
			    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
			    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId());
		}

		// Check if there is an inflight shard that is overlapped with the queued relocateShard (rd)
		bool overlappingInFlight = false;
		auto intersectingInFlight = inFlight.intersectingRanges(rd.keys);
		for (auto it = intersectingInFlight.begin(); it != intersectingInFlight.end(); ++it) {
			if (fetchKeysComplete.contains(it->value()) && inFlightActors.liveActorAt(it->range().begin) &&
			    !rd.keys.contains(it->range()) && it->value().priority >= rd.priority &&
			    rd.healthPriority < SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY) {

				DebugRelocationTraceEvent("OverlappingInFlight", distributorId)
				    .detail("KeyBegin", it->value().keys.begin)
				    .detail("KeyEnd", it->value().keys.end)
				    .detail("Priority", it->value().priority);

				overlappingInFlight = true;
				break;
			}
		}

		if (overlappingInFlight) {
			ASSERT(!rd.isRestore());
			// logRelocation( rd, "SkippingOverlappingInFlight" );
			continue;
		}

		// Because the busyness of a server is decreased when a superseding relocation is issued, we
		//  need to consider what the busyness of a server WOULD be if
		auto containedRanges = inFlight.containedRanges(rd.keys);
		std::vector<RelocateData> cancellableRelocations;
		for (auto it = containedRanges.begin(); it != containedRanges.end(); ++it) {
			if (it.value().cancellable) {
				cancellableRelocations.push_back(it->value());
			}
		}

		// Data movement avoids overloading source servers in moving data.
		// SOMEDAY: the list of source servers may be outdated since they were fetched when the work was put in the
		// queue
		// FIXME: we need spare capacity even when we're just going to be cancelling work via TEAM_HEALTHY
		if (!rd.isRestore() && !canLaunchSrc(rd, teamSize, singleRegionTeamSize, busymap, cancellableRelocations)) {
			// logRelocation( rd, "SkippingQueuedRelocation" );
			if (rd.bulkLoadTask.present()) {
				TraceEvent(SevError, "DDBulkLoadTaskDelayedByBusySrc", this->distributorId)
				    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
				    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
				    .detail("TaskRange", rd.bulkLoadTask.get().coreState.getRange());
			}
			continue;
		}

		// From now on, the source servers for the RelocateData rd have enough resource to move the data away,
		// because they do not have too much inflight data movement.

		// logRelocation( rd, "LaunchingRelocation" );
		DebugRelocationTraceEvent(rd.interval.end(), distributorId).detail("Result", "Success");

		if (!rd.isRestore()) {
			queuedRelocations--;
			TraceEvent(SevVerbose, "QueuedRelocationsChanged")
			    .detail("DataMoveID", rd.dataMoveId)
			    .detail("RandomID", rd.randomId)
			    .detail("Total", queuedRelocations);
			finishRelocation(rd.priority, rd.healthPriority);

			// now we are launching: remove this entry from the queue of all the src servers
			for (size_t i = 0; i < rd.src.size(); i++) {
				const auto result = queue[rd.src[i]].erase(rd);
				ASSERT(result);
			}
		}

		// If there is a job in flight that wants data relocation which we are about to cancel/modify,
		//     make sure that we keep the relocation intent for the job that we launch
		auto f = inFlight.intersectingRanges(rd.keys);
		for (auto it = f.begin(); it != f.end(); ++it) {
			if (inFlightActors.liveActorAt(it->range().begin)) {
				rd.wantsNewServers |= it->value().wantsNewServers;
			}
		}
		startedHere++;

		// update both inFlightActors and inFlight key range maps, cancelling deleted RelocateShards
		std::vector<KeyRange> ranges;
		inFlightActors.getRangesAffectedByInsertion(rd.keys, ranges);
		inFlightActors.cancel(KeyRangeRef(ranges.front().begin, ranges.back().end));
		// The cancelDataMove feature assumes inFlightActors are immediately cancelled.
		// If this is not true, multiple inflightActors can have overlapped range,
		// which leads to conflicts of moving keys

		Future<Void> fCleanup =
		    SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA ? cancelDataMove(this, rd.keys, ddEnabledState) : Void();

		inFlight.insert(rd.keys, rd);
		for (int r = 0; r < ranges.size(); r++) {
			RelocateData& rrs = inFlight.rangeContaining(ranges[r].begin)->value();
			rrs.keys = ranges[r];
			if (rrs.bulkLoadTask.present() && rrs.bulkLoadTask.get().coreState.getRange() != rrs.keys) {
				// The new bulk load data move partially overwrites an old bulk load data move.
				// In this case, the old bulk load task is cancelled.
				// For the range that is not covered by the new data move, drop the bulk load task and
				// run it as a normal data move.
				ASSERT(rrs.bulkLoadTask.get().coreState.getRange().contains(rrs.keys));
				rrs.bulkLoadTask.reset();
			}
			if (rd.keys == ranges[r] && rd.isRestore()) {
				ASSERT(rd.dataMove != nullptr);
				ASSERT(SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
				rrs.dataMoveId = rd.dataMove->meta.id;
			} else {
				// Restored data moves can split ordinary relocations, but not other restored data moves.
				ASSERT_WE_THINK(!rd.isRestore() || !rrs.isRestore());
				// TODO(psm): The shard id is determined by DD.
				rrs.dataMove.reset();
				if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
					if (SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD) {
						rrs.dataMoveId = UID();
					} else if (rrs.bulkLoadTask.present()) {
						// We have to decide this after prevCleanup completes.
						// For details, see the comment in dataDistributionRelocator.
						rrs.dataMoveId = UID();
					} else {
						DataMoveType dataMoveType = newDataMoveType(rrs.bulkLoadTask.present());
						rrs.dataMoveId = newDataMoveId(
						    deterministicRandom()->randomUInt64(), AssignEmptyRange::False, dataMoveType, rrs.dmReason);
						TraceEvent(SevInfo, "NewDataMoveWithRandomDestID", this->distributorId)
						    .detail("DataMoveID", rrs.dataMoveId.toString())
						    .detail("TrackID", rrs.randomId)
						    .detail("Range", rrs.keys)
						    .detail("Reason", rrs.reason.toString())
						    .detail("DataMoveType", dataMoveType)
						    .detail("DataMoveReason", static_cast<int>(rrs.dmReason));
					}
				} else {
					rrs.dataMoveId = anonymousShardId;
					TraceEvent(SevInfo, "NewDataMoveWithAnonymousDestID", this->distributorId)
					    .detail("DataMoveID", rrs.dataMoveId.toString())
					    .detail("TrackID", rrs.randomId)
					    .detail("Range", rrs.keys)
					    .detail("Reason", rrs.reason.toString())
					    .detail("DataMoveReason", static_cast<int>(rrs.dmReason));
				}
			}

			launch(rrs, busymap, singleRegionTeamSize);
			activeRelocations++;
			TraceEvent(SevVerbose, "InFlightRelocationChange")
			    .detail("Launch", rrs.dataMoveId)
			    .detail("Total", activeRelocations);
			startRelocation(rrs.priority, rrs.healthPriority);
			// Start the actor that relocates data in the rrs.keys
			inFlightActors.insert(rrs.keys, dataDistributionRelocator(this, rrs, fCleanup, ddEnabledState));
		}

		// logRelocation( rd, "LaunchedRelocation" );
	}
	if (now() - startTime > .001 && deterministicRandom()->random01() < 0.001)
		TraceEvent(SevWarnAlways, "LaunchingQueueSlowx1000").detail("Elapsed", now() - startTime);

	/*if( startedHere > 0 ) {
	    TraceEvent("StartedDDRelocators", distributorId)
	        .detail("QueueSize", queuedRelocations)
	        .detail("StartedHere", startedHere)
	        .detail("ActiveRelocations", activeRelocations);
	} */

	validate();
}

int DDQueue::getHighestPriorityRelocation() const {
	int highestPriority{ 0 };
	for (const auto& [priority, count] : priority_relocations) {
		if (count > 0) {
			highestPriority = std::max(highestPriority, priority);
		}
	}
	return highestPriority;
}

// return true if the servers are throttled as source for read rebalance
bool DDQueue::timeThrottle(const std::vector<UID>& ids) const {
	return std::any_of(ids.begin(), ids.end(), [this](const UID& id) {
		if (this->lastAsSource.contains(id)) {
			return (now() - this->lastAsSource.at(id)) * SERVER_KNOBS->READ_REBALANCE_SRC_PARALLELISM <
			       SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL;
		}
		return false;
	});
}

void DDQueue::updateLastAsSource(const std::vector<UID>& ids, double t) {
	for (auto& id : ids)
		lastAsSource[id] = t;
}

// Schedules cancellation of a data move.
void DDQueue::enqueueCancelledDataMove(UID dataMoveId, KeyRange range, const DDEnabledState* ddEnabledState) {
	std::vector<Future<Void>> cleanup;
	auto f = this->dataMoves.intersectingRanges(range);
	for (auto it = f.begin(); it != f.end(); ++it) {
		if (it->value().isValid()) {
			TraceEvent(SevError, "DDEnqueueCancelledDataMoveConflict", this->distributorId)
			    .detail("DataMoveID", dataMoveId)
			    .detail("CancelledRange", range)
			    .detail("ConflictingDataMoveID", it->value().id)
			    .detail("ConflictingRange", KeyRangeRef(it->range().begin, it->range().end));
			return;
		}
	}

	DDQueue::DDDataMove dataMove(dataMoveId);
	dataMove.cancel = cleanUpDataMove(txnProcessor->context(),
	                                  dataMoveId,
	                                  this->lock,
	                                  &this->cleanUpDataMoveParallelismLock,
	                                  range,
	                                  ddEnabledState,
	                                  this->addBackgroundCleanUpDataMoveActor);
	this->dataMoves.insert(range, dataMove);
	TraceEvent(SevInfo, "DDEnqueuedCancelledDataMove", this->distributorId)
	    .detail("DataMoveID", dataMoveId)
	    .detail("Range", range);
}

void DDQueue::refreshCounter() {
	serverCounter.traceAll(distributorId);
	serverCounter.clear();
}

Future<Void> DDQueue::periodicalRefreshCounter() {
	return recurring(std::bind_front(&DDQueue::refreshCounter, this), SERVER_KNOBS->DD_QUEUE_COUNTER_REFRESH_INTERVAL);
}

int DDQueue::getUnhealthyRelocationCount() const {
	return unhealthyRelocations;
}

// Cancel existing relocation if exists, and serialize all concurrent relocations
Future<Void> cancelDataMove(class DDQueue* self, KeyRange range, const DDEnabledState* ddEnabledState) {
	std::vector<Future<Void>> cleanup;
	std::vector<std::pair<KeyRange, UID>> lastObservedDataMoves;

	try {
		lastObservedDataMoves.clear();
		auto f = self->dataMoves.intersectingRanges(range);
		for (auto it = f.begin(); it != f.end(); ++it) {
			if (!it->value().isValid()) {
				continue;
			}
			KeyRange keys = KeyRangeRef(it->range().begin, it->range().end);
			TraceEvent(SevInfo, "DDQueueCancelDataMove", self->distributorId)
			    .detail("DataMoveID", it->value().id)
			    .detail("DataMoveRange", keys)
			    .detail("Range", range);
			if (!it->value().cancel.isValid()) {
				it->value().cancel = cleanUpDataMove(self->txnProcessor->context(),
				                                     it->value().id,
				                                     self->lock,
				                                     &self->cleanUpDataMoveParallelismLock,
				                                     keys,
				                                     ddEnabledState,
				                                     self->addBackgroundCleanUpDataMoveActor);
			}
			lastObservedDataMoves.push_back(std::make_pair(keys, it->value().id));
			cleanup.push_back(it->value().cancel);
		}

		co_await waitForAll(cleanup);

		// Since self->dataMoves can only be updated by relocator,
		// and any old relocator has been cancelled (by overwrite inflightActors)
		// Thus, any update of self->dataMoves during the wait must come from
		// a newer relocator.
		// This cancelDataMove should be transparent to the new relocator
		std::vector<KeyRange> toResetRanges;
		for (const auto& observedDataMove : lastObservedDataMoves) {
			auto f = self->dataMoves.intersectingRanges(observedDataMove.first);
			for (auto it = f.begin(); it != f.end(); ++it) {
				if (it->value().id != observedDataMove.second) {
					TraceEvent(SevInfo, "DataMoveWrittenByConcurrentDataMove", self->distributorId)
					    .detail("Range", range)
					    .detail("OldRange", observedDataMove.first)
					    .detail("LastObservedDataMoveID", observedDataMove.second)
					    .detail("CurrentDataMoveID", it->value().id);
				} else {
					ASSERT(!it->value().isValid() || (it->value().cancel.isValid() && it->value().cancel.isReady()));
					toResetRanges.push_back(Standalone(it->range()));
				}
			}
		}
		for (auto& toResetRange : toResetRanges) {
			self->dataMoves.insert(toResetRange, DDQueue::DDDataMove());
		}

	} catch (Error& e) {
		throw e;
	}
}

static std::string destServersString(std::vector<std::pair<Reference<IDataDistributionTeam>, bool>> const& bestTeams) {
	std::stringstream ss;

	for (auto& tc : bestTeams) {
		for (const auto& id : tc.first->getServerIDs()) {
			ss << id.toString() << " ";
		}
	}

	return std::move(ss).str();
}

void traceRelocateDecision(TraceEvent& ev, const UID& pairId, const RelocateDecision& decision) {
	ev.detail("PairId", pairId)
	    .detail("Priority", decision.rd.priority)
	    .detail("KeyBegin", decision.rd.keys.begin)
	    .detail("KeyEnd", decision.rd.keys.end)
	    .detail("Reason", decision.rd.reason.toString())
	    .detail("SourceServers", describe(decision.rd.src))
	    .detail("DestinationTeam", describe(decision.destIds))
	    .detail("ExtraIds", describe(decision.extraIds));
	if (SERVER_KNOBS->DD_ENABLE_VERBOSE_TRACING) {
		// StorageMetrics is the rd shard's metrics, e.g., bytes and write bandwidth
		ev.detail("StorageMetrics", decision.metrics.toString());
	}

	if (decision.rd.reason == RelocateReason::WRITE_SPLIT) {
		// tell if the splitter acted as expected for write bandwidth splitting
		// SOMEDAY: trace the source team write bytes if necessary
		ev.detail("ShardWriteBytes", decision.metrics.bytesWrittenPerKSecond)
		    .detail("ParentShardWriteBytes", decision.parentMetrics.get().bytesWrittenPerKSecond);
	} else if (decision.rd.reason == RelocateReason::SIZE_SPLIT) {
		ev.detail("ShardSize", decision.metrics.bytes).detail("ParentShardSize", decision.parentMetrics.get().bytes);
	}
}

static int nonOverlappedServerCount(const std::vector<UID>& srcIds, const std::vector<UID>& destIds) {
	std::unordered_set<UID> srcSet{ srcIds.begin(), srcIds.end() };
	int count = 0;
	for (int i = 0; i < destIds.size(); i++) {
		if (!srcSet.contains(destIds[i])) {
			count++;
		}
	}
	return count;
}

void validateBulkLoadRelocateData(const RelocateData& rd, const std::vector<UID>& destIds, UID logId) {
	if (rd.keys != rd.bulkLoadTask.get().coreState.getRange()) {
		TraceEvent(SevError, "DDBulkLoadTaskLaunchFailed", logId)
		    .detail("Reason", "Wrong data move range")
		    .detail("DataMovePriority", rd.priority)
		    .detail("DataMoveId", rd.dataMoveId)
		    .detail("RelocatorRange", rd.keys)
		    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
		    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
		    .detail("TaskRange", rd.bulkLoadTask.get().coreState.getRange());
		throw movekeys_conflict();
		// Very important invariant. If this error appears, check the logic
	}
	for (const auto& destId : destIds) {
		if (std::find(rd.src.begin(), rd.src.end(), destId) != rd.src.end()) {
			// In this case, getTeam has to select src as dest when remote team collection is not ready
			// This is not expected
			TraceEvent(SevError, "DDBulkLoadEngineTaskLaunchFailed", logId)
			    .detail("Reason", "Conflict src and destd due to remote recovery")
			    .detail("DataMovePriority", rd.priority)
			    .detail("DataMoveId", rd.dataMoveId)
			    .detail("RelocatorRange", rd.keys)
			    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
			    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
			    .detail("TaskRange", rd.bulkLoadTask.get().coreState.getRange());
			throw movekeys_conflict();
		}
	}
	return;
}

// With probability, set wantTrueBestIfMoveout true for teamUnhealthy data moves and teamRedundant data moves only.
// This flag takes effect in getTeam. When the flag is set true, DD always getBestTeam for teamRedundant data moves and
// do getBestTeam for a teamRedundant data move if the data move decides to move data out of a SS.
bool getWantTrueBestIfMoveout(int priority) {
	if (priority == SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY) {
		return deterministicRandom()->random01() <
		       SERVER_KNOBS->PROBABILITY_TEAM_UNHEALTHY_DATAMOVE_CHOOSE_TRUE_BEST_DEST;
	} else if (priority == SERVER_KNOBS->PRIORITY_TEAM_REDUNDANT) {
		return deterministicRandom()->random01() <
		       SERVER_KNOBS->PROBABILITY_TEAM_REDUNDANT_DATAMOVE_CHOOSE_TRUE_BEST_DEST;
	} else {
		return false;
	}
}

// This actor relocates the specified keys to a good place.
// The inFlightActor key range map stores the actor for each RelocateData
Future<Void> dataDistributionRelocator(DDQueue* self,
                                       RelocateData rd,
                                       Future<Void> prevCleanup,
                                       const DDEnabledState* ddEnabledState) {
	Promise<Void> errorOut(self->error);
	TraceInterval relocateShardInterval("RelocateShard", rd.randomId);
	PromiseStream<RelocateData> dataTransferComplete(self->dataTransferComplete);
	PromiseStream<RelocateData> relocationComplete(self->relocationComplete);
	bool signalledTransferComplete = false;
	// Source transfer completion is shared across retries, while each attempt registers new destination work.
	bool ownsDestBusyness = false;
	bool retryAfterDestinationTeamFailure = false;
	UID distributorId = self->distributorId;
	ParallelTCInfo healthyDestinations;

	bool anyHealthy = false;
	bool allHealthy = true;
	bool anyWithSource = false;
	bool anyDestOverloaded = false;
	int destOverloadedCount = 0;
	int stuckCount = 0;
	std::vector<std::pair<Reference<IDataDistributionTeam>, bool>> bestTeams;
	double startTime = now();
	std::vector<UID> destIds;
	WantTrueBest wantTrueBest(isValleyFillerPriority(rd.priority));
	WantTrueBestIfMoveout wantTrueBestIfMoveout(getWantTrueBestIfMoveout(rd.priority));
	uint64_t debugID = deterministicRandom()->randomUInt64();
	bool enableShardMove = SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD;

	// We will decide doBulkLoading after prevCleanup completes.
	// rd.bulkLoadTask.present() is just the default value.
	bool doBulkLoading = rd.bulkLoadTask.present();

	Error err;
	try {
		if (now() - self->lastInterval < 1.0) {
			relocateShardInterval.severity = SevDebug;
			self->suppressIntervals++;
		}

		TraceEvent(relocateShardInterval.begin(), distributorId)
		    .detail("KeyBegin", rd.keys.begin)
		    .detail("KeyEnd", rd.keys.end)
		    .detail("Priority", rd.priority)
		    .detail("WantTrueBestIfMoveout", wantTrueBestIfMoveout)
		    .detail("SuppressedEventCount", self->suppressIntervals);

		if (relocateShardInterval.severity != SevDebug) {
			self->lastInterval = now();
			self->suppressIntervals = 0;
		}

		if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
			auto inFlightRange = self->inFlight.rangeContaining(rd.keys.begin);
			ASSERT(inFlightRange.range() == rd.keys);
			ASSERT(inFlightRange.value().randomId == rd.randomId);
			ASSERT(inFlightRange.value().dataMoveId == rd.dataMoveId);
			inFlightRange.value().cancellable = false;

			co_await prevCleanup;

			if (doBulkLoading) {
				// Wait until the overlapped old bulkload relocator get cancelled.
				// At this point, the existing metadata is updated by the old relocator.
				// No more change will be made to the metadata.
				// At this point, we check if the current rd.bulkLoadTask is outdated.
				// If yes, do not do bulkload.
				Transaction tr(self->txnProcessor->context());
				while (true) {
					Error innerErr;
					try {
						BulkLoadTaskState currentBulkLoadTaskState =
						    co_await getBulkLoadTask(&tr,
						                             rd.bulkLoadTask.get().coreState.getRange(),
						                             rd.bulkLoadTask.get().coreState.getTaskId(),
						                             { BulkLoadPhase::Triggered, BulkLoadPhase::Running });
						TraceEvent(bulkLoadVerboseEventSev(), "DDBulkLoadTaskDataMoveLaunched", self->distributorId)
						    .detail("TrackID", rd.randomId)
						    .detail("DataMovePriority", rd.priority)
						    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
						    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
						    .detail("TaskRange", rd.bulkLoadTask.get().coreState.getRange())
						    .detail("ExistingJobID", currentBulkLoadTaskState.getJobId())
						    .detail("ExistingTaskID", currentBulkLoadTaskState.getTaskId())
						    .detail("ExistingTaskRange", currentBulkLoadTaskState.getRange());
						break;
					} catch (Error& e) {
						innerErr = e;
					}
					if (innerErr.code() == error_code_bulkload_task_outdated) {
						// Notify the bulkload task actor to exit. This avoid bulkload task engine
						// gets stuck in case a new task overwrite this task on metadata. To schedule
						// the new task, the old task actor has to exit.
						if (rd.bulkLoadTask.get().completeAck.canBeSet()) {
							rd.bulkLoadTask.get().completeAck.sendError(bulkload_task_outdated());
						}
						doBulkLoading = false;
						// we do not want to reset rd.bulkLoadTask here because
						// the busyness calculation is based on whether rd.bulkLoadTask is set.
						// When calculating the busyness, we do not count the work for any
						// rd with bulkLoadTask set.
						// TODO(BulkLoad): reset rd.bulkLoadTask here for the risk of overloading the source
						// servers.
						TraceEvent(SevWarn, "DDBulkLoadTaskFallbackToNormalDataMove", self->distributorId)
						    .detail("TrackID", rd.randomId)
						    .detail("DataMovePriority", rd.priority)
						    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
						    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
						    .detail("TaskRange", rd.bulkLoadTask.get().coreState.getRange());
						break;
					}
					co_await tr.onError(innerErr);
				}
				DataMoveType dataMoveType = newDataMoveType(doBulkLoading);
				rd.dataMoveId = newDataMoveId(
				    deterministicRandom()->randomUInt64(), AssignEmptyRange::False, dataMoveType, rd.dmReason);
				TraceEvent(bulkLoadVerboseEventSev(), "DDBulkLoadTaskNewDataMoveID", self->distributorId)
				    .detail("DataMoveID", rd.dataMoveId.toString())
				    .detail("TrackID", rd.randomId)
				    .detail("Range", rd.keys)
				    .detail("Priority", rd.priority)
				    .detail("DataMoveType", dataMoveType)
				    .detail("DoBulkLoading", doBulkLoading)
				    .detail("DataMoveReason", static_cast<int>(rd.dmReason));
			}

			auto f = self->dataMoves.intersectingRanges(rd.keys);
			for (auto it = f.begin(); it != f.end(); ++it) {
				KeyRangeRef kr(it->range().begin, it->range().end);
				const UID mId = it->value().id;
				if (mId.isValid() && mId != rd.dataMoveId) {
					TraceEvent("DDRelocatorConflictingDataMove", distributorId)
					    .detail("CurrentDataMoveID", rd.dataMoveId)
					    .detail("DataMoveID", mId)
					    .detail("Range", kr);
				}
			}
			if (rd.isRestore() || !SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD) {
				if (SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD) {
					ASSERT(rd.dataMoveId.isValid());
				}
				self->dataMoves.insert(rd.keys, DDQueue::DDDataMove(rd.dataMoveId));
			}
			// TODO: split-brain issue for physical shard move
			// the update of inflightActor and the update of dataMoves is not atomic
			// Currently, the relocator is triggered based on the inflightActor info.
			// In future, we should assert at here that the intersecting DataMove in self->dataMoves
			// are all invalid. i.e. the range of new relocators is match to the range of prevCleanup.
		}

		Optional<StorageMetrics> parentMetrics;
		StorageMetrics metrics;

		Future<StorageMetrics> metricsF =
		    brokenPromiseToNever(self->getShardMetrics.getReply(GetMetricsRequest(rd.keys)));
		if (rd.getParentRange().present()) {
			Future<StorageMetrics> parentMetricsF =
			    brokenPromiseToNever(self->getShardMetrics.getReply(GetMetricsRequest(rd.getParentRange().get())));
			co_await (store(metrics, metricsF) && store(parentMetrics, parentMetricsF));
		} else {
			metrics = co_await metricsF;
		}

		std::unordered_set<uint64_t> excludedDstPhysicalShards;

		ASSERT(!rd.src.empty());
		while (true) {
			destOverloadedCount = 0;
			stuckCount = 0;
			uint64_t physicalShardIDCandidate = UID().first();
			bool forceToUseNewPhysicalShard = false;
			while (true) {
				int tciIndex = 0;
				bool foundTeams = true;
				bool bestTeamReady = false;
				anyHealthy = false;
				allHealthy = true;
				anyWithSource = false;
				anyDestOverloaded = false;
				bestTeams.clear();
				// Get team from teamCollections in different DCs and find the best one
				while (tciIndex < self->teamCollections.size()) {
					if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && rd.isRestore()) {
						auto req = GetTeamRequest(tciIndex == 0 ? rd.dataMove->primaryDest : rd.dataMove->remoteDest);
						req.keys = rd.keys;
						Future<std::pair<Optional<Reference<IDataDistributionTeam>>, bool>> fbestTeam =
						    brokenPromiseToNever(self->teamCollections[tciIndex].getTeam.getReply(req));
						bestTeamReady = fbestTeam.isReady();
						std::pair<Optional<Reference<IDataDistributionTeam>>, bool> bestTeam = co_await fbestTeam;
						if (tciIndex > 0 && !bestTeamReady) {
							// self->shardsAffectedByTeamFailure->moveShard must be called without any waits after
							// getting the destination team or we could miss failure notifications for the storage
							// servers in the destination team
							TraceEvent("BestTeamNotReady")
							    .detail("TraceID", rd.randomId)
							    .detail("TeamCollectionIndex", tciIndex)
							    .detail("RestoreDataMoveForDest",
							            describe(tciIndex == 0 ? rd.dataMove->primaryDest : rd.dataMove->remoteDest));
							self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::RemoteBestTeamNotReady]++;
							foundTeams = false;
							break;
						}
						if (!bestTeam.first.present() || !bestTeam.first.get()->isHealthy()) {
							self->retryFindDstReasonCount[tciIndex == 0
							                                  ? DDQueue::RetryFindDstReason::PrimaryNoHealthyTeam
							                                  : DDQueue::RetryFindDstReason::RemoteNoHealthyTeam]++;
							foundTeams = false;
							break;
						}
						anyHealthy = true;
						bestTeams.emplace_back(bestTeam.first.get(), bestTeam.second);
						if (doBulkLoading) {
							TraceEvent(bulkLoadVerboseEventSev(), "DDBulkLoadTaskSelectDestTeam", self->distributorId)
							    .detail("Context", "Restore")
							    .detail("SrcIds", describe(rd.src))
							    .detail("DestIds", bestTeam.first.get()->getServerIDs())
							    .detail("DestTeam", bestTeam.first.get()->getTeamID())
							    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
							    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
							    .detail("TaskRange", rd.bulkLoadTask.get().coreState.getRange())
							    .detail("Priority", rd.priority)
							    .detail("DataMoveId", rd.dataMoveId)
							    .detail("Primary", tciIndex == 0);
						}
					} else {
						double inflightPenalty = SERVER_KNOBS->INFLIGHT_PENALTY_HEALTHY;
						if (rd.healthPriority == SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY ||
						    rd.healthPriority == SERVER_KNOBS->PRIORITY_TEAM_2_LEFT)
							inflightPenalty = SERVER_KNOBS->INFLIGHT_PENALTY_UNHEALTHY;
						if (rd.healthPriority == SERVER_KNOBS->PRIORITY_POPULATE_REGION ||
						    rd.healthPriority == SERVER_KNOBS->PRIORITY_TEAM_1_LEFT ||
						    rd.healthPriority == SERVER_KNOBS->PRIORITY_TEAM_0_LEFT)
							inflightPenalty = SERVER_KNOBS->INFLIGHT_PENALTY_ONE_LEFT;

						TeamSelect destTeamSelect;
						if (!rd.wantsNewServers) {
							destTeamSelect = TeamSelect::WANT_COMPLETE_SRCS;
						} else if (wantTrueBest) {
							destTeamSelect = TeamSelect::WANT_TRUE_BEST;
						} else {
							destTeamSelect = TeamSelect::ANY;
						}
						PreferLowerReadUtil preferLowerReadTeam =
						    SERVER_KNOBS->DD_PREFER_LOW_READ_UTIL_TEAM || rd.reason == RelocateReason::REBALANCE_READ
						        ? PreferLowerReadUtil::True
						        : PreferLowerReadUtil::False;
						auto req = GetTeamRequest(destTeamSelect,
						                          PreferLowerDiskUtil::True,
						                          TeamMustHaveShards::False,
						                          preferLowerReadTeam,
						                          PreferWithinShardLimit::True,
						                          ForReadBalance(rd.reason == RelocateReason::REBALANCE_READ),
						                          inflightPenalty,
						                          rd.keys);

						req.src = rd.src;
						req.completeSources = rd.completeSources;
						req.storageQueueAware = SERVER_KNOBS->ENABLE_STORAGE_QUEUE_AWARE_TEAM_SELECTION;
						req.findTeamForBulkLoad = doBulkLoading;
						req.wantTrueBestIfMoveout = wantTrueBestIfMoveout;

						if (enableShardMove && tciIndex == 1) {
							ASSERT(physicalShardIDCandidate != UID().first() &&
							       physicalShardIDCandidate != anonymousShardId.first());
							std::pair<Optional<ShardsAffectedByTeamFailure::Team>, bool> remoteTeamWithPhysicalShard =
							    self->physicalShardCollection->tryGetAvailableRemoteTeamWith(
							        physicalShardIDCandidate, metrics, debugID);
							if (!remoteTeamWithPhysicalShard.second) {
								// Physical shard with `physicalShardIDCandidate` is not available. Retry selecting
								// new dst physical shard.
								self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::NoAvailablePhysicalShard]++;
								foundTeams = false;
								break;
							}
							if (remoteTeamWithPhysicalShard.first.present()) {
								// Exists a remoteTeam in the mapping that has the physicalShardIDCandidate
								// use the remoteTeam with the physicalShard as the bestTeam
								req = GetTeamRequest(remoteTeamWithPhysicalShard.first.get().servers);
								req.keys = rd.keys;
							}
						}

						// bestTeam.second = false if the bestTeam in the teamCollection (in the DC) does not have
						// any server that hosts the relocateData. This is possible, for example, in a fearless
						// configuration when the remote DC is just brought up.
						Future<std::pair<Optional<Reference<IDataDistributionTeam>>, bool>> fbestTeam =
						    brokenPromiseToNever(self->teamCollections[tciIndex].getTeam.getReply(req));
						bestTeamReady = fbestTeam.isReady();
						std::pair<Optional<Reference<IDataDistributionTeam>>, bool> bestTeam = co_await fbestTeam;
						if (doBulkLoading) {
							TraceEvent(bulkLoadVerboseEventSev(),
							           "DDBulkLoadTaskRelocatorBestTeamReceived",
							           self->distributorId)
							    .detail("DataMoveID", rd.dataMoveId)
							    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
							    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
							    .detail("BestTeamReady", bestTeamReady);
						}
						if (tciIndex > 0 && !bestTeamReady) {
							// self->shardsAffectedByTeamFailure->moveShard must be called without any waits after
							// getting the destination team or we could miss failure notifications for the storage
							// servers in the destination team
							TraceEvent("BestTeamNotReady");
							self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::RemoteBestTeamNotReady]++;
							foundTeams = false;
							break;
						}
						// If a DC has no healthy team, we stop checking the other DCs until
						// the unhealthy DC is healthy again or is excluded.
						if (!bestTeam.first.present()) {
							self->retryFindDstReasonCount[tciIndex == 0
							                                  ? DDQueue::RetryFindDstReason::PrimaryNoHealthyTeam
							                                  : DDQueue::RetryFindDstReason::RemoteNoHealthyTeam]++;
							foundTeams = false;
							break;
						}
						if (!bestTeam.first.get()->isHealthy()) {
							allHealthy = false;
						} else {
							anyHealthy = true;
						}

						if (bestTeam.second) {
							anyWithSource = true;
						}

						if (enableShardMove) {
							if (tciIndex == 1 && !forceToUseNewPhysicalShard) {
								// critical to the correctness of team selection by PhysicalShardCollection
								// tryGetAvailableRemoteTeamWith() enforce to select a remote team paired with a
								// primary team Thus, tryGetAvailableRemoteTeamWith() may select an almost full
								// remote team In this case, we must re-select a remote team We set foundTeams =
								// false to avoid finishing team selection Then, forceToUseNewPhysicalShard is set,
								// which enforce to use getTeam to select a remote team
								bool minAvailableSpaceRatio = bestTeam.first.get()->getMinAvailableSpaceRatio(true);
								if (minAvailableSpaceRatio < SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO) {
									self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::RemoteTeamIsFull]++;
									foundTeams = false;
									break;
								}

								// critical to the correctness of team selection by PhysicalShardCollection
								// tryGetAvailableRemoteTeamWith() enforce to select a remote team paired with a
								// primary team Thus, tryGetAvailableRemoteTeamWith() may select an unhealthy remote
								// team In this case, we must re-select a remote team We set foundTeams = false to
								// avoid finishing team selection Then, forceToUseNewPhysicalShard is set, which
								// enforce to use getTeam to select a remote team
								if (!bestTeam.first.get()->isHealthy()) {
									self->retryFindDstReasonCount
									    [DDQueue::RetryFindDstReason::RemoteTeamIsNotHealthy]++;
									foundTeams = false;
									break;
								}
							}

							bestTeams.emplace_back(bestTeam.first.get(), true);
							// Always set bestTeams[i].second = true to disable optimization in data move between
							// DCs for the correctness of PhysicalShardCollection Currently, enabling the
							// optimization will break the invariant of PhysicalShardCollection Invariant: once a
							// physical shard is created with a specific set of SSes, this SS set will never get
							// changed.

							if (tciIndex == 0) {
								ASSERT(foundTeams);
								ShardsAffectedByTeamFailure::Team primaryTeam =
								    ShardsAffectedByTeamFailure::Team(bestTeams[0].first->getServerIDs(), true);

								if (forceToUseNewPhysicalShard) {
									physicalShardIDCandidate =
									    self->physicalShardCollection->generateNewPhysicalShardID(debugID);
								} else {
									Optional<uint64_t> candidate =
									    self->physicalShardCollection->trySelectAvailablePhysicalShardFor(
									        primaryTeam, metrics, excludedDstPhysicalShards, debugID);
									if (candidate.present()) {
										physicalShardIDCandidate = candidate.get();
									} else {
										self->retryFindDstReasonCount
										    [DDQueue::RetryFindDstReason::NoAvailablePhysicalShard]++;
										if (wantTrueBest) {
											// Next retry will likely get the same team, and we know that we can't
											// reuse any existing physical shard in this team. So force to create
											// new physical shard.
											forceToUseNewPhysicalShard = true;
										}
										foundTeams = false;
										break;
									}
								}
								ASSERT(physicalShardIDCandidate != UID().first() &&
								       physicalShardIDCandidate != anonymousShardId.first());
							}
						} else {
							bestTeams.emplace_back(bestTeam.first.get(), bestTeam.second);
							if (doBulkLoading) {
								TraceEvent(
								    bulkLoadVerboseEventSev(), "DDBulkLoadTaskSelectDestTeam", self->distributorId)
								    .detail("Context", "New")
								    .detail("SrcIds", describe(rd.src))
								    .detail("DestIds", bestTeam.first.get()->getServerIDs())
								    .detail("DestTeam", bestTeam.first.get()->getTeamID())
								    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
								    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
								    .detail("Priority", rd.priority)
								    .detail("DataMoveId", rd.dataMoveId)
								    .detail("Primary", tciIndex == 0);
							}
						}
					}
					tciIndex++;
				}

				// once we've found healthy candidate teams, make sure they're not overloaded with outstanding moves
				// already
				anyDestOverloaded = !canLaunchDest(bestTeams, rd.priority, self->destBusymap);
				if (doBulkLoading) {
					anyDestOverloaded = false;
				}

				if (foundTeams && anyHealthy && !anyDestOverloaded) {
					ASSERT(rd.completeDests.empty());
					break;
				}

				if (foundTeams) {
					if (!anyHealthy) {
						self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::NoAnyHealthy]++;
					} else if (anyDestOverloaded) {
						self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::DstOverloaded]++;
					}
				}

				if (anyDestOverloaded) {
					CODE_PROBE(true, "Destination overloaded throttled move");
					destOverloadedCount++;
					TraceEvent(destOverloadedCount > 50 ? SevInfo : SevDebug, "DestSSBusy", distributorId)
					    .suppressFor(1.0)
					    .detail("TraceID", rd.randomId)
					    .detail("WantTrueBestIfMoveout", wantTrueBestIfMoveout)
					    .detail("IsRestore", rd.isRestore())
					    .detail("Priority", rd.priority)
					    .detail("StuckCount", stuckCount)
					    .detail("DestOverloadedCount", destOverloadedCount)
					    .detail("TeamCollectionId", tciIndex)
					    .detail("AnyDestOverloaded", anyDestOverloaded)
					    .detail("NumOfTeamCollections", self->teamCollections.size())
					    .detail("Servers", destServersString(bestTeams));
					if (enableShardMove) {
						if (rd.isRestore() && destOverloadedCount > 50) {
							throw data_move_dest_team_not_found();
						}
					}
					co_await delay(SERVER_KNOBS->DEST_OVERLOADED_DELAY, TaskPriority::DataDistributionLaunch);
				} else {
					CODE_PROBE(true, "did not find a healthy destination team on the first attempt");
					stuckCount++;
					TraceEvent(stuckCount > 50 ? SevWarnAlways : SevWarn, "BestTeamStuck", distributorId)
					    .suppressFor(1.0)
					    .detail("TraceID", rd.randomId)
					    .detail("WantTrueBestIfMoveout", wantTrueBestIfMoveout)
					    .detail("IsRestore", rd.isRestore())
					    .detail("Priority", rd.priority)
					    .detail("StuckCount", stuckCount)
					    .detail("DestOverloadedCount", destOverloadedCount)
					    .detail("TeamCollectionId", tciIndex)
					    .detail("AnyDestOverloaded", anyDestOverloaded)
					    .detail("NumOfTeamCollections", self->teamCollections.size())
					    .detail("ConductingBulkLoad", doBulkLoading);
					if (doBulkLoading && stuckCount == 50) {
						ASSERT(rd.bulkLoadTask.present());
						TraceEvent(SevWarnAlways, "DDBulkLoadTaskStuck", self->distributorId)
						    .detail("TraceID", rd.randomId)
						    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
						    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
						    .detail("TaskRange", rd.bulkLoadTask.get().coreState.getRange())
						    .detail("DataMoveID", rd.dataMoveId)
						    .detail("Reason", rd.reason.toString())
						    .detail("Priority", rd.priority)
						    .detail("DataMoveReason", static_cast<int>(rd.dmReason));
						if (rd.bulkLoadTask.get().completeAck.canBeSet()) {
							// Unretriable error. So, we give up the task at this time.
							rd.bulkLoadTask.get().completeAck.send(BulkLoadAck(/*unretryableError=*/true, rd.priority));
							throw data_move_dest_team_not_found();
							// This relocator should silently exit. Note that if this bulkload data move is
							// a team unhealthy data move, the bulkload engine will issue a new data move on
							// the same range. See createShardToBulkLoad() for details.
						}
					}
					if (rd.isRestore() && stuckCount > 50) {
						throw data_move_dest_team_not_found();
					}
					co_await delay(SERVER_KNOBS->BEST_TEAM_STUCK_DELAY, TaskPriority::DataDistributionLaunch);
				}
				// When forceToUseNewPhysicalShard = false, we get paired primary team and remote team
				// However, this may be failed
				// Any retry triggers to use new physicalShard which enters the normal routine
				if (enableShardMove) {
					if (destOverloadedCount + stuckCount > 20) {
						self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::RetryLimitReached]++;
						forceToUseNewPhysicalShard = true;
					}
					excludedDstPhysicalShards.insert(physicalShardIDCandidate);
				}

				// TODO different trace event + knob for overloaded? Could wait on an async var for done moves
			}

			if (enableShardMove) {
				// TODO(BulkLoad): double check if bulk loading can do with physical shard collection feature
				if (!rd.isRestore()) {
					// when !rd.isRestore(), dataMoveId is just decided as physicalShardIDCandidate
					// thus, update the physicalShardIDCandidate to related data structures
					ASSERT(physicalShardIDCandidate != UID().first());
					if (self->physicalShardCollection->physicalShardExists(physicalShardIDCandidate)) {
						self->moveReusePhysicalShard++;
					} else {
						self->moveCreateNewPhysicalShard++;
					}
					rd.dataMoveId = newDataMoveId(
					    physicalShardIDCandidate, AssignEmptyRange::False, newDataMoveType(doBulkLoading), rd.dmReason);
					TraceEvent(SevInfo, "NewDataMoveWithPhysicalShard")
					    .detail("DataMoveID", rd.dataMoveId.toString())
					    .detail("Reason", rd.reason.toString())
					    .detail("DataMoveReason", static_cast<int>(rd.dmReason));
					auto inFlightRange = self->inFlight.rangeContaining(rd.keys.begin);
					inFlightRange.value().dataMoveId = rd.dataMoveId;
					auto f = self->dataMoves.intersectingRanges(rd.keys);
					for (auto it = f.begin(); it != f.end(); ++it) {
						KeyRangeRef kr(it->range().begin, it->range().end);
						const UID mId = it->value().id;
						if (mId.isValid() && mId != rd.dataMoveId) {
							TraceEvent("DDRelocatorConflictingDataMoveAfterGetTeam", distributorId)
							    .detail("CurrentDataMoveID", rd.dataMoveId)
							    .detail("DataMoveID", mId)
							    .detail("Range", kr);
						}
					}
					self->dataMoves.insert(rd.keys, DDQueue::DDDataMove(rd.dataMoveId));
				}
				ASSERT(rd.dataMoveId.first() != UID().first());
				auto dataMoveRange = self->dataMoves.rangeContaining(rd.keys.begin);
				ASSERT(dataMoveRange.value().id == rd.dataMoveId);
			}

			// set cancellable to false on inFlight's entry for this key range
			auto inFlightRange = self->inFlight.rangeContaining(rd.keys.begin);
			ASSERT(inFlightRange.range() == rd.keys);
			ASSERT(inFlightRange.value().randomId == rd.randomId);
			inFlightRange.value().cancellable = false;

			destIds.clear();
			std::vector<UID> healthyIds;
			std::vector<UID> extraIds;
			std::vector<ShardsAffectedByTeamFailure::Team> destinationTeams;

			for (int i = 0; i < bestTeams.size(); i++) {
				auto& serverIds = bestTeams[i].first->getServerIDs();
				destinationTeams.push_back(ShardsAffectedByTeamFailure::Team(serverIds, i == 0));

				// TODO(psm): Make DataMoveMetaData aware of the two-step data move optimization.
				if (allHealthy && anyWithSource && !bestTeams[i].second && !doBulkLoading) {
					// When all servers in bestTeams[i] do not hold the shard (!bestTeams[i].second), it indicates
					// the bestTeams[i] is in a new DC where data has not been replicated to.
					// To move data (specified in RelocateShard) to bestTeams[i] in the new DC AND reduce data movement
					// across DC, we randomly choose a server in bestTeams[i] as the shard's destination, and
					// move the shard to the randomly chosen server (in the remote DC), which will later
					// propagate its data to the servers in the same team. This saves data movement bandwidth across DC
					// Bulk loading data move avoids this optimization since it does not move any data from source
					// servers
					int idx = deterministicRandom()->randomInt(0, serverIds.size());
					destIds.push_back(serverIds[idx]);
					healthyIds.push_back(serverIds[idx]);
					for (int j = 0; j < serverIds.size(); j++) {
						if (j != idx) {
							extraIds.push_back(serverIds[j]);
						}
					}
					healthyDestinations.addTeam(bestTeams[i].first);
				} else {
					destIds.insert(destIds.end(), serverIds.begin(), serverIds.end());
					if (bestTeams[i].first->isHealthy()) {
						healthyIds.insert(healthyIds.end(), serverIds.begin(), serverIds.end());
						healthyDestinations.addTeam(bestTeams[i].first);
					}
				}
			}

			// Sanity check for bulk loading data move
			if (doBulkLoading) {
				validateBulkLoadRelocateData(rd, destIds, self->distributorId);
			}
			TraceEvent(SevInfo, "DDRelocatorGotDestTeam", self->distributorId)
			    .detail("KeyBegin", rd.keys.begin)
			    .detail("KeyEnd", rd.keys.end)
			    .detail("Priority", rd.priority)
			    .detail("DataMoveId", rd.dataMoveId)
			    .detail("SrcIds", describe(rd.src))
			    .detail("DestId", describe(destIds))
			    .detail("BulkLoadTaskID", doBulkLoading ? rd.bulkLoadTask.get().coreState.getTaskId().toString() : "");

			// Sanity check
			int totalIds = 0;
			for (auto& destTeam : destinationTeams) {
				totalIds += destTeam.servers.size();
			}
			if (totalIds < self->teamSize) {
				TraceEvent(SevWarn, "IncorrectDestTeamSize")
				    .suppressFor(1.0)
				    .detail("ExpectedTeamSize", self->teamSize)
				    .detail("DestTeamSize", totalIds);
			}

			if (!rd.isRestore()) {
				self->shardsAffectedByTeamFailure->moveShard(rd.keys, destinationTeams);
			}

			// FIXME: do not add data in flight to servers that were already in the src.
			healthyDestinations.addDataInFlightToTeam(+metrics.bytes);
			healthyDestinations.addReadInFlightToTeam(+metrics.readLoadKSecond());

			// At this point, we are about to launch the data move, so we should update the busy map counter
			// for destination servers.
			ASSERT(!ownsDestBusyness);
			launchDest(rd, bestTeams, self->destBusymap);
			ownsDestBusyness = true;
			if (doBulkLoading) {
				for (const auto& [team, _] : bestTeams) {
					for (const UID& ssid : team->getServerIDs()) {
						self->bulkLoadTaskCollection->busyMap.addTask(ssid);
					}
				}
			}

			TraceEvent ev(relocateShardInterval.severity, "RelocateShardHasDestination", distributorId);
			RelocateDecision decision{ rd, destIds, extraIds, metrics, parentMetrics };
			traceRelocateDecision(ev, relocateShardInterval.pairID, decision);

			self->serverCounter.increaseForTeam(rd.src, rd.reason, DDQueue::ServerCounter::LaunchedSource);
			self->serverCounter.increaseForTeam(destIds, rd.reason, DDQueue::ServerCounter::LaunchedDest);
			self->serverCounter.increaseForTeam(extraIds, rd.reason, DDQueue::ServerCounter::LaunchedDest);

			Error error = success();
			Promise<Void> dataMovementComplete;
			// Move keys from source to destination by changing the serverKeyList and keyServerList system keys
			std::unique_ptr<MoveKeysParams> params;
			if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
				params = std::make_unique<MoveKeysParams>(rd.dataMoveId,
				                                          std::vector<KeyRange>{ rd.keys },
				                                          destIds,
				                                          healthyIds,
				                                          self->lock,
				                                          dataMovementComplete,
				                                          &self->startMoveKeysParallelismLock,
				                                          &self->finishMoveKeysParallelismLock,
				                                          self->teamCollections.size() > 1,
				                                          relocateShardInterval.pairID,
				                                          ddEnabledState,
				                                          CancelConflictingDataMoves::False,
				                                          doBulkLoading ? rd.bulkLoadTask.get().coreState
				                                                        : Optional<BulkLoadTaskState>());
			} else {
				params = std::make_unique<MoveKeysParams>(rd.dataMoveId,
				                                          rd.keys,
				                                          destIds,
				                                          healthyIds,
				                                          self->lock,
				                                          dataMovementComplete,
				                                          &self->startMoveKeysParallelismLock,
				                                          &self->finishMoveKeysParallelismLock,
				                                          self->teamCollections.size() > 1,
				                                          relocateShardInterval.pairID,
				                                          ddEnabledState,
				                                          CancelConflictingDataMoves::False,
				                                          doBulkLoading ? rd.bulkLoadTask.get().coreState
				                                                        : Optional<BulkLoadTaskState>());
			}
			Future<Void> doMoveKeys = self->txnProcessor->moveKeys(*params);
			Future<Void> pollHealth = signalledTransferComplete && !SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA
			                              ? Never()
			                              : delay(SERVER_KNOBS->HEALTH_POLL_TIME, TaskPriority::DataDistributionLaunch);
			try {
				while (true) {
					Future<Void> dataMovementFuture =
					    signalledTransferComplete ? Never() : dataMovementComplete.getFuture();
					auto res = co_await race(doMoveKeys, pollHealth, dataMovementFuture);
					if (res.index() == 0) {
						if (!extraIds.empty()) {
							destIds.insert(destIds.end(), extraIds.begin(), extraIds.end());
							healthyIds.insert(healthyIds.end(), extraIds.begin(), extraIds.end());
							extraIds.clear();
							ASSERT(totalIds == destIds.size()); // Sanity check the destIDs before we move keys
							std::unique_ptr<MoveKeysParams> params;
							if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
								params = std::make_unique<MoveKeysParams>(rd.dataMoveId,
								                                          std::vector<KeyRange>{ rd.keys },
								                                          destIds,
								                                          healthyIds,
								                                          self->lock,
								                                          Promise<Void>(),
								                                          &self->startMoveKeysParallelismLock,
								                                          &self->finishMoveKeysParallelismLock,
								                                          self->teamCollections.size() > 1,
								                                          relocateShardInterval.pairID,
								                                          ddEnabledState,
								                                          CancelConflictingDataMoves::False,
								                                          rd.bulkLoadTask.present()
								                                              ? rd.bulkLoadTask.get().coreState
								                                              : Optional<BulkLoadTaskState>());
							} else {
								params = std::make_unique<MoveKeysParams>(rd.dataMoveId,
								                                          rd.keys,
								                                          destIds,
								                                          healthyIds,
								                                          self->lock,
								                                          Promise<Void>(),
								                                          &self->startMoveKeysParallelismLock,
								                                          &self->finishMoveKeysParallelismLock,
								                                          self->teamCollections.size() > 1,
								                                          relocateShardInterval.pairID,
								                                          ddEnabledState,
								                                          CancelConflictingDataMoves::False,
								                                          rd.bulkLoadTask.present()
								                                              ? rd.bulkLoadTask.get().coreState
								                                              : Optional<BulkLoadTaskState>());
							}
							doMoveKeys = self->txnProcessor->moveKeys(*params);
						} else {
							self->fetchKeysComplete.insert(rd);
							if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
								auto ranges = self->dataMoves.getAffectedRangesAfterInsertion(rd.keys);
								if (ranges.size() == 1 && static_cast<KeyRange>(ranges[0]) == rd.keys &&
								    ranges[0].value.id == rd.dataMoveId && !ranges[0].value.cancel.isValid()) {
									self->dataMoves.insert(rd.keys, DDQueue::DDDataMove());
									TraceEvent(SevVerbose, "DequeueDataMoveOnSuccess", self->distributorId)
									    .detail("DataMoveID", rd.dataMoveId)
									    .detail("DataMoveRange", rd.keys);
								}
							}
							break;
						}
					} else if (res.index() == 1) {
						if (!healthyDestinations.isHealthy()) {
							if (!signalledTransferComplete) {
								signalledTransferComplete = true;
								self->dataTransferComplete.send(rd);
								ownsDestBusyness = false;
							}
							if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
								CODE_PROBE(true,
								           "Cancel shard-encoded data move after destination team becomes unhealthy");
								TraceEvent(SevWarnAlways, "RelocateShardDestinationTeamUnhealthy", distributorId)
								    .detail("DataMoveID", rd.dataMoveId)
								    .detail("Range", rd.keys)
								    .detail("Dest", describe(destIds));
								if (doBulkLoading && rd.bulkLoadTask.get().completeAck.canBeSet()) {
									rd.bulkLoadTask.get().completeAck.send(
									    BulkLoadAck(/*unretryableError=*/true, rd.priority));
								}
								retryAfterDestinationTeamFailure = shouldRetryDestinationTeamFailure(doBulkLoading, rd);
								throw data_move_dest_team_not_found();
							}
						}
						pollHealth = signalledTransferComplete && !SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA
						                 ? Never()
						                 : delay(SERVER_KNOBS->HEALTH_POLL_TIME, TaskPriority::DataDistributionLaunch);
					} else if (res.index() == 2) {
						self->fetchKeysComplete.insert(rd);
						if (!signalledTransferComplete) {
							signalledTransferComplete = true;
							self->dataTransferComplete.send(rd);
							ownsDestBusyness = false;
						}
					} else {
						UNREACHABLE();
					}
				}
			} catch (Error& e) {
				error = e;
			}

			//TraceEvent("RelocateShardFinished", distributorId).detail("RelocateId", relocateShardInterval.pairID);

			if (error.code() != error_code_move_to_removed_server &&
			    error.code() != error_code_finish_move_keys_too_many_retries &&
			    error.code() != error_code_start_move_keys_too_many_retries) {
				if (!error.code()) {
					try {
						co_await healthyDestinations
						    .updateStorageMetrics(); // prevent a gap between the polling for an increase in
						                             // storage metrics and decrementing data in flight
					} catch (Error& e) {
						error = e;
					}
				}

				healthyDestinations.addDataInFlightToTeam(-metrics.bytes);
				auto readLoad = metrics.readLoadKSecond();
				// Note: It’s equal to trigger([healthyDestinations, readLoad], which is a value capture of
				// healthyDestinations. Have to create a reference to healthyDestinations because in ACTOR the state
				// variable is actually a member variable, I can’t write trigger([healthyDestinations, readLoad]
				// directly.
				auto& destinationRef = healthyDestinations;
				self->noErrorActors.add(
				    trigger([destinationRef, readLoad]() mutable { destinationRef.addReadInFlightToTeam(-readLoad); },
				            delay(SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL)));

				// onFinished.send( rs );
				if (!error.code()) {
					TraceEvent(relocateShardInterval.end(), distributorId)
					    .detail("Duration", now() - startTime)
					    .detail("Result", "Success");
					TraceEvent("DataMoveStats", distributorId)
					    .detail("Duration", now() - startTime)
					    .detail("Bytes", metrics.bytes)
					    .detail("Rate", static_cast<double>(metrics.bytes) / (now() - startTime))
					    .detail("Reason", rd.reason.toString())
					    .detail("DataMoveReason", static_cast<int>(rd.dmReason))
					    .detail("DataMoveID", rd.dataMoveId)
					    .detail("DataMoveType", getDataMoveTypeFromDataMoveId(rd.dataMoveId));
					if (now() - startTime > 600) {
						TraceEvent(SevWarnAlways, "RelocateShardTooLong")
						    .detail("Duration", now() - startTime)
						    .detail("Dest", describe(destIds))
						    .detail("Src", describe(rd.src));
					}
					if (rd.keys.begin == keyServersPrefix) {
						TraceEvent("MovedKeyServerKeys")
						    .detail("Dest", describe(destIds))
						    .trackLatest(self->movedKeyServersEventHolder->trackingKey);
					}

					if (!signalledTransferComplete) {
						signalledTransferComplete = true;
						dataTransferComplete.send(rd);
						ownsDestBusyness = false;
					} else {
						completeOwnedDest(rd, self->destBusymap, ownsDestBusyness);
					}

					// In the case of merge, rd.completeSources would be the intersection set of two source server
					// lists, while rd.src would be the union set.
					// FIXME. It is a bit over-estimated here with rd.completeSources.
					const int nonOverlappingCount = nonOverlappedServerCount(rd.completeSources, destIds);
					self->bytesWritten += metrics.bytes;
					self->moveBytesRate.addSample(metrics.bytes * nonOverlappingCount);
					self->shardsAffectedByTeamFailure->finishMove(rd.keys);
					relocationComplete.send(rd);

					if (enableShardMove) {
						// update physical shard collection
						std::vector<ShardsAffectedByTeamFailure::Team> selectedTeams;
						for (int i = 0; i < bestTeams.size(); i++) {
							auto serverIds = bestTeams[i].first->getServerIDs();
							selectedTeams.push_back(ShardsAffectedByTeamFailure::Team(serverIds, i == 0));
						}
						// The update of PhysicalShardToTeams, PhysicalShardInstances, keyRangePhysicalShardIDMap
						// should be atomic
						self->physicalShardCollection->updatePhysicalShardCollection(
						    rd.keys, rd.isRestore(), selectedTeams, rd.dataMoveId.first(), metrics, debugID);
					}

					if (doBulkLoading) {
						for (const auto& [team, _] : bestTeams) {
							for (const UID& ssid : team->getServerIDs()) {
								self->bulkLoadTaskCollection->busyMap.removeTask(ssid);
							}
						}
						try {
							self->bulkLoadTaskCollection->terminateTask(rd.bulkLoadTask.get().coreState);
							TraceEvent(
							    bulkLoadVerboseEventSev(), "DDBulkLoadTaskRelocatorComplete", self->distributorId)
							    .detail("Dests", describe(destIds))
							    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
							    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId());
						} catch (Error& bulkLoadError) {
							ASSERT_WE_THINK(bulkLoadError.code() == error_code_bulkload_task_outdated);
							if (bulkLoadError.code() != error_code_bulkload_task_outdated) {
								throw bulkLoadError;
							}
							TraceEvent(bulkLoadVerboseEventSev(),
							           "DDBulkLoadTaskRelocatorCompleteButOutdated",
							           self->distributorId)
							    .detail("Dests", describe(destIds))
							    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
							    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId());
						}
					}
					co_return;
				} else {
					if (doBulkLoading) {
						TraceEvent(bulkLoadVerboseEventSev(), "DDBulkLoadTaskRelocatorError")
						    .errorUnsuppressed(error)
						    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
						    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId());
						for (const auto& [team, _] : bestTeams) {
							for (const UID& ssid : team->getServerIDs()) {
								self->bulkLoadTaskCollection->busyMap.removeTask(ssid);
							}
						}
					}
					throw error;
				}
			} else {
				CODE_PROBE(true, "move keys failed -- removed server or exceeded retries");
				healthyDestinations.addDataInFlightToTeam(-metrics.bytes);
				auto readLoad = metrics.readLoadKSecond();
				auto& destinationRef = healthyDestinations;
				self->noErrorActors.add(
				    trigger([destinationRef, readLoad]() mutable { destinationRef.addReadInFlightToTeam(-readLoad); },
				            delay(SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL)));

				resetDestinationsForRetry(rd, healthyDestinations, self->destBusymap, ownsDestBusyness);

				if (doBulkLoading) {
					TraceEvent(bulkLoadVerboseEventSev(), "DDBulkLoadTaskRelocatorError")
					    .errorUnsuppressed(error)
					    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
					    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId());
					for (const auto& [team, _] : bestTeams) {
						for (const UID& ssid : team->getServerIDs()) {
							self->bulkLoadTaskCollection->busyMap.removeTask(ssid);
						}
					}
				}

				co_await delay(SERVER_KNOBS->RETRY_RELOCATESHARD_DELAY, TaskPriority::DataDistributionLaunch);
			}
		}
	} catch (Error& e) {
		err = e;
	}

	TraceEvent(relocateShardInterval.end(), distributorId).errorUnsuppressed(err).detail("Duration", now() - startTime);
	if (now() - startTime > 600) {
		TraceEvent(SevWarnAlways, "RelocateShardTooLong")
		    .errorUnsuppressed(err)
		    .detail("Duration", now() - startTime)
		    .detail("Dest", describe(destIds))
		    .detail("Src", describe(rd.src));
	}
	if (!signalledTransferComplete) {
		dataTransferComplete.send(rd);
		ownsDestBusyness = false;
	} else {
		completeOwnedDest(rd, self->destBusymap, ownsDestBusyness);
	}

	if (err.code() == error_code_data_move_dest_team_not_found && rd.isRestore()) {
		std::vector<ShardsAffectedByTeamFailure::Team> destinationTeams = { ShardsAffectedByTeamFailure::Team(
			rd.dataMove->primaryDest, true) };
		std::vector<ShardsAffectedByTeamFailure::Team> sourceTeams = { ShardsAffectedByTeamFailure::Team(
			rd.dataMove->primarySrc, true) };
		if (!rd.dataMove->remoteDest.empty()) {
			destinationTeams.emplace_back(rd.dataMove->remoteDest, false);
		}
		if (!rd.dataMove->remoteSrc.empty()) {
			sourceTeams.emplace_back(rd.dataMove->remoteSrc, false);
		}
		auto restoredRanges = self->shardsAffectedByTeamFailure->cancelMove(rd.keys, destinationTeams, sourceTeams);
		TraceEvent(SevWarnAlways, "RelocateShardRestoreDataMoveSources", self->distributorId)
		    .detail("Range", rd.keys)
		    .detail("DataMoveID", rd.dataMoveId)
		    .detail("RestoredRanges", restoredRanges.size());
	}

	if (err.code() == error_code_data_move_dest_team_not_found && retryAfterDestinationTeamFailure) {
		// randomId participates in RelocateData's queue ordering, so a new attempt needs a new identity.
		RelocateShard retry = makeDestinationFailureRetry(rd, deterministicRandom()->randomUniqueID());
		self->output.send(retry);
		TraceEvent(SevWarnAlways, "RelocateShardRetryDestinationTeamFailure", self->distributorId)
		    .detail("Range", rd.keys)
		    .detail("DataMoveID", rd.dataMoveId)
		    .detail("Priority", rd.priority)
		    .detail("Reason", rd.reason.toString())
		    .detail("TraceID", retry.traceId)
		    .detail("PreviousTraceID", rd.randomId);
	}

	relocationComplete.send(rd);

	if (doBulkLoading && err.code() != error_code_actor_cancelled && err.code() != error_code_movekeys_conflict) {
		TraceEvent(SevWarnAlways, "DDBulkLoadTaskRelocatorFailed", self->distributorId)
		    .errorUnsuppressed(err)
		    .detail("TaskID", rd.bulkLoadTask.get().coreState.getTaskId())
		    .detail("JobID", rd.bulkLoadTask.get().coreState.getJobId())
		    .detail("TaskRange", rd.bulkLoadTask.get().coreState.getRange());
	}

	if (err.code() == error_code_data_move_dest_team_not_found) {
		co_await cancelDataMove(self, rd.keys, ddEnabledState);
		TraceEvent(SevWarnAlways, "RelocateShardCancelDataMoveTeamNotFound")
		    .detail("Src", describe(rd.src))
		    .detail("DataMoveMetaData", rd.dataMove != nullptr ? rd.dataMove->meta.toString() : "Empty");
	} else if (err.code() != error_code_actor_cancelled && err.code() != error_code_data_move_cancelled) {
		if (errorOut.canBeSet()) {
			errorOut.sendError(err);
		}
	}
	throw err;
}

inline double getWorstCpu(const HealthMetrics& metrics, const std::vector<UID>& ids) {
	double cpu = 0;
	for (auto& id : ids) {
		if (metrics.storageStats.contains(id)) {
			cpu = std::max(cpu, metrics.storageStats.at(id).cpuUsage);
		} else {
			// assume the server is too busy to report its stats
			cpu = std::max(cpu, 100.0);
			break;
		}
	}
	return cpu;
}

// Move the shard with the top K highest read density of sourceTeam's to destTeam if sourceTeam has much more read
// load than destTeam
Future<bool> rebalanceReadLoad(DDQueue* self,
                               DataMovementReason moveReason,
                               Reference<IDataDistributionTeam> sourceTeam,
                               Reference<IDataDistributionTeam> destTeam,
                               bool primary,
                               TraceEvent* traceEvent) {
	if (g_network->isSimulated() && g_simulator->speedUpSimulation) {
		traceEvent->detail("CancelingDueToSimulationSpeedup", true);
		co_return false;
	}

	std::vector<KeyRange> shards = self->shardsAffectedByTeamFailure->getShardsFor(
	    ShardsAffectedByTeamFailure::Team(sourceTeam->getServerIDs(), primary));
	traceEvent->detail("ShardsInSource", shards.size());
	// For read rebalance if there is just 1 hot shard remained, move this shard to another server won't solve the
	// problem.
	// TODO: This situation should be solved by split and merge
	if (shards.size() <= 1) {
		traceEvent->detail("SkipReason", "NoShardOnSource");
		co_return false;
	}

	// Check lastAsSource, at most SERVER_KNOBS->READ_REBALANCE_SRC_PARALLELISM shards can be moved within a sample
	// period. It takes time for the sampled metrics being updated after a shard is moved, so we should control the
	// cadence of movement here to avoid moving churn caused by making many decision based on out-of-date sampled
	// metrics.
	if (self->timeThrottle(sourceTeam->getServerIDs())) {
		traceEvent->detail("SkipReason", "SourceTeamThrottle");
		co_return false;
	}
	// check team difference
	auto srcLoad = sourceTeam->getReadLoad(false), destLoad = destTeam->getReadLoad();
	traceEvent->detail("SrcReadBandwidth", srcLoad).detail("DestReadBandwidth", destLoad);

	// read bandwidth difference is less than 30% of src load
	if ((1.0 - SERVER_KNOBS->READ_REBALANCE_DIFF_FRAC) * srcLoad <= destLoad) {
		traceEvent->detail("SkipReason", "TeamTooSimilar");
		co_return false;
	}
	// randomly choose topK shards
	int topK = std::max(1, std::min(int(0.1 * shards.size()), SERVER_KNOBS->READ_REBALANCE_SHARD_TOPK));
	Future<HealthMetrics> healthMetrics = self->txnProcessor->getHealthMetrics(true);
	GetTopKMetricsRequest req(shards,
	                          topK,
	                          (srcLoad - destLoad) * SERVER_KNOBS->READ_REBALANCE_MAX_SHARD_FRAC,
	                          std::min(srcLoad / shards.size(), SERVER_KNOBS->READ_REBALANCE_MIN_READ_BYTES_KS));
	GetTopKMetricsReply reply = co_await brokenPromiseToNever(self->getTopKMetrics.getReply(req));
	co_await ready(healthMetrics);
	auto cpu = getWorstCpu(healthMetrics.get(), sourceTeam->getServerIDs());
	if (cpu < SERVER_KNOBS->READ_REBALANCE_CPU_THRESHOLD) { // 15.0 +- (0.3 * 15) < 20.0
		traceEvent->detail("SkipReason", "LowReadLoad").detail("WorstSrcCpu", cpu);
		co_return false;
	}

	auto& metricsList = reply.shardMetrics;
	// NOTE: randomize is important here since we don't want to always push the same shard into the queue
	deterministicRandom()->randomShuffle(metricsList);
	traceEvent->detail("MinReadLoad", reply.minReadLoad).detail("MaxReadLoad", reply.maxReadLoad);

	if (metricsList.empty()) {
		traceEvent->detail("SkipReason", "NoEligibleShards");
		co_return false;
	}

	auto& [shard, metrics] = metricsList[0];
	traceEvent->detail("ShardReadBandwidth", metrics.bytesReadPerKSecond)
	    .detail("ShardReadOps", metrics.opsReadPerKSecond);

	//  Verify the shard is still in ShardsAffectedByTeamFailure
	shards = self->shardsAffectedByTeamFailure->getShardsFor(
	    ShardsAffectedByTeamFailure::Team(sourceTeam->getServerIDs(), primary));
	for (int i = 0; i < shards.size(); i++) {
		if (shard == shards[i]) {
			UID traceId = deterministicRandom()->randomUniqueID();
			self->output.send(RelocateShard(shard, moveReason, RelocateReason::REBALANCE_READ, traceId));
			traceEvent->detail("TraceId", traceId);

			auto serverIds = sourceTeam->getServerIDs();
			self->updateLastAsSource(serverIds);

			self->serverCounter.increaseForTeam(
			    serverIds, RelocateReason::REBALANCE_READ, DDQueue::ServerCounter::ProposedSource);
			co_return true;
		}
	}
	traceEvent->detail("SkipReason", "ShardNotPresent");
	co_return false;
}

// Move a random shard from sourceTeam if sourceTeam has much more data than provided destTeam
static Future<bool> rebalanceTeams(DDQueue* self,
                                   DataMovementReason moveReason,
                                   Reference<IDataDistributionTeam const> sourceTeam,
                                   Reference<IDataDistributionTeam const> destTeam,
                                   bool primary,
                                   TraceEvent* traceEvent) {
	if (g_network->isSimulated() && g_simulator->speedUpSimulation) {
		traceEvent->detail("CancelingDueToSimulationSpeedup", true);
		co_return false;
	}

	Promise<int64_t> req;
	self->getAverageShardBytes.send(req);

	int64_t averageShardBytes = co_await req.getFuture();
	std::vector<KeyRange> shards = self->shardsAffectedByTeamFailure->getShardsFor(
	    ShardsAffectedByTeamFailure::Team(sourceTeam->getServerIDs(), primary));

	traceEvent->detail("AverageShardBytes", averageShardBytes).detail("ShardsInSource", shards.size());

	if (shards.empty()) {
		traceEvent->detail("SkipReason", "NoShardOnSource");
		co_return false;
	}

	KeyRange moveShard;
	StorageMetrics metrics;
	int retries = 0;
	while (retries < SERVER_KNOBS->REBALANCE_MAX_RETRIES) {
		KeyRange testShard = deterministicRandom()->randomChoice(shards);
		StorageMetrics testMetrics =
		    co_await brokenPromiseToNever(self->getShardMetrics.getReply(GetMetricsRequest(testShard)));
		if (testMetrics.bytes > metrics.bytes) {
			moveShard = testShard;
			metrics = testMetrics;
			if (metrics.bytes > averageShardBytes) {
				break;
			}
		}
		retries++;
	}

	int64_t sourceBytes = sourceTeam->getLoadBytes(false);
	int64_t destBytes = destTeam->getLoadBytes();

	bool sourceAndDestTooSimilar =
	    sourceBytes - destBytes <= 3 * std::max<int64_t>(SERVER_KNOBS->MIN_SHARD_BYTES, metrics.bytes);
	traceEvent->detail("SourceBytes", sourceBytes)
	    .detail("DestBytes", destBytes)
	    .detail("ShardBytes", metrics.bytes)
	    .detail("SourceAndDestTooSimilar", sourceAndDestTooSimilar);

	if (sourceAndDestTooSimilar || metrics.bytes == 0) {
		traceEvent->detail("SkipReason", sourceAndDestTooSimilar ? "TeamTooSimilar" : "ShardZeroSize");
		co_return false;
	}

	// Verify the shard is still in ShardsAffectedByTeamFailure
	shards = self->shardsAffectedByTeamFailure->getShardsFor(
	    ShardsAffectedByTeamFailure::Team(sourceTeam->getServerIDs(), primary));
	for (int i = 0; i < shards.size(); i++) {
		if (moveShard == shards[i]) {
			UID traceId = deterministicRandom()->randomUniqueID();
			self->output.send(RelocateShard(moveShard, moveReason, RelocateReason::REBALANCE_DISK, traceId));
			traceEvent->detail("TraceId", traceId);

			self->serverCounter.increaseForTeam(
			    sourceTeam->getServerIDs(), RelocateReason::REBALANCE_DISK, DDQueue::ServerCounter::ProposedSource);
			co_return true;
		}
	}

	traceEvent->detail("SkipReason", "ShardNotPresent");
	co_return false;
}

Future<SrcDestTeamPair> getSrcDestTeams(DDQueue* self,
                                        int teamCollectionIndex,
                                        GetTeamRequest srcReq,
                                        GetTeamRequest destReq,
                                        int priority,
                                        TraceEvent* traceEvent) {

	std::pair<Optional<ITeamRef>, bool> randomTeam =
	    co_await brokenPromiseToNever(self->teamCollections[teamCollectionIndex].getTeam.getReply(destReq));
	traceEvent->detail("DestTeam", printable(randomTeam.first.mapRef(&IDataDistributionTeam::getDesc)));

	if (randomTeam.first.present()) {
		std::pair<Optional<ITeamRef>, bool> loadedTeam =
		    co_await brokenPromiseToNever(self->teamCollections[teamCollectionIndex].getTeam.getReply(srcReq));

		traceEvent->detail("SourceTeam", printable(loadedTeam.first.mapRef(&IDataDistributionTeam::getDesc)));

		if (loadedTeam.first.present()) {
			co_return std::make_pair(loadedTeam.first.get(), randomTeam.first.get());
		}
	}
	co_return SrcDestTeamPair{};
}

Future<SrcDestTeamPair> DDQueue::getSrcDestTeams(const int& teamCollectionIndex,
                                                 const GetTeamRequest& srcReq,
                                                 const GetTeamRequest& destReq,
                                                 const int& priority,
                                                 TraceEvent* traceEvent) {
	return ::getSrcDestTeams(this, teamCollectionIndex, srcReq, destReq, priority, traceEvent);
}
Future<bool> DDQueue::rebalanceReadLoad(DataMovementReason moveReason,
                                        Reference<IDataDistributionTeam> sourceTeam,
                                        Reference<IDataDistributionTeam> destTeam,
                                        bool primary,
                                        TraceEvent* traceEvent) {
	return ::rebalanceReadLoad(this, moveReason, sourceTeam, destTeam, primary, traceEvent);
}

Future<bool> DDQueue::rebalanceTeams(DataMovementReason moveReason,
                                     Reference<const IDataDistributionTeam> sourceTeam,
                                     Reference<const IDataDistributionTeam> destTeam,
                                     bool primary,
                                     TraceEvent* traceEvent) {
	return ::rebalanceTeams(this, moveReason, sourceTeam, destTeam, primary, traceEvent);
}

Future<bool> getSkipRebalanceValue(Reference<IDDTxnProcessor> txnProcessor, bool readRebalance) {
	Optional<Value> val = co_await txnProcessor->readRebalanceDDIgnoreKey();

	if (!val.present())
		co_return false;

	bool skipCurrentLoop = false;
	// NOTE: check special value "" and "on" might written in old version < 7.2
	if (!val.get().empty() && val.get() != "on"_sr) {
		int ddIgnore = BinaryReader::fromStringRef<uint8_t>(val.get(), Unversioned());
		if (readRebalance) {
			skipCurrentLoop = (ddIgnore & DDIgnore::REBALANCE_READ) > 0;
		} else {
			skipCurrentLoop = (ddIgnore & DDIgnore::REBALANCE_DISK) > 0;
		}
	} else {
		skipCurrentLoop = true;
	}

	co_return skipCurrentLoop;
}

Future<Void> BgDDLoadRebalance(DDQueue* self, int teamCollectionIndex, DataMovementReason reason) {
	int resetCount = 0;
	double lastRead = 0;
	bool skipCurrentLoop = false;
	const bool readRebalance = isDataMovementForReadBalancing(reason);
	const std::string moveType = isDataMovementForMountainChopper(reason) ? "BgDDMountainChopper" : "BgDDValleyFiller";
	int ddPriority = dataMovementPriority(reason);
	bool mcMove = isDataMovementForMountainChopper(reason);
	PreferLowerReadUtil preferLowerReadTeam = readRebalance || SERVER_KNOBS->DD_PREFER_LOW_READ_UTIL_TEAM
	                                              ? PreferLowerReadUtil::True
	                                              : PreferLowerReadUtil::False;
	double rebalancePollingInterval = 0;

	while (true) {
		bool moved = false;
		Reference<IDataDistributionTeam> sourceTeam;
		Reference<IDataDistributionTeam> destTeam;

		// NOTE: the DD throttling relies on DDQueue, so here just trigger the balancer periodically
		co_await delay(rebalancePollingInterval, TaskPriority::DataDistributionLaunch);
		try {
			if ((now() - lastRead) > SERVER_KNOBS->BG_REBALANCE_SWITCH_CHECK_INTERVAL) {
				skipCurrentLoop = co_await getSkipRebalanceValue(self->txnProcessor, readRebalance);
				lastRead = now();
			}

			if (skipCurrentLoop) {
				rebalancePollingInterval =
				    std::max(rebalancePollingInterval, SERVER_KNOBS->BG_REBALANCE_SWITCH_CHECK_INTERVAL);
				TraceEvent("DDRebalancePaused", self->distributorId)
				    .suppressFor(5.0)
				    .detail("MoveType", moveType)
				    .detail("Reason", "Disabled");
				continue;
			}

			if (self->priority_relocations[ddPriority] >= SERVER_KNOBS->DD_REBALANCE_PARALLELISM) {
				rebalancePollingInterval =
				    std::min(rebalancePollingInterval * 2, SERVER_KNOBS->BG_REBALANCE_MAX_POLLING_INTERVAL);
				TraceEvent("DDRebalancePaused", self->distributorId)
				    .suppressFor(5.0)
				    .detail("MoveType", moveType)
				    .detail("Reason", "DataMoveLimitReached")
				    .detail("QueuedRelocations", self->priority_relocations[ddPriority])
				    .detail("PollingInterval", rebalancePollingInterval);
				continue;
			}

			rebalancePollingInterval =
			    std::max(rebalancePollingInterval / 2, SERVER_KNOBS->BG_REBALANCE_POLLING_INTERVAL);

			TraceEvent traceEvent(mcMove ? "MountainChopperSample" : "ValleyFillerSample", self->distributorId);
			traceEvent.suppressFor(5.0);
			GetTeamRequest srcReq = GetTeamRequest(mcMove ? TeamSelect::WANT_TRUE_BEST : TeamSelect::ANY,
			                                       PreferLowerDiskUtil::False,
			                                       TeamMustHaveShards::True,
			                                       PreferLowerReadUtil::False,
			                                       PreferWithinShardLimit::False,
			                                       ForReadBalance(readRebalance));

			GetTeamRequest destReq = GetTeamRequest(!mcMove ? TeamSelect::WANT_TRUE_BEST : TeamSelect::ANY,
			                                        PreferLowerDiskUtil::True,
			                                        TeamMustHaveShards::False,
			                                        preferLowerReadTeam,
			                                        PreferWithinShardLimit::False,
			                                        ForReadBalance(readRebalance));
			Future<SrcDestTeamPair> getTeamFuture =
			    self->getSrcDestTeams(teamCollectionIndex, srcReq, destReq, ddPriority, &traceEvent);
			co_await ready(getTeamFuture);
			sourceTeam = getTeamFuture.get().first;
			destTeam = getTeamFuture.get().second;

			// clang-format off
			if (sourceTeam.isValid() && destTeam.isValid()) {
				if (readRebalance) {
					moved = co_await self->rebalanceReadLoad( reason, sourceTeam, destTeam, teamCollectionIndex == 0, &traceEvent);
				} else {
					moved = co_await self->rebalanceTeams( reason, sourceTeam, destTeam, teamCollectionIndex == 0, &traceEvent);
				}
			}
			// clang-format on
			traceEvent.detail("Moved", moved).log();

			if (moved) {
				resetCount = 0;
				TraceEvent(mcMove ? "MountainChopperMoved" : "ValleyFillerMoved", self->distributorId)
				    .suppressFor(5.0)
				    .detail("QueuedRelocations", self->priority_relocations[ddPriority])
				    .detail("PollingInterval", rebalancePollingInterval);
			} else {
				++resetCount;
				if (resetCount > 30) {
					rebalancePollingInterval = SERVER_KNOBS->BG_REBALANCE_MAX_POLLING_INTERVAL;
				}
				TraceEvent(mcMove ? "MountainChopperSkipped" : "ValleyFillerSkipped", self->distributorId)
				    .suppressFor(5.0)
				    .detail("QueuedRelocations", self->priority_relocations[ddPriority])
				    .detail("ResetCount", resetCount)
				    .detail("PollingInterval", rebalancePollingInterval);
			}

		} catch (Error& e) {
			// Log actor_cancelled because it's not legal to suppress an event that's initialized
			TraceEvent("RebalanceMoveError", self->distributorId).detail("MoveType", moveType).errorUnsuppressed(e);
			throw;
		}
	}
}

// Gates the relocation input stream by the pipeline limit. Cancellations always pass through
// immediately because they reduce tracked metadata rather than adding to it. All other
// relocations, regardless of priority, are held when the pipeline is full, waiting for
// pipelineFull to become false before forwarding.
// The global isDDPipelineControlEnabled() flag (cleared by disableDDPipelineControl()) also
// bypasses the gate, allowing the test harness to open up the pipeline so DD can quiesce.
// We poll it via delay() rather than AsyncVar to avoid cross-process callbacks in simulation.
Future<Void> pipelineGateActor(Reference<DDQueue> self,
                               FutureStream<RelocateShard> input,
                               PromiseStream<RelocateShard> output) {
	while (true) {
		RelocateShard rs = co_await input;
		if (!rs.cancelled) {
			while (self->pipelineFull->get() && isDDPipelineControlEnabled()) {
				TraceEvent("DDPipelineFull", self->distributorId)
				    .suppressFor(30.0)
				    .detail("PipelineFull", self->pipelineFull->get());
				co_await (self->pipelineFull->onChange() || delay(1.0));
			}
		}
		self->pendingGateRelocations++;
		self->updatePipelineFull();
		output.send(rs);
	}
}

struct DDQueueImpl {
	struct RunState {
		explicit RunState(Reference<DDQueue> self)
		  : self(self), completedRelocations(self->relocationComplete.getFuture()) {}

		Reference<DDQueue> self;
		std::set<UID> serversToLaunchFrom;
		FutureStream<RelocateData> completedRelocations;
		PromiseStream<KeyRange> rangesComplete;
		PromiseStream<Void> launchQueuedWorkTrigger;
		// Serialize queue mutations so inline stream callbacks cannot observe partially updated state.
		FlowLock queueMutationLock;
	};

	static void validate(RunState* state) { state->self->validate(); }

	static void scheduleQueuedServerWork(RunState* state) { state->launchQueuedWorkTrigger.send(Void()); }

	static Future<Void> processGatedRelocations(RunState* state, FutureStream<RelocateShard> input) {
		while (true) {
			RelocateShard rs = co_await input;
			co_await state->queueMutationLock.take(TaskPriority::DataDistributionLaunch);
			FlowLock::Releaser lockGuard(state->queueMutationLock);
			state->self->pendingGateRelocations--;
			state->self->updatePipelineFull();
			if (rs.isRestore()) {
				ASSERT(rs.dataMove != nullptr);
				ASSERT(rs.dataMoveId.isValid());
				state->self->launchQueuedWork(RelocateData(rs), state->self->ddEnabledState);
			} else if (rs.cancelled) {
				state->self->enqueueCancelledDataMove(rs.dataMoveId, rs.keys, state->self->ddEnabledState);
			} else {
				bool wasEmpty = state->serversToLaunchFrom.empty();
				state->self->queueRelocation(rs, state->serversToLaunchFrom);
				if (wasEmpty && !state->serversToLaunchFrom.empty()) {
					scheduleQueuedServerWork(state);
				}
			}
			validate(state);
		}
	}

	static Future<Void> launchQueuedServerWork(RunState* state) {
		FutureStream<Void> trigger = state->launchQueuedWorkTrigger.getFuture();
		while (true) {
			co_await trigger;
			co_await delay(0, TaskPriority::DataDistributionLaunch);
			co_await state->queueMutationLock.take(TaskPriority::DataDistributionLaunch);
			FlowLock::Releaser lockGuard(state->queueMutationLock);
			state->self->launchQueuedWork(state->serversToLaunchFrom, state->self->ddEnabledState);
			state->serversToLaunchFrom.clear();
			validate(state);
		}
	}

	static Future<Void> processFetchedSourceServers(RunState* state, FutureStream<RelocateData> input) {
		while (true) {
			RelocateData results = co_await input;
			co_await state->queueMutationLock.take(TaskPriority::DataDistributionLaunch);
			FlowLock::Releaser lockGuard(state->queueMutationLock);
			// A source-fetch actor can finish after an overlapping relocation has already cancelled and
			// replaced the item it was fetching for. The old serialized choose loop could not observe that
			// completion after the replacement had been processed, but the coroutine split can.
			if (!state->self->fetchingSourcesQueue.contains(results)) {
				DebugRelocationTraceEvent("StaleSourceFetchResult", state->self->distributorId)
				    .detail("KeyBegin", results.keys.begin)
				    .detail("KeyEnd", results.keys.end)
				    .detail("Priority", results.priority)
				    .detail("RandomID", results.randomId);
				continue;
			}
			// This stream is triggered by queueRelocation(), which is triggered by sending self->input.
			state->self->completeSourceFetch(results);
			validate(state);
			if (results.startTime != -1) {
				state->self->launchQueuedWork(results, state->self->ddEnabledState);
			}
		}
	}

	static Future<Void> processCompletedDataTransfers(RunState* state, FutureStream<RelocateData> input) {
		while (true) {
			RelocateData done = co_await input;
			co_await state->queueMutationLock.take(TaskPriority::DataDistributionLaunch);
			FlowLock::Releaser lockGuard(state->queueMutationLock);
			complete(done, state->self->busymap, state->self->destBusymap);
			bool wasEmpty = state->serversToLaunchFrom.empty();
			state->serversToLaunchFrom.insert(done.src.begin(), done.src.end());
			if (wasEmpty && !state->serversToLaunchFrom.empty()) {
				scheduleQueuedServerWork(state);
			}
			validate(state);
		}
	}

	static Future<Void> processCompletedRelocations(RunState* state) {
		while (true) {
			RelocateData done = co_await state->completedRelocations;
			co_await state->queueMutationLock.take(TaskPriority::DataDistributionLaunch);
			FlowLock::Releaser lockGuard(state->queueMutationLock);
			state->self->processRelocationComplete(done);
			// self->logRelocation( done, "ShardRelocatorDone" );
			auto scheduleRangesComplete = [&](KeyRange keys) {
				state->self->noErrorActors.add(
				    tag(delay(0, TaskPriority::DataDistributionLaunch), keys, state->rangesComplete));
			};
			scheduleRangesComplete(done.keys);
			// Batch drain: process all remaining ready completions without yielding.
			// This prevents fetchKeysComplete from growing when completions are starved by other events.
			int drained = 0;
			while (state->completedRelocations.isReady() && !state->completedRelocations.isError() &&
			       drained++ < 1000) {
				RelocateData next = state->completedRelocations.pop();
				state->self->processRelocationComplete(next);
				scheduleRangesComplete(next.keys);
			}
			if (drained > 0) {
				static auto* c = SimpleCounter<int64_t>::makeCounter("/dd/queue/relocationComplete/batchDrained");
				c->increment(drained + 1);
				TraceEvent("RelocationCompleteBatchDrain", state->self->distributorId)
				    .suppressFor(1.0)
				    .detail("Drained", drained + 1);
			}
			validate(state);
		}
	}

	static Future<Void> launchCompletedRanges(RunState* state) {
		FutureStream<KeyRange> rangesComplete = state->rangesComplete.getFuture();
		while (true) {
			KeyRange done = co_await rangesComplete;
			co_await state->queueMutationLock.take(TaskPriority::DataDistributionLaunch);
			FlowLock::Releaser lockGuard(state->queueMutationLock);
			validate(state);
			if (!done.empty()) {
				state->self->launchQueuedWork(done, state->self->ddEnabledState);
			}
		}
	}

	static void traceMovingData(DDQueue* self) {
		Promise<int64_t> req;
		self->getAverageShardBytes.send(req);

		auto const highestPriorityRelocation = self->getHighestPriorityRelocation();

		TraceEvent("MovingData", self->distributorId)
		    .detail("InFlight", self->activeRelocations)
		    .detail("InQueue", self->queuedRelocations)
		    .detail("AverageShardSize", req.getFuture().isReady() ? req.getFuture().get() : -1)
		    .detail("UnhealthyRelocations", self->unhealthyRelocations)
		    .detail("HighestPriority", highestPriorityRelocation)
		    .detail("BytesWritten", self->moveBytesRate.getTotal())
		    .detail("BytesWrittenAverageRate", self->moveBytesRate.getAverage())
		    .detail("PipelineSize", self->pipelineSize())
		    .detail("PipelineLimit", SERVER_KNOBS->DD_MAX_PIPELINE_MOVES)
		    .detail("PendingGateRelocations", self->pendingGateRelocations)
		    .detail("PriorityRecoverMove", self->priority_relocations[SERVER_KNOBS->PRIORITY_RECOVER_MOVE])
		    .detail("PriorityRebalanceUnderutilizedTeam",
		            self->priority_relocations[SERVER_KNOBS->PRIORITY_REBALANCE_UNDERUTILIZED_TEAM])
		    .detail("PriorityRebalanceOverutilizedTeam",
		            self->priority_relocations[SERVER_KNOBS->PRIORITY_REBALANCE_OVERUTILIZED_TEAM])
		    .detail("PriorityRebalanceReadUnderutilTeam",
		            self->priority_relocations[SERVER_KNOBS->PRIORITY_REBALANCE_READ_UNDERUTIL_TEAM])
		    .detail("PriorityRebalanceReadOverutilTeam",
		            self->priority_relocations[SERVER_KNOBS->PRIORITY_REBALANCE_READ_OVERUTIL_TEAM])
		    .detail("PriorityStorageWiggle",
		            self->priority_relocations[SERVER_KNOBS->PRIORITY_PERPETUAL_STORAGE_WIGGLE])
		    .detail("PriorityTeamHealthy", self->priority_relocations[SERVER_KNOBS->PRIORITY_TEAM_HEALTHY])
		    .detail("PriorityTeamContainsUndesiredServer",
		            self->priority_relocations[SERVER_KNOBS->PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER])
		    .detail("PriorityTeamRedundant", self->priority_relocations[SERVER_KNOBS->PRIORITY_TEAM_REDUNDANT])
		    .detail("PriorityMergeShard", self->priority_relocations[SERVER_KNOBS->PRIORITY_MERGE_SHARD])
		    .detail("PriorityPopulateRegion", self->priority_relocations[SERVER_KNOBS->PRIORITY_POPULATE_REGION])
		    .detail("PriorityTeamUnhealthy", self->priority_relocations[SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY])
		    .detail("PriorityTeam2Left", self->priority_relocations[SERVER_KNOBS->PRIORITY_TEAM_2_LEFT])
		    .detail("PriorityTeam1Left", self->priority_relocations[SERVER_KNOBS->PRIORITY_TEAM_1_LEFT])
		    .detail("PriorityTeam0Left", self->priority_relocations[SERVER_KNOBS->PRIORITY_TEAM_0_LEFT])
		    .detail("PrioritySplitShard", self->priority_relocations[SERVER_KNOBS->PRIORITY_SPLIT_SHARD])
		    .trackLatest("MovingData"); // This trace event's trackLatest lifetime is controlled by
		                                // DataDistributor::movingDataEventHolder. The track latest
		                                // key we use here must match the key used in the holder.

		if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && SERVER_KNOBS->ENABLE_DD_PHYSICAL_SHARD) {
			TraceEvent("PhysicalShardMoveStats")
			    .detail("MoveCreateNewPhysicalShard", self->moveCreateNewPhysicalShard)
			    .detail("MoveReusePhysicalShard", self->moveReusePhysicalShard)
			    .detail("RemoteBestTeamNotReady",
			            self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::RemoteBestTeamNotReady])
			    .detail("PrimaryNoHealthyTeam",
			            self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::PrimaryNoHealthyTeam])
			    .detail("RemoteNoHealthyTeam",
			            self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::RemoteNoHealthyTeam])
			    .detail("RemoteTeamIsFull",
			            self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::RemoteTeamIsFull])
			    .detail("RemoteTeamIsNotHealthy",
			            self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::RemoteTeamIsNotHealthy])
			    .detail("UnknownForceNew", self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::UnknownForceNew])
			    .detail("NoAnyHealthy", self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::NoAnyHealthy])
			    .detail("DstOverloaded", self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::DstOverloaded])
			    .detail("NoAvailablePhysicalShard",
			            self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::NoAvailablePhysicalShard])
			    .detail("RetryLimitReached",
			            self->retryFindDstReasonCount[DDQueue::RetryFindDstReason::RetryLimitReached]);
			self->moveCreateNewPhysicalShard = 0;
			self->moveReusePhysicalShard = 0;
			for (int i = 0; i < self->retryFindDstReasonCount.size(); ++i) {
				self->retryFindDstReasonCount[i] = 0;
			}
		}
	}

	static Future<Void> recordMetrics(RunState* state) {
		Future<Void> next = delay(SERVER_KNOBS->DD_QUEUE_LOGGING_INTERVAL);
		while (true) {
			co_await next;
			next = delay(SERVER_KNOBS->DD_QUEUE_LOGGING_INTERVAL, TaskPriority::FlushTrace);
			co_await state->queueMutationLock.take(TaskPriority::FlushTrace);
			FlowLock::Releaser lockGuard(state->queueMutationLock);
			traceMovingData(state->self.getPtr());
			validate(state);
		}
	}

	static Future<Void> answerUnhealthyRelocationCount(RunState* state, FutureStream<Promise<int>> requests) {
		while (true) {
			Promise<int> request = co_await requests;
			co_await state->queueMutationLock.take();
			FlowLock::Releaser lockGuard(state->queueMutationLock);
			request.send(state->self->getUnhealthyRelocationCount());
			validate(state);
		}
	}

	static Future<Void> waitAndValidate(RunState* state, Future<Void> future) {
		Error error;
		try {
			co_await future;
		} catch (Error& e) {
			error = e;
		}
		// A relocator can signal an error inline while launchQueuedWork() is repairing its maps. Keep DD alive
		// until that mutation finishes before propagating the error and tearing the queue down. Preserve the
		// immediate error path when no mutation is active, since taking an available FlowLock still yields.
		if (error.isValid() && state->queueMutationLock.available() > 0) {
			throw error;
		}
		co_await state->queueMutationLock.take();
		FlowLock::Releaser lockGuard(state->queueMutationLock);
		if (error.isValid()) {
			throw error;
		}
		validate(state);
	}

	static Future<Void> run(Reference<DDQueue> self,
	                        Reference<AsyncVar<bool>> processingUnhealthy,
	                        Reference<AsyncVar<bool>> processingWiggle,
	                        FutureStream<Promise<int>> getUnhealthyRelocationCount) {
		RunState state(self);

		std::vector<Future<Void>> ddQueueFutures;
		Future<Void> onCleanUpDataMoveActorError = actorCollection(self->addBackgroundCleanUpDataMoveActor.getFuture());

		// Gate the input stream by the pipeline limit so that DD never tracks more
		// than DD_MAX_PIPELINE_MOVES relocations at once (queued + in-flight).
		PromiseStream<RelocateShard> gatedRelocationStream;
		Future<Void> pipelineGate = pipelineGateActor(self, self->input, gatedRelocationStream);

		for (int i = 0; i < self->teamCollections.size(); i++) {
			ddQueueFutures.push_back(
			    BgDDLoadRebalance(self.getPtr(), i, DataMovementReason::REBALANCE_OVERUTILIZED_TEAM));
			ddQueueFutures.push_back(
			    BgDDLoadRebalance(self.getPtr(), i, DataMovementReason::REBALANCE_UNDERUTILIZED_TEAM));
			if (SERVER_KNOBS->READ_SAMPLING_ENABLED) {
				ddQueueFutures.push_back(
				    BgDDLoadRebalance(self.getPtr(), i, DataMovementReason::REBALANCE_READ_OVERUTIL_TEAM));
				ddQueueFutures.push_back(
				    BgDDLoadRebalance(self.getPtr(), i, DataMovementReason::REBALANCE_READ_UNDERUTIL_TEAM));
			}
		}
		ddQueueFutures.push_back(delayedAsyncVar(self->rawProcessingUnhealthy, processingUnhealthy, 0));
		ddQueueFutures.push_back(delayedAsyncVar(self->rawProcessingWiggle, processingWiggle, 0));
		ddQueueFutures.push_back(self->periodicalRefreshCounter());

		self->validate();

		std::vector<Future<Void>> actors;
		actors.reserve(12);
		actors.push_back(processGatedRelocations(&state, gatedRelocationStream.getFuture()));
		actors.push_back(launchQueuedServerWork(&state));
		actors.push_back(processFetchedSourceServers(&state, self->fetchSourceServersComplete.getFuture()));
		actors.push_back(processCompletedDataTransfers(&state, self->dataTransferComplete.getFuture()));
		actors.push_back(processCompletedRelocations(&state));
		actors.push_back(launchCompletedRanges(&state));
		actors.push_back(recordMetrics(&state));
		actors.push_back(waitAndValidate(&state, self->error.getFuture()));
		actors.push_back(waitAndValidate(&state, waitForAll(ddQueueFutures)));
		actors.push_back(waitAndValidate(&state, pipelineGate));
		actors.push_back(answerUnhealthyRelocationCount(&state, getUnhealthyRelocationCount));
		actors.push_back(waitAndValidate(&state, onCleanUpDataMoveActorError));

		try {
			co_await waitForAll(actors);
		} catch (Error& e) {
			if (e.code() != error_code_broken_promise && // FIXME: Get rid of these broken_promise errors every time
			                                             // we are killed by the master dying
			    e.code() != error_code_movekeys_conflict && e.code() != error_code_data_move_cancelled &&
			    e.code() != error_code_data_move_dest_team_not_found && e.code() != error_code_dd_config_changed) {
				TraceEvent(SevError, "DataDistributionQueueError", self->distributorId).error(e);
			}
			throw;
		}
	}
};

Future<Void> DDQueue::run(Reference<DDQueue> self,
                          Reference<AsyncVar<bool>> processingUnhealthy,
                          Reference<AsyncVar<bool>> processingWiggle,
                          FutureStream<Promise<int>> getUnhealthyRelocationCount,
                          const DDEnabledState* ddEnabledState) {
	self->ddEnabledState = ddEnabledState;
	return DDQueueImpl::run(self, processingUnhealthy, processingWiggle, getUnhealthyRelocationCount);
}
TEST_CASE("/DataDistribution/DDQueue/ServerCounterTrace") {
	double duration = 2.5 * SERVER_KNOBS->DD_QUEUE_COUNTER_REFRESH_INTERVAL;
	DDQueue self;
	Future<Void> counterFuture = self.periodicalRefreshCounter();
	Future<Void> finishFuture = delay(duration);
	std::cout << "Start trace counter unit test for " << duration << "s ...\n";
	while (true) {
		auto res = co_await race(counterFuture, finishFuture, delayJittered(2.0));
		if (res.index() == 0) {
		} else if (res.index() == 1) {
			break;
		} else if (res.index() == 2) {
			std::vector<UID> team(3);
			for (int i = 0; i < team.size(); ++i) {
				team[i] = UID(deterministicRandom()->randomInt(1, 400), 0);
			}
			auto reason = RelocateReason(deterministicRandom()->randomInt(0, RelocateReason::typeCount()));
			auto countType = DDQueue::ServerCounter::randomCountType();
			self.serverCounter.increaseForTeam(team, reason, countType);
			ASSERT(self.serverCounter.get(team[0], reason, countType));
		} else {
			UNREACHABLE();
		}
	}
	std::cout << "Finished.";
}

TEST_CASE("/DataDistribution/DDQueue/DestinationRetryHelperAccounting") {
	Reference<LocalitySet> locality = makeReference<LocalityMap<UID>>();
	auto makeTeam = [&locality](UID id) -> Reference<IDataDistributionTeam> {
		StorageServerInterface ssi(id);
		ssi.locality.set("machineid"_sr, Standalone<StringRef>(id.toString()));
		ssi.locality.set("zoneid"_sr, Standalone<StringRef>(id.toString()));
		auto server = makeReference<TCServerInfo>(ssi, nullptr, ProcessClass(), true, locality);
		return makeReference<TCTeamInfo>(std::vector<Reference<TCServerInfo>>{ server });
	};

	const UID firstId(1, 0);
	const UID secondId(2, 0);
	const UID sourceId(3, 0);
	Reference<IDataDistributionTeam> firstTeam = makeTeam(firstId);
	Reference<IDataDistributionTeam> secondTeam = makeTeam(secondId);
	const int64_t bytes = 100;
	const int64_t readLoad = 10;
	const int workFactor = getDestWorkFactor();
	auto ledgerIsEmpty = [](Busyness const& busyness) {
		return std::all_of(busyness.ledger.begin(), busyness.ledger.end(), [](int work) { return work == 0; });
	};

	RelocateData rd;
	rd.priority = SERVER_KNOBS->PRIORITY_TEAM_HEALTHY;
	rd.src = { sourceId };
	rd.workFactor = workFactor;
	std::map<UID, Busyness> sourceBusymap;
	std::map<UID, Busyness> destBusymap;
	PromiseStream<RelocateData> dataTransferComplete;
	FutureStream<RelocateData> completedTransfers = dataTransferComplete.getFuture();
	ParallelTCInfo healthyDestinations;
	bool ownsDestBusyness = false;
	sourceBusymap[sourceId].addWork(rd.priority, rd.workFactor);

	healthyDestinations.addTeam(firstTeam);
	healthyDestinations.addDataInFlightToTeam(bytes);
	healthyDestinations.addReadInFlightToTeam(readLoad);
	launchDest(rd, { { firstTeam, false } }, destBusymap);
	ownsDestBusyness = true;
	ASSERT_EQ(rd.completeDests, std::vector<UID>{ firstId });
	ASSERT_EQ(destBusymap[firstId].ledger[rd.priority / 100], workFactor);

	// Transfer completion owns the first attempt's destination cleanup, even if the queue has not drained it yet.
	dataTransferComplete.send(rd);
	ASSERT(completedTransfers.isReady());
	ownsDestBusyness = false;
	healthyDestinations.addDataInFlightToTeam(-bytes);
	ParallelTCInfo firstReadCleanup(healthyDestinations);
	resetDestinationsForRetry(rd, healthyDestinations, destBusymap, ownsDestBusyness);
	ASSERT(rd.completeDests.empty());
	ASSERT(healthyDestinations.getServerIDs().empty());
	ASSERT_EQ(destBusymap[firstId].ledger[rd.priority / 100], workFactor);
	ASSERT_EQ(sourceBusymap[sourceId].ledger[rd.priority / 100], rd.workFactor);
	firstReadCleanup.addReadInFlightToTeam(-readLoad);
	ASSERT_EQ(firstTeam->getDataInFlightToTeam(), 0);
	ASSERT_EQ(firstTeam->getReadInFlightToTeam(), 0);

	// A stale, now-unhealthy first team must not be charged or polled by the replacement attempt.
	firstTeam->setHealthy(false);
	healthyDestinations.addTeam(secondTeam);
	ASSERT(healthyDestinations.isHealthy());
	ASSERT_EQ(healthyDestinations.getServerIDs(), std::vector<UID>{ secondId });
	healthyDestinations.addDataInFlightToTeam(bytes);
	healthyDestinations.addReadInFlightToTeam(readLoad);
	launchDest(rd, { { secondTeam, false } }, destBusymap);
	ownsDestBusyness = true;
	ASSERT_EQ(firstTeam->getDataInFlightToTeam(), 0);
	ASSERT_EQ(firstTeam->getReadInFlightToTeam(), 0);
	ASSERT_EQ(secondTeam->getDataInFlightToTeam(), bytes);
	ASSERT_EQ(secondTeam->getReadInFlightToTeam(), readLoad);
	ASSERT_EQ(destBusymap[secondId].ledger[rd.priority / 100], workFactor);

	// A bounded error releases the replacement attempt before another retry.
	healthyDestinations.addDataInFlightToTeam(-bytes);
	ParallelTCInfo secondReadCleanup(healthyDestinations);
	resetDestinationsForRetry(rd, healthyDestinations, destBusymap, ownsDestBusyness);
	secondReadCleanup.addReadInFlightToTeam(-readLoad);
	ASSERT(!ownsDestBusyness);
	ASSERT(rd.completeDests.empty());
	ASSERT(healthyDestinations.getServerIDs().empty());
	ASSERT(ledgerIsEmpty(destBusymap[secondId]));
	ASSERT_EQ(secondTeam->getDataInFlightToTeam(), 0);
	ASSERT_EQ(secondTeam->getReadInFlightToTeam(), 0);

	// The queued first-attempt completion still contains the original destination after later retries mutate rd.
	RelocateData transferComplete = completedTransfers.pop();
	ASSERT_EQ(transferComplete.completeDests, std::vector<UID>{ firstId });
	complete(transferComplete, sourceBusymap, destBusymap);
	ASSERT(ledgerIsEmpty(sourceBusymap[sourceId]));
	ASSERT(ledgerIsEmpty(destBusymap[firstId]));

	// A subsequent successful attempt also balances its destination ledger.
	healthyDestinations.addTeam(secondTeam);
	healthyDestinations.addDataInFlightToTeam(bytes);
	healthyDestinations.addReadInFlightToTeam(readLoad);
	launchDest(rd, { { secondTeam, false } }, destBusymap);
	ownsDestBusyness = true;
	ASSERT_EQ(destBusymap[secondId].ledger[rd.priority / 100], workFactor);
	completeOwnedDest(rd, destBusymap, ownsDestBusyness);
	healthyDestinations.addDataInFlightToTeam(-bytes);
	healthyDestinations.addReadInFlightToTeam(-readLoad);
	ASSERT(!ownsDestBusyness);
	ASSERT(ledgerIsEmpty(destBusymap[secondId]));
	ASSERT_EQ(secondTeam->getDataInFlightToTeam(), 0);
	ASSERT_EQ(secondTeam->getReadInFlightToTeam(), 0);
	co_return;
}

// Verify the batch drain in the relocationComplete handler processes all queued
// completions in one iteration. Simulates a burst of completions arriving at
// once and checks that fetchKeysComplete is fully drained.
TEST_CASE("/DataDistribution/DDQueue/BatchDrainRelocationComplete") {
	DDQueue self;
	self.rawProcessingUnhealthy = makeReference<AsyncVar<bool>>(false);
	self.rawProcessingWiggle = makeReference<AsyncVar<bool>>(false);
	self.pipelineFull = makeReference<AsyncVar<bool>>(false);
	int N = 100;

	std::vector<RelocateData> entries;
	for (int i = 0; i < N; i++) {
		RelocateData rd;
		Key begin = Key(format("key/%05d", i));
		Key end = Key(format("key/%05d/", i));
		rd.keys = KeyRangeRef(begin, end);
		rd.startTime = now();
		entries.push_back(rd);
	}

	self.activeRelocations = N;
	for (auto& rd : entries) {
		self.fetchKeysComplete.insert(rd);
		self.relocationComplete.send(rd);
	}

	ASSERT(self.fetchKeysComplete.size() == N);
	ASSERT(self.activeRelocations == N);

	FutureStream<RelocateData> completions = self.relocationComplete.getFuture();
	RelocateData first = co_await completions;
	self.processRelocationComplete(first);

	// Mirrors the production drain loop. Note: noErrorActors.add(tag(..., rangesComplete)) is
	// not exercised here since that requires a running actor collection.
	int drained = 0;
	while (completions.isReady() && !completions.isError() && drained++ < 1000) {
		RelocateData next = completions.pop();
		self.processRelocationComplete(next);
	}

	ASSERT(drained == N - 1);
	ASSERT(self.fetchKeysComplete.empty());
	ASSERT(self.activeRelocations == 0);

	std::cout << "BatchDrainRelocationComplete: drained " << drained << " of " << N << " completions\n";
}

TEST_CASE("/DataDistribution/DDQueue/RetryDestinationTeamFailure") {
	KeyRange keys(KeyRangeRef("a"_sr, "b"_sr));
	KeyRange parent(KeyRangeRef(""_sr, "z"_sr));
	RelocateShard original(keys, DataMovementReason::SPLIT_SHARD, RelocateReason::SIZE_SPLIT, UID(1, 2));
	original.setParentRange(parent);
	RelocateData rd(original);
	rd.priority = SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY;
	rd.boundaryPriority = SERVER_KNOBS->PRIORITY_SPLIT_SHARD;
	rd.healthPriority = SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY;
	rd.wantsNewServers = true;

	RelocateShard retryRequest = makeDestinationFailureRetry(rd, UID(3, 4));
	RelocateData retry(retryRequest);
	ASSERT(retry.keys == keys);
	ASSERT(retry.priority == rd.priority);
	ASSERT(retry.boundaryPriority == rd.boundaryPriority);
	ASSERT(retry.healthPriority == rd.healthPriority);
	ASSERT(retry.reason == rd.reason);
	ASSERT(retry.dmReason == rd.dmReason);
	ASSERT(retry.wantsNewServers == rd.wantsNewServers);
	ASSERT(retry.randomId == UID(3, 4));
	ASSERT(retry.randomId != rd.randomId);
	retry.startTime = rd.startTime;
	std::set<RelocateData, std::greater<RelocateData>> relocations;
	relocations.insert(retry);
	ASSERT(!relocations.contains(rd));
	relocations.insert(rd);
	ASSERT(relocations.size() == 2);
	ASSERT(retry.getParentRange().present());
	ASSERT(retry.getParentRange().get() == parent);
	ASSERT(retry.src.empty());
	ASSERT(retry.completeSources.empty());
	ASSERT(retry.completeDests.empty());
	ASSERT(retry.workFactor == 0);
	ASSERT(retry.cancellable);
	ASSERT(retry.dataMove == nullptr);
	ASSERT(retry.dataMoveId == anonymousShardId);
	ASSERT(!retry.isRestore());
	ASSERT(!retry.bulkLoadTask.present());
	ASSERT(shouldRetryDestinationTeamFailure(false, rd));
	ASSERT(!shouldRetryDestinationTeamFailure(true, rd));
	RelocateData nested(
	    RelocateShard(KeyRangeRef("a"_sr, "aa"_sr), DataMovementReason::SPLIT_SHARD, RelocateReason::SIZE_SPLIT));
	ASSERT(shouldYieldDestinationFailureRetry(retry, nested));
	ASSERT(!shouldYieldDestinationFailureRetry(retry, retry));
	RelocateData unrelated(
	    RelocateShard(KeyRangeRef("c"_sr, "d"_sr), DataMovementReason::SPLIT_SHARD, RelocateReason::SIZE_SPLIT));
	ASSERT(!shouldYieldDestinationFailureRetry(retry, unrelated));
	RelocateData restore = rd;
	restore.dataMove = std::make_shared<DataMove>();
	ASSERT(shouldRetryDestinationTeamFailure(false, restore));
	ASSERT(!shouldRetryDestinationTeamFailure(true, restore));
	return Void();
}

TEST_CASE("/DataDistribution/DDQueue/SerializeRelocatorError") {
	Reference<DDQueue> self = makeReference<DDQueue>();
	DDQueueImpl::RunState state(self);
	Promise<Void> error;
	Future<Void> propagated;

	{
		co_await state.queueMutationLock.take();
		FlowLock::Releaser lockGuard(state.queueMutationLock);
		propagated = DDQueueImpl::waitAndValidate(&state, error.getFuture());
		error.sendError(movekeys_conflict());
		ASSERT(!propagated.isReady());
	}

	Error observed;
	try {
		co_await propagated;
	} catch (Error& e) {
		observed = e;
	}
	ASSERT(observed.code() == error_code_movekeys_conflict);

	Promise<Void> immediateError;
	Future<Void> immediate = DDQueueImpl::waitAndValidate(&state, immediateError.getFuture());
	immediateError.sendError(movekeys_conflict());
	ASSERT(immediate.isReady());
	ASSERT(immediate.isError());
	ASSERT(immediate.getError().code() == error_code_movekeys_conflict);
	co_return;
}
