/*
 * TLogGroup.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include <iostream>
#include <iterator>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/TLogGroup.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/serialize.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // has to be last include

TLogGroupCollection::TLogGroupCollection(const Reference<IReplicationPolicy>& policy, int numGroups, int groupSize)
  : policy(policy), targetNumGroups(numGroups), GROUP_SIZE(groupSize) {}

const std::vector<TLogGroupRef>& TLogGroupCollection::groups() const {
	return recruitedGroups;
}

int TLogGroupCollection::groupSize() const {
	return GROUP_SIZE;
}

int TLogGroupCollection::targetGroupSize() const {
	return targetNumGroups;
}

TLogGroupRef TLogGroupCollection::getGroup(UID groupId) const {
	// TODO: Change the vector of groups to a map for faster lookup.
	for (auto& g : recruitedGroups) {
		if (g->id() == groupId) {
			return g;
		}
	}
	return Reference<TLogGroup>();
}

void TLogGroupCollection::addWorkers(const std::vector<WorkerInterface>& logWorkers) {
	for (const auto& worker : logWorkers) {
		recruitMap.emplace(worker.id(), TLogWorkerData::fromInterface(worker));
	}
}

void TLogGroupCollection::recruitEverything() {
	std::unordered_set<UID> selectedServers;
	std::vector<TLogWorkerData*> bestSet;
	auto localityMap = buildLocalityMap(selectedServers);

	while (recruitedGroups.size() < targetNumGroups) {
		bestSet.clear();

		// TODO: We are doing this randomly for now, but should make sure number of teams served by each
		//   tLog server is approximately same.
		if (localityMap.selectReplicas(policy, bestSet)) {
			// ASSERT_WE_THINK(bestSet.size() == GROUP_SIZE);

			Reference<TLogGroup> group(new TLogGroup());
			for (auto& entry : bestSet) {
				group->addServer(Reference<TLogWorkerData>(entry));
			}

			recruitedGroups.push_back(group);
			TraceEvent("TLogGroupAdd").detail("GroupID", group->id()).detail("Servers", describe(group->servers()));
		} else {
			// TODO: We may have scenarios (simulation), with recruits/zone's < RF. Handle that case.
		}
	}
}

LocalityMap<TLogWorkerData> TLogGroupCollection::buildLocalityMap(const std::unordered_set<UID>& ignoreServers) {
	LocalityMap<TLogWorkerData> localityMap;
	for (const auto& [_, logInterf] : recruitMap) {
		if (ignoreServers.find(logInterf->id) != ignoreServers.end()) {
			// Server already selected.
			continue;
		}
		localityMap.add(logInterf->locality, logInterf.getPtr());
	}
	return localityMap;
}

TLogGroupRef TLogGroupCollection::selectFreeGroup() {
	return deterministicRandom()->randomChoice(recruitedGroups);
}

bool TLogGroupCollection::assignStorageTeam(ptxn::StorageTeamID teamId, TLogGroupRef group) {
	ASSERT(storageTeamToTLogGroupMap.find(teamId) == storageTeamToTLogGroupMap.end());
	storageTeamToTLogGroupMap[teamId] = group;
	group->assignStorageTeam(teamId);
	return true;
}

void TLogGroupCollection::storeState(CommitTransactionRequest* recoveryCommitReq) {
	CommitTransactionRef& tr = recoveryCommitReq->transaction;
	const auto& serversPrefix = LiteralStringRef("/servers");

	tr.clear(recoveryCommitReq->arena, tLogGroupKeys);
	for (const auto& group : recruitedGroups) {
		const auto& groupServerPrefix = tLogGroupKeyFor(group->id()).withSuffix(serversPrefix);
		TraceEvent("TLogGroupStore")
		    .detail("GroupID", group->id())
		    .detail("Size", group->size())
		    .detail("Group", group->toString());
		tr.set(recoveryCommitReq->arena, groupServerPrefix, group->toValue());
	}
}

void TLogGroupCollection::loadState(const Standalone<RangeResultRef>& store) {
	// ASSERT_WE_THINK(store.begin() != nullptr);
	for (int ii = 0; ii < store.size(); ++ii) {
		auto groupId = decodeTLogGroupKey(store[ii].key);
		auto group = TLogGroup::fromValue(groupId, store[ii].value, recruitMap);
		TraceEvent("TLogGroupLoad")
		    .detail("GroupID", group->id())
		    .detail("Size", group->size())
		    .detail("Group", group->toString());
		recruitedGroups.push_back(group);
	}
}

void TLogGroupCollection::seedTLogGroupAssignment(Arena& arena,
                                                  CommitTransactionRef& tr,
                                                  vector<StorageServerInterface> servers) {

	// Collect UID of SS
	std::vector<UID> serverSrcUID;
	serverSrcUID.reserve(servers.size());
	for (auto& s : servers) {
		serverSrcUID.push_back(s.id());
	}

	// Step 1: Create the first storage server team.
	auto teamId = deterministicRandom()->randomUniqueID();
	tr.set(arena, storageTeamIdKey(teamId), encodeStorageTeams(serverSrcUID));

	// Step 2: Map from SS to teamID.
	for (const auto& ss : serverSrcUID) {
		Key teamIdKey = storageServerToTeamIdKey(ss).withSuffix(BinaryWriter::toValue(teamId, Unversioned()));
		BinaryWriter wr(Unversioned());
		wr << 1;
		wr << teamId;
		tr.set(arena, teamIdKey, wr.toValue());
	}

	// Step 3: Assign the storage team to a TLogGroup (Storage Team -> TLogGroup).
	TLogGroupRef group = selectFreeGroup();
	tr.set(arena, storageTeamIdToTLogGroupKey(teamId), BinaryWriter::toValue(group->id(), Unversioned()));
	assignStorageTeam(teamId, group);
	storageTeams[teamId] = serverSrcUID;
}

void TLogGroupCollection::initializeOrRecoverStorageTeamAssignments(Database cx) {
	initializeOrReocverStorageTeamAssignments = initializeOrRecoverStorageTeamAssignmentActor(this, cx);
}

// Returns a map from StorageTeamID that uniquely identifies a team, to the
// list of storage servers in that storage team.
ACTOR Future<std::map<ptxn::StorageTeamID, std::vector<UID>>> fetchStorageTeams(Database cx) {
	state Transaction tr(cx);
	state std::map<ptxn::StorageTeamID, vector<UID>> teams;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			GetRangeLimits limits;
			RangeResult results = wait(tr.getRange(storageTeamIdKeyRange, limits));
			ASSERT(!results.more);

			for (auto team : results) {
				const UID teamId = storageTeamIdKeyDecode(team.key);
				const std::vector<UID> servers = decodeStorageTeams(team.value);
				teams[teamId] = servers;
			}

			break;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			TraceEvent(SevWarn, "TLogGroupFetchTeamError");
			wait(tr.onError(e));
		}
	}

	return teams;
}

// Restore storage team to TLogGroup assignments. This will make sure that across recovery, we don't assign
// a different TLogGroup to a team. It reads the state from `storageTeamIdToTLogGroupRange` range.
//
// TODO (tLogGroup): Reads the existing assignments from recoveryCommit. This can't be a normal transaction as proxy
// isn't accepting commits yet.
ACTOR Future<Void> restoreStorageTeamToGroupAssignment(TLogGroupCollection* self, Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeResult results = wait(tr.getRange(storageTeamIdToTLogGroupRange, GetRangeLimits()));
			for (auto& r : results) {
				auto teamId = decodeStorageTeamIdToTLogGroupKey(r.key);
				auto group = self->getGroup(decodeTLogGroupKey(r.value));
				ASSERT(group.isValid());
				self->assignStorageTeam(teamId, group);
			}
			break;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			wait(tr.onError(e));
		}
	}
	return Void();
}

// TODO (tLogGroup): Set new assignments in recoveryCommitReq. This needs to be refactored accordingly.
ACTOR Future<Void> TLogGroupCollection::initializeOrRecoverStorageTeamAssignmentActor(TLogGroupCollection* self,
                                                                                      Database cx) {
	wait(restoreStorageTeamToGroupAssignment(self, cx));

	loop {
		try {
			state std::map<UID, std::vector<UID>> teams = wait(fetchStorageTeams(cx));
			state std::map<UID, std::vector<UID>>::iterator it;
			for (it = teams.begin(); it != teams.end(); ++it) {
				state ptxn::StorageTeamID teamId = it->first;

				if (self->storageTeams.find(teamId) != self->storageTeams.end()) {
					continue;
				}

				// Save assignment state to \xff keyspace
				state TLogGroupRef group = self->selectFreeGroup();
				state Transaction tr(cx);

				loop {
					try {
						tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
						tr.setOption(FDBTransactionOptions::LOCK_AWARE);
						tr.set(storageTeamIdToTLogGroupKey(teamId), BinaryWriter::toValue(group->id(), Unversioned()));
						wait(tr.commit());
						break;
					} catch (Error& e) {
						if (e.code() == error_code_actor_cancelled) {
							throw;
						}

						TraceEvent(SevWarn, "TLogGroupMonitorAddStorageError")
						    .detail("TeamId", teamId)
						    .detail("GroupId", group->id());
						wait(tr.onError(e));
					}
				}

				// Once \xff key-space is updated, update our internal state.

				// Check if this is already assigned. Possible when we recovered. Check
				// `restoreStorageTramGroupAssignment()`.
				if (self->storageTeamToTLogGroupMap.find(teamId) == self->storageTeamToTLogGroupMap.end()) {

					self->assignStorageTeam(teamId, group);
				}

				self->storageTeams[teamId] = it->second;
			}

			// All teams successfully assigned.
			break;

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			TraceEvent(SevWarn, "TLogGroupMonitorError");
		}
	}

	return Void();
}

void TLogGroup::addServer(const TLogWorkerDataRef& workerData) {
	serverMap.emplace(workerData->id, workerData);
}

std::vector<TLogWorkerDataRef> TLogGroup::servers() const {
	std::vector<TLogWorkerDataRef> results;
	for (auto& [_, worker] : serverMap) {
		results.push_back(worker);
	}
	return results;
}

int TLogGroup::size() const {
	return serverMap.size();
}

void TLogGroup::assignStorageTeam(const UID& teamId) {
	storageTeamSet.insert(teamId);
}

Standalone<StringRef> TLogGroup::toValue() const {
	BinaryWriter result(Unversioned()); // TODO: Add version
	result << serverMap.size();
	for (auto& [id, _] : serverMap) {
		result << id;
	}
	return result.toValue();
}

TLogGroupRef TLogGroup::fromValue(UID groupId,
                                  StringRef value,
                                  const std::unordered_map<UID, TLogWorkerDataRef>& recruits) {
	BinaryReader reader(value, Unversioned()); // TODO : Add version
	int size;
	reader >> size;

	auto group = makeReference<TLogGroup>(groupId);
	for (int ii = 0; ii < size; ++ii) {
		UID id;
		reader >> id;

		auto workerData = recruits.find(id);
		if (workerData == recruits.end()) {
			// TODO: Can happen if the worker died since. Handle the case.
			continue;
		}
		group->addServer(workerData->second);
	}

	return group;
}

std::vector<UID> TLogGroup::serverIds() const {
	std::vector<UID> results;
	for (auto& [id, _] : serverMap) {
		results.push_back(id);
	}
	return results;
}

std::string TLogGroup::toString() const {
	return format("TLogGroup[%s]{logs=%s}", groupId.toString().c_str(), describe(serverIds()).c_str());
}

std::string TLogWorkerData::toString() const {
	return format("TLogWorkerData{id=%s, address=%s, locality=%s",
	              id.toString().c_str(),
	              address.toString().c_str(),
	              locality.toString().c_str());
}

//-------------------------------------------------------------------------------------------------------------------
// Unit Tests

namespace testTLogGroup {

// Returns a vector of size 'processCount' containing mocked WorkerInterface, spread across diffeent localities.
std::vector<WorkerInterface> testTLogGroupRecruits(int processCount) {
	std::vector<WorkerInterface> recruits;
	for (int id = 1; id <= processCount; id++) {
		UID uid(id, 0);
		WorkerInterface interface;
		interface.initEndpoints();

		int process_id = id;
		int dc_id = process_id / 1000;
		int data_hall_id = process_id / 100;
		int zone_id = process_id / 10;
		int machine_id = process_id / 5;

		printf("testMachine: process_id:%d zone_id:%d machine_id:%d ip_addr:%s\n",
		       process_id,
		       zone_id,
		       machine_id,
		       interface.address().toString().c_str());
		interface.locality.set(LiteralStringRef("processid"), Standalone<StringRef>(std::to_string(process_id)));
		interface.locality.set(LiteralStringRef("machineid"), Standalone<StringRef>(std::to_string(machine_id)));
		interface.locality.set(LiteralStringRef("zoneid"), Standalone<StringRef>(std::to_string(zone_id)));
		interface.locality.set(LiteralStringRef("data_hall"), Standalone<StringRef>(std::to_string(data_hall_id)));
		interface.locality.set(LiteralStringRef("dcid"), Standalone<StringRef>(std::to_string(dc_id)));
		recruits.push_back(interface);
	}
	return recruits;
}

void printTLogGroup(const TLogGroupRef& group) {
	using std::cout;
	using std::endl;

	cout << format("  --> TLogGroup [id = %s]", group->id().toString().c_str()) << endl;
	for (const auto& server : group->servers()) {
		cout << "      - " << server->toString() << endl;
	}
}

void printTLogGroupCollection(const TLogGroupCollection& collection) {
	using std::cout;
	using std::endl;

	cout << format("-> TLogGroupCollection {GroupSize = %d [NumRecruits = %d, NumRecruitedGroups = %d]",
	               collection.groupSize(),
	               0, // TODO
	               collection.groups().size())
	     << endl;

	for (const auto& group : collection.groups()) {
		printTLogGroup(group);
	}
}

// Checks if each TLog belongs to only one TLogGroup in 'collection', number of workers inside
// each group is equal to 'groupSize' and the total number of recruited workers is equal to
// 'totalProcesses', or else will fail assertion.
void checkGroupMembersUnique(const TLogGroupCollection& collection, int groupSize, int totalProcesses) {
	const auto& groups = collection.groups();
	ASSERT_EQ(groups.size(), collection.targetGroupSize());

	std::unordered_map<UID, int> groupsPerServer;

	for (const auto& group : groups) {
		auto servers = group->servers();
		ASSERT_EQ(servers.size(), groupSize);
		for (const auto& s : servers) {
			groupsPerServer[s->id] += 1;
		}
	}

	for (const auto [id, ngroups] : groupsPerServer) {
		std::cout << format("Number of TLogGroups served by %s = %d", id.toString().c_str(), ngroups) << std::endl;
	}

	ASSERT_EQ(groupsPerServer.size(), totalProcesses);
}

} // namespace testTLogGroup

TEST_CASE("/fdbserver/TLogGroup/basic") {
	using namespace testTLogGroup;

	const int TOTAL_PROCESSES = 27;
	const int GROUP_SIZE = 3;
	const int NUM_GROUPS = 100;

	Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(
	    new PolicyAcross(GROUP_SIZE, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	std::vector<WorkerInterface> recruits = testTLogGroupRecruits(TOTAL_PROCESSES);

	TLogGroupCollection collection(policy, NUM_GROUPS, GROUP_SIZE);
	collection.addWorkers(recruits);
	collection.recruitEverything();

	printTLogGroupCollection(collection);
	checkGroupMembersUnique(collection, GROUP_SIZE, TOTAL_PROCESSES);
	return Void();
}
