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

#include "fdbrpc/Replication.h"
#include "fdbserver/TLogGroup.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Error.h"
#include <algorithm>
#include <flow/UnitTest.h>
#include <iterator>
#include <vector>

TLogGroupCollection::TLogGroupCollection(const Reference<IReplicationPolicy>& policy, int groupSize)
  : policy(policy), GROUP_SIZE(groupSize) {}

const std::vector<TLogGroupRef>& TLogGroupCollection::groups() const {
	return recruitedGroups;
}

void TLogGroupCollection::addWorkers(const std::vector<WorkerInterface>& logWorkers) {
	for (const auto& worker : logWorkers) {
		recruitMap.emplace(worker.id(), TLogWorkerData::fromInterface(worker));
	}
}

void TLogGroupCollection::recruitEverything() {
	ASSERT_EQ(recruitMap.size() % GROUP_SIZE, 0); // FIXME
	int numTeams = recruitMap.size() / GROUP_SIZE + (recruitMap.size() % GROUP_SIZE > 0);

	std::vector<TLogGroupRef> newGroups;
	std::unordered_set<UID> selectedServers;
	std::vector<TLogWorkerData*> bestSet;

	printf("---> Going to recruit %d groups\n", numTeams);
	while (newGroups.size() < numTeams) {
		bestSet.clear();
		auto localityMap = buildLocalityMap(selectedServers);

		if (localityMap.selectReplicas(policy, bestSet)) {
			ASSERT_WE_THINK(bestSet.size() == GROUP_SIZE);

			printf("----> Recruiting Group:\n");
			Reference<TLogGroup> group(new TLogGroup());
			for (auto& entry : bestSet) {
				printf("Recruited Group: %s\n", entry->locality.processId().get().toString().c_str());
				group->addServer(Reference<TLogWorkerData>(entry));
				selectedServers.insert(entry->id);
			}

			newGroups.push_back(group);
		} else {
			printf("---> Error recruiting remaining %d logs\n", recruitMap.size() - selectedServers.size());
			// TODO: Currently we add remaining workers to same group. Better way, should just ignore?
			// TODO: What if there are workers less that GROUP_SIZE?
			Reference<TLogGroup> group(new TLogGroup());
			for (const auto& entry : localityMap.getObjects()) {
				printf("Recruited Group: %s\n", entry->locality.processId().get().toString().c_str());
				group->addServer(Reference<TLogWorkerData>((TLogWorkerData*)entry));
				selectedServers.insert(entry->id);
			}
			newGroups.push_back(group);
		}
	}

	ASSERT_EQ(newGroups.size(), numTeams);
	recruitedGroups = newGroups;
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

// Checks if each TLog belongs to only one TLogGroup in 'collection', number of workers inside
// each group is equal to 'groupSize' and the total number of recruited workers iseequal to
// 'totalProcesses', or else will fail assertion.
void checkGroupMembersUnique(const TLogGroupCollection& collection, int groupSize, int totalProcesses) {
	const auto& groups = collection.groups();
	std::unordered_set<UID> foundSoFar;

	int total = 0;
	for (const auto& group : groups) {
		auto servers = group->servers();
		ASSERT_EQ(servers.size(), groupSize);
		for (const auto& s : servers) {
			ASSERT(foundSoFar.find(s->id) == foundSoFar.end());
			foundSoFar.insert(s->id);
			++total;
		}
	}

	ASSERT_EQ(total, totalProcesses);
}

} // namespace testTLogGroup

TEST_CASE("/fdbserver/TLogGroup/basic") {
	using namespace testTLogGroup;

	const int TOTAL_PROCESSES = 27;
	const int GROUP_SIZE = 3;

	Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(
	    new PolicyAcross(GROUP_SIZE, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	std::vector<WorkerInterface> recruits = testTLogGroupRecruits(TOTAL_PROCESSES);

	TLogGroupCollection collection(policy, GROUP_SIZE);
	collection.addWorkers(recruits);
	collection.recruitEverything();

	checkGroupMembersUnique(collection, GROUP_SIZE, TOTAL_PROCESSES);
	return Void();
}
