/*
 * PerpetualWiggleStatsWorkload.actor.cpp
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

#include "fdbserver/DDTeamCollection.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/DDSharedContext.h"
#include "fdbserver/DDTxnProcessor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/VersionedMap.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
ACTOR Future<bool> IssueConfigurationChange(Database cx, std::string config, bool force) {
	printf("Issuing configuration change: %s\n", config.c_str());
	state ConfigurationResult res = wait(ManagementAPI::changeConfig(cx.getReference(), config, force));
	if (res != ConfigurationResult::SUCCESS) {
		return false;
	}
	wait(delay(5.0)); // wait for read window
	return true;
}
} // namespace

struct PerpetualWiggleStorageMigrationWorkload : public TestWorkload {

	static constexpr auto NAME = "PerpetualWiggleStorageMigrationWorkload";
	StorageWiggleMetrics lastMetrics;

	PerpetualWiggleStorageMigrationWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	/*
	ACTOR static Future<Void> _setup(PerpetualWiggleStorageMigrationWorkload* self, Database cx) {
	    // wait(success(setHealthyZone(cx, ignoreSSFailuresZoneString, 0)));
	    // bool success = wait(IssueConfigurationChange(cx, "storage_migration_type=disabled", true));
	    // ASSERT(success);
	    // wait(delay(30.0)); // make sure the DD has already quit before the test start
	    return Void();
	}

	Future<Void> setup(Database const& cx) override {
	    if (clientId == 0) {
	        return _setup(this, cx); // force to disable DD
	    }
	    return Void();
	} */

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(this, cx);
		}
		return Void();
	};

	Future<bool> check(Database const& cx) override { return true; };

	ACTOR static Future<Void> _start(PerpetualWiggleStorageMigrationWorkload* self, Database cx) {
		state std::vector<StorageServerInterface> storageServers = wait(getStorageServers(cx));
		if (storageServers.size() < 2) {
			TraceEvent("ZZZZZTestDoesNotHaveEnoughStorageServer").detail("StorageServerCount", storageServers.size());
			return Void();
		}
		state StorageServerInterface randomSS1 =
		    storageServers[deterministicRandom()->randomInt(0, storageServers.size())];
		state ISimulator::ProcessInfo* p = g_simulator->getProcessByAddress(randomSS1.address());
		while (!p->isReliable()) {
			randomSS1 = storageServers[deterministicRandom()->randomInt(0, storageServers.size())];
			p = g_simulator->getProcessByAddress(randomSS1.address());
		}

		TraceEvent("ZZZZZFoundProcessToReboot")
		    .detail("ProcessID", randomSS1.locality.processId())
		    .detail("Address", randomSS1.address());

		state StorageServerInterface randomSS2 =
		    storageServers[deterministicRandom()->randomInt(0, storageServers.size())];
		while (randomSS1.locality.processId() == randomSS2.locality.processId()) {
			randomSS2 = storageServers[deterministicRandom()->randomInt(0, storageServers.size())];
		}
		TraceEvent("ZZZZZFoundProcessToMigrate")
		    .detail("ProcessID", randomSS2.locality.processId())
		    .detail("Address", randomSS2.address());

		std::string migrationLocality =
		    LocalityData::keyProcessId.toString() + ":" + randomSS2.locality.processId()->toString();
		// std::string migrationLocality = LocalityData::keyProcessId.toString() + ":101010101";
		bool change =
		    wait(IssueConfigurationChange(cx,
		                                  "storage_engine=ssd-rocksdb-v1 perpetual_storage_wiggle=1 "
		                                  "storage_migration_type=gradual perpetual_storage_wiggle_locality=" +
		                                      migrationLocality,
		                                  true));
		TraceEvent("ZZZZZConfigChangeResult").detail("Success", change);

		// g_simulator->rebootProcess(p, ISimulator::KillType::RebootProcessAndDelete);
		state std::vector<AddressExclusion> servers;
		servers.push_back(AddressExclusion(randomSS1.address().ip, randomSS1.address().port));
		state double timeout = 100.0;
		wait(excludeServers(cx, servers));
		TraceEvent("ZZZZZDoneExcludeServer").log();
		// timeoutError() is needed because sometimes excluding process can take forever
		// std::set<NetworkAddress> inProgress = wait(timeoutError(checkForExcludingServers(cx, servers, true),
		// timeout));
		std::set<NetworkAddress> inProgress = wait(checkForExcludingServers(cx, servers, true));
		TraceEvent("ZZZZZDoneCheckingExcludeServer").log();

		wait(includeServers(cx, std::vector<AddressExclusion>(1)));
		TraceEvent("ZZZZZIncludeServer").log();

		state std::vector<StorageServerInterface> allSSes;
		loop {
			std::vector<StorageServerInterface> SSes = wait(getStorageServers(cx));
			bool foundTarget = false;
			for (auto& ss : SSes) {
				if (ss.address() == randomSS1.address()) {
					foundTarget = true;
				}
			}
			if (foundTarget) {
				allSSes = SSes;
				break;
			}
			wait(delay(10));
		}
		loop {
			std::vector<StorageServerInterface> SSes = wait(getStorageServers(cx));
			allSSes = SSes;
			TraceEvent("ZZZZZCheckingStorageEngineType").log();
			state int i = 0;
			state bool doneCheckingWiggleStorage = false;
			for (i = 0; i < allSSes.size(); ++i) {
				state StorageServerInterface ssInterface = allSSes[i];
				state ReplyPromise<KeyValueStoreType> typeReply;
				ErrorOr<KeyValueStoreType> keyValueStoreType =
				    wait(ssInterface.getKeyValueStoreType.getReplyUnlessFailedFor(typeReply, 2, 0));
				if (keyValueStoreType.present()) {
					TraceEvent("ZZZZZKvStorageType")
					    .detail("SS", ssInterface.address())
					    .detail("StorageType", keyValueStoreType.get().toString());
					if (ssInterface.address() == randomSS1.address()) {
						ASSERT(keyValueStoreType.get().toString() == "ssd-rocksdb-v1");
					}
					if (ssInterface.address() == randomSS2.address()) {
						if (keyValueStoreType.get().toString() == "ssd-rocksdb-v1") {
							TraceEvent("ZZZZZWiggleDone").log();
							doneCheckingWiggleStorage = true;
						}
					}
				} else {
					TraceEvent("ZZZZZKvStorageType").detail("SS", ssInterface.address()).detail("StorageType", "None");
				}
			}
			if (doneCheckingWiggleStorage) {
				break;
			}
			wait(delay(10));
		}
		TraceEvent("ZZZZZFinishTest").log();
		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override { return; }
};

WorkloadFactory<PerpetualWiggleStorageMigrationWorkload> PerpetualWiggleStorageMigrationWorkload;