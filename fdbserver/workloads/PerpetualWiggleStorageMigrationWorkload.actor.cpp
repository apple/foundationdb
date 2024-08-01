/*
 * PerpetualWiggleStorageMigrationWorkload.actor.cpp
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

constexpr bool hasRocksDB =
#ifdef WITH_ROCKSDB
    true
#else
    false
#endif
    ;

} // namespace

struct PerpetualWiggleStorageMigrationWorkload : public TestWorkload {

	static constexpr auto NAME = "PerpetualWiggleStorageMigrationWorkload";

	PerpetualWiggleStorageMigrationWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		// This test requires exclude/include runs smoothly, so we disable all the failure injection workloads.
		out.insert("all");
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(this, cx);
		}
		return Void();
	};

	Future<bool> check(Database const& cx) override { return true; };

	ACTOR static Future<Void> _start(PerpetualWiggleStorageMigrationWorkload* self, Database cx) {
		if (!hasRocksDB) {
			// RocksDB, which is required by this test, is not supported
			return Void();
		}
		state std::vector<StorageServerInterface> storageServers = wait(getStorageServers(cx));
		// The test should have enough storage servers to exclude.
		ASSERT(storageServers.size() > 3);

		// Pick a storage process to exclude and later include. This process should always use storage engine from
		// `storage_engine` configuration.
		state StorageServerInterface ssToExcludeInclude =
		    storageServers[deterministicRandom()->randomInt(0, storageServers.size())];
		state ISimulator::ProcessInfo* p = g_simulator->getProcessByAddress(ssToExcludeInclude.address());
		while (!p->isReliable()) {
			ssToExcludeInclude = storageServers[deterministicRandom()->randomInt(0, storageServers.size())];
			p = g_simulator->getProcessByAddress(ssToExcludeInclude.address());
		}

		TraceEvent("Test_PickedProcessToExcludeInclude")
		    .detail("ProcessID", ssToExcludeInclude.locality.processId())
		    .detail("Address", ssToExcludeInclude.address());

		// Pick a storage process to migrate to storage engine specified in `perpetual_storage_wiggle_engine`.
		state StorageServerInterface ssToWiggle =
		    storageServers[deterministicRandom()->randomInt(0, storageServers.size())];
		while (ssToExcludeInclude.locality.processId() == ssToWiggle.locality.processId()) {
			ssToWiggle = storageServers[deterministicRandom()->randomInt(0, storageServers.size())];
		}
		TraceEvent("Test_PickedProcessToMigrate")
		    .detail("ProcessID", ssToWiggle.locality.processId())
		    .detail("Address", ssToWiggle.address());

		// Issue a configuration change to ONLY migrate `ssToWiggle` using perpetual wiggle.
		std::string migrationLocality =
		    LocalityData::keyProcessId.toString() + ":" + ssToWiggle.locality.processId()->toString();
		bool change =
		    wait(IssueConfigurationChange(cx,
		                                  "perpetual_storage_wiggle_engine=ssd-rocksdb-v1 perpetual_storage_wiggle=1 "
		                                  "storage_migration_type=gradual perpetual_storage_wiggle_locality=" +
		                                      migrationLocality,
		                                  true));
		TraceEvent("Test_ConfigChangeDone").detail("Success", change);
		ASSERT(change);

		wait(excludeIncludeServer(cx, ssToExcludeInclude));

		wait(validateDatabase(cx, ssToExcludeInclude, ssToWiggle, /*wiggleStorageType=*/"ssd-rocksdb-v1"));

		// We probablistically validate that resetting perpetual_storage_wiggle_engine to none works as expected.
		if (deterministicRandom()->coinflip()) {
			TraceEvent("Test_ClearPerpetualStorageWiggleEngine").log();
			bool change = wait(IssueConfigurationChange(cx, "perpetual_storage_wiggle_engine=none", true));
			TraceEvent("Test_ClearPerpetualStorageWiggleEngineDone").detail("Success", change);
			ASSERT(change);

			// Next, we run exclude and then include `ssToWiggle`. Because perpetual_storage_wiggle_engine is set to
			// none, the engine for `ssToWiggle` should be `storage_engine`.
			wait(excludeIncludeServer(cx, ssToWiggle));
			wait(validateDatabase(cx, ssToExcludeInclude, ssToWiggle, /*wiggleStorageType=*/"ssd-2"));
		}
		return Void();
	}

	ACTOR static Future<Void> excludeIncludeServer(Database cx, StorageServerInterface ssToExcludeInclude) {
		// Now, let's exclude `ssToExcludeInclude` process and include it again. The new SS created on this process
		// should always uses `storage_engine` config, which is `ssd-2`.
		state std::vector<AddressExclusion> servers;
		servers.push_back(AddressExclusion(ssToExcludeInclude.address().ip, ssToExcludeInclude.address().port));

		// Since we have enough storage servers and there won't be any failure, let's use exclude failed to make sure
		// the exclude process can succeed.
		wait(excludeServers(cx, servers, true));
		TraceEvent("Test_DoneExcludeServer").log();

		try {
			std::set<NetworkAddress> inProgress = wait(checkForExcludingServers(cx, servers, true));
			ASSERT(inProgress.empty());
		} catch (Error& e) {
			if (e.code() == error_code_timed_out) {
				// it might never be excluded from serverList
				TraceEvent(SevError, "Test_WaitingForExclusionTakeTooLong").log();
			}
			throw e;
		}

		TraceEvent("Test_CheckingExcludeServerDone").log();

		// Include all the processes the cluster knows.
		wait(includeServers(cx, std::vector<AddressExclusion>(1)));
		TraceEvent("Test_IncludeServerDone").log();

		return Void();
	}

	ACTOR static Future<Void> validateDatabase(Database cx,
	                                           StorageServerInterface ssToExcludeInclude,
	                                           StorageServerInterface ssToWiggle,
	                                           std::string wiggleStorageType) {
		// Wait until `ssToExcludeInclude` to be recruited as storage server again.
		state int missingTargetCount = 0;
		loop {
			std::vector<StorageServerInterface> allStorageServers = wait(getStorageServers(cx));
			bool foundTarget = false;
			for (auto& ss : allStorageServers) {
				if (ss.address() == ssToExcludeInclude.address()) {
					foundTarget = true;
					break;
				}
			}
			if (foundTarget) {
				break;
			}
			++missingTargetCount;
			if (missingTargetCount > 5) {
				// Sometimes, the excluded storage process may not be recruited as storage server again (depending on
				// the process class). So we don't wait indefinitely here.
				break;
			}
			wait(delay(20));
		}

		// Wait until wiggle process to migrate to new storage engine.
		state int missingWiggleStorageCount = 0;
		state std::vector<StorageServerInterface> allSSes;
		state bool doneCheckingWiggleStorage = false;
		state bool containWiggleStorage = false;
		loop {
			std::vector<StorageServerInterface> SSes = wait(getStorageServers(cx));
			allSSes = SSes;

			state int i = 0;
			containWiggleStorage = false;
			doneCheckingWiggleStorage = false;
			for (i = 0; i < allSSes.size(); ++i) {
				state StorageServerInterface ssInterface = allSSes[i];
				state ReplyPromise<KeyValueStoreType> typeReply;
				ErrorOr<KeyValueStoreType> keyValueStoreType =
				    wait(ssInterface.getKeyValueStoreType.getReplyUnlessFailedFor(typeReply, 2, 0));
				if (keyValueStoreType.present()) {
					TraceEvent(SevDebug, "Test_KvStorageType")
					    .detail("StorageServer", ssInterface.address())
					    .detail("StorageType", keyValueStoreType.get().toString());

					if (ssInterface.address() == ssToExcludeInclude.address()) {
						// If `ssToExcludeInclude` exists, it must remain using `storage_engine` type.
						ASSERT(keyValueStoreType.get().toString() == "ssd-2");
					}
					if (ssInterface.address() == ssToWiggle.address()) {
						// If `ssToWiggle` exists, we wait until it is migrate to `perpetual_storage_wiggle_engine`.
						containWiggleStorage = true;
						if (keyValueStoreType.get().toString() == wiggleStorageType) {
							doneCheckingWiggleStorage = true;
						}
					}
				} else {
					TraceEvent(SevDebug, "Test_KvStorageType")
					    .detail("StorageServer", ssInterface.address())
					    .detail("StorageType", "Unknown")
					    .detail("Error", keyValueStoreType.getError().name());
				}
			}
			if (doneCheckingWiggleStorage) {
				break;
			}
			if (!containWiggleStorage) {
				++missingWiggleStorageCount;
				if (missingWiggleStorageCount > 5) {
					TraceEvent("Test_TimeoutWaitingForWiggleStorageToShowUp").log();
					break;
				}
			}
			wait(delay(20));
		}

		if (!doneCheckingWiggleStorage) {
			// If we fail to validate that the wiggle storage has been migrated to new storage engine, sometimes it is
			// because after exclusion, the process may not be recruited as storage server, so we must not see it as a
			// storage engine in the last check.
			ASSERT(!containWiggleStorage);
		}
		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override { return; }
};

WorkloadFactory<PerpetualWiggleStorageMigrationWorkload> PerpetualWiggleStorageMigrationWorkload;
