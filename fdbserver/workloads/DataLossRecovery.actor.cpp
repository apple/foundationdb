/*
 *DataLossRecovery.actor.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct DataLossRecoveryWorkload : TestWorkload {
	FlowLock startMoveKeysParallelismLock;
	FlowLock finishMoveKeysParallelismLock;

	DataLossRecoveryWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), startMoveKeysParallelismLock(1), finishMoveKeysParallelismLock(1) {}

	std::string description() const override { return "DataLossRecovery"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		std::string key = "TestKey";
		Future<Void> f = init(cx, key);
		Future<std::vector<UID>> excluded = exclude(cx, key, f);
		return moveRange(this, cx, KeyRangeRef(KeyRef(key), LiteralStringRef("TestKey0")), excluded);
	}

	ACTOR Future<Void> init(Database cx, std::string key) {
		state Transaction tr(cx);
		state StringRef k(key);
		loop {
			tr.reset();
			try {
				tr.set(k, LiteralStringRef("Value"));
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<std::vector<UID>> exclude(Database cx, std::string key, Future<Void> init) {
		wait(init);
		state Transaction tr(cx);
		state StringRef k(key);
		state std::vector<AddressExclusion> servers;
		state std::unordered_set<std::string> addressSet;
		state std::vector<UID> dest;
		loop {
			tr.reset();
			servers.clear();
			addressSet.clear();
			try {
				Standalone<VectorRef<const char*>> addresses = wait(tr.getAddressesForKey(k));
				for (int i = 0; i < addresses.size(); ++i) {
					std::cout << addresses[i] << std::endl;
					servers.push_back(AddressExclusion::parse(StringRef(std::string(addresses[i]))));
					addressSet.insert(std::string(addresses[i]));
				}
				std::cout << std::endl;
				excludeServers(tr, servers);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				std::cout << e.name() << std::endl;
				wait(tr.onError(e));
			}
		}

		// Wait until all data are moved out of servers.
		std::set<NetworkAddress> inProgress = wait(checkForExcludingServers(cx, servers, true));
		assert(inProgress.empty());

		wait(includeServers(cx, servers, false));

		// Fetch server ids.
		while (dest.empty()) {
			state std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
			for (auto& ssi : interfs) {
				std::cout << "SS: " << ssi.address().toString() << std::endl;
				if (addressSet.count(ssi.address().toString()) > 0) {
					dest.push_back(ssi.uniqueID);
				}
			}
			wait(delay(1));
			std::cout << std::endl;
		}
		std::cout << "dest: " << describe(dest) << std::endl;
		int ignore = wait(setDDMode(cx, 0));

		return dest;
	}

	ACTOR Future<Void> moveRange(DataLossRecoveryWorkload* self,
	                             Database cx,
	                             KeyRange keys,
	                             Future<std::vector<UID>> excluded) {
		std::vector<UID> dest = wait(excluded);
		std::cout << "Recived dest: " << describe(dest) << std::endl;
		state DDEnabledState ddEnabledState;
		wait(moveKeys(cx,
		              keys,
		              dest,
		              dest,
		              MoveKeysLock(),
		              Promise<Void>(),
		              &self->startMoveKeysParallelismLock,
		              &self->finishMoveKeysParallelismLock,
		              false,
		              UID(), // for logging only
		              &ddEnabledState));
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<DataLossRecoveryWorkload> DataLossRecoveryWorkloadFactory("DataLossRecovery");