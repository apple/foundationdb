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
	const bool enabled;

	DataLossRecoveryWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), startMoveKeysParallelismLock(1), finishMoveKeysParallelismLock(1), enabled(!clientId) {}

	std::string description() const override { return "DataLossRecovery"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		std::cout << clientId << " started" << std::endl;
		if (!enabled) {
			std::cout << clientId << " skipping" << std::endl;
			return Void();
		}
		return _start(this, cx);
	}

	ACTOR Future<Void> _start(DataLossRecoveryWorkload* self, Database cx) {
		state std::string key = "TestKey";
		state std::string oldValue = "TestValue";
		state std::string newValue = "TestNewValue";

		wait(self->readWriteKey(cx, key, "", oldValue));
		wait(self->moveRange(self, cx, KeyRangeRef(KeyRef(key), LiteralStringRef("TestKey0"))));
		wait(self->exclude(cx, key));
		wait(self->readWriteKey(cx, key, oldValue, newValue));

		return Void();
	}

	ACTOR Future<Void> readWriteKey(Database cx, std::string key, std::string oldValue, std::string newValue) {
		std::cout << "r" << std::endl;
		std::cout << "d" << std::endl;
		state Transaction tr(cx);
		state StringRef k(key);
		state StringRef v(newValue);

		try {
			Optional<Value> res = wait(tr.get(k));
			if (res.present()) {
				std::cout << "old value: " << res.get().toString();
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}

		loop {
			tr.reset();
			try {
				tr.set(k, v);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		std::cout << "Write Done" << std::endl;

		loop {
			tr.reset();
			try {
				Optional<Value> res = wait(tr.get(k));
				if (res.present()) {
					assert(res.get() == v);
					break;
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<Void> exclude(Database cx, std::string key) {
		int ignore = wait(setDDMode(cx, 1));
		state Transaction tr(cx);
		state StringRef k(key);
		state std::vector<AddressExclusion> servers;
		loop {
			std::cout << "Start exluding..." << std::endl;
			tr.reset();
			servers.clear();
			try {
				Standalone<VectorRef<const char*>> addresses = wait(tr.getAddressesForKey(k));
				for (int i = 0; i < addresses.size(); ++i) {
					std::cout << "Exclude address: " << addresses[i] << std::endl;
					servers.push_back(AddressExclusion::parse(StringRef(std::string(addresses[i]))));
				}
				std::cout << std::endl;
				excludeServers(tr, servers, true);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				std::cout << e.name() << std::endl;
				wait(tr.onError(e));
			}
		}

		std::cout << "Waiting for exclude to complete..." << std::endl;
		// Wait until all data are moved out of servers.
		std::set<NetworkAddress> inProgress = wait(checkForExcludingServers(cx, servers, true));
		assert(inProgress.empty());

		std::cout << "Exclude done." << std::endl;
		// Wait until all data are moved out of servers.
		return Void();
	}

	ACTOR Future<Void> moveRange(DataLossRecoveryWorkload* self, Database cx, KeyRange keys) {
		int ignore = wait(setDDMode(cx, 0));
		state std::string addr;
		state std::vector<UID> dest;

		while (dest.empty()) {
			std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
			if (!interfs.empty()) {
				int idx = random() % interfs.size();
				dest.push_back(interfs[idx].uniqueID);
				addr = interfs[idx].address().toString();
			}
		}
		std::cout << "dest: " << describe(dest) << ", address: " << addr << std::endl;

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

		Transaction tr(cx);
		Standalone<VectorRef<const char*>> addresses = wait(tr.getAddressesForKey(keys.begin));
		assert(addresses.size() == 1);
		assert(std::string(addresses[0]) == address);

		std::cout << "Moved" << std::endl;

		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<DataLossRecoveryWorkload> DataLossRecoveryWorkloadFactory("DataLossRecovery");