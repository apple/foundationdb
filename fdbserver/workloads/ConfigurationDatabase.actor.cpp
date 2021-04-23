/*
 * ConfigurationDatabase.actor.cpp
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

#include "fdbclient/IConfigTransaction.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/ConfigFollowerInterface.h"
#include "fdbserver/IConfigBroadcaster.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // has to be last include

class ConfigurationDatabaseWorkload : public TestWorkload {

	Key key;
	int numIncrements;
	int numClients;
	int numBroadcasters;
	int numConsumersPerBroadcaster;
	Promise<int> expectedTotal; // when clients finish, publish expected total value here

	ACTOR static Future<int> getCurrentValue(Database cx, Key key) {
		state SimpleConfigTransaction tr(cx->getConnectionFile()->getConnectionString());
		state int result = 0;
		loop {
			try {
				Optional<Value> value = wait(tr.get(key));
				if (value.present()) {
					result = BinaryReader::fromStringRef<int>(value.get(), Unversioned());
				}
				return result;
			} catch (Error &e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> increment(ConfigurationDatabaseWorkload* self, Database cx) {
		state SimpleConfigTransaction tr(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				state int currentValue = 0;
				Optional<Value> value = wait(tr.get(self->key));
				if (value.present()) {
					currentValue = BinaryReader::fromStringRef<int>(value.get(), Unversioned());
				}
				++currentValue;
				tr.set(self->key, BinaryWriter::toValue(currentValue, Unversioned()));
				wait(tr.commit());
				return Void();
			} catch (Error &e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> runClient(ConfigurationDatabaseWorkload* self, Database cx) {
		state int i = 0;
		for (; i < self->numIncrements; ++i) {
			wait(increment(self, cx));
			wait(delay(deterministicRandom()->random01()));
		}
		return Void();
	}

	ACTOR static Future<Void> runClients(ConfigurationDatabaseWorkload* self, Database cx) {
		std::vector<Future<Void>> clients;
		for (int i = 0; i < self->numClients; ++i) {
			clients.push_back(runClient(self, cx));
		}
		wait(waitForAll(clients));
		int expectedTotal = wait(getCurrentValue(cx, self->key));
		self->expectedTotal.send(expectedTotal);
		return Void();
	}

	ACTOR static Future<Version> getCurrentVersion(Reference<ConfigFollowerInterface> cfi) {
		ConfigFollowerGetVersionReply versionReply = wait(cfi->getVersion.getReply(ConfigFollowerGetVersionRequest{}));
		return versionReply.version;
	}

	ACTOR static Future<Void> runConsumer(ConfigurationDatabaseWorkload* self, Reference<ConfigFollowerInterface> cfi) {
		state std::map<Key, Value> database;
		state Version mostRecentVersion = wait(getCurrentVersion(cfi));
		state Future<int> expectedTotal = self->expectedTotal.getFuture();
		state int currentValue = 0;
		loop {
			state ConfigFollowerGetChangesReply reply =
			    wait(cfi->getChanges.getReply(ConfigFollowerGetChangesRequest{ mostRecentVersion, {} }));
			mostRecentVersion = reply.mostRecentVersion;
			for (const auto& versionedMutation : reply.versionedMutations) {
				const auto& mutation = versionedMutation.mutation;
				if (mutation.type == MutationRef::SetValue) {
					database[mutation.param1] = mutation.param2;
				} else if (mutation.type == MutationRef::ClearRange) {
					database.erase(database.find(mutation.param1), database.find(mutation.param2));
				}
			}
			if (database.count(self->key)) {
				currentValue = BinaryReader::fromStringRef<int>(database[self->key], Unversioned());
			}
			if (expectedTotal.isReady() && currentValue >= expectedTotal.get()) {
				return Void();
			}
			wait(delayJittered(0.5));
		}
	}

	ACTOR static Future<Void> runBroadcasterAndConsumers(ConfigurationDatabaseWorkload* self, Database cx) {
		state SimpleConfigBroadcaster broadcaster(cx->getConnectionFile()->getConnectionString());
		state std::vector<Future<Void>> consumers;
		state Reference<ConfigFollowerInterface> cfi = makeReference<ConfigFollowerInterface>();
		for (int i = 0; i < self->numConsumersPerBroadcaster; ++i) {
			consumers.push_back(runConsumer(self, cfi));
		}
		choose {
			when(wait(waitForAll(consumers))) {}
			when(wait(broadcaster.serve(cfi))) { ASSERT(false); }
		}
		return Void();
	}

	ACTOR static Future<Void> start(ConfigurationDatabaseWorkload *self, Database cx) {
		state std::vector<Future<Void>> futures;
		futures.push_back(runClients(self, cx));
		for (int i = 0; i < self->numBroadcasters; ++i) {
			futures.push_back(runBroadcasterAndConsumers(self, cx));
		}
		wait(waitForAll(futures));
		return Void();
	}

public:
	ConfigurationDatabaseWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numIncrements = getOption(options, "numIncrements"_sr, 10);
		numClients = getOption(options, "numClients"_sr, 2);
		numBroadcasters = getOption(options, "numBroadcasters"_sr, 2);
		numConsumersPerBroadcaster = getOption(options, "numConsumersPerBroadcaster"_sr, 2);
	}
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : start(this, cx); }
	Future<bool> check(Database const& cx) override {
		return clientId ? true : (expectedTotal.getFuture().get() >= numClients * numIncrements);
	}
	std::string description() const override { return "ConfigurationDatabase"; }
	void getMetrics(std::vector<PerfMetric>& m) override {
		if (clientId == 0) {
			m.push_back(PerfMetric("TotalWrites", expectedTotal.getFuture().get(), false));
		}
	}
};

WorkloadFactory<ConfigurationDatabaseWorkload> ConfigurationDatabaseWorkloadFactory("ConfigurationDatabase");
