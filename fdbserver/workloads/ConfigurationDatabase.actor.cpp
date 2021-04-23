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
	KeyRange keys;
	int initialCycleSize;
	int numTransactionsPerClient;
	int numClients;
	int numBroadcasters;
	int numConsumersPerBroadcaster;
	double meanSleepBetweenTransactions;
	double meanSleepWithinTransaction;
	int numSwaps{ 0 };
	int transactionTooOldErrors{ 0 };
	Promise<std::map<uint32_t, uint32_t>> finalSnapshot; // when clients finish, publish final snapshot here

	ACTOR static Future<std::map<uint32_t, uint32_t>> getSnapshot(ConfigurationDatabaseWorkload* self, Database cx) {
		state std::map<uint32_t, uint32_t> result;
		state Reference<IConfigTransaction> tr =
		    makeReference<SimpleConfigTransaction>(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				Standalone<RangeResultRef> range = wait(tr->getRange(self->keys));
				for (const auto& kv : range) {
					result[self->fromKey(kv.key)] = self->fromKey(kv.value);
				}
				return result;
			} catch (Error &e) {
				wait(tr->onError(e));
			}
		}
	}

	Key toKey(uint32_t index) const { return BinaryWriter::toValue(index, Unversioned()).withPrefix(keys.begin); }

	uint32_t fromKey(KeyRef key) const {
		return BinaryReader::fromStringRef<uint32_t>(key.removePrefix(keys.begin), Unversioned());
	}

	ACTOR static Future<int> getCycleLength(ConfigurationDatabaseWorkload const* self,
	                                        Reference<IConfigTransaction> tr) {
		state Standalone<RangeResultRef> range = wait(tr->getRange(self->keys));
		return range.size();
	}

	ACTOR static Future<Void> cycleSwap(ConfigurationDatabaseWorkload* self, Database cx) {
		state Reference<IConfigTransaction> tr =
		    makeReference<SimpleConfigTransaction>(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				int length = wait(getCycleLength(self, tr));
				state Key k0 = self->toKey(deterministicRandom()->randomInt(0, length));
				Optional<Value> _k1 = wait(tr->get(k0));
				state Key k1 = _k1.get();
				wait(delay(deterministicRandom()->random01() * self->meanSleepWithinTransaction));
				Optional<Value> _k2 = wait(tr->get(k1));
				state Key k2 = _k2.get();
				Optional<Value> _k3 = wait(tr->get(k2));
				state Key k3 = _k3.get();
				tr->set(k0, k2);
				tr->set(k1, k3);
				tr->set(k2, k1);
				wait(delay(deterministicRandom()->random01() * self->meanSleepWithinTransaction));
				wait(tr->commit());
				++self->numSwaps;
				return Void();
			} catch (Error &e) {
				if (e.code() == error_code_transaction_too_old) {
					++self->transactionTooOldErrors;
				}
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> runClient(ConfigurationDatabaseWorkload* self, Database cx) {
		state int i = 0;
		for (; i < self->numTransactionsPerClient; ++i) {
			wait(cycleSwap(self, cx));
			wait(delay(2 * self->meanSleepBetweenTransactions * deterministicRandom()->random01()));
		}
		return Void();
	}

	ACTOR static Future<Void> runClients(ConfigurationDatabaseWorkload* self, Database cx) {
		std::vector<Future<Void>> clients;
		for (int i = 0; i < self->numClients; ++i) {
			clients.push_back(runClient(self, cx));
		}
		wait(waitForAll(clients));
		state std::map<uint32_t, uint32_t> finalSnapshot = wait(getSnapshot(self, cx));
		self->finalSnapshot.send(finalSnapshot);
		return Void();
	}

	ACTOR static Future<Version> getCurrentVersion(Reference<ConfigFollowerInterface> cfi) {
		ConfigFollowerGetVersionReply versionReply = wait(cfi->getVersion.getReply(ConfigFollowerGetVersionRequest{}));
		return versionReply.version;
	}

	ACTOR static Future<Void> runConsumer(ConfigurationDatabaseWorkload* self, Reference<ConfigFollowerInterface> cfi) {
		state std::map<uint32_t, uint32_t> database;
		state Version mostRecentVersion = wait(getCurrentVersion(cfi));
		ConfigFollowerGetFullDatabaseReply reply =
		    wait(cfi->getFullDatabase.getReply(ConfigFollowerGetFullDatabaseRequest{ mostRecentVersion, {} }));
		for (const auto& [k, v] : reply.database) {
			database[self->fromKey(k)] = self->fromKey(v);
		}
		state Future<std::map<uint32_t, uint32_t>> finalSnapshot = self->finalSnapshot.getFuture();
		loop {
			state ConfigFollowerGetChangesReply changesReply =
			    wait(cfi->getChanges.getReply(ConfigFollowerGetChangesRequest{ mostRecentVersion, {} }));
			mostRecentVersion = changesReply.mostRecentVersion;
			// wait(cfi->compact.getReply(ConfigFollowerCompactRequest{ mostRecentVersion }));
			for (const auto& versionedMutation : changesReply.versionedMutations) {
				const auto& mutation = versionedMutation.mutation;
				if (mutation.type == MutationRef::SetValue) {
					database[self->fromKey(mutation.param1)] = self->fromKey(mutation.param2);
				} else if (mutation.type == MutationRef::ClearRange) {
					database.erase(database.find(self->fromKey(mutation.param1)),
					               database.find(self->fromKey(mutation.param2)));
				} else {
					ASSERT(false);
				}
			}
			if (finalSnapshot.isReady() && database == finalSnapshot.get()) {
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

	ACTOR static Future<Void> setup(ConfigurationDatabaseWorkload* self, Database cx) {
		state Reference<IConfigTransaction> tr =
		    makeReference<SimpleConfigTransaction>(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				for (int i = 0; i < self->initialCycleSize; ++i) {
					tr->set(self->toKey(i), self->toKey((i + 1) % self->initialCycleSize));
				}
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
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
		Key keyPrefix = getOption(options, "keyPrefix"_sr, "key"_sr);
		keys = KeyRangeRef(keyPrefix.withSuffix("/"_sr), keyPrefix.withSuffix("0"_sr));
		initialCycleSize = getOption(options, "initialCycleSize"_sr, 10);
		numTransactionsPerClient = getOption(options, "numTransactionsPerClient"_sr, 100);
		numClients = getOption(options, "numClients"_sr, 10);
		numBroadcasters = getOption(options, "numBroadcasters"_sr, 2);
		numConsumersPerBroadcaster = getOption(options, "numConsumersPerBroadcaster"_sr, 2);
		meanSleepBetweenTransactions = getOption(options, "meanSleepBetweenTransactions"_sr, 0.1);
		meanSleepWithinTransaction = getOption(options, "meanSleepWithinTransaction"_sr, 0.01);
	}
	Future<Void> setup(Database const& cx) override { return clientId ? Void() : setup(this, cx); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : start(this, cx); }
	Future<bool> check(Database const& cx) override {
		if (clientId > 0) {
			return true;
		}
		// Validate cycle invariant
		auto snapshot = finalSnapshot.getFuture().get();
		int current = 0;
		for (int i = 0; i < snapshot.size(); ++i) {
			if (i > 0 && current == 0)
				return false;
			current = snapshot[current];
		}
		return (current == 0);
	}
	std::string description() const override { return "ConfigurationDatabase"; }
	void getMetrics(std::vector<PerfMetric>& m) override {
		if (clientId == 0) {
			m.push_back(PerfMetric("Swaps", numSwaps, false));
			m.push_back(PerfMetric("TransactionTooOldErrors", transactionTooOldErrors, false));
		}
	}
};

WorkloadFactory<ConfigurationDatabaseWorkload> ConfigurationDatabaseWorkloadFactory("ConfigurationDatabase");
