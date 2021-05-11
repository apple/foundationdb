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

#include "fdbclient/SimpleConfigTransaction.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/ConfigFollowerInterface.h"
#include "fdbserver/IConfigBroadcaster.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // has to be last include

class ConfigurationDatabaseWorkload : public TestWorkload {
	KeyRange keys;
	int minCycleSize;
	int initialCycleSize;
	int maxCycleSize;
	int numTransactionsPerClient;
	int numClients;
	int numBroadcasters;
	int numConsumersPerBroadcaster;
	double meanSleepBetweenTransactions;
	double meanSleepWithinTransaction;
	double meanCompactionInterval;
	double changeSizeProbability;
	int swapCount{ 0 };
	int transactionTooOldCount{ 0 };
	int growCount{ 0 };
	int shrinkCount{ 0 };
	int getChangesCount{ 0 };
	int getFullDatabaseCount{ 0 };
	int compactionCount{ 0 };
	Promise<std::map<uint32_t, uint32_t>> finalSnapshot; // when clients finish, publish final snapshot here
	std::vector<ConfigFollowerInterface> followerInterfaces;

	ACTOR static Future<std::map<uint32_t, uint32_t>> getSnapshot(ConfigurationDatabaseWorkload* self, Database cx) {
		state std::map<uint32_t, uint32_t> result;
		state SimpleConfigTransaction tr(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				Standalone<RangeResultRef> range =
				    wait(tr.getRange(firstGreaterOrEqual(self->keys.begin), lastLessThan(self->keys.end), 1000));
				for (const auto& kv : range) {
					result[self->fromKey(kv.key)] = self->fromKey(kv.value);
				}
				return result;
			} catch (Error &e) {
				wait(tr.onError(e));
			}
		}
	}

	Key toKey(uint32_t index) const {
		return BinaryWriter::toValue(bigEndian32(index), Unversioned()).withPrefix(keys.begin);
	}

	uint32_t fromKey(ConfigKeyRef key) const {
		ASSERT(key.knobName.startsWith(keys.begin));
		return fromBigEndian32(
		    BinaryReader::fromStringRef<uint32_t>(key.knobName.removePrefix(keys.begin), Unversioned()));
	}

	ACTOR static Future<int> getCycleLength(ConfigurationDatabaseWorkload const* self, SimpleConfigTransaction* tr) {
		Standalone<RangeResultRef> range =
		    wait(tr->getRange(firstGreaterOrEqual(self->keys.begin), lastLessThan(self->keys.end), 1000));
		return range.size();
	}

	ACTOR static Future<Void> cycleSwap(ConfigurationDatabaseWorkload* self, Database cx) {
		state SimpleConfigTransaction tr(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				int length = wait(getCycleLength(self, &tr));
				state Key k0 = self->toKey(deterministicRandom()->randomInt(0, length));
				Optional<Value> _k1 = wait(tr.get(k0));
				state Key k1 = _k1.get();
				wait(delay(deterministicRandom()->random01() * self->meanSleepWithinTransaction));
				Optional<Value> _k2 = wait(tr.get(k1));
				state Key k2 = _k2.get();
				Optional<Value> _k3 = wait(tr.get(k2));
				state Key k3 = _k3.get();
				tr.set(k0, k2);
				tr.set(k1, k3);
				tr.set(k2, k1);
				wait(delay(deterministicRandom()->random01() * self->meanSleepWithinTransaction));
				wait(tr.commit());
				TraceEvent(SevDebug, "ConfigDatabaseClientSwap")
				    .detail("Version", tr.getCommittedVersion())
				    .detail("PivotIndex", self->fromKey(k0));
				++self->swapCount;
				return Void();
			} catch (Error &e) {
				if (e.code() == error_code_transaction_too_old) {
					++self->transactionTooOldCount;
				}
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> cycleGrow(ConfigurationDatabaseWorkload* self, Database cx) {
		state SimpleConfigTransaction tr(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				state int length = wait(getCycleLength(self, &tr));
				if (length == self->maxCycleSize) {
					return Void();
				}
				state Key k0 = self->toKey(deterministicRandom()->randomInt(0, length));
				state Key k1 = self->toKey(length);
				Optional<Value> _k2 = wait(tr.get(k0));
				state Key k2 = _k2.get();
				tr.set(k0, k1);
				tr.set(k1, k2);
				wait(delay(2 * deterministicRandom()->random01() * self->meanSleepWithinTransaction));
				wait(tr.commit());
				TraceEvent(SevDebug, "ConfigDatabaseClientGrow")
				    .detail("InsertIndex", self->fromKey(k0))
				    .detail("Version", tr.getCommittedVersion())
				    .detail("NewSize", length + 1);
				++self->growCount;
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_transaction_too_old) {
					++self->transactionTooOldCount;
				}
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> cycleShrink(ConfigurationDatabaseWorkload* self, Database cx) {
		state SimpleConfigTransaction tr(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				state Version currentVersion = wait(tr.getReadVersion());
				state Standalone<RangeResultRef> range =
				    wait(tr.getRange(firstGreaterOrEqual(self->keys.begin), lastLessThan(self->keys.end), 1000));
				if (range.size() == self->minCycleSize) {
					return Void();
				}
				state Key k0;
				state Key k1 = self->toKey(range.size() - 1);
				state Key k2;
				for (const auto& kv : range) {
					if (kv.value == k1) {
						k0 = kv.key;
					} else if (kv.key == k1) {
						k2 = kv.value;
					}
				}
				tr.clear(k1);
				tr.set(k0, k2);
				wait(tr.commit());
				TraceEvent(SevDebug, "ConfigDatabaseClientShrink")
				    .detail("Version", tr.getCommittedVersion())
				    .detail("NewSize", range.size() - 1);
				++self->shrinkCount;
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_transaction_too_old) {
					++self->transactionTooOldCount;
				}
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> runClient(ConfigurationDatabaseWorkload* self, Database cx) {
		state int i = 0;
		for (; i < self->numTransactionsPerClient; ++i) {
			state double p = deterministicRandom()->random01();
			if (p < self->changeSizeProbability * 0.5) {
				wait(cycleGrow(self, cx));
			} else if (p < self->changeSizeProbability) {
				wait(cycleShrink(self, cx));
			} else {
				wait(cycleSwap(self, cx));
			}
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
		TraceEvent("ConfigDatabaseClientsDone").detail("FinalSnapshotSize", finalSnapshot.size());
		return Void();
	}

	ACTOR static Future<Version> getCurrentVersion(ConfigFollowerInterface cfi) {
		ConfigFollowerGetVersionReply versionReply = wait(cfi.getVersion.getReply(ConfigFollowerGetVersionRequest{}));
		return versionReply.version;
	}

	ACTOR static Future<Void> runConsumer(ConfigurationDatabaseWorkload* self, ConfigFollowerInterface cfi) {
		state std::map<uint32_t, uint32_t> database;
		state Version mostRecentVersion = wait(getCurrentVersion(cfi));
		TraceEvent(SevDebug, "ConfigDatabaseConsumerGotInitialDB").detail("Version", mostRecentVersion);
		ConfigFollowerGetFullDatabaseReply reply =
		    wait(cfi.getFullDatabase.getReply(ConfigFollowerGetFullDatabaseRequest{ mostRecentVersion, {} }));
		for (const auto& [k, v] : reply.database) {
			database[self->fromKey(k)] = self->fromKey(v);
		}
		++self->getFullDatabaseCount;
		state Future<std::map<uint32_t, uint32_t>> finalSnapshot = self->finalSnapshot.getFuture();
		loop {
			try {
				state ConfigFollowerGetChangesReply changesReply =
				    wait(cfi.getChanges.getReply(ConfigFollowerGetChangesRequest{ mostRecentVersion, {} }));
				mostRecentVersion = changesReply.mostRecentVersion;
				for (const auto& versionedMutation : changesReply.versionedMutations) {
					const auto& mutation = versionedMutation.mutation;
					if (mutation.type == MutationRef::SetValue) {
						database[self->fromKey(mutation.param1)] = self->fromKey(mutation.param2);
					} else if (mutation.type == MutationRef::ClearRange) {
						// FIXME: Here we're assuming all clears are point clears on existing keys
						auto it = database.find(self->fromKey(mutation.param1));
						ASSERT(it != database.end());
						database.erase(it);
					} else {
						ASSERT(false);
					}
				}
				++self->getChangesCount;
			} catch (Error& e) {
				if (e.code() == error_code_version_already_compacted) {
					Version version = wait(getCurrentVersion(cfi));
					mostRecentVersion = version;
					ConfigFollowerGetFullDatabaseReply reply = wait(
					    cfi.getFullDatabase.getReply(ConfigFollowerGetFullDatabaseRequest{ mostRecentVersion, {} }));
					TraceEvent(SevDebug, "ConfigDatabaseConsumerGotFullDB").detail("Version", mostRecentVersion);
					database.clear();
					for (const auto& [k, v] : reply.database) {
						database[self->fromKey(k)] = self->fromKey(v);
					}
					++self->getFullDatabaseCount;
				} else {
					throw e;
				}
			}
			if (finalSnapshot.isReady() && database == finalSnapshot.get()) {
				TraceEvent("ConfigDatabaseConsumerFinshed");
				return Void();
			}
			wait(delayJittered(0.5));
		}
	}

	ACTOR static Future<Void> runCompactor(ConfigurationDatabaseWorkload* self, ConfigFollowerInterface cfi) {
		loop {
			wait(delay(2 * deterministicRandom()->random01() * self->meanCompactionInterval));
			Version version = wait(getCurrentVersion(cfi));
			wait(cfi.compact.getReply(ConfigFollowerCompactRequest{ version }));
			++self->compactionCount;
		}
	}

	ACTOR static Future<Void> runBroadcasterAndConsumers(ConfigurationDatabaseWorkload* self, Database cx) {
		state SimpleConfigBroadcaster broadcaster(cx->getConnectionFile()->getConnectionString());
		state std::vector<Future<Void>> consumers;
		state ConfigFollowerInterface cfi = self->followerInterfaces.emplace_back();
		for (int i = 0; i < self->numConsumersPerBroadcaster; ++i) {
			consumers.push_back(runConsumer(self, cfi));
		}
		choose {
			when(wait(waitForAll(consumers))) {}
			when(wait(runCompactor(self, cfi))) { ASSERT(false); }
			when(wait(broadcaster.serve(cfi))) { ASSERT(false); }
		}
		return Void();
	}

	ACTOR static Future<Void> setup(ConfigurationDatabaseWorkload* self, Database cx) {
		state SimpleConfigTransaction tr(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				for (int i = 0; i < self->initialCycleSize; ++i) {
					tr.set(self->toKey(i), self->toKey((i + 1) % self->initialCycleSize));
				}
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
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
		minCycleSize = getOption(options, "minCycleSize"_sr, 5);
		initialCycleSize = getOption(options, "initialCycleSize"_sr, 10);
		maxCycleSize = getOption(options, "maxCycleSize"_sr, 15);
		numTransactionsPerClient = getOption(options, "numTransactionsPerClient"_sr, 100);
		numClients = getOption(options, "numClients"_sr, 2);
		numBroadcasters = getOption(options, "numBroadcasters"_sr, 2);
		numConsumersPerBroadcaster = getOption(options, "numConsumersPerBroadcaster"_sr, 1);
		meanSleepBetweenTransactions = getOption(options, "meanSleepBetweenTransactions"_sr, 0.1);
		meanSleepWithinTransaction = getOption(options, "meanSleepWithinTransaction"_sr, 0.01);
		meanCompactionInterval = getOption(options, "meanCompactionInterval"_sr, 1.0);
		changeSizeProbability = getOption(options, "changeSizeProbability"_sr, 0.1);
	}
	Future<Void> setup(Database const& cx) override { return clientId ? Void() : setup(this, cx); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : start(this, cx); }
	Future<bool> check(Database const& cx) override {
		if (clientId > 0) {
			return true;
		}
		// Validate cycle invariant
		auto snapshot = finalSnapshot.getFuture().get();
		uint32_t current = 0;
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
			m.push_back(PerfMetric("SwapCount", swapCount, false));
			m.push_back(PerfMetric("TransactionTooOldCount", transactionTooOldCount, false));
			m.push_back(PerfMetric("GrowCount", growCount, false));
			m.push_back(PerfMetric("ShrinkCount", shrinkCount, false));
			m.push_back(PerfMetric("FinalSize", finalSnapshot.getFuture().get().size(), false));
			m.push_back(PerfMetric("GetChangesCount", getChangesCount, false));
			m.push_back(PerfMetric("GetFullDatabaseCount", getFullDatabaseCount, false));
			m.push_back(PerfMetric("CompactionCount", compactionCount, false));
		}
	}
};

WorkloadFactory<ConfigurationDatabaseWorkload> ConfigurationDatabaseWorkloadFactory("ConfigurationDatabase");
