/*
 * ChangeFeedOperations.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/Util.h"
#include "flow/serialize.h"
#include <cstring>
#include <limits>

#include "flow/actorcompiler.h" // This must be the last #include.

// enable to debug specific operations for a given change feed
#define DEBUG_KEY ""_sr

#define DEBUG_CF(feedKey) (feedKey.printable() == DEBUG_KEY)
// #define DEBUG_CF(feedKey) true

// TODO refactor this to deterministicRandom() from here and BlobGranuleFiles
// picks a number between 2^minExp and 2^maxExp, but uniformly distributed over exponential buckets 2^n an 2^n+1
// For example, randomExp(0, 4) would have a 25% chance of returning 1, a 25% chance of returning 2-3, a 25% chance of
// returning 4-7, and a 25% chance of returning 8-15
int randomExp(int minExp, int maxExp) {
	ASSERT(minExp <= maxExp);
	ASSERT(minExp >= 0);
	if (minExp == maxExp) { // N=2, case
		return 1 << minExp;
	}
	int val = 1 << deterministicRandom()->randomInt(minExp, maxExp);
	ASSERT(val > 0);
	return deterministicRandom()->randomInt(val, val * 2);
}

struct FeedTestData : ReferenceCounted<FeedTestData>, NonCopyable {
	Key key;
	KeyRange keyRange;
	Key feedID;
	int nextVal;
	Future<Void> liveReader;
	bool lastCleared = false;

	Version popVersion;
	Optional<Version> stopVersion;
	bool destroying;
	bool destroyed;
	bool complete;

	std::deque<std::pair<Version, Optional<Value>>> writesByVersion;

	// these were all committed
	std::deque<std::pair<Version, Optional<Value>>> pendingCheck;
	NotifiedVersion checkVersion;

	FeedTestData(Key key)
	  : key(key), keyRange(KeyRangeRef(key, keyAfter(key))), feedID(key.withPrefix(LiteralStringRef("CF"))), nextVal(0),
	    lastCleared(false), popVersion(0), destroying(false), destroyed(false), complete(false), checkVersion(0) {}

	Value nextValue() {
		std::string v = std::to_string(nextVal);
		nextVal++;
		return Value(v);
	}

	void update(Version version, Optional<Value> value) {
		if (!stopVersion.present()) {
			// if feed is stopped, value should not get read
			writesByVersion.push_back({ version, value });
			pendingCheck.push_back(writesByVersion.back());
			checkVersion.set(version);
		}
	}

	void testComplete() {
		complete = true;
		checkVersion.set(checkVersion.get() + 1);
	}
};

static void rollbackFeed(Key key,
                         std::deque<Standalone<MutationsAndVersionRef>>& buffered,
                         Version version,
                         MutationRef rollbackMutation) {
	Version rollbackVersion;
	BinaryReader br(rollbackMutation.param2, Unversioned());
	br >> rollbackVersion;
	TraceEvent("ChangeFeedRollback").detail("Key", key).detail("Ver", version).detail("RollbackVer", rollbackVersion);
	if (DEBUG_CF(key)) {
		fmt::print("DBG) {0} Rolling back {1} -> {2}\n", key.printable(), version, rollbackVersion);
	}
	while (!buffered.empty() && buffered.back().version > rollbackVersion) {
		TraceEvent("ChangeFeedRollbackVer").detail("Ver", buffered.back().version);
		buffered.pop_back();
	}
}

static void checkNextResult(Key key,
                            std::deque<Standalone<MutationsAndVersionRef>>& buffered,
                            std::deque<std::pair<Version, Optional<Value>>>& checkData) {
	// TODO: may need to handle case where update transaction was retried. Should see same value at a
	// lower version and then the correct value at the desired version. Just change version assert to
	// read data <= check data, and only pop pendingCheck if versions are equal
	if (buffered.empty()) {
		fmt::print("{0}) Buffered Empty!! Check Version {1}\n",
		           key.printable(),
		           checkData.empty() ? -1 : checkData.front().first);
	}
	ASSERT(!buffered.empty());
	// FIXME: why some empty mutations?
	if (buffered.front().mutations.size() != 1) {
		fmt::print("{0}) Mutations size @ {1}: {2}\n",
		           key.printable(),
		           buffered.front().version,
		           buffered.front().mutations.size());
	}
	ASSERT(buffered.front().mutations.size() == 1);
	ASSERT(buffered.front().mutations[0].param1 == key);

	// these asserts are correctness of change feed invariants. TODO add trace events for when they fail
	// to debug

	if (checkData.front().second.present()) {
		ASSERT(buffered.front().mutations[0].param2 == checkData.front().second.get());
	} else {
		ASSERT(buffered.front().mutations[0].param2 == keyAfter(key));
	}

	// Handle case where txn retried and wrote same value twice. checkData's version is the committed one, so the same
	// update may appear at an earlier version. This is fine, as long as it then actually appears at the committed
	// version
	// TODO: could strengthen this check a bit and only allow it to appear at the lower version if the txn retried on
	// commit_unknown_result?
	if (checkData.front().first < buffered.front().version) {
		fmt::print("ERROR. {0} Check version {1} != {2}.\n  Check: {3} {4}\n  Buffered: {5} {6}\n",
		           key.printable(),
		           checkData.front().first,
		           buffered.front().version,
		           checkData.front().second.present() ? "SET" : "CLEAR",
		           checkData.front().second.present() ? checkData.front().second.get().printable()
		                                              : keyAfter(key).printable(),
		           buffered.front().mutations[0].type == MutationRef::SetValue ? "SET" : "CLEAR",
		           buffered.front().mutations[0].param2.printable());
	}
	ASSERT(checkData.front().first >= buffered.front().version);

	if (checkData.front().first == buffered.front().version) {
		checkData.pop_front();
	}
	buffered.pop_front();
}

// TODO launch pops occasionally
ACTOR Future<Void> liveReader(Database cx, Reference<FeedTestData> data, Version begin) {
	state Version lastCheckVersion = 0;
	state std::deque<Standalone<MutationsAndVersionRef>> buffered;
	state Reference<ChangeFeedData> results = makeReference<ChangeFeedData>();
	state Future<Void> stream =
	    cx->getChangeFeedStream(results, data->feedID, begin, std::numeric_limits<Version>::max(), data->keyRange);
	try {
		loop {
			if (data->complete && data->pendingCheck.empty()) {
				// done
				return Void();
			}
			choose {
				when(Standalone<VectorRef<MutationsAndVersionRef>> res = waitNext(results->mutations.getFuture())) {
					for (auto& it : res) {
						if (it.mutations.size() == 1 && it.mutations.back().param1 == lastEpochEndPrivateKey) {
							rollbackFeed(data->key, buffered, it.version, it.mutations.back());
						} else {
							if (it.mutations.size() == 0) {
								// FIXME: THIS SHOULD NOT HAPPEN
								// FIXME: these are also getting sent past stopVersion!!
							} else {
								if (data->stopVersion.present()) {
									if (it.version > data->stopVersion.get()) {
										fmt::print("DBG) {0} Read data with version {1} > stop version {2} ({3})\n",
										           data->key.printable(),
										           it.version,
										           data->stopVersion.get(),
										           it.mutations.size());
									}
									ASSERT(it.version <= data->stopVersion.get());
								}
								buffered.push_back(Standalone<MutationsAndVersionRef>(it));
								if (DEBUG_CF(data->key)) {
									fmt::print("DBG) {0} Live read through {1} ({2})\n",
									           data->key.printable(),
									           it.version,
									           it.mutations.size());
								}
							}
						}
					}
				}
				when(wait(data->checkVersion.whenAtLeast(lastCheckVersion + 1))) {
					// wake loop and start new whenAtLeast whenever checkVersion is set
					lastCheckVersion = data->checkVersion.get();
					if (DEBUG_CF(data->key)) {
						fmt::print("DBG) {0} Live can check through {1}\n", data->key.printable(), lastCheckVersion);
					}
				}
				when(wait(data->pendingCheck.empty() ? Never()
				                                     : results->whenAtLeast(data->pendingCheck.front().first))) {
					Version v = buffered.front().version;
					if (DEBUG_CF(data->key)) {
						fmt::print("DBG) {0} Live checking through {1}\n",
						           data->key.printable(),
						           data->pendingCheck.front().first);
					}
					checkNextResult(data->key, buffered, data->pendingCheck);
					if (DEBUG_CF(data->key)) {
						fmt::print("DBG) {0} Live Checked through {1}\n", data->key.printable(), v);
					}
				}
			}
		}
	} catch (Error& e) {
		// TODO when feed is destroyed, this should trigger something
		throw e;
	}
}

ACTOR Future<Void> historicReader(Database cx, Reference<FeedTestData> data, Version begin, Version end) {
	state std::deque<std::pair<Version, Optional<Value>>> checkData;
	state std::deque<Standalone<MutationsAndVersionRef>> buffered;
	state Reference<ChangeFeedData> results = makeReference<ChangeFeedData>();
	state Future<Void> stream = cx->getChangeFeedStream(results, data->feedID, begin, end, data->keyRange);

	// TODO could cpu optimize this
	for (auto& it : data->writesByVersion) {
		if (it.first >= end) {
			break;
		}
		if (it.first >= begin) {
			checkData.push_back(it);
		}
	}

	try {
		loop {
			Standalone<VectorRef<MutationsAndVersionRef>> res = waitNext(results->mutations.getFuture());
			for (auto& it : res) {
				if (it.mutations.size() == 1 && it.mutations.back().param1 == lastEpochEndPrivateKey) {
					rollbackFeed(data->key, buffered, it.version, it.mutations.back());
				} else {
					if (it.mutations.size() == 0) {
						// FIXME: THIS SHOULD NOT HAPPEN
						// FIXME: these are also getting sent past stopVersion!!
					} else {
						if (data->stopVersion.present()) {
							ASSERT(it.version <= data->stopVersion.get());
						}
						buffered.push_back(Standalone<MutationsAndVersionRef>(it));
					}
				}
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream) {
			throw;
		}
	}

	while (!checkData.empty() && !buffered.empty()) {
		checkNextResult(data->key, buffered, checkData);
	}
	ASSERT(checkData.empty());
	if (!buffered.empty()) {
		fmt::print("ERROR {0}: buffered still has {1} for historic read {2} - {3}!\n",
		           data->key.printable(),
		           buffered.size(),
		           begin,
		           end);
		for (int i = 0; i < buffered.size(); i++) {
			fmt::print("  {0}): {1} ({2})\n",
			           buffered[i].version,
			           buffered[i].mutations[0].param2.printable(),
			           buffered[i].mutations.size());
		}
	}
	ASSERT(buffered.empty());

	return Void();
}

enum Op {
	CREATE_DELETE = 0,
	READ = 1,
	UPDATE_CLEAR = 2,
	STOP = 3,
	OP_COUNT = 4 /* keep this last */
};

struct ChangeFeedOperationsWorkload : TestWorkload {
	// test settings
	double testDuration;
	int operationsPerSecond;
	int targetFeeds;
	bool clientsDisjointKeyspace;
	bool clearKeyWhenDestroy;
	double clearFrequency;

	int opWeights[Op::OP_COUNT];
	int totalOpWeight;

	Future<Void> client;
	std::unordered_set<Key> usedKeys;
	std::vector<Reference<FeedTestData>> data;

	ChangeFeedOperationsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 60.0);
		operationsPerSecond = getOption(options, "opsPerSecond"_sr, 100.0);
		int64_t rand = wcx.sharedRandomNumber;
		targetFeeds = randomExp(1, 1 + rand % 10);
		targetFeeds *= (0.8 + (deterministicRandom()->random01() * 0.4));
		targetFeeds = std::max(1, targetFeeds / clientCount);
		rand /= 10;
		clientsDisjointKeyspace = rand % 2;
		rand /= 2;
		clearKeyWhenDestroy = rand % 2;
		rand /= 2;
		bool doStops = rand % 2;
		rand /= 2;
		bool noCreateDelete = rand % 10 == 0;
		rand /= 10;

		ASSERT(clientId >= 0);
		ASSERT(clientId < clientCount);
		ASSERT(clientCount < 255);

		clearFrequency = deterministicRandom()->random01();

		for (int i = 0; i < Op::OP_COUNT; i++) {
			int randWeight = randomExp(0, 5);
			ASSERT(randWeight > 0);
			opWeights[i] = randWeight;
		}

		if (!doStops) {
			opWeights[Op::STOP] = 0;
		}
		if (noCreateDelete) {
			opWeights[Op::CREATE_DELETE] = 0;
		}

		std::string weightString = "|";
		totalOpWeight = 0;
		for (int i = 0; i < Op::OP_COUNT; i++) {
			totalOpWeight += opWeights[i];
			weightString += std::to_string(opWeights[i]) + "|";
		}

		TraceEvent("ChangeFeedOperationsInit")
		    .detail("TargetFeeds", targetFeeds)
		    .detail("DisjointKeyspace", clientsDisjointKeyspace)
		    .detail("ClearWhenDestroy", clearKeyWhenDestroy)
		    .detail("DoStops", doStops)
		    .detail("NoCreateDelete", noCreateDelete)
		    .detail("Weights", weightString);
	}

	Key unusedNewRandomKey() {
		while (true) {
			Key k = newRandomKey();
			if (usedKeys.insert(k).second) {
				return k;
			}
		}
	}

	Key newRandomKey() {
		if (clientsDisjointKeyspace) {
			double keyspaceRange = (1.0 / clientCount);
			double randPartOfRange = deterministicRandom()->random01() * (keyspaceRange - 0.0001);
			double randomDouble = clientId * keyspaceRange + 0.0001 + randPartOfRange;
			return doubleToTestKey(randomDouble);
		} else {
			// this is kinda hacky but it guarantees disjoint keys per client
			Key ret = doubleToTestKey(deterministicRandom()->random01());
			std::string str = ret.toString();
			str.back() = (uint8_t)clientId;
			return Key(str);
		}
	}

	// Pick op with weighted average
	Op pickRandomOp() {
		int r = deterministicRandom()->randomInt(0, totalOpWeight);
		int i = 0;
		while (i < Op::OP_COUNT && (opWeights[i] <= r || opWeights[i] == 0)) {
			r -= opWeights[i];
			i++;
		}
		ASSERT(i < Op::OP_COUNT);
		return (Op)i;
	}

	ACTOR Future<Void> createNewFeed(Database cx, ChangeFeedOperationsWorkload* self) {
		state Transaction tr(cx);
		state Key key = self->unusedNewRandomKey();
		state Reference<FeedTestData> feedData = makeReference<FeedTestData>(key);
		state Value initialValue = feedData->nextValue();

		if (DEBUG_CF(key)) {
			fmt::print("DBG) Creating {0}\n", key.printable());
		}

		loop {
			try {
				tr.set(key, initialValue);
				wait(updateChangeFeed(&tr, feedData->feedID, ChangeFeedStatus::CHANGE_FEED_CREATE, feedData->keyRange));
				wait(tr.commit());

				Version createVersion = tr.getCommittedVersion();
				if (DEBUG_CF(key)) {
					fmt::print("DBG) Created {0} @ {1}\n", key.printable(), createVersion);
				}
				feedData->update(createVersion, initialValue);
				feedData->liveReader = liveReader(cx, feedData, createVersion);

				self->data.push_back(feedData);

				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	std::string description() const override { return "ChangeFeedOperationsWorkload"; }
	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	ACTOR Future<Void> _setup(Database cx, ChangeFeedOperationsWorkload* self) {
		// create initial targetFeeds feeds
		TraceEvent("ChangeFeedOperationsSetup").detail("InitialFeeds", self->targetFeeds).log();
		state int i;
		for (i = 0; i < self->targetFeeds; i++) {
			wait(self->createNewFeed(cx, self));
		}
		TraceEvent("ChangeFeedOperationsSetupComplete");
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		client = changeFeedOperationsClient(cx->clone(), this);
		return delay(testDuration);
	}
	Future<bool> check(Database const& cx) override {
		client = Future<Void>();
		return _check(cx, this);
	}

	ACTOR Future<bool> _check(Database cx, ChangeFeedOperationsWorkload* self) {
		TraceEvent("ChangeFeedOperationsCheck").detail("FeedCount", self->data.size()).log();
		state int i;
		for (i = 0; i < self->data.size(); i++) {
			if (self->data[i]->destroying) {
				continue;
			}
			self->data[i]->testComplete();
			wait(self->data[i]->liveReader);
			// TODO: wait on active reader to be caught up and make sure they completed
			// do final check of non-destroyed feeds by reading everything
			wait(self->doRead(cx, self, i, self->data[i]->writesByVersion.size()));
		}
		// TODO maybe check that all destroyed feeds are actually destroyed?
		TraceEvent("ChangeFeedOperationsCheckComplete");
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> stopFeed(Database cx, ChangeFeedOperationsWorkload* self, int feedIdx) {
		state Reference<FeedTestData> feedData = self->data[feedIdx];
		state Transaction tr(cx);
		if (DEBUG_CF(feedData->key)) {
			fmt::print("DBG) {0} Stopping\n", feedData->key.printable());
		}
		loop {
			try {
				wait(updateChangeFeed(&tr, feedData->feedID, ChangeFeedStatus::CHANGE_FEED_STOP, feedData->keyRange));
				wait(tr.commit());

				Version stopVersion = tr.getCommittedVersion();
				if (!feedData->stopVersion.present()) {
					feedData->stopVersion = stopVersion;
				}
				if (DEBUG_CF(feedData->key)) {
					fmt::print("DBG) {0} Stopped @ {1}\n", feedData->key.printable(), stopVersion);
				}
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> destroyFeed(Database cx, ChangeFeedOperationsWorkload* self, int feedIdx) {
		state Reference<FeedTestData> feedData = self->data[feedIdx];
		state Transaction tr(cx);
		feedData->destroying = true;
		if (DEBUG_CF(feedData->key)) {
			fmt::print("DBG) {0} Destroying\n", feedData->key.printable());
		}
		loop {
			try {
				wait(
				    updateChangeFeed(&tr, feedData->feedID, ChangeFeedStatus::CHANGE_FEED_DESTROY, feedData->keyRange));
				if (self->clearKeyWhenDestroy) {
					tr.clear(feedData->key);
				}
				wait(tr.commit());

				feedData->destroyed = true;
				// remove feed from list
				swapAndPop(&self->data, feedIdx);
				if (DEBUG_CF(feedData->key)) {
					fmt::print("DBG) {0} Destroyed @ {1}\n", feedData->key.printable(), tr.getCommittedVersion());
				}
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> doRead(Database cx, ChangeFeedOperationsWorkload* self, int feedIdx, int targetReadWidth) {
		state Reference<FeedTestData> feedData = self->data[feedIdx];
		Version beginVersion;
		Version endVersion;
		if (targetReadWidth >= feedData->writesByVersion.size()) {
			beginVersion = feedData->writesByVersion.front().first;
			endVersion = feedData->writesByVersion.back().first + 1;
		} else {
			// either up to or including end
			int randStart = deterministicRandom()->randomInt(0, feedData->writesByVersion.size() - targetReadWidth);
			beginVersion = feedData->writesByVersion[randStart].first;
			int end = randStart + targetReadWidth;
			if (end == feedData->writesByVersion.size()) {
				endVersion = feedData->writesByVersion.back().first + 1;
			} else {
				// Make sure last included value (end version -1) is a committed version for checking
				endVersion = feedData->writesByVersion[end].first + 1;
			}
		}

		if (DEBUG_CF(feedData->key)) {
			fmt::print("DBG) {0} Reading @ {1} - {2}\n", feedData->key.printable(), beginVersion, endVersion);
		}

		wait(historicReader(cx, feedData, beginVersion, endVersion));

		if (DEBUG_CF(feedData->key)) {
			fmt::print("DBG) {0} Read complete\n", feedData->key.printable());
		}

		return Void();
	}

	ACTOR Future<Void> doUpdateClear(Database cx, ChangeFeedOperationsWorkload* self, int feedIdx) {
		state Transaction tr(cx);
		state Reference<FeedTestData> feedData = self->data[feedIdx];
		state Optional<Value> updateValue;

		// if value is already not set, don't do a clear, otherwise pick either
		if (feedData->lastCleared || deterministicRandom()->random01() > self->clearFrequency) {
			updateValue = feedData->nextValue();
			if (DEBUG_CF(feedData->key)) {
				fmt::print("DBG) {0} Setting {1}\n", feedData->key.printable(), updateValue.get().printable());
			}
		} else if (DEBUG_CF(feedData->key)) {
			fmt::print("DBG) {0} Clearing\n", feedData->key.printable());
		}
		loop {
			try {
				if (updateValue.present()) {
					tr.set(feedData->key, updateValue.get());
				} else {
					tr.clear(feedData->key);
				}

				wait(tr.commit());

				Version writtenVersion = tr.getCommittedVersion();

				if (DEBUG_CF(feedData->key) && updateValue.present()) {
					fmt::print("DBG) {0} Set {1} @ {2}\n",
					           feedData->key.printable(),
					           updateValue.get().printable(),
					           writtenVersion);
				}
				if (DEBUG_CF(feedData->key) && !updateValue.present()) {
					fmt::print("DBG) {0} Cleared @ {1}\n", feedData->key.printable(), writtenVersion);
				}

				feedData->update(writtenVersion, updateValue);
				return Void();
			} catch (Error& e) {
				// TODO REMOVE
				if (DEBUG_CF(feedData->key)) {
					fmt::print("DBG) {0} Error updating/clearing: {1}\n", feedData->key.printable(), e.name());
				}
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> changeFeedOperationsClient(Database cx, ChangeFeedOperationsWorkload* self) {
		state double last = now();
		loop {
			state Future<Void> waitNextOp = poisson(&last, 1.0 / self->operationsPerSecond);
			Op op = self->pickRandomOp();
			int feedIdx = deterministicRandom()->randomInt(0, self->data.size());
			if (op == Op::CREATE_DELETE) {
				// bundle these together so random creates/deletes keep about the target number of feeds
				if (deterministicRandom()->random01() < 0.5 || self->data.size() == 1) {
					wait(self->createNewFeed(cx, self));
				} else {
					wait(self->destroyFeed(cx, self, feedIdx));
				}
			} else if (op == Op::READ) {
				// relatively small random read
				wait(self->doRead(cx, self, feedIdx, randomExp(2, 8)));
			} else if (op == Op::UPDATE_CLEAR) {
				wait(self->doUpdateClear(cx, self, feedIdx));
			} else if (op == Op::STOP) {
				wait(self->stopFeed(cx, self, feedIdx));
			} else {
				fmt::print("Invalid Op {0}\n", op);
				ASSERT(false);
			}

			wait(waitNextOp);
		}
	}
};

WorkloadFactory<ChangeFeedOperationsWorkload> ChangeFeedOperationsWorkloadFactory("ChangeFeedOperations");
