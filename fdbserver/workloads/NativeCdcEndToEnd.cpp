/*
 * NativeCdcEndToEnd.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include <algorithm>
#include <limits>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/functional/hash.hpp>

#include "NativeCdcInternal.h"
#include "fdbserver/core/NativeCdcMetadata.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/clustercontroller/NativeCdcProxyBalancer.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/RecoveryState.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/logsystem/LogSystemConsumer.h"
#include "fdbserver/logsystem/LogSystemFactory.h"
#include "fdbserver/tester/workloads.h"
#include "fdbrpc/simulator.h"
#include "flow/DeterministicRandom.h"

// Exercises native CDC by registering overlapping streams, writing mutations, consuming and acknowledging them,
// and checking delivery, retention, assignment publication, failure recovery, and drain behavior. Test options
// select focused scenarios such as proxy replacement, memory bounds, retired tags, and restart-after-disable drains.
class NativeCdcEndToEndWorkload : public TestWorkload {
	struct ExpectedWrite {
		Version committedVersion;
		std::set<Version> observedVersions;
	};

	struct KeyValueHash {
		size_t operator()(const std::pair<Key, Value>& item) const {
			size_t hash = 0;
			boost::hash_combine(hash, std::hash<Key>{}(item.first));
			boost::hash_combine(hash, std::hash<Value>{}(item.second));
			return hash;
		}
	};

	struct StreamState {
		Key name;
		KeyRange keys;
		Reference<NativeCdcConsumer> consumer;
		std::unordered_map<std::pair<Key, Value>, ExpectedWrite, KeyValueHash> expected;
	};

	class RetagMarkerLedger : public ReferenceCounted<RetagMarkerLedger> {
		Key markerKey;
		std::unordered_map<Value, ExpectedWrite> writes;
		std::unordered_map<Value, Key> markerKeys;
		std::unordered_map<Value, std::set<Version>> epochObservations;
		Version committedThrough = invalidVersion;
		Version acknowledgedThrough = invalidVersion;
		int nextValue = 0;
		int replayedMutations = 0;

	public:
		explicit RetagMarkerLedger(Key key) : markerKey(std::move(key)) {}
		const Key& key() const { return markerKey; }
		Version lastCommittedVersion() const { return committedThrough; }
		int replayCount() const { return replayedMutations; }
		// Every unacknowledged marker must be delivered again after replacement, independently of earlier observations.
		void allowReplay() { epochObservations.clear(); }

		Value expectWrite(int valueBytes, Optional<Key> key = Optional<Key>()) {
			std::string bytes = format("retag/%010d/", nextValue++);
			bytes.resize(valueBytes, 'x');
			Value value{ StringRef(bytes) };
			ASSERT(writes.emplace(value, ExpectedWrite{ invalidVersion, {} }).second);
			markerKeys.emplace(value, key.present() ? key.get() : markerKey);
			return value;
		}

		void committed(Value const& value, Version version) {
			writes.at(value).committedVersion = version;
			committedThrough = std::max(committedThrough, version);
		}

		void observe(CDCConsumeReply const& reply) {
			Version previousGroup = invalidVersion;
			for (const auto& versioned : reply.mutations) {
				ASSERT_GT(versioned.version, previousGroup);
				ASSERT_GT(versioned.version, acknowledgedThrough);
				ASSERT_LE(versioned.version, reply.lastConsumedVersion);
				previousGroup = versioned.version;
				for (const auto& mutation : versioned.mutations) {
					ASSERT_EQ(mutation.type, MutationRef::SetValue);
					const Value value(mutation.param2);
					auto expected = writes.find(value);
					ASSERT(expected != writes.end());
					ASSERT_EQ(mutation.param1, markerKeys.at(value));
					ASSERT(epochObservations[value].insert(versioned.version).second);
					if (expected->second.committedVersion != invalidVersion) {
						ASSERT_LE(versioned.version, expected->second.committedVersion);
					}
					if (!expected->second.observedVersions.insert(versioned.version).second) {
						++replayedMutations;
					}
				}
			}
		}

		void verifyThrough(Version version) const {
			for (const auto& [value, expected] : writes) {
				ASSERT_NE(expected.committedVersion, invalidVersion);
				if (expected.committedVersion > acknowledgedThrough && expected.committedVersion <= version) {
					const auto observed = epochObservations.find(value);
					ASSERT(observed != epochObservations.end());
					ASSERT(observed->second.contains(expected.committedVersion));
				}
			}
		}

		void verifyBoundary(Version boundary) const {
			bool before = false;
			bool after = false;
			for (const auto& [value, expected] : writes) {
				before |= expected.committedVersion < boundary;
				after |= expected.committedVersion >= boundary;
			}
			ASSERT(before && after);
			verifyThrough(committedThrough);
		}

		void acknowledged(Version version) {
			verifyThrough(version);
			acknowledgedThrough = version;
		}
	};

	struct RetagSnapshot {
		NativeCdcTagState state;
		std::vector<CDCTagHistoryEntry> history;
	};

	struct RetagFixtureAttempt {
		Version readVersion;
		Version gapVersion;
		Version committedVersion = invalidVersion;
	};

	struct RetagRestartMarkers {
		CDCStreamId streamId;
		Version before;
		Version cutover;
		Version after;
		Tag oldTag;
		Tag newTag;
	};

	int initialStreamCount;
	int minStreamCount;
	int maxStreamCount;
	int keyCount;
	int writesPerRound;
	int rounds;
	int assignmentPublicationChecks;
	bool testProxyReplacement;
	bool testProxyRebalance;
	bool testProxyRebalanceAutomatic;
	bool testTagOwnership;
	bool injectUndeliveredProxyHalt;
	bool testMemoryBound;
	bool testReplyChunking;
	bool testMultipleRanges;
	bool testOversizedPeek;
	bool testDurableAckScan;
	bool testDelayedRetention;
	bool testRetiredRecovery;
	bool blockRetiredPopWithLiveStream;
	bool testRetiredSharedTagSnapshot;
	bool testRetagCompatibility;
	bool testRetaggingMemoryBound;
	bool prepareRestartDrain;
	bool drainAfterRestart;
	bool testRetaggedRestart;
	bool testRetagTransactionRetries;
	int memoryTestValueBytes;
	double retentionValidationDelay;
	double drainProbability;
	double delayBetweenRounds;
	double operationTimeout;
	int nextStreamNumber = 0;
	Version retentionMarkerVersion = invalidVersion;
	std::vector<StreamState> streams;

	Key keyForIndex(int index) const { return Key(StringRef(format("native-cdc-e2e/data/%04d", index))); }

	KeyRange randomOverlappingRange() const {
		const int middle = keyCount / 2;
		const int begin = deterministicRandom()->randomInt(0, middle + 1);
		const int end = deterministicRandom()->randomInt(middle + 1, keyCount + 1);
		return KeyRange(KeyRangeRef(keyForIndex(begin), keyForIndex(end)));
	}

	Future<Version> writeValues(Database cx, std::vector<std::pair<Key, Value>> values) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				for (const auto& [key, value] : values) {
					tr.set(key, value);
				}
				co_await tr.commit();
				co_return tr.getCommittedVersion();
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Version> writeValue(Database cx, Key key, Value value) {
		std::vector<std::pair<Key, Value>> values;
		values.emplace_back(std::move(key), std::move(value));
		co_return co_await writeValues(cx, std::move(values));
	}

	Future<Version> getReadVersion(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				co_return co_await tr.getReadVersion();
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> acknowledgeDurablyWithoutProxy(Database cx, CDCStreamId streamId, Version consumedThrough) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				Optional<Value> current = co_await tr.get(cdcMinVersionKeyFor(streamId));
				if (!current.present()) {
					throw client_invalid_operation();
				}
				const Version minVersion = consumedThrough + 1;
				if (minVersion <= decodeCDCMinVersionValue(current.get())) {
					co_return;
				}
				tr.set(cdcMinVersionKeyFor(streamId), cdcMinVersionValue(minVersion));
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	void resumeConsumerAfterDirectAcknowledgement(Database cx, CDCStreamId streamId, Version consumedThrough) {
		ASSERT_EQ(streams.size(), 1);
		streams.front().consumer = resumeNativeCdcConsumer(cx, CDCCursor(streamId, consumedThrough));
		CODE_PROBE(true, "Native CDC durable acknowledgement test updates its resumed consumer cursor");
	}

	Future<Void> consumeThroughValue(Reference<NativeCdcConsumer> consumer, Version committed, Key key, Value value) {
		bool observed = false;
		const double deadline = now() + operationTimeout;
		while (consumer->position().lastConsumedVersion < committed) {
			const Version previous = consumer->position().lastConsumedVersion;
			CDCConsumeReply reply = co_await timeoutError(consumer->consume(), operationTimeout);
			if (reply.lastConsumedVersion == previous) {
				ASSERT_LT(now(), deadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT_GT(reply.lastConsumedVersion, previous);
			for (const auto& versioned : reply.mutations) {
				ASSERT_GT(versioned.version, previous);
				ASSERT_LE(versioned.version, reply.lastConsumedVersion);
				for (const auto& mutation : versioned.mutations) {
					if (versioned.version == committed && mutation.type == MutationRef::SetValue &&
					    mutation.param1 == key && mutation.param2 == value) {
						observed = true;
					}
				}
			}
			co_await timeoutError(consumer->acknowledge(), operationTimeout);
		}
		ASSERT(observed);
	}

	Future<CDCProxyInterface> waitForAssignedProxy(Database cx,
	                                               CDCStreamId streamId,
	                                               Optional<UID> previousProxy = Optional<UID>()) {
		while (true) {
			Future<Void> changed = cx->clientInfo->onChange();
			Optional<CDCProxyInterface> assignedProxy;
			{
				const ClientDBInfo& clientInfo = cx->clientInfo->get();
				auto assignment = clientInfo.streamToCDCProxyId.find(streamId);
				if (assignment != clientInfo.streamToCDCProxyId.end() &&
				    (!previousProxy.present() || assignment->second != previousProxy.get())) {
					auto proxy = std::find_if(
					    clientInfo.cdcProxies.begin(),
					    clientInfo.cdcProxies.end(),
					    [&](CDCProxyInterface const& candidate) { return candidate.id() == assignment->second; });
					if (proxy != clientInfo.cdcProxies.end()) {
						assignedProxy = *proxy;
					}
				}
			}
			if (assignedProxy.present()) {
				co_return assignedProxy.get();
			}
			co_await changed;
		}
	}

	Future<Void> addStream(Database cx, KeyRange keys) {
		StreamState stream;
		stream.name = Key(StringRef(format("native-cdc-e2e/stream/%04d", nextStreamNumber++)));
		stream.keys = std::move(keys);
		// GCC 13 cannot lower initializer-list vector arguments inside these awaited expressions.
		const std::vector<KeyRange> ranges{ stream.keys };
		co_await timeoutError(registerNativeCdcStreamClient(cx, stream.name, ranges), operationTimeout);
		stream.consumer = co_await timeoutError(createNativeCdcConsumer(cx, stream.name), operationTimeout);
		streams.push_back(std::move(stream));
	}

	Future<Void> addStream(Database cx) { return addStream(cx, randomOverlappingRange()); }

	Future<Void> initializeStreams(Database cx) {
		if (testProxyRebalance || testProxyRebalanceAutomatic) {
			for (int i = 0; i < initialStreamCount; ++i) {
				co_await addStream(cx, KeyRange(KeyRangeRef(keyForIndex(0), keyForIndex(keyCount))));
			}
			co_return;
		}
		for (int i = 0; i < initialStreamCount; ++i) {
			co_await addStream(cx);
		}
		if (testDelayedRetention) {
			std::vector<std::pair<Key, Value>> marker;
			marker.emplace_back(keyForIndex(keyCount / 2), "retained-across-region-failure"_sr);
			retentionMarkerVersion = co_await writeValues(cx, marker);
			recordExpectedWrites(marker, retentionMarkerVersion);
		}
	}

	Future<Void> initializeOversizedPeekStreams(Database cx) {
		ASSERT_GE(keyCount, 4);
		co_await addStream(cx, KeyRange(KeyRangeRef(keyForIndex(0), keyForIndex(2))));
		co_await addStream(cx, KeyRange(KeyRangeRef(keyForIndex(2), keyForIndex(4))));
	}

	Future<Void> initializeReplyChunkingStream(Database cx) {
		ASSERT_GE(keyCount, 2);
		co_await addStream(cx, KeyRange(KeyRangeRef(keyForIndex(0), keyForIndex(keyCount))));
	}

	Future<Void> initializeRetaggingStreams(Database cx) {
		for (int i = 0; i < initialStreamCount; ++i) {
			const Key key = keyForIndex(i);
			const Key end = testRetaggingMemoryBound ? keyForIndex(i + 1) : keyAfter(key);
			co_await addStream(cx, KeyRange(KeyRangeRef(key, end)));
		}
	}

	Future<RetagSnapshot> readRetagSnapshot(Transaction* tr, int index, int maxStreams) {
		const CDCStreamId streamId = streams[index].consumer->position().streamId;
		const auto states = co_await readNativeCdcTagStates(tr, maxStreams);
		ASSERT(states.present());
		const auto found = std::find_if(states.get().begin(), states.get().end(), [streamId](const auto& state) {
			return state.streamId == streamId;
		});
		ASSERT(found != states.get().end());
		RetagSnapshot result;
		result.state = *found;
		ASSERT(result.state.ranges == std::vector<KeyRange>{ streams[index].keys });
		const RangeResult history = co_await tr->getRange(cdcTagHistoryRangeFor(streamId), 3);
		ASSERT(!history.more && !history.empty() && history.size() <= 2);
		for (const auto& row : history) {
			result.history.push_back(decodeCDCTagHistoryEntry(row.key, row.value));
		}
		co_return result;
	}

	Future<RetagSnapshot> readRetagSnapshot(Database cx, int index, int maxStreams = 16) {
		RetagSnapshot result;
		// NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines) Database::run owns the closure.
		co_await cx.run([this, &result, index, maxStreams](Transaction* tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			result = co_await readRetagSnapshot(tr, index, maxStreams);
		});
		co_return result;
	}

	Future<Version> writeRetagMarkers(Database cx,
	                                  std::vector<int> indices,
	                                  std::vector<Reference<RetagMarkerLedger>> ledgers) {
		std::vector<std::pair<Key, Value>> values;
		values.reserve(indices.size());
		for (const int index : indices) {
			values.emplace_back(ledgers[index]->key(), ledgers[index]->expectWrite(memoryTestValueBytes));
		}
		const Version committed = co_await writeValues(cx, values);
		for (int i = 0; i < static_cast<int>(indices.size()); ++i) {
			ledgers[indices[i]]->committed(values[i].second, committed);
		}
		co_return committed;
	}

	Future<Void> drainRetagMarkers(int index, Reference<RetagMarkerLedger> ledger, bool acknowledge) {
		const double deadline = now() + operationTimeout;
		while (streams[index].consumer->position().lastConsumedVersion < ledger->lastCommittedVersion()) {
			ledger->observe(co_await timeoutError(streams[index].consumer->consume(), operationTimeout));
			ASSERT_LT(now(), deadline);
			co_await delay(0.01);
		}
		ledger->verifyThrough(ledger->lastCommittedVersion());
		if (acknowledge) {
			const Version position = streams[index].consumer->position().lastConsumedVersion;
			co_await timeoutError(streams[index].consumer->acknowledge(), operationTimeout);
			ledger->acknowledged(position);
		}
	}

	Future<Void> waitForCanonicalRetag(Database cx, int index, CDCTagHistoryEntry assignment) {
		const double deadline = now() + operationTimeout;
		while (true) {
			const RetagSnapshot snapshot = co_await readRetagSnapshot(cx, index);
			ASSERT_EQ(snapshot.state.assignment.tag, assignment.tag);
			ASSERT_EQ(snapshot.state.assignment.version, assignment.version);
			if (!snapshot.state.pending) {
				ASSERT_EQ(snapshot.history.size(), 1);
				co_return;
			}
			ASSERT_LT(now(), deadline);
			co_await delay(0.05);
		}
	}

	Future<RetagSnapshot> commitRetagFixture(Database cx,
	                                         int index,
	                                         RetagSnapshot original,
	                                         Tag destination,
	                                         int maxStreams,
	                                         std::vector<Reference<RetagMarkerLedger>> gapLedgers) {
		ASSERT(!original.state.pending);
		ASSERT_EQ(original.history.size(), 1);
		std::unordered_map<Key, std::vector<RetagFixtureAttempt>> attempts;
		bool readRetryInjected = false;
		bool commitRetryInjected = false;
		bool ambiguousCommit = false;
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				if (testRetagTransactionRetries && !readRetryInjected) {
					readRetryInjected = true;
					throw future_version();
				}
				RetagSnapshot snapshot = co_await readRetagSnapshot(&tr, index, maxStreams);
				ASSERT_EQ(snapshot.state.streamId, original.state.streamId);
				ASSERT(snapshot.state.ranges == original.state.ranges);
				ASSERT_EQ(snapshot.state.minVersion, original.state.minVersion);
				if (snapshot.state.pending) {
					ASSERT_EQ(snapshot.history.size(), 2);
					ASSERT_EQ(snapshot.history.front().tag, original.state.assignment.tag);
					ASSERT_EQ(snapshot.history.front().version, original.state.assignment.version);
					ASSERT_EQ(snapshot.state.assignment.tag, destination);
					// An ambiguous commit may already have installed our exact history row. Never turn that retry into
					// another move, or accept an unrelated move merely because it chose the same destination.
					const auto submitted = attempts.find(snapshot.state.historyKey);
					ASSERT(submitted != attempts.end());
					const Version cutover = snapshot.state.assignment.version;
					ASSERT_LT(snapshot.state.minVersion, cutover);
					bool matchesAttempt = false;
					for (const auto& attempt : submitted->second) {
						if (attempt.committedVersion != invalidVersion) {
							ASSERT_EQ(cutover, attempt.committedVersion);
						}
						matchesAttempt |= cutover > attempt.readVersion &&
						                  (attempt.gapVersion == invalidVersion ||
						                   (attempt.gapVersion > attempt.readVersion && cutover > attempt.gapVersion));
					}
					ASSERT(matchesAttempt);
					ASSERT(!testRetagTransactionRetries || (readRetryInjected && ambiguousCommit));
					CODE_PROBE(ambiguousCommit, "Native CDC retag fixture recognizes its own ambiguous committed move");
					co_return snapshot;
				}
				ASSERT_EQ(snapshot.history.size(), 1);
				ASSERT_EQ(snapshot.state.historyKey, original.state.historyKey);
				ASSERT_EQ(snapshot.state.assignment.tag, original.state.assignment.tag);
				ASSERT_EQ(snapshot.state.assignment.version, original.state.assignment.version);
				const bool prepared = co_await retagNativeCdcStream(&tr, snapshot.state, destination);
				ASSERT(prepared);
				const Version readVersion = co_await tr.getReadVersion();
				Version gapVersion = invalidVersion;
				if (!gapLedgers.empty()) {
					// Each retried attempt needs its own separately committed marker after its fresh read version.
					// Failed attempts remain in the ledger and must still be delivered.
					gapVersion = co_await writeRetagMarkers(cx, { index }, gapLedgers);
					ASSERT_GT(gapVersion, readVersion);
				}
				const Key historyKey = cdcTagHistoryKeyFor(snapshot.state.streamId, readVersion, destination);
				auto& attempt = attempts[historyKey].emplace_back(RetagFixtureAttempt{ readVersion, gapVersion });
				co_await tr.commit();
				attempt.committedVersion = tr.getCommittedVersion();
				if (testRetagTransactionRetries && !commitRetryInjected) {
					commitRetryInjected = true;
					throw commit_unknown_result();
				}
				tr.reset();
				continue;
			} catch (Error& e) {
				ambiguousCommit |= e.code() == error_code_commit_unknown_result;
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> assertRetagRejected(Database cx, NativeCdcTagState expected, Tag destination) {
		// NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines) Database::run owns the closure.
		co_await cx.run([expected = std::move(expected), destination](Transaction* tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			const bool prepared = co_await retagNativeCdcStream(tr, expected, destination);
			ASSERT(!prepared);
		});
	}

	Future<RetagSnapshot> retagAcrossConcurrentWrite(Database cx,
	                                                 int index,
	                                                 Tag destination,
	                                                 std::vector<Reference<RetagMarkerLedger>> ledgers) {
		const RetagSnapshot original = co_await readRetagSnapshot(cx, index);
		RetagSnapshot snapshot = co_await commitRetagFixture(cx, index, original, destination, 16, ledgers);
		co_await assertRetagRejected(cx, original.state, destination);
		co_await assertRetagRejected(cx, snapshot.state, original.state.assignment.tag);
		CODE_PROBE(true, "Native CDC live retag uses its commit boundary and rejects stale or pending moves");
		co_return snapshot;
	}

	Future<Version> writeRetagBatch(Database cx, Reference<RetagMarkerLedger> ledger) {
		std::vector<std::pair<Key, Value>> values;
		// Small independent mutations fit in one raw peek but expand beyond it when materialized.
		for (int i = 0; i < 12; ++i) {
			Key key = ledger->key().withSuffix(StringRef(format("/%02d", i)));
			values.emplace_back(key, ledger->expectWrite(32, key));
		}
		const Version committed = co_await writeValues(cx, values);
		for (const auto& [key, value] : values) {
			ledger->committed(value, committed);
		}
		co_return committed;
	}

	Future<Void> consumeRetagWithAckPause(int index,
	                                      Reference<RetagMarkerLedger> ledger,
	                                      Version through,
	                                      Reference<AsyncVar<int>> firstBatches,
	                                      Future<Void> releaseAcknowledgements) {
		bool first = true;
		while (streams[index].consumer->position().lastConsumedVersion < through) {
			const CDCConsumeReply reply = co_await streams[index].consumer->consume();
			ledger->observe(reply);
			ledger->verifyThrough(reply.lastConsumedVersion);
			if (first) {
				if (reply.mutations.empty()) {
					continue;
				}
				first = false;
				firstBatches->set(firstBatches->get() + 1);
				co_await releaseAcknowledgements;
			}
			co_await streams[index].consumer->acknowledge();
			ledger->acknowledged(reply.lastConsumedVersion);
		}
		ASSERT(!first);
		ledger->verifyThrough(ledger->lastCommittedVersion());
	}

	Future<int64_t> retainedTagBytes(Tag tag, Version begin, Version end) {
		Reference<LogSystemConsumer> logs = makeLogSystemConsumerFromServerDBInfo(UID(), dbInfo->get());
		Reference<IReplayPeekCursor> cursor = logs->peekSingle(UID(), begin, tag);
		int64_t bytes = 0;
		while (cursor->version().version <= end) {
			if (!cursor->hasMessage()) {
				co_await cursor->getMore();
				ASSERT_LE(cursor->popped(), begin);
				continue;
			}
			bytes += cursor->getMessageWithTags().size();
			cursor->nextMessage();
		}
		co_return bytes;
	}

	void checkRetagBufferStatus(CDCProxyBufferStatus const& status) const {
		ASSERT_GE(status.bufferedBytes, 0);
		ASSERT_LE(status.bufferedBytes, status.activePermits);
		ASSERT_LE(status.activePermits, status.bufferLimit);
		ASSERT_LE(status.peakActivePermits, status.bufferLimit);
	}

	Future<CDCProxyBufferStatus> getRetagBufferStatus(Database cx, UID owner) {
		const auto result = co_await timeoutError(
		    getAssignedProxyStatus(cx, streams.front().consumer->position().streamId), operationTimeout);
		ASSERT_EQ(result.first.id(), owner);
		checkRetagBufferStatus(result.second);
		co_return result.second;
	}

	Future<Void> validateRetaggingMemoryBound(Database cx) {
		ASSERT_EQ(streams.size(), 2);
		const RetagSnapshot original = co_await readRetagSnapshot(cx, 0);
		const RetagSnapshot destination = co_await readRetagSnapshot(cx, 1);
		ASSERT_NE(original.state.assignment.tag, destination.state.assignment.tag);
		ASSERT_EQ(original.state.proxyId, destination.state.proxyId);
		const Tag oldTag = original.state.assignment.tag;
		const Tag newTag = destination.state.assignment.tag;
		auto moving = makeReference<RetagMarkerLedger>(streams[0].keys.begin);
		auto active = makeReference<RetagMarkerLedger>(streams[1].keys.begin);
		const Version before = co_await writeRetagBatch(cx, moving);
		const double oldestCommittedAt = now();
		const RetagSnapshot pending = co_await commitRetagFixture(cx, 0, original, newTag, 16, {});
		const Version destinationVersion = co_await writeRetagBatch(cx, active);
		const Version after = co_await writeRetagBatch(cx, moving);
		ASSERT_LT(before, pending.state.assignment.version);
		ASSERT_GE(after, pending.state.assignment.version);

		ASSERT_LT(operationTimeout, SERVER_KNOBS->CDC_PROXY_CONSUME_POLL_TIMEOUT);
		const double consumeStarted = now();
		Promise<Void> releaseAcknowledgements;
		auto firstBatches = makeReference<AsyncVar<int>>(0);
		std::vector<Future<Void>> consumers{
			consumeRetagWithAckPause(0, moving, after, firstBatches, releaseAcknowledgements.getFuture()),
			consumeRetagWithAckPause(1, active, after, firstBatches, releaseAcknowledgements.getFuture())
		};
		// Both real readers must deliver before either acknowledges, and well before a consume lease can expire.
		while (firstBatches->get() < 2) {
			co_await timeoutError(firstBatches->onChange(), std::max(0.0, consumeStarted + operationTimeout - now()));
		}
		ASSERT_LT(now() - consumeStarted, SERVER_KNOBS->CDC_PROXY_CONSUME_POLL_TIMEOUT);
		auto status = co_await getRetagBufferStatus(cx, original.state.proxyId);
		ASSERT_GT(status.bufferedBytes, 0);
		const int64_t oldBytes = co_await timeoutError(retainedTagBytes(oldTag, before, before), operationTimeout);
		const int64_t newBytes =
		    co_await timeoutError(retainedTagBytes(newTag, destinationVersion, after), operationTimeout);
		ASSERT_GT(oldBytes, 0);
		ASSERT_GT(newBytes, 0);
		const double pauseStarted = now();
		co_await delay(retentionValidationDelay);
		ASSERT_EQ(co_await timeoutError(retainedTagBytes(oldTag, before, before), operationTimeout), oldBytes);
		ASSERT_EQ(co_await timeoutError(retainedTagBytes(newTag, destinationVersion, after), operationTimeout),
		          newBytes);
		const RetagSnapshot held = co_await readRetagSnapshot(cx, 0);
		ASSERT(held.state.pending);
		ASSERT_EQ(held.state.minVersion, original.state.minVersion);
		status = co_await getRetagBufferStatus(cx, original.state.proxyId);
		TraceEvent("NativeCdcRetagRetentionPause")
		    .detail("OldTagBytes", oldBytes)
		    .detail("DestinationTagBytes", newBytes)
		    .detail("PauseSeconds", now() - pauseStarted)
		    .detail("OldestCommitAgeSeconds", now() - oldestCommittedAt)
		    .detail("BufferedBytes", status.bufferedBytes)
		    .detail("PeakActivePermits", status.peakActivePermits);
		releaseAcknowledgements.send(Void());
		co_await timeoutError(waitForAll(consumers), operationTimeout);
		co_await waitForCanonicalRetag(cx, 0, pending.state.assignment);
		Reference<LogSystemConsumer> logs = makeLogSystemConsumerFromServerDBInfo(UID(), dbInfo->get());
		co_await timeoutError(logs->waitForPopped(pending.state.assignment.version, oldTag), operationTimeout);
		co_await timeoutError(logs->waitForPopped(after + 1, newTag), operationTimeout);
		status = co_await getRetagBufferStatus(cx, original.state.proxyId);
		ASSERT_EQ(status.bufferedBytes, 0);
		CODE_PROBE(true, "Native CDC retagging delivers both streams before acknowledgement within a bounded budget");
		CODE_PROBE(true, "Native CDC retains both retag histories during an acknowledgement pause then drains");
		for (const auto& stream : streams) {
			co_await removeNativeCdcStreamClient(cx, stream.name);
		}
		streams.clear();
		co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
	}

	Future<Void> validateRetagCompatibility(Database cx) {
		ASSERT_EQ(streams.size(), 4);
		ASSERT_EQ(cx->clientInfo->get().nativeCdcTagCount, 2);
		std::vector<Reference<RetagMarkerLedger>> ledgers;
		std::vector<RetagSnapshot> initial;
		for (int i = 0; i < static_cast<int>(streams.size()); ++i) {
			ledgers.push_back(makeReference<RetagMarkerLedger>(streams[i].keys.begin));
			initial.push_back(co_await readRetagSnapshot(cx, i));
			ASSERT(!initial.back().state.pending);
			ASSERT_EQ(initial.back().state.proxyId, initial.front().state.proxyId);
		}
		const Tag originalTag = initial.front().state.assignment.tag;
		const Tag destination(tagLocalityCDC, originalTag.id == 0 ? 1 : 0);
		int sibling = -1;
		for (int i = 1; i < static_cast<int>(streams.size()); ++i) {
			if (initial[i].state.assignment.tag == originalTag) {
				sibling = i;
				break;
			}
		}
		ASSERT_GE(sibling, 0);
		co_await writeRetagMarkers(cx, { 0, 1, 2, 3 }, ledgers);
		const RetagSnapshot pending = co_await retagAcrossConcurrentWrite(cx, 0, destination, ledgers);
		co_await writeRetagMarkers(cx, { 0, 1, 2, 3 }, ledgers);
		co_await drainRetagMarkers(0, ledgers[0], false);
		ledgers[0]->verifyBoundary(pending.state.assignment.version);
		const RetagSnapshot unacknowledged = co_await readRetagSnapshot(cx, 0);
		ASSERT_EQ(unacknowledged.history.size(), 2);

		const CDCProxyInterface originalOwner =
		    co_await timeoutError(waitForAssignedProxy(cx, pending.state.streamId), operationTimeout);
		ledgers[0]->allowReplay();
		const int previousReplays = ledgers[0]->replayCount();
		co_await timeoutError(haltProxyUntilReplaced(cx, originalOwner, false), operationTimeout);
		co_await timeoutError(waitForAssignedProxy(cx, pending.state.streamId, originalOwner.id()), operationTimeout);
		co_await forceTransactionSystemRecovery();
		co_await writeRetagMarkers(cx, { 0, sibling }, ledgers);
		co_await drainRetagMarkers(0, ledgers[0], false);
		ASSERT_GT(ledgers[0]->replayCount(), previousReplays);
		ledgers[0]->verifyBoundary(pending.state.assignment.version);
		const RetagSnapshot recovered = co_await readRetagSnapshot(cx, 0);
		ASSERT_EQ(recovered.state.minVersion, initial[0].state.minVersion);
		co_await drainRetagMarkers(0, ledgers[0], true);
		co_await waitForCanonicalRetag(cx, 0, pending.state.assignment);
		CODE_PROBE(true, "Native CDC compatibility reader replays both retag intervals after recovery");

		// An unacknowledged sibling still protects the retired tag after this stream's transition is canonical.
		const RetagSnapshot lagging = co_await readRetagSnapshot(cx, sibling);
		ASSERT_EQ(lagging.state.minVersion, initial[sibling].state.minVersion);
		ASSERT_EQ(lagging.state.assignment.tag, originalTag);
		// NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines) Database::run owns the closure.
		co_await cx.run([originalTag, pending](Transaction* tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			const Optional<Value> retired = co_await tr->get(cdcRetiredTagPopKeyFor(originalTag));
			const Optional<Value> watermark = co_await tr->get(cdcRetiredTagPopVersionKeyFor(originalTag));
			ASSERT(retired.present() && watermark.present());
			ASSERT_GE(decodeCDCMinVersionValue(watermark.get()), pending.state.assignment.version);
		});
		const RetagSnapshot returned = co_await retagAcrossConcurrentWrite(cx, 0, originalTag, ledgers);
		co_await writeRetagMarkers(cx, { 0 }, ledgers);
		co_await drainRetagMarkers(0, ledgers[0], true);
		ledgers[0]->verifyBoundary(returned.state.assignment.version);
		co_await waitForCanonicalRetag(cx, 0, returned.state.assignment);
		for (int i = 1; i < static_cast<int>(streams.size()); ++i) {
			co_await drainRetagMarkers(i, ledgers[i], true);
		}
		CODE_PROBE(true,
		           "Native CDC retag compatibility preserves shared-tag data and supports returning to an old tag");
		for (const auto& stream : streams) {
			co_await timeoutError(removeNativeCdcStreamClient(cx, stream.name), operationTimeout);
		}
		streams.clear();
		co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
		co_await timeoutError(waitForFullyRecovered(), operationTimeout);
	}

	Future<Void> validatePublicLifecycle(Database cx) {
		const Key name = "native-cdc-e2e/lifecycle"_sr;
		const KeyRange keys(KeyRangeRef("native-cdc-e2e/lifecycle/"_sr, "native-cdc-e2e/lifecycle0"_sr));
		const KeyRange conflictingKeys(KeyRangeRef("native-cdc-e2e/lifecycle/"_sr, "native-cdc-e2e/lifecycle1"_sr));
		const std::vector<KeyRange> ranges{ keys };

		const CDCStreamId streamId =
		    co_await timeoutError(registerNativeCdcStreamClient(cx, name, ranges), operationTimeout);
		ASSERT_EQ(co_await timeoutError(registerNativeCdcStreamClient(cx, name, ranges), operationTimeout), streamId);

		bool conflictingRegistrationRejected = false;
		try {
			const std::vector<KeyRange> conflictingRanges{ conflictingKeys };
			co_await timeoutError(registerNativeCdcStreamClient(cx, name, conflictingRanges), operationTimeout);
		} catch (Error& e) {
			if (e.code() != error_code_client_invalid_operation) {
				throw;
			}
			conflictingRegistrationRejected = true;
		}
		ASSERT_EQ(conflictingRegistrationRejected, true);

		const std::vector<NativeCdcStreamInfo> listed =
		    co_await timeoutError(listNativeCdcStreamsClient(cx), operationTimeout);
		auto found = std::find_if(
		    listed.begin(), listed.end(), [&](NativeCdcStreamInfo const& stream) { return stream.name == name; });
		ASSERT_EQ(found != listed.end(), true);
		ASSERT_EQ(found->streamId, streamId);
		ASSERT_EQ(found->ranges.size(), 1);
		ASSERT_EQ(found->ranges.front(), keys);

		bool futureConsumeRejected = false;
		try {
			co_await timeoutError(
			    resumeNativeCdcConsumer(cx, CDCCursor(streamId, std::numeric_limits<Version>::max() - 2))->consume(),
			    operationTimeout);
		} catch (Error& e) {
			if (e.code() != error_code_client_invalid_operation) {
				throw;
			}
			futureConsumeRejected = true;
		}
		ASSERT_EQ(futureConsumeRejected, true);

		bool unprovenConsumeRejected = false;
		try {
			const Version unprovenVersion = co_await getReadVersion(cx);
			co_await timeoutError(resumeNativeCdcConsumer(cx, CDCCursor(streamId, unprovenVersion))->consume(),
			                      operationTimeout);
		} catch (Error& e) {
			if (e.code() != error_code_client_invalid_operation) {
				throw;
			}
			unprovenConsumeRejected = true;
		}
		ASSERT_EQ(unprovenConsumeRejected, true);

		bool futureAcknowledgeRejected = false;
		try {
			co_await timeoutError(
			    resumeNativeCdcConsumer(cx, CDCCursor(streamId, std::numeric_limits<Version>::max() - 2))
			        ->acknowledge(),
			    operationTimeout);
		} catch (Error& e) {
			if (e.code() != error_code_client_invalid_operation) {
				throw;
			}
			futureAcknowledgeRejected = true;
		}
		ASSERT_EQ(futureAcknowledgeRejected, true);

		co_await timeoutError(removeNativeCdcStreamClient(cx, name), operationTimeout);

		bool staleAcknowledgeRejected = false;
		try {
			co_await timeoutError(resumeNativeCdcConsumer(cx, CDCCursor(streamId, 0))->acknowledge(), operationTimeout);
		} catch (Error& e) {
			if (e.code() != error_code_client_invalid_operation) {
				throw;
			}
			staleAcknowledgeRejected = true;
		}
		ASSERT_EQ(staleAcknowledgeRejected, true);
	}

	Future<Void> validateClearClipping(Database cx) {
		const Key name = "native-cdc-e2e/clear-stream"_sr;
		const KeyRange keys(KeyRangeRef("native-cdc-e2e/clear/c"_sr, "native-cdc-e2e/clear/m"_sr));
		const KeyRange lowerClear(KeyRangeRef("native-cdc-e2e/clear/a"_sr, "native-cdc-e2e/clear/f"_sr));
		const KeyRange upperClear(KeyRangeRef("native-cdc-e2e/clear/j"_sr, "native-cdc-e2e/clear/z"_sr));
		const KeyRange expectedLower(KeyRangeRef(keys.begin, lowerClear.end));
		const KeyRange expectedUpper(KeyRangeRef(upperClear.begin, keys.end));

		const std::vector<KeyRange> ranges{ keys };
		co_await timeoutError(registerNativeCdcStreamClient(cx, name, ranges), operationTimeout);
		Reference<NativeCdcConsumer> consumer =
		    co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);

		Version committed;
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.clear(lowerClear);
				tr.clear(upperClear);
				co_await tr.commit();
				committed = tr.getCommittedVersion();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		bool sawLower = false;
		bool sawUpper = false;
		while (consumer->position().lastConsumedVersion < committed) {
			CDCConsumeReply reply = co_await timeoutError(consumer->consume(), operationTimeout);
			for (const auto& versioned : reply.mutations) {
				if (versioned.version != committed) {
					continue;
				}
				for (const auto& mutation : versioned.mutations) {
					ASSERT_EQ(mutation.type, MutationRef::ClearRange);
					const KeyRangeRef cleared(mutation.param1, mutation.param2);
					if (cleared.begin == expectedLower.begin && cleared.end == expectedLower.end) {
						sawLower = true;
					} else if (cleared.begin == expectedUpper.begin && cleared.end == expectedUpper.end) {
						sawUpper = true;
					} else {
						ASSERT(false);
					}
				}
			}
			co_await timeoutError(consumer->acknowledge(), operationTimeout);
		}
		ASSERT(sawLower);
		ASSERT(sawUpper);
		co_await timeoutError(removeNativeCdcStreamClient(cx, name), operationTimeout);
	}

	Future<Void> validateAssignmentPublicationOnce(Database cx, int check) {
		const KeyRange keys(KeyRangeRef("native-cdc-e2e/assignment/data/"_sr, "native-cdc-e2e/assignment/data0"_sr));
		const Key name = Key(StringRef(format("native-cdc-e2e/assignment/%04d", check)));
		const Key key = Key(StringRef(format("native-cdc-e2e/assignment/data/%04d", check)));
		const Value value = Value(StringRef(format("assignment-value/%04d", check)));

		co_await delay(0.1);
		const std::vector<KeyRange> ranges{ keys };
		const CDCStreamId streamId =
		    co_await timeoutError(registerNativeCdcStreamClient(cx, name, ranges), operationTimeout);
		Reference<NativeCdcConsumer> consumer =
		    co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
		ASSERT_EQ(consumer->position().streamId, streamId);

		const Version committed = co_await writeValue(cx, key, value);
		co_await consumeThroughValue(consumer, committed, key, value);
		co_await timeoutError(removeNativeCdcStreamClient(cx, name), operationTimeout);
	}

	Key multipleRangeKey(StringRef suffix) const {
		return suffix.withPrefix("native-cdc-e2e/multiple-ranges/data/"_sr);
	}

	KeyRange multipleRangeKeys(StringRef begin, StringRef end) const {
		return KeyRange(KeyRangeRef(multipleRangeKey(begin), multipleRangeKey(end)));
	}

	Future<Void> consumeExpectedVersion(Reference<NativeCdcConsumer> consumer,
	                                    Version committed,
	                                    Standalone<VectorRef<MutationRef>> expected) {
		const double deadline = now() + operationTimeout;
		bool observed = false;
		while (!observed) {
			ASSERT_LT(now(), deadline);
			CDCConsumeReply reply = co_await timeoutError(consumer->consume(), deadline - now());
			for (const auto& versioned : reply.mutations) {
				ASSERT_LE(versioned.version, reply.lastConsumedVersion);
				if (versioned.version != committed) {
					continue;
				}
				ASSERT(!observed);
				ASSERT_EQ(versioned.mutations.size(), expected.size());
				for (int i = 0; i < expected.size(); ++i) {
					ASSERT_EQ(versioned.mutations[i].type, expected[i].type);
					ASSERT_EQ(versioned.mutations[i].param1, expected[i].param1);
					ASSERT_EQ(versioned.mutations[i].param2, expected[i].param2);
				}
				observed = true;
			}
		}
		ASSERT_GE(consumer->position().lastConsumedVersion, committed);
	}

	Future<Version> writeMultipleRangeClear(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.set(multipleRangeKey("e"_sr), "before-left"_sr);
				tr.set(multipleRangeKey("q"_sr), "before-middle"_sr);
				tr.set(multipleRangeKey("w"_sr), "before-right"_sr);
				tr.set(multipleRangeKey("h"_sr), "before-gap"_sr);
				tr.clear(multipleRangeKeys("d"_sr, "x"_sr));
				tr.set(multipleRangeKey("e"_sr), "after-left"_sr);
				tr.set(multipleRangeKey("q"_sr), "after-middle"_sr);
				tr.set(multipleRangeKey("y"_sr), "after-right"_sr);
				tr.clear(multipleRangeKeys("g"_sr, "j"_sr));
				co_await tr.commit();
				co_return tr.getCommittedVersion();
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> validateMultipleRanges(Database cx) {
		ASSERT(streams.empty());
		ASSERT_EQ(cx->clientInfo->get().nativeCdcTagCount, 1);
		const Key name = "native-cdc-e2e/multiple-ranges"_sr;
		const Key gapName = "native-cdc-e2e/multiple-ranges-gap"_sr;
		const std::vector<KeyRange> ranges{ multipleRangeKeys("b"_sr, "f"_sr),
			                                multipleRangeKeys("m"_sr, "r"_sr),
			                                multipleRangeKeys("w"_sr, "z"_sr) };
		const std::vector<KeyRange> registrationRanges{
			multipleRangeKeys("m"_sr, "r"_sr), multipleRangeKeys("c"_sr, "f"_sr), multipleRangeKeys("b"_sr, "d"_sr),
			multipleRangeKeys("b"_sr, "c"_sr), multipleRangeKeys("x"_sr, "z"_sr), multipleRangeKeys("w"_sr, "x"_sr),
			multipleRangeKeys("m"_sr, "r"_sr)
		};
		const CDCStreamId streamId =
		    co_await timeoutError(registerNativeCdcStreamClient(cx, name, registrationRanges), operationTimeout);
		ASSERT_EQ(co_await timeoutError(registerNativeCdcStreamClient(cx, name, ranges), operationTimeout), streamId);
		bool changedGapsRejected = false;
		try {
			const std::vector<KeyRange> mergedRanges{ multipleRangeKeys("b"_sr, "z"_sr) };
			co_await timeoutError(registerNativeCdcStreamClient(cx, name, mergedRanges), operationTimeout);
		} catch (Error& e) {
			if (e.code() != error_code_client_invalid_operation) {
				throw;
			}
			changedGapsRejected = true;
		}
		ASSERT(changedGapsRejected);
		const std::vector<NativeCdcStreamInfo> listed =
		    co_await timeoutError(listNativeCdcStreamsClient(cx), operationTimeout);
		const auto found = std::find_if(
		    listed.begin(), listed.end(), [&](NativeCdcStreamInfo const& stream) { return stream.name == name; });
		ASSERT(found != listed.end());
		ASSERT_EQ(found->streamId, streamId);
		ASSERT_EQ(found->ranges, ranges);

		// Route excluded keys onto the same tag so the proxy must filter shared-tag false positives.
		const std::vector<KeyRange> gapRanges{ multipleRangeKeys("a"_sr, "zz"_sr) };
		co_await timeoutError(registerNativeCdcStreamClient(cx, gapName, gapRanges), operationTimeout);
		Reference<NativeCdcConsumer> consumer =
		    co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
		ASSERT_EQ(consumer->position().streamId, streamId);
		std::vector<std::pair<Key, Value>> values;
		for (StringRef suffix :
		     { "a"_sr, "b"_sr, "e"_sr, "f"_sr, "h"_sr, "m"_sr, "q"_sr, "r"_sr, "s"_sr, "w"_sr, "y"_sr, "z"_sr }) {
			values.emplace_back(multipleRangeKey(suffix), suffix);
		}
		Standalone<VectorRef<MutationRef>> expected;
		for (StringRef suffix : { "b"_sr, "e"_sr, "m"_sr, "q"_sr, "w"_sr, "y"_sr }) {
			expected.push_back_deep(expected.arena(),
			                        MutationRef(MutationRef::SetValue, multipleRangeKey(suffix), suffix));
		}
		const Version written = co_await writeValues(cx, values);
		co_await consumeExpectedVersion(consumer, written, expected);
		co_await timeoutError(consumer->acknowledge(), operationTimeout);

		expected = Standalone<VectorRef<MutationRef>>();
		for (const auto& [suffix, value] : { std::pair("e"_sr, "before-left"_sr),
		                                     std::pair("q"_sr, "before-middle"_sr),
		                                     std::pair("w"_sr, "before-right"_sr) }) {
			expected.push_back_deep(expected.arena(),
			                        MutationRef(MutationRef::SetValue, multipleRangeKey(suffix), value));
		}
		for (const auto& range : { multipleRangeKeys("d"_sr, "f"_sr), ranges[1], multipleRangeKeys("w"_sr, "x"_sr) }) {
			expected.push_back_deep(expected.arena(), MutationRef(MutationRef::ClearRange, range.begin, range.end));
		}
		for (const auto& [suffix, value] : { std::pair("e"_sr, "after-left"_sr),
		                                     std::pair("q"_sr, "after-middle"_sr),
		                                     std::pair("y"_sr, "after-right"_sr) }) {
			expected.push_back_deep(expected.arena(),
			                        MutationRef(MutationRef::SetValue, multipleRangeKey(suffix), value));
		}
		const Version cleared = co_await writeMultipleRangeClear(cx);
		co_await consumeExpectedVersion(consumer, cleared, expected);

		CDCProxyInterface original = co_await timeoutError(waitForAssignedProxy(cx, streamId), operationTimeout);
		co_await timeoutError(haltProxyUntilReplaced(cx, original, false), operationTimeout);
		CDCProxyInterface replacement =
		    co_await timeoutError(waitForAssignedProxy(cx, streamId, original.id()), operationTimeout);
		ASSERT_NE(original.id(), replacement.id());
		// One unacknowledged version must be replayed in full, including all disjoint clear fragments.
		co_await consumeExpectedVersion(consumer, cleared, expected);
		co_await timeoutError(consumer->acknowledge(), operationTimeout);
		const CDCCursor checkpoint = consumer->position();
		ASSERT_EQ(checkpoint.streamId, streamId);
		const std::vector<NativeCdcStreamInfo> acknowledged =
		    co_await timeoutError(listNativeCdcStreamsClient(cx), operationTimeout);
		const auto acknowledgedStream = std::find_if(
		    acknowledged.begin(), acknowledged.end(), [&](const auto& stream) { return stream.name == name; });
		ASSERT(acknowledgedStream != acknowledged.end());
		ASSERT_EQ(acknowledgedStream->ranges, ranges);
		ASSERT_EQ(acknowledgedStream->minVersion, checkpoint.lastConsumedVersion + 1);
		// The multi-range stream must retain unread history without another stream holding its tag back.
		co_await timeoutError(removeNativeCdcStreamClient(cx, gapName), operationTimeout);

		values.clear();
		expected = Standalone<VectorRef<MutationRef>>();
		for (StringRef suffix : { "b"_sr, "m"_sr, "w"_sr }) {
			values.emplace_back(multipleRangeKey(suffix), "recovered-range"_sr);
			expected.push_back_deep(expected.arena(),
			                        MutationRef(MutationRef::SetValue, values.back().first, values.back().second));
		}
		values.emplace_back(multipleRangeKey("h"_sr), "excluded-gap"_sr);
		const Version retained = co_await writeValues(cx, values);
		co_await timeoutError(forceTransactionSystemRecovery(), operationTimeout);
		consumer = resumeNativeCdcConsumer(cx, checkpoint);
		co_await consumeExpectedVersion(consumer, retained, expected);
		co_await timeoutError(consumer->acknowledge(), operationTimeout);

		// A new commit must use the recovered range routing, including exclusion of the untracked gap.
		const Version resumed = co_await writeValues(cx, values);
		co_await consumeExpectedVersion(consumer, resumed, expected);
		co_await timeoutError(consumer->acknowledge(), operationTimeout);
		co_await timeoutError(removeNativeCdcStreamClient(cx, name), operationTimeout);
		co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
		co_await timeoutError(waitForFullyRecovered(), operationTimeout);
		CODE_PROBE(true, "Native CDC consumes and recovers one stream covering multiple disjoint ranges");
	}

	Future<Void> validateAssignmentPublication(Database cx) {
		for (int check = 0; check < assignmentPublicationChecks; ++check) {
			co_await validateAssignmentPublicationOnce(cx, check);
		}
	}

	Future<Void> waitForIndependentProxyPublications(Database cx, std::vector<CDCProxyInterface> originalProxies) {
		bool sawPartialPublication = false;
		while (true) {
			Future<Void> changed = cx->clientInfo->onChange();
			const ClientDBInfo& clientInfo = cx->clientInfo->get();
			int originalEndpoints = 0;
			for (const auto& proxy : originalProxies) {
				if (std::find(clientInfo.cdcProxies.begin(), clientInfo.cdcProxies.end(), proxy) !=
				    clientInfo.cdcProxies.end()) {
					++originalEndpoints;
				}
			}
			if (originalEndpoints == 1) {
				sawPartialPublication = true;
			}
			if (originalEndpoints == 0) {
				ASSERT(sawPartialPublication);
				co_return;
			}
			co_await changed;
		}
	}

	Future<Void> haltProxyUntilReplaced(Database cx, CDCProxyInterface proxy, bool dropFirstAttempt) {
		while (true) {
			{
				const auto& proxies = cx->clientInfo->get().cdcProxies;
				if (std::find(proxies.begin(), proxies.end(), proxy) == proxies.end()) {
					co_return;
				}
			}

			// The focused scenario can drop one halt before delivery and immediately retry its published proxy.
			const bool droppedAttempt = std::exchange(dropFirstAttempt, false);
			ErrorOr<Void> halted = request_maybe_delivered();
			if (!droppedAttempt) {
				halted = co_await proxy.haltForTesting.tryGetReply(HaltCDCProxyRequest());
			}
			if (halted.present()) {
				co_return;
			}
			CODE_PROBE(true, "Native CDC retries an undelivered proxy halt");
			if (!droppedAttempt) {
				co_await delay(0.1);
			}
		}
	}

	// Remove a stream after its metadata read to verify that its paused initializer cannot revive removed state.
	Future<Void> validateStaleStreamInitialization(Database cx) {
		const Key name = "native-cdc-e2e/stale-initialization"_sr;
		const KeyRange keys(
		    KeyRangeRef("native-cdc-e2e/stale-initialization/"_sr, "native-cdc-e2e/stale-initialization0"_sr));
		const std::vector<KeyRange> ranges{ keys };
		const double deadline = now() + operationTimeout;
		bool recovering = false;
		bool streamRegistered = false;

		while (true) {
			ASSERT_LT(now(), deadline);
			try {
				if (recovering) {
					co_await timeoutError(setAllProxyPopsPaused(cx, false), operationTimeout);
					if (streamRegistered) {
						co_await timeoutError(removeNativeCdcStreamClient(cx, name), operationTimeout);
						streamRegistered = false;
					}
					recovering = false;
				}

				co_await timeoutError(setAllProxyPopsPaused(cx, true), operationTimeout);
				streamRegistered = true;
				const CDCStreamId streamId =
				    co_await timeoutError(registerNativeCdcStreamClient(cx, name, ranges), operationTimeout);
				CDCProxyInterface proxy = co_await timeoutError(waitForAssignedProxy(cx, streamId), operationTimeout);
				const CDCCursor cursor(streamId, invalidVersion);
				Future<ErrorOr<CDCConsumeReply>> pendingConsume = proxy.consume.tryGetReply(CDCConsumeRequest(cursor));
				while (true) {
					const CDCProxyBufferStatus status = co_await getPublishedProxyStatus(cx, proxy);
					if (!status.popsPaused) {
						throw wrong_shard_server();
					}
					if (pendingConsume.isReady()) {
						const ErrorOr<CDCConsumeReply> result = co_await pendingConsume;
						ASSERT(!result.present());
						if (result.getError().code() != error_code_wrong_shard_server) {
							throw result.getError();
						}
						pendingConsume = proxy.consume.tryGetReply(CDCConsumeRequest(cursor));
					} else if (status.activeConsumeRequests > 0) {
						break;
					}
					ASSERT_LT(now(), deadline);
					co_await delay(0.01);
				}

				co_await timeoutError(removeNativeCdcStreamClient(cx, name), operationTimeout);
				streamRegistered = false;
				const ErrorOr<CDCConsumeReply> result = co_await timeoutError(pendingConsume, operationTimeout);
				ASSERT(!result.present());
				if (result.getError().code() != error_code_wrong_shard_server) {
					throw result.getError();
				}
				co_await timeoutError(setAllProxyPopsPaused(cx, false), operationTimeout);
				co_await getPublishedProxyStatus(cx, proxy);
				co_return;
			} catch (Error& e) {
				if (e.code() != error_code_wrong_shard_server && e.code() != error_code_broken_promise &&
				    e.code() != error_code_connection_failed && e.code() != error_code_request_maybe_delivered) {
					throw;
				}
				recovering = true;
			}
			co_await delay(0);
		}
	}

	Future<Void> validateProxyReplacement(Database cx) {
		co_await validateStaleStreamOwnership(cx);
		const Key name = "native-cdc-e2e/proxy-replacement"_sr;
		const KeyRange keys(
		    KeyRangeRef("native-cdc-e2e/proxy-replacement/"_sr, "native-cdc-e2e/proxy-replacement0"_sr));
		const Key key = "native-cdc-e2e/proxy-replacement/value"_sr;
		const Value value = "replacement-value"_sr;

		const std::vector<KeyRange> ranges{ keys };
		const CDCStreamId streamId =
		    co_await timeoutError(registerNativeCdcStreamClient(cx, name, ranges), operationTimeout);
		Reference<NativeCdcConsumer> consumer =
		    co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
		CDCProxyInterface original = co_await timeoutError(waitForAssignedProxy(cx, streamId), operationTimeout);
		const std::vector<CDCProxyInterface> originalProxies = cx->clientInfo->get().cdcProxies;
		ASSERT_EQ(originalProxies.size(), 2);
		ASSERT(std::find(originalProxies.begin(), originalProxies.end(), original) != originalProxies.end());
		Future<Void> publications = waitForIndependentProxyPublications(cx, originalProxies);

		std::vector<Future<Void>> halts;
		halts.reserve(originalProxies.size());
		bool dropFirstHalt = injectUndeliveredProxyHalt;
		for (const auto& proxy : originalProxies) {
			halts.push_back(haltProxyUntilReplaced(cx, proxy, std::exchange(dropFirstHalt, false)));
		}
		co_await timeoutError(waitForAll(halts), operationTimeout);
		co_await timeoutError(publications, operationTimeout);
		CODE_PROBE(true, "Native CDC publishes successful proxy replacements independently");

		CDCProxyInterface replacement =
		    co_await timeoutError(waitForAssignedProxy(cx, streamId, original.id()), operationTimeout);
		ASSERT_NE(original.id(), replacement.id());

		const Version committed = co_await writeValue(cx, key, value);
		co_await consumeThroughValue(consumer, committed, key, value);
		co_await timeoutError(removeNativeCdcStreamClient(cx, name), operationTimeout);
	}

	Future<Void> validateProxyRebalance(Database cx) {
		ASSERT_EQ(streams.size(), 3);
		const CDCStreamId firstId = streams[0].consumer->position().streamId;
		const CDCStreamId otherTagId = streams[1].consumer->position().streamId;
		const CDCStreamId sharedTagId = streams[2].consumer->position().streamId;
		const auto proxies = cx->clientInfo->get().cdcProxies;
		ASSERT_EQ(proxies.size(), 2);
		// Registration commits durable ownership before the controller publishes each assignment.
		for (const auto& stream : streams) {
			co_await timeoutError(waitForAssignedProxy(cx, stream.consumer->position().streamId), operationTimeout);
		}
		const NativeCdcStatus initial = co_await timeoutError(getNativeCdcStatus(cx), operationTimeout);
		ASSERT(initial.metadataComplete);
		ASSERT_EQ(initial.tagCount, 2);
		ASSERT_EQ(initial.streams.size(), 3);
		const auto findStream = [](NativeCdcStatus const& status, CDCStreamId id) -> NativeCdcStreamStatus const& {
			const auto stream = std::find_if(status.streams.begin(), status.streams.end(), [&](auto const& candidate) {
				return candidate.info.streamId == id;
			});
			ASSERT(stream != status.streams.end());
			return *stream;
		};
		const auto& first = findStream(initial, firstId);
		const auto& otherTag = findStream(initial, otherTagId);
		const auto& shared = findStream(initial, sharedTagId);
		ASSERT_EQ(first.tags.size(), 1);
		ASSERT_EQ(otherTag.tags.size(), 1);
		ASSERT_EQ(shared.tags.size(), 1);
		const Tag tag = first.tags.front();
		ASSERT_EQ(shared.tags.front(), tag);
		ASSERT_NE(otherTag.tags.front(), tag);
		co_await checkTagOwner(cx, tag, firstId);
		co_await checkTagOwner(cx, otherTag.tags.front(), otherTagId);

		const CDCProxyInterface source = co_await timeoutError(waitForAssignedProxy(cx, firstId), operationTimeout);
		const CDCProxyInterface target = proxies[proxies.front().id() == source.id() ? 1 : 0];
		ASSERT_NE(source.id(), target.id());
		for (const auto& stream : initial.streams) {
			ASSERT(stream.owner.present());
			ASSERT_EQ(stream.owner.get(), source.id());
			ASSERT(stream.ownerPublished);
		}

		const Key key = keyForIndex(keyCount / 2);
		const Value beforeMove = "native-cdc-rebalance-before"_sr;
		const Version beforeVersion = co_await writeValue(cx, key, beforeMove);
		co_await consumeThroughValue(streams[0].consumer, beforeVersion, key, beforeMove);
		co_await consumeThroughValue(streams[1].consumer, beforeVersion, key, beforeMove);
		const auto containsValue = [](CDCConsumeReply const& reply, Version version, KeyRef key, ValueRef value) {
			return std::any_of(reply.mutations.begin(), reply.mutations.end(), [&](auto const& versioned) {
				return versioned.version == version &&
				       std::any_of(versioned.mutations.begin(), versioned.mutations.end(), [&](auto const& mutation) {
					       return mutation.type == MutationRef::SetValue && mutation.param1 == key &&
					              mutation.param2 == value;
				       });
			});
		};
		bool primed = false;
		const double primeDeadline = now() + operationTimeout;
		while (streams[2].consumer->position().lastConsumedVersion < beforeVersion) {
			CDCConsumeReply reply = co_await timeoutError(streams[2].consumer->consume(), operationTimeout);
			primed |= containsValue(reply, beforeVersion, key, beforeMove);
			ASSERT_LT(now(), primeDeadline);
		}
		ASSERT(primed);
		// Leave this stream unacknowledged so its old tag data must remain readable by the new owner.
		Future<CDCConsumeReply> pending = streams[0].consumer->consume();
		ASSERT(!pending.isReady());

		std::vector<UID> availableProxies{ proxies[0].id(), proxies[1].id() };
		ASSERT(co_await timeoutError(rebalanceNativeCdcProxyAssignments(cx, availableProxies, [] { return true; }),
		                             operationTimeout));
		const CDCProxyInterface moved =
		    co_await timeoutError(waitForAssignedProxy(cx, firstId, source.id()), operationTimeout);
		ASSERT_EQ(moved.id(), target.id());
		const ClientDBInfo& published = cx->clientInfo->get();
		ASSERT_EQ(published.cdcProxies, proxies);
		ASSERT_EQ(published.streamToCDCProxyId.at(firstId), target.id());
		ASSERT_EQ(published.streamToCDCProxyId.at(sharedTagId), target.id());
		ASSERT_EQ(published.streamToCDCProxyId.at(otherTagId), source.id());
		const NativeCdcStatus afterMove = co_await timeoutError(getNativeCdcStatus(cx), operationTimeout);
		ASSERT(afterMove.metadataComplete);
		for (CDCStreamId id : { firstId, sharedTagId }) {
			const auto& stream = findStream(afterMove, id);
			ASSERT(stream.owner.present());
			ASSERT_EQ(stream.owner.get(), target.id());
			ASSERT(stream.ownerPublished);
		}
		const auto& untouched = findStream(afterMove, otherTagId);
		ASSERT(untouched.owner.present());
		ASSERT_EQ(untouched.owner.get(), source.id());
		ASSERT(untouched.ownerPublished);
		co_await checkTagOwner(cx, tag, firstId);
		const auto blockingTag = std::find_if(
		    afterMove.tags.begin(), afterMove.tags.end(), [&](auto const& state) { return state.tag == tag; });
		ASSERT(blockingTag != afterMove.tags.end());
		ASSERT_LE(blockingTag->safePopVersion, beforeVersion);
		ASSERT(std::find(blockingTag->blockingStreams.begin(), blockingTag->blockingStreams.end(), sharedTagId) !=
		       blockingTag->blockingStreams.end());
		ASSERT_EQ(afterMove.proxies.size(), 2);
		for (const auto& proxy : afterMove.proxies) {
			ASSERT(proxy.sample.present());
		}
		ASSERT(!(co_await timeoutError(rebalanceNativeCdcProxyAssignments(cx, availableProxies, [] { return true; }),
		                               operationTimeout)));

		const ErrorOr<Void> staleAck =
		    co_await timeoutError(source.ack.tryGetReply(CDCAckRequest(firstId, beforeVersion)), operationTimeout);
		ASSERT(!staleAck.present());
		ASSERT_EQ(staleAck.getError().code(), error_code_wrong_shard_server);
		bool replayed = false;
		const double replayDeadline = now() + operationTimeout;
		do {
			ASSERT_LT(now(), replayDeadline);
			CDCConsumeReply replay = co_await timeoutError(streams[2].consumer->consume(), replayDeadline - now());
			replayed |= containsValue(replay, beforeVersion, key, beforeMove);
		} while (streams[2].consumer->position().lastConsumedVersion < beforeVersion);
		ASSERT(replayed);
		co_await timeoutError(streams[2].consumer->acknowledge(), operationTimeout);
		const NativeCdcStatus afterAck = co_await timeoutError(getNativeCdcStatus(cx), operationTimeout);
		const auto advancedTag = std::find_if(
		    afterAck.tags.begin(), afterAck.tags.end(), [&](auto const& state) { return state.tag == tag; });
		ASSERT(advancedTag != afterAck.tags.end());
		ASSERT_GT(advancedTag->safePopVersion, beforeVersion);

		const Value afterMoveValue = "native-cdc-rebalance-after"_sr;
		const Version afterVersion = co_await writeValue(cx, key, afterMoveValue);
		bool pendingObserved = false;
		const double deliveryDeadline = now() + operationTimeout;
		while (!pendingObserved) {
			CDCConsumeReply reply = co_await timeoutError(pending, operationTimeout);
			pendingObserved = containsValue(reply, afterVersion, key, afterMoveValue);
			co_await timeoutError(streams[0].consumer->acknowledge(), operationTimeout);
			ASSERT_LT(now(), deliveryDeadline);
			if (!pendingObserved) {
				pending = streams[0].consumer->consume();
			}
		}
		co_await consumeThroughValue(streams[1].consumer, afterVersion, key, afterMoveValue);
		co_await consumeThroughValue(streams[2].consumer, afterVersion, key, afterMoveValue);
		for (const auto& stream : streams) {
			co_await timeoutError(removeNativeCdcStreamClient(cx, stream.name), operationTimeout);
		}
		streams.clear();
		co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
		CODE_PROBE(true, "Native CDC rebalances a shared tag across live proxies without losing delivery");
	}

	Future<Void> validateAutomaticProxyRebalance(Database cx) {
		ASSERT_EQ(streams.size(), 3);
		const CDCStreamId firstId = streams[0].consumer->position().streamId;
		const CDCStreamId otherTagId = streams[1].consumer->position().streamId;
		const CDCStreamId sharedTagId = streams[2].consumer->position().streamId;
		const auto proxies = cx->clientInfo->get().cdcProxies;
		ASSERT_EQ(proxies.size(), 2);
		const NativeCdcStatus initial = co_await timeoutError(getNativeCdcStatus(cx), operationTimeout);
		ASSERT(initial.metadataComplete);
		ASSERT_EQ(initial.tagCount, 2);
		ASSERT_EQ(initial.streams.size(), 3);
		const auto findStream = [](NativeCdcStatus const& status, CDCStreamId id) -> NativeCdcStreamStatus const& {
			const auto stream = std::find_if(status.streams.begin(), status.streams.end(), [&](auto const& candidate) {
				return candidate.info.streamId == id;
			});
			ASSERT(stream != status.streams.end());
			return *stream;
		};
		const auto& first = findStream(initial, firstId);
		const auto& otherTag = findStream(initial, otherTagId);
		const auto& shared = findStream(initial, sharedTagId);
		ASSERT_EQ(first.tags.size(), 1);
		ASSERT_EQ(otherTag.tags.size(), 1);
		ASSERT_EQ(shared.tags.size(), 1);
		ASSERT_EQ(first.tags.front(), shared.tags.front());
		ASSERT_NE(first.tags.front(), otherTag.tags.front());
		for (const auto& stream : initial.streams) {
			ASSERT(stream.owner.present());
			ASSERT(std::any_of(
			    proxies.begin(), proxies.end(), [&](auto const& proxy) { return proxy.id() == stream.owner.get(); }));
		}
		ASSERT_EQ(first.owner.get(), shared.owner.get());

		UID groupOwner;
		UID otherOwner;
		const double deadline = now() + operationTimeout;
		while (true) {
			Future<Void> changed = cx->clientInfo->onChange();
			const ClientDBInfo& published = cx->clientInfo->get();
			ASSERT_EQ(published.cdcProxies, proxies);
			const auto group = published.streamToCDCProxyId.find(firstId);
			const auto sharedGroup = published.streamToCDCProxyId.find(sharedTagId);
			const auto other = published.streamToCDCProxyId.find(otherTagId);
			const auto isLiveProxy = [&](UID id) {
				return std::any_of(proxies.begin(), proxies.end(), [&](auto const& proxy) { return proxy.id() == id; });
			};
			if (group != published.streamToCDCProxyId.end() && sharedGroup != published.streamToCDCProxyId.end() &&
			    other != published.streamToCDCProxyId.end() && group->second == sharedGroup->second &&
			    group->second != other->second && isLiveProxy(group->second) && isLiveProxy(other->second)) {
				groupOwner = group->second;
				otherOwner = other->second;
				break;
			}
			ASSERT_LT(now(), deadline);
			co_await timeoutError(changed, deadline - now());
		}
		if (first.owner.get() == otherTag.owner.get()) {
			ASSERT_NE(groupOwner, first.owner.get());
			ASSERT_EQ(otherOwner, otherTag.owner.get());
		} else {
			ASSERT_EQ(groupOwner, first.owner.get());
			ASSERT_EQ(otherOwner, otherTag.owner.get());
		}
		const NativeCdcStatus afterMove = co_await timeoutError(getNativeCdcStatus(cx), operationTimeout);
		ASSERT(afterMove.metadataComplete);
		ASSERT_EQ(afterMove.streams.size(), 3);
		for (const auto& stream : afterMove.streams) {
			ASSERT(stream.owner.present());
			ASSERT_EQ(stream.owner.get(), stream.info.streamId == otherTagId ? otherOwner : groupOwner);
			ASSERT(stream.ownerPublished);
		}
		co_await checkTagOwner(cx, first.tags.front(), firstId);
		co_await checkTagOwner(cx, otherTag.tags.front(), otherTagId);
		ASSERT_EQ(afterMove.proxies.size(), 2);
		for (const auto& proxy : afterMove.proxies) {
			ASSERT(proxy.sample.present());
		}

		const Key key = keyForIndex(keyCount / 2);
		const Value value = "native-cdc-automatic-rebalance"_sr;
		const Version committed = co_await writeValue(cx, key, value);
		for (const auto& stream : streams) {
			co_await consumeThroughValue(stream.consumer, committed, key, value);
			co_await timeoutError(removeNativeCdcStreamClient(cx, stream.name), operationTimeout);
		}
		streams.clear();
		co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
		CODE_PROBE(true, "Native CDC controller rebalances a whole tag without proxy replacement");
	}

	Future<Void> checkTagOwner(Database cx, Tag tag, Optional<CDCStreamId> expected) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> value = co_await tr.get(cdcTagOwnerKeyFor(tag));
				ASSERT_EQ(value.present(), expected.present());
				if (value.present()) {
					ASSERT_EQ(decodeCDCTagOwnerValue(value.get()), expected.get());
				}
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> overwriteTagOwnerForTesting(Database cx, Tag tag, Optional<CDCStreamId> anchor) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				if (anchor.present()) {
					tr.set(cdcTagOwnerKeyFor(tag), cdcTagOwnerValue(anchor.get()));
				} else {
					tr.clear(cdcTagOwnerKeyFor(tag));
				}
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<CDCStreamId> registerThroughProxy(Database cx,
	                                         Key name,
	                                         KeyRange keys,
	                                         Optional<CDCStreamId> sharedTagStream = Optional<CDCStreamId>()) {
		const std::vector<KeyRange> ranges{ keys };
		const double deadline = now() + operationTimeout;
		CDCStreamId streamId = 0;
		if (sharedTagStream.present()) {
			while (true) {
				if (now() >= deadline) {
					throw timed_out();
				}
				Future<Void> proxyChanged = cx->clientInfo->onChange();
				const ClientDBInfo& clientInfo = cx->clientInfo->get();
				const auto anchor = clientInfo.streamToCDCProxyId.find(sharedTagStream.get());
				if (clientInfo.cdcProxies.size() != 2 || anchor == clientInfo.streamToCDCProxyId.end() ||
				    std::none_of(
				        clientInfo.cdcProxies.begin(),
				        clientInfo.cdcProxies.end(),
				        [&](CDCProxyInterface const& candidate) { return candidate.id() == anchor->second; })) {
					co_await timeoutError(proxyChanged, deadline - now());
					continue;
				}
				CDCProxyInterface proxy =
				    clientInfo.cdcProxies[clientInfo.cdcProxies.front().id() == anchor->second ? 1 : 0];
				ASSERT_NE(proxy.id(), anchor->second);
				try {
					Future<ErrorOr<CDCRegisterStreamReply>> request =
					    proxy.registerStream.tryGetReply(CDCRegisterStreamRequest(name, ranges));
					// A different stream's assignment publication must not discard an in-flight registration.
					while (true) {
						auto result =
						    co_await timeoutError(race(throwErrorOr(request), proxyChanged), deadline - now());
						if (result.index() == 0) {
							streamId = std::get<0>(result).streamId;
							break;
						}
						proxyChanged = cx->clientInfo->onChange();
						const auto& proxies = cx->clientInfo->get().cdcProxies;
						if (std::none_of(proxies.begin(), proxies.end(), [&](CDCProxyInterface const& candidate) {
							    return candidate.id() == proxy.id();
						    })) {
							break;
						}
					}
				} catch (Error& e) {
					if (e.code() != error_code_wrong_shard_server && e.code() != error_code_broken_promise &&
					    e.code() != error_code_connection_failed && e.code() != error_code_request_maybe_delivered) {
						throw;
					}
				}
				if (streamId != 0) {
					break;
				}
				co_await delay(0.1);
			}
		} else {
			streamId = co_await timeoutError(registerNativeCdcStreamClient(cx, name, ranges), deadline - now());
		}
		co_await timeoutError(waitForAssignedProxy(cx, streamId), deadline - now());
		// Recovery can replace the owner while registration or consumption is in flight. Compare live streams
		// in the same published snapshot instead of retaining a proxy ID across those waits.
		const auto& assignments = cx->clientInfo->get().streamToCDCProxyId;
		const auto owner = assignments.find(streamId);
		ASSERT(owner != assignments.end());
		if (sharedTagStream.present()) {
			const auto sharedOwner = assignments.find(sharedTagStream.get());
			ASSERT(sharedOwner != assignments.end());
			ASSERT_EQ(owner->second, sharedOwner->second);
		}
		co_return streamId;
	}

	Future<Void> validateTagOwnership(Database cx) {
		ASSERT(streams.empty());
		ASSERT_EQ(cx->clientInfo->get().nativeCdcTagCount, 1);
		const Tag tag(tagLocalityCDC, 0);
		const Key firstName = "native-cdc-e2e/tag-owner/first"_sr;
		const Key secondName = "native-cdc-e2e/tag-owner/second"_sr;
		const Key thirdName = "native-cdc-e2e/tag-owner/third"_sr;
		const Key key = "native-cdc-e2e/tag-owner/data"_sr;
		const KeyRange keys(KeyRangeRef(key, keyAfter(key)));
		const std::vector<KeyRange> ranges{ keys };
		const CDCStreamId firstId =
		    co_await timeoutError(registerNativeCdcStreamClient(cx, firstName, ranges), operationTimeout);
		CDCProxyInterface owner = co_await timeoutError(waitForAssignedProxy(cx, firstId), operationTimeout);
		ASSERT_EQ(cx->clientInfo->get().cdcProxies.size(), 2);
		co_await checkTagOwner(cx, tag, firstId);

		// The public client usually chooses the first proxy, which cannot distinguish an index hit from caller choice.
		const CDCStreamId removedId = co_await registerThroughProxy(cx, secondName, keys, firstId);
		co_await checkTagOwner(cx, tag, firstId);
		co_await timeoutError(removeNativeCdcStreamClient(cx, secondName), operationTimeout);
		co_await checkTagOwner(cx, tag, firstId);

		for (const Optional<CDCStreamId> invalidAnchor :
		     { Optional<CDCStreamId>(), Optional<CDCStreamId>(removedId) }) {
			co_await overwriteTagOwnerForTesting(cx, tag, invalidAnchor);
			co_await registerThroughProxy(cx, secondName, keys, firstId);
			co_await checkTagOwner(cx, tag, firstId);
			co_await timeoutError(removeNativeCdcStreamClient(cx, secondName), operationTimeout);
		}

		Reference<NativeCdcConsumer> consumer =
		    co_await timeoutError(createNativeCdcConsumer(cx, firstName), operationTimeout);
		const Value value = "tag-owner-retained-across-replacement"_sr;
		const Version committed = co_await writeValue(cx, key, value);
		co_await timeoutError(haltProxyUntilReplaced(cx, owner, false), operationTimeout);
		co_await timeoutError(waitForAssignedProxy(cx, firstId, owner.id()), operationTimeout);
		ASSERT_EQ(cx->clientInfo->get().cdcProxies.size(), 2);
		const CDCStreamId secondId = co_await registerThroughProxy(cx, secondName, keys, firstId);
		co_await checkTagOwner(cx, tag, firstId);
		co_await consumeThroughValue(consumer, committed, key, value);

		co_await timeoutError(removeNativeCdcStreamClient(cx, firstName), operationTimeout);
		co_await checkTagOwner(cx, tag, Optional<CDCStreamId>());
		co_await registerThroughProxy(cx, thirdName, keys, secondId);
		co_await checkTagOwner(cx, tag, secondId);
		co_await timeoutError(removeNativeCdcStreamClient(cx, thirdName), operationTimeout);
		co_await checkTagOwner(cx, tag, secondId);
		co_await timeoutError(removeNativeCdcStreamClient(cx, secondName), operationTimeout);
		co_await checkTagOwner(cx, tag, Optional<CDCStreamId>());

		const CDCStreamId reusedId = co_await registerThroughProxy(cx, firstName, keys);
		co_await checkTagOwner(cx, tag, reusedId);
		co_await timeoutError(removeNativeCdcStreamClient(cx, firstName), operationTimeout);
		co_await checkTagOwner(cx, tag, Optional<CDCStreamId>());
		co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
	}

	Future<bool> setUnpublishedStreamOwner(Database cx,
	                                       Key name,
	                                       CDCStreamId streamId,
	                                       UID expectedOwner,
	                                       UID newOwner,
	                                       bool publishAssignment = false) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				Optional<Value> currentId = co_await tr.get(cdcStreamNameKeyFor(name));
				ASSERT(currentId.present());
				ASSERT_EQ(decodeCDCStreamNameValue(currentId.get()), streamId);

				const KeyRange assignmentRange = cdcProxyRangeFor(streamId);
				RangeResult assignments = co_await tr.getRange(assignmentRange, 2);
				ASSERT_EQ(assignments.size(), 1);
				const auto [assignedStreamId, assignedOwner] = decodeCDCProxyKey(assignments[0].key);
				ASSERT_EQ(assignedStreamId, streamId);
				if (assignedOwner == newOwner && !publishAssignment) {
					co_return true;
				}
				const bool unexpectedOwner = assignedOwner != newOwner && assignedOwner != expectedOwner;
				if (unexpectedOwner && !publishAssignment) {
					co_return false;
				}
				if (!unexpectedOwner && assignedOwner != newOwner) {
					tr.clear(assignmentRange);
					tr.set(cdcProxyKeyFor(streamId, newOwner), Value());
				}
				if (publishAssignment) {
					tr.set(cdcProxyAssignmentChangeKey,
					       BinaryWriter::toValue(deterministicRandom()->randomUniqueID(),
					                             IncludeVersion(ProtocolVersion::withNativeCdc())));
				}
				co_await tr.commit();
				co_return !unexpectedOwner;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	// Stale proxy ownership and old stream IDs must not modify a live replacement stream.
	Future<Void> validateStaleStreamOwnership(Database cx) {
		const Key name = "native-cdc-e2e/stale-ownership"_sr;
		const KeyRange keys(KeyRangeRef("native-cdc-e2e/stale-ownership/"_sr, "native-cdc-e2e/stale-ownership0"_sr));
		const Key key = "native-cdc-e2e/stale-ownership/value"_sr;
		const Value value = "replacement-survives-stale-removal"_sr;
		const double deadline = now() + operationTimeout;
		const std::vector<KeyRange> ranges{ keys };
		CDCStreamId expectedStreamId =
		    co_await timeoutError(registerNativeCdcStreamClient(cx, name, ranges), operationTimeout);

		const auto rejectedWrongOwner = [](ErrorOr<Void> const& result) {
			ASSERT(!result.present());
			const int errorCode = result.getError().code();
			if (errorCode == error_code_wrong_shard_server) {
				return true;
			}
			ASSERT(errorCode == error_code_request_maybe_delivered || errorCode == error_code_connection_failed ||
			       errorCode == error_code_broken_promise);
			return false;
		};

		while (true) {
			ASSERT_LT(now(), deadline);
			Reference<NativeCdcConsumer> existing =
			    co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
			ASSERT_EQ(existing->position().streamId, expectedStreamId);
			const CDCStreamId streamId = expectedStreamId;
			CDCProxyInterface owner = co_await timeoutError(waitForAssignedProxy(cx, streamId), operationTimeout);
			Optional<CDCProxyInterface> wrongOwner;
			for (const auto& proxy : cx->clientInfo->get().cdcProxies) {
				if (proxy.id() != owner.id()) {
					wrongOwner = proxy;
					break;
				}
			}
			if (!wrongOwner.present()) {
				co_await delay(0.01);
				continue;
			}

			const ErrorOr<Void> initialized =
			    co_await timeoutError(owner.ack.tryGetReply(CDCAckRequest(streamId, 0)), operationTimeout);
			if (!initialized.present()) {
				const int errorCode = initialized.getError().code();
				ASSERT(errorCode == error_code_wrong_shard_server || errorCode == error_code_request_maybe_delivered ||
				       errorCode == error_code_connection_failed || errorCode == error_code_broken_promise);
				existing = co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
				ASSERT_EQ(existing->position().streamId, expectedStreamId);
				co_await delay(0.01);
				continue;
			}

			// Keep published ownership and initialized local state unchanged while durable ownership differs.
			const bool swappedOwner = co_await timeoutError(
			    setUnpublishedStreamOwner(cx, name, streamId, owner.id(), wrongOwner.get().id()), operationTimeout);
			if (!swappedOwner) {
				continue;
			}
			ErrorOr<Void> acknowledgement = request_maybe_delivered();
			Optional<Error> acknowledgementError;
			bool retainedPublishedOwner = false;
			try {
				acknowledgement =
				    co_await timeoutError(owner.ack.tryGetReply(CDCAckRequest(streamId, 0)), operationTimeout);
				const auto& publishedOwners = cx->clientInfo->get().streamToCDCProxyId;
				const auto publishedOwner = publishedOwners.find(streamId);
				retainedPublishedOwner =
				    publishedOwner != publishedOwners.end() && publishedOwner->second == owner.id();
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				acknowledgementError = e;
			}
			const bool restoredOwner = co_await timeoutError(
			    setUnpublishedStreamOwner(cx, name, streamId, wrongOwner.get().id(), owner.id(), true),
			    operationTimeout);
			if (acknowledgementError.present()) {
				throw acknowledgementError.get();
			}
			if (!restoredOwner) {
				continue;
			}
			if (!retainedPublishedOwner) {
				co_await timeoutError(waitForAssignedProxy(cx, streamId), operationTimeout);
				continue;
			}
			if (!rejectedWrongOwner(acknowledgement)) {
				existing = co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
				ASSERT_EQ(existing->position().streamId, expectedStreamId);
				continue;
			}
			const ErrorOr<Void> removal = co_await timeoutError(
			    wrongOwner.get().removeStream.tryGetReply(CDCRemoveStreamRequest(name, streamId)), operationTimeout);
			if (!rejectedWrongOwner(removal)) {
				existing = co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
				ASSERT_EQ(existing->position().streamId, expectedStreamId);
				continue;
			}
			existing = co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
			ASSERT_EQ(existing->position().streamId, expectedStreamId);

			co_await timeoutError(removeNativeCdcStreamClient(cx, name), operationTimeout);
			const CDCStreamId replacementId =
			    co_await timeoutError(registerNativeCdcStreamClient(cx, name, ranges), operationTimeout);
			ASSERT_NE(replacementId, streamId);
			expectedStreamId = replacementId;
			const NativeCdcRemoveResult guardedRemoval =
			    co_await timeoutError(removeNativeCdcStreamGuarded(cx, name, streamId), operationTimeout);
			ASSERT(guardedRemoval == NativeCdcRemoveResult::StreamReplaced);
			const ErrorOr<Void> staleRemoval = co_await timeoutError(
			    owner.removeStream.tryGetReply(CDCRemoveStreamRequest(name, streamId)), operationTimeout);
			existing = co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
			ASSERT_EQ(existing->position().streamId, expectedStreamId);
			if (!staleRemoval.present()) {
				const int errorCode = staleRemoval.getError().code();
				ASSERT(errorCode == error_code_request_maybe_delivered || errorCode == error_code_connection_failed ||
				       errorCode == error_code_broken_promise);
				continue;
			}

			Reference<NativeCdcConsumer> consumer =
			    co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
			ASSERT_EQ(consumer->position().streamId, replacementId);
			const Version committed = co_await writeValue(cx, key, value);
			co_await consumeThroughValue(consumer, committed, key, value);
			co_await timeoutError(removeNativeCdcStreamClient(cx, name), operationTimeout);
			co_return;
		}
	}

	Future<Void> consumeMemoryMarker(Reference<NativeCdcConsumer> consumer,
	                                 Version committed,
	                                 Key key,
	                                 Value value,
	                                 Reference<AsyncVar<int>> completed,
	                                 Future<Void> releaseAcknowledgements) {
		bool observed = false;
		while (consumer->position().lastConsumedVersion < committed) {
			CDCConsumeReply reply = co_await timeoutError(consumer->consume(), operationTimeout);
			for (const auto& versioned : reply.mutations) {
				for (const auto& mutation : versioned.mutations) {
					if (versioned.version == committed && mutation.type == MutationRef::SetValue &&
					    mutation.param1 == key && mutation.param2 == value) {
						observed = true;
					}
				}
			}
			if (consumer->position().lastConsumedVersion < committed) {
				co_await timeoutError(consumer->acknowledge(), operationTimeout);
			}
		}
		ASSERT(observed);
		completed->set(completed->get() + 1);
		co_await releaseAcknowledgements;
		co_await timeoutError(consumer->acknowledge(), operationTimeout);
	}

	Future<CDCProxyBufferStatus> getProxyStatus(CDCProxyInterface proxy) {
		co_return co_await timeoutError(proxy.getBufferStatusForTesting.getReply(GetCDCProxyBufferStatusRequest()),
		                                operationTimeout);
	}

	Future<CDCProxyBufferStatus> getPublishedProxyStatus(Database cx, CDCProxyInterface proxy) {
		Future<Void> changed = cx->clientInfo->onChange();
		const auto& proxies = cx->clientInfo->get().cdcProxies;
		if (std::find(proxies.begin(), proxies.end(), proxy) == proxies.end()) {
			throw wrong_shard_server();
		}
		Future<CDCProxyBufferStatus> status = getProxyStatus(proxy);
		while (true) {
			auto result = co_await race(status, changed);
			changed = cx->clientInfo->onChange();
			const auto& currentProxies = cx->clientInfo->get().cdcProxies;
			if (std::find(currentProxies.begin(), currentProxies.end(), proxy) == currentProxies.end()) {
				throw wrong_shard_server();
			}
			if (result.index() == 0) {
				co_return std::get<0>(result);
			}
		}
	}

	Future<std::pair<CDCProxyInterface, CDCProxyBufferStatus>> getAssignedProxyStatus(Database cx,
	                                                                                  CDCStreamId streamId) {
		while (true) {
			Future<Void> changed = cx->clientInfo->onChange();
			Optional<CDCProxyInterface> proxy;
			{
				const ClientDBInfo& clientInfo = cx->clientInfo->get();
				auto assignment = clientInfo.streamToCDCProxyId.find(streamId);
				if (assignment != clientInfo.streamToCDCProxyId.end()) {
					auto found = std::find_if(
					    clientInfo.cdcProxies.begin(),
					    clientInfo.cdcProxies.end(),
					    [&](CDCProxyInterface const& candidate) { return candidate.id() == assignment->second; });
					if (found != clientInfo.cdcProxies.end()) {
						proxy = *found;
					}
				}
			}
			if (!proxy.present()) {
				co_await changed;
				continue;
			}

			try {
				auto result = co_await race(getProxyStatus(proxy.get()), changed);
				if (result.index() == 0) {
					const ClientDBInfo& clientInfo = cx->clientInfo->get();
					auto assignment = clientInfo.streamToCDCProxyId.find(streamId);
					if (assignment != clientInfo.streamToCDCProxyId.end() && assignment->second == proxy.get().id()) {
						co_return std::make_pair(proxy.get(), std::get<0>(result));
					}
				}
			} catch (Error& e) {
				if (e.code() != error_code_broken_promise && e.code() != error_code_connection_failed &&
				    e.code() != error_code_request_maybe_delivered) {
					throw;
				}
			}
			co_await delay(0);
		}
	}

	void updateObservedProxy(CDCProxyInterface& proxy, CDCProxyInterface current) {
		if (proxy.id() != current.id()) {
			CODE_PROBE(true, "Native CDC memory validation follows proxy replacement");
			proxy = current;
		}
	}

	void recordDurableAckProxyReplacement() {
		CODE_PROBE(true, "Native CDC durable acknowledgement validation retries after proxy replacement");
	}

	Future<CDCProxyBufferStatus> getCurrentProxyStatus(Database cx,
	                                                   CDCStreamId streamId,
	                                                   CDCProxyInterface* observedProxy) {
		auto proxyStatus = co_await timeoutError(getAssignedProxyStatus(cx, streamId), operationTimeout);
		updateObservedProxy(*observedProxy, proxyStatus.first);
		co_return proxyStatus.second;
	}

	Future<Void> waitForNoActiveConsumes(Database cx, CDCStreamId streamId, CDCProxyInterface* proxy) {
		const double deadline = now() + operationTimeout;
		while (true) {
			CDCProxyBufferStatus status = co_await getCurrentProxyStatus(cx, streamId, proxy);
			if (status.activeConsumeRequests == 0 && status.readDemand == 0) {
				co_return;
			}
			ASSERT_LT(now(), deadline);
			co_await delay(0.01);
		}
	}

	Future<Void> expectConcurrentConsumeRejected(CDCProxyInterface proxy, CDCCursor cursor) {
		Optional<Error> error;
		try {
			co_await throwErrorOr(proxy.consume.tryGetReply(CDCConsumeRequest(cursor)));
		} catch (Error& e) {
			error = e;
		}
		ASSERT(error.present());
		if (error.get().code() != error_code_client_invalid_operation) {
			throw error.get();
		}
	}

	Future<Void> validateConsumeLeaseAndExclusivity(Database cx, CDCStreamId streamId, CDCProxyInterface* proxy) {
		ASSERT(!streams.empty());
		// Acknowledgements advance one durable frontier for the whole stream. Exercise cancellation and exclusivity on
		// the tracked consumer so later workload phases do not retain a cursor behind acknowledgements made here.
		Reference<NativeCdcConsumer> idleConsumer = streams.front().consumer;
		const Version idleStartVersion = idleConsumer->position().lastConsumedVersion;
		// Check client exclusivity before yielding: committed-version progress can complete a consume before
		// a status request observes read demand, even when the client correctly rejects overlapping operations.
		Future<CDCConsumeReply> idleConsume = idleConsumer->consume();
		ASSERT(!idleConsume.isReady());
		Future<CDCConsumeReply> overlappingConsume = idleConsumer->consume();
		ASSERT(overlappingConsume.isReady() && overlappingConsume.isError());
		ASSERT_EQ(overlappingConsume.getError().code(), error_code_client_invalid_operation);
		Future<Void> overlappingAcknowledgement = idleConsumer->acknowledge();
		ASSERT(overlappingAcknowledgement.isReady() && overlappingAcknowledgement.isError());
		ASSERT_EQ(overlappingAcknowledgement.getError().code(), error_code_client_invalid_operation);

		// Assignment publications for unrelated streams used to abandon the client reply without canceling the
		// corresponding server actor. The active request and read demand must remain bounded at one.
		for (int i = 0; i < 4; ++i) {
			Key name = Key(StringRef(format("native-cdc-e2e/lease/%04d", i)));
			Key key = keyForIndex(keyCount / 2);
			const std::vector<KeyRange> ranges{ KeyRangeRef(key, keyAfter(key)) };
			co_await timeoutError(registerNativeCdcStreamClient(cx, name, ranges), operationTimeout);
			CDCProxyBufferStatus status = co_await getCurrentProxyStatus(cx, streamId, proxy);
			ASSERT_LE(status.activeConsumeRequests, 1);
			ASSERT_LE(status.readDemand, 1);
			co_await timeoutError(removeNativeCdcStreamClient(cx, name), operationTimeout);
			status = co_await getCurrentProxyStatus(cx, streamId, proxy);
			ASSERT_LE(status.activeConsumeRequests, 1);
			ASSERT_LE(status.readDemand, 1);
		}

		// The long poll can complete while unrelated assignment publications are in flight. Preserve any resulting
		// cursor progress before direct proxy requests so a replacement owner can validate it.
		if (idleConsume.isReady()) {
			co_await idleConsume;
			if (idleConsumer->position().lastConsumedVersion > idleStartVersion) {
				co_await timeoutError(idleConsumer->acknowledge(), operationTimeout);
			}
		} else {
			idleConsume.cancel();
		}
		co_await waitForNoActiveConsumes(cx, streamId, proxy);

		CDCCursor currentCursor = idleConsumer->position();
		const double deadline = now() + operationTimeout;
		while (true) {
			ASSERT_LT(now(), deadline);
			try {
				// Send both requests without yielding. The first request marks the stream active before its metadata
				// read, so the second request deterministically exercises server-side exclusivity even while versions
				// advance.
				co_await getCurrentProxyStatus(cx, streamId, proxy);
				Future<ErrorOr<CDCConsumeReply>> first = proxy->consume.tryGetReply(CDCConsumeRequest(currentCursor));
				co_await timeoutError(expectConcurrentConsumeRejected(*proxy, currentCursor), operationTimeout);
				first.cancel();
				co_await waitForNoActiveConsumes(cx, streamId, proxy);

				// The first request may finish before the retry reaches the proxy. A pending request is superseded,
				// while an already-completed request retains its reply; either ordering must allow the same consumer to
				// retry.
				const UID consumerId = deterministicRandom()->randomUniqueID();
				Future<ErrorOr<CDCConsumeReply>> original =
				    proxy->consume.tryGetReply(CDCConsumeRequest(currentCursor, consumerId));
				Future<ErrorOr<CDCConsumeReply>> retry =
				    proxy->consume.tryGetReply(CDCConsumeRequest(currentCursor, consumerId));
				const ErrorOr<CDCConsumeReply> firstReply = co_await timeoutError(original, operationTimeout);
				if (firstReply.isError()) {
					if (firstReply.getError().code() != error_code_request_maybe_delivered) {
						throw firstReply.getError();
					}
				} else {
					ASSERT_GE(firstReply.get().lastConsumedVersion, currentCursor.lastConsumedVersion);
				}
				co_await timeoutError(throwErrorOr(retry), operationTimeout);
				co_await waitForNoActiveConsumes(cx, streamId, proxy);
				co_return;
			} catch (Error& e) {
				if (e.code() != error_code_wrong_shard_server && e.code() != error_code_broken_promise &&
				    e.code() != error_code_connection_failed && e.code() != error_code_request_maybe_delivered) {
					throw;
				}
				// A status reply cannot prevent a proxy replacement or disconnect before the consume replies.
				// Retry both requests so transport failure cannot count as evidence of exclusivity.
				CODE_PROBE(true, "Native CDC server consume validation retries after proxy request failure");
			}
			co_await waitForNoActiveConsumes(cx, streamId, proxy);
		}
	}

	Future<Void> requestPopsUntilStopped(Database cx, Reference<AsyncVar<bool>> stopped) {
		while (!stopped->get()) {
			co_await setAllProxyPopsPaused(cx, false);
			co_await delay(0);
		}
	}

	Future<Void> validatePopProgressUnderContinuousRequests(Database cx,
	                                                        CDCStreamId streamId,
	                                                        CDCProxyInterface* proxy) {
		auto initialProxyStatus = co_await timeoutError(getAssignedProxyStatus(cx, streamId), operationTimeout);
		bool followedProxyReplacement = proxy->id() != initialProxyStatus.first.id();
		updateObservedProxy(*proxy, initialProxyStatus.first);
		CDCProxyBufferStatus initial = initialProxyStatus.second;
		auto stopped = makeReference<AsyncVar<bool>>(false);
		Future<Void> requester = requestPopsUntilStopped(cx, stopped);
		const double deadline = now() + operationTimeout;
		while (true) {
			const UID previousProxy = proxy->id();
			const CDCProxyBufferStatus status = co_await getCurrentProxyStatus(cx, streamId, proxy);
			if (previousProxy != proxy->id()) {
				// Pop counters belong to one proxy instance; require fresh progress after replacement.
				initial = status;
				followedProxyReplacement = true;
			}
			if (status.popCompletions > initial.popCompletions) {
				ASSERT_GT(status.popRequests, initial.popRequests);
				break;
			}
			ASSERT_LT(now(), deadline);
			co_await delay(0.01);
		}
		CODE_PROBE(followedProxyReplacement,
		           "Native CDC pop progress validation follows proxy replacement",
		           probe::decoration::rare);
		stopped->set(true);
		co_await timeoutError(requester, operationTimeout);
	}

	Future<Void> validateProxyMemoryBound(Database cx) {
		ASSERT(!streams.empty());
		const Key key = keyForIndex(keyCount / 2);
		const Value value(std::string(memoryTestValueBytes, 'x'));
		const Version committed = co_await writeValue(cx, key, value);

		const CDCStreamId firstStreamId = streams.front().consumer->position().streamId;
		CDCProxyInterface proxy = co_await timeoutError(waitForAssignedProxy(cx, firstStreamId), operationTimeout);
		for (const auto& stream : streams) {
			const CDCProxyInterface assigned =
			    co_await timeoutError(waitForAssignedProxy(cx, stream.consumer->position().streamId), operationTimeout);
			ASSERT_EQ(assigned.id(), proxy.id());
		}

		Promise<Void> releaseAcknowledgements;
		auto completed = makeReference<AsyncVar<int>>(0);
		std::vector<Future<Void>> consumers;
		consumers.reserve(streams.size());
		for (const auto& stream : streams) {
			consumers.push_back(consumeMemoryMarker(
			    stream.consumer, committed, key, value, completed, releaseAcknowledgements.getFuture()));
		}

		CDCProxyBufferStatus status;
		const double deadline = now() + operationTimeout;
		while (true) {
			auto proxyStatus = co_await timeoutError(getAssignedProxyStatus(cx, firstStreamId), operationTimeout);
			updateObservedProxy(proxy, proxyStatus.first);
			status = proxyStatus.second;
			if (completed->get() > 0 && (completed->get() == static_cast<int>(streams.size()) || status.waiters > 0)) {
				break;
			}
			ASSERT_LT(now(), deadline);
			co_await delay(0.01);
		}
		ASSERT_GT(status.bufferedBytes, 0);
		ASSERT_LE(status.bufferedBytes, status.bufferLimit);
		ASSERT_LE(status.activePermits, status.bufferLimit);
		ASSERT_LE(status.peakActivePermits, status.bufferLimit);
		ASSERT_GE(status.activePermits, status.bufferedBytes);

		releaseAcknowledgements.send(Void());
		co_await timeoutError(waitForAll(consumers), operationTimeout);
		auto proxyStatus = co_await timeoutError(getAssignedProxyStatus(cx, firstStreamId), operationTimeout);
		updateObservedProxy(proxy, proxyStatus.first);
		status = proxyStatus.second;
		ASSERT_EQ(status.bufferedBytes, 0);
		ASSERT_LE(status.activePermits, status.bufferLimit);
		ASSERT_LE(status.peakActivePermits, status.bufferLimit);
		co_await validateConsumeLeaseAndExclusivity(cx, firstStreamId, &proxy);
		co_await validatePopProgressUnderContinuousRequests(cx, firstStreamId, &proxy);
	}

	Future<Void> validateReplyChunking(Database cx) {
		ASSERT_EQ(streams.size(), 1);
		ASSERT_GE(keyCount, 2);
		auto& stream = streams.front();
		struct ExpectedVersion {
			Version version;
			std::vector<std::pair<Key, Value>> values;
		};
		std::vector<ExpectedVersion> expected;
		for (int i = 0; i < 8; ++i) {
			std::vector<std::pair<Key, Value>> values;
			values.emplace_back(keyForIndex(0), Value(std::string(memoryTestValueBytes, static_cast<char>('a' + i))));
			values.emplace_back(keyForIndex(1), Value(std::string(memoryTestValueBytes, static_cast<char>('A' + i))));
			const Version committed = co_await writeValues(cx, values);
			if (!expected.empty()) {
				ASSERT_GT(committed, expected.back().version);
			}
			expected.push_back(ExpectedVersion{ committed, std::move(values) });
		}
		const Version lastVersion = expected.back().version;

		// Prime one proxy-owned retained buffer without acknowledging. Several complete versions fit one TLog peek,
		// but all versions exceed its bound; a batched reply proves one pass buffered multiple complete versions.
		bool sawBatchedPrimingReply = false;
		const double primeDeadline = now() + operationTimeout;
		while (stream.consumer->position().lastConsumedVersion < lastVersion) {
			const Version previous = stream.consumer->position().lastConsumedVersion;
			CDCConsumeReply reply = co_await timeoutError(stream.consumer->consume(), operationTimeout);
			// The priming consumer intentionally does not acknowledge. If its delivery proxy is replaced, the
			// consumer rewinds to its durable cursor and replays retained versions through the replacement proxy.
			if (reply.lastConsumedVersion < previous) {
				ASSERT_LT(now(), primeDeadline);
				continue;
			}
			if (reply.lastConsumedVersion == previous) {
				ASSERT_LT(now(), primeDeadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT_GT(reply.lastConsumedVersion, previous);
			sawBatchedPrimingReply |= reply.mutations.size() > 1;
		}
		ASSERT(sawBatchedPrimingReply);

		Reference<NativeCdcConsumer> resumed =
		    resumeNativeCdcConsumer(cx, CDCCursor(stream.consumer->position().streamId, invalidVersion));
		std::set<Version> observedVersions;
		int replyCount = 0;
		bool checkedFirstReply = false;
		const double resumeDeadline = now() + operationTimeout;
		while (resumed->position().lastConsumedVersion < lastVersion) {
			const Version previous = resumed->position().lastConsumedVersion;
			CDCConsumeReply reply = co_await timeoutError(resumed->consume(), operationTimeout);
			if (reply.lastConsumedVersion == previous) {
				ASSERT_LT(now(), resumeDeadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT_GT(reply.lastConsumedVersion, previous);
			if (!checkedFirstReply) {
				ASSERT_LT(reply.lastConsumedVersion, lastVersion);
				checkedFirstReply = true;
			}
			for (const auto& versioned : reply.mutations) {
				ASSERT_GT(versioned.version, previous);
				ASSERT_LE(versioned.version, reply.lastConsumedVersion);
				auto expectedVersion = std::find_if(expected.begin(), expected.end(), [&](const auto& item) {
					return item.version == versioned.version;
				});
				ASSERT(expectedVersion != expected.end());
				ASSERT(observedVersions.insert(versioned.version).second);
				ASSERT_EQ(versioned.mutations.size(), expectedVersion->values.size());
				for (const auto& [key, value] : expectedVersion->values) {
					const bool found = std::any_of(
					    versioned.mutations.begin(), versioned.mutations.end(), [&](const MutationRef& mutation) {
						    return mutation.type == MutationRef::SetValue && mutation.param1 == key &&
						           mutation.param2 == value;
					    });
					ASSERT(found);
				}
			}
			for (const auto& item : expected) {
				if (item.version <= reply.lastConsumedVersion) {
					ASSERT(observedVersions.contains(item.version));
				}
			}
			co_await timeoutError(resumed->acknowledge(), operationTimeout);
			++replyCount;
		}
		ASSERT(checkedFirstReply);
		ASSERT_GT(replyCount, 1);
		ASSERT_EQ(observedVersions.size(), expected.size());

		co_await timeoutError(removeNativeCdcStreamClient(cx, stream.name), operationTimeout);
		streams.clear();
		co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
	}

	Future<Void> validateOversizedPeek(Database cx) {
		ASSERT_EQ(streams.size(), 2);
		// Both mutations share one CDC tag and commit version, so the raw TLog reply exceeds its cap. Each mutation
		// matches only one stream and fits that stream's filtered-batch limit, keeping the rejection path unambiguous.
		std::vector<std::pair<Key, Value>> values;
		values.emplace_back(keyForIndex(1), Value(std::string(memoryTestValueBytes, 'x')));
		values.emplace_back(keyForIndex(3), Value(std::string(memoryTestValueBytes, 'y')));
		ASSERT(streams[0].keys.contains(values[0].first));
		ASSERT(!streams[0].keys.contains(values[1].first));
		ASSERT(!streams[1].keys.contains(values[0].first));
		ASSERT(streams[1].keys.contains(values[1].first));
		const Version committed = co_await writeValues(cx, values);

		// Register a third stream after the oversized version. It shares the tag but starts at a later frontier and
		// must not inherit the failure of consumers that still require the oversized reply.
		co_await addStream(cx, KeyRange(KeyRangeRef(keyForIndex(4), keyForIndex(6))));
		const Key laterKey = keyForIndex(5);
		const Value laterValue = "after-oversized-reply"_sr;
		const Version laterCommitted = co_await writeValue(cx, laterKey, laterValue);
		ASSERT_EQ(streams.size(), 3);

		int rejected = 0;
		for (int i = 0; i < 2; ++i) {
			auto& stream = streams[i];
			const double deadline = now() + operationTimeout;
			while (stream.consumer->position().lastConsumedVersion < committed) {
				try {
					co_await timeoutError(stream.consumer->consume(), operationTimeout);
					co_await timeoutError(stream.consumer->acknowledge(), operationTimeout);
				} catch (Error& e) {
					if (e.code() != error_code_server_overloaded) {
						throw;
					}
					++rejected;
					break;
				}
				ASSERT_LT(now(), deadline);
			}
		}
		ASSERT_EQ(rejected, 2);
		co_await consumeThroughValue(streams.back().consumer, laterCommitted, laterKey, laterValue);
		CODE_PROBE(true, "Native CDC raw peek failure is scoped to the blocked stream frontier");
		CODE_PROBE(true, "Native CDC rejects a TLog response larger than its raw peek reservation");
		for (const auto& stream : streams) {
			co_await timeoutError(removeNativeCdcStreamClient(cx, stream.name), operationTimeout);
		}
		streams.clear();
		co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
	}

	Future<Void> validateDurableAcknowledgementScan(Database cx) {
		ASSERT_EQ(streams.size(), 1);
		const Key key = keyForIndex(keyCount / 2);
		const CDCStreamId streamId = streams.front().consumer->position().streamId;
		while (true) {
			CDCProxyInterface reconcileSetupProxy =
			    co_await timeoutError(waitForAssignedProxy(cx, streamId), operationTimeout);
			const Value reconcileValue = "durable-ack-consume-reconcile"_sr;
			const Version reconcileCommitted = co_await writeValue(cx, key, reconcileValue);
			while (streams.front().consumer->position().lastConsumedVersion < reconcileCommitted) {
				co_await timeoutError(streams.front().consumer->consume(), operationTimeout);
				if (streams.front().consumer->position().lastConsumedVersion < reconcileCommitted) {
					co_await timeoutError(streams.front().consumer->acknowledge(), operationTimeout);
				}
			}

			co_await delay(1.0);
			auto initialProxyStatus = co_await timeoutError(getAssignedProxyStatus(cx, streamId), operationTimeout);
			if (initialProxyStatus.first.id() != reconcileSetupProxy.id()) {
				recordDurableAckProxyReplacement();
				continue;
			}
			const CDCProxyBufferStatus initial = initialProxyStatus.second;
			ASSERT_GT(initial.bufferedBytes, 0);

			// A consume can observe the durable frontier before the explicit acknowledgement RPC arrives. Hold TLog
			// pops so this path must release acknowledged batches and permits itself instead of relying on the periodic
			// pop scan.
			co_await setAllProxyPopsPaused(cx, true);
			co_await timeoutError(acknowledgeDurablyWithoutProxy(cx, streamId, reconcileCommitted), operationTimeout);
			resumeConsumerAfterDirectAcknowledgement(cx, streamId, reconcileCommitted);
			CDCProxyInterface observedProxy = initialProxyStatus.first;
			Future<ErrorOr<CDCConsumeReply>> reconcileRequest =
			    observedProxy.consume.tryGetReply(CDCConsumeRequest(streams.front().consumer->position()));
			const double reconcileDeadline = now() + operationTimeout;
			bool proxyReplaced = false;
			while (true) {
				auto currentProxyStatus = co_await timeoutError(getAssignedProxyStatus(cx, streamId), operationTimeout);
				if (currentProxyStatus.first.id() != observedProxy.id()) {
					proxyReplaced = true;
					break;
				}
				if (currentProxyStatus.second.bufferedBytes < initial.bufferedBytes) {
					break;
				}
				ASSERT_LT(now(), reconcileDeadline);
				co_await delay(0.01);
			}
			if (proxyReplaced) {
				co_await setAllProxyPopsPaused(cx, false);
				recordDurableAckProxyReplacement();
				continue;
			}

			const Value scanValue = "durable-ack-periodic-scan"_sr;
			const Version scanCommitted = co_await writeValue(cx, key, scanValue);
			bool reconcileRequestFailed = false;
			try {
				co_await timeoutError(throwErrorOr(reconcileRequest), operationTimeout);
			} catch (Error& e) {
				if (e.code() != error_code_wrong_shard_server && e.code() != error_code_broken_promise &&
				    e.code() != error_code_connection_failed && e.code() != error_code_request_maybe_delivered) {
					throw;
				}
				reconcileRequestFailed = true;
			}
			co_await setAllProxyPopsPaused(cx, false);
			if (reconcileRequestFailed) {
				recordDurableAckProxyReplacement();
				continue;
			}

			CDCProxyInterface scanConsumeProxy =
			    co_await timeoutError(waitForAssignedProxy(cx, streamId), operationTimeout);
			while (streams.front().consumer->position().lastConsumedVersion < scanCommitted) {
				co_await timeoutError(streams.front().consumer->consume(), operationTimeout);
			}
			co_await delay(1.0);
			initialProxyStatus = co_await timeoutError(getAssignedProxyStatus(cx, streamId), operationTimeout);
			if (initialProxyStatus.first.id() != scanConsumeProxy.id()) {
				recordDurableAckProxyReplacement();
				continue;
			}
			const CDCProxyBufferStatus scanInitial = initialProxyStatus.second;
			ASSERT_GT(scanInitial.bufferedBytes, 0);
			co_await timeoutError(acknowledgeDurablyWithoutProxy(cx, streamId, scanCommitted), operationTimeout);
			resumeConsumerAfterDirectAcknowledgement(cx, streamId, scanCommitted);

			const double deadline = now() + operationTimeout;
			bool unrelatedPopRequest = false;
			while (true) {
				auto currentProxyStatus = co_await timeoutError(getAssignedProxyStatus(cx, streamId), operationTimeout);
				if (currentProxyStatus.first.id() != initialProxyStatus.first.id()) {
					proxyReplaced = true;
					break;
				}
				const CDCProxyBufferStatus& status = currentProxyStatus.second;
				if (status.popRequests != scanInitial.popRequests) {
					unrelatedPopRequest = true;
					CODE_PROBE(true,
					           "Native CDC durable acknowledgement scan retries after an unrelated proxy pop wake");
					break;
				}
				if (status.bufferedBytes < scanInitial.bufferedBytes &&
				    status.popCompletions > scanInitial.popCompletions) {
					break;
				}
				ASSERT_LT(now(), deadline);
				co_await delay(0.01);
			}
			if (proxyReplaced || unrelatedPopRequest) {
				if (proxyReplaced) {
					recordDurableAckProxyReplacement();
				}
				continue;
			}
			CODE_PROBE(true, "Native CDC durable acknowledgement progresses without a proxy notification");
			co_await timeoutError(removeNativeCdcStreamClient(cx, streams.front().name), operationTimeout);
			streams.clear();
			co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
			co_return;
		}
	}

	Future<Void> waitForRetiredTagState(Database cx, bool present) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				RangeResult markers = co_await tr.getRange(cdcRetiredTagPopKeys, 1);
				RangeResult versions = co_await tr.getRange(cdcRetiredTagPopVersionKeys, 1);
				if ((present && !markers.empty() && !versions.empty()) ||
				    (!present && markers.empty() && versions.empty())) {
					co_return;
				}
				tr.reset();
				co_await delay(0.1);
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> waitForRetiredTagCleanup(Database cx) { return waitForRetiredTagState(cx, false); }

	Future<Void> setAllProxyPopsPaused(Database cx, bool paused, bool afterSnapshot = false) {
		while (true) {
			Future<Void> changed = cx->clientInfo->onChange();
			const std::vector<CDCProxyInterface> proxies = cx->clientInfo->get().cdcProxies;
			if (proxies.empty()) {
				co_await changed;
				continue;
			}

			std::vector<Future<Void>> requests;
			requests.reserve(proxies.size());
			for (const auto& proxy : proxies) {
				requests.push_back(
				    proxy.setPopsPausedForTesting.getReply(SetCDCProxyPopsPausedRequest(paused, afterSnapshot)));
			}
			auto result = co_await race(waitForAll(requests), changed);
			if (result.index() == 0 && proxies == cx->clientInfo->get().cdcProxies) {
				co_return;
			}
			CODE_PROBE(true, "Native CDC workload retries pop control after proxy replacement");
		}
	}

	Future<CDCProxyBufferStatus> getSingleProxyStatus(Database cx) {
		while (true) {
			const std::vector<CDCProxyInterface> proxies = cx->clientInfo->get().cdcProxies;
			if (proxies.empty()) {
				co_await cx->clientInfo->onChange();
				continue;
			}
			ASSERT_EQ(proxies.size(), 1);
			co_return co_await timeoutError(
			    proxies.front().getBufferStatusForTesting.getReply(GetCDCProxyBufferStatusRequest()), operationTimeout);
		}
	}

	Future<Void> waitForPopSnapshotPause(Database cx, int64_t previousPauses) {
		const double deadline = now() + operationTimeout;
		while (true) {
			const CDCProxyBufferStatus status = co_await getSingleProxyStatus(cx);
			ASSERT(status.popsPausedAfterSnapshot);
			if (status.popSnapshotsPaused > previousPauses) {
				co_return;
			}
			ASSERT_LT(now(), deadline);
			co_await delay(0.01);
		}
	}

	Future<Void> validateRetiredSharedTagSnapshot(Database cx) {
		ASSERT(streams.empty());
		ASSERT_EQ(cx->clientInfo->get().nativeCdcTagCount, 1);
		const CDCProxyBufferStatus initialStatus = co_await getSingleProxyStatus(cx);
		co_await setAllProxyPopsPaused(cx, true, true);
		co_await waitForPopSnapshotPause(cx, initialStatus.popSnapshotsPaused);

		const Key key = keyForIndex(keyCount / 2);
		const KeyRange keys(KeyRangeRef(key, keyAfter(key)));
		co_await addStream(cx, keys);
		const Value value = "retired-shared-tag-snapshot"_sr;
		const Version committed = co_await writeValue(cx, key, value);

		// This second stream shares the only configured tag. Removing it advances the retired watermark after the
		// paused snapshot, while the first stream still needs the mutation above.
		co_await addStream(cx, keys);
		co_await timeoutError(removeNativeCdcStreamClient(cx, streams.back().name), operationTimeout);
		streams.pop_back();

		NativeCdcStatus blockedStatus;
		// A proxy sample can fail during recovery even though the durable status read succeeded.
		const double sampleDeadline = now() + operationTimeout;
		while (true) {
			ASSERT_LT(now(), sampleDeadline);
			blockedStatus = co_await timeoutError(getNativeCdcStatus(cx), sampleDeadline - now());
			ASSERT(blockedStatus.metadataComplete);
			ASSERT_EQ(blockedStatus.streams.size(), 1);
			const NativeCdcStreamStatus& blocker = blockedStatus.streams.front();
			ASSERT_EQ(blocker.info.streamId, streams.front().consumer->position().streamId);
			ASSERT_EQ(blocker.info.name, streams.front().name);
			ASSERT_EQ(blocker.info.ranges.size(), 1);
			ASSERT_EQ(blocker.info.ranges.front(), keys);
			ASSERT_GT(blocker.info.minVersion, invalidVersion);
			ASSERT_LE(blocker.info.minVersion, committed);
			ASSERT_EQ(blocker.tags.size(), 1);
			ASSERT_EQ(blockedStatus.tags.size(), 1);
			const NativeCdcTagStatus& tag = blockedStatus.tags.front();
			ASSERT_EQ(tag.tag, blocker.tags.front());
			ASSERT(tag.pendingRetiredPop);
			ASSERT_EQ(tag.blockingStreams.size(), 1);
			ASSERT_EQ(tag.blockingStreams.front(), blocker.info.streamId);
			ASSERT_GT(tag.safePopVersion, invalidVersion);
			ASSERT_LE(tag.safePopVersion, committed);
			ASSERT(blocker.owner.present());
			const auto proxySample =
			    std::find_if(blockedStatus.proxies.begin(),
			                 blockedStatus.proxies.end(),
			                 [&](NativeCdcProxyStatus const& proxy) { return proxy.id == blocker.owner.get(); });
			if (proxySample == blockedStatus.proxies.end()) {
				// The durable owner may not yet be in the published proxy list after replacement.
				co_await delay(0.1);
				continue;
			}
			if (!proxySample->sample.present()) {
				ASSERT(proxySample->error.present());
				const int code = proxySample->error.get().code();
				if (code != error_code_timed_out && code != error_code_broken_promise &&
				    code != error_code_connection_failed && code != error_code_request_maybe_delivered &&
				    code != error_code_wrong_shard_server) {
					throw proxySample->error.get();
				}
				co_await delay(0.1);
				continue;
			}
			ASSERT_GT(proxySample->sample.get().bufferLimit, 0);
			const auto& sampledStreams = proxySample->sample.get().streams;
			ASSERT(std::any_of(sampledStreams.begin(), sampledStreams.end(), [&](CDCProxyStreamStatus const& stream) {
				return stream.streamId == blocker.info.streamId;
			}));
			break;
		}
		const NativeCdcStreamStatus& blocker = blockedStatus.streams.front();

		co_await setAllProxyPopsPaused(cx, false);
		co_await consumeThroughValue(streams.front().consumer, committed, key, value);
		const CDCProxyInterface proxy =
		    co_await timeoutError(waitForAssignedProxy(cx, blocker.info.streamId), operationTimeout);
		GetCDCProxyStatusRequest request;
		request.streamIds = { blocker.info.streamId, 0 };
		const CDCProxyStatusReply reply = co_await timeoutError(proxy.getStatus.getReply(request), operationTimeout);
		ASSERT_EQ(reply.streams.size(), 2);
		ASSERT(std::any_of(reply.streams.begin(), reply.streams.end(), [&](CDCProxyStreamStatus const& stream) {
			return stream.streamId == blocker.info.streamId && stream.present;
		}));
		ASSERT(std::any_of(reply.streams.begin(), reply.streams.end(), [](CDCProxyStreamStatus const& stream) {
			return stream.streamId == 0 && !stream.present;
		}));
		GetCDCProxyStatusRequest oversizedRequest;
		oversizedRequest.streamIds.resize(GetCDCProxyStatusRequest::MAX_STREAMS + 1, 0);
		const ErrorOr<CDCProxyStatusReply> oversizedReply =
		    co_await timeoutError(proxy.getStatus.tryGetReply(oversizedRequest), operationTimeout);
		ASSERT(!oversizedReply.present());
		ASSERT_EQ(oversizedReply.getError().code(), error_code_client_invalid_operation);
		co_await timeoutError(removeNativeCdcStreamClient(cx, streams.front().name), operationTimeout);
		streams.clear();
		co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
		CODE_PROBE(true, "Native CDC retired pop snapshot preserves a newly shared live stream");
	}

	Future<Void> waitForTransactionSystemRecoveryAfter(uint64_t recoveryCount) {
		while (dbInfo->get().recoveryCount <= recoveryCount ||
		       dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			co_await dbInfo->onChange();
		}
	}

	Future<Void> waitForTransactionSystemAvailable() {
		while (dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			co_await dbInfo->onChange();
		}
	}

	Future<Void> waitForFullyRecovered() {
		while (dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
			co_await dbInfo->onChange();
		}
	}

	Future<Void> forceTransactionSystemRecovery() {
		ASSERT(g_network->isSimulated());
		const uint64_t recoveryCount = dbInfo->get().recoveryCount;
		while (true) {
			const auto masterMachine = dbInfo->get().master.locality.machineId();
			if (g_simulator->killMachine(masterMachine, ISimulator::KillType::Reboot, true)) {
				break;
			}
			co_await (dbInfo->onChange() || delay(1.0));
		}
		co_await timeoutError(waitForTransactionSystemRecoveryAfter(recoveryCount), operationTimeout);
	}

	Future<Void> validateRetiredCleanupAcrossRecovery(Database cx) {
		ASSERT_EQ(streams.size(), 1);
		co_await timeoutError(waitForTransactionSystemAvailable(), operationTimeout);
		if (blockRetiredPopWithLiveStream) {
			ASSERT_EQ(cx->clientInfo->get().nativeCdcTagCount, 1);
			// Another region's proxy can finish a retired pop while the locally published proxy is paused.
			// Keep the original stream on the shared tag until after recovery so that pop remains pending.
			const Key key = keyForIndex(keyCount / 2);
			co_await addStream(cx, KeyRange(KeyRangeRef(key, keyAfter(key))));
		}
		const Key name = streams.back().name;
		const CDCStreamId streamId = streams.back().consumer->position().streamId;
		co_await setAllProxyPopsPaused(cx, true);
		const NativeCdcRemoveResult removed =
		    co_await timeoutError(removeNativeCdcStreamGuarded(cx, name, streamId), operationTimeout);
		ASSERT(removed == NativeCdcRemoveResult::Removed);
		streams.pop_back();
		co_await timeoutError(waitForRetiredTagState(cx, true), operationTimeout);
		const NativeCdcRemoveResult repeated =
		    co_await timeoutError(removeNativeCdcStreamGuarded(cx, name, streamId), operationTimeout);
		ASSERT(repeated == NativeCdcRemoveResult::AlreadyAbsent);
		const NativeCdcStatus pendingStatus = co_await timeoutError(getNativeCdcStatus(cx), operationTimeout);
		ASSERT(pendingStatus.metadataComplete);
		ASSERT_EQ(pendingStatus.streams.size(), streams.size());
		if (blockRetiredPopWithLiveStream) {
			ASSERT_EQ(pendingStatus.streams.front().info.streamId, streams.front().consumer->position().streamId);
			ASSERT_EQ(pendingStatus.tags.size(), 1);
			const NativeCdcTagStatus& tag = pendingStatus.tags.front();
			ASSERT_EQ(tag.blockingStreams.size(), 1);
			ASSERT_EQ(tag.blockingStreams.front(), streams.front().consumer->position().streamId);
			ASSERT_LT(tag.safePopVersion, tag.retiredPopVersion);
		}
		ASSERT(std::any_of(pendingStatus.tags.begin(), pendingStatus.tags.end(), [](NativeCdcTagStatus const& tag) {
			return tag.pendingRetiredPop;
		}));
		TraceEvent("NativeCdcRetiredMarkerCreated").log();

		co_await forceTransactionSystemRecovery();
		TraceEvent("NativeCdcRetiredRecoveryComplete").log();
		if (blockRetiredPopWithLiveStream) {
			const NativeCdcStatus recoveredStatus = co_await timeoutError(getNativeCdcStatus(cx), operationTimeout);
			ASSERT(recoveredStatus.metadataComplete);
			ASSERT_EQ(recoveredStatus.streams.size(), 1);
			ASSERT(std::any_of(recoveredStatus.tags.begin(),
			                   recoveredStatus.tags.end(),
			                   [](NativeCdcTagStatus const& tag) { return tag.pendingRetiredPop; }));
			co_await timeoutError(removeNativeCdcStreamClient(cx, streams.front().name), operationTimeout);
			streams.clear();
		}
		co_await setAllProxyPopsPaused(cx, false);
		co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
		TraceEvent("NativeCdcRetiredCleanupComplete").log();
		co_await timeoutError(waitForFullyRecovered(), operationTimeout);
		const NativeCdcStatus drainedStatus = co_await timeoutError(getNativeCdcStatus(cx), operationTimeout);
		ASSERT(drainedStatus.metadataComplete);
		ASSERT(drainedStatus.streams.empty());
		ASSERT(std::none_of(drainedStatus.tags.begin(), drainedStatus.tags.end(), [](NativeCdcTagStatus const& tag) {
			return tag.pendingRetiredPop;
		}));
		CODE_PROBE(true, "Native CDC retired tag cleanup allows recovery to complete");
	}

	Future<Void> prepareRetaggedRestartState(Database cx, Version before) {
		ASSERT_EQ(cx->clientInfo->get().nativeCdcTagCount, 2);
		const RetagSnapshot original = co_await readRetagSnapshot(cx, 0, 1);
		const Tag destination(tagLocalityCDC, original.state.assignment.tag.id == 0 ? 1 : 0);
		const RetagSnapshot committed = co_await commitRetagFixture(cx, 0, original, destination, 1, {});
		const Version cutover = committed.state.assignment.version;
		const Version after = co_await writeValue(cx, keyForIndex(keyCount / 2), "native-cdc-restart-after-retag"_sr);
		ASSERT_LT(before, cutover);
		ASSERT_GT(after, cutover);

		// Keep exact marker versions outside the tracked range so the restarted reader can verify both log intervals.
		BinaryWriter fixture{ Unversioned() };
		fixture << original.state.streamId << before << cutover << after << original.state.assignment.tag
		        << destination;
		co_await writeValue(cx, "native-cdc-e2e/restart-retag-state"_sr, fixture.toValue());
		const RetagSnapshot pending = co_await readRetagSnapshot(cx, 0);
		ASSERT(pending.state.pending);
		ASSERT_EQ(pending.history.size(), 2);
		ASSERT_EQ(pending.state.assignment.version, cutover);
		ASSERT_LT(pending.state.minVersion, cutover);
		CODE_PROBE(true, "Native CDC restart preserves an unacknowledged retag boundary");
	}

	Future<RetagRestartMarkers> loadRetaggedRestartState(Database cx, Key name, Reference<NativeCdcConsumer> consumer) {
		const std::vector<NativeCdcStreamInfo> listed =
		    co_await timeoutError(listNativeCdcStreamsClient(cx), operationTimeout);
		ASSERT_EQ(listed.size(), 1);
		ASSERT_EQ(listed.front().name, name);
		ASSERT_EQ(listed.front().streamId, consumer->position().streamId);
		StreamState stream;
		stream.name = name;
		ASSERT_EQ(listed.front().ranges.size(), 1);
		stream.keys = listed.front().ranges.front();
		stream.consumer = consumer;
		streams.push_back(std::move(stream));

		RetagRestartMarkers markers;
		// NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines) Database::run owns the closure.
		co_await cx.run([this, &markers, &consumer, &cx](Transaction* tr) -> Future<Void> {
			const Optional<Value> fixture = co_await tr->get("native-cdc-e2e/restart-retag-state"_sr);
			ASSERT(fixture.present());
			BinaryReader reader(fixture.get(), Unversioned());
			reader >> markers.streamId >> markers.before >> markers.cutover >> markers.after >> markers.oldTag >>
			    markers.newTag;
			ASSERT_EQ(markers.streamId, consumer->position().streamId);
			const RetagSnapshot pending = co_await readRetagSnapshot(cx, 0);
			ASSERT(pending.state.pending);
			ASSERT_EQ(pending.history.size(), 2);
			ASSERT_EQ(pending.history.front().tag, markers.oldTag);
			ASSERT_EQ(pending.state.assignment.tag, markers.newTag);
			ASSERT_EQ(pending.state.assignment.version, markers.cutover);
			ASSERT_LT(pending.state.minVersion, markers.cutover);
		});
		co_return markers;
	}

	Future<Void> finishRetaggedRestartState(Database cx, RetagRestartMarkers markers) {
		const RetagSnapshot acknowledged = co_await readRetagSnapshot(cx, 0);
		co_await waitForCanonicalRetag(cx, 0, acknowledged.state.assignment);
		// NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines) Database::run owns the closure.
		co_await cx.run([this, markers](Transaction* tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			const RetagSnapshot completed = co_await readRetagSnapshot(tr, 0, 1);
			ASSERT(!completed.state.pending);
			ASSERT_EQ(completed.state.assignment.version, markers.cutover);
			const bool prepared = co_await retagNativeCdcStream(tr, completed.state, markers.oldTag);
			ASSERT(!prepared);
		});
		CODE_PROBE(true, "Native CDC disabled admission finishes pending retags without admitting new moves");
	}

	Future<Void> prepareRestartDrainState(Database cx) {
		ASSERT_EQ(streams.size(), 1);
		const Version before = co_await writeValue(cx, keyForIndex(keyCount / 2), "native-cdc-restart-drain"_sr);
		if (testRetaggedRestart) {
			co_await timeoutError(prepareRetaggedRestartState(cx, before), operationTimeout);
		}
		CODE_PROBE(true, "Native CDC restart marker is durable before save and kill");
	}

	Future<Void> prepareRestartDrainSetup(Database cx) {
		co_await initializeStreams(cx);
		co_await prepareRestartDrainState(cx);
	}

	Future<Void> drainRestartState(Database cx) {
		while (cx->clientInfo->get().nativeCdcEnabled) {
			co_await cx->clientInfo->onChange();
		}
		const Key name = "native-cdc-e2e/stream/0000"_sr;
		Reference<NativeCdcConsumer> consumer =
		    co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
		const NativeCdcStatus activeStatus = co_await timeoutError(getNativeCdcStatus(cx), operationTimeout);
		ASSERT(activeStatus.metadataComplete);
		ASSERT(!activeStatus.admissionEnabled);
		ASSERT_EQ(activeStatus.streams.size(), 1);
		ASSERT_EQ(activeStatus.streams.front().info.name, name);
		ASSERT_EQ(activeStatus.streams.front().info.streamId, consumer->position().streamId);
		Optional<RetagRestartMarkers> retagMarkers;
		if (testRetaggedRestart) {
			retagMarkers = co_await timeoutError(loadRetaggedRestartState(cx, name, consumer), operationTimeout);
		}
		bool observed = false;
		bool observedAfterRetag = !testRetaggedRestart;
		const double retagDeadline = now() + operationTimeout;
		while (!observed || !observedAfterRetag) {
			if (testRetaggedRestart) {
				ASSERT_LT(now(), retagDeadline);
			}
			CDCConsumeReply reply = co_await timeoutError(consumer->consume(), operationTimeout);
			for (const auto& versioned : reply.mutations) {
				for (const auto& mutation : versioned.mutations) {
					if (mutation.type == MutationRef::SetValue && mutation.param1 == keyForIndex(keyCount / 2) &&
					    mutation.param2 == "native-cdc-restart-drain"_sr) {
						if (retagMarkers.present()) {
							ASSERT_LT(versioned.version, retagMarkers.get().cutover);
							observed |= versioned.version == retagMarkers.get().before;
						} else {
							observed = true;
						}
					}
					if (retagMarkers.present() && mutation.type == MutationRef::SetValue &&
					    mutation.param1 == keyForIndex(keyCount / 2) &&
					    mutation.param2 == "native-cdc-restart-after-retag"_sr) {
						ASSERT_GE(versioned.version, retagMarkers.get().cutover);
						observedAfterRetag |= versioned.version == retagMarkers.get().after;
					}
				}
			}
			if (!testRetaggedRestart) {
				co_await timeoutError(consumer->acknowledge(), operationTimeout);
			}
		}
		if (testRetaggedRestart) {
			co_await timeoutError(consumer->acknowledge(), operationTimeout);
			co_await timeoutError(finishRetaggedRestartState(cx, retagMarkers.get()), operationTimeout);
		}
		const NativeCdcRemoveResult removed = co_await timeoutError(
		    removeNativeCdcStreamGuarded(cx, name, consumer->position().streamId), operationTimeout);
		ASSERT(removed == NativeCdcRemoveResult::Removed);
		co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
		const std::vector<NativeCdcStreamInfo> remainingStreams =
		    co_await timeoutError(listNativeCdcStreamsClient(cx), operationTimeout);
		ASSERT(remainingStreams.empty());
		const NativeCdcStatus drainedStatus = co_await timeoutError(getNativeCdcStatus(cx), operationTimeout);
		ASSERT(drainedStatus.metadataComplete);
		ASSERT(!drainedStatus.admissionEnabled);
		ASSERT(drainedStatus.streams.empty());
		ASSERT(std::none_of(drainedStatus.tags.begin(), drainedStatus.tags.end(), [](NativeCdcTagStatus const& tag) {
			return tag.pendingRetiredPop;
		}));

		Optional<Error> registrationError;
		try {
			const std::vector<KeyRange> ranges{ normalKeys };
			co_await timeoutError(registerNativeCdcStreamClient(cx, "native-cdc-e2e/disabled-registration"_sr, ranges),
			                      operationTimeout);
		} catch (Error& e) {
			registrationError = e;
		}
		ASSERT(registrationError.present());
		ASSERT_EQ(registrationError.get().code(), error_code_client_invalid_operation);
	}

	void recordExpectedWrites(std::vector<std::pair<Key, Value>> const& values, Version committedVersion) {
		for (auto& stream : streams) {
			for (const auto& [key, value] : values) {
				if (stream.keys.contains(key)) {
					const auto inserted =
					    stream.expected.emplace(std::make_pair(key, value), ExpectedWrite{ committedVersion, {} });
					ASSERT(inserted.second);
				}
			}
		}
	}

	Future<Void> drainThrough(StreamState* stream, Version throughVersion) {
		const double deadline = now() + operationTimeout;
		while (stream->consumer->position().lastConsumedVersion < throughVersion) {
			const Version previous = stream->consumer->position().lastConsumedVersion;
			CDCConsumeReply reply = co_await timeoutError(stream->consumer->consume(), operationTimeout);
			if (reply.lastConsumedVersion == previous) {
				ASSERT_LT(now(), deadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT_GT(reply.lastConsumedVersion, previous);
			for (const auto& versioned : reply.mutations) {
				ASSERT_GT(versioned.version, previous);
				ASSERT_LE(versioned.version, reply.lastConsumedVersion);
				for (const auto& mutation : versioned.mutations) {
					ASSERT_EQ(mutation.type, MutationRef::SetValue);
					ASSERT(stream->keys.contains(mutation.param1));
					const auto found =
					    stream->expected.find(std::make_pair(Key(mutation.param1), Value(mutation.param2)));
					ASSERT(found != stream->expected.end());
					ASSERT_LE(versioned.version, found->second.committedVersion);
					CODE_PROBE(versioned.version < found->second.committedVersion,
					           "Native CDC validation accepts a committed retry before the returned commit version");
					ASSERT(found->second.observedVersions.insert(versioned.version).second);
				}
			}
			co_await timeoutError(stream->consumer->acknowledge(), operationTimeout);
		}
		for (const auto& expected : stream->expected) {
			if (expected.second.committedVersion <= throughVersion) {
				ASSERT(expected.second.observedVersions.contains(expected.second.committedVersion));
			}
		}
	}

	Future<Void> consumeUntilRemoved(Reference<NativeCdcConsumer> consumer) {
		while (true) {
			try {
				co_await timeoutError(consumer->consume(), operationTimeout);
				co_await timeoutError(consumer->acknowledge(), operationTimeout);
			} catch (Error& e) {
				if (e.code() != error_code_client_invalid_operation) {
					throw;
				}
				co_return;
			}
		}
	}

	Future<Void> removeStream(Database cx, int index, Version throughVersion) {
		ASSERT_GT(index, 0);
		co_await drainThrough(&streams[index], throughVersion);
		Reference<NativeCdcConsumer> pendingConsumer = resumeNativeCdcConsumer(cx, streams[index].consumer->position());
		Future<Void> pendingConsume = consumeUntilRemoved(pendingConsumer);
		co_await delay(0.1);
		co_await timeoutError(removeNativeCdcStreamClient(cx, streams[index].name), operationTimeout);
		co_await timeoutError(pendingConsume, operationTimeout);
		streams.erase(streams.begin() + index);
	}

	Future<Void> run(Database cx) {
		if (testRetagCompatibility) {
			co_await validateRetagCompatibility(cx);
			co_return;
		}
		if (testRetaggingMemoryBound) {
			co_await validateRetaggingMemoryBound(cx);
			co_return;
		}
		if (testProxyRebalanceAutomatic) {
			co_await timeoutError(validateAutomaticProxyRebalance(cx), operationTimeout);
			co_return;
		}
		if (testProxyRebalance) {
			co_await timeoutError(validateProxyRebalance(cx), operationTimeout);
			co_return;
		}
		if (testMultipleRanges) {
			co_await validateMultipleRanges(cx);
			co_return;
		}
		if (testRetiredSharedTagSnapshot) {
			co_await validateRetiredSharedTagSnapshot(cx);
			co_return;
		}
		if (testOversizedPeek) {
			co_await validateOversizedPeek(cx);
			co_return;
		}
		if (testReplyChunking) {
			co_await validateReplyChunking(cx);
			co_return;
		}
		if (testDurableAckScan) {
			co_await validateDurableAcknowledgementScan(cx);
			co_return;
		}
		if (testDelayedRetention) {
			ASSERT_NE(retentionMarkerVersion, invalidVersion);
			co_await delay(retentionValidationDelay);
			for (auto& stream : streams) {
				co_await drainThrough(&stream, retentionMarkerVersion);
			}
		}
		co_await validatePublicLifecycle(cx);
		co_await validateClearClipping(cx);
		co_await validateAssignmentPublication(cx);
		if (testProxyReplacement) {
			co_await validateStaleStreamInitialization(cx);
			co_await validateProxyReplacement(cx);
		}
		if (testMemoryBound) {
			co_await validateProxyMemoryBound(cx);
		}
		Version mostRecentWrite = invalidVersion;
		for (int round = 0; round < rounds; ++round) {
			if (round > 0 && static_cast<int>(streams.size()) > minStreamCount &&
			    (round % 3 == 0 || deterministicRandom()->random01() < 0.35)) {
				const int removalIndex = deterministicRandom()->randomInt(1, static_cast<int>(streams.size()));
				co_await removeStream(cx, removalIndex, mostRecentWrite);
			}
			if (static_cast<int>(streams.size()) < maxStreamCount &&
			    (round % 2 == 0 || deterministicRandom()->random01() < 0.35)) {
				co_await addStream(cx);
			}

			std::set<int> chosenKeys{ keyCount / 2 };
			while (static_cast<int>(chosenKeys.size()) < writesPerRound) {
				chosenKeys.insert(deterministicRandom()->randomInt(0, keyCount));
			}
			std::vector<std::pair<Key, Value>> values;
			values.reserve(chosenKeys.size());
			for (int index : chosenKeys) {
				values.emplace_back(keyForIndex(index), Value(StringRef(format("round/%04d/key/%04d", round, index))));
			}
			mostRecentWrite = co_await writeValues(cx, values);
			recordExpectedWrites(values, mostRecentWrite);

			// streams[0] intentionally stays behind while other streams are removed.
			for (int i = 1; i < static_cast<int>(streams.size()); ++i) {
				if (deterministicRandom()->random01() < drainProbability) {
					co_await drainThrough(&streams[i], mostRecentWrite);
				}
			}
			co_await delay(delayBetweenRounds);
		}

		for (auto& stream : streams) {
			co_await drainThrough(&stream, mostRecentWrite);
		}
		while (streams.size() > (testRetiredRecovery ? 1 : 0)) {
			co_await timeoutError(removeNativeCdcStreamClient(cx, streams.back().name), operationTimeout);
			streams.pop_back();
		}
		if (testRetiredRecovery) {
			co_await validateRetiredCleanupAcrossRecovery(cx);
		} else {
			co_await timeoutError(waitForRetiredTagCleanup(cx), operationTimeout);
		}
		if (testTagOwnership) {
			co_await timeoutError(validateTagOwnership(cx), operationTimeout);
		}
	}

public:
	static constexpr auto NAME = "NativeCdcEndToEnd";

	explicit NativeCdcEndToEndWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		initialStreamCount = getOption(options, "initialStreamCount"_sr, 12);
		minStreamCount = getOption(options, "minStreamCount"_sr, 6);
		maxStreamCount = getOption(options, "maxStreamCount"_sr, 20);
		keyCount = getOption(options, "keyCount"_sr, 16);
		writesPerRound = getOption(options, "writesPerRound"_sr, 5);
		rounds = getOption(options, "rounds"_sr, 30);
		assignmentPublicationChecks = getOption(options, "assignmentPublicationChecks"_sr, 0);
		testProxyReplacement = getOption(options, "testProxyReplacement"_sr, false);
		testProxyRebalance = getOption(options, "testProxyRebalance"_sr, false);
		testProxyRebalanceAutomatic = getOption(options, "testProxyRebalanceAutomatic"_sr, false);
		testTagOwnership = getOption(options, "testTagOwnership"_sr, false);
		injectUndeliveredProxyHalt = getOption(options, "injectUndeliveredProxyHalt"_sr, false);
		testMemoryBound = getOption(options, "testMemoryBound"_sr, false);
		testReplyChunking = getOption(options, "testReplyChunking"_sr, false);
		testMultipleRanges = getOption(options, "testMultipleRanges"_sr, false);
		testOversizedPeek = getOption(options, "testOversizedPeek"_sr, false);
		testDurableAckScan = getOption(options, "testDurableAckScan"_sr, false);
		testDelayedRetention = getOption(options, "testDelayedRetention"_sr, false);
		testRetiredRecovery = getOption(options, "testRetiredRecovery"_sr, false);
		blockRetiredPopWithLiveStream = getOption(options, "blockRetiredPopWithLiveStream"_sr, false);
		testRetiredSharedTagSnapshot = getOption(options, "testRetiredSharedTagSnapshot"_sr, false);
		testRetagCompatibility = getOption(options, "testRetagCompatibility"_sr, false);
		testRetaggingMemoryBound = getOption(options, "testRetaggingMemoryBound"_sr, false);
		prepareRestartDrain = getOption(options, "prepareRestartDrain"_sr, false);
		drainAfterRestart = getOption(options, "drainAfterRestart"_sr, false);
		testRetaggedRestart = getOption(options, "testRetaggedRestart"_sr, false);
		testRetagTransactionRetries = getOption(options, "testRetagTransactionRetries"_sr, false);
		memoryTestValueBytes = getOption(options, "memoryTestValueBytes"_sr, 1024);
		retentionValidationDelay = getOption(options, "retentionValidationDelay"_sr, 0.0);
		drainProbability = getOption(options, "drainProbability"_sr, 0.25);
		delayBetweenRounds = getOption(options, "delayBetweenRounds"_sr, 0.5);
		operationTimeout = getOption(options, "operationTimeout"_sr, 120.0);
		ASSERT_GE(minStreamCount, 1);
		ASSERT_GE(initialStreamCount, minStreamCount);
		ASSERT_GE(maxStreamCount, initialStreamCount);
		ASSERT_GE(keyCount, 2);
		ASSERT_GE(writesPerRound, 1);
		ASSERT_LE(writesPerRound, keyCount);
		ASSERT_GE(assignmentPublicationChecks, 0);
		ASSERT(!(testProxyRebalance && testProxyRebalanceAutomatic));
		ASSERT(!(testProxyRebalance || testProxyRebalanceAutomatic) || initialStreamCount == 3);
		ASSERT(!injectUndeliveredProxyHalt || testProxyReplacement);
		ASSERT_GT(memoryTestValueBytes, 0);
		ASSERT_GE(retentionValidationDelay, 0.0);
		ASSERT(!(prepareRestartDrain && drainAfterRestart));
		ASSERT(!testRetaggedRestart || prepareRestartDrain || drainAfterRestart);
		ASSERT(!testRetagTransactionRetries || testRetagCompatibility || testRetaggedRestart);
		ASSERT(!(testReplyChunking && (testOversizedPeek || testDurableAckScan)));
		ASSERT(!(testOversizedPeek && testDurableAckScan));
		ASSERT(!(testRetiredSharedTagSnapshot && testRetiredRecovery));
		ASSERT(!(testRetagCompatibility && testMemoryBound));
		ASSERT(!testRetaggingMemoryBound || (!testRetagCompatibility && !testMemoryBound && !prepareRestartDrain &&
		                                     !drainAfterRestart && initialStreamCount == 2 && keyCount >= 2));
		ASSERT(!testRetagCompatibility ||
		       (initialStreamCount == 4 && keyCount >= 4 && memoryTestValueBytes >= 32 && !prepareRestartDrain &&
		        !drainAfterRestart && !testRetiredSharedTagSnapshot && !testOversizedPeek && !testReplyChunking &&
		        !testDurableAckScan));
		ASSERT(!blockRetiredPopWithLiveStream || testRetiredRecovery);
	}

	// RandomRangeLock can outlive this bounded CDC workload and mask its progress check.
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("RandomRangeLock"); }

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		if (drainAfterRestart) {
			return Void();
		}
		if (testMultipleRanges) {
			return Void();
		}
		if (prepareRestartDrain) {
			return prepareRestartDrainSetup(cx);
		}
		if (testRetagCompatibility || testRetaggingMemoryBound) {
			return initializeRetaggingStreams(cx);
		}
		if (testRetiredSharedTagSnapshot) {
			return Void();
		}
		if (testOversizedPeek) {
			return initializeOversizedPeekStreams(cx);
		}
		if (testReplyChunking) {
			return initializeReplyChunkingStream(cx);
		}
		return initializeStreams(cx);
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		if (prepareRestartDrain) {
			return Void();
		}
		if (drainAfterRestart) {
			return drainRestartState(cx);
		}
		return run(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<NativeCdcEndToEndWorkload> NativeCdcEndToEndWorkloadFactory;
