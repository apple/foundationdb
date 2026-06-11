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
#include <unordered_map>
#include <utility>
#include <vector>

#include "fdbclient/NativeCdc.h"
#include "fdbserver/tester/workloads.h"
#include "flow/DeterministicRandom.h"

struct NativeCdcEndToEndWorkload : TestWorkload {
	static constexpr auto NAME = "NativeCdcEndToEnd";

	struct ExpectedWrite {
		Version deadline;
		bool observed = false;
	};

	struct KeyValueHash {
		size_t operator()(const std::pair<Key, Value>& item) const {
			size_t hash = std::hash<Key>{}(item.first);
			hash ^= std::hash<Value>{}(item.second) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
			return hash;
		}
	};

	struct StreamState {
		Key name;
		KeyRange keys;
		Reference<NativeCdcConsumer> consumer;
		std::unordered_map<std::pair<Key, Value>, ExpectedWrite, KeyValueHash> expected;
	};

	int initialStreamCount;
	int minStreamCount;
	int maxStreamCount;
	int keyCount;
	int writesPerRound;
	int rounds;
	int assignmentPublicationChecks;
	double drainProbability;
	double delayBetweenRounds;
	double operationTimeout;
	int nextStreamNumber = 0;
	std::vector<StreamState> streams;

	explicit NativeCdcEndToEndWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		initialStreamCount = getOption(options, "initialStreamCount"_sr, 12);
		minStreamCount = getOption(options, "minStreamCount"_sr, 6);
		maxStreamCount = getOption(options, "maxStreamCount"_sr, 20);
		keyCount = getOption(options, "keyCount"_sr, 16);
		writesPerRound = getOption(options, "writesPerRound"_sr, 5);
		rounds = getOption(options, "rounds"_sr, 30);
		assignmentPublicationChecks = getOption(options, "assignmentPublicationChecks"_sr, 0);
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
	}

	// RandomRangeLock can outlive this bounded CDC workload and mask its progress check.
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("RandomRangeLock"); }

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return initializeStreams(cx);
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return run(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

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

	Future<Void> addStream(Database cx) {
		StreamState stream;
		stream.name = Key(StringRef(format("native-cdc-e2e/stream/%04d", nextStreamNumber++)));
		stream.keys = randomOverlappingRange();
		co_await timeoutError(registerNativeCdcStreamClient(cx, stream.name, stream.keys), operationTimeout);
		stream.consumer = co_await timeoutError(createNativeCdcConsumer(cx, stream.name), operationTimeout);
		streams.push_back(std::move(stream));
	}

	Future<Void> initializeStreams(Database cx) {
		for (int i = 0; i < initialStreamCount; ++i) {
			co_await addStream(cx);
		}
	}

	Future<Void> validatePublicLifecycle(Database cx) {
		const Key name = "native-cdc-e2e/lifecycle"_sr;
		const KeyRange keys(KeyRangeRef("native-cdc-e2e/lifecycle/"_sr, "native-cdc-e2e/lifecycle0"_sr));
		const KeyRange conflictingKeys(KeyRangeRef("native-cdc-e2e/lifecycle/"_sr, "native-cdc-e2e/lifecycle1"_sr));

		const CDCStreamId streamId =
		    co_await timeoutError(registerNativeCdcStreamClient(cx, name, keys), operationTimeout);
		ASSERT_EQ(co_await timeoutError(registerNativeCdcStreamClient(cx, name, keys), operationTimeout), streamId);

		bool conflictingRegistrationRejected = false;
		try {
			co_await timeoutError(registerNativeCdcStreamClient(cx, name, conflictingKeys), operationTimeout);
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
		ASSERT_EQ(found->keys, keys);

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
	}

	Future<Void> validateAssignmentPublication(Database cx) {
		const KeyRange keys(KeyRangeRef("native-cdc-e2e/assignment/data/"_sr, "native-cdc-e2e/assignment/data0"_sr));
		for (int check = 0; check < assignmentPublicationChecks; ++check) {
			const Key name = Key(StringRef(format("native-cdc-e2e/assignment/%04d", check)));
			const Key key = Key(StringRef(format("native-cdc-e2e/assignment/data/%04d", check)));
			const Value value = Value(StringRef(format("assignment-value/%04d", check)));

			co_await delay(0.1);
			const CDCStreamId streamId =
			    co_await timeoutError(registerNativeCdcStreamClient(cx, name, keys), operationTimeout);
			Reference<NativeCdcConsumer> consumer =
			    co_await timeoutError(createNativeCdcConsumer(cx, name), operationTimeout);
			ASSERT_EQ(consumer->position().streamId, streamId);

			const Version committed = co_await writeValues(cx, { { key, value } });
			bool observed = false;
			const double deadline = now() + operationTimeout;
			while (!observed && consumer->position().lastConsumedVersion < committed) {
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
			co_await timeoutError(removeNativeCdcStreamClient(cx, name), operationTimeout);
		}
	}

	void recordExpectedWrites(std::vector<std::pair<Key, Value>> const& values, Version committedVersion) {
		for (auto& stream : streams) {
			for (const auto& [key, value] : values) {
				if (stream.keys.contains(key)) {
					const auto inserted =
					    stream.expected.emplace(std::make_pair(key, value), ExpectedWrite{ committedVersion });
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
					found->second.observed = true;
				}
			}
			co_await timeoutError(stream->consumer->acknowledge(), operationTimeout);
		}
		for (const auto& expected : stream->expected) {
			if (expected.second.deadline <= throughVersion) {
				ASSERT(expected.second.observed);
			}
		}
	}

	Future<Void> removeStream(Database cx, int index, Version throughVersion) {
		ASSERT_GT(index, 0);
		co_await drainThrough(&streams[index], throughVersion);
		Reference<NativeCdcConsumer> pendingConsumer = resumeNativeCdcConsumer(
		    cx, CDCCursor(streams[index].consumer->position().streamId, std::numeric_limits<Version>::max() - 2));
		Future<CDCConsumeReply> pendingConsume = pendingConsumer->consume();
		co_await delay(0.1);
		co_await timeoutError(removeNativeCdcStreamClient(cx, streams[index].name), operationTimeout);
		bool pendingConsumeRejected = false;
		try {
			co_await timeoutError(pendingConsume, operationTimeout);
		} catch (Error& e) {
			if (e.code() != error_code_client_invalid_operation) {
				throw;
			}
			pendingConsumeRejected = true;
		}
		ASSERT_EQ(pendingConsumeRejected, true);
		streams.erase(streams.begin() + index);
	}

	Future<Void> run(Database cx) {
		co_await validatePublicLifecycle(cx);
		co_await validateAssignmentPublication(cx);
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
		while (!streams.empty()) {
			co_await timeoutError(removeNativeCdcStreamClient(cx, streams.back().name), operationTimeout);
			streams.pop_back();
		}
	}
};

WorkloadFactory<NativeCdcEndToEndWorkload> NativeCdcEndToEndWorkloadFactory;
