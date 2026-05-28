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

#include <map>
#include <set>
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

	struct StreamState {
		Key name;
		KeyRange keys;
		CDCCursor cursor;
		std::map<std::pair<Key, Value>, ExpectedWrite> expected;
	};

	int initialStreamCount;
	int minStreamCount;
	int maxStreamCount;
	int keyCount;
	int writesPerRound;
	int rounds;
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
		drainProbability = getOption(options, "drainProbability"_sr, 0.25);
		delayBetweenRounds = getOption(options, "delayBetweenRounds"_sr, 0.5);
		operationTimeout = getOption(options, "operationTimeout"_sr, 120.0);
		ASSERT(minStreamCount >= 1);
		ASSERT(initialStreamCount >= minStreamCount);
		ASSERT(maxStreamCount >= initialStreamCount);
		ASSERT(keyCount >= 2);
		ASSERT(writesPerRound >= 1 && writesPerRound <= keyCount);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

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
		stream.cursor = co_await timeoutError(createNativeCdcCursor(cx, stream.name), operationTimeout);
		streams.push_back(std::move(stream));
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

	Future<Void> drainThrough(Database cx, StreamState* stream, Version throughVersion) {
		const double deadline = now() + operationTimeout;
		while (stream->cursor.lastConsumedVersion < throughVersion) {
			const Version previous = stream->cursor.lastConsumedVersion;
			CDCConsumeReply reply = co_await timeoutError(consumeNativeCdcStream(cx, stream->cursor), operationTimeout);
			if (reply.lastConsumedVersion == previous) {
				ASSERT(now() < deadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT(reply.lastConsumedVersion > previous);
			for (const auto& versioned : reply.mutations) {
				ASSERT(versioned.version > previous);
				ASSERT(versioned.version <= reply.lastConsumedVersion);
				for (const auto& mutation : versioned.mutations) {
					ASSERT(mutation.type == MutationRef::SetValue);
					ASSERT(stream->keys.contains(mutation.param1));
					const auto found =
					    stream->expected.find(std::make_pair(Key(mutation.param1), Value(mutation.param2)));
					ASSERT(found != stream->expected.end());
					found->second.observed = true;
				}
			}
			stream->cursor.lastConsumedVersion = reply.lastConsumedVersion;
		}
		for (const auto& expected : stream->expected) {
			if (expected.second.deadline <= throughVersion) {
				ASSERT(expected.second.observed);
			}
		}
		co_await timeoutError(acknowledgeNativeCdcStreamClient(cx, stream->cursor), operationTimeout);
	}

	Future<Void> removeStream(Database cx, int index, Version throughVersion) {
		ASSERT(index > 0);
		co_await drainThrough(cx, &streams[index], throughVersion);
		co_await timeoutError(removeNativeCdcStreamClient(cx, streams[index].name), operationTimeout);
		streams.erase(streams.begin() + index);
	}

	Future<Void> run(Database cx) {
		for (int i = 0; i < initialStreamCount; ++i) {
			co_await addStream(cx);
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
			for (int index : chosenKeys) {
				values.emplace_back(keyForIndex(index), Value(StringRef(format("round/%04d/key/%04d", round, index))));
			}
			mostRecentWrite = co_await writeValues(cx, values);
			recordExpectedWrites(values, mostRecentWrite);

			// streams[0] intentionally stays behind while other streams are removed.
			for (int i = 1; i < static_cast<int>(streams.size()); ++i) {
				if (deterministicRandom()->random01() < drainProbability) {
					co_await drainThrough(cx, &streams[i], mostRecentWrite);
				}
			}
			co_await delay(delayBetweenRounds);
		}

		for (auto& stream : streams) {
			co_await drainThrough(cx, &stream, mostRecentWrite);
		}
		while (!streams.empty()) {
			co_await timeoutError(removeNativeCdcStreamClient(cx, streams.back().name), operationTimeout);
			streams.pop_back();
		}
	}
};

WorkloadFactory<NativeCdcEndToEndWorkload> NativeCdcEndToEndWorkloadFactory;
