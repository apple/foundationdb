/*
 * BenchTempTagMessages.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "benchmark/benchmark.h"
#include "fdbclient/FDBTypes.h"
#include "flow/IRandom.h"
#include "flow/Arena.h"
#include <vector>

// Benchmark for TLog tempTagMessages vector reserve optimization
// This measures the impact of pre-reserving vector capacity vs dynamic growth

// Helper function to create realistic TagsAndMessage objects
static TagsAndMessage createTestMessage(Arena& arena, int messageSize, int numTags) {
	TagsAndMessage msg;

	// Create message payload
	uint8_t* data = new (arena) uint8_t[messageSize];
	for (int i = 0; i < messageSize; i++) {
		data[i] = (uint8_t)(deterministicRandom()->randomInt(0, 256));
	}
	msg.message = StringRef(data, messageSize);

	// Create tags
	Tag* tagData = new (arena) Tag[numTags];
	for (int i = 0; i < numTags; i++) {
		tagData[i] = Tag(deterministicRandom()->randomInt(0, 10), deterministicRandom()->randomInt(0, 100));
	}
	msg.tags = VectorRef<Tag>(tagData, numTags);

	return msg;
}

// Baseline: Current implementation without reserve (dynamic growth)
static void BM_TempTagMessages_NoReserve(benchmark::State& state) {
	int numMessages = state.range(0);
	int avgMessageSize = state.range(1);

	for (auto _ : state) {
		state.PauseTiming();
		Arena arena;
		std::vector<TagsAndMessage> sourceMessages;

		// Generate test messages
		for (int i = 0; i < numMessages; i++) {
			int msgSize = avgMessageSize + deterministicRandom()->randomInt(-50, 51);
			sourceMessages.push_back(createTestMessage(arena, msgSize, 3));
		}
		state.ResumeTiming();

		// Simulate commitMessages() without reserve
		std::vector<TagsAndMessage> tempTagMessages;
		tempTagMessages.clear();
		for (auto& msg : sourceMessages) {
			tempTagMessages.push_back(std::move(msg)); // Dynamic reallocation
		}

		benchmark::DoNotOptimize(tempTagMessages);
		benchmark::ClobberMemory();
	}

	state.SetItemsProcessed(state.iterations() * numMessages);
	state.SetBytesProcessed(state.iterations() * numMessages * avgMessageSize);
}

// Optimized: Pre-reserve capacity based on message size estimation
static void BM_TempTagMessages_WithReserve(benchmark::State& state) {
	int numMessages = state.range(0);
	int avgMessageSize = state.range(1);

	for (auto _ : state) {
		state.PauseTiming();
		Arena arena;
		std::vector<TagsAndMessage> sourceMessages;
		size_t totalBytes = 0;

		// Generate test messages
		for (int i = 0; i < numMessages; i++) {
			int msgSize = avgMessageSize + deterministicRandom()->randomInt(-50, 51);
			sourceMessages.push_back(createTestMessage(arena, msgSize, 3));
			totalBytes += msgSize;
		}
		state.ResumeTiming();

		// Simulate commitMessages() WITH reserve (proposed optimization)
		std::vector<TagsAndMessage> tempTagMessages;
		tempTagMessages.clear();

		// Pre-reserve capacity using estimation from TLOG_OPTIMIZATIONS.md
		size_t estimatedMsgCount = std::max(totalBytes / 150, size_t(10));
		estimatedMsgCount = std::min(estimatedMsgCount, size_t(5000));
		tempTagMessages.reserve(estimatedMsgCount);

		for (auto& msg : sourceMessages) {
			tempTagMessages.push_back(std::move(msg)); // No reallocations
		}

		benchmark::DoNotOptimize(tempTagMessages);
		benchmark::ClobberMemory();
	}

	state.SetItemsProcessed(state.iterations() * numMessages);
	state.SetBytesProcessed(state.iterations() * numMessages * avgMessageSize);
}

// Test with exact reserve (optimal case for comparison)
static void BM_TempTagMessages_ExactReserve(benchmark::State& state) {
	int numMessages = state.range(0);
	int avgMessageSize = state.range(1);

	for (auto _ : state) {
		state.PauseTiming();
		Arena arena;
		std::vector<TagsAndMessage> sourceMessages;

		// Generate test messages
		for (int i = 0; i < numMessages; i++) {
			int msgSize = avgMessageSize + deterministicRandom()->randomInt(-50, 51);
			sourceMessages.push_back(createTestMessage(arena, msgSize, 3));
		}
		state.ResumeTiming();

		// Simulate with exact reserve (best case scenario)
		std::vector<TagsAndMessage> tempTagMessages;
		tempTagMessages.clear();
		tempTagMessages.reserve(sourceMessages.size()); // Perfect knowledge

		for (auto& msg : sourceMessages) {
			tempTagMessages.push_back(std::move(msg));
		}

		benchmark::DoNotOptimize(tempTagMessages);
		benchmark::ClobberMemory();
	}

	state.SetItemsProcessed(state.iterations() * numMessages);
	state.SetBytesProcessed(state.iterations() * numMessages * avgMessageSize);
}

// Benchmark different scenarios:
// - Small batches (10 messages, 100 bytes avg)
// - Medium batches (100 messages, 150 bytes avg)
// - Large batches (1000 messages, 200 bytes avg)
// - Very large batches (5000 messages, 150 bytes avg)

BENCHMARK(BM_TempTagMessages_NoReserve)
    ->Args({ 10, 100 })
    ->Args({ 100, 150 })
    ->Args({ 1000, 200 })
    ->Args({ 5000, 150 });

BENCHMARK(BM_TempTagMessages_WithReserve)
    ->Args({ 10, 100 })
    ->Args({ 100, 150 })
    ->Args({ 1000, 200 })
    ->Args({ 5000, 150 });

BENCHMARK(BM_TempTagMessages_ExactReserve)
    ->Args({ 10, 100 })
    ->Args({ 100, 150 })
    ->Args({ 1000, 200 })
    ->Args({ 5000, 150 });
