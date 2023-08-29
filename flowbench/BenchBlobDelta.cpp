/*
 * BenchBlobDeltaFiles.cpp
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

#include "benchmark/benchmark.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "flow/IRandom.h"
#include "flow/DeterministicRandom.h"

#include "fdbclient/BlobGranuleFiles.h"
#include "flow/flow.h"
#include <cstdlib>
#include <stdexcept>

// Pre-generated GranuleDelta size in bytes for benchmark.
const static int PRE_GEN_TARGET_BYTES[] = { 128 * 1024, 512 * 1024, 1024 * 1024 };

// Generate GranuleDelta using a deterministic way. Change the seed if you would test a new data set
class DeltaGenerator {
public:
	DeltaGenerator(uint32_t seed = 12345678) {
		randGen = Reference<IRandom>(new DeterministicRandom(seed));
		// Generate key range
		prefix = StringRef(ar, randGen->randomUniqueID().toString() + "_");
		range = KeyRangeRef(prefix, StringRef(ar, strinc(prefix)));
		// Generate version jump size
		minVersionJump = randGen->randomExp(0, 25);
		maxVersionJump = minVersionJump + randGen->randomExp(0, 25);
		// Generate value size range
		maxValueSize = randGen->randomExp(7, 9);
		// Generate start version
		version = randGen->randomUInt32();
		// Generate probabilty of update existing keys
		updateExistingKeysProb = randGen->random01();
		// Generate deltas
		for (auto i : PRE_GEN_TARGET_BYTES) {
			genDeltas(i);
		}

		fmt::print("key range: {} - {}\n", range.begin.printable(), range.end.printable());
		fmt::print("start version: {}\n", version);
		fmt::print("max value bytes: {}\n", maxValueSize);
		fmt::print("version jump range: {} - {}\n", minVersionJump, maxVersionJump);
		fmt::print("probability for update: {}\n", updateExistingKeysProb);
		fmt::print("unseed: {}\n", randGen->randomUInt32());
	}

	KeyRange getRange() { return range; }

	Standalone<GranuleDeltas> getDelta(int targetBytes) {
		if (deltas.find(targetBytes) != deltas.end()) {
			return deltas[targetBytes];
		}
		throw std::invalid_argument("Test delta file size is not pre-generated!");
	}

private:
	void genDeltas(int targetBytes) {
		Standalone<GranuleDeltas> data;
		int totalDataBytes = 0;
		while (totalDataBytes < targetBytes) {
			data.push_back(ar, newDelta());
			totalDataBytes += data.back().expectedSize();
		}
		deltas[targetBytes] = data;
	}

	MutationRef newMutation() { return MutationRef(ar, MutationRef::SetValue, key(), value()); }

	MutationsAndVersionRef newDelta() {
		version += randGen->randomInt(minVersionJump, maxVersionJump);
		MutationsAndVersionRef ret(version, version);
		for (int i = 0; i < 10; i++) {
			ret.mutations.push_back_deep(ar, newMutation());
		}
		return ret;
	}

	StringRef key() {
		// Pick an existing key
		if (randGen->random01() < updateExistingKeysProb && !usedKeys.empty()) {
			int r = randGen->randomUInt32() % usedKeys.size();
			auto it = usedKeys.begin();
			for (; r != 0; r--)
				it++;
			return StringRef(ar, *it);
		}

		// Create a new key
		std::string key = prefix.toString() + randGen->randomUniqueID().toString();
		usedKeys.insert(key);
		return StringRef(ar, key);
	}

	StringRef value() {
		int valueSize = randGen->randomInt(maxValueSize / 2, maxValueSize * 3 / 2);
		std::string value = randGen->randomUniqueID().toString();
		if (value.size() > valueSize) {
			value = value.substr(0, valueSize);
		}
		if (value.size() < valueSize) {
			// repeated string so it's compressible
			value += std::string(valueSize - value.size(), 'x');
		}
		return StringRef(ar, value);
	}

	Reference<IRandom> randGen;
	Arena ar;
	KeyRangeRef range;
	Key prefix;
	int maxValueSize;
	Version version;
	int minVersionJump;
	int maxVersionJump;
	std::set<std::string> usedKeys;
	double updateExistingKeysProb;
	std::map<int, Standalone<GranuleDeltas>> deltas;
};

static DeltaGenerator deltaGen; // Pre-generate deltas

// Benchmark serialization without compression/encryption. The main CPU cost should be sortDeltasByKey
static void bench_serialize_deltas(benchmark::State& state) {
	int targetBytes = state.range(0);
	int chunkSize = state.range(1);
	bool enableEncryption = state.range(2);

	Standalone<GranuleDeltas> delta = deltaGen.getDelta(targetBytes);
	KeyRange range = deltaGen.getRange();

	Standalone<StringRef> fileName = "testdelta"_sr; // unused
	Optional<CompressionFilter> compressFilter; // unused. no compression
	Arena arena;
	Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx; // unused. no encryption
	if (enableEncryption) {
		cipherKeysCtx = getCipherKeysCtx(arena);
	}

	uint32_t serializedBytes = 0;
	for (auto _ : state) {
		Value serialized = serializeChunkedDeltaFile(fileName, delta, range, chunkSize, compressFilter, cipherKeysCtx);
		serializedBytes += serialized.size();
	}
	state.SetBytesProcessed(static_cast<long>(state.iterations()) * targetBytes);
	state.counters["serialized_bytes"] = serializedBytes;
}

// Benchmark sorting deltas
static void bench_sort_deltas(benchmark::State& state) {
	int targetBytes = state.range(0);
	Standalone<GranuleDeltas> delta = deltaGen.getDelta(targetBytes);
	KeyRange range = deltaGen.getRange();

	for (auto _ : state) {
		sortDeltasByKey(delta, range);
	}
	state.SetBytesProcessed(static_cast<long>(state.iterations()) * targetBytes);
}

// Benchmark serialization for granule deltas 128KB, 512KB and 1024KB. Chunk size 32KB
BENCHMARK(bench_serialize_deltas)
    ->Args({ 128 * 1024, 32 * 1024, false })
    ->Args({ 512 * 1024, 32 * 1024, false })
    ->Args({ 1024 * 1024, 32 * 1024, false })
    ->Args({ 128 * 1024, 32 * 1024, true })
    ->Args({ 512 * 1024, 32 * 1024, true })
    ->Args({ 1024 * 1024, 32 * 1024, true });

// Benchmark sorting for granule deltas 128KB, 512KB and 1024KB. Chunk size 32KB
BENCHMARK(bench_sort_deltas)->Args({ 128 * 1024 })->Args({ 512 * 1024 })->Args({ 1024 * 1024 });