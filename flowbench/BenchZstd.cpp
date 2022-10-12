/*
 * BenchZstd.cpp
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
#include "flow/IRandom.h"
#include "flow/DeterministicRandom.h"

#include <cstdio>
#include <memory>
#include <sstream>
#include <fstream>
#include <utility>

#ifdef ZSTD_LIB_SUPPORTED

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h"

// Benchmark zstd performance. To use it:
//  # export BM_ZSTD_DATA=path/to/datafile
//  # bin/flowbench --benchmark_filter=bench_zstd

// Compress with raw ZSTD API
static inline std::pair<std::unique_ptr<char[]>, size_t> compress(const std::string& data, int level) {
	const char* srcBegin = data.data();
	size_t destSize = ZSTD_compressBound(data.size());
	std::unique_ptr<char[]> dest = std::make_unique<char[]>(destSize);
	size_t bytes = ZSTD_compress(dest.get(), destSize, srcBegin, data.size(), level);
	return std::make_pair(std::move(dest), bytes);
}

// Compress with raw ZSTD API ZSTD_compress2
static inline std::pair<std::unique_ptr<char[]>, size_t> compress2(const std::string& data, ZSTD_CCtx* cctx) {
	const char* srcBegin = data.data();
	size_t destSize = ZSTD_compressBound(data.size());
	std::unique_ptr<char[]> dest = std::make_unique<char[]>(destSize);
	size_t bytes = ZSTD_compress2(cctx, dest.get(), destSize, srcBegin, data.size());
	return std::make_pair(std::move(dest), bytes);
}

// Deompress with raw ZSTD API
static inline std::pair<std::unique_ptr<char[]>, size_t> decompress(const std::string& data) {
	size_t destSize = ZSTD_decompressBound(data.data(), data.size());
	std::unique_ptr<char[]> dest = std::make_unique<char[]>(destSize);
	size_t bytes = ZSTD_decompress(dest.get(), destSize, data.data(), data.size());
	return std::make_pair(std::move(dest), bytes);
}

// Compress with raw ZSTD stream API
static inline std::pair<std::unique_ptr<char[]>, size_t> compressAsStream(const std::string& data,
                                                                          ZSTD_CStream* cstream) {
	size_t dstSize = ZSTD_compressBound(data.size());
	std::unique_ptr<char[]> dst = std::make_unique<char[]>(dstSize);

	ZSTD_inBuffer in;
	in.src = data.data();
	in.size = data.size();
	in.pos = 0;
	ZSTD_outBuffer out;
	out.dst = dst.get();
	out.size = dstSize;
	out.pos = 0;

	ZSTD_compressStream(cstream, &out, &in);
	ZSTD_flushStream(cstream, &out);
	return std::make_pair(std::move(dst), out.pos);
}

// Decompress with raw ZSTD stream API
static inline std::pair<std::unique_ptr<char[]>, size_t> decompressAsStream(const std::string& data,
                                                                            ZSTD_DStream* dstream) {
	size_t destSize = ZSTD_decompressBound(data.data(), data.size());
	std::unique_ptr<char[]> dest = std::make_unique<char[]>(destSize);

	ZSTD_inBuffer in;
	in.src = data.data();
	in.size = data.size();
	in.pos = 0;
	ZSTD_outBuffer out;
	out.dst = dest.get();
	out.size = destSize;
	out.pos = 0;

	do {
		ZSTD_decompressStream(dstream, &out, &in);
	} while (in.pos < in.size && out.pos < out.size);

	return std::make_pair(std::move(dest), destSize);
}

// Generate uncompressed data for compression testing
static std::string genUncompressedData() {
	char* dataFileName = std::getenv("BM_ZSTD_DATA");
	if (dataFileName) {
		std::ifstream file(dataFileName, std::ios::binary | std::ios::ate);
		std::streamsize size = file.tellg();
		file.seekg(0, std::ios::beg);
		std::string buf(size, ' ');
		file.read(buf.data(), size);
		std::printf("Load test data %s: %ld bytes\n", dataFileName, size);
		return buf;
	} else {
		DeterministicRandom random(0x1234567, true);
		return random.randomAlphaNumeric(1048576);
	}
}

// Generate compressed data for decompression testing
static std::map<int, std::string> genCompressedData() {
	std::map<int, std::string> result;
	std::string data = genUncompressedData();
	// test compression level 1, 3, 9
	for (int level : { 1, 3, 9 }) {
		auto compressed = compress(data, level);
		result[level] = std::string(compressed.first.get(), compressed.second);
	}
	return result;
}

static std::string UNCOMPRESSED = genUncompressedData();
static std::map<int, std::string> COMPRESSED = genCompressedData();

static void bench_zstd_compress(benchmark::State& state) {
	auto chunkSize = state.range(0);
	auto level = state.range(1);
	float ratio = 0;
	for (auto _ : state) {
		size_t compressedSize = 0;
		for (int i = 0; i < UNCOMPRESSED.size(); i += chunkSize) {
			auto compressed = compress(UNCOMPRESSED.substr(i, chunkSize), level);
			compressedSize += compressed.second;
		}
		ratio = compressedSize * 1.0 / UNCOMPRESSED.size();
	}
	state.SetBytesProcessed(UNCOMPRESSED.size() * static_cast<long>(state.iterations()));
	state.counters["compression_ratio"] = ratio;
}

static void bench_zstd_compress2(benchmark::State& state) {
	auto chunkSize = state.range(0);
	auto level = state.range(1);
	float ratio = 0;
	ZSTD_CCtx* cctx = ZSTD_createCCtx();
	ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, level);
	// ZSTD_CCtx_setParameter(cctx, ZSTD_c_strategy, 2);
	// ZSTD_CCtx_setParameter(cctx, ZSTD_c_windowLog, 21);
	// ZSTD_CCtx_setParameter(cctx, ZSTD_c_hashLog, 17);
	// ZSTD_CCtx_setParameter(cctx, ZSTD_c_chainLog, 16);
	// ZSTD_CCtx_setParameter(cctx, ZSTD_c_searchLog, 1);
	// ZSTD_CCtx_setParameter(cctx, ZSTD_c_minMatch, 5);
	for (auto _ : state) {
		size_t compressedSize = 0;
		for (int i = 0; i < UNCOMPRESSED.size(); i += chunkSize) {
			auto compressed = compress2(UNCOMPRESSED.substr(i, chunkSize), cctx);
			compressedSize += compressed.second;
		}
		ratio = compressedSize * 1.0 / UNCOMPRESSED.size();
	}
	state.SetBytesProcessed(UNCOMPRESSED.size() * static_cast<long>(state.iterations()));
	state.counters["compression_ratio"] = ratio;
}

static void bench_zstd_compress_stream(benchmark::State& state) {
	auto chunkSize = state.range(0);
	auto level = state.range(1);
	float ratio = 0;

	ZSTD_CStream* cstream = ZSTD_createCStream();
	ZSTD_initCStream(cstream, level);
	for (auto _ : state) {
		size_t compressedSize = 0;
		for (int i = 0; i < UNCOMPRESSED.size(); i += chunkSize) {
			auto compressed = compressAsStream(UNCOMPRESSED.substr(i, chunkSize), cstream);
			compressedSize += compressed.second;
		}
		ratio = compressedSize * 1.0 / UNCOMPRESSED.size();
	}
	ZSTD_freeCStream(cstream);
	state.SetBytesProcessed(UNCOMPRESSED.size() * static_cast<long>(state.iterations()));
	state.counters["compression_ratio"] = ratio;
}

static void bench_zstd_decompress(benchmark::State& state) {
	auto level = state.range(0);
	for (auto _ : state) {
		benchmark::DoNotOptimize(decompress(COMPRESSED[level]));
	}
	state.SetBytesProcessed(UNCOMPRESSED.size() * static_cast<long>(state.iterations()));
}

static void bench_zstd_decompress_stream(benchmark::State& state) {
	auto level = state.range(0);
	ZSTD_DStream* dstream = ZSTD_createDStream();
	ZSTD_initDStream(dstream);
	for (auto _ : state) {
		benchmark::DoNotOptimize(decompressAsStream(COMPRESSED[level], dstream));
	}
	ZSTD_freeDStream(dstream);
	state.SetBytesProcessed(UNCOMPRESSED.size() * static_cast<long>(state.iterations()));
}

// chunk size from 4K to 1MB, compression level from 1, 3, 9
BENCHMARK(bench_zstd_compress)
    ->Args({ 1 << 12, 1 })
    ->Args({ 1 << 18, 1 })
    ->Args({ 1 << 20, 1 })
    ->Args({ 1 << 21, 1 })
    ->Args({ 1 << 22, 1 })
    ->Args({ 1 << 23, 1 })
    ->Args({ 1 << 12, 3 })
    ->Args({ 1 << 14, 3 })
    ->Args({ 1 << 20, 3 })
    ->Args({ 1 << 12, 9 })
    ->Args({ 1 << 14, 9 })
    ->Args({ 1 << 20, 9 });

BENCHMARK(bench_zstd_compress2)->Args({ 1 << 18, 1 })->Args({ 1 << 20, 1 });

BENCHMARK(bench_zstd_compress_stream)
    ->Args({ 1 << 12, 1 })
    ->Args({ 1 << 18, 1 })
    ->Args({ 1 << 20, 1 })
    ->Args({ 1 << 21, 1 })
    ->Args({ 1 << 22, 1 })
    ->Args({ 1 << 23, 1 })
    ->Args({ 1 << 12, 3 })
    ->Args({ 1 << 14, 3 })
    ->Args({ 1 << 20, 3 })
    ->Args({ 1 << 12, 9 })
    ->Args({ 1 << 14, 9 })
    ->Args({ 1 << 20, 9 });

BENCHMARK(bench_zstd_decompress)->Arg(1)->Arg(3)->Arg(9);
BENCHMARK(bench_zstd_decompress_stream)->Arg(1)->Arg(3)->Arg(9);
#endif
