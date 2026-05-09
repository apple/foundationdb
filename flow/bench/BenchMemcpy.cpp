/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <string>

#include "benchmark/benchmark.h"

#include "flow/IRandom.h"
#include "flow/Platform.h"

void* rte_memcpy_noinline(void* dst, const void* src, size_t length);

namespace {

constexpr size_t kSmallBufferSize = 8192;
constexpr size_t kLargeBufferSize = 100 * 1024 * 1024;
constexpr size_t kAlignmentUnit = 64;
constexpr size_t kAddressCount = 1 << 16;

constexpr std::array<size_t, 63> kBufferSizes = { 1,    2,    3,    4,    5,    6,    7,    8,    9,    12,   15,
	                                              16,   17,   31,   32,   33,   63,   64,   65,   127,  128,  129,
	                                              191,  192,  193,  255,  256,  257,  319,  320,  321,  383,  384,
	                                              385,  447,  448,  449,  511,  512,  513,  767,  768,  769,  1023,
	                                              1024, 1025, 1518, 1522, 1536, 1600, 2048, 2560, 3072, 3584, 4096,
	                                              4608, 5120, 5632, 6144, 6656, 7168, 7680, 8192 };

enum class CopyFunction {
	Rte,
	Memcpy,
};

enum class CacheMode {
	CacheToCache,
	CacheToMem,
	MemToCache,
	MemToMem,
};

enum class CopyAlignment {
	Aligned,
	Unaligned,
};

static size_t roundUp(size_t size, size_t alignment) {
	return (((size - 1) / alignment) + 1) * alignment;
}

struct AlignedFree {
	void operator()(uint8_t* ptr) const { aligned_free(ptr); }
};

class AlignedBuffer {
public:
	explicit AlignedBuffer(size_t size)
	  : ptr(static_cast<uint8_t*>(aligned_alloc(kAlignmentUnit, roundUp(size, kAlignmentUnit)))) {
		if (ptr == nullptr) {
			throw std::bad_alloc();
		}
	}

	uint8_t* get() { return ptr.get(); }
	const uint8_t* get() const { return ptr.get(); }

private:
	std::unique_ptr<uint8_t, AlignedFree> ptr;
};

class MemcpyBuffers {
public:
	MemcpyBuffers()
	  : largeRead(kLargeBufferSize + kAlignmentUnit), largeWrite(kLargeBufferSize + kAlignmentUnit),
	    smallRead(kSmallBufferSize + kAlignmentUnit), smallWrite(kSmallBufferSize + kAlignmentUnit) {
		deterministicRandom()->randomBytes(largeRead.get(), static_cast<int>(kLargeBufferSize));
		deterministicRandom()->randomBytes(smallRead.get(), static_cast<int>(kSmallBufferSize));

		std::memset(largeWrite.get(), 0, kLargeBufferSize);
		std::memset(smallWrite.get(), 0, kSmallBufferSize);

		for (size_t i = 0; i < kAddressCount; ++i) {
			largeReadOffsets[i] = randomLargeBufferOffset();
			largeWriteOffsets[i] = randomLargeBufferOffset();
		}
	}

	uint8_t* dstBuffer(bool cached) { return cached ? smallWrite.get() : largeWrite.get(); }
	const uint8_t* srcBuffer(bool cached) const { return cached ? smallRead.get() : largeRead.get(); }

	size_t dstOffset(bool cached, size_t index, size_t unalignedOffset) const {
		return cached ? unalignedOffset : largeWriteOffsets[index] + unalignedOffset;
	}

	size_t srcOffset(bool cached, size_t index, size_t unalignedOffset) const {
		return cached ? unalignedOffset : largeReadOffsets[index] + unalignedOffset;
	}

private:
	static size_t randomLargeBufferOffset() {
		return (deterministicRandom()->randomUInt32() % (kLargeBufferSize - kSmallBufferSize)) & ~(kAlignmentUnit - 1);
	}

	AlignedBuffer largeRead;
	AlignedBuffer largeWrite;
	AlignedBuffer smallRead;
	AlignedBuffer smallWrite;
	std::array<size_t, kAddressCount> largeReadOffsets;
	std::array<size_t, kAddressCount> largeWriteOffsets;
};

static MemcpyBuffers& memcpyBuffers() {
	static MemcpyBuffers buffers;
	return buffers;
}

template <CacheMode Mode>
constexpr bool isDstCached() {
	return Mode == CacheMode::CacheToCache || Mode == CacheMode::MemToCache;
}

template <CacheMode Mode>
constexpr bool isSrcCached() {
	return Mode == CacheMode::CacheToCache || Mode == CacheMode::CacheToMem;
}

template <CopyFunction Function, size_t ConstantSize>
void copy(uint8_t* dst, const uint8_t* src, size_t size) {
	benchmark::DoNotOptimize(dst);
	benchmark::DoNotOptimize(src);
	if constexpr (Function == CopyFunction::Rte) {
		if constexpr (ConstantSize != 0) {
			benchmark::DoNotOptimize(rte_memcpy_noinline(dst, src, ConstantSize));
		} else {
			benchmark::DoNotOptimize(rte_memcpy_noinline(dst, src, size));
		}
	} else {
		if constexpr (ConstantSize != 0) {
			benchmark::DoNotOptimize(std::memcpy(dst, src, ConstantSize));
		} else {
			benchmark::DoNotOptimize(std::memcpy(dst, src, size));
		}
	}
	benchmark::ClobberMemory();
}

template <CopyFunction Function, CacheMode Mode, CopyAlignment Alignment, size_t ConstantSize>
static void benchMemcpy(benchmark::State& state) {
	const size_t size = ConstantSize == 0 ? state.range(0) : ConstantSize;
	constexpr size_t dstUnalignedOffset = Alignment == CopyAlignment::Aligned ? 0 : 1;
	constexpr size_t srcUnalignedOffset = Alignment == CopyAlignment::Aligned ? 0 : 5;
	constexpr bool dstCached = isDstCached<Mode>();
	constexpr bool srcCached = isSrcCached<Mode>();
	auto& buffers = memcpyBuffers();
	size_t addressIndex = 0;

	for (auto _ : state) {
		const size_t index = addressIndex & (kAddressCount - 1);
		auto* dst = buffers.dstBuffer(dstCached) + buffers.dstOffset(dstCached, index, dstUnalignedOffset);
		const auto* src = buffers.srcBuffer(srcCached) + buffers.srcOffset(srcCached, index, srcUnalignedOffset);
		copy<Function, ConstantSize>(dst, src, size);
		++addressIndex;
	}

	state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
	state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(size));
}

template <CopyFunction Function, CacheMode Mode, CopyAlignment Alignment>
static void benchMemcpyVariable(benchmark::State& state) {
	benchMemcpy<Function, Mode, Alignment, 0>(state);
}

template <CopyFunction Function, CacheMode Mode, CopyAlignment Alignment, size_t Size>
static void benchMemcpyConstant(benchmark::State& state) {
	benchMemcpy<Function, Mode, Alignment, Size>(state);
}

template <CopyFunction Function, CacheMode Mode, CopyAlignment Alignment>
static void registerVariableBenchmark(const std::string& name) {
	auto* registeredBenchmark =
	    benchmark::RegisterBenchmark(name.c_str(), &benchMemcpyVariable<Function, Mode, Alignment>);
	for (const auto size : kBufferSizes) {
		registeredBenchmark->Arg(size);
	}
	registeredBenchmark->MinTime(0.01);
}

template <CopyFunction Function, CacheMode Mode, CopyAlignment Alignment, size_t Size>
static void registerConstantBenchmark(const std::string& name) {
	benchmark::RegisterBenchmark(name.c_str(), &benchMemcpyConstant<Function, Mode, Alignment, Size>)->MinTime(0.01);
}

template <CopyFunction Function, CacheMode Mode, CopyAlignment Alignment>
static void registerConstantBenchmarks(const std::string& prefix) {
	registerConstantBenchmark<Function, Mode, Alignment, 6>(prefix + "/6");
	registerConstantBenchmark<Function, Mode, Alignment, 64>(prefix + "/64");
	registerConstantBenchmark<Function, Mode, Alignment, 128>(prefix + "/128");
	registerConstantBenchmark<Function, Mode, Alignment, 192>(prefix + "/192");
	registerConstantBenchmark<Function, Mode, Alignment, 256>(prefix + "/256");
	registerConstantBenchmark<Function, Mode, Alignment, 512>(prefix + "/512");
	registerConstantBenchmark<Function, Mode, Alignment, 768>(prefix + "/768");
	registerConstantBenchmark<Function, Mode, Alignment, 1024>(prefix + "/1024");
	registerConstantBenchmark<Function, Mode, Alignment, 1536>(prefix + "/1536");
}

template <CopyFunction Function, CacheMode Mode, CopyAlignment Alignment>
static void registerMemcpyBenchmarksForCase(const std::string& functionName,
                                            const std::string& cacheModeName,
                                            const std::string& alignmentName) {
	const auto prefix = "Memcpy/" + functionName + "/" + alignmentName + "/" + cacheModeName;
	registerVariableBenchmark<Function, Mode, Alignment>(prefix + "/variable");
	registerConstantBenchmarks<Function, Mode, Alignment>(prefix + "/constant");
}

template <CopyFunction Function, CacheMode Mode>
static void registerMemcpyBenchmarksForCacheMode(const std::string& functionName, const std::string& cacheModeName) {
	registerMemcpyBenchmarksForCase<Function, Mode, CopyAlignment::Aligned>(functionName, cacheModeName, "aligned");
	registerMemcpyBenchmarksForCase<Function, Mode, CopyAlignment::Unaligned>(functionName, cacheModeName, "unaligned");
}

template <CopyFunction Function>
static void registerMemcpyBenchmarksForFunction(const std::string& functionName) {
	registerMemcpyBenchmarksForCacheMode<Function, CacheMode::CacheToCache>(functionName, "cache_to_cache");
	registerMemcpyBenchmarksForCacheMode<Function, CacheMode::CacheToMem>(functionName, "cache_to_mem");
	registerMemcpyBenchmarksForCacheMode<Function, CacheMode::MemToCache>(functionName, "mem_to_cache");
	registerMemcpyBenchmarksForCacheMode<Function, CacheMode::MemToMem>(functionName, "mem_to_mem");
}

static bool registerMemcpyBenchmarks() {
	registerMemcpyBenchmarksForFunction<CopyFunction::Rte>("rte_memcpy");
	registerMemcpyBenchmarksForFunction<CopyFunction::Memcpy>("memcpy");
	return true;
}

[[maybe_unused]] const bool memcpyBenchmarksRegistered = registerMemcpyBenchmarks();

} // namespace
