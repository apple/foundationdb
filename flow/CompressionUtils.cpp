/*
 * CompressionUtils.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "flow/CompressionUtils.h"

#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"

#ifdef ZSTD_LIB_SUPPORTED
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
static constexpr int ZSTD_COMPRESSION_LEVEL_1 = 1;
#endif

namespace {
std::unordered_set<CompressionFilter> getSupportedFilters() {
	std::unordered_set<CompressionFilter> filters;

	filters.insert(CompressionFilter::NONE);
#ifdef ZSTD_LIB_SUPPORTED
	filters.insert(CompressionFilter::ZSTD);
#endif
	ASSERT_GE(filters.size(), 1);
	return filters;
}
} // namespace

std::unordered_set<CompressionFilter> CompressionUtils::supportedFilters = getSupportedFilters();

StringRef CompressionUtils::compress(const CompressionFilter filter, const StringRef& data, Arena& arena) {
	checkFilterSupported(filter);

	if (filter == CompressionFilter::NONE) {
		return StringRef(arena, data);
	}
#ifdef ZSTD_LIB_SUPPORTED
	if (filter == CompressionFilter::ZSTD) {
		return CompressionUtils::compress(filter, data, ZSTD_COMPRESSION_LEVEL_1, arena);
	}
#endif

	throw internal_error(); // We should never get here
}

StringRef CompressionUtils::compress(const CompressionFilter filter, const StringRef& data, int level, Arena& arena) {
	checkFilterSupported(filter);

	if (filter == CompressionFilter::NONE) {
		return StringRef(arena, data);
	}
#ifdef ZSTD_LIB_SUPPORTED
	if (filter == CompressionFilter::ZSTD) {
		const char* src = reinterpret_cast<const char*>(data.begin());
		size_t destSize = ZSTD_compressBound(data.size());
		std::unique_ptr<uint8_t[]> dest = std::make_unique<uint8_t[]>(destSize);
		size_t bytes = ZSTD_compress(dest.get(), destSize, src, data.size(), level);
		if (ZSTD_isError(bytes)) {
			throw internal_error();
		}
		return StringRef(arena, StringRef(dest.get(), bytes));
	}
#endif
	throw internal_error(); // We should never get here
}

StringRef CompressionUtils::decompress(const CompressionFilter filter, const StringRef& data, Arena& arena) {
	checkFilterSupported(filter);

	if (filter == CompressionFilter::NONE) {
		return StringRef(arena, data);
	}
#ifdef ZSTD_LIB_SUPPORTED
	if (filter == CompressionFilter::ZSTD) {
		const char* src = reinterpret_cast<const char*>(data.begin());
		size_t destSize = ZSTD_decompressBound(src, data.size());
		std::unique_ptr<uint8_t[]> dest = std::make_unique<uint8_t[]>(destSize);
		size_t bytes = ZSTD_decompress(dest.get(), destSize, src, data.size());
		if (ZSTD_isError(bytes)) {
			throw internal_error();
		}
		return StringRef(arena, StringRef(dest.get(), bytes));
	}
#endif
	throw internal_error(); // We should never get here
}

int CompressionUtils::getDefaultCompressionLevel(CompressionFilter filter) {
	checkFilterSupported(filter);

	if (filter == CompressionFilter::NONE) {
		return -1;
	}

#ifdef ZSTD_LIB_SUPPORTED
	if (filter == CompressionFilter::ZSTD) {
		// optimize for high speed compression, larger levels have a high cpu cost and not much compression ratio
		// improvement, according to benchmarks
		return ZSTD_COMPRESSION_LEVEL_1;
	}
#endif

	throw internal_error(); // We should never get here
}

CompressionFilter CompressionUtils::getRandomFilter() {
	ASSERT_GE(supportedFilters.size(), 1);
	std::vector<CompressionFilter> filters;
	filters.insert(filters.end(), CompressionUtils::supportedFilters.begin(), CompressionUtils::supportedFilters.end());

	ASSERT_GE(filters.size(), 1);

	CompressionFilter res;
	if (filters.size() == 1) {
		res = filters[0];
	} else {
		int idx = deterministicRandom()->randomInt(0, filters.size());
		res = filters[idx];
	}

	ASSERT(supportedFilters.find(res) != supportedFilters.end());
	return res;
}

// Only used to link unit tests
void forceLinkCompressionUtilsTest() {}

namespace {
void testCompression(CompressionFilter filter) {
	Arena arena;
	const int size = deterministicRandom()->randomInt(512, 1024);
	Standalone<StringRef> uncompressed = makeString(size);
	deterministicRandom()->randomBytes(mutateString(uncompressed), size);

	Standalone<StringRef> compressed = CompressionUtils::compress(filter, uncompressed, arena);
	ASSERT_NE(compressed.compare(uncompressed), 0);

	StringRef verify = CompressionUtils::decompress(filter, compressed, arena);
	ASSERT_EQ(verify.compare(uncompressed), 0);
}

void testCompression2(CompressionFilter filter) {
	Arena arena;
	const int size = deterministicRandom()->randomInt(512, 1024);
	std::string s(size, 'x');
	Standalone<StringRef> uncompressed = Standalone<StringRef>(StringRef(s));
	printf("Size before: %d\n", (int)uncompressed.size());

	Standalone<StringRef> compressed = CompressionUtils::compress(filter, uncompressed, arena);
	ASSERT_NE(compressed.compare(uncompressed), 0);
	printf("Size after: %d\n", (int)compressed.size());
	// Assert compressed size is less than half.
	ASSERT(compressed.size() * 2 < uncompressed.size());

	StringRef verify = CompressionUtils::decompress(filter, compressed, arena);
	ASSERT_EQ(verify.compare(uncompressed), 0);
}

} // namespace

TEST_CASE("/CompressionUtils/noCompression") {
	Arena arena;
	const int size = deterministicRandom()->randomInt(512, 1024);
	Standalone<StringRef> uncompressed = makeString(size);
	deterministicRandom()->randomBytes(mutateString(uncompressed), size);

	Standalone<StringRef> compressed = CompressionUtils::compress(CompressionFilter::NONE, uncompressed, arena);
	ASSERT_EQ(compressed.compare(uncompressed), 0);

	StringRef verify = CompressionUtils::decompress(CompressionFilter::NONE, compressed, arena);
	ASSERT_EQ(verify.compare(uncompressed), 0);

	TraceEvent("NoCompressionDone");

	return Void();
}
#ifdef ZSTD_LIB_SUPPORTED
TEST_CASE("/CompressionUtils/zstdCompression") {
	testCompression(CompressionFilter::ZSTD);
	TraceEvent("ZstdCompressionDone");

	return Void();
}

TEST_CASE("/CompressionUtils/zstdCompression2") {
	testCompression2(CompressionFilter::ZSTD);
	TraceEvent("ZstdCompression2Done");

	return Void();
}
#endif
