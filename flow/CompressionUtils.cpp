/*
 * CompressionUtils.cpp
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

#include "flow/CompressionUtils.h"

#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"

#include <boost/iostreams/copy.hpp>
#ifdef ZLIB_LIB_SUPPORTED
#include <boost/iostreams/filter/gzip.hpp>
#endif
#include <boost/iostreams/filtering_streambuf.hpp>
#include <sstream>

StringRef CompressionUtils::compress(const CompressionFilter filter, const StringRef& data, Arena& arena) {
	if (filter == CompressionFilter::NONE) {
		return StringRef(arena, data);
	}

	namespace bio = boost::iostreams;
#ifdef ZLIB_LIB_SUPPORTED
	if (filter == CompressionFilter::GZIP) {
		return CompressionUtils::compress(filter, data, bio::gzip::default_compression, arena);
	}
#endif
	throw not_implemented();
}

StringRef CompressionUtils::compress(const CompressionFilter filter, const StringRef& data, int level, Arena& arena) {
	ASSERT(filter < CompressionFilter::LAST);

	if (filter == CompressionFilter::NONE) {
		return StringRef(arena, data);
	}

	namespace bio = boost::iostreams;
	std::stringstream compStream;
	std::stringstream decomStream(data.toString());

	bio::filtering_streambuf<bio::input> out;
#ifdef ZLIB_LIB_SUPPORTED
	if (filter == CompressionFilter::GZIP) {
		out.push(bio::gzip_compressor(bio::gzip_params(level)));
	}
#endif
	out.push(decomStream);
	bio::copy(out, compStream);

	return StringRef(arena, compStream.str());
}

StringRef CompressionUtils::decompress(const CompressionFilter filter, const StringRef& data, Arena& arena) {
	ASSERT(filter < CompressionFilter::LAST);

	if (filter == CompressionFilter::NONE) {
		return StringRef(arena, data);
	}

	namespace bio = boost::iostreams;
	std::stringstream compStream(data.toString());
	std::stringstream decompStream;

	bio::filtering_streambuf<bio::input> out;
#ifdef ZLIB_LIB_SUPPORTED
	if (filter == CompressionFilter::GZIP) {
		out.push(bio::gzip_decompressor());
	}
#endif
	out.push(compStream);
	bio::copy(out, decompStream);

	return StringRef(arena, decompStream.str());
}

// Only used to link unit tests
void forceLinkCompressionUtilsTest() {}

TEST_CASE("/CompressionUtils/noCompression") {
	Arena arena;
	const int size = deterministicRandom()->randomInt(512, 1024);
	Standalone<StringRef> uncompressed = makeString(size);
	generateRandomData(mutateString(uncompressed), size);

	Standalone<StringRef> compressed = CompressionUtils::compress(CompressionFilter::NONE, uncompressed, arena);
	ASSERT_EQ(compressed.compare(uncompressed), 0);

	StringRef verify = CompressionUtils::decompress(CompressionFilter::NONE, compressed, arena);
	ASSERT_EQ(verify.compare(uncompressed), 0);

	TraceEvent("NoCompression_Done").log();

	return Void();
}

#ifdef ZLIB_LIB_SUPPORTED
TEST_CASE("/CompressionUtils/gzipCompression") {
	Arena arena;
	const int size = deterministicRandom()->randomInt(512, 1024);
	Standalone<StringRef> uncompressed = makeString(size);
	generateRandomData(mutateString(uncompressed), size);

	Standalone<StringRef> compressed = CompressionUtils::compress(CompressionFilter::GZIP, uncompressed, arena);
	ASSERT_NE(compressed.compare(uncompressed), 0);

	StringRef verify = CompressionUtils::decompress(CompressionFilter::GZIP, compressed, arena);
	ASSERT_EQ(verify.compare(uncompressed), 0);

	TraceEvent("GzipCompression_Done").log();

	return Void();
}
#endif
