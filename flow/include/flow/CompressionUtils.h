/*
 * CompressionUtils.h
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

#ifndef FLOW_COMPRESSION_UTILS_H
#define FLOW_COMPRESSION_UTILS_H
#pragma once

#include "flow/Arena.h"

#include <unordered_set>

enum class CompressionFilter {
	NONE,
	ZSTD,
	LAST // Always the last member
};

struct CompressionUtils {
	static StringRef compress(const CompressionFilter filter, const StringRef& data, Arena& arena);
	static StringRef compress(const CompressionFilter filter, const StringRef& data, int level, Arena& arena);
	static StringRef decompress(const CompressionFilter filter, const StringRef& data, Arena& arena);

	static int getDefaultCompressionLevel(CompressionFilter filter);
	static CompressionFilter getRandomFilter();

	static CompressionFilter fromFilterString(const std::string& filter) {
		if (filter == "NONE") {
			return CompressionFilter::NONE;
		} else if (filter == "ZSTD") {
			return CompressionFilter::ZSTD;
		} else {
			throw not_implemented();
		}
	}

	static std::string toString(const CompressionFilter filter) {
		if (filter == CompressionFilter::NONE) {
			return "NONE";
		} else if (filter == CompressionFilter::ZSTD) {
			return "ZSTD";
		} else {
			throw not_implemented();
		}
	}

	static void checkFilterSupported(const CompressionFilter filter) {
		if (CompressionUtils::supportedFilters.find(filter) == CompressionUtils::supportedFilters.end()) {
			throw not_implemented();
		}
	}

	static std::unordered_set<CompressionFilter> supportedFilters;
};

#endif // FLOW_COMPRRESSION_UTILS_H
