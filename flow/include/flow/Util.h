/*
 * Util.h
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

#ifndef _FLOW_UTIL_H_
#define _FLOW_UTIL_H_

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <iosfwd>
#include <string>

// Read key/value pairs from stream. The stream is constituted by lines of text.
// Each line contains a pair of key/value, separated by space/tab. e.g.
//
// Key1   Value1      tailing characters
// Key2   Value2      tailing characters
//
// The tailing characters will be ignored.
//
// K and V should have
//
//   std::istream& operator>>(std::istream&, K&);
//
// implemented.
template <typename K, typename V>
void keyValueReader(std::istream& stream, std::function<bool(const K&, const V&)> consumer) {
	std::stringstream lineParser;
	std::string line;
	K key;
	V value;
	while (std::getline(stream, line)) {
		lineParser.clear();
		lineParser.str(std::move(line));
		try {
			lineParser >> key >> value;
		} catch (std::ios_base::failure&) {
			continue;
		}
		if (lineParser.fail() || lineParser.bad()) {
			continue;
		}
		if (!consumer(key, value)) {
			break;
		}
	}
}

template <typename C>
void swapAndPop(C* container, int index) {
	if (index != container->size() - 1) {
		std::swap((*container)[index], container->back());
	}

	container->pop_back();
}

// Adds n to pCount upon construction, subtracts in upon destruction
template <typename T>
struct Hold {
	Hold(T* pCount = nullptr, T n = 1) : pCount(pCount), n(n) {
		if (pCount != nullptr) {
			*pCount += n;
		}
	}
	~Hold() {
		if (pCount != nullptr) {
			*pCount -= n;
		}
	}

	Hold(Hold&& other) {
		pCount = other.pCount;
		other.pCount = nullptr;
		n = other.n;
	}

	Hold& operator=(Hold&& other) {
		if (pCount != nullptr) {
			*pCount -= n;
		}
		pCount = other.pCount;
		other.pCount = nullptr;
		n = other.n;
		return *this;
	};

	void release() {
		if (pCount != nullptr) {
			*pCount -= n;
			pCount = nullptr;
		}
	}

	T* pCount;
	T n;

	void operator=(const Hold& other) = delete;
};

// Format byte count as human-readable string (e.g., "1.23 TB", "456.78 GB", "789.01 MB")
// Uses binary units (1 KB = 1024 bytes)
inline std::string formatBytesHumanReadable(int64_t bytes) {
	char buf[64];
	if (bytes >= 1099511627776LL) { // TB
		std::snprintf(buf, sizeof(buf), "%.2f TB", bytes / 1099511627776.0);
	} else if (bytes >= 1073741824LL) { // GB
		std::snprintf(buf, sizeof(buf), "%.2f GB", bytes / 1073741824.0);
	} else if (bytes >= 1048576LL) { // MB
		std::snprintf(buf, sizeof(buf), "%.2f MB", bytes / 1048576.0);
	} else if (bytes >= 1024LL) { // KB
		std::snprintf(buf, sizeof(buf), "%.2f KB", bytes / 1024.0);
	} else {
		std::snprintf(buf, sizeof(buf), "%lld bytes", static_cast<long long>(bytes));
	}
	return std::string(buf);
}

// Format duration in seconds as human-readable string (e.g., "2 hours 30 minutes", "45 minutes", "30 seconds")
inline std::string formatDurationHumanReadable(int seconds) {
	char buf[64];
	if (seconds >= 3600) {
		int hours = seconds / 3600;
		int mins = (seconds % 3600) / 60;
		if (mins > 0) {
			std::snprintf(buf, sizeof(buf), "%d hours %d minutes", hours, mins);
		} else {
			std::snprintf(buf, sizeof(buf), "%d hours", hours);
		}
	} else if (seconds >= 60) {
		std::snprintf(buf, sizeof(buf), "%d minutes", seconds / 60);
	} else {
		std::snprintf(buf, sizeof(buf), "%d seconds", seconds);
	}
	return std::string(buf);
}

#endif // _FLOW_UTIL_H_
