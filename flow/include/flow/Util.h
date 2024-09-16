/*
 * Util.h
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

#ifndef _FLOW_UTIL_H_
#define _FLOW_UTIL_H_

#pragma once

#include <algorithm>
#include <functional>
#include <iosfwd>

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

#endif // _FLOW_UTIL_H_
