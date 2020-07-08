/*
 * Util.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/Arena.h"

template <typename C>
void swapAndPop(C* container, int index) {
	if (index != container->size()-1) {
		using std::swap;
		swap((*container)[index], container->back());
	}

	container->pop_back();
}

typedef uint64_t Word;
// Get the number of prefix bytes that are the same between a and b, up to their common length of cl
static inline int commonPrefixLength(uint8_t const* ap, uint8_t const* bp, int cl) {
	int i = 0;
	const int wordEnd = cl - sizeof(Word) + 1;

	for (; i < wordEnd; i += sizeof(Word)) {
		Word a = *(Word*)ap;
		Word b = *(Word*)bp;
		if (a != b) {
			return i + ctzll(a ^ b) / 8;
		}
		ap += sizeof(Word);
		bp += sizeof(Word);
	}

	for (; i < cl; i++) {
		if (*ap != *bp) {
			return i;
		}
		++ap;
		++bp;
	}
	return cl;
}

static inline int commonPrefixLength(const StringRef& a, const StringRef& b) {
	return commonPrefixLength(a.begin(), b.begin(), std::min(a.size(), b.size()));
}

static inline int commonPrefixLength(const StringRef& a, const StringRef& b, int skipLen) {
	return commonPrefixLength(a.begin() + skipLen, b.begin() + skipLen, std::min(a.size(), b.size()) - skipLen);
}

#endif  // _FLOW_UTIL_H_
