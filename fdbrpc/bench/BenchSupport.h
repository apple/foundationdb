/*
 * BenchSupport.h
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

#ifndef FDB_FDBRPC_BENCH_SUPPORT_H
#define FDB_FDBRPC_BENCH_SUPPORT_H

#pragma once

#include <vector>

#include "flow/flow.h"

template <class T>
struct InputGenerator {
	InputGenerator() = default;

	template <class Fn>
	InputGenerator(int n, Fn genFunc) : lastIndex(-1) {
		ASSERT(n > 0);
		data.reserve(n);
		for (int i = 0; i < n; ++i) {
			data.push_back(genFunc());
		}
	}

	const T& next() {
		if (++lastIndex == data.size()) {
			lastIndex = 0;
		}
		return data[lastIndex];
	}

	std::vector<T> data;
	int lastIndex;
};

#endif
