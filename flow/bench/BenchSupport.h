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

#ifndef FDB_FLOW_BENCH_SUPPORT_H
#define FDB_FLOW_BENCH_SUPPORT_H

#pragma once

#include "flow/Arena.h"
#include "flow/FastAlloc.h"
#include "flow/IRandom.h"
#include "flow/flow.h"

static constexpr size_t flowBenchDataSize = 1 << 20;

inline uint8_t* flowBenchData() {
	static uint8_t* globalData = nullptr;
	if (!globalData) {
		globalData = static_cast<uint8_t*>(allocateFast(flowBenchDataSize));
	}
	deterministicRandom()->randomBytes(globalData, flowBenchDataSize);
	return globalData;
}

inline StringRef getString(size_t size) {
	ASSERT(size <= flowBenchDataSize);
	return StringRef(flowBenchData(), size);
}

#endif
