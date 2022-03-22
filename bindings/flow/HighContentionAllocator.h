/*
 * HighContentionAllocator.h
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

#ifndef FDB_FLOW_HIGH_CONTENTION_ALLOCATOR_H
#define FDB_FLOW_HIGH_CONTENTION_ALLOCATOR_H

#pragma once

#include "Subspace.h"

namespace FDB {
class HighContentionAllocator {
public:
	HighContentionAllocator(Subspace subspace) : counters(subspace.get(0)), recent(subspace.get(1)) {}
	Future<Standalone<StringRef>> allocate(Reference<Transaction> const& tr) const;

	static int64_t windowSize(int64_t start);

private:
	Subspace counters;
	Subspace recent;
};
} // namespace FDB

#endif
