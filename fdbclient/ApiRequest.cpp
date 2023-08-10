/*
 * ApiRequest.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ApiRequest.h"
#include "fdbclient/FDBTypes.h"
#include "foundationdb/fdb_c_requests.h"

namespace {

void* allocatorAllocate(void** handle, uint64_t sz) {
	Reference<ArenaBlock> ref((ArenaBlock*)*handle);
	void* res = ArenaBlock::allocate(ref, sz);
	*handle = ref.extractPtr();
	return res;
}

void allocatorAddRef(void* handle) {
	((ArenaBlock*)handle)->addref();
}

void allocatorDelRef(void* handle) {
	((ArenaBlock*)handle)->delref();
}

} // namespace

FDBAllocatorIfc* localAllocatorInterface() {
	static FDBAllocatorIfc interface = { allocatorAllocate, allocatorAddRef, allocatorDelRef };
	return &interface;
}
