/*
 * ApiRequest.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
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
