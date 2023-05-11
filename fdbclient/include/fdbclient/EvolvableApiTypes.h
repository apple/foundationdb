/*
 * EvolvableApiTypes.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef __FDBCLIENT_EVOLVABLE_API_TYPES_H__
#define __FDBCLIENT_EVOLVABLE_API_TYPES_H__
#pragma once

#include "flow/Arena.h"
#include "foundationdb/fdb_c_evolvable_internal.h"

struct ClientHandle;

struct ApiRequestRef {
	FDBRequest* request;
	ApiRequestRef(FDBRequest* request) : request(request) {}
	ApiRequestRef() = default;

	bool isValid() const {
		return request != nullptr && request->header != nullptr && request->header->allocator.ifc != nullptr &&
		       request->header->allocator.handle != nullptr;
	}

	bool isAllocatorCompatible(FDBAllocatorIfc* alloc) const { return getAllocatorInterface() == alloc; }

	Arena getArena() const { return Arena::addRef((ArenaBlock*)request->header->allocator.handle); }

	int32_t getType() const { return request->header->request_type; }

	FDBAllocatorIfc* getAllocatorInterface() const { return request->header->allocator.ifc; }

	template <class RequestType>
	RequestType* getRequest() {
		return reinterpret_cast<RequestType*>(request);
	}
};

struct ApiResponseRef {
	FDBResponse* response;
	ApiResponseRef(FDBResponse* response) : response(response) {}
	ApiResponseRef() = default;

	template <class ResponseType>
	ResponseType* getResponse() {
		return reinterpret_cast<ResponseType*>(response);
	}
};

using ApiRequest = Standalone<ApiRequestRef>;
using ApiResponse = Standalone<ApiResponseRef>;

#endif