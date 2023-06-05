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

class ApiRequest {
public:
	ApiRequest() = default;
	ApiRequest(const ApiRequest& r) = default;
	ApiRequest(ApiRequest&& r) noexcept = default;
	ApiRequest& operator=(const ApiRequest& r) = default;
	ApiRequest& operator=(ApiRequest&& r) noexcept = default;

	bool hasValidHeader() const {
		return reqRef.isValid() && reqRef.getPtr()->header != nullptr &&
		       reqRef.getPtr()->header->allocator.ifc != nullptr &&
		       reqRef.getPtr()->header->allocator.handle != nullptr;
	}

	bool isAllocatorCompatible(FDBAllocatorIfc* alloc) const {
		ASSERT(reqRef.isValid());
		return getAllocatorInterface() == alloc;
	}

	int32_t getType() const {
		ASSERT(reqRef.isValid());
		return reqRef.getPtr()->header->request_type;
	}

	FDBAllocatorIfc* getAllocatorInterface() const {
		ASSERT(reqRef.isValid());
		return reqRef.getPtr()->header->allocator.ifc;
	}

	template <class RequestType>
	RequestType* getTypedRequest() const {
		return reinterpret_cast<RequestType*>(reqRef.getPtr());
	}

	FDBRequest* getFDBRequest() const { return reqRef.getPtr(); }

	static ApiRequest addRef(FDBRequest* response) {
		ASSERT(response != NULL);
		return ApiRequest(Reference<FDBRequestRefCounted>::addRef((FDBRequestRefCounted*)response));
	}

private:
	struct FDBRequestRefCounted : public FDBRequest {
		void addref() { header->allocator.ifc->addref(header->allocator.handle); }
		void delref() { header->allocator.ifc->delref(header->allocator.handle); }
	};

	Reference<FDBRequestRefCounted> reqRef;

	ApiRequest(Reference<FDBRequestRefCounted>&& ref) : reqRef(ref) {}
};

class ApiResponse {
public:
	ApiResponse() = default;
	ApiResponse(const ApiResponse& r) = default;
	ApiResponse(ApiResponse&& r) noexcept = default;
	ApiResponse& operator=(const ApiResponse& r) = default;
	ApiResponse& operator=(ApiResponse&& r) noexcept = default;

	template <class ResponseType>
	ResponseType* getTypedResponse() const {
		return reinterpret_cast<ResponseType*>(respRef.getPtr());
	}

	FDBResponse* getFDBResponse() const { return respRef.getPtr(); }

	Arena& arena() {
		ASSERT(respRef.isValid());
		static_assert(sizeof(Arena) == sizeof(void*));
		return *reinterpret_cast<Arena*>(&respRef.getPtr()->header->allocator.handle);
	}

	static ApiResponse addRef(FDBResponse* response) {
		ASSERT(response != NULL);
		return ApiResponse(Reference<FDBResponseRefCounted>::addRef((FDBResponseRefCounted*)response));
	}

	template <class ResponseType>
	static ApiResponse create(const ApiRequest& req) {
		Arena arena(sizeof(ResponseType) + sizeof(FDBResponseHeader));
		ResponseType* resp = new (arena) ResponseType();
		resp->header = new (arena) FDBResponseHeader();
		resp->header->request_type = req.getType();
		resp->header->allocator.handle = arena.getPtr();
		resp->header->allocator.ifc = req.getAllocatorInterface();
		return addRef((FDBResponse*)resp);
	}

private:
	struct FDBResponseRefCounted : public FDBResponse {
		void addref() { header->allocator.ifc->addref(header->allocator.handle); }
		void delref() { header->allocator.ifc->delref(header->allocator.handle); }
	};

	Reference<FDBResponseRefCounted> respRef;

	ApiResponse(Reference<FDBResponseRefCounted>&& ref) : respRef(ref) {}
};

#endif