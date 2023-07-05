/*
 * ApiRequest.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef __FDBCLIENT_API_REQUEST_H__
#define __FDBCLIENT_API_REQUEST_H__

#pragma once

#include "foundationdb/fdb_c_internal.h"
#include "foundationdb/fdb_c_requests.h"
#include "flow/Arena.h"

FDBAllocatorIfc* localAllocatorInterface();

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

	static ApiRequest addRef(FDBRequest* request) {
		ASSERT(request != NULL);
		return ApiRequest(Reference<FDBRequestRefCounted>::addRef((FDBRequestRefCounted*)request));
	}

private:
	struct FDBRequestRefCounted : public FDBRequest {
		void addref() { header->allocator.ifc->addref(header->allocator.handle); }
		void delref() { header->allocator.ifc->delref(header->allocator.handle); }
	};

	Reference<FDBRequestRefCounted> reqRef;

	ApiRequest(Reference<FDBRequestRefCounted>&& ref) : reqRef(ref) {}
};

class ApiResult {
public:
	ApiResult() = default;
	ApiResult(const ApiResult& r) = default;
	ApiResult(ApiResult&& r) noexcept = default;
	ApiResult& operator=(const ApiResult& r) = default;
	ApiResult& operator=(ApiResult&& r) noexcept = default;

	FDBResult* getPtr() const { return resRef.getPtr(); }
	FDBResult* extractPtr() { return resRef.extractPtr(); }

	Arena& arena() {
		ASSERT(resRef.isValid());
		static_assert(sizeof(Arena) == sizeof(void*));
		return *reinterpret_cast<Arena*>(&resRef.getPtr()->header->allocator.handle);
	}

	const Arena& arena() const {
		ASSERT(resRef.isValid());
		static_assert(sizeof(Arena) == sizeof(void*));
		return *reinterpret_cast<Arena*>(&resRef.getPtr()->header->allocator.handle);
	}

	bool isError() const { return resRef->header->result_type == FDBApiResult_Error; }

	Error getError() const {
		if (!isError())
			return success();
		else
			return Error(((FDBErrorResult*)getPtr())->error);
	}

	FDBResult* getData() const {
		if (isError()) {
			throw Error(((FDBErrorResult*)getPtr())->error);
		} else {
			return getPtr();
		}
	}

	static ApiResult addRef(FDBResult* result) {
		ASSERT(result != NULL);
		return ApiResult(Reference<FDBResultRefCounted>::addRef((FDBResultRefCounted*)result));
	}

	static ApiResult fromPtr(FDBResult* result) {
		return ApiResult(Reference<FDBResultRefCounted>((FDBResultRefCounted*)result));
	}

	static void release(FDBResult* result) {
		if (result) {
			((FDBResultRefCounted*)result)->delref();
		}
	}

protected:
	struct FDBResultRefCounted : public FDBResult {
		void addref() { header->allocator.ifc->addref(header->allocator.handle); }
		void delref() { header->allocator.ifc->delref(header->allocator.handle); }
	};

	Reference<FDBResultRefCounted> resRef;

	ApiResult(Reference<FDBResultRefCounted>&& ref) : resRef(ref) {}
};

template <class ResultType>
class TypedApiResult : public ApiResult {
public:
	TypedApiResult() {}
	explicit TypedApiResult(const ApiResult& r) : ApiResult(r) {}
	explicit TypedApiResult(ApiResult&& r) : ApiResult(r) {}

	ResultType* getPtr() const { return reinterpret_cast<ResultType*>(ApiResult::getPtr()); }
	ResultType* getData() const { return reinterpret_cast<ResultType*>(ApiResult::getData()); }

	static TypedApiResult<ResultType> create(FDBApiResultType resType) {
		Arena arena(sizeof(ResultType) + sizeof(FDBResultHeader));
		ResultType* res = new (arena) ResultType();
		res->header = new (arena) FDBResultHeader();
		res->header->result_type = resType;
		res->header->allocator.handle = arena.getPtr();
		res->header->allocator.ifc = localAllocatorInterface();
		return TypedApiResult<ResultType>(addRef((FDBResult*)res));
	}

	static TypedApiResult<ResultType> createError(Error err) {
		auto errRes = TypedApiResult<FDBErrorResult>::create(FDBApiResult_Error);
		errRes.getPtr()->error = err.code();
		return TypedApiResult<ResultType>(errRes);
	}
};

using ReadRangeApiResult = TypedApiResult<FDBReadRangeResult>;
using ReadBGMutationsApiResult = TypedApiResult<FDBReadBGMutationsResult>;
using ReadBGDescriptionsApiResult = TypedApiResult<FDBReadBGDescriptionResult>;

#endif