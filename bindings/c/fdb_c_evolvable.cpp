/*
 * fdb_c_evolvable.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "foundationdb/fdb_c_evolvable.h"
#include "fdbclient/IClientApi.h"
#include "foundationdb/fdb_c_evolvable_internal.h"
#include "flow/Platform.h"
#include "fdbclient/MultiVersionTransaction.h"

#define RETURN_ON_ERROR(code_to_run)                                                                                   \
	try {                                                                                                              \
		code_to_run                                                                                                    \
	} catch (Error & e) {                                                                                              \
		if (e.code() <= 0)                                                                                             \
			return internal_error().code();                                                                            \
		else                                                                                                           \
			return e.code();                                                                                           \
	} catch (...) {                                                                                                    \
		return error_code_unknown_error;                                                                               \
	}

#define CATCH_AND_RETURN(code_to_run)                                                                                  \
	RETURN_ON_ERROR(code_to_run);                                                                                      \
	return error_code_success;

namespace {

MultiVersionApi* mvcApi() {
	return MultiVersionApi::api;
}

template <class RequestType>
RequestType* create_request_impl(FDBTransaction* tr, int32_t request_type) {
	FDBAllocatorIfc* allocIfc = ((ITransaction*)tr)->getAllocatorInterface();
	void* alloc = nullptr;
	RequestType* request = reinterpret_cast<RequestType*>(allocIfc->allocate(&alloc, sizeof(RequestType)));
	request->header = reinterpret_cast<FDBRequestHeader*>(allocIfc->allocate(&alloc, sizeof(FDBRequestHeader)));
	request->header->allocator.handle = alloc;
	request->header->allocator.ifc = allocIfc;
	request->header->request_type = request_type;
	return request;
}

template <class RequestType>
FDBFuture* exec_request_impl(FDBTransaction* tr, RequestType* request) {
	return (FDBFuture*)((ITransaction*)tr)->execAsyncRequest((FDBRequest*)request).extractPtr();
}

} // namespace

/**
 *	Internal API implementation
 */

extern "C" DLLEXPORT FDBAllocatorIfc* fdb_get_allocator_interface() {
	return mvcApi()->getAllocatorInterface();
}

extern "C" DLLEXPORT FDBFuture* fdb_transaction_exec_async(FDBTransaction* tx, FDBRequest* request) {
	return (FDBFuture*)(((ITransaction*)(tx))->execAsyncRequest(request).extractPtr());
}

extern "C" DLLEXPORT fdb_error_t fdb_future_get_response(FDBFuture* f, FDBResponse** response) {
	CATCH_AND_RETURN(*response = ((ThreadSingleAssignmentVar<ApiResponse>*)(f))->get().response;);
}

/**
 *	Public API implementation
 */

extern "C" DLLEXPORT void* fdb_copy_to_request(FDBRequestHeader* request_header, const void* begin, uint64_t length) {
	void* out = request_header->allocator.ifc->allocate(&request_header->allocator.handle, length);
	memcpy(out, begin, length);
	return out;
}

extern "C" DLLEXPORT FDBReadBGDescriptionRequest* fdb_create_read_bg_description_request(FDBTransaction* tr) {
	return create_request_impl<FDBReadBGDescriptionRequest>(tr, FDB_API_READ_BG_DESCRIPTION_REQUEST);
}

extern "C" DLLEXPORT FDBFuture* fdb_execute_read_bg_description_request(FDBTransaction* tr,
                                                                        FDBReadBGDescriptionRequest* request) {
	FDBFuture* future = exec_request_impl<FDBReadBGDescriptionRequest>(tr, request);
	request->header->allocator.ifc->delref(request->header->allocator.handle);
	return future;
}

extern "C" DLLEXPORT fdb_error_t fdb_future_get_read_bg_description_response(FDBFuture* f,
                                                                             FDBReadBGDescriptionResponse** out) {
	return fdb_future_get_response(f, reinterpret_cast<FDBResponse**>(out));
}
