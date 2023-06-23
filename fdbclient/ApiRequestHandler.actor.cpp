/*
 * ApiRequestHandler.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/ApiRequestHandler.h"
#include "fdbclient/BlobGranuleApiImpl.h"

Future<ApiResult> handleApiRequest(ISingleThreadTransaction* tr, ApiRequest req) {
	switch (req.getType()) {
	case FDBApiRequest_ReadBGDescription:
		return readBlobGranuleDescriptions(tr, req);
		break;
	default:
		return unknown_api_request();
	}
}

ReadRangeApiResult createReadRangeApiResult(RangeResult rangeResult) {
	auto ret = ReadRangeApiResult::create(FDBApiResult_ReadRange);
	ret.arena().dependsOn(rangeResult.arena());
	auto data = ret.getPtr();
	static_assert(sizeof(FDBKeyValue) == sizeof(KeyValueRef));
	data->kv_arr = (FDBKeyValue*)rangeResult.begin();
	data->kv_count = rangeResult.size();
	data->more = rangeResult.more;
	return ret;
}
