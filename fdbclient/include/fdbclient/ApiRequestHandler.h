/*
 * ApiRequestHandler.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDBCLIENT_API_REQUEST_HANDLER_H
#define FDBCLIENT_API_REQUEST_HANDLER_H

#pragma once

#include "fdbclient/ApiRequest.h"
#include "fdbclient/ISingleThreadTransaction.h"
#include "fdbclient/FDBTypes.h"

Future<ApiResult> handleApiRequest(ISingleThreadTransaction* tr, ApiRequest req);

ReadRangeApiResult createReadRangeApiResult(RangeResult rangeResult);

#endif
