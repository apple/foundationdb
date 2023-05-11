/*
 * EvolvableApiRequestHandler.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDBCLIENT_EVOLVABLE_API_REQUEST_HANDLER_H
#define FDBCLIENT_EVOLVABLE_API_REQUEST_HANDLER_H

#pragma once

#include "fdbclient/EvolvableApiTypes.h"
#include "fdbclient/ISingleThreadTransaction.h"

Future<ApiResponse> handleEvolvableApiRequest(ISingleThreadTransaction* tr, ApiRequest req);

#endif
