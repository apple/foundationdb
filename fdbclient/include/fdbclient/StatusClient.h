/*
 * StatusClient.h
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

#ifndef FDBCLIENT_STATUSCLIENT_H
#define FDBCLIENT_STATUSCLIENT_H

#include "flow/flow.h"
#include "fdbclient/Status.h"
#include "fdbclient/DatabaseContext.h"

class StatusClient {
public:
	enum StatusLevel { MINIMAL = 0, NORMAL = 1, DETAILED = 2, JSON = 3 };
	// Fetches status json from FoundationDB.
	// @in statusField if not specified (or an empty string is specified) the actor will
	// fetch the entire status json object. If set to "fault_tolerance" the actor will
	// fetch fault tolerance related status json fields ("fault_tolerance", "data", "logs",
	// "maintenance_zone", "maintenance_seconds_remaining",	"qos", "recovery_state", "messages")
	// only.
	// @out status json
	// @note we don't have support to fetch arbitrary status json fields at this point.
	static Future<StatusObject> statusFetcher(Database db, std::string statusField = "");
};

#endif
