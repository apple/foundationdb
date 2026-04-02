/*
 * RatekeeperLimitReasons.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/core/RatekeeperLimitReasons.h"

const char* limitReasonName[] = {
	"workload",
	"storage_server_write_queue_size",
	"storage_server_write_bandwidth_mvcc",
	"storage_server_readable_behind",
	"log_server_mvcc_write_bandwidth",
	"log_server_write_queue",
	"storage_server_min_free_space",
	"storage_server_min_free_space_ratio",
	"log_server_min_free_space",
	"log_server_min_free_space_ratio",
	"storage_server_durability_lag",
	"storage_server_list_fetch_failed",
};
static_assert(sizeof(limitReasonName) / sizeof(limitReasonName[0]) == limitReason_t_end, "limitReasonName table size");

int limitReasonEnd = limitReason_t_end;

const char* limitReasonDesc[] = {
	"Workload or read performance.",
	"Storage server performance (storage queue).",
	"Storage server MVCC memory.",
	"Storage server version falling behind.",
	"Log server MVCC memory.",
	"Storage server performance (log queue).",
	"Storage server running out of space (approaching 100MB limit).",
	"Storage server running out of space (approaching 5% limit).",
	"Log server running out of space (approaching 100MB limit).",
	"Log server running out of space (approaching 5% limit).",
	"Storage server durable version falling behind.",
	"Unable to fetch storage server list.",
};
static_assert(sizeof(limitReasonDesc) / sizeof(limitReasonDesc[0]) == limitReason_t_end, "limitReasonDesc table size");
