/*
 * RatekeeperLimitReasons.h
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

#ifndef FDBSERVER_CORE_RATEKEEPERLIMITREASONS_H
#define FDBSERVER_CORE_RATEKEEPERLIMITREASONS_H

enum limitReason_t {
	unlimited,
	storage_server_write_queue_size,
	storage_server_write_bandwidth_mvcc,
	storage_server_readable_behind,
	log_server_mvcc_write_bandwidth,
	log_server_write_queue,
	storage_server_min_free_space,
	storage_server_min_free_space_ratio,
	log_server_min_free_space,
	log_server_min_free_space_ratio,
	storage_server_durability_lag,
	storage_server_list_fetch_failed,
	limitReason_t_end
};

extern int limitReasonEnd;
extern const char* limitReasonName[];
extern const char* limitReasonDesc[];

#endif
