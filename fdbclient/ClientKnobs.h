/*
 * ClientKnobs.h
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

#ifndef FDBCLIENT_KNOBS_H
#define FDBCLIENT_KNOBS_H
#pragma once

#include "flow/BooleanParam.h"
#include "flow/Knobs.h"
#include "flow/flow.h"

FDB_DECLARE_BOOLEAN_PARAM(Randomize);
FDB_DECLARE_BOOLEAN_PARAM(IsSimulated);

class ClientKnobs : public KnobsImpl<ClientKnobs> {
public:
	int TOO_MANY; // FIXME: this should really be split up so we can control these more specifically

	double SYSTEM_MONITOR_INTERVAL;
	double NETWORK_BUSYNESS_MONITOR_INTERVAL; // The interval in which we should update the network busyness metric
	double TSS_METRICS_LOGGING_INTERVAL;

	double FAILURE_MAX_DELAY;
	double FAILURE_MIN_DELAY;
	double FAILURE_TIMEOUT_DELAY;
	double CLIENT_FAILURE_TIMEOUT_DELAY;
	double FAILURE_EMERGENCY_DELAY;
	double FAILURE_MAX_GENERATIONS;
	double RECOVERY_DELAY_START_GENERATION;
	double RECOVERY_DELAY_SECONDS_PER_GENERATION;
	double MAX_GENERATIONS;
	double MAX_GENERATIONS_OVERRIDE;
	double MAX_GENERATIONS_SIM;

	double COORDINATOR_HOSTNAME_RESOLVE_DELAY;
	double COORDINATOR_RECONNECTION_DELAY;
	int CLIENT_EXAMPLE_AMOUNT;
	double MAX_CLIENT_STATUS_AGE;
	int MAX_COMMIT_PROXY_CONNECTIONS;
	int MAX_GRV_PROXY_CONNECTIONS;
	double STATUS_IDLE_TIMEOUT;
	bool SEND_ENTIRE_VERSION_VECTOR;

	// wrong_shard_server sometimes comes from the only nonfailed server, so we need to avoid a fast spin
	double WRONG_SHARD_SERVER_DELAY; // SOMEDAY: This delay can limit performance of retrieving data when the cache is
	                                 // mostly wrong (e.g. dumping the database after a test)
	double FUTURE_VERSION_RETRY_DELAY;
	double UNKNOWN_TENANT_RETRY_DELAY;
	int REPLY_BYTE_LIMIT;
	double DEFAULT_BACKOFF;
	double DEFAULT_MAX_BACKOFF;
	double BACKOFF_GROWTH_RATE;
	double RESOURCE_CONSTRAINED_MAX_BACKOFF;
	int PROXY_COMMIT_OVERHEAD_BYTES;
	double SHARD_STAT_SMOOTH_AMOUNT;
	int INIT_MID_SHARD_BYTES;

	int TRANSACTION_SIZE_LIMIT;
	int64_t KEY_SIZE_LIMIT;
	int64_t SYSTEM_KEY_SIZE_LIMIT;
	int64_t VALUE_SIZE_LIMIT;
	int64_t SPLIT_KEY_SIZE_LIMIT;
	int METADATA_VERSION_CACHE_SIZE;
	int64_t CHANGE_FEED_LOCATION_LIMIT;
	int64_t CHANGE_FEED_CACHE_SIZE;
	double CHANGE_FEED_POP_TIMEOUT;
	int64_t CHANGE_FEED_STREAM_MIN_BYTES;

	int MAX_BATCH_SIZE;
	double GRV_BATCH_TIMEOUT;
	int BROADCAST_BATCH_SIZE;
	double TRANSACTION_TIMEOUT_DELAY_INTERVAL;

	// When locationCache in DatabaseContext gets to be this size, items will be evicted
	int LOCATION_CACHE_EVICTION_SIZE;
	int LOCATION_CACHE_EVICTION_SIZE_SIM;
	double LOCATION_CACHE_ENDPOINT_FAILURE_GRACE_PERIOD;
	double LOCATION_CACHE_FAILED_ENDPOINT_RETRY_INTERVAL;
	int TENANT_CACHE_EVICTION_SIZE;
	int TENANT_CACHE_EVICTION_SIZE_SIM;

	int GET_RANGE_SHARD_LIMIT;
	int WARM_RANGE_SHARD_LIMIT;
	int STORAGE_METRICS_SHARD_LIMIT;
	int SHARD_COUNT_LIMIT;
	double STORAGE_METRICS_UNFAIR_SPLIT_LIMIT;
	double STORAGE_METRICS_TOO_MANY_SHARDS_DELAY;
	double AGGREGATE_HEALTH_METRICS_MAX_STALENESS;
	double DETAILED_HEALTH_METRICS_MAX_STALENESS;
	double MID_SHARD_SIZE_MAX_STALENESS;
	bool TAG_ENCODE_KEY_SERVERS;
	int64_t RANGESTREAM_FRAGMENT_SIZE;
	int RANGESTREAM_BUFFERED_FRAGMENTS_LIMIT;
	bool QUARANTINE_TSS_ON_MISMATCH;
	double CHANGE_FEED_EMPTY_BATCH_TIME;

	// KeyRangeMap
	int KRM_GET_RANGE_LIMIT;
	int KRM_GET_RANGE_LIMIT_BYTES; // This must be sufficiently larger than KEY_SIZE_LIMIT to ensure that at least two
	                               // entries will be returned from an attempt to read a key range map

	int DEFAULT_MAX_OUTSTANDING_WATCHES;
	int ABSOLUTE_MAX_WATCHES; // The client cannot set the max outstanding watches higher than this
	double WATCH_POLLING_TIME;
	double NO_RECENT_UPDATES_DURATION;
	double FAST_WATCH_TIMEOUT;
	double WATCH_TIMEOUT;

	double IS_ACCEPTABLE_DELAY;

	// Core
	int64_t CORE_VERSIONSPERSECOND; // This is defined within the server but used for knobs based on server value
	int LOG_RANGE_BLOCK_SIZE;
	int MUTATION_BLOCK_SIZE;
	double MAX_VERSION_CACHE_LAG; // The upper bound in seconds for OK amount of staleness when using a cached RV
	double MAX_PROXY_CONTACT_LAG; // The upper bound in seconds for how often we want a response from the GRV proxies
	double DEBUG_USE_GRV_CACHE_CHANCE; // Debug setting to change the chance for a regular GRV request to use the cache
	bool FORCE_GRV_CACHE_OFF; // Panic button to turn off cache. Holds priority over other options.
	double GRV_CACHE_RK_COOLDOWN; // Required number of seconds to pass after throttling to re-allow cache use
	double GRV_SUSTAINED_THROTTLING_THRESHOLD; // If ALL GRV requests have been throttled in the last number of seconds
	                                           // specified here, ratekeeper is throttling and not a false positive

	// Taskbucket
	double TASKBUCKET_LOGGING_DELAY;
	int TASKBUCKET_MAX_PRIORITY;
	double TASKBUCKET_CHECK_TIMEOUT_CHANCE;
	double TASKBUCKET_TIMEOUT_JITTER_OFFSET;
	double TASKBUCKET_TIMEOUT_JITTER_RANGE;
	double TASKBUCKET_CHECK_ACTIVE_DELAY;
	int TASKBUCKET_CHECK_ACTIVE_AMOUNT;
	int TASKBUCKET_TIMEOUT_VERSIONS;
	int TASKBUCKET_MAX_TASK_KEYS;

	// Backup
	int BACKUP_LOCAL_FILE_WRITE_BLOCK;
	int BACKUP_CONCURRENT_DELETES;
	int BACKUP_SIMULATED_LIMIT_BYTES;
	int BACKUP_GET_RANGE_LIMIT_BYTES;
	int BACKUP_LOCK_BYTES;
	double BACKUP_RANGE_TIMEOUT;
	double BACKUP_RANGE_MINWAIT;
	int BACKUP_SNAPSHOT_DISPATCH_INTERVAL_SEC;
	int BACKUP_DEFAULT_SNAPSHOT_INTERVAL_SEC;
	int BACKUP_SHARD_TASK_LIMIT;
	double BACKUP_AGGREGATE_POLL_RATE;
	double BACKUP_AGGREGATE_POLL_RATE_UPDATE_INTERVAL;
	int BACKUP_LOG_WRITE_BATCH_MAX_SIZE;
	int BACKUP_LOG_ATOMIC_OPS_SIZE;
	int BACKUP_MAX_LOG_RANGES;
	int BACKUP_SIM_COPY_LOG_RANGES;
	int BACKUP_OPERATION_COST_OVERHEAD;
	int BACKUP_VERSION_DELAY;
	int BACKUP_MAP_KEY_LOWER_LIMIT;
	int BACKUP_MAP_KEY_UPPER_LIMIT;
	int BACKUP_COPY_TASKS;
	int BACKUP_BLOCK_SIZE;
	int COPY_LOG_BLOCK_SIZE;
	int COPY_LOG_BLOCKS_PER_TASK;
	int COPY_LOG_PREFETCH_BLOCKS;
	int COPY_LOG_READ_AHEAD_BYTES;
	double COPY_LOG_TASK_DURATION_NANOS;
	int BACKUP_TASKS_PER_AGENT;
	int BACKUP_POLL_PROGRESS_SECONDS;
	int64_t VERSIONS_PER_SECOND; // Copy of SERVER_KNOBS, as we can't link with it
	int SIM_BACKUP_TASKS_PER_AGENT;
	int BACKUP_RANGEFILE_BLOCK_SIZE;
	int BACKUP_LOGFILE_BLOCK_SIZE;
	int BACKUP_DISPATCH_ADDTASK_SIZE;
	int RESTORE_DISPATCH_ADDTASK_SIZE;
	int RESTORE_DISPATCH_BATCH_SIZE;
	int RESTORE_WRITE_TX_SIZE;
	int APPLY_MAX_LOCK_BYTES;
	int APPLY_MIN_LOCK_BYTES;
	int APPLY_BLOCK_SIZE;
	double APPLY_MAX_DECAY_RATE;
	double APPLY_MAX_INCREASE_FACTOR;
	double BACKUP_ERROR_DELAY;
	double BACKUP_STATUS_DELAY;
	double BACKUP_STATUS_JITTER;
	double MIN_CLEANUP_SECONDS;
	int64_t FASTRESTORE_ATOMICOP_WEIGHT; // workload amplication factor for atomic op

	// Configuration
	int32_t DEFAULT_AUTO_COMMIT_PROXIES;
	int32_t DEFAULT_AUTO_GRV_PROXIES;
	int32_t DEFAULT_COMMIT_GRV_PROXIES_RATIO;
	int32_t DEFAULT_MAX_GRV_PROXIES;
	int32_t DEFAULT_AUTO_RESOLVERS;
	int32_t DEFAULT_AUTO_LOGS;

	// Dynamic Knobs
	double COMMIT_QUORUM_TIMEOUT;
	double GET_GENERATION_QUORUM_TIMEOUT;
	double GET_KNOB_TIMEOUT;
	double TIMEOUT_RETRY_UPPER_BOUND;

	// Client Status Info
	double CSI_SAMPLING_PROBABILITY;
	int64_t CSI_SIZE_LIMIT;
	double CSI_STATUS_DELAY;

	int HTTP_SEND_SIZE;
	int HTTP_READ_SIZE;
	int HTTP_VERBOSE_LEVEL;
	std::string HTTP_REQUEST_ID_HEADER;
	bool HTTP_REQUEST_AWS_V4_HEADER; // setting this knob to true will enable AWS V4 style header.
	std::string BLOBSTORE_ENCRYPTION_TYPE;
	int BLOBSTORE_CONNECT_TRIES;
	int BLOBSTORE_CONNECT_TIMEOUT;
	int BLOBSTORE_MAX_CONNECTION_LIFE;
	int BLOBSTORE_REQUEST_TRIES;
	int BLOBSTORE_REQUEST_TIMEOUT_MIN;
	int BLOBSTORE_REQUESTS_PER_SECOND;
	int BLOBSTORE_LIST_REQUESTS_PER_SECOND;
	int BLOBSTORE_WRITE_REQUESTS_PER_SECOND;
	int BLOBSTORE_READ_REQUESTS_PER_SECOND;
	int BLOBSTORE_DELETE_REQUESTS_PER_SECOND;
	int BLOBSTORE_CONCURRENT_REQUESTS;
	int BLOBSTORE_MULTIPART_MAX_PART_SIZE;
	int BLOBSTORE_MULTIPART_MIN_PART_SIZE;
	int BLOBSTORE_CONCURRENT_UPLOADS;
	int BLOBSTORE_CONCURRENT_LISTS;
	int BLOBSTORE_CONCURRENT_WRITES_PER_FILE;
	int BLOBSTORE_CONCURRENT_READS_PER_FILE;
	int BLOBSTORE_READ_BLOCK_SIZE;
	int BLOBSTORE_READ_AHEAD_BLOCKS;
	int BLOBSTORE_READ_CACHE_BLOCKS_PER_FILE;
	int BLOBSTORE_MAX_SEND_BYTES_PER_SECOND;
	int BLOBSTORE_MAX_RECV_BYTES_PER_SECOND;

	int CONSISTENCY_CHECK_RATE_LIMIT_MAX;
	int CONSISTENCY_CHECK_ONE_ROUND_TARGET_COMPLETION_TIME;

	// fdbcli
	int CLI_CONNECT_PARALLELISM;
	double CLI_CONNECT_TIMEOUT;

	// trace
	int TRACE_LOG_FILE_IDENTIFIER_MAX_LENGTH;

	// transaction tags
	int MAX_TRANSACTION_TAG_LENGTH;
	int MAX_TAGS_PER_TRANSACTION;
	int COMMIT_SAMPLE_COST; // The expectation of sampling is every COMMIT_SAMPLE_COST sample once
	int WRITE_COST_BYTE_FACTOR;
	int INCOMPLETE_SHARD_PLUS; // The size of (possible) incomplete shard when estimate clear range
	double READ_TAG_SAMPLE_RATE; // Communicated to clients from cluster
	double TAG_THROTTLE_SMOOTHING_WINDOW;
	double TAG_THROTTLE_RECHECK_INTERVAL;
	double TAG_THROTTLE_EXPIRATION_INTERVAL;

	// busyness reporting
	double BUSYNESS_SPIKE_START_THRESHOLD;
	double BUSYNESS_SPIKE_SATURATED_THRESHOLD;

	// multi-version client control
	int MVC_CLIENTLIB_CHUNK_SIZE;
	int MVC_CLIENTLIB_CHUNKS_PER_TRANSACTION;

	// Blob Granules
	int BG_MAX_GRANULE_PARALLELISM;

	ClientKnobs(Randomize randomize);
	void initialize(Randomize randomize);
};

#endif
