/*
 * ClientKnobs.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

FDB_BOOLEAN_PARAM(Randomize);
FDB_BOOLEAN_PARAM(IsSimulated);

class SWIFT_CXX_IMMORTAL_SINGLETON_TYPE ClientKnobs : public KnobsImpl<ClientKnobs> {
public:
	int TOO_MANY; // FIXME: this should really be split up so we can control these more specifically

	double SYSTEM_MONITOR_INTERVAL;
	double NETWORK_BUSYNESS_MONITOR_INTERVAL; // The interval in which we should update the network busyness metric
	double TSS_METRICS_LOGGING_INTERVAL;

	double FAILURE_MAX_DELAY;
	double FAILURE_MIN_DELAY;
	double RECOVERY_DELAY_START_GENERATION;
	double RECOVERY_DELAY_SECONDS_PER_GENERATION;
	double MAX_GENERATIONS;
	double MAX_GENERATIONS_OVERRIDE;
	double MAX_GENERATIONS_SIM;

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
	double GRV_ERROR_RETRY_DELAY;
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
	double CHANGE_FEED_START_INTERVAL;
	bool CHANGE_FEED_COALESCE_LOCATIONS;
	int64_t CHANGE_FEED_CACHE_FLUSH_BYTES;
	double CHANGE_FEED_CACHE_EXPIRE_TIME;
	int64_t CHANGE_FEED_CACHE_LIMIT_BYTES;

	int MAX_BATCH_SIZE;
	double GRV_BATCH_TIMEOUT;
	int BROADCAST_BATCH_SIZE;
	double TRANSACTION_TIMEOUT_DELAY_INTERVAL;

	// When locationCache in DatabaseContext gets to be this size, items will be evicted
	int LOCATION_CACHE_EVICTION_SIZE;
	int LOCATION_CACHE_EVICTION_SIZE_SIM;
	double LOCATION_CACHE_ENDPOINT_FAILURE_GRACE_PERIOD;
	double LOCATION_CACHE_FAILED_ENDPOINT_RETRY_INTERVAL;

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
	int BACKUP_MAX_LOG_RANGES;
	int BACKUP_SIM_COPY_LOG_RANGES;
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
	int64_t VERSIONS_PER_SECOND; // Copy of SERVER_KNOBS, as we can't link with it.
	int64_t MAX_WRITE_TRANSACTION_LIFE_VERSIONS; // Copy of SERVER_KNOBS, as we can't link with it.
	int SIM_BACKUP_TASKS_PER_AGENT;
	int BACKUP_RANGEFILE_BLOCK_SIZE;
	int BACKUP_LOGFILE_BLOCK_SIZE;
	int BACKUP_DISPATCH_ADDTASK_SIZE;
	bool BACKUP_ALLOW_DRYRUN;
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
	int RESTORE_RANGES_READ_BATCH;
	int BLOB_GRANULE_RESTORE_CHECK_INTERVAL;
	bool BACKUP_CONTAINER_LOCAL_ALLOW_RELATIVE_PATH;
	bool ENABLE_REPLICA_CONSISTENCY_CHECK_ON_BACKUP_READS;
	int CONSISTENCY_CHECK_REQUIRED_REPLICAS;

	// Configuration
	int32_t DEFAULT_AUTO_COMMIT_PROXIES;
	int32_t DEFAULT_AUTO_GRV_PROXIES;
	int32_t DEFAULT_COMMIT_GRV_PROXIES_RATIO;
	int32_t DEFAULT_MAX_GRV_PROXIES;
	int32_t DEFAULT_AUTO_RESOLVERS;
	int32_t DEFAULT_AUTO_LOGS;

	double GLOBAL_CONFIG_REFRESH_BACKOFF;
	double GLOBAL_CONFIG_REFRESH_MAX_BACKOFF;
	double GLOBAL_CONFIG_REFRESH_TIMEOUT;

	// Dynamic Knobs
	double COMMIT_QUORUM_TIMEOUT;
	double GET_GENERATION_QUORUM_TIMEOUT;
	double GET_KNOB_TIMEOUT;
	double TIMEOUT_RETRY_UPPER_BOUND;

	// Client Status Info
	double CSI_SAMPLING_PROBABILITY;
	int64_t CSI_SIZE_LIMIT;
	double CSI_STATUS_DELAY;

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
	int BLOBSTORE_ENABLE_READ_CACHE;
	int BLOBSTORE_READ_BLOCK_SIZE;
	int BLOBSTORE_READ_AHEAD_BLOCKS;
	int BLOBSTORE_READ_CACHE_BLOCKS_PER_FILE;
	int BLOBSTORE_MAX_SEND_BYTES_PER_SECOND;
	int BLOBSTORE_MAX_RECV_BYTES_PER_SECOND;
	bool BLOBSTORE_GLOBAL_CONNECTION_POOL;
	bool BLOBSTORE_ENABLE_LOGGING;
	double BLOBSTORE_STATS_LOGGING_INTERVAL;
	double BLOBSTORE_LATENCY_LOGGING_INTERVAL;
	double BLOBSTORE_LATENCY_LOGGING_ACCURACY;
	int BLOBSTORE_MAX_DELAY_RETRYABLE_ERROR;
	int BLOBSTORE_MAX_DELAY_CONNECTION_FAILED;
	bool
	    BLOBSTORE_ENABLE_OBJECT_INTEGRITY_CHECK; // Enable integrity check of download. When not set, on upload, we
	                                             // we volunteer an md5 of the content and upload will fail if our
	                                             // md5 doesn't match that calculated by the server. When this flag
	                                             // is set, we hash the content using sha256 and have the server store
	                                             // the hash in the object metadata. On download, if this flag is set
	                                             // we will check the serverside proffered hash against that we
	                                             // calculate on the received content.  If no match, throw an error. See
	                                             // https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
	                                             // (We can't depend on etags in the download because they are not the
	                                             // md5 of the content when the upload uses encryption such as aws:kms)
	int CONSISTENCY_CHECK_RATE_LIMIT_MAX; // Available in both normal and urgent mode
	int CONSISTENCY_CHECK_ONE_ROUND_TARGET_COMPLETION_TIME; // Available in normal mode
	int CONSISTENCY_CHECK_URGENT_NEXT_WAIT_TIME; // Available in urgent mode
	int CONSISTENCY_CHECK_URGENT_BATCH_SHARD_COUNT; // Available in urgent mode
	int CONSISTENCY_CHECK_URGENT_RETRY_DEPTH_MAX; // Available in urgent mode
	std::string CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_0; // Available in urgent mode
	std::string CONSISTENCY_CHECK_URGENT_RANGE_END_0; // Available in urgent mode
	std::string CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_1; // Available in urgent mode
	std::string CONSISTENCY_CHECK_URGENT_RANGE_END_1; // Available in urgent mode
	std::string CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_2;
	std::string CONSISTENCY_CHECK_URGENT_RANGE_END_2;
	std::string CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_3;
	std::string CONSISTENCY_CHECK_URGENT_RANGE_END_3;

	// fdbcli
	int CLI_CONNECT_PARALLELISM;
	double CLI_CONNECT_TIMEOUT;

	// trace
	int TRACE_LOG_FILE_IDENTIFIER_MAX_LENGTH;

	// transaction tags
	int MAX_TRANSACTION_TAG_LENGTH;
	int MAX_TAGS_PER_TRANSACTION;
	int COMMIT_SAMPLE_COST; // The expectation of sampling is every COMMIT_SAMPLE_COST sample once
	int INCOMPLETE_SHARD_PLUS; // The size of (possible) incomplete shard when estimate clear range
	double READ_TAG_SAMPLE_RATE; // Communicated to clients from cluster
	double TAG_THROTTLE_SMOOTHING_WINDOW;
	double TAG_THROTTLE_RECHECK_INTERVAL;
	double TAG_THROTTLE_EXPIRATION_INTERVAL;
	int64_t TAG_THROTTLING_PAGE_SIZE; // Used to round up the cost of operations
	// Cost multiplier for writes (because write operations are more expensive than reads):
	double GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO;
	// Maximum duration that a transaction can be tag throttled by proxy before being rejected
	double PROXY_MAX_TAG_THROTTLE_DURATION;

	// Enable to automatically retry transactions in the presence of transaction_lock_rejection error
	// Set to false only for the rangeLocking simulation test
	bool TRANSACTION_LOCK_REJECTION_RETRIABLE;

	// busyness reporting
	double BUSYNESS_SPIKE_START_THRESHOLD;
	double BUSYNESS_SPIKE_SATURATED_THRESHOLD;

	// Blob Granules
	int BG_MAX_GRANULE_PARALLELISM;
	int BG_TOO_MANY_GRANULES;
	int64_t BLOB_METADATA_REFRESH_INTERVAL;
	bool DETERMINISTIC_BLOB_METADATA;
	bool ENABLE_BLOB_GRANULE_FILE_LOGICAL_SIZE;

	// The coordinator key/value in storage server might be inconsistent to the value stored in the cluster file.
	// This might happen when a recovery is happening together with a cluster controller coordinator key change.
	// During this process the database will be marked as "bad state" in changeQuorumChecker while later it will be
	// available again. Using a backoffed retry when it happens.
	int CHANGE_QUORUM_BAD_STATE_RETRY_TIMES;
	double CHANGE_QUORUM_BAD_STATE_RETRY_DELAY;

	// Tenants and Metacluster
	int MAX_TENANTS_PER_CLUSTER;
	int TENANT_TOMBSTONE_CLEANUP_INTERVAL;
	int MAX_DATA_CLUSTERS;
	int REMOVE_CLUSTER_TENANT_BATCH_SIZE;
	int METACLUSTER_ASSIGNMENT_CLUSTERS_TO_CHECK;
	double METACLUSTER_ASSIGNMENT_FIRST_CHOICE_DELAY;
	double METACLUSTER_ASSIGNMENT_AVAILABILITY_TIMEOUT;
	int METACLUSTER_RESTORE_BATCH_SIZE;
	int TENANT_ENTRY_CACHE_LIST_REFRESH_INTERVAL; // How often the TenantEntryCache is refreshed
	bool CLIENT_ENABLE_USING_CLUSTER_ID_KEY;

	// Encryption-at-rest
	bool ENABLE_ENCRYPTION_CPU_TIME_LOGGING;
	// This Knob will be a comma-delimited string (i.e 0,1,2,3) that specifies which tenants the the EKP should throw
	// key_not_found errors for. If TenantInfo::INVALID_TENANT is contained within the list then no tenants will be
	// dropped. This Knob should ONLY be used in simulation for testing purposes
	std::string SIMULATION_EKP_TENANT_IDS_TO_DROP;
	int ENCRYPT_HEADER_FLAGS_VERSION;
	int ENCRYPT_HEADER_AES_CTR_NO_AUTH_VERSION;
	int ENCRYPT_HEADER_AES_CTR_AES_CMAC_AUTH_VERSION;
	int ENCRYPT_HEADER_AES_CTR_HMAC_SHA_AUTH_VERSION;
	double ENCRYPT_GET_CIPHER_KEY_LONG_REQUEST_THRESHOLD;

	// REST KMS configurations
	bool REST_KMS_ALLOW_NOT_SECURE_CONNECTION;
	int SIM_KMS_VAULT_MAX_KEYS;

	bool ENABLE_MUTATION_CHECKSUM;
	// Enable to start accumulative checksum population and validation
	bool ENABLE_ACCUMULATIVE_CHECKSUM;
	// Enable to logging verbose trace events related to the accumulative checksum
	bool ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING;

	ClientKnobs(Randomize randomize);
	void initialize(Randomize randomize);
};

#endif
