/*
 * Knobs.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_KNOBS_H
#define FDBSERVER_KNOBS_H
#pragma once

#include "flow/Knobs.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbclient/Knobs.h"

// Disk queue
static const int _PAGE_SIZE = 4096;

class ServerKnobs : public Knobs {
public:
	// Versions
	int64_t VERSIONS_PER_SECOND;
	int64_t MAX_VERSIONS_IN_FLIGHT;
	int64_t MAX_VERSIONS_IN_FLIGHT_FORCED;
	int64_t MAX_READ_TRANSACTION_LIFE_VERSIONS;
	int64_t MAX_WRITE_TRANSACTION_LIFE_VERSIONS;
	double MAX_COMMIT_BATCH_INTERVAL; // Each commit proxy generates a CommitTransactionBatchRequest at least this
	                                  // often, so that versions always advance smoothly

	// TLogs
	double TLOG_TIMEOUT; // tlog OR commit proxy failure - master's reaction time
	double RECOVERY_TLOG_SMART_QUORUM_DELAY;		// smaller might be better for bug amplification
	double TLOG_STORAGE_MIN_UPDATE_INTERVAL;
	double BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL;
	int DESIRED_TOTAL_BYTES;
	int DESIRED_UPDATE_BYTES;
	double UPDATE_DELAY;
	int MAXIMUM_PEEK_BYTES;
	int APPLY_MUTATION_BYTES;
	int RECOVERY_DATA_BYTE_LIMIT;
	int BUGGIFY_RECOVERY_DATA_LIMIT;
	double LONG_TLOG_COMMIT_TIME;
	int64_t LARGE_TLOG_COMMIT_BYTES;
	double BUGGIFY_RECOVER_MEMORY_LIMIT;
	double BUGGIFY_WORKER_REMOVED_MAX_LAG;
	int64_t UPDATE_STORAGE_BYTE_LIMIT;
	int64_t REFERENCE_SPILL_UPDATE_STORAGE_BYTE_LIMIT;
	double TLOG_PEEK_DELAY;
	int LEGACY_TLOG_UPGRADE_ENTRIES_PER_VERSION;
	int VERSION_MESSAGES_OVERHEAD_FACTOR_1024THS; // Multiplicative factor to bound total space used to store a version message (measured in 1/1024ths, e.g. a value of 2048 yields a factor of 2).
	int64_t VERSION_MESSAGES_ENTRY_BYTES_WITH_OVERHEAD;
	double TLOG_MESSAGE_BLOCK_OVERHEAD_FACTOR;
	int64_t TLOG_MESSAGE_BLOCK_BYTES;
	int64_t MAX_MESSAGE_SIZE;
	int LOG_SYSTEM_PUSHED_DATA_BLOCK_SIZE;
	double PEEK_TRACKER_EXPIRATION_TIME;
	int PARALLEL_GET_MORE_REQUESTS;
	int MULTI_CURSOR_PRE_FETCH_LIMIT;
	int64_t MAX_QUEUE_COMMIT_BYTES;
	int DESIRED_OUTSTANDING_MESSAGES;
	double DESIRED_GET_MORE_DELAY;
	int CONCURRENT_LOG_ROUTER_READS;
	int LOG_ROUTER_PEEK_FROM_SATELLITES_PREFERRED; // 0==peek from primary, non-zero==peek from satellites
	double DISK_QUEUE_ADAPTER_MIN_SWITCH_TIME;
	double DISK_QUEUE_ADAPTER_MAX_SWITCH_TIME;
	int64_t TLOG_SPILL_REFERENCE_MAX_PEEK_MEMORY_BYTES;
	int64_t TLOG_SPILL_REFERENCE_MAX_BATCHES_PER_PEEK;
	int64_t TLOG_SPILL_REFERENCE_MAX_BYTES_PER_BATCH;
	int64_t DISK_QUEUE_FILE_EXTENSION_BYTES; // When we grow the disk queue, by how many bytes should it grow?
	int64_t DISK_QUEUE_FILE_SHRINK_BYTES; // When we shrink the disk queue, by how many bytes should it shrink?
	int DISK_QUEUE_MAX_TRUNCATE_BYTES;  // A truncate larger than this will cause the file to be replaced instead.
	double TLOG_DEGRADED_DURATION;
	int64_t MAX_CACHE_VERSIONS;
	double TXS_POPPED_MAX_DELAY;
	double TLOG_MAX_CREATE_DURATION;
	int PEEK_LOGGING_AMOUNT;
	double PEEK_LOGGING_DELAY;
	double PEEK_RESET_INTERVAL;
	double PEEK_MAX_LATENCY;
	bool PEEK_COUNT_SMALL_MESSAGES;
	double PEEK_STATS_INTERVAL;
	double PEEK_STATS_SLOW_AMOUNT;
	double PEEK_STATS_SLOW_RATIO;
	double PUSH_RESET_INTERVAL;
	double PUSH_MAX_LATENCY;
	double PUSH_STATS_INTERVAL;
	double PUSH_STATS_SLOW_AMOUNT;
	double PUSH_STATS_SLOW_RATIO;
	int TLOG_POP_BATCH_SIZE;

	// Data distribution queue
	double HEALTH_POLL_TIME;
	double BEST_TEAM_STUCK_DELAY;
	double BG_REBALANCE_POLLING_INTERVAL;
	double BG_REBALANCE_SWITCH_CHECK_INTERVAL;
	double DD_QUEUE_LOGGING_INTERVAL;
	double RELOCATION_PARALLELISM_PER_SOURCE_SERVER;
	int DD_QUEUE_MAX_KEY_SERVERS;
	int DD_REBALANCE_PARALLELISM;
	int DD_REBALANCE_RESET_AMOUNT;
	double BG_DD_MAX_WAIT;
	double BG_DD_MIN_WAIT;
	double BG_DD_INCREASE_RATE;
	double BG_DD_DECREASE_RATE;
	double BG_DD_SATURATION_DELAY;
	double INFLIGHT_PENALTY_HEALTHY;
	double INFLIGHT_PENALTY_REDUNDANT;
	double INFLIGHT_PENALTY_UNHEALTHY;
	double INFLIGHT_PENALTY_ONE_LEFT;
	bool USE_OLD_NEEDED_SERVERS;

	// Higher priorities are executed first
	// Priority/100 is the "priority group"/"superpriority".  Priority inversion
	//   is possible within but not between priority groups; fewer priority groups
	//   mean better worst case time bounds
	// Maximum allowable priority is 999.
	int PRIORITY_RECOVER_MOVE;
	int PRIORITY_REBALANCE_UNDERUTILIZED_TEAM;
	int PRIORITY_REBALANCE_OVERUTILIZED_TEAM;
	int PRIORITY_TEAM_HEALTHY;
	int PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER;
	int PRIORITY_TEAM_REDUNDANT;
	int PRIORITY_MERGE_SHARD;
	int PRIORITY_POPULATE_REGION;
	int PRIORITY_TEAM_UNHEALTHY;
	int PRIORITY_TEAM_2_LEFT;
	int PRIORITY_TEAM_1_LEFT;
	int PRIORITY_TEAM_FAILED;         // Priority when a server in the team is excluded as failed
	int PRIORITY_TEAM_0_LEFT;
	int PRIORITY_SPLIT_SHARD;

	// Data distribution
	double RETRY_RELOCATESHARD_DELAY;
	double DATA_DISTRIBUTION_FAILURE_REACTION_TIME;
	int MIN_SHARD_BYTES, SHARD_BYTES_RATIO, SHARD_BYTES_PER_SQRT_BYTES, MAX_SHARD_BYTES, KEY_SERVER_SHARD_BYTES;
	int64_t SHARD_MAX_BYTES_PER_KSEC, // Shards with more than this bandwidth will be split immediately
		SHARD_MIN_BYTES_PER_KSEC,     // Shards with more than this bandwidth will not be merged
		SHARD_SPLIT_BYTES_PER_KSEC;   // When splitting a shard, it is split into pieces with less than this bandwidth
	double SHARD_MAX_READ_DENSITY_RATIO;
	int64_t SHARD_READ_HOT_BANDWITH_MIN_PER_KSECONDS;
	double SHARD_MAX_BYTES_READ_PER_KSEC_JITTER;
	double STORAGE_METRIC_TIMEOUT;
	double METRIC_DELAY;
	double ALL_DATA_REMOVED_DELAY;
	double INITIAL_FAILURE_REACTION_DELAY;
	double CHECK_TEAM_DELAY;
	double LOG_ON_COMPLETION_DELAY;
	int BEST_TEAM_MAX_TEAM_TRIES;
	int BEST_TEAM_OPTION_COUNT;
	int BEST_OF_AMT;
	double SERVER_LIST_DELAY;
	double RECRUITMENT_IDLE_DELAY;
	double STORAGE_RECRUITMENT_DELAY;
	double DATA_DISTRIBUTION_LOGGING_INTERVAL;
	double DD_ENABLED_CHECK_DELAY;
	double DD_STALL_CHECK_DELAY;
	double DD_LOW_BANDWIDTH_DELAY;
	double DD_MERGE_COALESCE_DELAY;
	double STORAGE_METRICS_POLLING_DELAY;
	double STORAGE_METRICS_RANDOM_DELAY;
	double AVAILABLE_SPACE_RATIO_CUTOFF;
	int DESIRED_TEAMS_PER_SERVER;
	int MAX_TEAMS_PER_SERVER;
	int64_t DD_SHARD_SIZE_GRANULARITY;
	int64_t DD_SHARD_SIZE_GRANULARITY_SIM;
	int DD_MOVE_KEYS_PARALLELISM;
	int DD_FETCH_SOURCE_PARALLELISM;
	int DD_MERGE_LIMIT;
	double DD_SHARD_METRICS_TIMEOUT;
	int64_t DD_LOCATION_CACHE_SIZE;
	double MOVEKEYS_LOCK_POLLING_DELAY;
	double DEBOUNCE_RECRUITING_DELAY;
	int REBALANCE_MAX_RETRIES;
	int DD_OVERLAP_PENALTY;
	int DD_EXCLUDE_MIN_REPLICAS;
	bool DD_VALIDATE_LOCALITY;
	int DD_CHECK_INVALID_LOCALITY_DELAY;
	bool DD_ENABLE_VERBOSE_TRACING;
	int64_t DD_SS_FAILURE_VERSIONLAG; // Allowed SS version lag from the current read version before marking it as failed.
	int64_t DD_SS_ALLOWED_VERSIONLAG; // SS will be marked as healthy if it's version lag goes below this value.
	double DD_SS_STUCK_TIME_LIMIT; // If a storage server is not getting new versions for this amount of time, then it becomes undesired.
	int DD_TEAMS_INFO_PRINT_INTERVAL;
	int DD_TEAMS_INFO_PRINT_YIELD_COUNT;
	int DD_TEAM_ZERO_SERVER_LEFT_LOG_DELAY;

	// TeamRemover to remove redundant teams
	bool TR_FLAG_DISABLE_MACHINE_TEAM_REMOVER; // disable the machineTeamRemover actor
	double TR_REMOVE_MACHINE_TEAM_DELAY; // wait for the specified time before try to remove next machine team
	bool TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS; // guard to select which machineTeamRemover logic to use

	bool TR_FLAG_DISABLE_SERVER_TEAM_REMOVER; // disable the serverTeamRemover actor
	double TR_REMOVE_SERVER_TEAM_DELAY; // wait for the specified time before try to remove next server team
	double TR_REMOVE_SERVER_TEAM_EXTRA_DELAY; // serverTeamRemover waits for the delay and check DD healthyness again to ensure it runs after machineTeamRemover

	// Remove wrong storage engines
	double DD_REMOVE_STORE_ENGINE_DELAY; // wait for the specified time before remove the next batch

	double DD_FAILURE_TIME;
	double DD_ZERO_HEALTHY_TEAM_DELAY;

	// Redwood Storage Engine
	int PREFIX_TREE_IMMEDIATE_KEY_SIZE_LIMIT;
	int PREFIX_TREE_IMMEDIATE_KEY_SIZE_MIN;

	// KeyValueStore SQLITE
	int CLEAR_BUFFER_SIZE;
	double READ_VALUE_TIME_ESTIMATE;
	double READ_RANGE_TIME_ESTIMATE;
	double SET_TIME_ESTIMATE;
	double CLEAR_TIME_ESTIMATE;
	double COMMIT_TIME_ESTIMATE;
	int CHECK_FREE_PAGE_AMOUNT;
	double DISK_METRIC_LOGGING_INTERVAL;
	int64_t SOFT_HEAP_LIMIT;

	int SQLITE_PAGE_SCAN_ERROR_LIMIT;
	int SQLITE_BTREE_PAGE_USABLE;
	int SQLITE_BTREE_CELL_MAX_LOCAL;
	int SQLITE_BTREE_CELL_MIN_LOCAL;
	int SQLITE_FRAGMENT_PRIMARY_PAGE_USABLE;
	int SQLITE_FRAGMENT_OVERFLOW_PAGE_USABLE;
	double SQLITE_FRAGMENT_MIN_SAVINGS;
	int SQLITE_CHUNK_SIZE_PAGES;
	int SQLITE_CHUNK_SIZE_PAGES_SIM;

	// KeyValueStoreSqlite spring cleaning
	double SPRING_CLEANING_NO_ACTION_INTERVAL;
	double SPRING_CLEANING_LAZY_DELETE_INTERVAL;
	double SPRING_CLEANING_VACUUM_INTERVAL;
	double SPRING_CLEANING_LAZY_DELETE_TIME_ESTIMATE;
	double SPRING_CLEANING_VACUUM_TIME_ESTIMATE;
	double SPRING_CLEANING_VACUUMS_PER_LAZY_DELETE_PAGE;
	int SPRING_CLEANING_MIN_LAZY_DELETE_PAGES;
	int SPRING_CLEANING_MAX_LAZY_DELETE_PAGES;
	int SPRING_CLEANING_LAZY_DELETE_BATCH_SIZE;
	int SPRING_CLEANING_MIN_VACUUM_PAGES;
	int SPRING_CLEANING_MAX_VACUUM_PAGES;

	// KeyValueStoreMemory
	int64_t REPLACE_CONTENTS_BYTES;

	// KeyValueStoreRocksDB
	int ROCKSDB_BACKGROUND_PARALLELISM;
	int ROCKSDB_READ_PARALLELISM;
	int64_t ROCKSDB_MEMTABLE_BYTES;
	bool ROCKSDB_UNSAFE_AUTO_FSYNC;
	int64_t ROCKSDB_PERIODIC_COMPACTION_SECONDS;

	// Leader election
	int MAX_NOTIFICATIONS;
	int MIN_NOTIFICATIONS;
	double NOTIFICATION_FULL_CLEAR_TIME;
	double CANDIDATE_MIN_DELAY;
	double CANDIDATE_MAX_DELAY;
	double CANDIDATE_GROWTH_RATE;
	double POLLING_FREQUENCY;
	double HEARTBEAT_FREQUENCY;

	// Commit CommitProxy
	double START_TRANSACTION_BATCH_INTERVAL_MIN;
	double START_TRANSACTION_BATCH_INTERVAL_MAX;
	double START_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION;
	double START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA;
	double START_TRANSACTION_BATCH_QUEUE_CHECK_INTERVAL;
	double START_TRANSACTION_MAX_TRANSACTIONS_TO_START;
	int START_TRANSACTION_MAX_REQUESTS_TO_START;
	double START_TRANSACTION_RATE_WINDOW;
	double START_TRANSACTION_MAX_EMPTY_QUEUE_BUDGET;
	int START_TRANSACTION_MAX_QUEUE_SIZE;
	int KEY_LOCATION_MAX_QUEUE_SIZE;

	double COMMIT_TRANSACTION_BATCH_INTERVAL_FROM_IDLE;
	double COMMIT_TRANSACTION_BATCH_INTERVAL_MIN;
	double COMMIT_TRANSACTION_BATCH_INTERVAL_MAX;
	double COMMIT_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION;
	double COMMIT_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA;
	int    COMMIT_TRANSACTION_BATCH_COUNT_MAX;
	int    COMMIT_TRANSACTION_BATCH_BYTES_MIN;
	int    COMMIT_TRANSACTION_BATCH_BYTES_MAX;
	double COMMIT_TRANSACTION_BATCH_BYTES_SCALE_BASE;
	double COMMIT_TRANSACTION_BATCH_BYTES_SCALE_POWER;
	int64_t COMMIT_BATCHES_MEM_BYTES_HARD_LIMIT;
	double COMMIT_BATCHES_MEM_FRACTION_OF_TOTAL;
	double COMMIT_BATCHES_MEM_TO_TOTAL_MEM_SCALE_FACTOR;

	double RESOLVER_COALESCE_TIME;
	int BUGGIFIED_ROW_LIMIT;
	double PROXY_SPIN_DELAY;
	double UPDATE_REMOTE_LOG_VERSION_INTERVAL;
	int MAX_TXS_POP_VERSION_HISTORY;
	double MIN_CONFIRM_INTERVAL;
	double ENFORCED_MIN_RECOVERY_DURATION;
	double REQUIRED_MIN_RECOVERY_DURATION;
	bool ALWAYS_CAUSAL_READ_RISKY;
	int MAX_COMMIT_UPDATES;
	double MAX_PROXY_COMPUTE;
	double MAX_COMPUTE_PER_OPERATION;
	int PROXY_COMPUTE_BUCKETS;
	double PROXY_COMPUTE_GROWTH_RATE;
	int TXN_STATE_SEND_AMOUNT;
	double REPORT_TRANSACTION_COST_ESTIMATION_DELAY;

	int RESET_MASTER_BATCHES;
	int RESET_RESOLVER_BATCHES;
	double RESET_MASTER_DELAY;
	double RESET_RESOLVER_DELAY;

	// Master Server
	double COMMIT_SLEEP_TIME;
	double MIN_BALANCE_TIME;
	int64_t MIN_BALANCE_DIFFERENCE;
	double SECONDS_BEFORE_NO_FAILURE_DELAY;
	int64_t MAX_TXS_SEND_MEMORY;
	int64_t MAX_RECOVERY_VERSIONS;
	double MAX_RECOVERY_TIME;
	double PROVISIONAL_START_DELAY;
	double PROVISIONAL_DELAY_GROWTH;
	double PROVISIONAL_MAX_DELAY;
	double SECONDS_BEFORE_RECRUIT_BACKUP_WORKER;
	double CC_INTERFACE_TIMEOUT;

	// Resolver
	int64_t KEY_BYTES_PER_SAMPLE;
	int64_t SAMPLE_OFFSET_PER_KEY;
	double SAMPLE_EXPIRATION_TIME;
	double SAMPLE_POLL_TIME;
	int64_t RESOLVER_STATE_MEMORY_LIMIT;

	// Backup Worker
	double BACKUP_TIMEOUT;  // master's reaction time for backup failure
	double BACKUP_NOOP_POP_DELAY;
	int BACKUP_FILE_BLOCK_BYTES;
	int64_t BACKUP_LOCK_BYTES;
	double BACKUP_UPLOAD_DELAY;

	//Cluster Controller
	double CLUSTER_CONTROLLER_LOGGING_DELAY;
	double MASTER_FAILURE_REACTION_TIME;
	double MASTER_FAILURE_SLOPE_DURING_RECOVERY;
	int WORKER_COORDINATION_PING_DELAY;
	double SIM_SHUTDOWN_TIMEOUT;
	double SHUTDOWN_TIMEOUT;
	double MASTER_SPIN_DELAY;
	double CC_CHANGE_DELAY;
	double CC_CLASS_DELAY;
	double WAIT_FOR_GOOD_RECRUITMENT_DELAY;
	double WAIT_FOR_GOOD_REMOTE_RECRUITMENT_DELAY;
	double ATTEMPT_RECRUITMENT_DELAY;
	double WAIT_FOR_DISTRIBUTOR_JOIN_DELAY;
	double WAIT_FOR_RATEKEEPER_JOIN_DELAY;
	double WORKER_FAILURE_TIME;
	double CHECK_OUTSTANDING_INTERVAL;
	double INCOMPATIBLE_PEERS_LOGGING_INTERVAL;
	double VERSION_LAG_METRIC_INTERVAL;
	int64_t MAX_VERSION_DIFFERENCE;
	double FORCE_RECOVERY_CHECK_DELAY;
	double RATEKEEPER_FAILURE_TIME;
	double REPLACE_INTERFACE_DELAY;
	double REPLACE_INTERFACE_CHECK_DELAY;
	double COORDINATOR_REGISTER_INTERVAL;
	double CLIENT_REGISTER_INTERVAL;

	// Knobs used to select the best policy (via monte carlo)
	int POLICY_RATING_TESTS;	// number of tests per policy (in order to compare)
	int POLICY_GENERATIONS;		// number of policies to generate

	int EXPECTED_MASTER_FITNESS;
	int EXPECTED_TLOG_FITNESS;
	int EXPECTED_LOG_ROUTER_FITNESS;
	int EXPECTED_COMMIT_PROXY_FITNESS;
	int EXPECTED_GRV_PROXY_FITNESS;
	int EXPECTED_RESOLVER_FITNESS;
	double RECRUITMENT_TIMEOUT;
	int DBINFO_SEND_AMOUNT;
	double DBINFO_BATCH_DELAY;

	//Move Keys
	double SHARD_READY_DELAY;
	double SERVER_READY_QUORUM_INTERVAL;
	double SERVER_READY_QUORUM_TIMEOUT;
	double REMOVE_RETRY_DELAY;
	int MOVE_KEYS_KRM_LIMIT;
	int MOVE_KEYS_KRM_LIMIT_BYTES; //This must be sufficiently larger than CLIENT_KNOBS->KEY_SIZE_LIMIT (fdbclient/Knobs.h) to ensure that at least two entries will be returned from an attempt to read a key range map
	int MAX_SKIP_TAGS;
	double MAX_ADDED_SOURCES_MULTIPLIER;

	//FdbServer
	double MIN_REBOOT_TIME;
	double MAX_REBOOT_TIME;
	std::string LOG_DIRECTORY;
	int64_t SERVER_MEM_LIMIT;

	//Ratekeeper
	double SMOOTHING_AMOUNT;
	double SLOW_SMOOTHING_AMOUNT;
	double METRIC_UPDATE_RATE;
	double DETAILED_METRIC_UPDATE_RATE;
	double LAST_LIMITED_RATIO;
	double RATEKEEPER_DEFAULT_LIMIT;

	int64_t TARGET_BYTES_PER_STORAGE_SERVER;
	int64_t SPRING_BYTES_STORAGE_SERVER;
	int64_t AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES;
	int64_t TARGET_BYTES_PER_STORAGE_SERVER_BATCH;
	int64_t SPRING_BYTES_STORAGE_SERVER_BATCH;

	int64_t TARGET_BYTES_PER_TLOG;
	int64_t SPRING_BYTES_TLOG;
	int64_t TARGET_BYTES_PER_TLOG_BATCH;
	int64_t SPRING_BYTES_TLOG_BATCH;
	int64_t TLOG_SPILL_THRESHOLD;
	int64_t TLOG_HARD_LIMIT_BYTES;
	int64_t TLOG_RECOVER_MEMORY_LIMIT;
	double TLOG_IGNORE_POP_AUTO_ENABLE_DELAY;

	int64_t MAX_MANUAL_THROTTLED_TRANSACTION_TAGS;
	int64_t MAX_AUTO_THROTTLED_TRANSACTION_TAGS;
	double MIN_TAG_COST;
	double AUTO_THROTTLE_TARGET_TAG_BUSYNESS;
	double AUTO_THROTTLE_RAMP_TAG_BUSYNESS;
	double AUTO_TAG_THROTTLE_RAMP_UP_TIME;
	double AUTO_TAG_THROTTLE_DURATION;
	double TAG_THROTTLE_PUSH_INTERVAL;
	double AUTO_TAG_THROTTLE_START_AGGREGATION_TIME;
	double AUTO_TAG_THROTTLE_UPDATE_FREQUENCY;
	double TAG_THROTTLE_EXPIRED_CLEANUP_INTERVAL;
	bool AUTO_TAG_THROTTLING_ENABLED;

	double MAX_TRANSACTIONS_PER_BYTE;

	int64_t MIN_AVAILABLE_SPACE;
	double MIN_AVAILABLE_SPACE_RATIO;
	double TARGET_AVAILABLE_SPACE_RATIO;
	double AVAILABLE_SPACE_UPDATE_DELAY;

	double MAX_TL_SS_VERSION_DIFFERENCE; // spring starts at half this value
	double MAX_TL_SS_VERSION_DIFFERENCE_BATCH;
	int MAX_MACHINES_FALLING_BEHIND;

	int MAX_TPS_HISTORY_SAMPLES;
	int NEEDED_TPS_HISTORY_SAMPLES;
	int64_t TARGET_DURABILITY_LAG_VERSIONS;
	int64_t AUTO_TAG_THROTTLE_DURABILITY_LAG_VERSIONS;
	int64_t TARGET_DURABILITY_LAG_VERSIONS_BATCH;
	int64_t DURABILITY_LAG_UNLIMITED_THRESHOLD;
	double INITIAL_DURABILITY_LAG_MULTIPLIER;
	double DURABILITY_LAG_REDUCTION_RATE;
	double DURABILITY_LAG_INCREASE_RATE;

	double STORAGE_SERVER_LIST_FETCH_TIMEOUT;

	// disk snapshot
	double SNAP_CREATE_MAX_TIMEOUT;

	//Storage Metrics
	double STORAGE_METRICS_AVERAGE_INTERVAL;
	double STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
	double SPLIT_JITTER_AMOUNT;
	int64_t IOPS_UNITS_PER_SAMPLE;
	int64_t BANDWIDTH_UNITS_PER_SAMPLE;
	int64_t BYTES_READ_UNITS_PER_SAMPLE;
	int64_t READ_HOT_SUB_RANGE_CHUNK_SIZE;
	int64_t EMPTY_READ_PENALTY;
	bool READ_SAMPLING_ENABLED;

	//Storage Server
	double STORAGE_LOGGING_DELAY;
	double STORAGE_SERVER_POLL_METRICS_DELAY;
	double FUTURE_VERSION_DELAY;
	int STORAGE_LIMIT_BYTES;
	int BUGGIFY_LIMIT_BYTES;
	int FETCH_BLOCK_BYTES;
	int FETCH_KEYS_PARALLELISM_BYTES;
	int FETCH_KEYS_LOWER_PRIORITY;
	int BUGGIFY_BLOCK_BYTES;
	int64_t STORAGE_HARD_LIMIT_BYTES;
	int64_t STORAGE_DURABILITY_LAG_HARD_MAX;
	int64_t STORAGE_DURABILITY_LAG_SOFT_MAX;
	double STORAGE_DURABILITY_LAG_REJECT_THRESHOLD;
	double STORAGE_DURABILITY_LAG_MIN_RATE;
	int STORAGE_COMMIT_BYTES;
	double STORAGE_COMMIT_INTERVAL;
	double UPDATE_SHARD_VERSION_INTERVAL;
	int BYTE_SAMPLING_FACTOR;
	int BYTE_SAMPLING_OVERHEAD;
	int MAX_STORAGE_SERVER_WATCH_BYTES;
	int MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE;
	double LONG_BYTE_SAMPLE_RECOVERY_DELAY;
	int BYTE_SAMPLE_LOAD_PARALLELISM;
	double BYTE_SAMPLE_LOAD_DELAY;
	double BYTE_SAMPLE_START_DELAY;
	double UPDATE_STORAGE_PROCESS_STATS_INTERVAL;
	double BEHIND_CHECK_DELAY;
	int BEHIND_CHECK_COUNT;
	int64_t BEHIND_CHECK_VERSIONS;
	double WAIT_METRICS_WRONG_SHARD_CHANCE;
	int64_t MIN_TAG_READ_PAGES_RATE;
	int64_t MIN_TAG_WRITE_PAGES_RATE;
	double TAG_MEASUREMENT_INTERVAL;
	int64_t READ_COST_BYTE_FACTOR;
	bool PREFIX_COMPRESS_KVS_MEM_SNAPSHOTS;
	bool REPORT_DD_METRICS;
	double DD_METRICS_REPORT_INTERVAL;
	double FETCH_KEYS_TOO_LONG_TIME_CRITERIA;

	//Wait Failure
	int MAX_OUTSTANDING_WAIT_FAILURE_REQUESTS;
	double WAIT_FAILURE_DELAY_LIMIT;

	//Worker
	double WORKER_LOGGING_INTERVAL;
	double HEAP_PROFILER_INTERVAL;
	double DEGRADED_RESET_INTERVAL;
	double DEGRADED_WARNING_LIMIT;
	double DEGRADED_WARNING_RESET_DELAY;
	int64_t TRACE_LOG_FLUSH_FAILURE_CHECK_INTERVAL_SECONDS;
	double TRACE_LOG_PING_TIMEOUT_SECONDS;
	double MIN_DELAY_CC_WORST_FIT_CANDIDACY_SECONDS;  // Listen for a leader for N seconds, and if not heard, then try to become the leader.
	double MAX_DELAY_CC_WORST_FIT_CANDIDACY_SECONDS;
	double DBINFO_FAILED_DELAY;

	// Test harness
	double WORKER_POLL_DELAY;

	// Coordination
	double COORDINATED_STATE_ONCONFLICT_POLL_INTERVAL;

	// Buggification
	double BUGGIFIED_EVENTUAL_CONSISTENCY;
	bool BUGGIFY_ALL_COORDINATION;

	// Status
	double STATUS_MIN_TIME_BETWEEN_REQUESTS;
	double MAX_STATUS_REQUESTS_PER_SECOND;
	int CONFIGURATION_ROWS_TO_FETCH;
	bool DISABLE_DUPLICATE_LOG_WARNING;
	double HISTOGRAM_REPORT_INTERVAL;

	// IPager
	int PAGER_RESERVED_PAGES;

	// IndirectShadowPager
	int FREE_PAGE_VACUUM_THRESHOLD;
	int VACUUM_QUEUE_SIZE;
	int VACUUM_BYTES_PER_SECOND;

	// Timekeeper
	int64_t TIME_KEEPER_DELAY;
	int64_t TIME_KEEPER_MAX_ENTRIES;

	// Fast Restore
	// TODO: After 6.3, review FR knobs, remove unneeded ones and change default value
	int64_t FASTRESTORE_FAILURE_TIMEOUT;
	int64_t FASTRESTORE_HEARTBEAT_INTERVAL;
	double FASTRESTORE_SAMPLING_PERCENT;
	int64_t FASTRESTORE_NUM_LOADERS;
	int64_t FASTRESTORE_NUM_APPLIERS;
	// FASTRESTORE_TXN_BATCH_MAX_BYTES is target txn size used by appliers to apply mutations
	double FASTRESTORE_TXN_BATCH_MAX_BYTES;
	// FASTRESTORE_VERSIONBATCH_MAX_BYTES is the maximum data size in each version batch
	double FASTRESTORE_VERSIONBATCH_MAX_BYTES;
	// FASTRESTORE_VB_PARALLELISM is the number of concurrently running version batches
	int64_t FASTRESTORE_VB_PARALLELISM;
	int64_t FASTRESTORE_VB_MONITOR_DELAY; // How quickly monitor finished version batch
	double FASTRESTORE_VB_LAUNCH_DELAY;
	int64_t FASTRESTORE_ROLE_LOGGING_DELAY;
	int64_t FASTRESTORE_UPDATE_PROCESS_STATS_INTERVAL; // How quickly to update process metrics for restore
	int64_t FASTRESTORE_ATOMICOP_WEIGHT; // workload amplication factor for atomic op
	int64_t FASTRESTORE_APPLYING_PARALLELISM; // number of outstanding txns writing to dest. DB
	int64_t FASTRESTORE_MONITOR_LEADER_DELAY;
	int64_t FASTRESTORE_STRAGGLER_THRESHOLD_SECONDS;
	bool FASTRESTORE_TRACK_REQUEST_LATENCY; // true to track reply latency of each request in a request batch
	bool FASTRESTORE_TRACK_LOADER_SEND_REQUESTS; // track requests of load send mutations to appliers?
	int64_t FASTRESTORE_MEMORY_THRESHOLD_MB_SOFT; // threshold when pipelined actors should be delayed
	int64_t FASTRESTORE_WAIT_FOR_MEMORY_LATENCY;
	int64_t FASTRESTORE_HEARTBEAT_DELAY; // interval for master to ping loaders and appliers
	int64_t FASTRESTORE_HEARTBEAT_MAX_DELAY; // master claim a node is down if no heart beat from the node for this delay
	int64_t FASTRESTORE_APPLIER_FETCH_KEYS_SIZE; // number of keys to fetch in a txn on applier
	int64_t FASTRESTORE_LOADER_SEND_MUTATION_MSG_BYTES; // desired size of mutation message sent from loader to appliers
	bool FASTRESTORE_GET_RANGE_VERSIONS_EXPENSIVE; // parse each range file to get (range, version) it has?
	int64_t FASTRESTORE_REQBATCH_PARALLEL; // number of requests to wait on for getBatchReplies()
	bool FASTRESTORE_REQBATCH_LOG; // verbose log information for getReplyBatches
	int FASTRESTORE_TXN_CLEAR_MAX; // threshold to start tracking each clear op in a txn
	int FASTRESTORE_TXN_RETRY_MAX; // threshold to start output error on too many retries
	double FASTRESTORE_TXN_EXTRA_DELAY; // extra delay to avoid overwhelming fdb
	bool FASTRESTORE_NOT_WRITE_DB; // do not write result to DB. Only for dev testing
	bool FASTRESTORE_USE_RANGE_FILE; // use range file in backup
	bool FASTRESTORE_USE_LOG_FILE; // use log file in backup
	int64_t FASTRESTORE_SAMPLE_MSG_BYTES; // sample message desired size
	double FASTRESTORE_SCHED_UPDATE_DELAY; // delay in seconds in updating process metrics
	int FASTRESTORE_SCHED_TARGET_CPU_PERCENT; // release as many requests as possible when cpu usage is below the knob
	int FASTRESTORE_SCHED_MAX_CPU_PERCENT; // max cpu percent when scheduler shall not release non-urgent requests
	int FASTRESTORE_SCHED_INFLIGHT_LOAD_REQS; // number of inflight requests to load backup files
	int FASTRESTORE_SCHED_INFLIGHT_SEND_REQS; // number of inflight requests for loaders to  send mutations to appliers
	int FASTRESTORE_SCHED_LOAD_REQ_BATCHSIZE; // number of load request to release at once
	int FASTRESTORE_SCHED_INFLIGHT_SENDPARAM_THRESHOLD; // we can send future VB requests if it is less than this knob
	int FASTRESTORE_SCHED_SEND_FUTURE_VB_REQS_BATCH; // number of future VB sendLoadingParam requests to process at once
	int FASTRESTORE_NUM_TRACE_EVENTS;
	bool FASTRESTORE_EXPENSIVE_VALIDATION; // when set true, performance will be heavily affected
	double FASTRESTORE_WRITE_BW_MB; // target aggregated write bandwidth from all appliers
	double FASTRESTORE_RATE_UPDATE_SECONDS; // how long to update appliers target write rate

	int REDWOOD_DEFAULT_PAGE_SIZE;  // Page size for new Redwood files
	int REDWOOD_KVSTORE_CONCURRENT_READS;  // Max number of simultaneous point or range reads in progress.
	int REDWOOD_COMMIT_CONCURRENT_READS;   // Max number of concurrent reads done to support commit operations
	double REDWOOD_PAGE_REBUILD_FILL_FACTOR; // When rebuilding pages, start a new page after this capacity
	int REDWOOD_LAZY_CLEAR_BATCH_SIZE_PAGES; // Number of pages to try to pop from the lazy delete queue and process at once
	int REDWOOD_LAZY_CLEAR_MIN_PAGES;  // Minimum number of pages to free before ending a lazy clear cycle, unless the queue is empty
	int REDWOOD_LAZY_CLEAR_MAX_PAGES;  // Maximum number of pages to free before ending a lazy clear cycle, unless the queue is empty
	int64_t REDWOOD_REMAP_CLEANUP_WINDOW;  // Remap remover lag interval in which to coalesce page writes
	double REDWOOD_REMAP_CLEANUP_LAG; // Maximum allowed remap remover lag behind the cleanup window as a multiple of the window size
	double REDWOOD_LOGGING_INTERVAL;

	// Server request latency measurement
	int LATENCY_SAMPLE_SIZE;
	double LATENCY_METRICS_LOGGING_INTERVAL;

	ServerKnobs();
	void initialize(bool randomize = false, ClientKnobs* clientKnobs = nullptr, bool isSimulated = false);
};

extern ServerKnobs const* SERVER_KNOBS;

#endif
