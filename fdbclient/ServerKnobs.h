/*
 * ServerKnobs.h
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

#pragma once

#include "flow/BooleanParam.h"
#include "flow/Knobs.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbclient/ClientKnobs.h"

// Disk queue
static constexpr int _PAGE_SIZE = 4096;

class ServerKnobs : public KnobsImpl<ServerKnobs> {
public:
	bool ALLOW_DANGEROUS_KNOBS;
	// Versions
	int64_t VERSIONS_PER_SECOND;
	int64_t MAX_VERSIONS_IN_FLIGHT;
	int64_t MAX_VERSIONS_IN_FLIGHT_FORCED;
	int64_t MAX_READ_TRANSACTION_LIFE_VERSIONS;
	int64_t MAX_WRITE_TRANSACTION_LIFE_VERSIONS;
	bool ENABLE_VERSION_VECTOR;
	bool ENABLE_VERSION_VECTOR_TLOG_UNICAST;
	double MAX_COMMIT_BATCH_INTERVAL; // Each commit proxy generates a CommitTransactionBatchRequest at least this
	                                  // often, so that versions always advance smoothly
	double MAX_VERSION_RATE_MODIFIER;
	int64_t MAX_VERSION_RATE_OFFSET;
	bool ENABLE_VERSION_VECTOR_HA_OPTIMIZATION;

	// TLogs
	bool PEEK_USING_STREAMING;
	double TLOG_TIMEOUT; // tlog OR commit proxy failure - master's reaction time
	double TLOG_SLOW_REJOIN_WARN_TIMEOUT_SECS; // Warns if a tlog takes too long to rejoin
	double RECOVERY_TLOG_SMART_QUORUM_DELAY; // smaller might be better for bug amplification
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
	int VERSION_MESSAGES_OVERHEAD_FACTOR_1024THS; // Multiplicative factor to bound total space used to store a version
	                                              // message (measured in 1/1024ths, e.g. a value of 2048 yields a
	                                              // factor of 2).
	int64_t VERSION_MESSAGES_ENTRY_BYTES_WITH_OVERHEAD;
	int64_t TLOG_POPPED_VER_LAG_THRESHOLD_FOR_TLOGPOP_TRACE;
	bool ENABLE_DETAILED_TLOG_POP_TRACE;
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
	double LOG_ROUTER_PEEK_SWITCH_DC_TIME;
	double DISK_QUEUE_ADAPTER_MIN_SWITCH_TIME;
	double DISK_QUEUE_ADAPTER_MAX_SWITCH_TIME;
	int64_t TLOG_SPILL_REFERENCE_MAX_PEEK_MEMORY_BYTES;
	int64_t TLOG_SPILL_REFERENCE_MAX_BATCHES_PER_PEEK;
	int64_t TLOG_SPILL_REFERENCE_MAX_BYTES_PER_BATCH;
	int64_t DISK_QUEUE_FILE_EXTENSION_BYTES; // When we grow the disk queue, by how many bytes should it grow?
	int64_t DISK_QUEUE_FILE_SHRINK_BYTES; // When we shrink the disk queue, by how many bytes should it shrink?
	int64_t DISK_QUEUE_MAX_TRUNCATE_BYTES; // A truncate larger than this will cause the file to be replaced instead.
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
	double BLOCKING_PEEK_TIMEOUT;
	bool PEEK_BATCHING_EMPTY_MSG;
	double PEEK_BATCHING_EMPTY_MSG_INTERVAL;
	double POP_FROM_LOG_DELAY;
	double TLOG_PULL_ASYNC_DATA_WARNING_TIMEOUT_SECS;

	// Data distribution queue
	double HEALTH_POLL_TIME;
	double BEST_TEAM_STUCK_DELAY;
	double DEST_OVERLOADED_DELAY;
	double BG_REBALANCE_POLLING_INTERVAL;
	double BG_REBALANCE_SWITCH_CHECK_INTERVAL;
	double DD_QUEUE_LOGGING_INTERVAL;
	double RELOCATION_PARALLELISM_PER_SOURCE_SERVER;
	double RELOCATION_PARALLELISM_PER_DEST_SERVER;
	double MANUAL_SPLIT_RELOCATION_PARALLELISM_PER_DEST_SERVER;
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
	int PRIORITY_PERPETUAL_STORAGE_WIGGLE;
	int PRIORITY_TEAM_HEALTHY;
	int PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER;
	int PRIORITY_TEAM_REDUNDANT;
	int PRIORITY_MERGE_SHARD;
	int PRIORITY_POPULATE_REGION;
	int PRIORITY_TEAM_STORAGE_QUEUE_TOO_LONG;
	int PRIORITY_TEAM_UNHEALTHY;
	int PRIORITY_TEAM_2_LEFT;
	int PRIORITY_TEAM_1_LEFT;
	int PRIORITY_TEAM_FAILED; // Priority when a server in the team is excluded as failed
	int PRIORITY_TEAM_0_LEFT;
	int PRIORITY_MANUAL_SHARD_SPLIT; // Priority when a server has a long storage queue
	int PRIORITY_SPLIT_SHARD;

	// Data distribution
	double RETRY_RELOCATESHARD_DELAY;
	double DATA_DISTRIBUTION_FAILURE_REACTION_TIME;
	int MIN_SHARD_BYTES, SHARD_BYTES_RATIO, SHARD_BYTES_PER_SQRT_BYTES, MAX_SHARD_BYTES, KEY_SERVER_SHARD_BYTES;
	int64_t SHARD_MAX_BYTES_PER_KSEC, // Shards with more than this bandwidth will be split immediately
	    SHARD_MIN_BYTES_PER_KSEC, // Shards with more than this bandwidth will not be merged
	    SHARD_SPLIT_BYTES_PER_KSEC; // When splitting a shard, it is split into pieces with less than this bandwidth
	double SHARD_MAX_READ_DENSITY_RATIO;
	int64_t SHARD_READ_HOT_BANDWITH_MIN_PER_KSECONDS;
	double SHARD_MAX_BYTES_READ_PER_KSEC_JITTER;
	double STORAGE_METRIC_TIMEOUT;
	double METRIC_DELAY;
	double ALL_DATA_REMOVED_DELAY;
	double INITIAL_FAILURE_REACTION_DELAY;
	double CHECK_TEAM_DELAY; // Perpetual wiggle check cluster team healthy
	double PERPETUAL_WIGGLE_SMALL_LOAD_RATIO; // If the average load of storage server is less than this ratio * average
	                                          // shard bytes, the perpetual wiggle won't consider the available space
	                                          // load balance in the cluster
	double PERPETUAL_WIGGLE_MIN_BYTES_BALANCE_RATIO; // target min : average space load balance ratio after re-include
	                                                 // before perpetual wiggle will start the next wiggle
	int PW_MAX_SS_LESSTHAN_MIN_BYTES_BALANCE_RATIO; // Maximum number of storage servers that can have the load bytes
	                                                // less than PERPETUAL_WIGGLE_MIN_BYTES_BALANCE_RATIO before
	                                                // perpetual wiggle will start the next wiggle.
	                                                // Used to speed up wiggling rather than waiting for every SS to get
	                                                // balanced/filledup before starting the next wiggle.
	double PERPETUAL_WIGGLE_DELAY; // The min interval between the last wiggle finish and the next wiggle start
	bool PERPETUAL_WIGGLE_DISABLE_REMOVER; // Whether the start of perpetual wiggle replace team remover
	double LOG_ON_COMPLETION_DELAY;
	int BEST_TEAM_MAX_TEAM_TRIES;
	int BEST_TEAM_OPTION_COUNT;
	int BEST_OF_AMT;
	double SERVER_LIST_DELAY;
	double RECRUITMENT_IDLE_DELAY;
	double STORAGE_RECRUITMENT_DELAY;
	double BLOB_WORKER_RECRUITMENT_DELAY;
	bool TSS_HACK_IDENTITY_MAPPING;
	double TSS_RECRUITMENT_TIMEOUT;
	double TSS_DD_CHECK_INTERVAL;
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
	int64_t
	    DD_SS_FAILURE_VERSIONLAG; // Allowed SS version lag from the current read version before marking it as failed.
	int64_t DD_SS_ALLOWED_VERSIONLAG; // SS will be marked as healthy if it's version lag goes below this value.
	double DD_SS_STUCK_TIME_LIMIT; // If a storage server is not getting new versions for this amount of time, then it
	                               // becomes undesired.
	int DD_TEAMS_INFO_PRINT_INTERVAL;
	int DD_TEAMS_INFO_PRINT_YIELD_COUNT;
	int DD_TEAM_ZERO_SERVER_LEFT_LOG_DELAY;
	int DD_STORAGE_WIGGLE_PAUSE_THRESHOLD; // How many unhealthy relocations are ongoing will pause storage wiggle
	int DD_STORAGE_WIGGLE_STUCK_THRESHOLD; // How many times bestTeamStuck accumulate will pause storage wiggle
	bool ENABLE_STORAGE_QUEUE_AWARE_TEAM_SELECTION; // experimental!
	int64_t DD_TARGET_STORAGE_QUEUE_SIZE;
	bool TRACE_STORAGE_QUEUE_AWARE_GET_TEAM_FOR_MANUAL_SPLIT_ONLY;
	bool ENABLE_AUTO_SHARD_SPLIT_FOR_LONG_STORAGE_QUEUE;
	int64_t DD_SS_TOO_LONG_STORAGE_QUEUE_BYTES;
	int64_t DD_SS_SHORT_STORAGE_QUEUE_BYTES;
	double DD_STORAGE_QUEUE_TOO_LONG_DURATION;
	double DD_MIN_LONG_STORAGE_QUEUE_SPLIT_INTERVAL_SEC;
	int64_t DD_MIN_SHARD_BYTES_PER_KSEC_TO_MOVE_OUT;

	// TeamRemover to remove redundant teams
	bool TR_FLAG_DISABLE_MACHINE_TEAM_REMOVER; // disable the machineTeamRemover actor
	double TR_REMOVE_MACHINE_TEAM_DELAY; // wait for the specified time before try to remove next machine team
	bool TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS; // guard to select which machineTeamRemover logic to use

	bool TR_FLAG_DISABLE_SERVER_TEAM_REMOVER; // disable the serverTeamRemover actor
	double TR_REMOVE_SERVER_TEAM_DELAY; // wait for the specified time before try to remove next server team
	double TR_REMOVE_SERVER_TEAM_EXTRA_DELAY; // serverTeamRemover waits for the delay and check DD healthyness again to
	                                          // ensure it runs after machineTeamRemover

	// Remove wrong storage engines
	double DD_REMOVE_STORE_ENGINE_DELAY; // wait for the specified time before remove the next batch

	double DD_FAILURE_TIME;
	double DD_ZERO_HEALTHY_TEAM_DELAY;
	int DD_BUILD_EXTRA_TEAMS_OVERRIDE; // build extra teams to allow data movement to progress. must be larger than 0
	bool DD_REMOVE_MAINTENANCE_ON_FAILURE; // If set to true DD will remove the maintenance mode if another SS fails
	                                       // outside of the maintenance zone.

	// Run storage enginee on a child process on the same machine with storage process
	bool REMOTE_KV_STORE;
	// A delay to avoid race on file resources if the new kv store process started immediately after the previous kv
	// store process died
	double REMOTE_KV_STORE_INIT_DELAY;
	// max waiting time for the remote kv store to initialize
	double REMOTE_KV_STORE_MAX_INIT_DURATION;

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
	int SQLITE_READER_THREADS;
	int SQLITE_WRITE_WINDOW_LIMIT;
	double SQLITE_WRITE_WINDOW_SECONDS;
	int64_t SQLITE_CURSOR_MAX_LIFETIME_BYTES;

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
	bool ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES;
	bool ROCKSDB_SUGGEST_COMPACT_CLEAR_RANGE;
	int ROCKSDB_THREAD_PROMISE_PRIORITY;
	int ROCKSDB_READER_THREAD_PRIORITY;
	int ROCKSDB_WRITER_THREAD_PRIORITY;
	int ROCKSDB_BACKGROUND_PARALLELISM;
	int ROCKSDB_READ_PARALLELISM;
	int64_t ROCKSDB_MEMTABLE_BYTES;
	bool ROCKSDB_LEVEL_STYLE_COMPACTION;
	bool ROCKSDB_UNSAFE_AUTO_FSYNC;
	int64_t ROCKSDB_PERIODIC_COMPACTION_SECONDS;
	int ROCKSDB_PREFIX_LEN;
	double ROCKSDB_MEMTABLE_PREFIX_BLOOM_SIZE_RATIO;
	int ROCKSDB_BLOOM_BITS_PER_KEY;
	bool ROCKSDB_BLOOM_WHOLE_KEY_FILTERING;
	int ROCKSDB_MAX_AUTO_READAHEAD_SIZE;
	int64_t ROCKSDB_BLOCK_CACHE_SIZE;
	double ROCKSDB_METRICS_DELAY;
	double ROCKSDB_READ_VALUE_TIMEOUT;
	double ROCKSDB_READ_VALUE_PREFIX_TIMEOUT;
	double ROCKSDB_READ_RANGE_TIMEOUT;
	double ROCKSDB_READ_QUEUE_WAIT;
	int ROCKSDB_READ_QUEUE_SOFT_MAX;
	int ROCKSDB_READ_QUEUE_HARD_MAX;
	int ROCKSDB_FETCH_QUEUE_SOFT_MAX;
	int ROCKSDB_FETCH_QUEUE_HARD_MAX;
	// These histograms are in read and write path which can cause performance overhead.
	// Set to 0 to disable histograms.
	double ROCKSDB_HISTOGRAMS_SAMPLE_RATE;
	double ROCKSDB_READ_RANGE_ITERATOR_REFRESH_TIME;
	bool ROCKSDB_READ_RANGE_REUSE_ITERATORS;
	bool ROCKSDB_READ_RANGE_REUSE_BOUNDED_ITERATORS;
	int ROCKSDB_READ_RANGE_BOUNDED_ITERATORS_MAX_LIMIT;
	int64_t ROCKSDB_WRITE_RATE_LIMITER_BYTES_PER_SEC;
	int ROCKSDB_WRITE_RATE_LIMITER_FAIRNESS;
	bool ROCKSDB_WRITE_RATE_LIMITER_AUTO_TUNE;
	std::string DEFAULT_FDB_ROCKSDB_COLUMN_FAMILY;
	bool ROCKSDB_DISABLE_AUTO_COMPACTIONS;
	bool ROCKSDB_PERFCONTEXT_ENABLE; // Enable rocks perf context metrics. May cause performance overhead
	double ROCKSDB_PERFCONTEXT_SAMPLE_RATE;
	int ROCKSDB_MAX_SUBCOMPACTIONS;
	int64_t ROCKSDB_SOFT_PENDING_COMPACT_BYTES_LIMIT;
	int64_t ROCKSDB_HARD_PENDING_COMPACT_BYTES_LIMIT;
	int64_t ROCKSDB_CAN_COMMIT_COMPACT_BYTES_LIMIT;
	bool ROCKSDB_PARANOID_FILE_CHECKS;
	double ROCKSDB_CAN_COMMIT_DELAY_ON_OVERLOAD;
	int ROCKSDB_CAN_COMMIT_DELAY_TIMES_ON_OVERLOAD;
	bool ROCKSDB_DISABLE_WAL_EXPERIMENTAL;
	int64_t ROCKSDB_WAL_TTL_SECONDS;
	int64_t ROCKSDB_WAL_SIZE_LIMIT_MB;
	bool ROCKSDB_LOG_LEVEL_DEBUG;
	bool ROCKSDB_SINGLEKEY_DELETES_ON_CLEARRANGE;
	int ROCKSDB_SINGLEKEY_DELETES_MAX;
	bool ROCKSDB_ENABLE_CLEAR_RANGE_EAGER_READS;
	bool ROCKSDB_FORCE_DELETERANGE_FOR_CLEARRANGE;
	bool ROCKSDB_ENABLE_COMPACT_ON_DELETION;
	int64_t ROCKSDB_CDCF_SLIDING_WINDOW_SIZE; // CDCF: CompactOnDeletionCollectorFactory
	int64_t ROCKSDB_CDCF_DELETION_TRIGGER; // CDCF: CompactOnDeletionCollectorFactory
	double ROCKSDB_CDCF_DELETION_RATIO; // CDCF: CompactOnDeletionCollectorFactory
	int ROCKSDB_STATS_LEVEL;
	int64_t ROCKSDB_COMPACTION_READAHEAD_SIZE;
	int64_t ROCKSDB_BLOCK_SIZE;
	int64_t ROCKSDB_WRITE_BUFFER_SIZE;
	int ROCKSDB_MAX_WRITE_BUFFER_NUMBER;
	int ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE;
	int ROCKSDB_LEVEL0_FILENUM_COMPACTION_TRIGGER;
	int ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
	int ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER;
	int ROCKSDB_TARGET_FILE_SIZE_BASE;
	int ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER;
	int ROCKSDB_MAX_LOG_FILE_SIZE;
	int ROCKSDB_KEEP_LOG_FILE_NUM;
	bool SS_BACKUP_KEYS_OP_LOGS;

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
	double COMMIT_PROXY_LIVENESS_TIMEOUT;

	double COMMIT_TRANSACTION_BATCH_INTERVAL_FROM_IDLE;
	double COMMIT_TRANSACTION_BATCH_INTERVAL_MIN;
	double COMMIT_TRANSACTION_BATCH_INTERVAL_MAX;
	double COMMIT_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION;
	double COMMIT_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA;
	int COMMIT_TRANSACTION_BATCH_COUNT_MAX;
	int COMMIT_TRANSACTION_BATCH_BYTES_MIN;
	int COMMIT_TRANSACTION_BATCH_BYTES_MAX;
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
	double MAX_COMPUTE_DURATION_LOG_CUTOFF;
	int PROXY_COMPUTE_BUCKETS;
	double PROXY_COMPUTE_GROWTH_RATE;
	int TXN_STATE_SEND_AMOUNT;
	double REPORT_TRANSACTION_COST_ESTIMATION_DELAY;
	bool PROXY_REJECT_BATCH_QUEUED_TOO_LONG;
	bool PROXY_USE_RESOLVER_PRIVATE_MUTATIONS;

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
	double BACKUP_TIMEOUT; // master's reaction time for backup failure
	double BACKUP_NOOP_POP_DELAY;
	int BACKUP_FILE_BLOCK_BYTES;
	int64_t BACKUP_LOCK_BYTES;
	double BACKUP_UPLOAD_DELAY;

	// Cluster Controller
	double CLUSTER_CONTROLLER_LOGGING_DELAY;
	double MASTER_FAILURE_REACTION_TIME;
	double MASTER_FAILURE_SLOPE_DURING_RECOVERY;
	int WORKER_COORDINATION_PING_DELAY;
	double SIM_SHUTDOWN_TIMEOUT;
	double SHUTDOWN_TIMEOUT;
	double MASTER_SPIN_DELAY;
	double CC_PRUNE_CLIENTS_INTERVAL;
	double CC_CHANGE_DELAY;
	double CC_CLASS_DELAY;
	double WAIT_FOR_GOOD_RECRUITMENT_DELAY;
	double WAIT_FOR_GOOD_REMOTE_RECRUITMENT_DELAY;
	double ATTEMPT_RECRUITMENT_DELAY;
	double WAIT_FOR_DISTRIBUTOR_JOIN_DELAY;
	double WAIT_FOR_RATEKEEPER_JOIN_DELAY;
	double WAIT_FOR_BLOB_MANAGER_JOIN_DELAY;
	double WAIT_FOR_ENCRYPT_KEY_PROXY_JOIN_DELAY;
	double WORKER_FAILURE_TIME;
	double CHECK_OUTSTANDING_INTERVAL;
	double INCOMPATIBLE_PEERS_LOGGING_INTERVAL;
	double VERSION_LAG_METRIC_INTERVAL;
	int64_t MAX_VERSION_DIFFERENCE;
	double INITIAL_UPDATE_CROSS_DC_INFO_DELAY; // The intial delay in a new Cluster Controller just started to refresh
	                                           // the info of remote DC, such as remote DC health, and whether we need
	                                           // to take remote DC health info when making failover decision.
	double CHECK_REMOTE_HEALTH_INTERVAL; // Remote DC health refresh interval.
	double FORCE_RECOVERY_CHECK_DELAY;
	double RATEKEEPER_FAILURE_TIME;
	double BLOB_MANAGER_FAILURE_TIME;
	double REPLACE_INTERFACE_DELAY;
	double REPLACE_INTERFACE_CHECK_DELAY;
	double COORDINATOR_REGISTER_INTERVAL;
	double CLIENT_REGISTER_INTERVAL;
	bool CC_ENABLE_WORKER_HEALTH_MONITOR;
	double CC_WORKER_HEALTH_CHECKING_INTERVAL; // The interval of refreshing the degraded server list.
	double CC_DEGRADED_LINK_EXPIRATION_INTERVAL; // The time period from the last degradation report after which a
	                                             // degraded server is considered healthy.
	double CC_MIN_DEGRADATION_INTERVAL; // The minimum interval that a server is reported as degraded to be considered
	                                    // as degraded by Cluster Controller.
	double ENCRYPT_KEY_PROXY_FAILURE_TIME;
	int CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE; // The maximum number of degraded peers when excluding a server. When the
	                                        // number of degraded peers is more than this value, we will not exclude
	                                        // this server since it may because of server overload.
	int CC_MAX_EXCLUSION_DUE_TO_HEALTH; // The max number of degraded servers to exclude by Cluster Controller due to
	                                    // degraded health.
	bool CC_HEALTH_TRIGGER_RECOVERY; // If true, cluster controller will kill the master to trigger recovery when
	                                 // detecting degraded servers. If false, cluster controller only prints a warning.
	double CC_TRACKING_HEALTH_RECOVERY_INTERVAL; // The number of recovery count should not exceed
	                                             // CC_MAX_HEALTH_RECOVERY_COUNT within
	                                             // CC_TRACKING_HEALTH_RECOVERY_INTERVAL.
	int CC_MAX_HEALTH_RECOVERY_COUNT; // The max number of recoveries can be triggered due to worker health within
	                                  // CC_TRACKING_HEALTH_RECOVERY_INTERVAL
	bool CC_HEALTH_TRIGGER_FAILOVER; // Whether to enable health triggered failover in CC.
	int CC_FAILOVER_DUE_TO_HEALTH_MIN_DEGRADATION; // The minimum number of degraded servers that can trigger a
	                                               // failover.
	int CC_FAILOVER_DUE_TO_HEALTH_MAX_DEGRADATION; // The maximum number of degraded servers that can trigger a
	                                               // failover.
	bool CC_ENABLE_ENTIRE_SATELLITE_MONITORING; // When enabled, gray failure tries to detect whether the entire
	                                            // satellite DC is degraded.
	int CC_SATELLITE_DEGRADATION_MIN_COMPLAINER; // When the network between primary and satellite becomes bad, all the
	                                             // workers in primary may have bad network talking to the satellite.
	                                             // This is the minimum amount of complainer for a satellite worker to
	                                             // be determined as degraded worker.
	int CC_SATELLITE_DEGRADATION_MIN_BAD_SERVER; // The minimum amount of degraded server in satellite DC to be
	                                             // determined as degraded satellite.
	bool CC_ENABLE_REMOTE_LOG_ROUTER_MONITORING; // When enabled, gray failure tries to detect whether the remote log
	                                             // router is degraded and may use trigger recovery to recover from it.
	double CC_THROTTLE_SINGLETON_RERECRUIT_INTERVAL; // The interval to prevent re-recruiting the same singleton if a
	                                                 // recruiting fight between two cluster controllers occurs.

	// Knobs used to select the best policy (via monte carlo)
	int POLICY_RATING_TESTS; // number of tests per policy (in order to compare)
	int POLICY_GENERATIONS; // number of policies to generate

	int EXPECTED_MASTER_FITNESS;
	int EXPECTED_TLOG_FITNESS;
	int EXPECTED_LOG_ROUTER_FITNESS;
	int EXPECTED_COMMIT_PROXY_FITNESS;
	int EXPECTED_GRV_PROXY_FITNESS;
	int EXPECTED_RESOLVER_FITNESS;
	double RECRUITMENT_TIMEOUT;
	int DBINFO_SEND_AMOUNT;
	double DBINFO_BATCH_DELAY;
	double SINGLETON_RECRUIT_BME_DELAY;

	// Move Keys
	double SHARD_READY_DELAY;
	double SERVER_READY_QUORUM_INTERVAL;
	double SERVER_READY_QUORUM_TIMEOUT;
	double REMOVE_RETRY_DELAY;
	int MOVE_KEYS_KRM_LIMIT;
	int MOVE_KEYS_KRM_LIMIT_BYTES; // This must be sufficiently larger than CLIENT_KNOBS->KEY_SIZE_LIMIT
	                               // (fdbclient/Knobs.h) to ensure that at least two entries will be returned from an
	                               // attempt to read a key range map
	int MAX_SKIP_TAGS;
	double MAX_ADDED_SOURCES_MULTIPLIER;

	// FdbServer
	double MIN_REBOOT_TIME;
	double MAX_REBOOT_TIME;
	std::string LOG_DIRECTORY;
	std::string CONN_FILE;
	int64_t SERVER_MEM_LIMIT;
	double SYSTEM_MONITOR_FREQUENCY;

	// Ratekeeper
	double SMOOTHING_AMOUNT;
	double SLOW_SMOOTHING_AMOUNT;
	double METRIC_UPDATE_RATE;
	double DETAILED_METRIC_UPDATE_RATE;
	double LAST_LIMITED_RATIO;
	double RATEKEEPER_DEFAULT_LIMIT;
	double RATEKEEPER_LIMIT_REASON_SAMPLE_RATE;
	bool RATEKEEPER_PRINT_LIMIT_REASON;
	double RATEKEEPER_MIN_RATE;
	double RATEKEEPER_MAX_RATE;

	int64_t TARGET_BYTES_PER_STORAGE_SERVER;
	int64_t SPRING_BYTES_STORAGE_SERVER;
	int64_t AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES;
	int64_t TARGET_BYTES_PER_STORAGE_SERVER_BATCH;
	int64_t SPRING_BYTES_STORAGE_SERVER_BATCH;
	int64_t STORAGE_HARD_LIMIT_BYTES;
	int64_t STORAGE_HARD_LIMIT_BYTES_OVERAGE;
	int64_t STORAGE_HARD_LIMIT_VERSION_OVERAGE;
	int64_t STORAGE_DURABILITY_LAG_HARD_MAX;
	int64_t STORAGE_DURABILITY_LAG_SOFT_MAX;

	int64_t LOW_PRIORITY_STORAGE_QUEUE_BYTES;
	int64_t LOW_PRIORITY_DURABILITY_LAG;

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
	double MIN_AVAILABLE_SPACE_RATIO_SAFETY_BUFFER;
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
	int64_t MAX_FORKED_PROCESS_OUTPUT;
	double SNAP_CREATE_MAX_TIMEOUT;
	// Maximum number of storage servers a snapshot can fail to
	// capture while still succeeding
	int64_t MAX_STORAGE_SNAPSHOT_FAULT_TOLERANCE;
	// Maximum number of coordinators a snapshot can fail to
	// capture while still succeeding
	int64_t MAX_COORDINATOR_SNAPSHOT_FAULT_TOLERANCE;

	// Storage Metrics
	double STORAGE_METRICS_AVERAGE_INTERVAL;
	double STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
	double SPLIT_JITTER_AMOUNT;
	int64_t IOPS_UNITS_PER_SAMPLE;
	int64_t BANDWIDTH_UNITS_PER_SAMPLE;
	int64_t BYTES_READ_UNITS_PER_SAMPLE;
	int64_t READ_HOT_SUB_RANGE_CHUNK_SIZE;
	int64_t EMPTY_READ_PENALTY;
	bool READ_SAMPLING_ENABLED;

	// Storage Server
	double STORAGE_LOGGING_DELAY;
	double STORAGE_SERVER_POLL_METRICS_DELAY;
	double FUTURE_VERSION_DELAY;
	int STORAGE_LIMIT_BYTES;
	int BUGGIFY_LIMIT_BYTES;
	bool FETCH_USING_STREAMING;
	int FETCH_BLOCK_BYTES;
	int FETCH_KEYS_PARALLELISM_BYTES;
	int FETCH_KEYS_PARALLELISM;
	int FETCH_KEYS_LOWER_PRIORITY;
	int FETCH_CHANGEFEED_PARALLELISM;
	int BUGGIFY_BLOCK_BYTES;
	int64_t STORAGE_RECOVERY_VERSION_LAG_LIMIT;
	double STORAGE_DURABILITY_LAG_REJECT_THRESHOLD;
	double STORAGE_DURABILITY_LAG_MIN_RATE;
	int STORAGE_COMMIT_BYTES;
	int STORAGE_FETCH_BYTES;
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
	double MAX_STORAGE_COMMIT_TIME;
	int64_t RANGESTREAM_LIMIT_BYTES;
	int64_t CHANGEFEEDSTREAM_LIMIT_BYTES;
	int64_t BLOBWORKERSTATUSSTREAM_LIMIT_BYTES;
	bool ENABLE_CLEAR_RANGE_EAGER_READS;
	bool QUICK_GET_VALUE_FALLBACK;
	bool QUICK_GET_KEY_VALUES_FALLBACK;
	bool STRICTLY_ENFORCE_BYTE_LIMIT;
	double FRACTION_INDEX_BYTELIMIT_PREFETCH;
	int MAX_PARALLEL_QUICK_GET_VALUE;
	int CHECKPOINT_TRANSFER_BLOCK_BYTES;
	int QUICK_GET_KEY_VALUES_LIMIT;
	int QUICK_GET_KEY_VALUES_LIMIT_BYTES;

	// Wait Failure
	int MAX_OUTSTANDING_WAIT_FAILURE_REQUESTS;
	double WAIT_FAILURE_DELAY_LIMIT;

	// Worker
	double WORKER_LOGGING_INTERVAL;
	double HEAP_PROFILER_INTERVAL;
	double UNKNOWN_CC_TIMEOUT;
	double DEGRADED_RESET_INTERVAL;
	double DEGRADED_WARNING_LIMIT;
	double DEGRADED_WARNING_RESET_DELAY;
	int64_t TRACE_LOG_FLUSH_FAILURE_CHECK_INTERVAL_SECONDS;
	double TRACE_LOG_PING_TIMEOUT_SECONDS;
	double MIN_DELAY_CC_WORST_FIT_CANDIDACY_SECONDS; // Listen for a leader for N seconds, and if not heard, then try to
	                                                 // become the leader.
	double MAX_DELAY_CC_WORST_FIT_CANDIDACY_SECONDS;
	double DBINFO_FAILED_DELAY;
	bool ENABLE_WORKER_HEALTH_MONITOR;
	double WORKER_HEALTH_MONITOR_INTERVAL; // Interval between two health monitor health check.
	int PEER_LATENCY_CHECK_MIN_POPULATION; // The minimum number of latency samples required to check a peer.
	double PEER_LATENCY_DEGRADATION_PERCENTILE; // The percentile latency used to check peer health among workers inside
	                                            // primary or remote DC.
	double PEER_LATENCY_DEGRADATION_THRESHOLD; // The latency threshold to consider a peer degraded.
	double PEER_LATENCY_DEGRADATION_PERCENTILE_SATELLITE; // The percentile latency used to check peer health between
	                                                      // primary and primary satellite.
	double PEER_LATENCY_DEGRADATION_THRESHOLD_SATELLITE; // The latency threshold to consider a peer degraded.
	double PEER_TIMEOUT_PERCENTAGE_DEGRADATION_THRESHOLD; // The percentage of timeout to consider a peer degraded.
	int PEER_DEGRADATION_CONNECTION_FAILURE_COUNT; // The number of connection failures experienced during measurement
	                                               // period to consider a peer degraded.
	bool WORKER_HEALTH_REPORT_RECENT_DESTROYED_PEER; // When enabled, the worker's health monitor also report any recent
	                                                 // destroyed peers who are part of the transaction system to
	                                                 // cluster controller.
	bool GRAY_FAILURE_ENABLE_TLOG_RECOVERY_MONITORING; // When enabled, health monitor will try to detect any gray
	                                                   // failure during tlog recovery during the recovery process.
	bool STORAGE_SERVER_REBOOT_ON_IO_TIMEOUT; // When enabled, storage server's worker will crash on io_timeout error;
	                                          // this allows fdbmonitor to restart the worker and recreate the same SS.
	                                          // When SS can be temporarily throttled by infrastructure, e.g, k8s,
	                                          // Enabling this can reduce toil of manually restarting the SS.
	                                          // Enable with caution: If io_timeout is caused by disk failure, we won't
	                                          // want to restart the SS, which increases risk of data corruption.
	bool CONSISTENCY_CHECK_ROCKSDB_ENGINE; // When set, consistency check only check data corruption for a
	                                       // shard which is in at least one SS with the rocksdb engine.
	bool CONSISTENCY_CHECK_SQLITE_ENGINE; // When set, consistency check only check data corruption for a
	                                      // shard which is in at least one SS with the sqlite engine.
	                                      // When both CONSISTENCY_CHECK_ROCKSDB_ENGINE and
	                                      // CONSISTENCY_CHECK_SQLITE_ENGINE are set, consistency check only checks for
	                                      // the rocksdb engine.
	int64_t TESTER_SHARED_RANDOM_MAX_PLUS_ONE;
	int64_t CONSISTENCY_CHECK_ID_MIN;
	int64_t CONSISTENCY_CHECK_ID_MAX_PLUS_ONE;
	bool CONSISTENCY_CHECK_USE_PERSIST_DATA;

	// Test harness
	double WORKER_POLL_DELAY;

	// Coordination
	double COORDINATED_STATE_ONCONFLICT_POLL_INTERVAL;
	bool ENABLE_CROSS_CLUSTER_SUPPORT; // Allow a coordinator to serve requests whose connection string does not match
	                                   // the local descriptor
	double FORWARD_REQUEST_TOO_OLD; // Do not forward requests older than this setting
	double COORDINATOR_LEADER_CONNECTION_TIMEOUT;

	// Dynamic Knobs (implementation)
	double COMPACTION_INTERVAL;
	double UPDATE_NODE_TIMEOUT;
	double GET_COMMITTED_VERSION_TIMEOUT;
	double GET_SNAPSHOT_AND_CHANGES_TIMEOUT;
	double FETCH_CHANGES_TIMEOUT;

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
	int64_t
	    FASTRESTORE_HEARTBEAT_MAX_DELAY; // master claim a node is down if no heart beat from the node for this delay
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
	bool FASTRESTORE_DUMP_INSERT_RANGE_VERSION; // Dump all the range version after insertion. This is for debugging
	                                            // purpose.

	int REDWOOD_DEFAULT_PAGE_SIZE; // Page size for new Redwood files
	int REDWOOD_DEFAULT_EXTENT_SIZE; // Extent size for new Redwood files
	int REDWOOD_DEFAULT_EXTENT_READ_SIZE; // Extent read size for Redwood files
	int REDWOOD_EXTENT_CONCURRENT_READS; // Max number of simultaneous extent disk reads in progress.
	int REDWOOD_KVSTORE_CONCURRENT_READS; // Max number of simultaneous point or range reads in progress.
	bool REDWOOD_KVSTORE_RANGE_PREFETCH; // Whether to use range read prefetching
	double REDWOOD_PAGE_REBUILD_MAX_SLACK; // When rebuilding pages, max slack to allow in page
	int REDWOOD_LAZY_CLEAR_BATCH_SIZE_PAGES; // Number of pages to try to pop from the lazy delete queue and process at
	                                         // once
	int REDWOOD_LAZY_CLEAR_MIN_PAGES; // Minimum number of pages to free before ending a lazy clear cycle, unless the
	                                  // queue is empty
	int REDWOOD_LAZY_CLEAR_MAX_PAGES; // Maximum number of pages to free before ending a lazy clear cycle, unless the
	                                  // queue is empty
	int64_t REDWOOD_REMAP_CLEANUP_WINDOW_BYTES; // Total size of remapped pages to keep before being removed by
	                                            // remap cleanup
	double REDWOOD_REMAP_CLEANUP_TOLERANCE_RATIO; // Maximum ratio of the remap cleanup window that remap cleanup is
	                                              // allowed to be ahead or behind
	int REDWOOD_PAGEFILE_GROWTH_SIZE_PAGES; // Number of pages to grow page file by
	double REDWOOD_METRICS_INTERVAL;
	double REDWOOD_HISTOGRAM_INTERVAL;
	bool REDWOOD_EVICT_UPDATED_PAGES; // Whether to prioritize eviction of updated pages from cache.
	int REDWOOD_DECODECACHE_REUSE_MIN_HEIGHT; // Minimum height for which to keep and reuse page decode caches

	// Server request latency measurement
	int LATENCY_SAMPLE_SIZE;
	double LATENCY_METRICS_LOGGING_INTERVAL;

	// Cluster recovery
	std::string CLUSTER_RECOVERY_EVENT_NAME_PREFIX;

	// Encryption
	bool ENABLE_ENCRYPTION;
	std::string ENCRYPTION_MODE;

	// blob granule stuff
	// FIXME: configure url with database configuration instead of knob eventually
	std::string BG_URL;

	int BG_SNAPSHOT_FILE_TARGET_BYTES;
	int BG_DELTA_FILE_TARGET_BYTES;
	int BG_DELTA_BYTES_BEFORE_COMPACT;
	int BG_MAX_SPLIT_FANOUT;
	int BG_HOT_SNAPSHOT_VERSIONS;
	int BG_CONSISTENCY_CHECK_ENABLED;
	int BG_CONSISTENCY_CHECK_TARGET_SPEED_KB;

	int BLOB_WORKER_INITIAL_SNAPSHOT_PARALLELISM;
	double BLOB_WORKER_TIMEOUT; // Blob Manager's reaction time to a blob worker failure
	double BLOB_WORKER_REQUEST_TIMEOUT; // Blob Worker's server-side request timeout
	double BLOB_WORKERLIST_FETCH_INTERVAL;
	double BLOB_WORKER_BATCH_GRV_INTERVAL;

	double BLOB_MANAGER_STATUS_EXP_BACKOFF_MIN;
	double BLOB_MANAGER_STATUS_EXP_BACKOFF_MAX;
	double BLOB_MANAGER_STATUS_EXP_BACKOFF_EXPONENT;
	double BGCC_TIMEOUT;
	double BGCC_MIN_INTERVAL;

	ServerKnobs(Randomize, ClientKnobs*, IsSimulated);
	void initialize(Randomize, ClientKnobs*, IsSimulated);
};
