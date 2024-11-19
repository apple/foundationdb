/*
 * ServerKnobs.h
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

#pragma once

#include "flow/BooleanParam.h"
#include "flow/Knobs.h"
#include "flow/swift_support.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbclient/ClientKnobs.h"

// Disk queue
static constexpr int _PAGE_SIZE = 4096;

class SWIFT_CXX_IMMORTAL_SINGLETON_TYPE ServerKnobs : public KnobsImpl<ServerKnobs> {
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
	double TLOG_STORAGE_MIN_UPDATE_INTERVAL;
	double BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL;
	int DESIRED_TOTAL_BYTES;
	int DESIRED_UPDATE_BYTES;
	double UPDATE_DELAY;
	int MAXIMUM_PEEK_BYTES;
	int APPLY_MUTATION_BYTES;
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
	double BG_REBALANCE_MAX_POLLING_INTERVAL;
	double BG_REBALANCE_SWITCH_CHECK_INTERVAL;
	double DD_QUEUE_LOGGING_INTERVAL;
	double DD_QUEUE_COUNTER_REFRESH_INTERVAL;
	double DD_QUEUE_COUNTER_MAX_LOG; // max number of servers for which trace events will be generated in each round of
	                                 // DD_QUEUE_COUNTER_REFRESH_INTERVAL duration
	bool DD_QUEUE_COUNTER_SUMMARIZE; // Enable summary of remaining servers when the number of servers with ongoing
	                                 // relocations in the last minute exceeds DD_QUEUE_COUNTER_MAX_LOG
	double WIGGLING_RELOCATION_PARALLELISM_PER_SOURCE_SERVER; // take effects when pertual wiggle priority is larger
	                                                          // than healthy priority
	double RELOCATION_PARALLELISM_PER_SOURCE_SERVER;
	double RELOCATION_PARALLELISM_PER_DEST_SERVER;
	double MERGE_RELOCATION_PARALLELISM_PER_TEAM;
	int DD_QUEUE_MAX_KEY_SERVERS;
	int DD_REBALANCE_PARALLELISM;
	int DD_REBALANCE_RESET_AMOUNT;
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
	// Update the status json .data.team_tracker.state field when necessary
	//
	// Priority for movement resume from previous unfinished in-flight movement when a new DD
	// start
	int PRIORITY_RECOVER_MOVE;
	// A load-balance priority for disk valley filler
	int PRIORITY_REBALANCE_UNDERUTILIZED_TEAM;
	// A load-balance priority disk mountain chopper
	int PRIORITY_REBALANCE_OVERUTILIZED_TEAM;
	// A load-balance priority read valley filler
	int PRIORITY_REBALANCE_READ_OVERUTIL_TEAM;
	// A load-balance priority read mountain chopper
	int PRIORITY_REBALANCE_READ_UNDERUTIL_TEAM;
	// A load-balance priority storage queue too long
	int PRIORITY_REBALANCE_STORAGE_QUEUE;
	// A team healthy priority for wiggle a storage server
	int PRIORITY_PERPETUAL_STORAGE_WIGGLE;
	// A team healthy priority when all servers in a team are healthy. When a team changes from any unhealthy states to
	// healthy, the unfinished relocations will be overridden to healthy priority
	int PRIORITY_TEAM_HEALTHY;
	// A team healthy priority when there's undesired servers in the team. (ex. same ip
	// address as other SS process, or SS is lagging too far ...)
	int PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER;
	// A team healthy priority for removing redundant team to make the team count within a good range
	int PRIORITY_TEAM_REDUNDANT;
	// A shard boundary priority for merge small and write cold shard.
	int PRIORITY_MERGE_SHARD;
	// A team healthy priority for populate remote region
	int PRIORITY_POPULATE_REGION;
	// A team healthy priority when the replica > 3 and there's at least one unhealthy server in a team.
	// Or when the team contains a server with wrong configuration (ex. storage engine,
	// locality, excluded ...)
	int PRIORITY_TEAM_UNHEALTHY;
	// A team healthy priority when there should be >= 3 replicas and there's 2 healthy servers in a team
	int PRIORITY_TEAM_2_LEFT;
	// A team healthy priority when there should be >= 2 replicas and there's 1 healthy server in a team
	int PRIORITY_TEAM_1_LEFT;
	// A team healthy priority when a server in the team is excluded as failed
	int PRIORITY_TEAM_FAILED;
	// A team healthy priority when there's no healthy server in a team
	int PRIORITY_TEAM_0_LEFT;
	// A shard boundary priority for split large or write hot shard.
	int PRIORITY_SPLIT_SHARD;
	// Priority when a physical shard is oversize or anonymous. When DD enable physical shard, the shard created before
	// it are default to be 'anonymous' for compatibility.
	int PRIORITY_ENFORCE_MOVE_OUT_OF_PHYSICAL_SHARD;

	// Data fetching rate is throttled if its priority is STRICTLY LOWER than this value. The rate limit is set as
	// STORAGE_FETCH_KEYS_RATE_LIMIT.
	int FETCH_KEYS_THROTTLE_PRIORITY_THRESHOLD;

	bool ENABLE_REPLICA_CONSISTENCY_CHECK_ON_DATA_MOVEMENT;
	int CONSISTENCY_CHECK_REQUIRED_REPLICAS;

	// Probability that a team redundant data move set TrueBest when get destination team
	double PROBABILITY_TEAM_REDUNDANT_DATAMOVE_CHOOSE_TRUE_BEST_DEST;
	// Probability that a team unhealthy data move set TrueBest when get destination team
	double PROBABILITY_TEAM_UNHEALTHY_DATAMOVE_CHOOSE_TRUE_BEST_DEST;

	// Data distribution
	// DD use AVAILABLE_SPACE_PIVOT_RATIO to calculate pivotAvailableSpaceRatio. Given an array that's descend
	// sorted by available space ratio, the pivot position is AVAILABLE_SPACE_PIVOT_RATIO * team count.
	// When pivotAvailableSpaceRatio is lower than TARGET_AVAILABLE_SPACE_RATIO, the DD won't move any shard to the team
	// has available space ratio < pivotAvailableSpaceRatio.
	double AVAILABLE_SPACE_PIVOT_RATIO;
	// Given an array that's ascend sorted by CPU percent, the pivot position is CPU_PIVOT_RATIO *
	// team count. DD won't move shard to teams that has CPU > pivot CPU.
	double CPU_PIVOT_RATIO;
	// DD won't move shard to teams that has CPU > MAX_DEST_CPU_PERCENT
	double MAX_DEST_CPU_PERCENT;
	// The constant interval DD update pivot values for team selection. It should be >=
	// min(STORAGE_METRICS_POLLING_DELAY,DETAILED_METRIC_UPDATE_RATE)  otherwise the pivot won't change;
	double DD_TEAM_PIVOT_UPDATE_DELAY;

	bool ALLOW_LARGE_SHARD;
	int MAX_LARGE_SHARD_BYTES;

	bool SHARD_ENCODE_LOCATION_METADATA; // If true, location metadata will contain shard ID.
	bool ENABLE_DD_PHYSICAL_SHARD; // EXPERIMENTAL; If true, SHARD_ENCODE_LOCATION_METADATA must be true.
	double DD_PHYSICAL_SHARD_MOVE_PROBABILITY; // Percentage of physical shard move, in the range of [0, 1].
	bool ENABLE_PHYSICAL_SHARD_MOVE_EXPERIMENT;
	bool BULKLOAD_ONLY_USE_PHYSICAL_SHARD_MOVE; // If true, bulk load only uses physical shard move
	int64_t MAX_PHYSICAL_SHARD_BYTES;
	double PHYSICAL_SHARD_METRICS_DELAY;
	double ANONYMOUS_PHYSICAL_SHARD_TRANSITION_TIME;
	bool PHYSICAL_SHARD_MOVE_VERBOSE_TRACKING;

	double READ_REBALANCE_CPU_THRESHOLD; // read rebalance only happens if the source servers' CPU > threshold
	int READ_REBALANCE_SRC_PARALLELISM; // the max count a server become a source server within a certain interval
	int READ_REBALANCE_SHARD_TOPK; // top k shards from which to select randomly for read-rebalance
	double
	    READ_REBALANCE_DIFF_FRAC; // only when (srcLoad - destLoad)/srcLoad > DIFF_FRAC the read rebalance will happen
	double READ_REBALANCE_MAX_SHARD_FRAC; // only move shard whose readLoad < (srcLoad - destLoad) * MAX_SHARD_FRAC
	double
	    READ_REBALANCE_MIN_READ_BYTES_KS; // only move shard whose readLoad > min(MIN_READ_BYTES_KS, shard avg traffic);

	double RETRY_RELOCATESHARD_DELAY;
	double DATA_DISTRIBUTION_FAILURE_REACTION_TIME;
	int MIN_SHARD_BYTES, SHARD_BYTES_RATIO, SHARD_BYTES_PER_SQRT_BYTES, MAX_SHARD_BYTES, KEY_SERVER_SHARD_BYTES;
	int64_t SHARD_MAX_BYTES_PER_KSEC, // Shards with more than this bandwidth will be split immediately
	    SHARD_MIN_BYTES_PER_KSEC, // Shards with more than this bandwidth will not be merged
	    SHARD_SPLIT_BYTES_PER_KSEC; // When splitting a shard, it is split into pieces with less than this bandwidth
	int64_t SHARD_MAX_READ_OPS_PER_KSEC; // When the read operations count is larger than this threshold, a range will
	                                     // be considered hot
	// When the sampled read operations changes more than this threshold, the
	// shard metrics will update immediately
	int64_t SHARD_READ_OPS_CHANGE_THRESHOLD;
	bool ENABLE_WRITE_BASED_SHARD_SPLIT; // Experimental. Enable to enforce shard split when write traffic is high
	int DD_SHARD_USABLE_REGION_CHECK_RATE; // Assuming all shards need to repair, the (rough) number of shards moving
	                                       // for usable region per second. Set 0 to disable shard usable region check
	double SHARD_MAX_READ_DENSITY_RATIO;
	int64_t SHARD_READ_HOT_BANDWIDTH_MIN_PER_KSECONDS;
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
	double PERPETUAL_WIGGLE_DELAY; // The max interval between the last wiggle finish and the next wiggle start
	bool PERPETUAL_WIGGLE_DISABLE_REMOVER; // Whether the start of perpetual wiggle replace team remover
	double LOG_ON_COMPLETION_DELAY;
	int BEST_TEAM_MAX_TEAM_TRIES;
	int BEST_TEAM_OPTION_COUNT;
	int BEST_OF_AMT;
	double SERVER_LIST_DELAY;
	double RATEKEEPER_MONITOR_SS_DELAY;
	int RATEKEEPER_MONITOR_SS_THRESHOLD;
	double RECRUITMENT_IDLE_DELAY;
	double STORAGE_RECRUITMENT_DELAY;
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
	int64_t
	    DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC; // Minimal age of a correct-configured server before it's chosen to be wiggled
	bool DD_TENANT_AWARENESS_ENABLED;
	bool STORAGE_QUOTA_ENABLED; // Whether storage quota enforcement for tenant groups and all the relevant storage
	                            // usage / quota monitors are enabled.
	int TENANT_CACHE_LIST_REFRESH_INTERVAL; // How often the TenantCache is refreshed
	int TENANT_CACHE_STORAGE_USAGE_REFRESH_INTERVAL; // How often the storage bytes used by each tenant is refreshed
	                                                 // in the TenantCache
	int TENANT_CACHE_STORAGE_QUOTA_REFRESH_INTERVAL; // How often the storage quota allocated to each tenant is
	                                                 // refreshed in the TenantCache
	int TENANT_CACHE_STORAGE_USAGE_TRACE_INTERVAL; // The minimum interval between consecutive trace events logging the
	                                               // storage bytes used by a tenant group
	int CP_FETCH_TENANTS_OVER_STORAGE_QUOTA_INTERVAL; // How often the commit proxies send requests to the data
	                                                  // distributor to fetch the list of tenants over storage quota
	bool ENABLE_STORAGE_QUEUE_AWARE_TEAM_SELECTION; // Experimental! Enable to avoid moving data to a team which has a
	                                                // long storage queue
	double DD_LONG_STORAGE_QUEUE_TEAM_MAJORITY_PERCENTILE; // p% amount teams which have longer queues (team queue size
	                                                       // = max SSes queue size)
	bool ENABLE_REBALANCE_STORAGE_QUEUE; // Experimental! Enable to trigger data moves to rebalance storage queues when
	                                     // a queue is significantly longer than others
	int64_t REBALANCE_STORAGE_QUEUE_LONG_BYTES; // Lower bound of length indicating the storage queue is too long
	int64_t REBALANCE_STORAGE_QUEUE_SHORT_BYTES; // Upper bound of length indicating the storage queue is back to short
	double DD_LONG_STORAGE_QUEUE_TIMESPAN;
	double DD_REBALANCE_STORAGE_QUEUE_TIME_INTERVAL;
	int64_t REBALANCE_STORAGE_QUEUE_SHARD_PER_KSEC_MIN;
	bool DD_ENABLE_REBALANCE_STORAGE_QUEUE_WITH_LIGHT_WRITE_SHARD; // Enable to allow storage queue rebalancer to move
	                                                               // light-traffic shards out of the overloading server
	double DD_WAIT_TSS_DATA_MOVE_DELAY;
	bool DD_VALIDATE_SERVER_TEAM_COUNT_AFTER_BUILD_TEAM; // Enable to validate server team count per server after build
	                                                     // team

	// TeamRemover to remove redundant teams
	double TR_LOW_SPACE_PIVOT_DELAY_SEC; // teamRedundant data moves can make the min SS available % smaller in
	                                     // particular when the majority of SSes have low available %. So, when the
	                                     // pivot is below the target, teamRemover wait for the specified time to check
	                                     // the pivot again. teamRemover triggers teamRedundant data moves only when the
	                                     // pivot is above the target.
	bool TR_FLAG_DISABLE_MACHINE_TEAM_REMOVER; // disable the machineTeamRemover actor
	double TR_REMOVE_MACHINE_TEAM_DELAY; // wait for the specified time before try to remove next machine team
	bool TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS; // guard to select which machineTeamRemover logic to use

	bool TR_FLAG_DISABLE_SERVER_TEAM_REMOVER; // disable the serverTeamRemover actor
	double TR_REMOVE_SERVER_TEAM_DELAY; // wait for the specified time before try to remove next server team
	double TR_REMOVE_SERVER_TEAM_EXTRA_DELAY; // serverTeamRemover waits for the delay and check DD healthyness again to
	                                          // ensure it runs after machineTeamRemover
	double TR_REDUNDANT_TEAM_PERCENTAGE_THRESHOLD; // serverTeamRemover will only remove teams if existing team number
	                                               // is p% more than the desired team number.

	// Remove wrong storage engines
	double DD_REMOVE_STORE_ENGINE_DELAY; // wait for the specified time before remove the next batch

	double DD_FAILURE_TIME;
	double DD_ZERO_HEALTHY_TEAM_DELAY;
	int DD_BUILD_EXTRA_TEAMS_OVERRIDE; // build extra teams to allow data movement to progress. must be larger than 0
	bool DD_REMOVE_MAINTENANCE_ON_FAILURE; // If set to true DD will remove the maintenance mode if another SS fails
	                                       // outside of the maintenance zone.
	int DD_SHARD_TRACKING_LOG_SEVERITY;
	bool ENFORCE_SHARD_COUNT_PER_TEAM; // Whether data movement selects dst team not exceeding
	                                   // DESIRED_MAX_SHARDS_PER_TEAM.
	int DESIRED_MAX_SHARDS_PER_TEAM; // When ENFORCE_SHARD_COUNT_PER_TEAM is true, this is the desired, but not strictly
	                                 // enforced, max shard count per team.

	int DD_MAX_SHARDS_ON_LARGE_TEAMS; // the maximum number of shards that can be assigned to large teams
	int DD_MAXIMUM_LARGE_TEAM_CLEANUP; // the maximum number of large teams data distribution will attempt to cleanup
	                                   // without yielding
	double DD_LARGE_TEAM_DELAY; // the amount of time data distribution will wait before returning less replicas than
	                            // requested
	double DD_FIX_WRONG_REPLICAS_DELAY; // the amount of time between attempts to increase the replication factor of
	                                    // under replicated shards
	int BULKLOAD_FILE_BYTES_MAX; // the maximum bytes of files to inject by bulk loading
	double DD_BULKLOAD_SHARD_BOUNDARY_CHANGE_DELAY_SEC; // seconds to delay shard boundary change when blocked by bulk
	                                                    // loading
	int DD_BULKLOAD_TASK_METADATA_READ_SIZE; // the number of bulk load tasks read from metadata at a time
	int DD_BULKLOAD_PARALLELISM; // the maximum number of running bulk load tasks
	double DD_BULKLOAD_SCHEDULE_MIN_INTERVAL_SEC; // the minimal seconds that the bulk load scheduler has to wait
	                                              // between two rounds
	int DD_BULKDUMP_TASK_METADATA_READ_SIZE; // the number of bulk dump tasks read from metadata at a time
	double DD_BULKDUMP_SCHEDULE_MIN_INTERVAL_SEC; // the minimal seconds that the bulk dump scheduler has to wait
	                                              // between two rounds
	int SS_SERVE_BULK_DUMP_PARALLELISM; // the number of bulk dump tasks that can concurrently happen at a SS

	// Run storage engine on a child process on the same machine with storage process
	bool REMOTE_KV_STORE;
	// A delay to avoid race on file resources after seeing lock_file_failure
	double REBOOT_KV_STORE_DELAY;
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
	bool ROCKSDB_SET_READ_TIMEOUT;
	bool ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES;
	bool ROCKSDB_SUGGEST_COMPACT_CLEAR_RANGE;
	int ROCKSDB_READ_RANGE_ROW_LIMIT;
	int ROCKSDB_READER_THREAD_PRIORITY;
	int ROCKSDB_WRITER_THREAD_PRIORITY;
	int ROCKSDB_COMPACTION_THREAD_PRIORITY;
	int ROCKSDB_BACKGROUND_PARALLELISM;
	int ROCKSDB_READ_PARALLELISM;
	int ROCKSDB_CHECKPOINT_READER_PARALLELISM;
	int64_t ROCKSDB_MEMTABLE_BYTES;
	bool ROCKSDB_LEVEL_STYLE_COMPACTION;
	bool ROCKSDB_UNSAFE_AUTO_FSYNC;
	bool ROCKSDB_MUTE_LOGS;
	int64_t ROCKSDB_PERIODIC_COMPACTION_SECONDS;
	int ROCKSDB_PREFIX_LEN;
	double ROCKSDB_MEMTABLE_PREFIX_BLOOM_SIZE_RATIO;
	int ROCKSDB_BLOOM_BITS_PER_KEY;
	bool ROCKSDB_BLOOM_WHOLE_KEY_FILTERING;
	int ROCKSDB_MAX_AUTO_READAHEAD_SIZE;
	int64_t ROCKSDB_BLOCK_CACHE_SIZE;
	double ROCKSDB_CACHE_HIGH_PRI_POOL_RATIO;
	bool ROCKSDB_CACHE_INDEX_AND_FILTER_BLOCKS;
	double ROCKSDB_METRICS_DELAY;
	double ROCKSDB_READ_VALUE_TIMEOUT;
	double ROCKSDB_READ_VALUE_PREFIX_TIMEOUT;
	double ROCKSDB_READ_RANGE_TIMEOUT;
	double ROCKSDB_READ_CHECKPOINT_TIMEOUT;
	int64_t ROCKSDB_CHECKPOINT_READ_AHEAD_SIZE;
	double ROCKSDB_READ_QUEUE_WAIT;
	int ROCKSDB_READ_QUEUE_SOFT_MAX;
	int ROCKSDB_READ_QUEUE_HARD_MAX;
	int ROCKSDB_FETCH_QUEUE_SOFT_MAX;
	int ROCKSDB_FETCH_QUEUE_HARD_MAX;
	// These histograms are in read and write path which can cause performance overhead.
	// Set to 0 to disable histograms.
	double ROCKSDB_HISTOGRAMS_SAMPLE_RATE;
	double ROCKSDB_READ_RANGE_ITERATOR_REFRESH_TIME;
	double ROCKSDB_PROBABILITY_REUSE_ITERATOR_SIM; // Probability that RocksDB reuses iterator in simulation
	bool ROCKSDB_READ_RANGE_REUSE_ITERATORS;
	bool SHARDED_ROCKSDB_REUSE_ITERATORS;
	bool ROCKSDB_READ_RANGE_REUSE_BOUNDED_ITERATORS;
	int ROCKSDB_READ_RANGE_BOUNDED_ITERATORS_MAX_LIMIT;
	int64_t ROCKSDB_WRITE_RATE_LIMITER_BYTES_PER_SEC;
	int ROCKSDB_WRITE_RATE_LIMITER_FAIRNESS;
	bool ROCKSDB_WRITE_RATE_LIMITER_AUTO_TUNE;
	std::string DEFAULT_FDB_ROCKSDB_COLUMN_FAMILY;
	bool ROCKSDB_DISABLE_AUTO_COMPACTIONS;
	bool ROCKSDB_PERFCONTEXT_ENABLE; // Enable rocks perf context metrics. May cause performance overhead
	double ROCKSDB_PERFCONTEXT_SAMPLE_RATE;
	double ROCKSDB_METRICS_SAMPLE_INTERVAL;
	int ROCKSDB_MAX_SUBCOMPACTIONS;
	int64_t ROCKSDB_SOFT_PENDING_COMPACT_BYTES_LIMIT;
	int64_t ROCKSDB_HARD_PENDING_COMPACT_BYTES_LIMIT;
	int64_t SHARD_SOFT_PENDING_COMPACT_BYTES_LIMIT;
	int64_t SHARD_HARD_PENDING_COMPACT_BYTES_LIMIT;
	int64_t ROCKSDB_CAN_COMMIT_COMPACT_BYTES_LIMIT;
	int ROCKSDB_CAN_COMMIT_IMMUTABLE_MEMTABLES_LIMIT;
	bool ROCKSDB_PARANOID_FILE_CHECKS;
	double ROCKSDB_CAN_COMMIT_DELAY_ON_OVERLOAD;
	int ROCKSDB_CAN_COMMIT_DELAY_TIMES_ON_OVERLOAD;
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
	bool ENABLE_SHARDED_ROCKSDB;
	int64_t ROCKSDB_WRITE_BUFFER_SIZE;
	int ROCKSDB_MAX_WRITE_BUFFER_NUMBER;
	int ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE;
	int ROCKSDB_LEVEL0_FILENUM_COMPACTION_TRIGGER;
	int ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
	int ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER;
	int64_t ROCKSDB_MAX_TOTAL_WAL_SIZE;
	int64_t ROCKSDB_MAX_BACKGROUND_JOBS;
	int64_t ROCKSDB_DELETE_OBSOLETE_FILE_PERIOD;
	double ROCKSDB_PHYSICAL_SHARD_CLEAN_UP_DELAY;
	bool ROCKSDB_EMPTY_RANGE_CHECK;
	int ROCKSDB_CREATE_BYTES_SAMPLE_FILE_RETRY_MAX;
	bool ROCKSDB_ATOMIC_FLUSH;
	bool ROCKSDB_IMPORT_MOVE_FILES;
	bool ROCKSDB_CHECKPOINT_REPLAY_MARKER;
	bool ROCKSDB_VERIFY_CHECKSUM_BEFORE_RESTORE; // Conduct block-level checksum when rocksdb injecting data
	bool ROCKSDB_ENABLE_CHECKPOINT_VALIDATION;
	bool ROCKSDB_RETURN_OVERLOADED_ON_TIMEOUT;
	int ROCKSDB_COMPACTION_PRI;
	int ROCKSDB_WAL_RECOVERY_MODE;
	int ROCKSDB_TARGET_FILE_SIZE_BASE;
	int ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER;
	bool ROCKSDB_USE_DIRECT_READS;
	bool ROCKSDB_USE_DIRECT_IO_FLUSH_COMPACTION;
	int ROCKSDB_MAX_OPEN_FILES;
	bool ROCKSDB_USE_POINT_DELETE_FOR_SYSTEM_KEYS;
	int ROCKSDB_CF_RANGE_DELETION_LIMIT;
	int ROCKSDB_MEMTABLE_MAX_RANGE_DELETIONS;
	bool ROCKSDB_WAIT_ON_CF_FLUSH;
	bool ROCKSDB_ALLOW_WRITE_STALL_ON_FLUSH;
	double ROCKSDB_CF_METRICS_DELAY;
	int ROCKSDB_MAX_LOG_FILE_SIZE;
	int ROCKSDB_KEEP_LOG_FILE_NUM;
	int ROCKSDB_MANUAL_FLUSH_TIME_INTERVAL;
	bool ROCKSDB_SKIP_STATS_UPDATE_ON_OPEN;
	bool ROCKSDB_SKIP_FILE_SIZE_CHECK_ON_OPEN;
	bool ROCKSDB_FULLFILE_CHECKSUM; // For validate sst files when compaction and producing backup files. TODO: set
	                                // verify_file_checksum when ingesting (for physical shard move).
	                                // This is different from ROCKSDB_VERIFY_CHECKSUM_BEFORE_RESTORE (block-level
	                                // checksum). The block-level checksum does not cover the corruption such as wrong
	                                // sst file or file move/copy.
	int ROCKSDB_WRITEBATCH_PROTECTION_BYTES_PER_KEY;
	int ROCKSDB_MEMTABLE_PROTECTION_BYTES_PER_KEY;
	int ROCKSDB_BLOCK_PROTECTION_BYTES_PER_KEY;
	bool ROCKSDB_METRICS_IN_SIMULATION; // Whether rocksdb traceevent metrics will be emitted in simulation. Note that
	                                    // turning this on in simulation could lead to non-deterministic runs since we
	                                    // rely on rocksdb metadata.
	bool SHARDED_ROCKSDB_ALLOW_WRITE_STALL_ON_FLUSH;
	int SHARDED_ROCKSDB_MEMTABLE_MAX_RANGE_DELETIONS;
	double SHARDED_ROCKSDB_VALIDATE_MAPPING_RATIO;
	int SHARD_METADATA_SCAN_BYTES_LIMIT;
	int ROCKSDB_MAX_MANIFEST_FILE_SIZE;
	int SHARDED_ROCKSDB_AVERAGE_FILE_SIZE;
	double SHARDED_ROCKSDB_COMPACTION_PERIOD;
	double SHARDED_ROCKSDB_COMPACTION_ACTOR_DELAY;
	int SHARDED_ROCKSDB_COMPACTION_SHARD_LIMIT;
	int64_t SHARDED_ROCKSDB_WRITE_BUFFER_SIZE;
	int64_t SHARDED_ROCKSDB_TOTAL_WRITE_BUFFER_SIZE;
	int64_t SHARDED_ROCKSDB_MEMTABLE_BUDGET;
	int64_t SHARDED_ROCKSDB_MAX_WRITE_BUFFER_NUMBER;
	int SHARDED_ROCKSDB_TARGET_FILE_SIZE_BASE;
	int SHARDED_ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER;
	bool SHARDED_ROCKSDB_SUGGEST_COMPACT_CLEAR_RANGE;
	int SHARDED_ROCKSDB_MAX_BACKGROUND_JOBS;
	int64_t SHARDED_ROCKSDB_BLOCK_CACHE_SIZE;
	double SHARDED_ROCKSDB_CACHE_HIGH_PRI_POOL_RATIO;
	bool SHARDED_ROCKSDB_CACHE_INDEX_AND_FILTER_BLOCKS;
	int64_t SHARDED_ROCKSDB_WRITE_RATE_LIMITER_BYTES_PER_SEC;
	int64_t SHARDED_ROCKSDB_RATE_LIMITER_MODE;
	int SHARDED_ROCKSDB_BACKGROUND_PARALLELISM;
	int SHARDED_ROCKSDB_MAX_SUBCOMPACTIONS;
	int SHARDED_ROCKSDB_LEVEL0_FILENUM_COMPACTION_TRIGGER;
	int SHARDED_ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
	int SHARDED_ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER;
	bool SHARDED_ROCKSDB_DELAY_COMPACTION_FOR_DATA_MOVE;
	int SHARDED_ROCKSDB_MAX_OPEN_FILES;
	bool SHARDED_ROCKSDB_READ_ASYNC_IO;
	int SHARDED_ROCKSDB_PREFIX_LEN;
	double SHARDED_ROCKSDB_HISTOGRAMS_SAMPLE_RATE;

	// Leader election
	int MAX_NOTIFICATIONS;
	int MIN_NOTIFICATIONS;
	double NOTIFICATION_FULL_CLEAR_TIME;
	double CANDIDATE_MIN_DELAY;
	double CANDIDATE_MAX_DELAY;
	double CANDIDATE_GROWTH_RATE;
	double POLLING_FREQUENCY;
	double HEARTBEAT_FREQUENCY;

	// Commit Proxy and GRV Proxy
	double START_TRANSACTION_BATCH_INTERVAL_MIN;
	double START_TRANSACTION_BATCH_INTERVAL_MAX;
	double START_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION;
	double START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA;
	double START_TRANSACTION_BATCH_QUEUE_CHECK_INTERVAL;
	double START_TRANSACTION_MAX_TRANSACTIONS_TO_START;
	int START_TRANSACTION_MAX_REQUESTS_TO_START;
	double START_TRANSACTION_RATE_WINDOW;
	double TAG_THROTTLE_RATE_WINDOW;
	double START_TRANSACTION_MAX_EMPTY_QUEUE_BUDGET;
	double TAG_THROTTLE_MAX_EMPTY_QUEUE_BUDGET;
	int START_TRANSACTION_MAX_QUEUE_SIZE;
	int KEY_LOCATION_MAX_QUEUE_SIZE;
	int TENANT_ID_REQUEST_MAX_QUEUE_SIZE;
	int BLOB_GRANULE_LOCATION_MAX_QUEUE_SIZE;
	double COMMIT_PROXY_LIVENESS_TIMEOUT;
	double COMMIT_PROXY_MAX_LIVENESS_TIMEOUT;

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
	double COMMIT_TRIGGER_DELAY;
	bool ENABLE_READ_LOCK_ON_RANGE;

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
	bool BURSTINESS_METRICS_ENABLED;
	// Interval on which to emit burstiness metrics on the commit proxy (in
	// seconds).
	double BURSTINESS_METRICS_LOG_INTERVAL;

	int RESET_MASTER_BATCHES;
	int RESET_RESOLVER_BATCHES;
	double RESET_MASTER_DELAY;
	double RESET_RESOLVER_DELAY;

	double GLOBAL_CONFIG_MIGRATE_TIMEOUT;
	double GLOBAL_CONFIG_REFRESH_INTERVAL;
	double GLOBAL_CONFIG_REFRESH_TIMEOUT;

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
	double WAIT_FOR_CONSISTENCYSCAN_JOIN_DELAY;
	double WAIT_FOR_BLOB_MANAGER_JOIN_DELAY;
	double WAIT_FOR_ENCRYPT_KEY_PROXY_JOIN_DELAY;
	double WORKER_FAILURE_TIME;
	double CHECK_OUTSTANDING_INTERVAL;
	double INCOMPATIBLE_PEERS_LOGGING_INTERVAL;
	double VERSION_LAG_METRIC_INTERVAL;
	int64_t MAX_VERSION_DIFFERENCE;
	double INITIAL_UPDATE_CROSS_DC_INFO_DELAY; // The initial delay in a new Cluster Controller just started to refresh
	                                           // the info of remote DC, such as remote DC health, and whether we need
	                                           // to take remote DC health info when making failover decision.
	double CHECK_REMOTE_HEALTH_INTERVAL; // Remote DC health refresh interval.
	double FORCE_RECOVERY_CHECK_DELAY;
	double RATEKEEPER_FAILURE_TIME;
	double CONSISTENCYSCAN_FAILURE_TIME;
	double BLOB_MANAGER_FAILURE_TIME;
	double BLOB_MIGRATOR_FAILURE_TIME;
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
	bool CC_ENABLE_REMOTE_LOG_ROUTER_DEGRADATION_MONITORING; // When enabled, gray failure tries to detect whether
	                                                         // remote log routers are experiencing degradation
	                                                         // (latency) with their peers. Gray failure may trigger
	                                                         // recovery based on this.
	bool CC_ENABLE_REMOTE_LOG_ROUTER_MONITORING; // When enabled, gray failure tries to detect whether
	                                             // remote log routers are disconnected from their peers. Gray failure
	                                             // may trigger recovery based on this.
	bool CC_ENABLE_REMOTE_TLOG_DEGRADATION_MONITORING; // When enabled, gray failure tries to detect whether remote
	                                                   // tlogs are experiencing degradation (latency) with their peers.
	                                                   // Gray failure may trigger recovery based on this.
	bool CC_ENABLE_REMOTE_TLOG_DISCONNECT_MONITORING; // When enabled, gray failure tries to detect whether remote
	                                                  // tlogs are disconnected from their peers. Gray failure may
	                                                  // trigger recovery based on this.
	bool CC_ONLY_CONSIDER_INTRA_DC_LATENCY; // When enabled, gray failure only considers intra-DC signal for latency
	                                        // degradations. For remote process knobs
	                                        // (CC_ENABLE_REMOTE_TLOG_DEGRADATION_MONITORING and
	                                        // CC_ENABLE_REMOTE_LOG_ROUTER_DEGRADATION_MONITORING), this knob must be
	                                        // turned on, because inter-DC latency signal is not reliable and it's
	                                        // challenging to pick a good latency threshold.
	bool CC_INVALIDATE_EXCLUDED_PROCESSES; // When enabled, invalidate the complaints by processes that were excluded
	                                       // in gray failure triggered recoveries.
	bool CC_GRAY_FAILURE_STATUS_JSON; // When enabled, returns gray failure information in machine readable status json.
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
	bool RECORD_RECOVER_AT_IN_CSTATE;
	bool TRACK_TLOG_RECOVERY;

	// Move Keys
	double SHARD_READY_DELAY;
	double SERVER_READY_QUORUM_INTERVAL;
	double SERVER_READY_QUORUM_TIMEOUT;
	double REMOVE_RETRY_DELAY;
	int MOVE_KEYS_KRM_LIMIT;
	int MOVE_KEYS_KRM_LIMIT_BYTES; // This must be sufficiently larger than CLIENT_KNOBS->KEY_SIZE_LIMIT
	                               // (fdbclient/Knobs.h) to ensure that at least two entries will be returned from an
	                               // attempt to read a key range map
	int MOVE_SHARD_KRM_ROW_LIMIT;
	int MOVE_SHARD_KRM_BYTE_LIMIT;
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
	// The interval of detailed HealthMetric is pushed to GRV proxies
	double DETAILED_METRIC_UPDATE_RATE;
	double LAST_LIMITED_RATIO;
	double RATEKEEPER_DEFAULT_LIMIT;
	double RATEKEEPER_LIMIT_REASON_SAMPLE_RATE;
	bool RATEKEEPER_PRINT_LIMIT_REASON;
	double RATEKEEPER_MIN_RATE;
	double RATEKEEPER_MAX_RATE;
	double RATEKEEPER_BATCH_MIN_RATE;
	double RATEKEEPER_BATCH_MAX_RATE;

	int64_t TARGET_BYTES_PER_STORAGE_SERVER;
	int64_t SPRING_BYTES_STORAGE_SERVER;
	int64_t AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES;
	int64_t AUTO_TAG_THROTTLE_SPRING_BYTES_STORAGE_SERVER;
	int64_t TARGET_BYTES_PER_STORAGE_SERVER_BATCH;
	int64_t SPRING_BYTES_STORAGE_SERVER_BATCH;
	int64_t STORAGE_HARD_LIMIT_BYTES;
	int64_t STORAGE_HARD_LIMIT_BYTES_OVERAGE;
	int64_t STORAGE_HARD_LIMIT_BYTES_SPEED_UP_SIM;
	int64_t STORAGE_HARD_LIMIT_BYTES_OVERAGE_SPEED_UP_SIM;
	int64_t STORAGE_HARD_LIMIT_VERSION_OVERAGE;
	int64_t STORAGE_DURABILITY_LAG_HARD_MAX;
	int64_t STORAGE_DURABILITY_LAG_SOFT_MAX;
	bool STORAGE_INCLUDE_FEED_STORAGE_QUEUE;
	double STORAGE_FETCH_KEYS_DELAY;
	bool STORAGE_FETCH_KEYS_USE_COMMIT_BUDGET;
	int64_t STORAGE_FETCH_KEYS_RATE_LIMIT; // Unit: MB/s
	double STORAGE_ROCKSDB_LOG_CLEAN_UP_DELAY;
	double STORAGE_ROCKSDB_LOG_TTL;

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

	// Tag throttling
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
	// Limit to the number of throttling tags each storage server
	// will track and send to the ratekeeper
	int64_t SS_THROTTLE_TAGS_TRACKED;
	// Use global tag throttling strategy. i.e. throttle based on the cluster-wide
	// throughput for tags and their associated quotas.
	bool GLOBAL_TAG_THROTTLING;
	// Enforce tag throttling on proxies rather than on clients
	bool ENFORCE_TAG_THROTTLING_ON_PROXIES;
	// Minimum number of transactions per second that the global tag throttler must allow for each tag.
	// When the measured tps for a tag gets too low, the denominator in the
	// average cost calculation gets small, resulting in an unstable calculation.
	// To protect against this, we do not compute the average cost when the
	// measured tps drops below this threshold
	double GLOBAL_TAG_THROTTLING_MIN_RATE;
	// Maximum number of tags tracked by global tag throttler. Additional tags will be ignored
	// until some existing tags expire
	int64_t GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED;
	// Global tag throttler forgets about throughput from a tag once no new transactions from that
	// tag have been received for this duration (in seconds):
	int64_t GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER;
	// Interval at which latency bands are logged for each tag on grv proxy
	double GLOBAL_TAG_THROTTLING_PROXY_LOGGING_INTERVAL;
	// Interval at which ratekeeper logs statistics for each tag:
	double GLOBAL_TAG_THROTTLING_TRACE_INTERVAL;
	// If this knob is set to true, the global tag throttler will still
	// compute rates, but these rates won't be sent to GRV proxies for
	// enforcement.
	bool GLOBAL_TAG_THROTTLING_REPORT_ONLY;
	// Below this throughput threshold (in bytes/second), ratekeeper will forget about the
	// throughput of a particular tag on a particular storage server
	int64_t GLOBAL_TAG_THROTTLING_FORGET_SS_THRESHOLD;
	// If a tag's throughput on a particular storage server exceeds this threshold,
	// this storage server's throttling ratio will contribute the calculation of the
	// throttlingId's limiting transaction rate
	double GLOBAL_TAG_THROTTLING_LIMITING_THRESHOLD;

	double GLOBAL_TAG_THROTTLING_TARGET_RATE_FOLDING_TIME;
	double GLOBAL_TAG_THROTTLING_TRANSACTION_COUNT_FOLDING_TIME;
	double GLOBAL_TAG_THROTTLING_TRANSACTION_RATE_FOLDING_TIME;
	double GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME;

	bool HOT_SHARD_THROTTLING_ENABLED;
	double HOT_SHARD_THROTTLING_EXPIRE_AFTER;
	int64_t HOT_SHARD_THROTTLING_TRACKED;
	double HOT_SHARD_MONITOR_FREQUENCY;

	// allow generating synthetic data for test clusters
	bool GENERATE_DATA_ENABLED;
	// maximum number of synthetic mutations per transaction
	int GENERATE_DATA_PER_VERSION_MAX;

	double MAX_TRANSACTIONS_PER_BYTE;

	int64_t MIN_AVAILABLE_SPACE;
	// DD won't move data to a team that has available space ratio < MIN_AVAILABLE_SPACE_RATIO
	double MIN_AVAILABLE_SPACE_RATIO;
	double MIN_AVAILABLE_SPACE_RATIO_SAFETY_BUFFER;
	double TARGET_AVAILABLE_SPACE_RATIO;

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
	bool BW_THROTTLING_ENABLED;
	double TARGET_BW_LAG;
	double TARGET_BW_LAG_BATCH;
	double TARGET_BW_LAG_UPDATE;
	int MIN_BW_HISTORY;
	double BW_ESTIMATION_INTERVAL;
	double BW_LAG_INCREASE_AMOUNT;
	double BW_LAG_DECREASE_AMOUNT;
	double BW_FETCH_WORKERS_INTERVAL;
	double BW_RW_LOGGING_INTERVAL;
	double BW_MAX_BLOCKED_INTERVAL;
	double BW_RK_SIM_QUIESCE_DELAY;

	// disk snapshot
	int64_t MAX_FORKED_PROCESS_OUTPUT;
	// retry limit after network failures
	int64_t SNAP_NETWORK_FAILURE_RETRY_LIMIT;
	// time limit for creating snapshot
	double SNAP_CREATE_MAX_TIMEOUT;
	// minimum gap time between two snapshot requests for the same process
	double SNAP_MINIMUM_TIME_GAP;
	// Maximum number of storage servers a snapshot can fail to
	// capture while still succeeding
	int64_t MAX_STORAGE_SNAPSHOT_FAULT_TOLERANCE;
	// Maximum number of coordinators a snapshot can fail to
	// capture while still succeeding
	int64_t MAX_COORDINATOR_SNAPSHOT_FAULT_TOLERANCE;
	// if true, all processes with class "storage", "transaction" and "log" will be snapshotted even not recruited as
	// the role
	bool SNAPSHOT_ALL_STATEFUL_PROCESSES;

	// Storage Metrics
	double STORAGE_METRICS_AVERAGE_INTERVAL;
	double STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS;
	double SPLIT_JITTER_AMOUNT;
	int64_t IOPS_UNITS_PER_SAMPLE;
	int64_t BYTES_WRITTEN_UNITS_PER_SAMPLE;
	int64_t BYTES_READ_UNITS_PER_SAMPLE;
	int64_t OPS_READ_UNITS_PER_SAMPLE;
	int64_t READ_HOT_SUB_RANGE_CHUNK_SIZE;
	int64_t EMPTY_READ_PENALTY;
	int DD_SHARD_COMPARE_LIMIT; // when read-aware DD is enabled, at most how many shards are compared together
	bool READ_SAMPLING_ENABLED;
	bool DD_PREFER_LOW_READ_UTIL_TEAM;
	// Rolling window duration over which the average bytes moved by DD is calculated for the 'MovingData' trace event.
	double DD_TRACE_MOVE_BYTES_AVERAGE_INTERVAL;
	int64_t MOVING_WINDOW_SAMPLE_SIZE;

	// Storage Server
	double STORAGE_LOGGING_DELAY;
	double STORAGE_SERVER_POLL_METRICS_DELAY;
	double FUTURE_VERSION_DELAY;
	int STORAGE_LIMIT_BYTES;
	int BUGGIFY_LIMIT_BYTES;
	bool FETCH_USING_STREAMING;
	bool FETCH_USING_BLOB;
	int FETCH_BLOCK_BYTES;
	int FETCH_KEYS_PARALLELISM_BYTES;
	int FETCH_KEYS_PARALLELISM;
	int FETCH_KEYS_PARALLELISM_CHANGE_FEED;
	int FETCH_KEYS_LOWER_PRIORITY;
	int SERVE_FETCH_CHECKPOINT_PARALLELISM;
	int SERVE_AUDIT_STORAGE_PARALLELISM;
	int PERSIST_FINISH_AUDIT_COUNT; // Num of persist complete/failed audits for each type
	int AUDIT_RETRY_COUNT_MAX;
	int CONCURRENT_AUDIT_TASK_COUNT_MAX;
	bool AUDIT_DATAMOVE_PRE_CHECK;
	bool AUDIT_DATAMOVE_POST_CHECK;
	int AUDIT_DATAMOVE_POST_CHECK_RETRY_COUNT_MAX;
	int AUDIT_STORAGE_RATE_PER_SERVER_MAX;
	bool ENABLE_AUDIT_VERBOSE_TRACE;
	bool LOGGING_STORAGE_COMMIT_WHEN_IO_TIMEOUT;
	double LOGGING_COMPLETE_STORAGE_COMMIT_PROBABILITY;
	int LOGGING_RECENT_STORAGE_COMMIT_SIZE;
	bool LOGGING_ROCKSDB_BG_WORK_WHEN_IO_TIMEOUT;
	double LOGGING_ROCKSDB_BG_WORK_PROBABILITY;
	double LOGGING_ROCKSDB_BG_WORK_PERIOD_SEC;
	int BUGGIFY_BLOCK_BYTES;
	int64_t STORAGE_RECOVERY_VERSION_LAG_LIMIT;
	double STORAGE_DURABILITY_LAG_REJECT_THRESHOLD;
	double STORAGE_DURABILITY_LAG_MIN_RATE;
	int STORAGE_COMMIT_BYTES;
	int STORAGE_FETCH_BYTES;
	int STORAGE_ROCKSDB_FETCH_BYTES;
	double STORAGE_COMMIT_INTERVAL;
	int BYTE_SAMPLING_FACTOR;
	int BYTE_SAMPLING_OVERHEAD;
	double MIN_BYTE_SAMPLING_PROBABILITY; // Adjustable only for test of PhysicalShardMove. Should always be 0 for other
	                                      // cases
	int MAX_STORAGE_SERVER_WATCH_BYTES;
	int MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE;
	double LONG_BYTE_SAMPLE_RECOVERY_DELAY;
	int BYTE_SAMPLE_LOAD_PARALLELISM;
	double BYTE_SAMPLE_LOAD_DELAY;
	double BYTE_SAMPLE_START_DELAY;
	double BEHIND_CHECK_DELAY;
	int BEHIND_CHECK_COUNT;
	int64_t BEHIND_CHECK_VERSIONS;
	double WAIT_METRICS_WRONG_SHARD_CHANCE;
	// Minimum read throughput (in pages/second) that a tag must register
	// on a storage server in order for tag throughput statistics to be
	// emitted to ratekeeper.
	int64_t MIN_TAG_READ_PAGES_RATE;
	// Minimum write throughput (in pages/second, multiplied by fungibility ratio)
	// that a tag must register on a storage server in order for ratekeeper to
	// track the write throughput of this tag on the storage server.
	int64_t MIN_TAG_WRITE_PAGES_RATE;
	double TAG_MEASUREMENT_INTERVAL;
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
	int STORAGE_FEED_QUERY_HARD_LIMIT;
	std::string STORAGESERVER_READ_PRIORITIES;
	int STORAGE_SERVER_READ_CONCURRENCY;
	std::string STORAGESERVER_READTYPE_PRIORITY_MAP;
	int SPLIT_METRICS_MAX_ROWS;
	double STORAGE_SHARD_CONSISTENCY_CHECK_INTERVAL;
	bool CONSISTENCY_CHECK_BACKWARD_READ;
	int PHYSICAL_SHARD_MOVE_LOG_SEVERITY;
	int FETCH_SHARD_BUFFER_BYTE_LIMIT;
	int FETCH_SHARD_UPDATES_BYTE_LIMIT;

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
	bool GRAY_FAILURE_ALLOW_PRIMARY_SS_TO_COMPLAIN; // When enabled, storage servers in the primary DC are allowed to
	                                                // complain about their peers in the transaction subsystem e.g.
	                                                // buddy tlogs.
	bool GRAY_FAILURE_ALLOW_REMOTE_SS_TO_COMPLAIN; // When enabled, storage servers in the remote DC are allowed to
	                                               // complain about their peers in the transaction subsystem e.g. buddy
	                                               // tlogs.
	bool STORAGE_SERVER_REBOOT_ON_IO_TIMEOUT; // When enabled, storage server's worker will crash on io_timeout error;
	                                          // this allows fdbmonitor to restart the worker and recreate the same SS.
	                                          // When SS can be temporarily throttled by infrastructure, e.g, k8s,
	                                          // Enabling this can reduce toil of manually restarting the SS.
	                                          // Enable with caution: If io_timeout is caused by disk failure, we won't
	                                          // want to restart the SS, which increases risk of data corruption.
	int STORAGE_DISK_CLEANUP_MAX_RETRIES; // Max retries to cleanup left-over disk files from last storage server
	int STORAGE_DISK_CLEANUP_RETRY_INTERVAL; // Sleep interval between cleanup retries
	double WORKER_START_STORAGE_DELAY;

	// Test harness
	double WORKER_POLL_DELAY;

	// Adjust storage engine probability in simulation tests
	int PROBABILITY_FACTOR_SHARDED_ROCKSDB_ENGINE_SELECTED_SIM;
	int PROBABILITY_FACTOR_ROCKSDB_ENGINE_SELECTED_SIM;
	int PROBABILITY_FACTOR_SQLITE_ENGINE_SELECTED_SIM;
	int PROBABILITY_FACTOR_MEMORY_SELECTED_SIM;

	// Coordination
	double COORDINATED_STATE_ONCONFLICT_POLL_INTERVAL;
	bool ENABLE_CROSS_CLUSTER_SUPPORT; // Allow a coordinator to serve requests whose connection string does not match
	                                   // the local descriptor
	double FORWARD_REQUEST_TOO_OLD; // Do not forward requests older than this setting
	double COORDINATOR_LEADER_CONNECTION_TIMEOUT;

	// Dynamic Knobs (implementation)
	double COMPACTION_INTERVAL;
	double BROADCASTER_SELF_UPDATE_DELAY;
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
	int64_t FASTRESTORE_MONITOR_LEADER_DELAY;
	int64_t FASTRESTORE_STRAGGLER_THRESHOLD_SECONDS;
	bool FASTRESTORE_TRACK_REQUEST_LATENCY; // true to track reply latency of each request in a request batch
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
	bool REDWOOD_KVSTORE_RANGE_PREFETCH; // Whether to use range read prefetching
	double REDWOOD_PAGE_REBUILD_MAX_SLACK; // When rebuilding pages, max slack to allow in page before extending it
	double REDWOOD_PAGE_REBUILD_SLACK_DISTRIBUTION; // When rebuilding pages, use this ratio of slack distribution
	                                                // between the rightmost (new) page and the previous page. Defaults
	                                                // to .5 (50%) so that slack is evenly distributed between the
	                                                // pages. A value close to 0 indicates most slack to remain in the
	                                                // old page, where a value close to 1 causes the new page to have
	                                                // most of the slack. Immutable workloads with an increasing key
	                                                // pattern benefit from setting this to a value close to 1.
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
	int REDWOOD_NODE_MAX_UNBALANCE; // Maximum imbalance in a node before it should be rebuilt instead of updated

	std::string REDWOOD_IO_PRIORITIES;

	// Server request latency measurement
	double LATENCY_SKETCH_ACCURACY;
	double FILE_LATENCY_SKETCH_ACCURACY;
	double LATENCY_METRICS_LOGGING_INTERVAL;

	// Cluster recovery
	std::string CLUSTER_RECOVERY_EVENT_NAME_PREFIX;

	// Encryption
	int SIM_KMS_MAX_KEYS;
	int ENCRYPT_PROXY_MAX_DBG_TRACE_LENGTH;
	double ENCRYPTION_LOGGING_INTERVAL;
	double DISABLED_ENCRYPTION_PROBABILITY_SIM; // Probability that encryption is forced to be disabled in simulation

	// Compression
	bool ENABLE_BLOB_GRANULE_COMPRESSION;
	std::string BLOB_GRANULE_COMPRESSION_FILTER;

	// Key Management Service (KMS) Connector
	std::string KMS_CONNECTOR_TYPE;

	// blob granule stuff
	// FIXME: configure url with database configuration instead of knob eventually
	std::string BG_URL;

	// Whether to use knobs or EKP for blob metadata and credentials
	std::string BG_METADATA_SOURCE;

	bool BG_USE_BLOB_RANGE_CHANGE_LOG;

	int BG_SNAPSHOT_FILE_TARGET_BYTES;
	int BG_SNAPSHOT_FILE_TARGET_CHUNK_BYTES;
	int BG_DELTA_FILE_TARGET_BYTES;
	int BG_DELTA_FILE_TARGET_CHUNK_BYTES;
	int BG_DELTA_BYTES_BEFORE_COMPACT;
	int BG_MAX_SPLIT_FANOUT;
	int BG_MAX_MERGE_FANIN;
	int BG_HOT_SNAPSHOT_VERSIONS;
	int BG_CONSISTENCY_CHECK_ENABLED;
	int BG_CONSISTENCY_CHECK_TARGET_SPEED_KB;
	bool BG_ENABLE_MERGING;
	int BG_MERGE_CANDIDATE_THRESHOLD_SECONDS;
	int BG_MERGE_CANDIDATE_DELAY_SECONDS;
	int BG_KEY_TUPLE_TRUNCATE_OFFSET;
	bool BG_ENABLE_SPLIT_TRUNCATED;
	bool BG_ENABLE_READ_DRIVEN_COMPACTION;
	int BG_RDC_BYTES_FACTOR;
	int BG_RDC_READ_FACTOR;
	bool BG_WRITE_MULTIPART;
	bool BG_ENABLE_DYNAMIC_WRITE_AMP;
	double BG_DYNAMIC_WRITE_AMP_MIN_FACTOR;
	double BG_DYNAMIC_WRITE_AMP_DECREASE_FACTOR;

	int BLOB_WORKER_INITIAL_SNAPSHOT_PARALLELISM;
	int BLOB_WORKER_RESNAPSHOT_PARALLELISM;
	int BLOB_WORKER_DELTA_FILE_WRITE_PARALLELISM;
	int BLOB_WORKER_RDC_PARALLELISM;
	// The resnapshot/delta parallelism knobs are deprecated and replaced by the budget_bytes knobs! FIXME: remove after
	// next release
	int64_t BLOB_WORKER_RESNAPSHOT_BUDGET_BYTES;
	int64_t BLOB_WORKER_DELTA_WRITE_BUDGET_BYTES;

	double BLOB_WORKER_TIMEOUT; // Blob Manager's reaction time to a blob worker failure
	double BLOB_WORKER_REQUEST_TIMEOUT; // Blob Worker's server-side request timeout
	double BLOB_WORKERLIST_FETCH_INTERVAL;
	double BLOB_WORKER_BATCH_GRV_INTERVAL;
	double BLOB_WORKER_EMPTY_GRV_INTERVAL;
	int BLOB_WORKER_GRV_HISTORY_MAX_SIZE;
	int64_t BLOB_WORKER_GRV_HISTORY_MIN_VERSION_GRANULARITY;
	bool BLOB_WORKER_DO_REJECT_WHEN_FULL;
	double BLOB_WORKER_REJECT_WHEN_FULL_THRESHOLD;
	double BLOB_WORKER_FORCE_FLUSH_CLEANUP_DELAY;
	bool BLOB_WORKER_DISK_ENABLED;
	int BLOB_WORKER_STORE_TYPE;
	double BLOB_WORKER_REJOIN_TIME;

	double BLOB_MANAGER_STATUS_EXP_BACKOFF_MIN;
	double BLOB_MANAGER_STATUS_EXP_BACKOFF_MAX;
	double BLOB_MANAGER_STATUS_EXP_BACKOFF_EXPONENT;
	int BLOB_MANAGER_CONCURRENT_MERGE_CHECKS;
	bool BLOB_MANAGER_ENABLE_MEDIAN_ASSIGNMENT_LIMITING;
	double BLOB_MANAGER_MEDIAN_ASSIGNMENT_ALLOWANCE;
	int BLOB_MANAGER_MEDIAN_ASSIGNMENT_MIN_SAMPLES_PER_WORKER;
	int BLOB_MANAGER_MEDIAN_ASSIGNMENT_MAX_SAMPLES_PER_WORKER;
	double BGCC_TIMEOUT;
	double BGCC_MIN_INTERVAL;
	bool BLOB_MANIFEST_BACKUP;
	double BLOB_MANIFEST_BACKUP_INTERVAL;
	double BLOB_MIGRATOR_CHECK_INTERVAL;
	int BLOB_MANIFEST_RW_ROWS;
	int BLOB_MANIFEST_MAX_ROWS_PER_TRANSACTION;
	int BLOB_MANIFEST_RETRY_INTERVAL;
	int BLOB_MIGRATOR_ERROR_RETRIES;
	int BLOB_MIGRATOR_PREPARE_TIMEOUT;
	int BLOB_RESTORE_MANIFEST_FILE_MAX_SIZE;
	int BLOB_RESTORE_MANIFEST_RETENTION_MAX;
	int BLOB_RESTORE_MLOGS_RETENTION_SECS;
	int BLOB_RESTORE_LOAD_KEY_VERSION_MAP_STEP_SIZE;
	int BLOB_GRANULES_FLUSH_BATCH_SIZE;
	bool BLOB_RESTORE_SKIP_EMPTY_RANGES;

	// Blob metadata
	int64_t BLOB_METADATA_CACHE_TTL;

	// HTTP KMS Connector
	std::string REST_KMS_CONNECTOR_KMS_DISCOVERY_URL_MODE;
	std::string REST_KMS_CONNECTOR_DISCOVER_KMS_URL_FILE;
	std::string REST_KMS_CONNECTOR_VALIDATION_TOKEN_MODE;
	std::string REST_KMS_CONNECTOR_VALIDATION_TOKEN_DETAILS;
	bool ENABLE_REST_KMS_COMMUNICATION;
	bool REST_KMS_CONNECTOR_REMOVE_TRAILING_NEWLINE;
	int REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE;
	int REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE;
	bool REST_KMS_CONNECTOR_REFRESH_KMS_URLS;
	double REST_KMS_CONNECTOR_REFRESH_KMS_URLS_INTERVAL_SEC;
	std::string REST_KMS_CONNECTOR_GET_ENCRYPTION_KEYS_ENDPOINT;
	std::string REST_KMS_CONNECTOR_GET_LATEST_ENCRYPTION_KEYS_ENDPOINT;
	std::string REST_KMS_CONNECTOR_GET_BLOB_METADATA_ENDPOINT;
	int REST_KMS_CURRENT_BLOB_METADATA_REQUEST_VERSION;
	int REST_KMS_MAX_BLOB_METADATA_REQUEST_VERSION;
	int REST_KMS_CURRENT_CIPHER_REQUEST_VERSION;
	int REST_KMS_MAX_CIPHER_REQUEST_VERSION;
	std::string REST_SIM_KMS_VAULT_DIR;
	double REST_KMS_STABILITY_CHECK_INTERVAL;

	double CONSISTENCY_SCAN_ACTIVE_THROTTLE_RATIO;

	// Idempotency ids
	double IDEMPOTENCY_ID_IN_MEMORY_LIFETIME;
	double IDEMPOTENCY_IDS_CLEANER_POLLING_INTERVAL;
	double IDEMPOTENCY_IDS_MIN_AGE_SECONDS;

	// Swift: Enable the Swift runtime hooks and use Swift implementations where possible
	bool FLOW_WITH_SWIFT;

	ServerKnobs(Randomize, ClientKnobs*, IsSimulated);
	void initialize(Randomize, ClientKnobs*, IsSimulated);
};
