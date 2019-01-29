/*
 * Knobs.cpp
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

#include "fdbserver/Knobs.h"
#include "fdbrpc/Locality.h"
#include <cmath>

ServerKnobs const* SERVER_KNOBS = new ServerKnobs();

#define init( knob, value ) initKnob( knob, value, #knob )

ServerKnobs::ServerKnobs(bool randomize, ClientKnobs* clientKnobs) {
	// Versions
	init( VERSIONS_PER_SECOND,                                   1e6 );
	init( MAX_VERSIONS_IN_FLIGHT,                100 * VERSIONS_PER_SECOND );
	init( MAX_VERSIONS_IN_FLIGHT_FORCED,         6e5 * VERSIONS_PER_SECOND ); //one week of versions
	init( MAX_READ_TRANSACTION_LIFE_VERSIONS,      5 * VERSIONS_PER_SECOND ); if (randomize && BUGGIFY) MAX_READ_TRANSACTION_LIFE_VERSIONS = VERSIONS_PER_SECOND; else if (randomize && BUGGIFY) MAX_READ_TRANSACTION_LIFE_VERSIONS = std::max<int>(1, 0.1 * VERSIONS_PER_SECOND); else if( randomize && BUGGIFY ) MAX_READ_TRANSACTION_LIFE_VERSIONS = 10 * VERSIONS_PER_SECOND;
	init( MAX_WRITE_TRANSACTION_LIFE_VERSIONS,     5 * VERSIONS_PER_SECOND ); if (randomize && BUGGIFY) MAX_WRITE_TRANSACTION_LIFE_VERSIONS=std::max<int>(1, 1 * VERSIONS_PER_SECOND);
	init( MAX_COMMIT_BATCH_INTERVAL,                             0.5 ); if( randomize && BUGGIFY ) MAX_COMMIT_BATCH_INTERVAL = 2.0; // Each master proxy generates a CommitTransactionBatchRequest at least this often, so that versions always advance smoothly
	MAX_COMMIT_BATCH_INTERVAL = std::min(MAX_COMMIT_BATCH_INTERVAL, MAX_READ_TRANSACTION_LIFE_VERSIONS/double(2*VERSIONS_PER_SECOND)); // Ensure that the proxy commits 2 times every MAX_READ_TRANSACTION_LIFE_VERSIONS, otherwise the master will not give out versions fast enough

	// TLogs
	init( TLOG_TIMEOUT,                                          0.4 ); //cannot buggify because of availability
	init( RECOVERY_TLOG_SMART_QUORUM_DELAY,                     0.25 ); if( randomize && BUGGIFY ) RECOVERY_TLOG_SMART_QUORUM_DELAY = 0.0; // smaller might be better for bug amplification
	init( TLOG_STORAGE_MIN_UPDATE_INTERVAL,                      0.5 );
	init( BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL,               30 );
	init( UNFLUSHED_DATA_RATIO,                                 0.05 ); if( randomize && BUGGIFY ) UNFLUSHED_DATA_RATIO = 0.0;
	init( DESIRED_TOTAL_BYTES,                                150000 ); if( randomize && BUGGIFY ) DESIRED_TOTAL_BYTES = 10000;
	init( DESIRED_UPDATE_BYTES,                2*DESIRED_TOTAL_BYTES );
	init( UPDATE_DELAY,                                        0.001 );
	init( MAXIMUM_PEEK_BYTES,                                   10e6 );
	init( APPLY_MUTATION_BYTES,                                  1e6 );
	init( RECOVERY_DATA_BYTE_LIMIT,                           100000 );
	init( BUGGIFY_RECOVERY_DATA_LIMIT,                          1000 );
	init( LONG_TLOG_COMMIT_TIME,                                0.25 ); //cannot buggify because of recovery time
	init( LARGE_TLOG_COMMIT_BYTES,                             4<<20 );
	init( BUGGIFY_RECOVER_MEMORY_LIMIT,                          1e6 );
	init( BUGGIFY_WORKER_REMOVED_MAX_LAG,                         30 );
	init( UPDATE_STORAGE_BYTE_LIMIT,                             1e6 );
	init( TLOG_PEEK_DELAY,                                   0.00005 );
	init( LEGACY_TLOG_UPGRADE_ENTRIES_PER_VERSION,               100 );
	init( VERSION_MESSAGES_OVERHEAD_FACTOR_1024THS,             1072 ); // Based on a naive interpretation of the gcc version of std::deque, we would expect this to be 16 bytes overhead per 512 bytes data. In practice, it seems to be 24 bytes overhead per 512.
	init( VERSION_MESSAGES_ENTRY_BYTES_WITH_OVERHEAD, std::ceil(16.0 * VERSION_MESSAGES_OVERHEAD_FACTOR_1024THS / 1024) );
	init( LOG_SYSTEM_PUSHED_DATA_BLOCK_SIZE,                     1e5 );
	init( MAX_MESSAGE_SIZE,            std::max<int>(LOG_SYSTEM_PUSHED_DATA_BLOCK_SIZE, 1e5 + 2e4 + 1) + 8 ); // VALUE_SIZE_LIMIT + SYSTEM_KEY_SIZE_LIMIT + 9 bytes (4 bytes for length, 4 bytes for sequence number, and 1 byte for mutation type)
	init( TLOG_MESSAGE_BLOCK_BYTES,                             10e6 );
	init( TLOG_MESSAGE_BLOCK_OVERHEAD_FACTOR,      double(TLOG_MESSAGE_BLOCK_BYTES) / (TLOG_MESSAGE_BLOCK_BYTES - MAX_MESSAGE_SIZE) ); //1.0121466709838096006362758832473
	init( PEEK_TRACKER_EXPIRATION_TIME,                          600 ); if( randomize && BUGGIFY ) PEEK_TRACKER_EXPIRATION_TIME = g_random->coinflip() ? 0.1 : 60;
	init( PARALLEL_GET_MORE_REQUESTS,                             32 ); if( randomize && BUGGIFY ) PARALLEL_GET_MORE_REQUESTS = 2;
	init( MULTI_CURSOR_PRE_FETCH_LIMIT,                           10 );
	init( MAX_QUEUE_COMMIT_BYTES,                               15e6 ); if( randomize && BUGGIFY ) MAX_QUEUE_COMMIT_BYTES = 5000;
	init( VERSIONS_PER_BATCH,                 VERSIONS_PER_SECOND/20 ); if( randomize && BUGGIFY ) VERSIONS_PER_BATCH = std::max<int64_t>(1,VERSIONS_PER_SECOND/1000);
	init( CONCURRENT_LOG_ROUTER_READS,                             1 );
	init( DISK_QUEUE_ADAPTER_MIN_SWITCH_TIME,                    1.0 );
	init( DISK_QUEUE_ADAPTER_MAX_SWITCH_TIME,                    5.0 );

	// Data distribution queue
	init( HEALTH_POLL_TIME,                                      1.0 );
	init( BEST_TEAM_STUCK_DELAY,                                 1.0 );
	init( BG_DD_POLLING_INTERVAL,                               10.0 );
	init( DD_QUEUE_LOGGING_INTERVAL,                             5.0 );
	init( RELOCATION_PARALLELISM_PER_SOURCE_SERVER,                4 ); if( randomize && BUGGIFY ) RELOCATION_PARALLELISM_PER_SOURCE_SERVER = 1;
	init( DD_QUEUE_MAX_KEY_SERVERS,                              100 ); if( randomize && BUGGIFY ) DD_QUEUE_MAX_KEY_SERVERS = 1;
	init( DD_REBALANCE_PARALLELISM,                               50 );
	init( DD_REBALANCE_RESET_AMOUNT,                              30 );
	init( BG_DD_MAX_WAIT,                                      120.0 );
	init( BG_DD_MIN_WAIT,                                        0.1 );
	init( BG_DD_INCREASE_RATE,                                  1.10 );
	init( BG_DD_DECREASE_RATE,                                  1.02 );
	init( BG_DD_SATURATION_DELAY,                                1.0 );
	init( INFLIGHT_PENALTY_HEALTHY,                              1.0 );
	init( INFLIGHT_PENALTY_UNHEALTHY,                           10.0 );
	init( INFLIGHT_PENALTY_ONE_LEFT,                          1000.0 );

	// Data distribution
	init( RETRY_RELOCATESHARD_DELAY,                             0.1 );
	init( DATA_DISTRIBUTION_FAILURE_REACTION_TIME,              10.0 ); if( randomize && BUGGIFY ) DATA_DISTRIBUTION_FAILURE_REACTION_TIME = 1.0;
	bool buggifySmallShards = randomize && BUGGIFY;
	init( MIN_SHARD_BYTES,                                    200000 ); if( buggifySmallShards ) MIN_SHARD_BYTES = 40000; //FIXME: data distribution tracker (specifically StorageMetrics) relies on this number being larger than the maximum size of a key value pair
	init( SHARD_BYTES_RATIO,                                       4 );
	init( SHARD_BYTES_PER_SQRT_BYTES,                             45 ); if( buggifySmallShards ) SHARD_BYTES_PER_SQRT_BYTES = 0;//Approximately 10000 bytes per shard
	init( MAX_SHARD_BYTES,                                 500000000 );
	init( KEY_SERVER_SHARD_BYTES,                          500000000 );
	bool buggifySmallBandwidthSplit = randomize && BUGGIFY;
	init( SHARD_MAX_BYTES_PER_KSEC,                 1LL*1000000*1000 ); if( buggifySmallBandwidthSplit ) SHARD_MAX_BYTES_PER_KSEC = 10LL*1000*1000;
	/* 10*1MB/sec * 1000sec/ksec
		Shards with more than this bandwidth will be split immediately.
		For a large shard (100MB), splitting it costs ~100MB of work or about 10MB/sec over a 10 sec sampling window.
		If the sampling window is too much longer, the MVCC window will fill up while we wait.
		If SHARD_MAX_BYTES_PER_KSEC is too much lower, we could do a lot of data movement work in response to a small impulse of bandwidth.
		If SHARD_MAX_BYTES_PER_KSEC is too high relative to the I/O bandwidth of a given server, a workload can remain concentrated on a single
		team indefinitely, limiting performance.
		*/

	init( SHARD_MIN_BYTES_PER_KSEC,                100 * 1000 * 1000 ); if( buggifySmallBandwidthSplit ) SHARD_MIN_BYTES_PER_KSEC = 200*1*1000;
	/* 200*1KB/sec * 1000sec/ksec
		Shards with more than this bandwidth will not be merged.
		Obviously this needs to be significantly less than SHARD_MAX_BYTES_PER_KSEC, else we will repeatedly merge and split.
		It should probably be significantly less than SHARD_SPLIT_BYTES_PER_KSEC, else we will merge right after splitting.

		The number of extra shards in the database because of bandwidth splitting can't be more than about W/SHARD_MIN_BYTES_PER_KSEC, where
		W is the maximum bandwidth of the entire database in bytes/ksec.  For 250MB/sec write bandwidth, (250MB/sec)/(200KB/sec) = 1250 extra
		shards.

		The bandwidth sample maintained by the storage server needs to be accurate enough to reliably measure this minimum bandwidth.  See
		BANDWIDTH_UNITS_PER_SAMPLE.  If this number is too low, the storage server needs to spend more memory and time on sampling.
		*/

	init( SHARD_SPLIT_BYTES_PER_KSEC,              250 * 1000 * 1000 ); if( buggifySmallBandwidthSplit ) SHARD_SPLIT_BYTES_PER_KSEC = 50 * 1000 * 1000;
	/* 500*1KB/sec * 1000sec/ksec
		When splitting a shard, it is split into pieces with less than this bandwidth.
		Obviously this should be less than half of SHARD_MAX_BYTES_PER_KSEC.

		Smaller values mean that high bandwidth shards are split into more pieces, more quickly utilizing large numbers of servers to handle the
		bandwidth.

		Too many pieces (too small a value) may stress data movement mechanisms (see e.g. RELOCATION_PARALLELISM_PER_SOURCE_SERVER).

		If this value is too small relative to SHARD_MIN_BYTES_PER_KSEC immediate merging work will be generated.
		*/

	init( STORAGE_METRIC_TIMEOUT,                              600.0 ); if( randomize && BUGGIFY ) STORAGE_METRIC_TIMEOUT = g_random->coinflip() ? 10.0 : 60.0;
	init( METRIC_DELAY,                                          0.1 ); if( randomize && BUGGIFY ) METRIC_DELAY = 1.0;
	init( ALL_DATA_REMOVED_DELAY,                                1.0 );
	init( INITIAL_FAILURE_REACTION_DELAY,                       30.0 ); if( randomize && BUGGIFY ) INITIAL_FAILURE_REACTION_DELAY = 0.0;
	init( CHECK_TEAM_DELAY,                                     30.0 );
	init( LOG_ON_COMPLETION_DELAY,         DD_QUEUE_LOGGING_INTERVAL );
	init( BEST_TEAM_MAX_TEAM_TRIES,                               10 );
	init( BEST_TEAM_OPTION_COUNT,                                  4 );
	init( BEST_OF_AMT,                                             4 );
	init( SERVER_LIST_DELAY,                                     1.0 );
	init( RECRUITMENT_IDLE_DELAY,                                1.0 );
	init( STORAGE_RECRUITMENT_DELAY,                            10.0 );
	init( DATA_DISTRIBUTION_LOGGING_INTERVAL,                    5.0 );
	init( DD_ENABLED_CHECK_DELAY,                                1.0 );
	init( DD_MERGE_COALESCE_DELAY,                             120.0 ); if( randomize && BUGGIFY ) DD_MERGE_COALESCE_DELAY = 0.001;
	init( STORAGE_METRICS_POLLING_DELAY,                         2.0 ); if( randomize && BUGGIFY ) STORAGE_METRICS_POLLING_DELAY = 15.0;
	init( STORAGE_METRICS_RANDOM_DELAY,                          0.2 );
	init( FREE_SPACE_RATIO_CUTOFF,                               0.1 );
	init( FREE_SPACE_RATIO_DD_CUTOFF,                            0.2 );
	init( DESIRED_TEAMS_PER_SERVER,                                5 ); if( randomize && BUGGIFY ) DESIRED_TEAMS_PER_SERVER = 1;
	init( MAX_TEAMS_PER_SERVER,           3*DESIRED_TEAMS_PER_SERVER );
	init( DD_SHARD_SIZE_GRANULARITY,                         5000000 );
	init( DD_SHARD_SIZE_GRANULARITY_SIM,                      500000 ); if( randomize && BUGGIFY ) DD_SHARD_SIZE_GRANULARITY_SIM = 0;
	init( DD_MOVE_KEYS_PARALLELISM,                               20 ); if( randomize && BUGGIFY ) DD_MOVE_KEYS_PARALLELISM = 1;
	init( DD_MERGE_LIMIT,                                       2000 ); if( randomize && BUGGIFY ) DD_MERGE_LIMIT = 2;
	init( DD_SHARD_METRICS_TIMEOUT,                             60.0 ); if( randomize && BUGGIFY ) DD_SHARD_METRICS_TIMEOUT = 0.1;
	init( DD_LOCATION_CACHE_SIZE,                            2000000 ); if( randomize && BUGGIFY ) DD_LOCATION_CACHE_SIZE = 3;
	init( MOVEKEYS_LOCK_POLLING_DELAY,                           5.0 );
	init( DEBOUNCE_RECRUITING_DELAY,                             5.0 );

	// Redwood Storage Engine
	init( PREFIX_TREE_IMMEDIATE_KEY_SIZE_LIMIT,                   30 );
	init( PREFIX_TREE_IMMEDIATE_KEY_SIZE_MIN,                     0 );

	// KeyValueStore SQLITE
	init( CLEAR_BUFFER_SIZE,                                   20000 );
	init( READ_VALUE_TIME_ESTIMATE,                           .00005 );
	init( READ_RANGE_TIME_ESTIMATE,                           .00005 );
	init( SET_TIME_ESTIMATE,                                  .00005 );
	init( CLEAR_TIME_ESTIMATE,                                .00005 );
	init( COMMIT_TIME_ESTIMATE,                                 .005 );
	init( CHECK_FREE_PAGE_AMOUNT,                                100 ); if( randomize && BUGGIFY ) CHECK_FREE_PAGE_AMOUNT = 5;
	init( DISK_METRIC_LOGGING_INTERVAL,                          5.0 );
	init( SOFT_HEAP_LIMIT,                                     300e6 );

	init( SQLITE_PAGE_SCAN_ERROR_LIMIT,                        10000 );
	init( SQLITE_BTREE_PAGE_USABLE,                          4096 - 8);  // pageSize - reserveSize for page checksum

	// Maximum and minimum cell payload bytes allowed on primary page as calculated in SQLite.
	// These formulas are copied from SQLite, using its hardcoded constants, so if you are
	// changing this you should also be changing SQLite.
	init( SQLITE_BTREE_CELL_MAX_LOCAL,  (SQLITE_BTREE_PAGE_USABLE - 12) * 64/255 - 23 );
	init( SQLITE_BTREE_CELL_MIN_LOCAL,  (SQLITE_BTREE_PAGE_USABLE - 12) * 32/255 - 23 );

	// Maximum FDB fragment key and value bytes that can fit in a primary btree page
	init( SQLITE_FRAGMENT_PRIMARY_PAGE_USABLE,
					SQLITE_BTREE_CELL_MAX_LOCAL
					 - 1 // vdbeRecord header length size
					 - 2 // max key length size
					 - 4 // max index length size
					 - 2 // max value fragment length size
	);

	// Maximum FDB fragment value bytes in an overflow page
	init( SQLITE_FRAGMENT_OVERFLOW_PAGE_USABLE,
					SQLITE_BTREE_PAGE_USABLE
					 - 4 // next pageNumber size
	);
	init( SQLITE_FRAGMENT_MIN_SAVINGS,                          0.20 );

	// KeyValueStoreSqlite spring cleaning
	init( CLEANING_INTERVAL,                                     1.0 );
	init( SPRING_CLEANING_TIME_ESTIMATE,                        .010 );
	init( SPRING_CLEANING_VACUUMS_PER_LAZY_DELETE_PAGE,          0.0 ); if( randomize && BUGGIFY ) SPRING_CLEANING_VACUUMS_PER_LAZY_DELETE_PAGE = g_random->coinflip() ? 1e9 : g_random->random01() * 5;
	init( SPRING_CLEANING_MIN_LAZY_DELETE_PAGES,                   0 ); if( randomize && BUGGIFY ) SPRING_CLEANING_MIN_LAZY_DELETE_PAGES = g_random->randomInt(1, 100);
	init( SPRING_CLEANING_MAX_LAZY_DELETE_PAGES,                 1e9 ); if( randomize && BUGGIFY ) SPRING_CLEANING_MAX_LAZY_DELETE_PAGES = g_random->coinflip() ? 0 : g_random->randomInt(1, 1e4);
	init( SPRING_CLEANING_LAZY_DELETE_BATCH_SIZE,                100 ); if( randomize && BUGGIFY ) SPRING_CLEANING_LAZY_DELETE_BATCH_SIZE = g_random->randomInt(1, 1000);
	init( SPRING_CLEANING_MIN_VACUUM_PAGES,                        1 ); if( randomize && BUGGIFY ) SPRING_CLEANING_MIN_VACUUM_PAGES = g_random->randomInt(0, 100);
	init( SPRING_CLEANING_MAX_VACUUM_PAGES,                      1e9 ); if( randomize && BUGGIFY ) SPRING_CLEANING_MAX_VACUUM_PAGES = g_random->coinflip() ? 0 : g_random->randomInt(1, 1e4);

	// KeyValueStoreMemory
	init( REPLACE_CONTENTS_BYTES,                                1e5 ); if( randomize && BUGGIFY ) REPLACE_CONTENTS_BYTES = 1e3;

	// Leader election
	bool longLeaderElection = randomize && BUGGIFY;
	init( MAX_NOTIFICATIONS,                                  100000 );
	init( MIN_NOTIFICATIONS,                                     100 );
	init( NOTIFICATION_FULL_CLEAR_TIME,                      10000.0 );
	init( CANDIDATE_MIN_DELAY,                                  0.05 );
	init( CANDIDATE_MAX_DELAY,                                   1.0 );
	init( CANDIDATE_GROWTH_RATE,                                 1.2 );
	init( POLLING_FREQUENCY,                                     1.0 ); if( longLeaderElection ) POLLING_FREQUENCY = 8.0;
	init( HEARTBEAT_FREQUENCY,                                  0.25 ); if( longLeaderElection ) HEARTBEAT_FREQUENCY = 1.0;

	// Master Proxy
	init( START_TRANSACTION_BATCH_INTERVAL_MIN,                 1e-6 );
	init( START_TRANSACTION_BATCH_INTERVAL_MAX,                0.010 );
	init( START_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION,     0.5 );
	init( START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA,       0.1 );
	init( START_TRANSACTION_BATCH_QUEUE_CHECK_INTERVAL,        0.001 );
	init( START_TRANSACTION_MAX_TRANSACTIONS_TO_START,         10000 );
	init( START_TRANSACTION_MAX_BUDGET_SIZE,                      20 ); // Currently set to match CLIENT_KNOBS->MAX_BATCH_SIZE

	init( COMMIT_TRANSACTION_BATCH_INTERVAL_FROM_IDLE,         0.0005 ); if( randomize && BUGGIFY ) COMMIT_TRANSACTION_BATCH_INTERVAL_FROM_IDLE = 0.005;
	init( COMMIT_TRANSACTION_BATCH_INTERVAL_MIN,                0.001 ); if( randomize && BUGGIFY ) COMMIT_TRANSACTION_BATCH_INTERVAL_MIN = 0.1;
	init( COMMIT_TRANSACTION_BATCH_INTERVAL_MAX,                0.020 );
	init( COMMIT_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION,     0.1 );
	init( COMMIT_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA,       0.1 );
	init( COMMIT_TRANSACTION_BATCH_COUNT_MAX,                   32768 ); if( randomize && BUGGIFY ) COMMIT_TRANSACTION_BATCH_COUNT_MAX = 1000; // Do NOT increase this number beyond 32768, as CommitIds only budget 2 bytes for storing transaction id within each batch
	init( COMMIT_BATCHES_MEM_BYTES_HARD_LIMIT,              8LL << 30 ); if (randomize && BUGGIFY) COMMIT_BATCHES_MEM_BYTES_HARD_LIMIT = g_random->randomInt64(100LL << 20,  8LL << 30);
	init( COMMIT_BATCHES_MEM_FRACTION_OF_TOTAL,                   0.5 );
	init( COMMIT_BATCHES_MEM_TO_TOTAL_MEM_SCALE_FACTOR,          10.0 );

	// these settings disable batch bytes scaling.  Try COMMIT_TRANSACTION_BATCH_BYTES_MAX=1e6, COMMIT_TRANSACTION_BATCH_BYTES_SCALE_BASE=50000, COMMIT_TRANSACTION_BATCH_BYTES_SCALE_POWER=0.5?
	init( COMMIT_TRANSACTION_BATCH_BYTES_MIN,                  100000 );
	init( COMMIT_TRANSACTION_BATCH_BYTES_MAX,                  100000 ); if( randomize && BUGGIFY ) { COMMIT_TRANSACTION_BATCH_BYTES_MIN = COMMIT_TRANSACTION_BATCH_BYTES_MAX = 1000000; }
	init( COMMIT_TRANSACTION_BATCH_BYTES_SCALE_BASE,           100000 );
	init( COMMIT_TRANSACTION_BATCH_BYTES_SCALE_POWER,             0.0 );

	init( TRANSACTION_BUDGET_TIME,							   0.050 ); if( randomize && BUGGIFY ) TRANSACTION_BUDGET_TIME = 0.0;
	init( RESOLVER_COALESCE_TIME,                                1.0 );
	init( BUGGIFIED_ROW_LIMIT,                  APPLY_MUTATION_BYTES ); if( randomize && BUGGIFY ) BUGGIFIED_ROW_LIMIT = g_random->randomInt(3, 30);
	init( PROXY_SPIN_DELAY,                                     0.01 );
	init( UPDATE_REMOTE_LOG_VERSION_INTERVAL,                    2.0 );
	init( MAX_TXS_POP_VERSION_HISTORY,                           1e5 );

	// Master Server
	// masterCommitter() in the master server will allow lower priority tasks (e.g. DataDistibution)
	//  by delay()ing for this amount of time between accepted batches of TransactionRequests.
	bool fastBalancing = randomize && BUGGIFY;
	init( COMMIT_SLEEP_TIME,								  0.0001 ); if( randomize && BUGGIFY ) COMMIT_SLEEP_TIME = 0;
	init( KEY_BYTES_PER_SAMPLE,                                  2e4 ); if( fastBalancing ) KEY_BYTES_PER_SAMPLE = 1e3;
	init( MIN_BALANCE_TIME,                                      0.2 );
	init( MIN_BALANCE_DIFFERENCE,                                1e6 ); if( fastBalancing ) MIN_BALANCE_DIFFERENCE = 1e4;
	init( SECONDS_BEFORE_NO_FAILURE_DELAY,                  8 * 3600 );
	init( MAX_TXS_SEND_MEMORY,                                   1e7 ); if( randomize && BUGGIFY ) MAX_TXS_SEND_MEMORY = 1e5;
	init( MAX_RECOVERY_VERSIONS,           200 * VERSIONS_PER_SECOND );
	init( MAX_RECOVERY_TIME,                                    20.0 ); if( randomize && BUGGIFY ) MAX_RECOVERY_TIME = 1.0;

	// Resolver
	init( SAMPLE_OFFSET_PER_KEY,                                 100 );
	init( SAMPLE_EXPIRATION_TIME,                                1.0 );
	init( SAMPLE_POLL_TIME,                                      0.1 );
	init( RESOLVER_STATE_MEMORY_LIMIT,                           1e6 );
	init( LAST_LIMITED_RATIO,                                    0.6 );

	//Cluster Controller
	init( CLUSTER_CONTROLLER_LOGGING_DELAY,                      5.0 );
	init( MASTER_FAILURE_REACTION_TIME,                          0.4 ); if( randomize && BUGGIFY ) MASTER_FAILURE_REACTION_TIME = 10.0;
	init( MASTER_FAILURE_SLOPE_DURING_RECOVERY,                  0.1 );
	init( WORKER_COORDINATION_PING_DELAY,                         60 );
	init( SIM_SHUTDOWN_TIMEOUT,                                   10 );
	init( SHUTDOWN_TIMEOUT,                                      600 ); if( randomize && BUGGIFY ) SHUTDOWN_TIMEOUT = 60.0;
	init( MASTER_SPIN_DELAY,                                     1.0 ); if( randomize && BUGGIFY ) MASTER_SPIN_DELAY = 10.0;
	init( CC_CHANGE_DELAY,                                       0.1 );
	init( CC_CLASS_DELAY,                                       0.01 );
	init( WAIT_FOR_GOOD_RECRUITMENT_DELAY,                       1.0 );
	init( WAIT_FOR_GOOD_REMOTE_RECRUITMENT_DELAY,                5.0 );
	init( ATTEMPT_RECRUITMENT_DELAY,                           0.035 );
	init( WORKER_FAILURE_TIME,                                   1.0 ); if( randomize && BUGGIFY ) WORKER_FAILURE_TIME = 10.0;
	init( CHECK_OUTSTANDING_INTERVAL,                            0.5 ); if( randomize && BUGGIFY ) CHECK_OUTSTANDING_INTERVAL = 0.001;
	init( VERSION_LAG_METRIC_INTERVAL,                           0.5 ); if( randomize && BUGGIFY ) VERSION_LAG_METRIC_INTERVAL = 10.0;
	init( MAX_VERSION_DIFFERENCE,           20 * VERSIONS_PER_SECOND );

	init( INCOMPATIBLE_PEERS_LOGGING_INTERVAL,                   600 ); if( randomize && BUGGIFY ) INCOMPATIBLE_PEERS_LOGGING_INTERVAL = 60.0;
	init( EXPECTED_MASTER_FITNESS,             ProcessClass::UnsetFit );
	init( EXPECTED_TLOG_FITNESS,               ProcessClass::UnsetFit );
	init( EXPECTED_LOG_ROUTER_FITNESS,         ProcessClass::UnsetFit );
	init( EXPECTED_PROXY_FITNESS,              ProcessClass::UnsetFit );
	init( EXPECTED_RESOLVER_FITNESS,           ProcessClass::UnsetFit );
	init( RECRUITMENT_TIMEOUT,                                   600 ); if( randomize && BUGGIFY ) RECRUITMENT_TIMEOUT = g_random->coinflip() ? 60.0 : 1.0;

	init( POLICY_RATING_TESTS,                                   200 ); if( randomize && BUGGIFY ) POLICY_RATING_TESTS = 20;
	init( POLICY_GENERATIONS,                                    100 ); if( randomize && BUGGIFY ) POLICY_GENERATIONS = 10;

	//Move Keys
	init( SHARD_READY_DELAY,                                    0.25 );
	init( SERVER_READY_QUORUM_INTERVAL,                         std::min(1.0, std::min(MAX_READ_TRANSACTION_LIFE_VERSIONS, MAX_WRITE_TRANSACTION_LIFE_VERSIONS)/(5.0*VERSIONS_PER_SECOND)) );
	init( SERVER_READY_QUORUM_TIMEOUT,                          15.0 ); if( randomize && BUGGIFY ) SERVER_READY_QUORUM_TIMEOUT = 1.0;
	init( REMOVE_RETRY_DELAY,                                    1.0 );
	init( MOVE_KEYS_KRM_LIMIT,                                  2000 ); if( randomize && BUGGIFY ) MOVE_KEYS_KRM_LIMIT = 2;
	init( MOVE_KEYS_KRM_LIMIT_BYTES,                             1e5 ); if( randomize && BUGGIFY ) MOVE_KEYS_KRM_LIMIT_BYTES = 5e4; //This must be sufficiently larger than CLIENT_KNOBS->KEY_SIZE_LIMIT (fdbclient/Knobs.h) to ensure that at least two entries will be returned from an attempt to read a key range map
	init( MAX_SKIP_TAGS,                                           1 ); //The TLogs require tags to be densely packed to be memory efficient, so be careful increasing this knob
	init( MAX_ADDED_SOURCES_MULTIPLIER,                          2.0 );

	//FdbServer
	bool longReboots = randomize && BUGGIFY;
	init( MIN_REBOOT_TIME,                                       4.0 ); if( longReboots ) MIN_REBOOT_TIME = 10.0;
	init( MAX_REBOOT_TIME,                                       5.0 ); if( longReboots ) MAX_REBOOT_TIME = 20.0;
	init( LOG_DIRECTORY,                                          ".");  // Will be set to the command line flag.
	init(SERVER_MEM_LIMIT, 8LL << 30);

	//Ratekeeper
	bool slowRateKeeper = randomize && BUGGIFY;
	init( SMOOTHING_AMOUNT,                                      1.0 ); if( slowRateKeeper ) SMOOTHING_AMOUNT = 5.0;
	init( SLOW_SMOOTHING_AMOUNT,                                10.0 ); if( slowRateKeeper ) SLOW_SMOOTHING_AMOUNT = 50.0;
	init( METRIC_UPDATE_RATE,                                     .1 ); if( slowRateKeeper ) METRIC_UPDATE_RATE = 0.5;

	bool smallStorageTarget = randomize && BUGGIFY;
	init( TARGET_BYTES_PER_STORAGE_SERVER,                    1000e6 ); if( smallStorageTarget ) TARGET_BYTES_PER_STORAGE_SERVER = 3000e3;
	init( SPRING_BYTES_STORAGE_SERVER,                         100e6 ); if( smallStorageTarget ) SPRING_BYTES_STORAGE_SERVER = 300e3;
	init( STORAGE_HARD_LIMIT_BYTES,                           1500e6 ); if( smallStorageTarget ) STORAGE_HARD_LIMIT_BYTES = 4500e3;

	bool smallTlogTarget = randomize && BUGGIFY;
	init( TARGET_BYTES_PER_TLOG,                              2400e6 ); if( smallTlogTarget ) TARGET_BYTES_PER_TLOG = 2000e3;
	init( SPRING_BYTES_TLOG,								   400e6 ); if( smallTlogTarget ) SPRING_BYTES_TLOG = 200e3;
	init( TLOG_SPILL_THRESHOLD,                               1500e6 ); if( smallTlogTarget ) TLOG_SPILL_THRESHOLD = 1500e3; if( randomize && BUGGIFY ) TLOG_SPILL_THRESHOLD = 0;
	init( TLOG_HARD_LIMIT_BYTES,                              3000e6 ); if( smallTlogTarget ) TLOG_HARD_LIMIT_BYTES = 3000e3;
	init( TLOG_RECOVER_MEMORY_LIMIT, TARGET_BYTES_PER_TLOG + SPRING_BYTES_TLOG );

	init( MAX_TRANSACTIONS_PER_BYTE,                            1000 );

	init( MIN_FREE_SPACE,                                        1e8 );
	init( MIN_FREE_SPACE_RATIO,                                 0.05 );

	init( MAX_TL_SS_VERSION_DIFFERENCE,                         1e99 ); // if( randomize && BUGGIFY ) MAX_TL_SS_VERSION_DIFFERENCE = std::max(1.0, 0.25 * VERSIONS_PER_SECOND); // spring starts at half this value //FIXME: this knob causes ratekeeper to clamp on idle cluster in simulation that have a large number of logs
	init( MAX_MACHINES_FALLING_BEHIND,                             1 );

	//Storage Metrics
	init( STORAGE_METRICS_AVERAGE_INTERVAL,                    120.0 );
	init( STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS,        1000.0 / STORAGE_METRICS_AVERAGE_INTERVAL );  // milliHz!
	init( SPLIT_JITTER_AMOUNT,                                  0.05 ); if( randomize && BUGGIFY ) SPLIT_JITTER_AMOUNT = 0.2;
	init( IOPS_UNITS_PER_SAMPLE,                                10000 * 1000 / STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS / 100 );
	init( BANDWIDTH_UNITS_PER_SAMPLE,                           SHARD_MIN_BYTES_PER_KSEC / STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS / 25 );

	//Storage Server
	init( STORAGE_LOGGING_DELAY,                                 5.0 );
	init( STORAGE_SERVER_POLL_METRICS_DELAY,                     1.0 );
	init( FUTURE_VERSION_DELAY,                                  1.0 );
	init( STORAGE_LIMIT_BYTES,                                500000 );
	init( BUGGIFY_LIMIT_BYTES,                                  1000 );
	init( FETCH_BLOCK_BYTES,                                     2e6 );
	init( FETCH_KEYS_PARALLELISM_BYTES,                          5e6 ); if( randomize && BUGGIFY ) FETCH_KEYS_PARALLELISM_BYTES = 4e6;
	init( BUGGIFY_BLOCK_BYTES,                                 10000 );
	init( STORAGE_COMMIT_BYTES,                             10000000 ); if( randomize && BUGGIFY ) STORAGE_COMMIT_BYTES = 2000000;
	init( STORAGE_COMMIT_INTERVAL,                               0.5 ); if( randomize && BUGGIFY ) STORAGE_COMMIT_INTERVAL = 2.0;
	init( UPDATE_SHARD_VERSION_INTERVAL,                        0.25 ); if( randomize && BUGGIFY ) UPDATE_SHARD_VERSION_INTERVAL = 1.0;
	init( BYTE_SAMPLING_FACTOR,                                  250 ); //cannot buggify because of differences in restarting tests
	init( BYTE_SAMPLING_OVERHEAD,                                100 );
	init( MAX_STORAGE_SERVER_WATCH_BYTES,                      100e6 ); if( randomize && BUGGIFY ) MAX_STORAGE_SERVER_WATCH_BYTES = 10e3;
	init( MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE,                        1e9 ); if( randomize && BUGGIFY ) MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE = 1e3;
	init( LONG_BYTE_SAMPLE_RECOVERY_DELAY,                      60.0 );
	init( BYTE_SAMPLE_LOAD_PARALLELISM,                           32 ); if( randomize && BUGGIFY ) BYTE_SAMPLE_LOAD_PARALLELISM = 1;
	init( BYTE_SAMPLE_LOAD_DELAY,                                0.0 ); if( randomize && BUGGIFY ) BYTE_SAMPLE_LOAD_DELAY = 0.1;

	//Wait Failure
	init( BUGGIFY_OUTSTANDING_WAIT_FAILURE_REQUESTS,               2 );
	init( MAX_OUTSTANDING_WAIT_FAILURE_REQUESTS,                 250 ); if( randomize && BUGGIFY ) MAX_OUTSTANDING_WAIT_FAILURE_REQUESTS = 2;
	init( WAIT_FAILURE_DELAY_LIMIT,                              1.0 ); if( randomize && BUGGIFY ) WAIT_FAILURE_DELAY_LIMIT = 5.0;

	//Worker
	init( WORKER_LOGGING_INTERVAL,                               5.0 );
	init( INCOMPATIBLE_PEER_DELAY_BEFORE_LOGGING,                5.0 );

	// Test harness
	init( WORKER_POLL_DELAY,                                     1.0 );

	// Coordination
	init( COORDINATED_STATE_ONCONFLICT_POLL_INTERVAL,            1.0 ); if( randomize && BUGGIFY ) COORDINATED_STATE_ONCONFLICT_POLL_INTERVAL = 10.0;

	// Buggification
	init( BUGGIFIED_EVENTUAL_CONSISTENCY,                        1.0 );
	BUGGIFY_ALL_COORDINATION =                                   false;   if( randomize && BUGGIFY ) { BUGGIFY_ALL_COORDINATION = true; TraceEvent("BuggifyAllCoordination"); }

	// Status
	init( STATUS_MIN_TIME_BETWEEN_REQUESTS,                      0.0 );
	init( CONFIGURATION_ROWS_TO_FETCH,                         20000 );

	// IPager
	init( PAGER_RESERVED_PAGES,                                    1 );

	// IndirectShadowPager
	init( FREE_PAGE_VACUUM_THRESHOLD,                              1 );
	init( VACUUM_QUEUE_SIZE,                                  100000 );
	init( VACUUM_BYTES_PER_SECOND,                               1e6 );

	// Timekeeper
	init( TIME_KEEPER_DELAY,                                      10 );
	init( TIME_KEEPER_MAX_ENTRIES,                              3600 * 24 * 30 * 6); if( randomize && BUGGIFY ) { TIME_KEEPER_MAX_ENTRIES = 2; }

	if(clientKnobs)
		clientKnobs->IS_ACCEPTABLE_DELAY = clientKnobs->IS_ACCEPTABLE_DELAY*std::min(MAX_READ_TRANSACTION_LIFE_VERSIONS, MAX_WRITE_TRANSACTION_LIFE_VERSIONS)/(5.0*VERSIONS_PER_SECOND);
}
