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

#include "fdbclient/Knobs.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "flow/UnitTest.h"

ClientKnobs const* CLIENT_KNOBS = new ClientKnobs();

#define init( knob, value ) initKnob( knob, value, #knob )

ClientKnobs::ClientKnobs() {
	initialize();
}

void ClientKnobs::initialize(bool randomize) {
	// clang-format off

	init( TOO_MANY,                            1000000 );

	init( SYSTEM_MONITOR_INTERVAL,                 5.0 );

	init( FAILURE_MAX_DELAY,                       5.0 );
	init( FAILURE_MIN_DELAY,                       4.0 ); if( randomize && BUGGIFY ) FAILURE_MIN_DELAY = 1.0;
	init( FAILURE_TIMEOUT_DELAY,     FAILURE_MIN_DELAY );
	init( CLIENT_FAILURE_TIMEOUT_DELAY, FAILURE_MIN_DELAY );
	init( FAILURE_EMERGENCY_DELAY,                30.0 );
	init( FAILURE_MAX_GENERATIONS,                  10 );
	init( RECOVERY_DELAY_START_GENERATION,          70 );
	init( RECOVERY_DELAY_SECONDS_PER_GENERATION,  60.0 );
	init( MAX_GENERATIONS,                         100 );
	init( MAX_GENERATIONS_OVERRIDE,                  0 );

	init( COORDINATOR_RECONNECTION_DELAY,          1.0 );
	init( CLIENT_EXAMPLE_AMOUNT,                    20 );
	init( MAX_CLIENT_STATUS_AGE,                   1.0 );
	init( MAX_PROXY_CONNECTIONS,                     5 ); if( randomize && BUGGIFY ) MAX_PROXY_CONNECTIONS = 1;
	init( STATUS_IDLE_TIMEOUT,                   120.0 );

	// wrong_shard_server sometimes comes from the only nonfailed server, so we need to avoid a fast spin

	init( WRONG_SHARD_SERVER_DELAY,                .01 ); if( randomize && BUGGIFY ) WRONG_SHARD_SERVER_DELAY = deterministicRandom()->random01(); // FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY; // SOMEDAY: This delay can limit performance of retrieving data when the cache is mostly wrong (e.g. dumping the database after a test)
	init( FUTURE_VERSION_RETRY_DELAY,              .01 ); if( randomize && BUGGIFY ) FUTURE_VERSION_RETRY_DELAY = deterministicRandom()->random01();// FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY;
	init( REPLY_BYTE_LIMIT,                      80000 );
	init( DEFAULT_BACKOFF,                         .01 ); if( randomize && BUGGIFY ) DEFAULT_BACKOFF = deterministicRandom()->random01();
	init( DEFAULT_MAX_BACKOFF,                     1.0 );
	init( BACKOFF_GROWTH_RATE,                     2.0 );
	init( RESOURCE_CONSTRAINED_MAX_BACKOFF,       30.0 );
	init( PROXY_COMMIT_OVERHEAD_BYTES,              23 ); //The size of serializing 7 tags (3 primary, 3 remote, 1 log router) + 2 for the tag length

	init( TRANSACTION_SIZE_LIMIT,                  1e7 );
	init( KEY_SIZE_LIMIT,                          1e4 );
	init( SYSTEM_KEY_SIZE_LIMIT,                   3e4 );
	init( VALUE_SIZE_LIMIT,                        1e5 );
	init( SPLIT_KEY_SIZE_LIMIT,                    KEY_SIZE_LIMIT/2 ); if( randomize && BUGGIFY ) SPLIT_KEY_SIZE_LIMIT = KEY_SIZE_LIMIT - 31;//serverKeysPrefixFor(UID()).size() - 1;
	init( METADATA_VERSION_CACHE_SIZE,            1000 );

	init( MAX_BATCH_SIZE,                         1000 ); if( randomize && BUGGIFY ) MAX_BATCH_SIZE = 1;
	init( GRV_BATCH_TIMEOUT,                     0.005 ); if( randomize && BUGGIFY ) GRV_BATCH_TIMEOUT = 0.1;
	init( BROADCAST_BATCH_SIZE,                     20 ); if( randomize && BUGGIFY ) BROADCAST_BATCH_SIZE = 1;
	init( TRANSACTION_TIMEOUT_DELAY_INTERVAL,     10.0 ); if( randomize && BUGGIFY ) TRANSACTION_TIMEOUT_DELAY_INTERVAL = 1.0;

	init( LOCATION_CACHE_EVICTION_SIZE,         600000 );
	init( LOCATION_CACHE_EVICTION_SIZE_SIM,         10 ); if( randomize && BUGGIFY ) LOCATION_CACHE_EVICTION_SIZE_SIM = 3;

	init( GET_RANGE_SHARD_LIMIT,                     2 );
	init( WARM_RANGE_SHARD_LIMIT,                  100 );
	init( STORAGE_METRICS_SHARD_LIMIT,             100 ); if( randomize && BUGGIFY ) STORAGE_METRICS_SHARD_LIMIT = 3;
	init( SHARD_COUNT_LIMIT,                        80 ); if( randomize && BUGGIFY ) SHARD_COUNT_LIMIT = 3;
	init( STORAGE_METRICS_UNFAIR_SPLIT_LIMIT,  2.0/3.0 );
	init( STORAGE_METRICS_TOO_MANY_SHARDS_DELAY,  15.0 );
	init( AGGREGATE_HEALTH_METRICS_MAX_STALENESS,  0.5 );
	init( DETAILED_HEALTH_METRICS_MAX_STALENESS,   5.0 );
	init( TAG_ENCODE_KEY_SERVERS,                false ); if( randomize && BUGGIFY ) TAG_ENCODE_KEY_SERVERS = true;

	//KeyRangeMap
	init( KRM_GET_RANGE_LIMIT,                     1e5 ); if( randomize && BUGGIFY ) KRM_GET_RANGE_LIMIT = 10;
	init( KRM_GET_RANGE_LIMIT_BYTES,               1e8 ); if( randomize && BUGGIFY ) KRM_GET_RANGE_LIMIT_BYTES = 10000; //This must be sufficiently larger than KEY_SIZE_LIMIT to ensure that at least two entries will be returned from an attempt to read a key range map

	init( DEFAULT_MAX_OUTSTANDING_WATCHES,         1e4 );
	init( ABSOLUTE_MAX_WATCHES,                    1e6 );
	init( WATCH_POLLING_TIME,                      1.0 ); if( randomize && BUGGIFY ) WATCH_POLLING_TIME = 5.0;
	init( NO_RECENT_UPDATES_DURATION,             20.0 ); if( randomize && BUGGIFY ) NO_RECENT_UPDATES_DURATION = 0.1;
	init( FAST_WATCH_TIMEOUT,                     20.0 ); if( randomize && BUGGIFY ) FAST_WATCH_TIMEOUT = 1.0;
	init( WATCH_TIMEOUT,                         900.0 ); if( randomize && BUGGIFY ) WATCH_TIMEOUT = 20.0;

	// Core
	init( CORE_VERSIONSPERSECOND,		           1e6 );
	init( LOG_RANGE_BLOCK_SIZE,                    1e6 ); //Dependent on CORE_VERSIONSPERSECOND
	init( MUTATION_BLOCK_SIZE,	            	  10000 );

	// TaskBucket
	init( TASKBUCKET_LOGGING_DELAY,                5.0 );
	init( TASKBUCKET_MAX_PRIORITY,                   1 );
	init( TASKBUCKET_CHECK_TIMEOUT_CHANCE,        0.02 ); if( randomize && BUGGIFY ) TASKBUCKET_CHECK_TIMEOUT_CHANCE = 1.0;
	init( TASKBUCKET_TIMEOUT_JITTER_OFFSET,        0.9 );
	init( TASKBUCKET_TIMEOUT_JITTER_RANGE,         0.2 );
	init( TASKBUCKET_CHECK_ACTIVE_DELAY,           0.5 );
	init( TASKBUCKET_CHECK_ACTIVE_AMOUNT,           10 );
	init( TASKBUCKET_TIMEOUT_VERSIONS,     60*CORE_VERSIONSPERSECOND ); if( randomize && BUGGIFY ) TASKBUCKET_TIMEOUT_VERSIONS = 30*CORE_VERSIONSPERSECOND;
	init( TASKBUCKET_MAX_TASK_KEYS,               1000 ); if( randomize && BUGGIFY ) TASKBUCKET_MAX_TASK_KEYS = 20;

	//Backup
	init( BACKUP_CONCURRENT_DELETES,               100 );
	init( BACKUP_SIMULATED_LIMIT_BYTES,		       1e6 ); if( randomize && BUGGIFY ) BACKUP_SIMULATED_LIMIT_BYTES = 1000;
	init( BACKUP_GET_RANGE_LIMIT_BYTES,		       1e6 );
	init( BACKUP_LOCK_BYTES,                       1e8 );
	init( BACKUP_RANGE_TIMEOUT,   TASKBUCKET_TIMEOUT_VERSIONS/CORE_VERSIONSPERSECOND/2.0 );
	init( BACKUP_RANGE_MINWAIT,   std::max(1.0, BACKUP_RANGE_TIMEOUT/2.0));
	init( BACKUP_SNAPSHOT_DISPATCH_INTERVAL_SEC,  10 * 60 );  // 10 minutes
	init( BACKUP_DEFAULT_SNAPSHOT_INTERVAL_SEC,   3600 * 24 * 10); // 10 days
	init( BACKUP_SHARD_TASK_LIMIT,                1000 ); if( randomize && BUGGIFY ) BACKUP_SHARD_TASK_LIMIT = 4;
	init( BACKUP_AGGREGATE_POLL_RATE_UPDATE_INTERVAL, 60);
	init( BACKUP_AGGREGATE_POLL_RATE,              2.0 ); // polls per second target for all agents on the cluster
	init( BACKUP_LOG_WRITE_BATCH_MAX_SIZE,         1e6 ); //Must be much smaller than TRANSACTION_SIZE_LIMIT
	init( BACKUP_LOG_ATOMIC_OPS_SIZE,			  1000 );
	init( BACKUP_OPERATION_COST_OVERHEAD,		    50 );
	init( BACKUP_MAX_LOG_RANGES,                    21 ); if( randomize && BUGGIFY ) BACKUP_MAX_LOG_RANGES = 4;
	init( BACKUP_SIM_COPY_LOG_RANGES,              100 );
	init( BACKUP_VERSION_DELAY,           5*CORE_VERSIONSPERSECOND );
	bool buggifyMapLimits = randomize && BUGGIFY;
	init( BACKUP_MAP_KEY_LOWER_LIMIT,              1e4 ); if( buggifyMapLimits ) BACKUP_MAP_KEY_LOWER_LIMIT = 4;
	init( BACKUP_MAP_KEY_UPPER_LIMIT,              1e5 ); if( buggifyMapLimits ) BACKUP_MAP_KEY_UPPER_LIMIT = 30;
	init( BACKUP_COPY_TASKS,                        90 );
	init( BACKUP_BLOCK_SIZE,   LOG_RANGE_BLOCK_SIZE/10 );
	init( COPY_LOG_BLOCK_SIZE,              LOG_RANGE_BLOCK_SIZE ); // the maximum possible value due the getLogRanges limitations
	init( COPY_LOG_BLOCKS_PER_TASK,               1000 );
	init( COPY_LOG_PREFETCH_BLOCKS,                  3 );
	init( COPY_LOG_READ_AHEAD_BYTES,        BACKUP_LOCK_BYTES / COPY_LOG_PREFETCH_BLOCKS); // each task will use up to COPY_LOG_PREFETCH_BLOCKS * COPY_LOG_READ_AHEAD_BYTES memory
	init( COPY_LOG_TASK_DURATION_NANOS,	      1e10 ); // 10 seconds
	init( BACKUP_TASKS_PER_AGENT,                   10 );
	init( BACKUP_POLL_PROGRESS_SECONDS,             10 );
	init( VERSIONS_PER_SECOND,                     1e6 ); // Must be the same as SERVER_KNOBS->VERSIONS_PER_SECOND
	init( SIM_BACKUP_TASKS_PER_AGENT,               10 );
	init( BACKUP_RANGEFILE_BLOCK_SIZE,      1024 * 1024);
	init( BACKUP_LOGFILE_BLOCK_SIZE,        1024 * 1024);
	init( BACKUP_DISPATCH_ADDTASK_SIZE,             50 );
	init( RESTORE_DISPATCH_ADDTASK_SIZE,           150 );
	init( RESTORE_DISPATCH_BATCH_SIZE,           30000 ); if( randomize && BUGGIFY ) RESTORE_DISPATCH_BATCH_SIZE = 20;
	init( RESTORE_WRITE_TX_SIZE,            256 * 1024 );
	init( APPLY_MAX_LOCK_BYTES,                    1e9 );
	init( APPLY_MIN_LOCK_BYTES,                   11e6 ); //Must be bigger than TRANSACTION_SIZE_LIMIT
	init( APPLY_BLOCK_SIZE,     LOG_RANGE_BLOCK_SIZE/5 );
	init( APPLY_MAX_DECAY_RATE,                   0.99 );
	init( APPLY_MAX_INCREASE_FACTOR,               1.1 );
	init( BACKUP_ERROR_DELAY,                     10.0 );
	init( BACKUP_STATUS_DELAY,                    40.0 );
	init( BACKUP_STATUS_JITTER,                   0.05 );
	init( MIN_CLEANUP_SECONDS,                  3600.0 );

	// Configuration
	init( DEFAULT_AUTO_PROXIES,                      3 );
	init( DEFAULT_AUTO_RESOLVERS,                    1 );
	init( DEFAULT_AUTO_LOGS,                         3 );

	init( IS_ACCEPTABLE_DELAY,                     1.5 );

	init( HTTP_READ_SIZE,                     128*1024 );
	init( HTTP_SEND_SIZE,                      32*1024 );
	init( HTTP_VERBOSE_LEVEL,                        0 );
	init( HTTP_REQUEST_ID_HEADER,                   "" );
	init( BLOBSTORE_CONNECT_TRIES,                  10 );
	init( BLOBSTORE_CONNECT_TIMEOUT,                10 );
	init( BLOBSTORE_MAX_CONNECTION_LIFE,           120 );
	init( BLOBSTORE_REQUEST_TRIES,                  10 );
	init( BLOBSTORE_REQUEST_TIMEOUT,                60 );

	init( BLOBSTORE_CONCURRENT_UPLOADS, BACKUP_TASKS_PER_AGENT*2 );
	init( BLOBSTORE_CONCURRENT_LISTS,               20 );
	init( BLOBSTORE_CONCURRENT_REQUESTS, BLOBSTORE_CONCURRENT_UPLOADS + BLOBSTORE_CONCURRENT_LISTS + 5);

	init( BLOBSTORE_CONCURRENT_WRITES_PER_FILE,      5 );
	init( BLOBSTORE_CONCURRENT_READS_PER_FILE,       3 );
	init( BLOBSTORE_READ_BLOCK_SIZE,       1024 * 1024 );
	init( BLOBSTORE_READ_AHEAD_BLOCKS,               0 );
	init( BLOBSTORE_READ_CACHE_BLOCKS_PER_FILE,      2 );
	init( BLOBSTORE_MULTIPART_MAX_PART_SIZE,  20000000 );
	init( BLOBSTORE_MULTIPART_MIN_PART_SIZE,   5242880 );

	// These are basically unlimited by default but can be used to reduce blob IO if needed
	init( BLOBSTORE_REQUESTS_PER_SECOND,            200 );
	init( BLOBSTORE_MAX_SEND_BYTES_PER_SECOND,      1e9 );
	init( BLOBSTORE_MAX_RECV_BYTES_PER_SECOND,      1e9 );

	init( BLOBSTORE_LIST_REQUESTS_PER_SECOND,       200 );
	init( BLOBSTORE_WRITE_REQUESTS_PER_SECOND,       50 );
	init( BLOBSTORE_READ_REQUESTS_PER_SECOND,       100 );
	init( BLOBSTORE_DELETE_REQUESTS_PER_SECOND,     200 );

	// Client Status Info
	init(CSI_SAMPLING_PROBABILITY, -1.0);
	init(CSI_SIZE_LIMIT, std::numeric_limits<int64_t>::max());
	if (randomize && BUGGIFY) {
		CSI_SAMPLING_PROBABILITY = deterministicRandom()->random01() / 10; // rand range 0 - 0.1
		CSI_SIZE_LIMIT = deterministicRandom()->randomInt(1024 * 1024, 100 * 1024 * 1024); // 1 MB - 100 MB
	}
	init(CSI_STATUS_DELAY,						  10.0  );

	init( CONSISTENCY_CHECK_RATE_LIMIT_MAX,        50e6 ); // Limit in per sec
	init( CONSISTENCY_CHECK_ONE_ROUND_TARGET_COMPLETION_TIME,	7 * 24 * 60 * 60 ); // 7 days
	
	//fdbcli		
	init( CLI_CONNECT_PARALLELISM,                  400 );
	init( CLI_CONNECT_TIMEOUT,                     10.0 );

	// trace
	init( TRACE_LOG_FILE_IDENTIFIER_MAX_LENGTH,      50 );

	// transaction tags
	init( MAX_TAGS_PER_TRANSACTION,                   5 );
	init( MAX_TRANSACTION_TAG_LENGTH,                16 );
	init( READ_TAG_SAMPLE_RATE,                    0.01 ); if( randomize && BUGGIFY ) READ_TAG_SAMPLE_RATE = 1.0; // Communicated to clients from cluster
	init( TAG_THROTTLE_SMOOTHING_WINDOW,            2.0 );
	init( TAG_THROTTLE_RECHECK_INTERVAL,            5.0 ); if( randomize && BUGGIFY ) TAG_THROTTLE_RECHECK_INTERVAL = 0.0;
	init( TAG_THROTTLE_EXPIRATION_INTERVAL,        60.0 ); if( randomize && BUGGIFY ) TAG_THROTTLE_EXPIRATION_INTERVAL = 1.0;

	// clang-format on
}

TEST_CASE("/fdbclient/knobs/initialize") {
	// This test depends on TASKBUCKET_TIMEOUT_VERSIONS being defined as a constant multiple of CORE_VERSIONSPERSECOND
	ClientKnobs clientKnobs;
	int initialCoreVersionsPerSecond = clientKnobs.CORE_VERSIONSPERSECOND;
	int initialTaskBucketTimeoutVersions = clientKnobs.TASKBUCKET_TIMEOUT_VERSIONS;
	clientKnobs.setKnob("core_versionspersecond", format("%ld", initialCoreVersionsPerSecond * 2));
	ASSERT(clientKnobs.CORE_VERSIONSPERSECOND == initialCoreVersionsPerSecond * 2);
	ASSERT(clientKnobs.TASKBUCKET_TIMEOUT_VERSIONS == initialTaskBucketTimeoutVersions);
	clientKnobs.initialize();
	ASSERT(clientKnobs.CORE_VERSIONSPERSECOND == initialCoreVersionsPerSecond * 2);
	ASSERT(clientKnobs.TASKBUCKET_TIMEOUT_VERSIONS == initialTaskBucketTimeoutVersions * 2);
	return Void();
}
