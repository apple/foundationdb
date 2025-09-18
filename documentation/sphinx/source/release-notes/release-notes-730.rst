#############
Release Notes
#############

7.3.71
======
* Same as 7.3.71 release with AVX enabled.

7.3.70
======
* Fixed handleTssMismatches crashes. `(PR #12377) <https://github.com/apple/foundationdb/pull/12377>`_
* Improved TLS handshake mechanism. `(PR #12348) <https://github.com/apple/foundationdb/pull/12348>`_
* Fixed restarting test failures. `(PR #12324) <https://github.com/apple/foundationdb/pull/12324>`_
* Suppressed PingLatency events. `(PR #12316) <https://github.com/apple/foundationdb/pull/12316>`_
* Fixed "Unknown error" when configuring regions. `(PR #12313) <https://github.com/apple/foundationdb/pull/12313>`_
* Replaced '...' with actual random data in simulation workloads. `(PR #12273) <https://github.com/apple/foundationdb/pull/12273>`_
* Added TLS performance counters. `(PR #12278) <https://github.com/apple/foundationdb/pull/12278>`_ and `(PR #12296) <https://github.com/apple/foundationdb/pull/12296>`_

7.3.69
======
* Same as 7.3.68 release with AVX enabled.

7.3.68
======
* Fixed a rare race that can cause recovery to be stuck. `(PR #12213) <https://github.com/apple/foundationdb/pull/12213>`_
* Backported various go binding and documentation changes. `(PR #12146) <https://github.com/apple/foundationdb/pull/12146>`_

7.3.67
======
* Same as 7.3.66 release with AVX enabled.

7.3.66
======
* Fixed a restore speed regression. `(PR #12088) <https://github.com/apple/foundationdb/pull/12088>`_
* Added per DC check for fdbcli's checkall command. `(PR #12089) <https://github.com/apple/foundationdb/pull/12089>`_
* Added periodical flushes of old column families for Sharded RocksDB engine. `(PR #12123) <https://github.com/apple/foundationdb/pull/12123>`_

7.3.65
======
* Same as 7.3.64 release with AVX enabled.

7.3.64
======
* Fixed fdb-kubernetes-monitor to only log an error when there is one. `(PR #12112) <https://github.com/apple/foundationdb/pull/12112>`_
* Fixed distributed conconsistency checker tester to always accept new requests from ConsistencyCheckUrgent role. `(PR #12001) <https://github.com/apple/foundationdb/pull/12001>`_
* Added RocksDB compaction knobs and more metrics. `(PR #12016) <https://github.com/apple/foundationdb/pull/12016>`_, `(PR #11993) <https://github.com/apple/foundationdb/pull/11993>`_, `(PR #12054) <https://github.com/apple/foundationdb/pull/12054>`_, and `(PR #12055) <https://github.com/apple/foundationdb/pull/12055>`_
* Fixed a rare restore bug due to a race condition. `(PR #12041) <https://github.com/apple/foundationdb/pull/12041>`_
* Fixed backup reporting on snapshot restorable state and continuous log end version. `(PR #12038) <https://github.com/apple/foundationdb/pull/12038>`_ and `(PR #12069) <https://github.com/apple/foundationdb/pull/12069>`_

7.3.63
======
* Same as 7.3.62 release with AVX enabled.

7.3.62
======
* Reverted backup agents dry run request changes since 7.3.49 release. `(PR #11965) <https://github.com/apple/foundationdb/pull/11965>`_

7.3.61
======
* Same as 7.3.60 release with AVX enabled.

7.3.60
======
* Fixed a potential corruption of the destination storage server when shard_encode_location_metadata is enabled. `(PR #11934) <https://github.com/apple/foundationdb/pull/11934>`_
* Paused storage wiggle when all storage servers do not have enough available space. `(PR #11911) <https://github.com/apple/foundationdb/pull/11911>`_
* Updated Sharded RocksDB knobs. `(PR #11936) <https://github.com/apple/foundationdb/pull/11936>`_
* Fixed storage migration to consider the perpetualStorageEngine setting. `(PR #11940) <https://github.com/apple/foundationdb/pull/11940>`_
* Improved the coverage of location metadata auditing. `(PR #11897) <https://github.com/apple/foundationdb/pull/11897>`_

7.3.59
======
* Same as 7.3.58 release with AVX enabled.

7.3.58
======
* Updated fdbmonitor to only delay the restart of fdbserver if the process exited with an exit code other than 0. `(PR #11812) <https://github.com/apple/foundationdb/pull/11812>`_
* Added knob to pause perpetual storage wiggle when TSS count target is met. `(PR #11824) <https://github.com/apple/foundationdb/pull/11824>`_
* Updated exclusion code to reduce the requests that are made by locality-based exclusions. `(PR #11848) <https://github.com/apple/foundationdb/pull/11848>`_
* Added multiple improvements to reduce the false positive rate of gray failure triggered recoveries. `(PR #11850) <https://github.com/apple/foundationdb/pull/11850>`_, `(PR #11852) <https://github.com/apple/foundationdb/pull/11852>`_, `(PR #11885) <https://github.com/apple/foundationdb/pull/11885>`_
* Added a knob to use direct IO for ShardedRocks storage engine. `(PR #11891) <https://github.com/apple/foundationdb/pull/11891>`_
* Added a knob to compact based on number of range deletions in file for ShardedRocks storage engine. `(PR #11890) <https://github.com/apple/foundationdb/pull/11890>`_
* Updated RocksDB force flush interval based on the last flush time. `(PR #11845) <https://github.com/apple/foundationdb/pull/11845>`_

7.3.57
======
* Same as 7.3.56 release with AVX enabled.

7.3.56
======
* Added an option to force flush if RocksCb flush does not happen within a time interval. `(PR #11792) <https://github.com/apple/foundationdb/pull/11792>`_
* Changed RocksDB histogram sample rate to 1. `(PR #11794) <https://github.com/apple/foundationdb/pull/11794>`_
* Updated TLS input handling to allow multiple Criteria per NID. `(PR #11763) <https://github.com/apple/foundationdb/pull/11763>`_
* Added Sharded RocksDB knobs and metrics for bloom filters. `(PR #11785) <https://github.com/apple/foundationdb/pull/11785>`_
* Fixed a backup agent crash bug when receiving an invalid token error from S3. `(PR #11774) <https://github.com/apple/foundationdb/pull/11774>`_
* Updated ROCKSDB_MEMTABLE_MAX_RANGE_DELETIONS Knob to prevent OOMs of RocksDB storage servers `(PR #11739) <https://github.com/apple/foundationdb/pull/11739>`_

7.3.55
======
* Same as 7.3.54 release with AVX enabled.

7.3.54
======
* Addressed urgent consistency checker related issues. `(PR #11736) <https://github.com/apple/foundationdb/pull/11736>`_
* Addressed a downgrade related incompatibility issue between 7.3 patch releases. `(PR #11732) <https://github.com/apple/foundationdb/pull/11732>`_
* Added knob LOG_CONNECTION_ATTEMPTS_ENABLED to log all incoming connections. `(PR #11713) <https://github.com/apple/foundationdb/pull/11713>`_

7.3.53
======
* Same as 7.3.52 release with AVX enabled.

7.3.52
======
* Improved Sharded Rocksdb to use a single iterator pool for all physical shards. `(PR #11694) <https://github.com/apple/foundationdb/pull/11694>`_
* Changed the default values of various Sharded Rocksdb related knobs. `(PR #11706) <https://github.com/apple/foundationdb/pull/11706>`_
* Removed CC_PAUSE_HEALTH_MONITOR knob. `(PR #11701) <https://github.com/apple/foundationdb/pull/11701>`_
* Addressed protocol incompatibility issues between 7.3 patch releases. `(PR #11697) <https://github.com/apple/foundationdb/pull/11697>`_, `(PR #11705) <https://github.com/apple/foundationdb/pull/11705>`_

7.3.51
======
* Same as 7.3.50 release with AVX enabled.

7.3.50
======
* Converted an assertion for Sharded Rocksdb in storage server to error logs. `(PR #11622) <https://github.com/apple/foundationdb/pull/11622>`_
* Fixed backup agents from retrying uploading in the presence of S3 token errors. `(PR #11602) <https://github.com/apple/foundationdb/pull/11602>`_
* Fixed an inconsistent location metadata issue and invalid shard IDs generated by seed shard servers when SHARD_ENCODE_LOCATION_METADATA is enabled. `(PR #11640) <https://github.com/apple/foundationdb/pull/11640>`_ and `(PR #11647) <https://github.com/apple/foundationdb/pull/11647>`_
* Enabled direct_io for RocksDB storage engine and wiggle knobs. `(PR #11636) <https://github.com/apple/foundationdb/pull/11636>`_
* Added knobs for caching index blocks for Sharded RocksDB. `(PR #11649) <https://github.com/apple/foundationdb/pull/11649>`_ and `(PR #11680) <https://github.com/apple/foundationdb/pull/11680>`_
* Added various data distributor knobs for performance tuning. `(PR #11668) <https://github.com/apple/foundationdb/pull/11668>`_ and `(PR #11665) <https://github.com/apple/foundationdb/pull/11665>`_
* Increased the minimum age to wiggle to avoid re-wiggling migrated storage servers. `(PR #11684) <https://github.com/apple/foundationdb/pull/11684>`_
* Added various version vector improvements. `(PR #11590) <https://github.com/apple/foundationdb/pull/11590>`_, `(PR #11599) <https://github.com/apple/foundationdb/pull/11599>`_, `(PR #11605) <https://github.com/apple/foundationdb/pull/11605>`_, `(PR #11608) <https://github.com/apple/foundationdb/pull/11608>`_, `(PR #11620) <https://github.com/apple/foundationdb/pull/11620>`_, `(PR #11626) <https://github.com/apple/foundationdb/pull/11626>`_ and `(PR #11631) <https://github.com/apple/foundationdb/pull/11631>`_
* Removed upgrade tests before FDB 6.3. `(PR #11662) <https://github.com/apple/foundationdb/pull/11662>`_

7.3.49
======
* Same as 7.3.48 release with AVX enabled.

7.3.48
======
* Added various fdb-kubernetes-monitor improvements. `(PR #11456) <https://github.com/apple/foundationdb/pull/11456>`_, `(PR #11462) <https://github.com/apple/foundationdb/pull/11462>`_, and `(PR #11488) <https://github.com/apple/foundationdb/pull/11488>`_
* Added more debug information for the fdbcli's checkall command. `(PR #11477) <https://github.com/apple/foundationdb/pull/11477>`_
* Improved distributed consistency checker and updated the documentation. `(PR #11496) <https://github.com/apple/foundationdb/pull/11496>`_
* Fixed a known committed version computation when version vector unicast is enabled. `(PR #11520) <https://github.com/apple/foundationdb/pull/11520>`_
* Added PeerAddress to all PeerAddr/Peer TraceEvents. `(PR #11521) <https://github.com/apple/foundationdb/pull/11521>`_
* Added a dynamic knob CC_PAUSE_HEALTH_MONITOR to disable gray failure recoveries. `(PR #11526) <https://github.com/apple/foundationdb/pull/11526>`_
* Changed the default base Docker image to RockyLinux9. `(PR #11549) <https://github.com/apple/foundationdb/pull/11549>`_
* Fixed a storage server crash when there are conflicting physical shard moves. `(PR #11485) <https://github.com/apple/foundationdb/pull/11485>`_
* Fixed a rare cluster controller crash when serving status requests. `(PR #11580) <https://github.com/apple/foundationdb/pull/11580>`_

7.3.47
======
* Same as 7.3.46 release with AVX enabled.

7.3.46
======
* Added exponential backoff when restarting fdbserver processes in fdb-kubernetes-monitor. `(PR #11453) <https://github.com/apple/foundationdb/pull/11453>`_

7.3.45
======
* Same as 7.3.44 release with AVX enabled.

7.3.44
======
* Fixed negative free disk space in RkUpdate metrics. `(PR #11413) <https://github.com/apple/foundationdb/pull/11413>`_
* Corrected the path where the fdb-kubernetes-monitor copies the binary into when running in sidecar mode. `(PR #11439) <https://github.com/apple/foundationdb/pull/11439>`_

7.3.43
======
* Same as 7.3.42 release with AVX enabled.

7.3.42
======
* Fixed a segfault when tlog encounters a platform_error. `(PR #11406) <https://github.com/apple/foundationdb/pull/11406>`_
* Fixed an assertion failure of cluster controller when waiting for recovery. `(PR #11402) <https://github.com/apple/foundationdb/pull/11402>`_
* Fixed a global config bug that causes client latency metrics to be missing after upgrade. `(PR #11400) <https://github.com/apple/foundationdb/pull/11400>`_
* Added a knob for shard merge parallelism and added logs for max shard size. `(PR #11389) <https://github.com/apple/foundationdb/pull/11389>`_
* Updated the RocksDB memtable max range deletions knob. `(PR #11387) <https://github.com/apple/foundationdb/pull/11387>`_
* Improved visibility when Sharded RocksDB takes a long time to commit. `(PR #11362) <https://github.com/apple/foundationdb/pull/11362>`_
* Fixed data move trigger for rebalancing storage queue. `(PR #11375) <https://github.com/apple/foundationdb/pull/11375>`_
* Fixed an infinite retry of GRV request bug. `(PR #11353) <https://github.com/apple/foundationdb/pull/11353>`_
* Improved distributed consistency checker to continuously run by default and visibility of recruitment errors. `(PR #11349) <https://github.com/apple/foundationdb/pull/11349>`_
* Fixed add-prefix and remove-prefix for fdbrestore. `(PR #11344) <https://github.com/apple/foundationdb/pull/11344>`_
* Fixed a crash of data distributor when taking a snapshot. `(PR #11341) <https://github.com/apple/foundationdb/pull/11341>`_
* Increased visibility of gray failure actions. `(PR #11324) <https://github.com/apple/foundationdb/pull/11324>`_
* Increased visibility of CommitProxyTerminated events for failed_to_progress errors. `(PR #11315) <https://github.com/apple/foundationdb/pull/11315>`_

7.3.41
======
* Same as 7.3.40 release with AVX enabled.

7.3.40
======
* Upgraded RocksDB to version 8.11.4. `(PR #11327) <https://github.com/apple/foundationdb/pull/11327>`_

7.3.39
======
* Same as 7.3.38 release with AVX enabled.

7.3.38
======
* Fixed the detection of private mutations in version vector. `(PR #11279) <https://github.com/apple/foundationdb/pull/11279>`_
* Added accumulative checksum feature. `(PR #11281) <https://github.com/apple/foundationdb/pull/11281>`_ and `(PR #11289) <https://github.com/apple/foundationdb/pull/11289>`_
* Added Go tenanting support. `(PR #11299) <https://github.com/apple/foundationdb/pull/11299>`_
* Added RocksDB caching knobs. `(PR #11312) <https://github.com/apple/foundationdb/pull/11312>`_
* Added RocksDB metrics in status json. `(PR #11320) <https://github.com/apple/foundationdb/pull/11320>`_
* Various Sharded RocksDB improvements. `(PR #11332) <https://github.com/apple/foundationdb/pull/11332>`_


7.3.37
======
* Same as 7.3.36 release with AVX enabled.

7.3.36
======
* Fixed a DR corruption issue where destination cluster gets no mutations. `(PR #11246) <https://github.com/apple/foundationdb/pull/11246>`_
* Added rocksdb direct_io knobs. `(PR #11267) <https://github.com/apple/foundationdb/pull/11267>`_

7.3.35
======
* Same as 7.3.34 release with AVX enabled.

7.3.34
======
* Added storage-queue-aware load balancer for data distributor. `(PR #11195) <https://github.com/apple/foundationdb/pull/11195>`_
* Added a checksum field in MutationRef. `(PR #11193) <https://github.com/apple/foundationdb/pull/11193>`_
* Abort processes when abnormal shutdown is initiated to enable coredumps. `(PR #11198) <https://github.com/apple/foundationdb/pull/11198>`_
* Fixed fdbcli's checkall debug command. `(PR #11208) <https://github.com/apple/foundationdb/pull/11208>`_
* Added knobs for enabling RocksDB in-memory checksums for data structures. `(PR #11214) <https://github.com/apple/foundationdb/pull/11214>`_
* Fixed calculation of EmptyMessageRatio when version vector was enabled. `(PR #11227) <https://github.com/apple/foundationdb/pull/11227>`_
* Added consistency checker urgent mode. `(PR #11228) <https://github.com/apple/foundationdb/pull/11228>`_
* Disabled compaction compaction for newly added shard and fixed block cache usage reporting. `(PR #11247) <https://github.com/apple/foundationdb/pull/11247>`_
* Fixed setting perpetual_storage_wiggle_engine is considered as wrongly configured. `(PR #11252) <https://github.com/apple/foundationdb/pull/11252>`_
* Added a max range deletions knob before flush. `(PR #11243) <https://github.com/apple/foundationdb/pull/11243>`_

7.3.33
======
* Same as 7.3.32 release with AVX enabled.

7.3.32
======
* Enabled data distributor verbose tracing by default. `(PR #11159) <https://github.com/apple/foundationdb/pull/11159>`_
* Added RocksDB file checksum knobs. `(PR #11171) <https://github.com/apple/foundationdb/pull/11171>`_
* Fixed a regression that caused rebalance data moves to be scheduled at a much lower frequency. `(PR #11167) <https://github.com/apple/foundationdb/pull/11167>`_
* Added throttling of RocksDB flushes when memtable layers exceed a limit. `(PR #11182) <https://github.com/apple/foundationdb/pull/11182>`_
* Added a trace event when a log router cannot find its primary peek location. `(PR #11180) <https://github.com/apple/foundationdb/pull/11180>`_
* Upgraded RocksDB version to 8.10.0. `(PR #11175) <https://github.com/apple/foundationdb/pull/11175>`_
* Added periodical logging for RocksDB compaction reasons. `(PR #11186) <https://github.com/apple/foundationdb/pull/11186>`_

7.3.31
======
* Same as 7.3.30 release with AVX enabled.

7.3.30
======
* Fixed an issue in Ratekeeper that could cause StorageQueueInfo loss. `(PR #11124) <https://github.com/apple/foundationdb/pull/11124>`_
* Fixed checkall command for large shards. `(PR #11121) <https://github.com/apple/foundationdb/pull/11121>`_

7.3.29
======
* Same as 7.3.28 release with AVX enabled.

7.3.28
======
* Fixed a race condition in kvstorerockddb when accessing latencySample. `(PR #11114) <https://github.com/apple/foundationdb/pull/11114>`_
* Added support for physical shard move. `(PR #11086) <https://github.com/apple/foundationdb/pull/11086>`_
* Disabled CPU based team selection in rebalance data move. `(PR #11110) <https://github.com/apple/foundationdb/pull/11110>`_

7.3.27
======
* Same as 7.3.26 release with AVX enabled.

7.3.26
======
* Updated RocskDB version to 8.6.7. `(PR #11043) <https://github.com/apple/foundationdb/pull/11043>`_
* Changed RocksDB rate limiter to all IO. `(PR #11016) <https://github.com/apple/foundationdb/pull/11016>`_
* Added ``fdb_c_apiversion.g.h`` to OSX package. `(PR #11042) <https://github.com/apple/foundationdb/pull/11042>`_
* Added write traffic metrics to ddMetricsGetRange. `(PR #10998) <https://github.com/apple/foundationdb/pull/10998>`_
* Fixed several locality-based exclusion bugs. `(PR #11024) <https://github.com/apple/foundationdb/pull/11024>`_, `(PR #11007) <https://github.com/apple/foundationdb/pull/11007>`_, and `(PR #11005) <https://github.com/apple/foundationdb/pull/11005>`_
* Fixed the null pointer issue in proxy setup. `(PR #11039) <https://github.com/apple/foundationdb/pull/11039>`_

7.3.25
======
* Same as 7.3.24 release with AVX enabled.

7.3.24
======
* Released with AVX disabled.
* Added support for large shard. `(PR #10965) <https://github.com/apple/foundationdb/pull/10965>`_
* Fixed perpetual wiggle locality match regex. `(PR #10973) <https://github.com/apple/foundationdb/pull/10973>`_
* Added a knob to throttle perpetual wiggle data move. `(PR #10957) <https://github.com/apple/foundationdb/pull/10957>`_

7.3.23
======
* Same as 7.3.22 release with AVX enabled.

7.3.22
======
* No code change, only version bumped.

7.3.21
======
* Same as 7.3.20 release with AVX enabled.

7.3.20
======
* Added data move throttling for perpetual wiggle. `(PR #10957) <https://github.com/apple/foundationdb/pull/10957>`_
* Fixed AuditStorage to check all DC replicas. `(PR #10966) <https://github.com/apple/foundationdb/pull/10966>`_
* Added large shards support. `(PR #10965) <https://github.com/apple/foundationdb/pull/10965>`_
* Fixed bugs for locality-based exclusion. `(PR #10946) <https://github.com/apple/foundationdb/pull/10946>`_
* Fixed various memory-related bugs. `(PR #10952) <https://github.com/apple/foundationdb/pull/10952>`_ and `(PR #10969) <https://github.com/apple/foundationdb/pull/10969>`_
* Fixed perpetual wiggling locality match regex. `(PR #10972) <https://github.com/apple/foundationdb/pull/10972>`_

7.3.19
======
* Same as 7.3.18 release with AVX enabled.

7.3.18
======
* Released with AVX disabled.
* Changed Event to use std::latch from c++20. `(PR #10929) <https://github.com/apple/foundationdb/pull/10929>`_
* Added support for preinstalled libfmt. `(PR #10929) <https://github.com/apple/foundationdb/pull/10929>`_
* Changed perpetual_storage_wiggle_locality database option to take a list of localities. `(PR #10928) <https://github.com/apple/foundationdb/pull/10928>`_
* Fixed the trailing newline in c++filt output for Implib.so. `(PR #10921) <https://github.com/apple/foundationdb/pull/10921>`_
* Stopped tracking a storage server after its removal. `(PR #10921) <https://github.com/apple/foundationdb/pull/10921>`_
* Fixed Ratekeeper for not accounting dropped requests. `(PR #10921) <https://github.com/apple/foundationdb/pull/10921>`_
* Fixed a memory leak of cluster controller's status json invocation. `(PR #10921) <https://github.com/apple/foundationdb/pull/10921>`_
* Fixed cluster controller from issuing many point reads for storage metadata. `(PR #10906) <https://github.com/apple/foundationdb/pull/10906>`_
* Fixed multiple issues with AuditStorage. `(PR #10895) <https://github.com/apple/foundationdb/pull/10895>`_
* Disabled storage server read sampling by default. `(PR #10899) <https://github.com/apple/foundationdb/pull/10899>`_

7.3.17
======
* Same as 7.3.16 release with AVX enabled.

7.3.16
======
* Released with AVX disabled.
* Added location_metadata fdbcli to query shard locations and assignements. `(PR #10428) <https://github.com/apple/foundationdb/pull/10428>`_
* Added degraded/disconnected peer recovery in gray failure. `(PR #10541) <https://github.com/apple/foundationdb/pull/10541>`_
* Added replica and metadata audit support. `(PR #10631) <https://github.com/apple/foundationdb/pull/10631>`_
* Added a SecurityMode for data distributor where data movements are not allowed but auditStorage is enabled. `(PR #10660) <https://github.com/apple/foundationdb/pull/10660>`_
* Remove SS entries from RateKeeper once it is down. `(PR #10681) <https://github.com/apple/foundationdb/pull/10681/files>`_
* Added the support of manual compaction for Sharded RocksDB. `(PR #10815) <https://github.com/apple/foundationdb/pull/10838>`_

7.3.0
=====

Fixes
-----
* Fixed a consistency scan infinite looping without progress bug when a storage server is removed. `(PR #9154) <https://github.com/apple/foundationdb/pull/9154>`_
* Fixed a backup worker assertion failure. `(PR #8886) <https://github.com/apple/foundationdb/pull/8886>`_
* Fixed a DD stuck issue when the remote data center is dead. `(PR #9338) <https://github.com/apple/foundationdb/pull/9338>`_
* Exclude command will not perform a write if the addresses being excluded are already excluded. `(PR #9873) <https://github.com/apple/foundationdb/pull/9873>`_
* ConsistencyCheck should finish after complete scan than failing on first mismatch. `(PR #8539) <https://github.com/apple/foundationdb/pull/8539>`_

Bindings
--------
* Allow Ruby bindings to run on arm64. `(PR #9575) <https://github.com/apple/foundationdb/pull/9575>`_

Performance
-----------
* Improvements on physical shard creation to reduce shard count. `(PR #9067) <https://github.com/apple/foundationdb/pull/9067>`_
* Older TLog generations are garbage collected as soon as they are no longer needed. `(PR #10289) <https://github.com/apple/foundationdb/pull/10289>`_

Reliability
-----------
* Gray failure will monitor satellite TLog disconnections.
* Storage progress is logged during the slow recovery. `(PR #9041) <https://github.com/apple/foundationdb/pull/9041>`_
* Added a new network option fail_incompatible_client. If the option is set, transactions are failing with fail_incompatible_client in case of an attempt to connect to a cluster without providing a compatible client library

Status
------

Other Changes
-------------

*  Added MonotonicTime field, based on system clock, to CommitDebug
   trace events, for accurate timing.

*  Added a new function fdb_database_get_client_status providing a
   client-side connection status information in json format.

*  Added a new network option retain_client_library_copies to avoid
   deleting the temporary library copies after completion of the
   process. This may be useful in various debugging and profiling
   scenarios.

*  Added a new network option trace_initialize_on_setup to enable client
   traces already on fdb_setup_network, so that traces do not get lost
   on client configuration issues

*  TraceEvents related to TLS handshake, new connections, and tenant
   access by authorization token are no longer subject to suppression or
   throttling, using an internal “AuditedEvent” TraceEvent
   classification

*  Usage of authorization token is logged as part of AuditedEvent, with
   5-second suppression time window for duplicate entries (suppression
   time window is controlled by AUDIT_TIME_WINDOW flow knob)

Earlier release notes
---------------------
* :doc:`7.2 (API Version 720) </release-notes/release-notes-720>`
* :doc:`7.1 (API Version 710) </release-notes/release-notes-710>`
* :doc:`7.0 (API Version 700) </release-notes/release-notes-700>`
* :doc:`6.3 (API Version 630) </release-notes/release-notes-630>`
* :doc:`6.2 (API Version 620) </release-notes/release-notes-620>`
* :doc:`6.1 (API Version 610) </release-notes/release-notes-610>`
* :doc:`6.0 (API Version 600) </release-notes/release-notes-600>`
* :doc:`5.2 (API Version 520) </release-notes/release-notes-520>`
* :doc:`5.1 (API Version 510) </release-notes/release-notes-510>`
* :doc:`5.0 (API Version 500) </release-notes/release-notes-500>`
* :doc:`4.6 (API Version 460) </release-notes/release-notes-460>`
* :doc:`4.5 (API Version 450) </release-notes/release-notes-450>`
* :doc:`4.4 (API Version 440) </release-notes/release-notes-440>`
* :doc:`4.3 (API Version 430) </release-notes/release-notes-430>`
* :doc:`4.2 (API Version 420) </release-notes/release-notes-420>`
* :doc:`4.1 (API Version 410) </release-notes/release-notes-410>`
* :doc:`4.0 (API Version 400) </release-notes/release-notes-400>`
* :doc:`3.0 (API Version 300) </release-notes/release-notes-300>`
* :doc:`2.0 (API Version 200) </release-notes/release-notes-200>`
* :doc:`1.0 (API Version 100) </release-notes/release-notes-100>`
* :doc:`Beta 3 (API Version 23) </release-notes/release-notes-023>`
* :doc:`Beta 2 (API Version 22) </release-notes/release-notes-022>`
* :doc:`Beta 1 (API Version 21) </release-notes/release-notes-021>`
* :doc:`Alpha 6 (API Version 16) </release-notes/release-notes-016>`
* :doc:`Alpha 5 (API Version 14) </release-notes/release-notes-014>`
