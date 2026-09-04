.. _release-notes:

#############
Release Notes
#############

7.4.7
=====

AVX enabled release.

* Avoided restarting transaction-system recovery when an old-epoch backup worker fails. `(PR #13939) <https://github.com/apple/foundationdb/pull/13939>`_
* Moved ``waitForShardReady`` outside the transaction used to finish data moves, preventing slow destination servers from exhausting the transaction lifetime and stalling data distribution. `(PR #13642) <https://github.com/apple/foundationdb/pull/13642>`_
* Fixed a use-after-free when cancelling S3 backup file copies. `(PR #13916) <https://github.com/apple/foundationdb/pull/13916>`_
* Fixed server knob overrides being silently skipped when a knob name is shared with client knobs. `(PR #13932) <https://github.com/apple/foundationdb/pull/13932>`_
* Added opt-in eviction of stale client connections to failed storage servers and proxies. `(PR #13912) <https://github.com/apple/foundationdb/pull/13912>`_
* Disabled data distribution global admission control by default. `(PR #13917) <https://github.com/apple/foundationdb/pull/13917>`_
* Fixed crashes and incorrect behavior in snapshot options, cancelled file reads, data distribution shutdown, and TLog minimum popped-version tracking. `(PR #13871) <https://github.com/apple/foundationdb/pull/13871>`_
* Fixed an incorrect shard assignment after an excluded server failed. `(PR #13869) <https://github.com/apple/foundationdb/pull/13869>`_
* Added a retry limit for repeated ``startMoveKeys`` failures to prevent infinite retries. `(PR #13782) <https://github.com/apple/foundationdb/pull/13782>`_
* Fixed exclusion tracking for data distribution. `(PR #13766) <https://github.com/apple/foundationdb/pull/13766>`_
* Fixed read and write overflow handling for snapshot manifests larger than 2 GB. `(PR #13820) <https://github.com/apple/foundationdb/pull/13820>`_
* Added GCM authentication tags to encrypted backup files and fixed encrypted restore block-size initialization. `(PR #13777) <https://github.com/apple/foundationdb/pull/13777>`_
* Dropped stale backup workers during recovery. `(PR #13655) <https://github.com/apple/foundationdb/pull/13655>`_
* Fixed successful automatic-idempotency replays not populating the committed version. `(PR #13294) <https://github.com/apple/foundationdb/pull/13294>`_
* Fixed read-ahead cache reads extending past the end of small or final blocks. `(PR #13292) <https://github.com/apple/foundationdb/pull/13292>`_
* Improved RocksDB memory accounting by charging write buffers and selected memory usage to the block cache. `(PR #13371) <https://github.com/apple/foundationdb/pull/13371>`_, `(PR #13346) <https://github.com/apple/foundationdb/pull/13346>`_
* Improved data distribution relocation backoff and admission control to reduce retry contention and pipeline pressure. `(PR #13145) <https://github.com/apple/foundationdb/pull/13145>`_, `(PR #13280) <https://github.com/apple/foundationdb/pull/13280>`_
* Added mutation-log-type configuration for backups. `(PR #13127) <https://github.com/apple/foundationdb/pull/13127>`_
* Fixed encrypted restore failures, including a ``platform_error`` crash. `(PR #13118) <https://github.com/apple/foundationdb/pull/13118>`_
* Made cluster status requests asynchronous so status retrieval can return before the timeout. `(PR #13099) <https://github.com/apple/foundationdb/pull/13099>`_
* Hardened S3 error handling for non-XML and HTTP 4xx responses. `(PR #12996) <https://github.com/apple/foundationdb/pull/12996>`_, `(PR #13021) <https://github.com/apple/foundationdb/pull/13021>`_
* Fixed ``minRestorableVersion`` and ``maxRestorableVersion`` updates when mutation logs are missing. `(PR #12710) <https://github.com/apple/foundationdb/pull/12710>`_
* Restricted ``backup_worker_enabled`` configuration through ``fdbcli``. `(PR #12711) <https://github.com/apple/foundationdb/pull/12711>`_
* Enabled TLS hostname validation against certificate CN and SAN values. `(PR #12730) <https://github.com/apple/foundationdb/pull/12730>`_

7.4.6
=====

AVX enabled release.

* Fixed a restore stuck bug where readLogData() errors were not propagated correctly. `(PR #12433) <https://github.com/apple/foundationdb/pull/12433>`_
* Fixed FdbDecode memory issues. `(PR #12495) <https://github.com/apple/foundationdb/pull/12495>`_
* Fixed decode range file keys and values interpretation bug. `(PR #12420) <https://github.com/apple/foundationdb/pull/12420>`_
* Fixed the flags used for opening the backup encryption key file. `(PR #12424) <https://github.com/apple/foundationdb/pull/12424>`_
* Fixed backup encryption on S3. `(PR #12289) <https://github.com/apple/foundationdb/pull/12289>`_
* Added support for backup encryption via fdbbackup modify command. `(PR #12591) <https://github.com/apple/foundationdb/pull/12591>`_
* Added support for backup worker to use proxy from command line to upload to S3. `(PR #12566) <https://github.com/apple/foundationdb/pull/12566>`_
* Added metadata for encrypted backups. `(PR #12354) <https://github.com/apple/foundationdb/pull/12354>`_
* Added FileLevelEncryption field to BackupDescription JSON output. `(PR #12626) <https://github.com/apple/foundationdb/pull/12626>`_
* Added encryption key info to status JSON. `(PR #12657) <https://github.com/apple/foundationdb/pull/12657>`_
* Added SNI in TLS handshake. `(PR #12385) <https://github.com/apple/foundationdb/pull/12385>`_
* Added ARM support for Docker images. `(PR #12425) <https://github.com/apple/foundationdb/pull/12425>`_
* Added restore validation feature allowing backup/restore validation in a single cluster. `(PR #12648) <https://github.com/apple/foundationdb/pull/12648>`_
* Downgraded RocksDB to 8.11.5. `(PR #12651) <https://github.com/apple/foundationdb/pull/12651>`_

7.4.5
=====
* Same as 7.4.4 release with AVX enabled.

7.4.4
=====

* Fixed a race that could cause recovery to be stuck when purging old generations. `(PR #12214) <https://github.com/apple/foundationdb/pull/12214>`_
* Fixed TSS mismatch handling crashes. `(PR #12330) <https://github.com/apple/foundationdb/pull/12330>`_
* Fixed potential DatabaseContext leaks in multi-version client. `(PR #12308) <https://github.com/apple/foundationdb/pull/12308>`_
* Fixed double-encoding of URIs in requests to S3BlobStore. `(PR #12302) <https://github.com/apple/foundationdb/pull/12302>`_
* Fixed "Unknown error" when configuring regions. `(PR #12314) <https://github.com/apple/foundationdb/pull/12314>`_
* Improved TLS handshake process to avoid blocking main thread. `(PR #12346) <https://github.com/apple/foundationdb/pull/12346>`_
* Fixed an issue in TLog server code that was causing OOMs. `(PR #12298) <https://github.com/apple/foundationdb/pull/12298>`_

7.4.3
=====
* Same as 7.4.2 release with AVX enabled.

7.4.2
=====

* Fixed a backup restore speed regression. `(PR #12098) <https://github.com/apple/foundationdb/issues/12098>`_
* Added backup URL validation for directory traversals. `(PR #12143) <https://github.com/apple/foundationdb/issues/12143>`_
* Fixed an issue where backup status update transaction would not retry in case of failure. `(PR #12175) <https://github.com/apple/foundationdb/issues/12175>`_
* Added various improvements to bulk dump/load. `(PR #12135) <https://github.com/apple/foundationdb/issues/12135>`_
* Turned off replica consistency check knobs by default which were overloading storage servers. `(PR #12133) <https://github.com/apple/foundationdb/issues/12133>`_, `(PR #12136) <https://github.com/apple/foundationdb/issues/12136>`_
* Fixed an issue where source storage server could be overloaded by data movements with replica consistency check. `(PR #12176) <https://github.com/apple/foundationdb/issues/12176>`_
* Added support for checkall command with DCID. `(PR #12100) <https://github.com/apple/foundationdb/issues/12100>`_
* Optimized keyInfo lookup in commit proxy. `(PR #12106) <https://github.com/apple/foundationdb/issues/12106>`_

7.4.1
=====
* Same as 7.4.0 release with AVX enabled.

7.4.0
=====

Features (Supported)
--------------------
* Added support to restore from new backup's partitioned log files. `(PR #11901) <https://github.com/apple/foundationdb/pull/11901>`_
* Added LRU-like cache replacement for in-memory page checksums to save memory usage. `(PR #11194) <https://github.com/apple/foundationdb/pull/11194>`_
* Added gray failure features to track remote processes and allow complaints from storage servers. `(PR #11717) <https://github.com/apple/foundationdb/pull/11717>`_, `(PR #11753) <https://github.com/apple/foundationdb/pull/11753>`_
* Added network option to disable non-TLS connections. `(PR #9984) <https://github.com/apple/foundationdb/pull/9984>`_

Features (Experimental)
-----------------------
* Added support to bulk load TBs' snapshot of key-values from S3 to an empty cluster. `(Bulk Load User Guide) <https://github.com/apple/foundationdb/blob/main/documentation/sphinx/source/bulkload-user.rst>`_, `(PR #11369) <https://github.com/apple/foundationdb/pull/11369>`_
* Added support to bulk dump TBs' snapshot of key-values to S3 from an idle cluster. `(Bulk Dump User Guide) <https://github.com/apple/foundationdb/blob/main/documentation/sphinx/source/bulkdump.rst>`_, `(PR #11780) <https://github.com/apple/foundationdb/pull/11780>`_
* Added support to upload/download to/from S3 (for bulk dump and bulk load). `(PR #11899) <https://github.com/apple/foundationdb/pull/11899>`_
* Added support to perform exclusive read range lock that blocks user write traffic to a specific range. `(Range Lock User Guide) <https://github.com/apple/foundationdb/blob/main/documentation/sphinx/source/rangelock.rst>`_, `(PR #11693) <https://github.com/apple/foundationdb/pull/11693>`_
* Added multiple improvements to the version vector feature, so that commits are sent only to tlogs buddied with the storage server that will receive the mutations.
* Added support to compute mutation and accumulative checksums to conduct real-time detection of mutation corruptions on write path. `(PR #11255) <https://github.com/apple/foundationdb/pull/11255>`_
* Added support to detect hot shards and throttle commits to them. `(PR #10970) <https://github.com/apple/foundationdb/pull/10970>`_
* Added support to synthesize test data on a cluster. `(PR #11107) <https://github.com/apple/foundationdb/pull/11107>`_
* Added support to compare storage replicas on reads. `(PR #11235) <https://github.com/apple/foundationdb/pull/11235>`_
* Added gRPC integration with Flow. `(PR #11782) <https://github.com/apple/foundationdb/pull/11782>`_, `(PR #11892) <https://github.com/apple/foundationdb/pull/11892>`_, `(PR #12023) <https://github.com/apple/foundationdb/pull/12023>`_

Performance
-----------
* Improved storage performance with ptree optimizations. `(PR #11435) <https://github.com/apple/foundationdb/pull/11435>`_
* Added yields to backup agents to avoid slow tasks. `(PR #10878) <https://github.com/apple/foundationdb/pull/10878>`_

Fixes
-----
* Fixed a fdbmonitor issue on FreeBSD where child processes continued to run after fdbmonitor termination. `(PR #11361) <https://github.com/apple/foundationdb/pull/11361>`_
* Fixed issues where backup workers missed mutations and caused assertion failures. `(PR #11908) <https://github.com/apple/foundationdb/pull/11908>`_, `(PR #12026) <https://github.com/apple/foundationdb/pull/12026>`_, `(PR #12046) <https://github.com/apple/foundationdb/pull/12046>`_
* Fixed AuditStorage empty range read error. `(PR #12043) <https://github.com/apple/foundationdb/pull/12043>`_
* Fixed an issue where failover was triggered even though remote storage servers were lagging behind. `(PR #11054) <https://github.com/apple/foundationdb/pull/11054>`_
* Fixed an issue where fdbserver was not being able to join the cluster during an upgrade. `(PR #9814) <https://github.com/apple/foundationdb/pull/9814>`_
* Fixed an assert in GetMappedRange that depends on the range read returning once it has at least one result. `(PR #10522) <https://github.com/apple/foundationdb/pull/10522>`_
* Fixed an issue where clients connecting to coordination server were never getting a response. `(PR #10363) <https://github.com/apple/foundationdb/pull/10363>`_
* Fixed an issue where describeBackup() never updated continousLogEnd property. `(PR #10488) <https://github.com/apple/foundationdb/pull/10488>`_
* Fixed an issue where watch was stuck due to races. `(PR #11112) <https://github.com/apple/foundationdb/pull/11112>`_
* Fixed an issue where ConnectionMonitor would incorrectly close connections. `(PR #10495) <https://github.com/apple/foundationdb/pull/10495>`_
* Fixed an issue where TSS conversion can be stuck sometimes. `(PR #10711) <https://github.com/apple/foundationdb/pull/10711>`_
* Fixed an issue where status timeout error on Cluster Controller was incorrectly triggering recovery. `(PR #10791) <https://github.com/apple/foundationdb/pull/10791>`_

Status
------
* Added RocksDB version to status JSON. `(PR #11868) <https://github.com/apple/foundationdb/pull/11868>`_
* Added support to fetch a specific group of status JSON fields. `(PR #10927) <https://github.com/apple/foundationdb/pull/10927>`_
* Added gray failure excluded processes to status JSON. `(PR #11758) <https://github.com/apple/foundationdb/pull/11758>`_

Bindings
--------
* Fixed an issue where not calling Close() on the db object would result in memory leak in Go bindings. This is a breaking change since clients are now expected to close their db to avoid memory leak. `(PR #11394) <https://github.com/apple/foundationdb/pull/11394>`_
* Fixed an issue causing SIGSEGV when network routine was started multiple times concurrently in Go bindings. `(PR #11104) <https://github.com/apple/foundationdb/pull/11104>`_
* Fixed an issue where user's transaction function FoundationDB error was getting reset in Go bindings. `(PR #11810) <https://github.com/apple/foundationdb/pull/11810>`_
* Fixed an issue causing panic when connecting to database from multiple threads in Go bindings. `(PR #10702) <https://github.com/apple/foundationdb/pull/10702>`_
* Added support to cancel snapshots and R/O transactions in Go bindings. `(PR #11614) <https://github.com/apple/foundationdb/pull/11614>`_
* Added GetClientStatus method to database in Go bindings. `(PR #11627) <https://github.com/apple/foundationdb/pull/11627>`_

Other Changes
-------------
* Removed upgrade support from 6.2 and earlier TLogs and made xxhash checksum the default for TLog. `(PR #11667) <https://github.com/apple/foundationdb/pull/11667>`_
* Added rate keeper logs for zones with lowest tps. `(PR #11067) <https://github.com/apple/foundationdb/pull/11067>`_
* Added LOG_CONNECTION_ATTEMPTS_ENABLED and CONNECTION_LOG_DIRECTORY to log all incoming connections to an external file. `(PR #11704) <https://github.com/apple/foundationdb/pull/11704>`_
* Added exclude in progress signal to fdbcli. `(PR #11569) <https://github.com/apple/foundationdb/pull/11569>`_
* Added a sidecar container that refreshes S3 credentials. `(PR #11945) <https://github.com/apple/foundationdb/pull/11945>`_
* Fixed an issue where storage and tlog store types were not valid as part of configure command. `(PR #10876) <https://github.com/apple/foundationdb/pull/10876>`_
* Improved BytesWritten in MovingData trace event to account for non-overlapped server lists. `(PR #10076) <https://github.com/apple/foundationdb/pull/10076>`_

Dependencies
------------
* Upgraded boost to version 1.86. `(PR #11788) <https://github.com/apple/foundationdb/pull/11788>`_
* Upgraded awssdk to version 1.11.473. `(PR #11853) <https://github.com/apple/foundationdb/pull/11853>`_
* Upgraded RocksDB to 9.7.3. `(PR #11735) <https://github.com/apple/foundationdb/pull/11735>`_
* Added support for GCC 13 and Clang 19 compilers.


Earlier release notes
---------------------
* :doc:`7.3 (API Version 730) </release-notes/release-notes-730>`
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
