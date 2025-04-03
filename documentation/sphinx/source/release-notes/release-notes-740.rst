#############
Release Notes
#############

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
* :doc:`Beta 3 (API Version 23) </release-notes/release-notes-023>`
* :doc:`Beta 2 (API Version 22) </release-notes/release-notes-022>`
* :doc:`Beta 1 (API Version 21) </release-notes/release-notes-021>`
* :doc:`Alpha 6 (API Version 16) </release-notes/release-notes-016>`
* :doc:`Alpha 5 (API Version 14) </release-notes/release-notes-014>`
