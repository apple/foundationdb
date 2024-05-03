#############
Release Notes
#############

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
* Added support for large shard. `(PR#10965) <https://github.com/apple/foundationdb/pull/10965>`_
* Fixed perpetual wiggle locality match regex. `(PR#10973) <https://github.com/apple/foundationdb/pull/10973>`_
* Added a knob to throttle perpetual wiggle data move. `(PR#10957) <https://github.com/apple/foundationdb/pull/10957>`_

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
