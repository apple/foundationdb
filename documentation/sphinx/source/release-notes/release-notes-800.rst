.. _release-notes-800:

###################
Release Notes (8.0)
###################

8.0.0 (unreleased)
==================

.. note::

   These are draft notes for the upcoming 8.0 release. They summarize notable
   changes on ``main`` relative to ``release-7.4`` as of September 3, 2026.
   Release contents, experimental feature status, and upgrade guidance remain
   subject to review before release.

.. Audit snapshot: main 3e8f0d8546c7cf723bb33e0decdb31fa1e9db841;
   release-7.4 a5c62d6846c0402801e8fb9ac881fc9a86c2304a.
   Compare branch-tip trees as well as history: cherry-picked 7.4 fixes are
   not necessarily new in 8.0. See the accompanying PR for the audit method.

Compatibility and removed features
----------------------------------

* The API version is now **800**. Native CDC's C API requires applications to
  select API version 800. `(PR #12510) <https://github.com/apple/foundationdb/pull/12510>`_,
  `(PR #13674) <https://github.com/apple/foundationdb/pull/13674>`_
* Removed the experimental **multitenancy and metacluster** features, including
  their administration commands. The tenant C symbols retained for older
  bindings to load are stubs that **abort the process when called**; they do not
  preserve tenant functionality. Applications using tenants require a migration
  plan before adopting 8.0. `(PR #12583) <https://github.com/apple/foundationdb/pull/12583>`_,
  `(PR #12593) <https://github.com/apple/foundationdb/pull/12593>`_
* Removed experimental **encryption at rest** and its key-management roles.
  This removal does not remove file-level backup encryption. Deployments using
  encryption at rest require a migration plan before adopting 8.0.
  `(PR #12667) <https://github.com/apple/foundationdb/pull/12667>`_
* Removed experimental **blob granules**, the storage-server **ChangeFeed**
  feature, and **storage cache servers**, together with their associated APIs
  and commands. Native CDC is a separate interface, not a compatible replacement
  for the removed ChangeFeed API.
  `(PR #12435) <https://github.com/apple/foundationdb/pull/12435>`_,
  `(PR #12470) <https://github.com/apple/foundationdb/pull/12470>`_,
  `(PR #12486) <https://github.com/apple/foundationdb/pull/12486>`_
* Removed the experimental **configuration database and dynamic knobs**,
  including the ``use_config_database`` database option.
  `(PR #12683) <https://github.com/apple/foundationdb/pull/12683>`_
* Removed **quota-based global tag throttling** and its proxy-side machinery.
  Manual and automatic tag throttling remain available.
  `(PR #13181) <https://github.com/apple/foundationdb/pull/13181>`_
* Removed the experimental **parallel restore** implementation and its restore
  worker roles. This does not remove the regular ``fdbrestore`` tool.
  `(PR #12903) <https://github.com/apple/foundationdb/pull/12903>`_
* Removed the synthetic-data generation feature and the orphaned Azure backup
  integration. `(PR #12707) <https://github.com/apple/foundationdb/pull/12707>`_,
  `(PR #13963) <https://github.com/apple/foundationdb/pull/13963>`_

Features (Experimental)
-----------------------

* Added **Native CDC**: durable named streams for a single user-key range,
  commit-version-grouped mutations, resumable consumers, and durable
  acknowledgements controlling log retention. The native C++ and C APIs support
  registration, listing, removal, consumption, and acknowledgement. New stream
  admission is disabled by default through ``ENABLE_NATIVE_CDC``. Consumers must
  handle redelivery and acknowledge only durably processed data; CDC does not
  provide exactly-once application side effects. See :doc:`/api-c` and the
  `CDC design <https://github.com/apple/foundationdb/blob/main/design/cdc.md>`_.
  `(PR #13287) <https://github.com/apple/foundationdb/pull/13287>`_,
  `(PR #13674) <https://github.com/apple/foundationdb/pull/13674>`_
* Extended **bulk dump and bulk load**, introduced in 7.4, to operate on selected
  key ranges. Bulk-load restore can target a non-empty database; data in the
  destination range is replaced. See :doc:`/bulkdump` and :doc:`/bulkload-user`.
  `(PR #12288) <https://github.com/apple/foundationdb/pull/12288>`_,
  `(PR #12252) <https://github.com/apple/foundationdb/pull/12252>`_,
  `(PR #13340) <https://github.com/apple/foundationdb/pull/13340>`_
* Integrated bulk dump/load with backup and restore. ``fdbbackup start`` accepts
  ``--mode bulkdump`` or ``--mode both`` for snapshots; ``fdbrestore start``
  accepts ``--mode bulkload`` to load an available BulkDump dataset. Traditional
  ``rangefile`` mode remains the default. An incomplete bulk dataset produces an
  error directing the operator to retry with ``--mode rangefile``.
  `(PR #12608) <https://github.com/apple/foundationdb/pull/12608>`_,
  `(PR #13873) <https://github.com/apple/foundationdb/pull/13873>`_
* Added **range-partitioned mutation logging (Backup V3)**, including partition
  maps, range backup worker recruitment, and partitioned log uploads. It is
  selected with ``--mutation-log-type range-partitioned-log-experimental``.
  This is under active development and is not ready for general or critical
  workloads; the on-disk layout, knobs, and command-line interface remain
  subject to change. See the
  `Backup V3 design <https://github.com/apple/foundationdb/blob/main/design/backup_v3_range_partitioned_logs.md>`_.
  `(PR #12671) <https://github.com/apple/foundationdb/pull/12671>`_,
  `(PR #13286) <https://github.com/apple/foundationdb/pull/13286>`_,
  `(PR #13304) <https://github.com/apple/foundationdb/pull/13304>`_
* Added a gRPC **ControlService** and the ``fdbctl`` library for cluster
  administration, including configuration, status, coordinator management,
  worker inclusion/exclusion, and maintenance. This extends the gRPC integration
  already present in 7.4; it requires a build with gRPC enabled.
  `(PR #12540) <https://github.com/apple/foundationdb/pull/12540>`_,
  `(PR #12555) <https://github.com/apple/foundationdb/pull/12555>`_,
  `(PR #12603) <https://github.com/apple/foundationdb/pull/12603>`_

Client APIs and bindings
------------------------

* Added the ``MAX_GRV_QUEUE_DELAY`` transaction option. A GRV proxy can reject a
  request with ``transaction_grv_queue_rejected`` when its estimated queue delay
  from ratekeeper throttling exceeds the supplied limit in milliseconds. The
  estimate is advisory, not an end-to-end transaction deadline.
  `(PR #13085) <https://github.com/apple/foundationdb/pull/13085>`_
* Added ``fdb_transaction_get_range_split_points_with_limit`` and Go
  ``GetRangeSplitPointsWithLimit`` to bound the number of interior split points,
  including shard boundaries. The endpoints are always included; a negative
  limit preserves unlimited behavior.
  `(PR #13693) <https://github.com/apple/foundationdb/pull/13693>`_
* Added the ``TRACE_IP`` network option to explicitly select the IPv4 or IPv6
  address recorded in traces.
  `(PR #12645) <https://github.com/apple/foundationdb/pull/12645>`_
* Added ``Database.GetMainThreadBusyness`` to the Go binding.
  `(PR #12594) <https://github.com/apple/foundationdb/pull/12594>`_
* Fixed read-your-writes transactions leaving watch futures unresolved when a
  commit is interrupted by a transaction error such as a timeout.
  `(PR #13395) <https://github.com/apple/foundationdb/pull/13395>`_

Backup, restore, and object storage
-----------------------------------

* Added Google Cloud Storage access through its S3-compatible XML API with
  OAuth2 bearer-token authentication.
  `(PR #12975) <https://github.com/apple/foundationdb/pull/12975>`_
* Added ``prefix=`` to blobstore backup URLs, placing both backup data and index
  objects under a chosen object-key prefix. All tools accessing such a backup
  must use the same prefix. Upgrade agents and client tools before using this
  option: older versions reject URLs containing it. See :doc:`/backups`.
  `(PR #13915) <https://github.com/apple/foundationdb/pull/13915>`_
* Added SHA-256 integrity checking for multipart S3 uploads and improved S3/REST
  request handling and connection reuse.
  `(PR #12246) <https://github.com/apple/foundationdb/pull/12246>`_,
  `(PR #12447) <https://github.com/apple/foundationdb/pull/12447>`_
* Coalesced encrypted blobstore backup reads to reduce small object-store read
  requests. `(PR #13750) <https://github.com/apple/foundationdb/pull/13750>`_
* Added progress-based timeouts for backup/restore bulk jobs. Failed or incomplete
  bulk dumps and unverifiable bulk-load restores now fail instead of appearing
  successful or waiting indefinitely.
  `(PR #13945) <https://github.com/apple/foundationdb/pull/13945>`_,
  `(PR #13936) <https://github.com/apple/foundationdb/pull/13936>`_,
  `(PR #13873) <https://github.com/apple/foundationdb/pull/13873>`_
* Split bulk-load tasks that cannot be placed on a destination team and preserved
  tasks across relocation failures, improving restore progress at scale.
  `(PR #13923) <https://github.com/apple/foundationdb/pull/13923>`_,
  `(PR #13873) <https://github.com/apple/foundationdb/pull/13873>`_
* Rejected path traversal in bulk-load manifest file paths.
  `(PR #13665) <https://github.com/apple/foundationdb/pull/13665>`_

Cluster operations and reliability
----------------------------------

* Added ``fdbcli rangelock`` commands to register and list owners, inspect locks,
  and take or release exclusive read locks. These locks reject writes only when
  ``ENABLE_READ_LOCK_ON_RANGE`` is enabled on commit proxies. Hardened range and
  owner validation, and prevented unregistering owners with active locks.
  See :doc:`/rangelock`.
  `(PR #13323) <https://github.com/apple/foundationdb/pull/13323>`_,
  `(PR #13922) <https://github.com/apple/foundationdb/pull/13922>`_,
  `(PR #13942) <https://github.com/apple/foundationdb/pull/13942>`_
* Added ``--tlog-spill-datadir`` to place TLog spill data in a separate directory,
  and ``--tlog-spill-filesystem`` for Linux mount validation. Disk snapshot
  procedures must include the spill directory when it is configured.
  `(PR #13314) <https://github.com/apple/foundationdb/pull/13314>`_
* Added log-router replacement after failures without requiring full
  transaction-system recovery.
  `(PR #12558) <https://github.com/apple/foundationdb/pull/12558>`_
* Changed low-disk TLog handling to keep existing logs available while recruiting
  replacements and to avoid repeatedly selecting low-disk workers.
  `(PR #13781) <https://github.com/apple/foundationdb/pull/13781>`_
* Hardened worker registration checks before recruitment to avoid using stale
  worker interfaces.
  `(PR #13646) <https://github.com/apple/foundationdb/pull/13646>`_
* Bounded retries for degraded storage teams across data-distribution pipeline
  transitions, allowing stranded shards to make progress without repeated
  duplicate submissions.
  `(PR #13838) <https://github.com/apple/foundationdb/pull/13838>`_
* Fixed Sharded RocksDB resource lifetime and compaction shutdown handling during
  storage-server rollback.
  `(PR #13726) <https://github.com/apple/foundationdb/pull/13726>`_

Performance and observability
-----------------------------

* Reduced conflict-detection work with word-level bitmap operations and reusable
  buffers, and reduced arena allocations when materializing RocksDB range reads.
  `(PR #12365) <https://github.com/apple/foundationdb/pull/12365>`_,
  `(PR #12367) <https://github.com/apple/foundationdb/pull/12367>`_,
  `(PR #13273) <https://github.com/apple/foundationdb/pull/13273>`_
* Added watch and version-vector metrics to status JSON, and system-keyspace
  size reporting to ``fdbcli status``.
  `(PR #13814) <https://github.com/apple/foundationdb/pull/13814>`_,
  `(PR #13191) <https://github.com/apple/foundationdb/pull/13191>`_,
  `(PR #12443) <https://github.com/apple/foundationdb/pull/12443>`_
* Added commit statistics and transaction-size histograms, data-distribution
  maintenance-duration reporting, commit-batch flush reasons, and TLog disk-queue
  write/commit size histograms.
  `(PR #13416) <https://github.com/apple/foundationdb/pull/13416>`_,
  `(PR #13403) <https://github.com/apple/foundationdb/pull/13403>`_,
  `(PR #13767) <https://github.com/apple/foundationdb/pull/13767>`_
* Added sampled allocation attribution by call site. Production sampling is off
  by default (``MEMORY_TRACKING_SAMPLE_INVERSE=0``), and the tracker can be
  compiled out with ``FDB_MEMORY_TRACKER=OFF``. See the
  `memory tracker design <https://github.com/apple/foundationdb/blob/main/design/memory-tracker.md>`_.
  `(PR #13344) <https://github.com/apple/foundationdb/pull/13344>`_

Build and packaging
--------------------

* Migrated Flow actors to standard C++20 coroutines and removed the actor
  compiler and its source-generation step. Updated developer documentation and
  tutorials to use ``co_await``, ``co_return``, and the Flow coroutine runtime.
  `(PR #13961) <https://github.com/apple/foundationdb/pull/13961>`_,
  `(PR #13958) <https://github.com/apple/foundationdb/pull/13958>`_
* Added optional build integration for the separately maintained Swift bindings.
  It requires Swift 6.1 or newer and Clang; Linux builds also require libc++.
  `(PR #12428) <https://github.com/apple/foundationdb/pull/12428>`_,
  `(PR #12500) <https://github.com/apple/foundationdb/pull/12500>`_
* The source build selects RocksDB **9.7.3**, compared with **8.11.4** on the
  audited 7.4 branch. See the
  `8.0 RocksDB version configuration <https://github.com/apple/foundationdb/blob/3e8f0d8546c7cf723bb33e0decdb31fa1e9db841/cmake/RocksDBVersion.cmake>`_
  and the
  `7.4 RocksDB version configuration <https://github.com/apple/foundationdb/blob/a5c62d6846c0402801e8fb9ac881fc9a86c2304a/cmake/RocksDBVersion.cmake>`_.
* Updated the main Docker image base to Rocky Linux **10.2**.
  `(PR #12549) <https://github.com/apple/foundationdb/pull/12549>`_
* Removed Flow's unused ``CompressionUtils`` abstraction and its zstd support.
  `(PR #13708) <https://github.com/apple/foundationdb/pull/13708>`_

Earlier release notes
----------------------

* :doc:`7.4 (API Version 740) </release-notes/release-notes-740>`
* :doc:`All earlier releases </earlier-release-notes>`
