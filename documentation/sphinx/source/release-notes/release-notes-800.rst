.. _release-notes:

#############
Release Notes
#############

8.0.1
=====

AVX enabled release.

* Raised the protocol versions that 8.0 binaries treat as the next release from 8.1 to 9.0 (future protocol version 9.0, minimum invalid protocol version 9.1). 8.0.0 rejects data stamped with protocol 8.2 or later, so a future 9.0 cluster can be downgraded only to 8.0.1 or later 8.0.x releases, not to 8.0.0. The current protocol version, minimum compatible version and API version are unchanged. `(PR #14423) <https://github.com/apple/foundationdb/pull/14423>`_
* Rejected file and object names that are absolute or contain a ``..`` component in places where a name is joined onto a base directory: local directory backup containers, backup file listing, S3 copy-down, bulk load job and task manifests, the profiler output file, and SST file names in fetched checkpoints. `(PR #14421) <https://github.com/apple/foundationdb/pull/14421>`_
* Avoided linking newer glibc symbols into RHEL 9 builds that prevented binaries built on RHEL 9.7 from starting on RHEL 9.6 systems and containers. `(PR #14278) <https://github.com/apple/foundationdb/pull/14278>`_
* Fixed network setup failures when older multiversion clients load the 8.0 client, even when the application does not use blob granules. Restored the required blob granule C symbols as stubs that abort the process when called. `(PR #14239) <https://github.com/apple/foundationdb/pull/14239>`_
* Removed the unused blob granule types ``FDBBGMutationType`` and the ``FDBBG*`` file description structs from ``fdb_c.h``. They were only used by the blob granule read API removed in 8.0.0. ``FDBGranuleSummary`` and ``FDBReadBlobGranuleContext`` are kept for the compatibility stubs. `(PR #14247) <https://github.com/apple/foundationdb/pull/14247>`_
* Outbound TLS connections that fdbserver opens by hostname (S3 and other HTTPS blob stores, REST and KMS servers, including connections through an HTTP proxy) are now verified by CA chain and hostname instead of the cluster's ``--tls-verify-peers`` rules. Previously, on clusters that set peer rules, every S3 server certificate was rejected, which broke bulk dump and bulk load uploads. Cluster traffic keeps using the peer rules. `(PR #14469) <https://github.com/apple/foundationdb/pull/14469>`_
* Fixed an interrupted remote-region recovery possibly starting the next generation one version too late and leaving a storage server behind, because a new log router reported its synthetic startup pop floor as committed progress. `(PR #14295) <https://github.com/apple/foundationdb/pull/14295>`_
* Added the ``SS_SKIP_DELIVERED_TO_NOT_ASSIGNED_VERSION`` server knob (default ``0``, disabled) to recover a storage server that crash-loops on a mutation delivered to a shard it is not assigned. Setting it to that one version makes the storage server drop such mutations at that version instead of asserting. `(PR #14480) <https://github.com/apple/foundationdb/pull/14480>`_
* Fixed DR (``fdbdr``) mutation log copying possibly skipping mutations when a copy step timed out. Copying now finishes the current version and resumes at the first version it did not copy. `(PR #14296) <https://github.com/apple/foundationdb/pull/14296>`_
* Fixed a race in the client library where an application thread could destroy a result while the network thread was still completing it, which could crash the client. `(PR #14373) <https://github.com/apple/foundationdb/pull/14373>`_
* Fixed a client memory leak when setting watches on many distinct keys. Watch reference-count entries are now removed once no watch uses them. `(PR #14490) <https://github.com/apple/foundationdb/pull/14490>`_
* Retried ``fallocate()`` and ``ftruncate()`` when they fail with ``EINTR`` in the Linux KAIO file truncate path, instead of failing the operation. `(PR #14326) <https://github.com/apple/foundationdb/pull/14326>`_
* Fixed Redwood page-cache memory accounting when a reused page ID changes size, so cache eviction uses the current page size. `(PR #14291) <https://github.com/apple/foundationdb/pull/14291>`_
* Fixed a Linux ``fdbmonitor`` memory leak when changes to a configuration-file symlink caused its target path to be resolved again. `(PR #14253) <https://github.com/apple/foundationdb/pull/14253>`_
* Remote-region TLogs reuse recent disk-space samples in the low-disk admission check instead of calling ``statvfs()`` for every version they pull. On a two-region cluster under write load, the per-version disk checks made spilling fall behind, and remote TLogs ran out of memory during a machine exclude. `(PR #14345) <https://github.com/apple/foundationdb/pull/14345>`_
* Reduced CPU spent on every completed RPC reply. Cancelling the per-request failure monitor wait no longer throws C++ exceptions. `(PR #14352) <https://github.com/apple/foundationdb/pull/14352>`_
* Cached the network interface name used for network-traffic metrics to reduce periodic network-thread stalls on hosts with many interfaces. `(PR #14283) <https://github.com/apple/foundationdb/pull/14283>`_
* Fixed one backup with a missing or invalid container URL dropping every backup from the backup layer status in ``status json`` (``layers.backup``). The other backups are still reported, and the failing one gets an ``error`` field. `(PR #14484) <https://github.com/apple/foundationdb/pull/14484>`_
* Mutation log tasks of a backup whose snapshot has finished now run ahead of other backups' initial snapshot tasks, so a new backup's first snapshot no longer starves other backups' logs and increases their backup lag. This raises the ``TASKBUCKET_MAX_PRIORITY`` client knob default from 1 to 2. Backup agents older than 8.0.1 (and older than 7.4.9 on 7.4) only run tasks up to priority 1, so keep at least one 8.0.1 or newer backup agent running for backups that ran under 8.0.1, and do not downgrade all backup agents while a backup is running. `(PR #14499) <https://github.com/apple/foundationdb/pull/14499>`_
* Added bounded retries for disk errors when opening or reading backup encryption-key files, allowing transient failures to recover before the backup container retains a failed initialization result. `(PR #14294) <https://github.com/apple/foundationdb/pull/14294>`_
* Added per-URL blobstore parameters for backups: ``server_side_encryption`` (or ``sse``) overrides the ``BLOBSTORE_ENCRYPTION_TYPE`` knob (``sse=none`` sends no server-side encryption header), ``proxy`` overrides the ``--proxy`` option and proxy environment variables (``proxy=none`` connects directly), and ``aws_v4_header`` (or ``v4``) overrides the ``HTTP_REQUEST_AWS_V4_HEADER`` knob. The URL is saved with the backup, so each backup on a cluster can use its own values. `(PR #14519) <https://github.com/apple/foundationdb/pull/14519>`_, `(PR #14521) <https://github.com/apple/foundationdb/pull/14521>`_
* Range file restores now send one clear per restore transaction instead of one clear per gap between restored keys, and skip that clear when the range has no existing keys. In tests on 20-storage-server clusters this made restores about 2x faster and roughly halved storage server CPU. Setting the client knobs ``RESTORE_RANGE_DISABLE_RYW`` or ``RESTORE_RANGE_READ_BEFORE_CLEAR`` (both default ``true``) to ``false`` on the backup agents restores the previous behavior. `(PR #14513) <https://github.com/apple/foundationdb/pull/14513>`_, `(PR #14525) <https://github.com/apple/foundationdb/pull/14525>`_
* Bulk dump now picks its target DC from the region configuration: the primary region by default, or the remote region when the new ``DD_BULKDUMP_USE_REMOTE_REGION`` knob is set and the cluster has more than one usable region. Previously the target was decided by the lexicographic order of DC ids. `(PR #14275) <https://github.com/apple/foundationdb/pull/14275>`_, `(PR #14384) <https://github.com/apple/foundationdb/pull/14384>`_
* Added opt-in audit logging to fdbcli. With ``--audit-log`` (or ``FDB_CLI_AUDIT_LOG_ENABLE``), every key returned by ``get``, ``getrange``, ``getrangekeys`` and ``getall`` is logged as an ``FdbCliAudit`` trace event. ``--audit-log-values`` (``none``, ``system`` or ``all``, default ``system``) picks which values are logged. Audit events go to their own ``trace_audit.*`` files, in the ``FDB_CLI_AUDIT_LOG_ENABLE`` value or ``--audit-log-dir`` if set, else next to the trace files, else the current directory. `(PR #14486) <https://github.com/apple/foundationdb/pull/14486>`_
* Added opt-in JSON audit logging to ``transaction_profiling_analyzer.py``. With ``--audit-log`` (or ``FDB_TRANSACTION_PROFILER_AUDIT_LOG_ENABLE``), every key, range and mutation it prints is appended to ``transaction_profiling_analyzer_audit.json``, in the ``FDB_TRANSACTION_PROFILER_AUDIT_LOG_ENABLE`` value or ``--audit-log-dir`` if set, else the ``FDB_NETWORK_OPTION_TRACE_ENABLE`` directory, else the current directory. `(PR #14486) <https://github.com/apple/foundationdb/pull/14486>`_
* Fixed truth testing of an absent value in the Python bindings on Python 3. ``if not tr[key]:`` raised ``TypeError`` for a missing key and now evaluates as false. `(PR #14517) <https://github.com/apple/foundationdb/pull/14517>`_
* Fixed ``FDB::Tuple.compare`` and ``FDB::Tuple::SingleFloat`` comparisons raising ``ArgumentError`` on Ruby 3.3 and later when comparing floating point values. `(PR #14518) <https://github.com/apple/foundationdb/pull/14518>`_
* Changed the Kubernetes monitor image to declare its existing non-root user numerically, allowing it to start with ``runAsNonRoot: true`` when ``runAsUser`` is not specified. `(PR #14248) <https://github.com/apple/foundationdb/pull/14248>`_
* Updated ``fdb-kubernetes-monitor``: it retries loading its configuration while the ``fdbserver`` binary is missing, empty or not executable, can be configured to always use the shared binaries (useful for compatible version upgrades without recreating pods), and exposes the staged and running ``fdbserver`` versions in a pod annotation for the operator. Its Go toolchain and dependencies now match ``main``. `(PR #14556) <https://github.com/apple/foundationdb/pull/14556>`_

8.0.0
=====

AVX enabled release.

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

* Added **Native CDC**: durable named streams over one or more disjoint user-key
  ranges, commit-version-grouped mutations, resumable consumers, and durable
  acknowledgements controlling log retention. Native C++, C, and Python APIs
  support registration, listing, removal, consumption, and acknowledgement;
  ordered fixed-partition streams can merge mutations into commit-version order.
  Operator commands report stream status and guard removal while consumers lag.
  New stream admission is disabled by default through ``ENABLE_NATIVE_CDC``.
  Consumers must handle redelivery and acknowledge only durably processed data;
  CDC does not provide exactly-once application side effects. See :doc:`/api-c`,
  :doc:`/api-python`, and the
  `CDC design <https://github.com/apple/foundationdb/blob/main/design/cdc.md>`_.
  `(PR #13287) <https://github.com/apple/foundationdb/pull/13287>`_,
  `(PR #13674) <https://github.com/apple/foundationdb/pull/13674>`_,
  `(PR #13925) <https://github.com/apple/foundationdb/pull/13925>`_,
  `(PR #13926) <https://github.com/apple/foundationdb/pull/13926>`_,
  `(PR #13971) <https://github.com/apple/foundationdb/pull/13971>`_,
  `(PR #14116) <https://github.com/apple/foundationdb/pull/14116>`_
* Extended **bulk dump and bulk load**, introduced in 7.4, to operate on selected
  key ranges. Bulk-load restore can target a non-empty database; data in the
  destination range is replaced. See :doc:`/bulkdump` and :doc:`/bulkload-user`.
  `(PR #12288) <https://github.com/apple/foundationdb/pull/12288>`_,
  `(PR #12252) <https://github.com/apple/foundationdb/pull/12252>`_,
  `(PR #13340) <https://github.com/apple/foundationdb/pull/13340>`_
* Integrated bulk dump/load with backup and restore. ``fdbbackup start`` accepts
  ``--mode bulkdump`` or ``--mode both`` for snapshots; ``fdbrestore start``
  accepts ``--mode bulkload`` to load an available BulkDump dataset. Traditional
  ``rangefile`` mode remains the default. A full rangefile restore skips BulkDump
  snapshots and requires a rangefile snapshot and any logs needed to reach
  the target version. Upgrade backup agents before submitting restores with the
  updated client. New restore-start tasks require a worker that enforces these
  file-selection and replay rules.
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
* Added the ``range_digest`` storage audit to compute a partition-independent
  content fingerprint on a quiescent cluster. This can validate a backup and
  restore without reading all data through an external client, and exposes
  per-range progress for localizing mismatches. See
  `the range-digest design <https://github.com/apple/foundationdb/blob/main/design/range-digest.md>`_.
  `(PR #13866) <https://github.com/apple/foundationdb/pull/13866>`_

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
* Removed deprecated Java finalizer-based cleanup. Java applications must close
  databases, transactions, and other native-backed objects explicitly.
  `(PR #13997) <https://github.com/apple/foundationdb/pull/13997>`_

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
* Coalesced encrypted blobstore backup reads and issued encrypted block reads
  concurrently, reducing small object-store read requests.
  `(PR #13750) <https://github.com/apple/foundationdb/pull/13750>`_,
  `(PR #13992) <https://github.com/apple/foundationdb/pull/13992>`_
* Fixed backup agents that reopen an existing encrypted backup writing unreadable
  data before the encryption key finished loading, which left restores retrying
  authentication failures without progress.
  `(PR #14072) <https://github.com/apple/foundationdb/pull/14072>`_
* Fixed reopening the same backup URL with a different proxy, encryption key file,
  or encryption block size silently reusing the first configuration.
  `(PR #14065) <https://github.com/apple/foundationdb/pull/14065>`_
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
* Fixed ``fdbbackup status --json`` omitting recorded errors, and made
  ``fdbdecode`` apply version filters before inspecting snapshot manifests.
  `(PR #14124) <https://github.com/apple/foundationdb/pull/14124>`_,
  `(PR #14122) <https://github.com/apple/foundationdb/pull/14122>`_

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
  transitions and avoided publishing transient pipeline capacity, allowing
  stranded shards to make progress without repeated duplicate submissions.
  `(PR #13838) <https://github.com/apple/foundationdb/pull/13838>`_,
  `(PR #14012) <https://github.com/apple/foundationdb/pull/14012>`_
* Fixed Sharded RocksDB resource lifetime and compaction shutdown handling during
  storage-server rollback.
  `(PR #13726) <https://github.com/apple/foundationdb/pull/13726>`_
* Enabled stale client-peer eviction and eager proxy refresh by default
  (``LOCATION_CACHE_PEER_EVICTOR_ENABLED``, ``DBCONTEXT_EAGER_PROXY_UPDATE``,
  ``SHRINK_PROXY_LIST_CLEAR_CACHE_BELOW_THRESHOLD``). These were available but
  disabled by default in 7.3 and 7.4. This reduces connection-timeout churn after
  storage-server or proxy failures.
  `(PR #14050) <https://github.com/apple/foundationdb/pull/14050>`_
* Preserved old TLog history until remote replicas have durably copied it, and
  retried lost old-log-router initialization replies during recovery.
  `(PR #13913) <https://github.com/apple/foundationdb/pull/13913>`_,
  `(PR #14047) <https://github.com/apple/foundationdb/pull/14047>`_
* Fixed commit-proxy recruitment in small configurations and enforced admission
  control on key-location requests after a cluster bounce.
  `(PR #13400) <https://github.com/apple/foundationdb/pull/13400>`_,
  `(PR #14090) <https://github.com/apple/foundationdb/pull/14090>`_
* Fixed a RocksDB checkpoint-reader initialization race.
  `(PR #14084) <https://github.com/apple/foundationdb/pull/14084>`_

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
* Upgraded RocksDB to **11.1.2**. Recent 7.4.x releases use RocksDB **8.11.x**.
  `(PR #14189) <https://github.com/apple/foundationdb/pull/14189>`_
* Updated the main Docker image base to Rocky Linux **10.2**.
  `(PR #12549) <https://github.com/apple/foundationdb/pull/12549>`_
* Upgraded Boost from **1.86.0** to **1.89.0**.
  `(PR #14137) <https://github.com/apple/foundationdb/pull/14137>`_
* Removed Flow's unused ``CompressionUtils`` abstraction and its zstd support.
  `(PR #13708) <https://github.com/apple/foundationdb/pull/13708>`_

Earlier release notes
----------------------

* :doc:`7.4 (API Version 740) </release-notes/release-notes-740>`
* :doc:`7.3 (API Version 730) </release-notes/release-notes-730>`
* :doc:`All earlier releases </earlier-release-notes>`
