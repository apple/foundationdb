#############
Release Notes
#############

7.0.0
=====

Features
--------
* First release of the Redwood Storage Engine, a BTree storage engine with higher throughput and lower write amplification than SQLite. See :ref:`Redwood Storage Engine Documentation <redwood-storage-engine>` for details.
* Replaced committed version broadcast through proxy with centralizing live committed versions into master. `(PR #3307) <https://github.com/apple/foundationdb/pull/3307>`_
* Added a new API in all bindings that can be used to get a list of split points that will split the given range into (roughly) equally sized chunks. `(PR #3394) <https://github.com/apple/foundationdb/pull/3394>`_
* Introduced a new role called GRV proxy specialized for serving GRV requests to decrease GRV tail latency since we prioritize commit paths over GRV in the current proxy. The original proxy is renamed to Commit proxy. `(PR #3549) <https://github.com/apple/foundationdb/pull/3549>`_ `(PR #3772) <https://github.com/apple/foundationdb/pull/3772>`_
* Added support for writing backup files directly to Azure blob storage. This is not yet performance tested on large-scale clusters. `(PR #3961) <https://github.com/apple/foundationdb/pull/3961>`_
* Tag-based throttling now also takes the write path into account. `(PR #3512) <https://github.com/apple/foundationdb/pull/3512>`_
* Added the ability to ratekeeper to throttle certain types of tags based on write hot spots in addition to read hot spots. `(PR #3571) <https://github.com/apple/foundationdb/pull/3571>`_
* Users now have the option to make ratekeeper recommend which transaction tags should be throttled, but not actually throttle them using fdbcli. `(PR #3669) <https://github.com/apple/foundationdb/pull/3669>`_
* Added a new ``--build_flags option`` to binaries to print build information. `(PR #3769) <https://github.com/apple/foundationdb/pull/3769>`_
* Added ``--incremental`` option to backup and restore that allows specification of only recording mutation log files and not range files. Incremental restore also allows restoring to a non-empty destination database. `(PR #3676) <https://github.com/apple/foundationdb/pull/3676>`_
* Added a tracing framework to track request latency through each FDB component. See :ref:`Documentation <request-tracing>` for details. `(PR #3329) <https://github.com/apple/foundationdb/pull/3329>`_
* Added the :ref:`(Global Configuration Framework) <global-configuration>`, an eventually consistent configuration mechanism to efficiently make runtime changes to all clients and servers. `(PR #4330) <https://github.com/apple/foundationdb/pull/4330>`_
* Added the ability to monitor and manage an fdb cluster via read/write specific special keys through transactions. See :ref:`Documentation <special-keys>` for details. `(PR #3455) <https://github.com/apple/foundationdb/pull/3455>`_
* Added TLS support to fdbdecode for decoding mutation log files stored in blobs. `(PR #4611) <https://github.com/apple/foundationdb/pull/4611>`_
* Added ``initial_snapshot_interval`` to fdbbackup that can specify the duration of the first inconsistent snapshot written to the backup. `(PR #4620) <https://github.com/apple/foundationdb/pull/4620>`_
* Added ``inconsistent_snapshot_only`` to fdbbackup that ignores mutation log files and only uses range files during the restore to speedup the process. `(PR #4704) <https://github.com/apple/foundationdb/pull/4704>`_
* Added the Testing Storage Server (TSS), which allows FoundationDB to run an "untrusted" storage engine with identical workload to the current storage engine, with zero impact on durability or correctness, and minimal impact on performance. `(Documentation) <https://github.com/apple/foundationdb/blob/main/documentation/sphinx/source/tss.rst>`_ `(PR #4556) <https://github.com/apple/foundationdb/pull/4556>`_
* Added perpetual storage wiggle that supports less impactful B-trees recreation and data migration. These will also be used for deploying the Testing Storage Server which compares 2 storage engines' results. See :ref:`Documentation <perpetual-storage-wiggle>` for details. `(PR #4838) <https://github.com/apple/foundationdb/pull/4838>`_
* Improved the efficiency with which storage servers replicate data between themselves. `(PR #5017) <https://github.com/apple/foundationdb/pull/5017>`_
* Added support to ``exclude command`` to exclude based on locality match. `(PR #5113) <https://github.com/apple/foundationdb/pull/5113>`_
* Add the ``trace_partial_file_suffix`` network option. This option will give unfinished trace files a special suffix to indicate they're not complete yet. When the trace file is complete, it is renamed to remove the suffix. `(PR #5328) <https://github.com/apple/foundationdb/pull/5328>`_

Performance
-----------
* Improved Deque copy performance. `(PR #3197) <https://github.com/apple/foundationdb/pull/3197>`_
* Increased performance of dr_agent when copying the mutation log. The ``COPY_LOG_BLOCK_SIZE``, ``COPY_LOG_BLOCKS_PER_TASK``, ``COPY_LOG_PREFETCH_BLOCKS``, ``COPY_LOG_READ_AHEAD_BYTES`` and ``COPY_LOG_TASK_DURATION_NANOS`` knobs can be set. `(PR #3436) <https://github.com/apple/foundationdb/pull/3436>`_
* Added multiple new microbenchmarks for PromiseStream, Reference, IRandom, and timer, as well as support for benchmarking actors. `(PR #3590) <https://github.com/apple/foundationdb/pull/3590>`_
* Use xxhash3 for SQLite page checksums. `(PR #4075) <https://github.com/apple/foundationdb/pull/4075>`_
* fdbserver now uses jemalloc on Linux instead of the system malloc. `(PR #4222) <https://github.com/apple/foundationdb/pull/4222>`_
* Watches have been optimized and are now significantly cheaper. `(PR #4266) <https://github.com/apple/foundationdb/pull/4266>`_ `(PR #4382 ) <https://github.com/apple/foundationdb/pull/4382>`_
* The Coro library has been replaced with boost::coroutine2. `(PR #4242) <https://github.com/apple/foundationdb/pull/4242>`_
* Reduce CPU overhead of load balancing on client processes. `(PR #4561) <https://github.com/apple/foundationdb/pull/4561>`_
* Used the restored key range to filter out files for faster restore. `(PR #4568) <https://github.com/apple/foundationdb/pull/4568>`_
* Transaction log files will be truncated by default if they are under 2GB in size. `(PR #4656) <https://github.com/apple/foundationdb/pull/4656>`_
* Reduced the number of connections required by the multi-version client when loading external clients. When connecting to 7.0 clusters, only one connection with version 6.2 or larger will be used. With older clusters, at most two connections with version 6.2 or larger will be used. Clients older than version 6.2 will continue to create an additional connection each. `(PR #4667) <https://github.com/apple/foundationdb/pull/4667>`_

Reliability
-----------
* Backup agents now pause themselves upon a successful snapshot recovery to avoid unintentional data corruption. Operators should manually abort backup agents and clear the backup agent keyspace to avoid using the old cluster's backup configuration. `(PR #4027) <https://github.com/apple/foundationdb/pull/4027>`_
* Log class processes are prioritized above transaction class proceses for becoming tlogs. `(PR #4509) <https://github.com/apple/foundationdb/pull/4509>`_ 
* Improved worker recruitment logic to avoid unnecessary recoveries when processes are added or removed from a cluster. `(PR #4695) <https://github.com/apple/foundationdb/pull/4695>`_ `(PR #4631) <https://github.com/apple/foundationdb/pull/4631>`_ `(PR #4509) <https://github.com/apple/foundationdb/pull/4509>`_

Fixes
-----
* List files asynchronously so many backup files on a slow disk won't cause the backup agent to lose its lease. `(PR #3094) <https://github.com/apple/foundationdb/pull/3094>`_
* Unknown endpoint has been tracked incorrectly and therefore showed up too frequently in our statistics. `(PR #4473) <https://github.com/apple/foundationdb/pull/4473>`_
* Using the ``exclude failed`` command could leave the data distributor in a state where it cannot complete relocations. `(PR #4495) <https://github.com/apple/foundationdb/pull/4495>`_ 
* Fixed a rare crash on the cluster controller when using multi-region configurations. `(PR #4547) <https://github.com/apple/foundationdb/pull/4547>`_ 
* Fixed a memory corruption bug in the data distributor. `(PR #4535) <https://github.com/apple/foundationdb/pull/4535>`_
* Fixed a rare crash that could happen on the sequencer during recovery. `(PR #4548) <https://github.com/apple/foundationdb/pull/4548>`_ 
* Added a new pre-backup action when creating a backup. Backups can now either verify the range data is being saved to is empty before the backup begins (current behavior) or clear the range where data is being saved to. Fixes a ``restore_destination_not_empty`` failure after a backup retry due to ``commit_unknown_failure``. `(PR #4595) <https://github.com/apple/foundationdb/pull/4595>`_
* When configured with ``usable_regions=2``, a cluster would not fail over to a region which contained only storage class processes. `(PR #4599) <https://github.com/apple/foundationdb/pull/4599>`_ 
* If a restore is done using a prefix to remove and specific key ranges to restore, the key range boundaries must begin with the prefix to remove. `(PR #4684) <https://github.com/apple/foundationdb/pull/4684>`_
* The multi-version client API would not propagate errors that occurred when creating databases on external clients. This could result in a invalid memory accesses. `(PR #5220) <https://github.com/apple/foundationdb/pull/5220>`_
* Fixed a race between the multi-version client connecting to a cluster and destroying the database that could cause an assertion failure. `(PR #5220) <https://github.com/apple/foundationdb/pull/5220>`_
* A client might not honor transaction timeouts when using the multi-version client if it cannot connect to the cluster. `(Issue #5595) <https://github.com/apple/foundationdb/issues/5595>`_

Status
------
* Added ``cluster.qos.throttled_tags`` and ``cluster.processes.*.roles.busiest_[read|write]_tag`` to report statistics on throttled tags and the busiest read or write transaction tags on each storage server. `(PR #3669) <https://github.com/apple/foundationdb/pull/3669>`_ `(PR #3696) <https://github.com/apple/foundationdb/pull/3696>`_
* Added ``seconds_since_last_recovered`` to the ``cluster.recovery_state`` section to report how long it has been since the cluster recovered to the point where it is able to accept requests. `(PR #3759) <https://github.com/apple/foundationdb/pull/3759>`_
* Added limiting metrics (limiting_storage_durability_lag and limiting_storage_queue) to health metrics. `(PR #4067) <https://github.com/apple/foundationdb/pull/4067>`_
* ``min_replicas_remaining`` is now populated for all regions, thus giving a clear picture of the data replicas that exist in the database. `(PR 4515) <https://github.com/apple/foundationdb/pull/4515>`_
* Added detailed metrics for batched transactions. `(PR #4540) <https://github.com/apple/foundationdb/pull/4540>`_
* Added ``commit_batching_window_size`` to the proxy roles section of status to record statistics about commit batching window size on each proxy. `(PR #4735) <https://github.com/apple/foundationdb/pull/4735>`_
* Added ``cluster.bounce_impact`` section to status to report if there will be any extra effects when bouncing the cluster, and if so, the reason for those effects. `(PR #4770) <https://github.com/apple/foundationdb/pull/4770>`_
* Added ``fetched_versions`` to the storage metrics section of status to report how fast a storage server is catching up in versions. `(PR #4770) <https://github.com/apple/foundationdb/pull/4770>`_
* Added ``fetches_from_logs`` to the storage metrics section of status to report how frequently a storage server fetches updates from transaction logs. `(PR #4770) <https://github.com/apple/foundationdb/pull/4770>`_

Bindings
--------
* Python: The function ``get_estimated_range_size_bytes`` will now throw an error if the ``begin_key`` or ``end_key`` is ``None``. `(PR #3394) <https://github.com/apple/foundationdb/pull/3394>`_
* C: Added a function, ``fdb_database_reboot_worker``, to reboot or suspend the specified process. `(PR #4094) <https://github.com/apple/foundationdb/pull/4094>`_
* C: Added a function, ``fdb_database_force_recovery_with_data_loss``, to force the database to recover into the given datacenter. `(PR #4220) <https://github.com/apple/foundationdb/pull/4220>`_
* C: Added a function, ``fdb_database_create_snapshot``, to create a snapshot of the database. `(PR #4241) <https://github.com/apple/foundationdb/pull/4241/files>`_
* C: Added ``fdb_database_get_main_thread_busyness`` function to report how busy a client's main thread is. `(PR #4504) <https://github.com/apple/foundationdb/pull/4504>`_
* Java: Added ``Database.getMainThreadBusyness`` function to report how busy a client's main thread is. `(PR #4564) <https://github.com/apple/foundationdb/pull/4564>`_

Other Changes
-------------
* Added rte_memcpy from DPDK for default usage. `(PR #3089) <https://github.com/apple/foundationdb/pull/3089/files>`_
* When ``fdbmonitor`` dies, all of its child processes are now killed. `(PR #3841) <https://github.com/apple/foundationdb/pull/3841>`_
* The ``foundationdb`` service installed by the RPM packages will now automatically restart ``fdbmonitor`` after 60 seconds when it fails. `(PR #3841) <https://github.com/apple/foundationdb/pull/3841>`_
* Capture output of forked snapshot processes in trace events. `(PR #4254) <https://github.com/apple/foundationdb/pull/4254/files>`_
* Add ErrorKind field to Severity 40 trace events. `(PR #4741) <https://github.com/apple/foundationdb/pull/4741/files>`_
* Added histograms for the storage server write path components. `(PR #5021) <https://github.com/apple/foundationdb/pull/5021/files>`_
* Committing a transaction will no longer partially reset it as of API version 700. `(PR #5271) <https://github.com/apple/foundationdb/pull/5271/files>`_

Earlier release notes
---------------------
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
