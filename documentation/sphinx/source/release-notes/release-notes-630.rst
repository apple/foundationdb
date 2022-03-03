#############
Release Notes
#############

6.3.24
======
* Fixed a bug where get key location can overload proxies. `(PR #6453) <https://github.com/apple/foundationdb/pull/6453>`_ 
* Added a mechanism that can reduce the number of empty peek reply by not always returning empty peek reply immediately. `(PR #6413) <https://github.com/apple/foundationdb/pull/6413>`_
* Enable TLS support for Windows. `(PR #6193) <https://github.com/apple/foundationdb/pull/6193>`_
* Fixed a bug where a shard gets merged too soon. `(PR #6115) <https://github.com/apple/foundationdb/pull/6115>`_

6.3.23
======
* Add AWS v4 header support for backup. `(PR #6025) <https://github.com/apple/foundationdb/pull/6025>`_
* Fixed a bug that remoteDCIsHealthy logic is not guarded by CC_ENABLE_WORKER_HEALTH_MONITOR, which may prevent HA failback. `(PR #6106) <https://github.com/apple/foundationdb/pull/6106>`_
* Fixed a race condition with updating the coordinated state and updating the master registration. `(PR #6088) <https://github.com/apple/foundationdb/pull/6088>`_
* Changed dbinfo broadcast to be explicitly requested by the worker registration message. `(PR #6073) <https://github.com/apple/foundationdb/pull/6073>`_

6.3.22
======
* Added histograms to client GRV batcher. `(PR #5760) <https://github.com/apple/foundationdb/pull/5760>`_
* Added FastAlloc memory utilization trace. `(PR #5759) <https://github.com/apple/foundationdb/pull/5759>`_
* Added locality cache size to TransactionMetrics. `(PR #5771) <https://github.com/apple/foundationdb/pull/5771>`_
* Added a new feature that allows FDB to failover to remote DC when the primary is experiencing massive grey failure. This feature is turned off by default. `(PR #5774) <https://github.com/apple/foundationdb/pull/5774>`_

6.3.21
======
* Added a ThreadID field to all trace events for the purpose of multi-threaded client debugging. `(PR #5665) <https://github.com/apple/foundationdb/pull/5665>`_
* Fixed some histograms' group name in the master proxy. `(PR #5674) <https://github.com/apple/foundationdb/pull/5674>`_
* Added histograms for GRV path components in the proxy. `(PR #5689) <https://github.com/apple/foundationdb/pull/5689>`_
* Fixed race condition introduced in 6.3.20 between setting timeouts and resetting or destroying transactions. `(PR #5695) <https://github.com/apple/foundationdb/pull/5695>`_
* Disable detailed transaction log pop tracing by default. `(PR #5696) <https://github.com/apple/foundationdb/pull/5696>`_

6.3.20
======
* Several minor problems with the versioned packages have been fixed. `(PR 5607) <https://github.com/apple/foundationdb/pull/5607>`_
* A client might not honor transaction timeouts when using the multi-version client if it cannot connect to the cluster. `(Issue #5595) <https://github.com/apple/foundationdb/issues/5595>`_
* Fixed a very rare bug where recovery could potentially roll back a committed transaction `(PR 5461) <https://github.com/apple/foundationdb/pull/5461>`_
* Added histograms for commit path components in the proxy. `(PR #5367) <https://github.com/apple/foundationdb/pull/5367>`_
* Fixed a false checkRegions call that could cause unwanted primary DC failover. `(PR #5330) <https://github.com/apple/foundationdb/pull/5330>`_

6.3.19
======
* Added the ``trace_partial_file_suffix`` network option. This option will give unfinished trace files a special suffix to indicate they're not complete yet. When the trace file is complete, it is renamed to remove the suffix. `(PR #5330) <https://github.com/apple/foundationdb/pull/5330>`_
* Added error details in ``RemovedDeadBackupLayerStatus`` trace event. `(PR #5356) <https://github.com/apple/foundationdb/pull/5356>`_
* Added RepeatableReadMultiThreadClientTest. `(PR #5212) <https://github.com/apple/foundationdb/pull/5212>`_
* Added a new feature that allows FDB to detect grey failures and automatically recover from them. `(PR #5249) <https://github.com/apple/foundationdb/pull/5249>`_
* Added version and timestamp to ``TimeKeeperCommit`` trace event. `(PR #5415) <https://github.com/apple/foundationdb/pull/5415>`_
* Added ``RecruitFromConfigurationRetry`` trace event to improve recruitment observability. `(PR #5455) <https://github.com/apple/foundationdb/pull/5455>`_
* Several fixes to pkg_tester and packaging. `(PR #5460) <https://github.com/apple/foundationdb/pull/5460>`_

6.3.18
======
* The multi-version client API would not propagate errors that occurred when creating databases on external clients. This could result in a invalid memory accesses. `(PR #5221) <https://github.com/apple/foundationdb/pull/5221>`_
* Fixed a race between the multi-version client connecting to a cluster and destroying the database that could cause an assertion failure. `(PR #5221) <https://github.com/apple/foundationdb/pull/5221>`_
* Added Mako latency measurements. `(PR #5255) <https://github.com/apple/foundationdb/pull/5255>`_
* Fixed a bug introduced when porting restoring an inconsistent snapshot feature from 7.0 branch to 6.3 branch. The parameter that controls whether to perform an inconsistent snapshot restore may instead be used to lock the database during restore. `(PR #5228) <https://github.com/apple/foundationdb/pull/5228>`_
* Added SidebandMultiThreadClientTest, which validates causal consistency for multi-threaded client. `(PR #5173) <https://github.com/apple/foundationdb/pull/5173>`_

6.3.17
======
* Made readValuePrefix consistent regarding error messages. `(PR #5160) <https://github.com/apple/foundationdb/pull/5160>`_
* Added ``TLogPopDetails`` trace event to tLog pop. `(PR #5134) <https://github.com/apple/foundationdb/pull/5134>`_
* Added ``CommitBatchingEmptyMessageRatio`` metric to track the ratio of empty messages to tlogs. `(PR #5087) <https://github.com/apple/foundationdb/pull/5087>`_
* Observability improvements in ProxyStats. `(PR #5046) <https://github.com/apple/foundationdb/pull/5046>`_
* Added ``RecoveryInternal`` and ``ProxyReplies`` trace events to recovery_transaction step in recovery. `(PR #5038) <https://github.com/apple/foundationdb/pull/5038>`_
* Multi-threaded client documentation improvements. `(PR #5033) <https://github.com/apple/foundationdb/pull/5033>`_
* Added ``ClusterControllerWorkerFailed`` trace event when a worker is removed from cluster controller. `(PR #5035) <https://github.com/apple/foundationdb/pull/5035>`_
* Added histograms for storage server write path components. `(PR #5019) <https://github.com/apple/foundationdb/pull/5019>`_

6.3.15
======
* Added several counters to the ``MasterMetrics`` trace event to count the number of requests of each type received. `(PR #4829) <https://github.com/apple/foundationdb/pull/4829>`_
* Added ``RecoveryCount`` to trace events when a transaction log begins. `(PR #4944) <https://github.com/apple/foundationdb/pull/4944>`_
* Added metrics to compare the bandwidth used by data distributions and updates. `(PR #4907) <https://github.com/apple/foundationdb/pull/4907>`_
* Batch transactions could be throttled with an error when the latency between ratekeeper and some proxies was high. `(PR #4932) <https://github.com/apple/foundationdb/pull/4932>`_
* Fix accounting issue that could cause higher priority GRV requests to be rejected after many batch priority requests have been rejected on a proxy. `(PR #4932) <https://github.com/apple/foundationdb/pull/4932>`_

6.3.14
======
* Fixed fdbbackup start command that automatically configures database with backup workers to only do so when using partitioned logs. `(PR #4863) <https://github.com/apple/foundationdb/pull/4863>`_
* Added ``cluster.bounce_impact`` section to status to report if there will be any extra effects when bouncing the cluster, and if so, the reason for those effects. `(PR #4770) <https://github.com/apple/foundationdb/pull/4770>`_
* Added ``fetched_versions`` to the storage metrics section of status to report how fast a storage server is catching up in versions. `(PR #4770) <https://github.com/apple/foundationdb/pull/4770>`_
* Added ``fetches_from_logs`` to the storage metrics section of status to report how frequently a storage server fetches updates from transaction logs. `(PR #4770) <https://github.com/apple/foundationdb/pull/4770>`_
* Added the ``bypass_unreadable`` transaction option which allows ``get`` operations to read from sections of keyspace that have become unreadable because of versionstamp operations. `(PR #4774) <https://github.com/apple/foundationdb/pull/4774>`_
* Fix several packaging issues. The osx package should now install successfully, and the structure of the RPM and DEB packages should match that of 6.2. `(PR #4810) <https://github.com/apple/foundationdb/pull/4810>`_
* Fix an accounting error that could potentially result in inaccuracies in priority busyness metrics. `(PR #4824) <https://github.com/apple/foundationdb/pull/4824>`_

6.3.13
======
* Added ``commit_batching_window_size`` to the proxy roles section of status to record statistics about commit batching window size on each proxy. `(PR #4736) <https://github.com/apple/foundationdb/pull/4736>`_
* The multi-version client now requires at most two client connections with version 6.2 or larger, regardless of how many external clients are configured. Clients older than 6.2 will continue to create an additional connection each. `(PR #4667) <https://github.com/apple/foundationdb/pull/4667>`_

6.3.12
======
* Change the default for --knob_tls_server_handshake_threads to 64. The previous was 1000. This avoids starting 1000 threads by default, but may adversely affect recovery time for large clusters using tls. Users with large tls clusters should consider explicitly setting this knob in their foundationdb.conf file. `(PR #4421) <https://github.com/apple/foundationdb/pull/4421>`_
* Fix accounting error that could cause commits to incorrectly fail with ``proxy_memory_limit_exceeded``. `(PR #4526) <https://github.com/apple/foundationdb/pull/4526>`_
* As an optimization, partial restore using target key ranges now filters backup log data prior to loading it into the database.  `(PR #4554) <https://github.com/apple/foundationdb/pull/4554>`_
* Fix fault tolerance calculation when there are no tLogs in LogSet.  `(PR #4454) <https://github.com/apple/foundationdb/pull/4454>`_
* Change client's ``iteration_progression`` size defaults from 256 to 4096 bytes for better performance. `(PR #4416) <https://github.com/apple/foundationdb/pull/4416>`_
* Add the ability to instrument java driver actions, such as ``FDBTransaction`` and ``RangeQuery``. `(PR #4385) <https://github.com/apple/foundationdb/pull/4385>`_

6.3.11
======

* Support multiple worker threads for each client version that is loaded. `(PR #4269) <https://github.com/apple/foundationdb/pull/4269>`_  
* fdbcli: Output errors and warnings to stderr. `(PR #4332) <https://github.com/apple/foundationdb/pull/4332>`_  
* Do not rely on shared memory to generate a machine id if it is set explicitly. `(Issue #4022) <https://github.com/apple/foundationdb/pull/4022>`_  
* Added ``workload.transactions.rejected_for_queued_too_long`` to status to report the number of transaction commits that failed because they were queued too long and could no longer be checked for conflicts. `(PR #4353) <https://github.com/apple/foundationdb/pull/4353>`_
* Add knobs for prefix bloom filters and larger block cache for RocksDB. `(PR #4201) <https://github.com/apple/foundationdb/pull/4201>`_ 
* Add option to prevent synchronous file deletes on reads for RocksDB. `(PR #4270) <https://github.com/apple/foundationdb/pull/4270>`_  
* Build on Windows using VS 2019 + LLVM/Clang. `(PR #4258) <https://github.com/apple/foundationdb/pull/4258>`_ 

6.3.10
======

* Make fault tolerance metric calculation in HA clusters consistent with 6.2 branch. `(PR #4175) <https://github.com/apple/foundationdb/pull/4175>`_
* Bug fix, stack overflow in redwood storage engine. `(PR #4161) <https://github.com/apple/foundationdb/pull/4161>`_
* Bug fix, getting certain special keys fail. `(PR #4128) <https://github.com/apple/foundationdb/pull/4128>`_ 
* Prevent slow task on TLog by yielding while processing ignored pop requests. `(PR #4112) <https://github.com/apple/foundationdb/pull/4112>`_
* Support reading xxhash3 sqlite checksums. `(PR #4104) <https://github.com/apple/foundationdb/pull/4104>`_
* Fix a race between submit and abort backup. `(PR #3935) <https://github.com/apple/foundationdb/pull/3935>`_

Packaging
---------

* Create versioned RPM and DEB packages. This will allow users to install multiple versions of FoundationDB on the same machine and use alternatives to switch between versions. `(PR #3983) <https://github.com/apple/foundationdb/pull/3983>`_
* Remove support for RHEL 6 and CentOS 6. This version reached EOL and is not anymore officially supported by FoundationDB. `(PR #3983) <https://github.com/apple/foundationdb/pull/3983>`_

6.3.9
=====

Features
--------

* Added the ability to set arbitrary tags on transactions. Tags can be specifically throttled using ``fdbcli``, and certain types of tags can be automatically throttled by ratekeeper. `(PR #2942) <https://github.com/apple/foundationdb/pull/2942>`_
* Add an option for transactions to report conflicting keys by calling ``getRange`` with the special key prefix ``\xff\xff/transaction/conflicting_keys/``. `(PR 2257) <https://github.com/apple/foundationdb/pull/2257>`_
* Added the ``exclude failed`` command to ``fdbcli``. This command designates that a process is dead and will never come back, so the transaction logs can forget about mutations sent to that process. `(PR #1955) <https://github.com/apple/foundationdb/pull/1955>`_
* A new fast restore system that can restore a database to a point in time from backup files. It is a Spark-like parallel processing framework that processes backup data asynchronously, in parallel and in pipeline. `(Fast Restore Project) <https://github.com/apple/foundationdb/projects/7>`_
* Added backup workers for pulling mutations from transaction logs and uploading them to blob storage. Switching from the previous backup implementation will double a cluster's maximum write bandwidth. `(PR #1625) <https://github.com/apple/foundationdb/pull/1625>`_ `(PR #2588) <https://github.com/apple/foundationdb/pull/2588>`_ `(PR #2642) <https://github.com/apple/foundationdb/pull/2642>`_ 
* Added a new API in all bindings that can be used to query the estimated byte size of a given range. `(PR #2537) <https://github.com/apple/foundationdb/pull/2537>`_
* Added the ``lock`` and ``unlock`` commands to ``fdbcli`` which lock or unlock a cluster. `(PR #2890) <https://github.com/apple/foundationdb/pull/2890>`_
* Add a framework which helps to add client functions using special keys (keys within ``[\xff\xff, \xff\xff\xff)``). `(PR #2662) <https://github.com/apple/foundationdb/pull/2662>`_
* Added capability of aborting replication to a clone of DR site without affecting replication to the original dr site with ``--dstonly`` option of ``fdbdr abort``. `(PR 3457) <https://github.com/apple/foundationdb/pull/3457>`_

Performance
-----------

* Improved the client's load balancing algorithm so that each proxy processes an equal number of requests. `(PR #2520) <https://github.com/apple/foundationdb/pull/2520>`_
* Significantly reduced the amount of work done on the cluster controller by removing the centralized failure monitoring. `(PR #2518) <https://github.com/apple/foundationdb/pull/2518>`_
* Improved master recovery speeds by more efficiently broadcasting the recovery state between processes.  `(PR #2941) <https://github.com/apple/foundationdb/pull/2941>`_
* Significantly reduced the number of network connections opened to the coordinators. `(PR #3069) <https://github.com/apple/foundationdb/pull/3069>`_
* Improve GRV tail latencies, particularly as the transaction rate gets nearer the ratekeeper limit. `(PR #2735) <https://github.com/apple/foundationdb/pull/2735>`_
* The proxies are now more responsive to changes in workload when unthrottling lower priority transactions. `(PR #2735) <https://github.com/apple/foundationdb/pull/2735>`_
* Removed a lot of unnecessary copying across the codebase. `(PR #2986) <https://github.com/apple/foundationdb/pull/2986>`_ `(PR #2915) <https://github.com/apple/foundationdb/pull/2915>`_ `(PR #3024) <https://github.com/apple/foundationdb/pull/3024>`_ `(PR #2999) <https://github.com/apple/foundationdb/pull/2999>`_
* Optimized the performance of the storage server. `(PR #1988) <https://github.com/apple/foundationdb/pull/1988>`_ `(PR #3103) <https://github.com/apple/foundationdb/pull/3103>`_
* Optimized the performance of the resolver. `(PR #2648) <https://github.com/apple/foundationdb/pull/2648>`_ 
* Replaced most uses of hashlittle2 with crc32 for better performance.  `(PR #2538) <https://github.com/apple/foundationdb/pull/2538>`_
* Significantly reduced the serialized size of conflict ranges and single key clears. `(PR #2513) <https://github.com/apple/foundationdb/pull/2513>`_
* Improved range read performance when the reads overlap recently cleared key ranges. `(PR #2028) <https://github.com/apple/foundationdb/pull/2028>`_
* Reduced the number of comparisons used by various map implementations. `(PR #2882) <https://github.com/apple/foundationdb/pull/2882>`_
* Reduced the serialized size of empty strings. `(PR #3063) <https://github.com/apple/foundationdb/pull/3063>`_
* Reduced the serialized size of various interfaces by 10x. `(PR #3068) <https://github.com/apple/foundationdb/pull/3068>`_
* TLS handshakes can now be done in a background thread pool. `(PR #3403) <https://github.com/apple/foundationdb/pull/3403>`_

Reliability
-----------

* Connections that disconnect frequently are not immediately marked available. `(PR #2932) <https://github.com/apple/foundationdb/pull/2932>`_
* The data distributor will consider storage servers that are continually lagging behind as if they were failed. `(PR #2917) <https://github.com/apple/foundationdb/pull/2917>`_
* Changing the storage engine type of a cluster will no longer cause the cluster to run out of memory. Instead, the cluster will gracefully migrate storage server processes to the new storage engine one by one. `(PR #1985) <https://github.com/apple/foundationdb/pull/1985>`_
* Batch priority transactions which are being throttled by ratekeeper will get a ``batch_transaction_throttled`` error instead of hanging indefinitely.  `(PR #1868) <https://github.com/apple/foundationdb/pull/1868>`_
* Avoid using too much memory on the transaction logs when multiple types of transaction logs exist in the same process. `(PR #2213) <https://github.com/apple/foundationdb/pull/2213>`_

Fixes
-----

* The ``SetVersionstampedKey`` atomic operation no longer conflicts with versions smaller than the current read version of the transaction. `(PR #2557) <https://github.com/apple/foundationdb/pull/2557>`_
* Ratekeeper would measure durability lag a few seconds higher than reality. `(PR #2499) <https://github.com/apple/foundationdb/pull/2499>`_
* In very rare scenarios, the data distributor process could get stuck in an infinite loop. `(PR #2228) <https://github.com/apple/foundationdb/pull/2228>`_
* If the number of configured transaction logs were reduced at the exact same time a change to the system keyspace took place, it was possible for the transaction state store to become corrupted. `(PR #3051) <https://github.com/apple/foundationdb/pull/3051>`_
* Fix multiple data races between threads on the client. `(PR #3026) <https://github.com/apple/foundationdb/pull/3026>`_
* Transaction logs configured to spill by reference had an unintended delay between each spilled batch. `(PR #3153) <https://github.com/apple/foundationdb/pull/3153>`_
* Added guards to honor ``DISABLE_POSIX_KERNEL_AIO``. `(PR #2888) <https://github.com/apple/foundationdb/pull/2888>`_
* Prevent blob upload timeout if request timeout is lower than expected request time. `(PR #3533) <https://github.com/apple/foundationdb/pull/3533>`_
* In very rare scenarios, the data distributor process would crash when being shutdown. `(PR #3530) <https://github.com/apple/foundationdb/pull/3530>`_
* The master would die immediately if it did not have the correct cluster controller interface when recruited. [6.3.4] `(PR #3537) <https://github.com/apple/foundationdb/pull/3537>`_
* Fix an issue where ``fdbcli --exec 'exclude no_wait ...'`` would incorrectly report that processes can safely be removed from the cluster. [6.3.5] `(PR #3566) <https://github.com/apple/foundationdb/pull/3566>`_
* Commit latencies could become large because of inaccurate compute estimates. [6.3.9] `(PR #3845) <https://github.com/apple/foundationdb/pull/3845>`_
* Added a timeout on TLS handshakes to prevent them from hanging indefinitely. [6.3.9] `(PR #3850) <https://github.com/apple/foundationdb/pull/3850>`_
* Bug fix, blob client did not support authentication key sizes over 64 bytes.  `(PR #3964) <https://github.com/apple/foundationdb/pull/3964>`_

Status
------

* A process's ``memory.available_bytes`` can no longer exceed the memory limit of the process. For purposes of this statistic, processes on the same machine will be allocated memory proportionally based on the size of their memory limits. `(PR #3174) <https://github.com/apple/foundationdb/pull/3174>`_
* Replaced ``cluster.database_locked`` status field with ``cluster.database_lock_state``, which contains two subfields: ``locked`` (boolean) and ``lock_uid`` (which contains the database lock uid if the database is locked). `(PR #2058) <https://github.com/apple/foundationdb/pull/2058>`_
* Removed fields ``worst_version_lag_storage_server`` and ``limiting_version_lag_storage_server`` from the ``cluster.qos`` section. The ``worst_data_lag_storage_server`` and ``limiting_data_lag_storage_server`` objects can be used instead. `(PR #3196) <https://github.com/apple/foundationdb/pull/3196>`_
* If a process is unable to flush trace logs to disk, the problem will now be reported via the output of ``status`` command inside ``fdbcli``. `(PR #2605) <https://github.com/apple/foundationdb/pull/2605>`_ `(PR #2820) <https://github.com/apple/foundationdb/pull/2820>`_
* When a configuration key is changed, it will always be included in ``status json`` output, even the value is reverted back to the default value. [6.3.5] `(PR #3610) <https://github.com/apple/foundationdb/pull/3610>`_
* Added transactions.rejected_for_queued_too_long for bookkeeping the number of transactions rejected by commit proxy because its queuing time exceeds MVCC window.[6.3.11] `(PR #4353) <https://github.com/apple/foundationdb/pull/4353>`_

Bindings
--------

* API version updated to 630. See the :ref:`API version upgrade guide <api-version-upgrade-guide-630>` for upgrade details.
* Python: The ``@fdb.transactional`` decorator will now throw an error if the decorated function returns a generator. `(PR #1724) <https://github.com/apple/foundationdb/pull/1724>`_
* Java: Add caching for various JNI objects to improve performance. `(PR #2809) <https://github.com/apple/foundationdb/pull/2809>`_
* Java: Optimize byte array comparisons in ``ByteArrayUtil``. `(PR #2823) <https://github.com/apple/foundationdb/pull/2823>`_
* Java: Add ``FDB.disableShutdownHook`` that can be used to prevent the default shutdown hook from running. Users of this new function should make sure to call ``stopNetwork`` before terminating a client process. `(PR #2635) <https://github.com/apple/foundationdb/pull/2635>`_
* Java: Introduced ``keyAfter`` utility function that can be used to create the immediate next key for a given byte array. `(PR #2458) <https://github.com/apple/foundationdb/pull/2458>`_
* Java:  Combined ``getSummary()`` and ``getResults()`` JNI calls for ``getRange()`` queries. [6.3.5] `(PR #3681) <https://github.com/apple/foundationdb/pull/3681>`_
* Java:  Added support to use ``DirectByteBuffers`` in ``getRange()`` requests for better performance, which can be enabled using ``FDB.enableDirectBufferQueries``. [6.3.5] `(PR #3681) <https://github.com/apple/foundationdb/pull/3681>`_
* Golang: The ``Transact`` function will unwrap errors that have been wrapped using ``xerrors`` to determine if a retryable FoundationDB error is in the error chain. `(PR #3131) <https://github.com/apple/foundationdb/pull/3131>`_
* Golang: Added ``Subspace.PackWithVersionstamp`` that can be used to pack a ``Tuple`` that contains a versionstamp. `(PR #2243) <https://github.com/apple/foundationdb/pull/2243>`_
* Golang: Implement ``Stringer`` interface for ``Tuple``, ``Subspace``, ``UUID``, and ``Versionstamp``. `(PR #3032) <https://github.com/apple/foundationdb/pull/3032>`_
* C: The ``FDBKeyValue`` struct's ``key`` and ``value`` members have changed type from ``void*`` to ``uint8_t*``. `(PR #2622) <https://github.com/apple/foundationdb/pull/2622>`_
* Deprecated ``enable_slow_task_profiling`` network option and replaced it with ``enable_run_loop_profiling``. `(PR #2608) <https://github.com/apple/foundationdb/pull/2608>`_

Other Changes
-------------

* Small key ranges which are being heavily read will be reported in the logs using the trace event ``ReadHotRangeLog``. `(PR #2046) <https://github.com/apple/foundationdb/pull/2046>`_ `(PR #2378) <https://github.com/apple/foundationdb/pull/2378>`_ `(PR #2532) <https://github.com/apple/foundationdb/pull/2532>`_
* Added the read version, commit version, and datacenter locality to the client transaction information.  `(PR #3079) <https://github.com/apple/foundationdb/pull/3079>`_  `(PR #3205) <https://github.com/apple/foundationdb/pull/3205>`_
* Added a network option ``TRACE_FILE_IDENTIFIER`` that can be used to provide a custom identifier string that will be part of the file name for all trace log files created on the client. `(PR #2869) <https://github.com/apple/foundationdb/pull/2869>`_
* It is now possible to use the ``TRACE_LOG_GROUP`` option on a client process after the database has been created. `(PR #2862) <https://github.com/apple/foundationdb/pull/2862>`_
* Added a network option ``TRACE_CLOCK_SOURCE`` that can be used to switch the trace event timestamps to use a realtime clock source. `(PR #2329) <https://github.com/apple/foundationdb/pull/2329>`_
* The ``INCLUDE_PORT_IN_ADDRESS`` transaction option is now on by default. This means ``get_addresses_for_key`` will always return ports in the address strings. `(PR #2639) <https://github.com/apple/foundationdb/pull/2639>`_
* Added the ``getversion`` command to ``fdbcli`` which returns the current read version of the cluster.  `(PR #2882) <https://github.com/apple/foundationdb/pull/2882>`_
* Added the ``advanceversion`` command to ``fdbcli`` which increases the current version of a cluster.  `(PR #2965) <https://github.com/apple/foundationdb/pull/2965>`_
* Improved the slow task profiler to also report backtraces for periods when the run loop is saturated. `(PR #2608) <https://github.com/apple/foundationdb/pull/2608>`_
* Double the number of shard locations that the client will cache locally. `(PR #2198) <https://github.com/apple/foundationdb/pull/2198>`_
* Replaced the ``-add_prefix`` and ``-remove_prefix`` options with ``--add_prefix`` and ``--remove_prefix`` in ``fdbrestore`` `(PR 3206) <https://github.com/apple/foundationdb/pull/3206>`_
* Data distribution metrics can now be read using the special keyspace ``\xff\xff/metrics/data_distribution_stats``. `(PR #2547) <https://github.com/apple/foundationdb/pull/2547>`_
* The ``\xff\xff/worker_interfaces/`` keyspace now begins at a key which includes a trailing ``/`` (previously ``\xff\xff/worker_interfaces``). Range reads to this range now respect the end key passed into the range and include the keyspace prefix in the resulting keys. `(PR #3095) <https://github.com/apple/foundationdb/pull/3095>`_
* Added FreeBSD support. `(PR #2634) <https://github.com/apple/foundationdb/pull/2634>`_
* Updated boost to 1.72.  `(PR #2684) <https://github.com/apple/foundationdb/pull/2684>`_
* Calling ``fdb_run_network`` multiple times in a single run of a client program now returns an error instead of causing undefined behavior. [6.3.1] `(PR #3229) <https://github.com/apple/foundationdb/pull/3229>`_
* Blob backup URL parameter ``request_timeout`` changed to ``request_timeout_min``, with prior name still supported. `(PR #3533) <https://github.com/apple/foundationdb/pull/3533>`_
* Support query command in backup CLI that allows users to query restorable files by key ranges. [6.3.6] `(PR #3703) <https://github.com/apple/foundationdb/pull/3703>`_
* Report missing old tlogs information when in recovery before storage servers are fully recovered. [6.3.6] `(PR #3706) <https://github.com/apple/foundationdb/pull/3706>`_
* Updated OpenSSL to version 1.1.1h. [6.3.7] `(PR #3809) <https://github.com/apple/foundationdb/pull/3809>`_
* Lowered the amount of time a watch will remain registered on a storage server from 900 seconds to 30 seconds. [6.3.8] `(PR #3833) <https://github.com/apple/foundationdb/pull/3833>`_

Fixes from previous versions
----------------------------

* The 6.3.1 patch release includes all fixes from the patch releases 6.2.21 and 6.2.22. :doc:`(6.2 Release Notes) </release-notes/release-notes-620>`
* The 6.3.3 patch release includes all fixes from the patch release 6.2.23. :doc:`(6.2 Release Notes) </release-notes/release-notes-620>`
* The 6.3.5 patch release includes all fixes from the patch releases 6.2.24 and 6.2.25. :doc:`(6.2 Release Notes) </release-notes/release-notes-620>`
* The 6.3.9 patch release includes all fixes from the patch releases 6.2.26. :doc:`(6.2 Release Notes) </release-notes/release-notes-620>`
* The 6.3.10 patch release includes all fixes from the patch releases 6.2.27-6.2.29 :doc:`(6.2 Release Notes) </release-notes/release-notes-620>`
* The 6.3.11 patch release includes all fixes from the patch releases 6.2.30-6.2.32 :doc:`(6.2 Release Notes) </release-notes/release-notes-620>`

Fixes only impacting 6.3.0+
---------------------------

* Clients did not probably balance requests to the proxies. [6.3.3] `(PR #3377) <https://github.com/apple/foundationdb/pull/3377>`_
* Renamed ``MIN_DELAY_STORAGE_CANDIDACY_SECONDS`` knob to ``MIN_DELAY_CC_WORST_FIT_CANDIDACY_SECONDS``. [6.3.2] `(PR #3327) <https://github.com/apple/foundationdb/pull/3327>`_
* Refreshing TLS certificates could cause crashes. [6.3.2] `(PR #3352) <https://github.com/apple/foundationdb/pull/3352>`_
* All storage class processes attempted to connect to the same coordinator. [6.3.2] `(PR #3361) <https://github.com/apple/foundationdb/pull/3361>`_
* Adjusted the proxy load balancing algorithm to be based on the CPU usage of the process instead of the number of requests processed. [6.3.5] `(PR #3653) <https://github.com/apple/foundationdb/pull/3653>`_
* Only return the error code ``batch_transaction_throttled`` for API versions greater than or equal to 630. [6.3.6] `(PR #3799) <https://github.com/apple/foundationdb/pull/3799>`_
* The fault tolerance calculation in status did not take into account region configurations. [6.3.8] `(PR #3836) <https://github.com/apple/foundationdb/pull/3836>`_
* Get read version tail latencies were high because some proxies were serving more read versions than other proxies. [6.3.9] `(PR #3845) <https://github.com/apple/foundationdb/pull/3845>`_

Earlier release notes
---------------------
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
