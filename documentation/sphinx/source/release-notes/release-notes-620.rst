#############
Release Notes
#############

6.2.33
======
* Fixed an issue where storage servers could shutdown with ``unknown_error``. `(PR #4437) <https://github.com/apple/foundationdb/pull/4437>`_
* Fix backup agent stall when writing to local filesystem with slow metadata operations. `(PR #4428) <https://github.com/apple/foundationdb/pull/4428>`_
* Backup agent no longer uses 4k block caching layer on local output files so that write operations are larger. `(PR #4428) <https://github.com/apple/foundationdb/pull/4428>`_
* Fix accounting error that could cause commits to incorrectly fail with ``proxy_memory_limit_exceeded``. `(PR #4529) <https://github.com/apple/foundationdb/pull/4529>`_
* Added support for downgrades from FDB version 6.3. For more details, see the :ref:`administration notes <downgrade-specific-version>`. `(PR #4673) <https://github.com/apple/foundationdb/pull/4673>`_ `(PR #4469) <https://github.com/apple/foundationdb/pull/4469>`_

6.2.32
======
* Fix an issue where symbolic links in cmake-built RPMs are broken if you unpack the RPM to a custom directory. `(PR #4380) <https://github.com/apple/foundationdb/pull/4380>`_

6.2.31
======
* Fix a rare invalid memory access on data distributor when snapshotting large clusters. This is a follow up to `PR #4076 <https://github.com/apple/foundationdb/pull/4076>`_. `(PR #4317) <https://github.com/apple/foundationdb/pull/4317>`_

6.2.30
======
* A storage server which has fallen behind will deprioritize reads in order to catch up. This change causes some saturating workloads to experience high read latencies instead of high GRV latencies. `(PR #4218) <https://github.com/apple/foundationdb/pull/4218>`_
* Added ``low_priority_queries`` to the ``processes.roles`` section of status to record the number of deprioritized reads on each storage server. `(PR #4218) <https://github.com/apple/foundationdb/pull/4218>`_
* Added ``low_priority_reads`` to the ``workload.operations`` section of status to record the total number of deprioritized reads. `(PR #4218) <https://github.com/apple/foundationdb/pull/4218>`_
* Backup to locally mounted filesystems now appends to files in large block writes, 1MB each by default. `(PR #4199) <https://github.com/apple/foundationdb/pull/4199>`_
* Changed the default SSL implementation from OpenSSL to BoringSSL `(PR #4153) <https://github.com/apple/foundationdb/pull/4153>`_
* SQLite now supports configurable disk write rate limiting. `(PR #4259) <https://github.com/apple/foundationdb/pull/4259>`_
* If a disk operation takes more than two minutes, the system will treat the disk as failed. `(PR #4243) <https://github.com/apple/foundationdb/pull/4243>`_

6.2.29
======
* Fix invalid memory access on data distributor when snapshotting large clusters. `(PR #4076) <https://github.com/apple/foundationdb/pull/4076>`_
* Add human-readable DateTime to trace events `(PR #4087) <https://github.com/apple/foundationdb/pull/4087>`_
* Proxy rejects transaction batch that exceeds MVCC window `(PR #4113) <https://github.com/apple/foundationdb/pull/4113>`_
* Add a command in fdbcli to manually trigger the detailed teams information loggings in data distribution. `(PR #4060) <https://github.com/apple/foundationdb/pull/4060>`_
* Add documentation on read and write Path. `(PR #4099) <https://github.com/apple/foundationdb/pull/4099>`_
* Add a histogram to expose commit batching window on Proxies. `(PR #4166) <https://github.com/apple/foundationdb/pull/4166>`_
* Fix double counting of range reads in TransactionMetrics. `(PR #4130) <https://github.com/apple/foundationdb/pull/4130>`_
* Add a trace event that can be used as an indicator of the load on the proxy. `(PR #4166) <https://github.com/apple/foundationdb/pull/4166>`_

6.2.28
======
* Log detailed team collection information when median available space ratio of all teams is too low. `(PR #3912) <https://github.com/apple/foundationdb/pull/3912>`_
* Bug fix, blob client did not support authentication key sizes over 64 bytes.  `(PR #3964) <https://github.com/apple/foundationdb/pull/3964>`_

6.2.27
======
* For clusters with a large number of shards, avoid slow tasks in the data distributor by adding yields to the shard map destruction. `(PR #3834) <https://github.com/apple/foundationdb/pull/3834>`_
* Reset the network connection between a proxy and master or resolvers if the proxy is too far behind in processing transactions. `(PR #3891) <https://github.com/apple/foundationdb/pull/3891>`_

6.2.26
======

* Fixed undefined behavior in configuring supported FoundationDB versions while starting up a client. `(PR #3849) <https://github.com/apple/foundationdb/pull/3849>`_
* Updated OpenSSL to version 1.1.1h. `(PR #3809) <https://github.com/apple/foundationdb/pull/3809>`_
* Attempt to detect when calling :func:`fdb_future_block_until_ready` would cause a deadlock, and throw ``blocked_from_network_thread`` if it would definitely cause a deadlock. `(PR #3786) <https://github.com/apple/foundationdb/pull/3786>`_

6.2.25
======

* Mitigate an issue where a non-lockaware transaction that changes certain ``\xff`` "metadata" keys, committed concurrently with locking the database, can cause corruption. If a non-lockaware transaction manually sets its read version to a version where the database is locked, and changes metadata keys, this can still cause corruption. `(PR #3674) <https://github.com/apple/foundationdb/pull/3674>`_
* Reset network connections between the proxies and satellite tlogs if the latencies are larger than 500ms. `(PR #3686) <https://github.com/apple/foundationdb/pull/3686>`_

6.2.24
======

* Added the ``suspend`` command to ``fdbcli`` which kills a process and prevents it from rejoining the cluster for a specified duration. `(PR #3550) <https://github.com/apple/foundationdb/pull/3550>`_

6.2.23
======

* When configured with ``usable_regions=2`` data distribution could temporarily lower the replication of a shard when moving it. `(PR #3487) <https://github.com/apple/foundationdb/pull/3487>`_
* Prevent data distribution from running out of memory by fetching the source servers for too many shards in parallel. `(PR #3487) <https://github.com/apple/foundationdb/pull/3487>`_
* Reset network connections between log routers and satellite tlogs if the latencies are larger than 500ms. `(PR #3487) <https://github.com/apple/foundationdb/pull/3487>`_
* Added per-process server request latency statistics reported in the role section of relevant processes. These are named ``grv_latency_statistics`` and ``commit_latency_statistics`` on proxy roles and ``read_latency_statistics`` on storage roles. `(PR #3480) <https://github.com/apple/foundationdb/pull/3480>`_
* Added ``cluster.active_primary_dc`` that indicates which datacenter is serving as the primary datacenter in multi-region setups. `(PR #3320) <https://github.com/apple/foundationdb/pull/3320>`_

6.2.22
======

* Coordinator class processes could be recruited as the cluster controller. `(PR #3282) <https://github.com/apple/foundationdb/pull/3282>`_
* HTTPS requests made by backup would fail (introduced in 6.2.21). `(PR #3284) <https://github.com/apple/foundationdb/pull/3284>`_

6.2.21
======

* HTTPS requests made by backup could hang indefinitely. `(PR #3027) <https://github.com/apple/foundationdb/pull/3027>`_
* ``fdbrestore`` prefix options required exactly a single hyphen instead of the standard two. `(PR #3056) <https://github.com/apple/foundationdb/pull/3056>`_
* Commits could stall on a newly elected proxy because of inaccurate compute estimates. `(PR #3123) <https://github.com/apple/foundationdb/pull/3123>`_
* A transaction class process with a bad disk could be repeatedly recruited as a transaction log. `(PR #3268) <https://github.com/apple/foundationdb/pull/3268>`_
* Fix a potential race condition that could lead to undefined behavior when connecting to a database using the multi-version client API. `(PR #3265) <https://github.com/apple/foundationdb/pull/3265>`_
* Added the ``getversion`` command to ``fdbcli`` which returns the current read version of the cluster.  `(PR #2882) <https://github.com/apple/foundationdb/pull/2882>`_
* Added the ``advanceversion`` command to ``fdbcli`` which increases the current version of a cluster.  `(PR #2965) <https://github.com/apple/foundationdb/pull/2965>`_
* Added the ``lock`` and ``unlock`` commands to ``fdbcli`` which lock or unlock a cluster. `(PR #2890) <https://github.com/apple/foundationdb/pull/2890>`_

6.2.20
======

* In rare scenarios, clients could send corrupted data to the server. `(PR #2976) <https://github.com/apple/foundationdb/pull/2976>`_
* Internal tools like ``fdbbackup`` are no longer tracked as clients in status (introduced in 6.2.18) `(PR #2849) <https://github.com/apple/foundationdb/pull/2849>`_
* Changed TLS error handling to match the behavior of 6.2.15. `(PR #2993) <https://github.com/apple/foundationdb/pull/2993>`_ `(PR #2977) <https://github.com/apple/foundationdb/pull/2977>`_

6.2.19
======

* Protect the proxies from running out of memory when bombarded with requests from clients. `(PR #2812) <https://github.com/apple/foundationdb/pull/2812>`_.
* One process with a ``proxy`` class would not become the first proxy when put with other ``stateless`` class processes. `(PR #2819) <https://github.com/apple/foundationdb/pull/2819>`_.
* If a transaction log stalled on a disk operation during recruitment the cluster would become unavailable until the process died. `(PR #2815) <https://github.com/apple/foundationdb/pull/2815>`_.
* Avoid recruiting satellite transaction logs when ``usable_regions=1``. `(PR #2813) <https://github.com/apple/foundationdb/pull/2813>`_.
* Prevent the cluster from having too many active generations as a safety measure against repeated failures. `(PR #2814) <https://github.com/apple/foundationdb/pull/2814>`_.
* ``fdbcli`` status JSON could become truncated because of unprintable characters. `(PR #2807) <https://github.com/apple/foundationdb/pull/2807>`_.
* The data distributor used too much CPU in large clusters (broken in 6.2.16). `(PR #2806) <https://github.com/apple/foundationdb/pull/2806>`_.
* Added ``cluster.workload.operations.memory_errors`` to measure the number of requests rejected by the proxies because the memory limit has been exceeded. `(PR #2812) <https://github.com/apple/foundationdb/pull/2812>`_.
* Added ``cluster.workload.operations.location_requests`` to measure the number of outgoing key server location responses from the proxies. `(PR #2812) <https://github.com/apple/foundationdb/pull/2812>`_.
* Added ``cluster.recovery_state.active_generations`` to track the number of generations for which the cluster still requires transaction logs. `(PR #2814) <https://github.com/apple/foundationdb/pull/2814>`_.
* Added ``network.tls_policy_failures`` to the ``processes`` section to record the number of TLS policy failures each process has observed. `(PR #2811) <https://github.com/apple/foundationdb/pull/2811>`_.
* Added ``--debug-tls`` as a command line argument to ``fdbcli`` to help diagnose TLS issues. `(PR #2810) <https://github.com/apple/foundationdb/pull/2810>`_.

6.2.18
======

* When configuring a cluster to usable_regions=2, data distribution would not react to machine failures while copying data to the remote region. `(PR #2774) <https://github.com/apple/foundationdb/pull/2774>`_.
* When a cluster is configured with usable_regions=2, data distribution could push a cluster into saturation by relocating too many shards simulatenously. `(PR #2776) <https://github.com/apple/foundationdb/pull/2776>`_.
* Do not allow the cluster controller to mark any process as failed within 30 seconds of startup. `(PR #2780) <https://github.com/apple/foundationdb/pull/2780>`_.
* Backup could not establish TLS connections (broken in 6.2.16). `(PR #2775) <https://github.com/apple/foundationdb/pull/2775>`_.
* Certificates were not refreshed automatically (broken in 6.2.16). `(PR #2781) <https://github.com/apple/foundationdb/pull/2781>`_.
* Improved the efficiency of establishing large numbers of network connections. `(PR #2777) <https://github.com/apple/foundationdb/pull/2777>`_.
* Add support for setting knobs to modify the behavior of ``fdbcli``. `(PR #2773) <https://github.com/apple/foundationdb/pull/2773>`_.
* Setting invalid knobs in backup and DR binaries is now a warning instead of an error and will not result in the application being terminated. `(PR #2773) <https://github.com/apple/foundationdb/pull/2773>`_.

6.2.17
======

* Restored the ability to set TLS configuration using environment variables (broken in 6.2.16). `(PR #2755) <https://github.com/apple/foundationdb/pull/2755>`_.

6.2.16
======

* Reduced tail commit latencies by improving commit pipelining on the proxies. `(PR #2589) <https://github.com/apple/foundationdb/pull/2589>`_.
* Data distribution does a better job balancing data when disks are more than 70% full. `(PR #2722) <https://github.com/apple/foundationdb/pull/2722>`_.
* Reverse range reads could read too much data from disk, resulting in poor performance relative to forward range reads. `(PR #2650) <https://github.com/apple/foundationdb/pull/2650>`_.
* Switched from LibreSSL to OpenSSL to improve the speed of establishing connections. `(PR #2646) <https://github.com/apple/foundationdb/pull/2646>`_.
* The cluster controller does a better job avoiding multiple recoveries when first recruited. `(PR #2698) <https://github.com/apple/foundationdb/pull/2698>`_.
* Storage servers could fail to advance their version correctly in response to empty commits. `(PR #2617) <https://github.com/apple/foundationdb/pull/2617>`_.
* Status could not label more than 5 processes as proxies. `(PR #2653) <https://github.com/apple/foundationdb/pull/2653>`_.
* The ``TR_FLAG_DISABLE_MACHINE_TEAM_REMOVER``, ``TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS``, ``TR_FLAG_DISABLE_SERVER_TEAM_REMOVER``, and ``BUGGIFY_ALL_COORDINATION`` knobs could not be set at runtime. `(PR #2661) <https://github.com/apple/foundationdb/pull/2661>`_.
* Backup container filename parsing was unnecessarily consulting the local filesystem which will error when permission is denied. `(PR #2693) <https://github.com/apple/foundationdb/pull/2693>`_.
* Rebalancing data movement could stop doing work even though the data in the cluster was not well balanced. `(PR #2703) <https://github.com/apple/foundationdb/pull/2703>`_.
* Data movement uses available space rather than free space when deciding how full a process is. `(PR #2708) <https://github.com/apple/foundationdb/pull/2708>`_.
* Fetching status attempts to reuse its connection with the cluster controller. `(PR #2583) <https://github.com/apple/foundationdb/pull/2583>`_.

6.2.15
======

* TLS throttling could block legitimate connections. `(PR #2575) <https://github.com/apple/foundationdb/pull/2575>`_.

6.2.14
======

* Data distribution was prioritizing shard merges too highly. `(PR #2562) <https://github.com/apple/foundationdb/pull/2562>`_.
* Status would incorrectly mark clusters as having no fault tolerance. `(PR #2562) <https://github.com/apple/foundationdb/pull/2562>`_.
* A proxy could run out of memory if disconnected from the cluster for too long. `(PR #2562) <https://github.com/apple/foundationdb/pull/2562>`_.

6.2.13
======

* Optimized the commit path the proxies to significantly reduce commit latencies in large clusters. `(PR #2536) <https://github.com/apple/foundationdb/pull/2536>`_.
* Data distribution could create temporarily untrackable shards which could not be split if they became hot. `(PR #2546) <https://github.com/apple/foundationdb/pull/2546>`_.

6.2.12
======

* Throttle TLS connect attempts from misconfigured clients. `(PR #2529) <https://github.com/apple/foundationdb/pull/2529>`_.
* Reduced master recovery times in large clusters. `(PR #2430) <https://github.com/apple/foundationdb/pull/2430>`_.
* Improved performance while a remote region is catching up. `(PR #2527) <https://github.com/apple/foundationdb/pull/2527>`_.
* The data distribution algorithm does a better job preventing hot shards while recovering from machine failures. `(PR #2526) <https://github.com/apple/foundationdb/pull/2526>`_.
* Improve the reliability of a ``kill`` command from ``fdbcli``. `(PR #2512) <https://github.com/apple/foundationdb/pull/2512>`_.
* The ``--traceclock`` parameter to fdbserver incorrectly had no effect. `(PR #2420) <https://github.com/apple/foundationdb/pull/2420>`_.
* Clients could throw an internal error during ``commit`` if client buggification was enabled. `(PR #2427) <https://github.com/apple/foundationdb/pull/2427>`_.
* Backup and DR agent transactions which update and clean up status had an unnecessarily high conflict rate. `(PR #2483) <https://github.com/apple/foundationdb/pull/2483>`_.
* The slow task profiler used an unsafe call to get a timestamp in its signal handler that could lead to rare crashes. `(PR #2515) <https://github.com/apple/foundationdb/pull/2515>`_.

6.2.11
======

* Clients could hang indefinitely on reads if all storage servers holding a keyrange were removed from a cluster since the last time the client read a key in the range. `(PR #2377) <https://github.com/apple/foundationdb/pull/2377>`_.
* In rare scenarios, status could falsely report no replicas remain of some data. `(PR #2380) <https://github.com/apple/foundationdb/pull/2380>`_.
* Latency band tracking could fail to configure correctly after a recovery or upon process startup. `(PR #2371) <https://github.com/apple/foundationdb/pull/2371>`_.

6.2.10
======

* ``backup_agent`` crashed on startup. `(PR #2356) <https://github.com/apple/foundationdb/pull/2356>`_.

6.2.9
=====

* Small clusters using specific sets of process classes could cause the data distributor to be continuously killed and re-recruited. `(PR #2344) <https://github.com/apple/foundationdb/pull/2344>`_.
* The data distributor and ratekeeper could be recruited on non-optimal processes. `(PR #2344) <https://github.com/apple/foundationdb/pull/2344>`_.
* A ``kill`` command from ``fdbcli`` could take a long time before being executed by a busy process. `(PR #2339) <https://github.com/apple/foundationdb/pull/2339>`_.
* Committing transactions larger than 1 MB could cause the proxy to stall for up to a second. `(PR #2350) <https://github.com/apple/foundationdb/pull/2350>`_.
* Transaction timeouts would use memory for the entire duration of the timeout, regardless of whether the transaction had been destroyed. `(PR #2353) <https://github.com/apple/foundationdb/pull/2353>`_.

6.2.8
=====

* Significantly improved the rate at which the transaction logs in a remote region can pull data from the primary region. `(PR #2307) <https://github.com/apple/foundationdb/pull/2307>`_ `(PR #2323) <https://github.com/apple/foundationdb/pull/2323>`_.
* The ``system_kv_size_bytes`` status field could report a size much larger than the actual size of the system keyspace. `(PR #2305) <https://github.com/apple/foundationdb/pull/2305>`_.

6.2.7
=====

Performance
-----------

* A new transaction log spilling implementation is now the default.  Write bandwidth and latency will no longer degrade during storage server or remote region failures. `(PR #1731) <https://github.com/apple/foundationdb/pull/1731>`_.
* Storage servers will locally throttle incoming read traffic when they are falling behind. `(PR #1447) <https://github.com/apple/foundationdb/pull/1477>`_.
* Use CRC32 checksum for SQLite pages. `(PR #1582) <https://github.com/apple/foundationdb/pull/1582>`_.
* Added a 96-byte fast allocator, so storage queue nodes use less memory. `(PR #1336) <https://github.com/apple/foundationdb/pull/1336>`_.
* Improved network performance when sending large packets. `(PR #1684) <https://github.com/apple/foundationdb/pull/1684>`_.
* Spilled data can be consumed from transaction logs more quickly and with less overhead. `(PR #1584) <https://github.com/apple/foundationdb/pull/1584>`_.
* Clients no longer talk to the cluster controller for failure monitoring information.  `(PR #1640) <https://github.com/apple/foundationdb/pull/1640>`_.
* Reduced the number of connection monitoring messages between clients and servers. `(PR #1768) <https://github.com/apple/foundationdb/pull/1768>`_.
* Close connections which have been idle for a long period of time. `(PR #1768) <https://github.com/apple/foundationdb/pull/1768>`_.
* Each client connects to exactly one coordinator, and at most five proxies. `(PR #1909) <https://github.com/apple/foundationdb/pull/1909>`_.
* Ratekeeper will throttle traffic when too many storage servers are not making versions durable fast enough. `(PR #1784) <https://github.com/apple/foundationdb/pull/1784>`_.
* Storage servers recovering a memory storage engine will abort recovery if the cluster is already healthy.  `(PR #1713) <https://github.com/apple/foundationdb/pull/1713>`_.
* Improved how the data distribution algorithm balances data across teams of storage servers. `(PR #1785) <https://github.com/apple/foundationdb/pull/1785>`_.
* Lowered the priority for data distribution team removal, to avoid prioritizing team removal work over splitting shards. `(PR #1853) <https://github.com/apple/foundationdb/pull/1853>`_.
* Made the storage cache eviction policy configurable, and added an LRU policy. `(PR #1506) <https://github.com/apple/foundationdb/pull/1506>`_.
* Improved the speed of recoveries on large clusters at ``log_version >= 4``. `(PR #1729) <https://github.com/apple/foundationdb/pull/1729>`_.
* Log routers will prefer to peek from satellites at ``log_version >= 4``. `(PR #1795) <https://github.com/apple/foundationdb/pull/1795>`_.
* In clusters using a region configuration, clients will read from the remote region if all of the servers in the primary region are overloaded. [6.2.3] `(PR #2019) <https://github.com/apple/foundationdb/pull/2019>`_.
* Significantly improved the rate at which the transaction logs in a remote region can pull data from the primary region. [6.2.4] `(PR #2101) <https://github.com/apple/foundationdb/pull/2101>`_.
* Raised the data distribution priority of splitting shards because delaying splits can cause hot write shards. [6.2.6] `(PR #2234) <https://github.com/apple/foundationdb/pull/2234>`_.

Fixes
-----

* During an upgrade, the multi-version client now persists database default options and transaction options that aren't reset on retry (e.g. transaction timeout). In order for these options to function correctly during an upgrade, a 6.2 or later client should be used as the primary client. `(PR #1767) <https://github.com/apple/foundationdb/pull/1767>`_.
* If a cluster is upgraded during an ``onError`` call, the cluster could return a ``cluster_version_changed`` error. `(PR #1734) <https://github.com/apple/foundationdb/pull/1734>`_.
* Data distribution will now pick a random destination when merging shards in the ``\xff`` keyspace. This avoids an issue with backup where the write-heavy mutation log shards could concentrate on a single process that has less data than everybody else. `(PR #1916) <https://github.com/apple/foundationdb/pull/1916>`_.
* Setting ``--machine_id`` (or ``-i``) for an ``fdbserver`` process now sets ``locality_machineid`` in addition to ``locality_zoneid``. `(PR #1928) <https://github.com/apple/foundationdb/pull/1928>`_.
* File descriptors opened by clients and servers set close-on-exec, if available on the platform. `(PR #1581) <https://github.com/apple/foundationdb/pull/1581>`_.
* ``fdbrestore`` commands other than ``start`` required a default cluster file to be found but did not actually use it. `(PR #1912) <https://github.com/apple/foundationdb/pull/1912>`_.
* Unneeded network connections were not being closed because peer reference counts were handled improperly. `(PR #1768) <https://github.com/apple/foundationdb/pull/1768>`_.
* In very rare scenarios, master recovery would restart because system metadata was loaded incorrectly. `(PR #1919) <https://github.com/apple/foundationdb/pull/1919>`_.
* Ratekeeper will aggressively throttle when unable to fetch the list of storage servers for a considerable period of time. `(PR #1858) <https://github.com/apple/foundationdb/pull/1858>`_.
* Proxies could become overloaded when all storage servers on a team fail. [6.2.1] `(PR #1976) <https://github.com/apple/foundationdb/pull/1976>`_.
* Proxies could start too few transactions if they didn't receive get read version requests frequently enough. [6.2.3] `(PR #1999) <https://github.com/apple/foundationdb/pull/1999>`_.
* The ``fileconfigure`` command in ``fdbcli`` could fail with an unknown error if the file did not contain a valid JSON object. `(PR #2017) <https://github.com/apple/foundationdb/pull/2017>`_.
* Configuring regions would fail with an internal error if the cluster contained storage servers that didn't set a datacenter ID. `(PR #2017) <https://github.com/apple/foundationdb/pull/2017>`_.
* Clients no longer prefer reading from servers with the same zone ID, because it could create hot shards. [6.2.3] `(PR #2019) <https://github.com/apple/foundationdb/pull/2019>`_.
* Data distribution could fail to start if any storage servers had misconfigured locality information. This problem could persist even after the offending storage servers were removed or fixed. [6.2.5] `(PR #2110) <https://github.com/apple/foundationdb/pull/2110>`_.
* Data distribution was running at too high of a priority, which sometimes caused other roles on the same process to stall. [6.2.5] `(PR #2170) <https://github.com/apple/foundationdb/pull/2170>`_.
* Loading a 6.1 or newer ``fdb_c`` library as a secondary client using the multi-version client could lead to an infinite recursion when run with API versions older than 610. [6.2.5] `(PR #2169) <https://github.com/apple/foundationdb/pull/2169>`_
* Using C API functions that were removed in 6.1 when using API version 610 or above now results in a compilation error. [6.2.5] `(PR #2169) <https://github.com/apple/foundationdb/pull/2169>`_
* Coordinator changes could fail to complete if the database wasn't allowing any transactions to start. [6.2.6] `(PR #2191) <https://github.com/apple/foundationdb/pull/2191>`_
* Status would report incorrect fault tolerance metrics when a remote region was configured and the primary region lost a storage replica. [6.2.6] `(PR #2230) <https://github.com/apple/foundationdb/pull/2230>`_
* The cluster would not change to a new set of satellite transaction logs when they become available in a better satellite location. [6.2.6] `(PR #2241) <https://github.com/apple/foundationdb/pull/2241>`_.
* The existence of ``proxy`` or ``resolver`` class processes prevented ``stateless`` class processes from being recruited as proxies or resolvers. [6.2.6] `(PR #2241) <https://github.com/apple/foundationdb/pull/2241>`_.
* The cluster controller could become saturated in clusters with large numbers of connected clients using TLS. [6.2.6] `(PR #2252) <https://github.com/apple/foundationdb/pull/2252>`_.
* Backup and DR would not share a mutation stream if they were started on different versions of FoundationDB. Either backup or DR must be restarted to resolve this issue. [6.2.6] `(PR #2202) <https://github.com/apple/foundationdb/pull/2202>`_.
* Don't track batch priority GRV requests in latency bands. [6.2.7] `(PR #2279) <https://github.com/apple/foundationdb/pull/2279>`_.
* Transaction log processes used twice their normal memory when switching spill types. [6.2.7] `(PR #2256) <https://github.com/apple/foundationdb/pull/2256>`_.
* Under certain conditions, cross region replication could stall for 10 minute periods. [6.2.7] `(PR #1818) <https://github.com/apple/foundationdb/pull/1818>`_ `(PR #2276) <https://github.com/apple/foundationdb/pull/2276>`_.
* When dropping a remote region from the configuration after processes in the region have failed, data distribution would create teams from the dead servers for one minute. [6.2.7] `(PR #2286) <https://github.com/apple/foundationdb/pull/1818>`_.

Status
------

* Added ``run_loop_busy`` to the ``processes`` section to record the fraction of time the run loop is busy. `(PR #1760) <https://github.com/apple/foundationdb/pull/1760>`_.
* Added ``cluster.page_cache`` section to status. In this section, added two new statistics ``storage_hit_rate`` and ``log_hit_rate`` that indicate the fraction of recent page reads that were served by cache. `(PR #1823) <https://github.com/apple/foundationdb/pull/1823>`_.
* Added transaction start counts by priority to ``cluster.workload.transactions``. The new counters are named ``started_immediate_priority``, ``started_default_priority``, and ``started_batch_priority``. `(PR #1836) <https://github.com/apple/foundationdb/pull/1836>`_.
* Remove ``cluster.datacenter_version_difference`` and replace it with ``cluster.datacenter_lag`` that has subfields ``versions`` and ``seconds``. `(PR #1800) <https://github.com/apple/foundationdb/pull/1800>`_.
* Added ``local_rate`` to the ``roles`` section to record the throttling rate of the local ratekeeper `(PR #1712) <http://github.com/apple/foundationdb/pull/1712>`_.
* Renamed ``cluster.fault_tolerance`` fields ``max_machines_without_losing_availability`` and ``max_machines_without_losing_data`` to ``max_zones_without_losing_availability`` and ``max_zones_without_losing_data`` `(PR #1925) <https://github.com/apple/foundationdb/pull/1925>`_.
* ``fdbcli`` status now reports the configured zone count. The fault tolerance is now reported in terms of the number of zones unless machine IDs are being used as zone IDs. `(PR #1924) <https://github.com/apple/foundationdb/pull/1924>`_.
* ``connected_clients`` is now only a sample of the connected clients, rather than a complete list. `(PR #1902) <https://github.com/apple/foundationdb/pull/1902>`_.
* Added ``max_protocol_clients`` to the ``supported_versions`` section, which provides a sample of connected clients which cannot connect to any higher protocol version. `(PR #1902) <https://github.com/apple/foundationdb/pull/1902>`_.
* Clients which connect without specifying their supported versions are tracked as an ``Unknown`` version in the ``supported_versions`` section. [6.2.2] `(PR #1990) <https://github.com/apple/foundationdb/pull/1990>`_.
* Add ``coordinator`` to the list of roles that can be reported for a process. [6.2.3] `(PR #2006) <https://github.com/apple/foundationdb/pull/2006>`_.
* Added ``worst_durability_lag_storage_server`` and ``limiting_durability_lag_storage_server`` to  the ``cluster.qos`` section, each with subfields ``versions`` and ``seconds``. These report the durability lag values being used by ratekeeper to potentially limit the transaction rate. [6.2.3] `(PR #2003) <https://github.com/apple/foundationdb/pull/2003>`_.
* Added ``worst_data_lag_storage_server`` and ``limiting_data_lag_storage_server`` to  the ``cluster.qos`` section, each with subfields ``versions`` and ``seconds``. These are meant to replace ``worst_version_lag_storage_server`` and ``limiting_version_lag_storage_server``, which are now deprecated. [6.2.3] `(PR #2003) <https://github.com/apple/foundationdb/pull/2003>`_.
* Added ``system_kv_size_bytes`` to the ``cluster.data`` section to record the size of the system keyspace. [6.2.5] `(PR #2170) <https://github.com/apple/foundationdb/pull/2170>`_.

Bindings
--------

* API version updated to 620. See the :ref:`API version upgrade guide <api-version-upgrade-guide-620>` for upgrade details.
* Add a transaction size limit as both a database option and a transaction option. `(PR #1725) <https://github.com/apple/foundationdb/pull/1725>`_.
* Added a new API to get the approximated transaction size before commit, e.g., ``fdb_transaction_get_approximate_size`` in the C binding. `(PR #1756) <https://github.com/apple/foundationdb/pull/1756>`_.
* C: ``fdb_future_get_version`` has been renamed to ``fdb_future_get_int64``. `(PR #1756) <https://github.com/apple/foundationdb/pull/1756>`_.
* C: Applications linking to ``libfdb_c`` can now use ``pkg-config foundationdb-client`` or ``find_package(FoundationDB-Client ...)`` (for cmake) to get the proper flags for compiling and linking. `(PR #1636) <https://github.com/apple/foundationdb/pull/1636>`_.
* Go: The Go bindings now require Go version 1.11 or later.
* Go: Finalizers could run too early leading to undefined behavior. `(PR #1451) <https://github.com/apple/foundationdb/pull/1451>`_.
* Added a transaction option to control the field length of keys and values in debug transaction logging in order to avoid truncation. `(PR #1844) <https://github.com/apple/foundationdb/pull/1844>`_.
* Added a transaction option to control the whether ``get_addresses_for_key`` includes a port in the address. This will be deprecated in api version 630, and addresses will include ports by default. [6.2.4] `(PR #2060) <https://github.com/apple/foundationdb/pull/2060>`_.
* Python: ``Versionstamp`` comparisons didn't work in Python 3. [6.2.4] `(PR #2089) <https://github.com/apple/foundationdb/pull/2089>`_.

Features
--------

* Added the ``cleanup`` command to ``fdbbackup`` which can be used to remove orphaned backups or DRs. [6.2.5] `(PR #2170) <https://github.com/apple/foundationdb/pull/2170>`_.
* Added the ability to configure ``satellite_logs`` by satellite location. This will overwrite the region configure of ``satellite_logs`` if both are present. [6.2.6] `(PR #2241) <https://github.com/apple/foundationdb/pull/2241>`_.

Other Changes
-------------

* Added the primitives for FDB backups based on disk snapshots. This provides an ability to take a cluster level backup based on disk level snapshots of the storage, tlogs and coordinators. `(PR #1733) <https://github.com/apple/foundationdb/pull/1733>`_.
* Foundationdb now uses the flatbuffers serialization format for all network messages. `(PR 1090) <https://github.com/apple/foundationdb/pull/1090>`_.
* Clients will throw ``transaction_too_old`` when attempting to read if ``setVersion`` was called with a version smaller than the smallest read version obtained from the cluster. This is a protection against reading from the wrong cluster in multi-cluster scenarios. `(PR #1413) <https://github.com/apple/foundationdb/pull/1413>`_.
* Trace files are now ordered lexicographically. This means that the filename format for trace files has changed. `(PR #1828) <https://github.com/apple/foundationdb/pull/1828>`_.
* Improved ``TransactionMetrics`` log events by adding a random UID to distinguish multiple open connections, a flag to identify internal vs. client connections, and logging of rates and roughness in addition to total count for several metrics. `(PR #1808) <https://github.com/apple/foundationdb/pull/1808>`_.
* FoundationDB can now be built with clang and libc++ on Linux. `(PR #1666) <https://github.com/apple/foundationdb/pull/1666>`_.
* Added experimental framework to run C and Java clients in simulator. `(PR #1678) <https://github.com/apple/foundationdb/pull/1678>`_.
* Added new network options for client buggify which will randomly throw expected exceptions in the client. This is intended to be used for client testing. `(PR #1417) <https://github.com/apple/foundationdb/pull/1417>`_.
* Added ``--cache_memory`` parameter for ``fdbserver`` processes to control the amount of memory dedicated to caching pages read from disk. `(PR #1889) <https://github.com/apple/foundationdb/pull/1889>`_.
* Added ``MakoWorkload``, used as a benchmark to do performance testing of FDB. `(PR #1586) <https://github.com/apple/foundationdb/pull/1586>`_.
* ``fdbserver`` now accepts a comma separated list of public and listen addresses. `(PR #1721) <https://github.com/apple/foundationdb/pull/1721>`_.
* ``CAUSAL_READ_RISKY`` has been enhanced to further reduce the chance of causally inconsistent reads. Existing users of ``CAUSAL_READ_RISKY`` may see increased GRV latency if proxies are distantly located from logs. `(PR #1841) <https://github.com/apple/foundationdb/pull/1841>`_.
* ``CAUSAL_READ_RISKY`` can be turned on for all transactions using a database option. `(PR #1841) <https://github.com/apple/foundationdb/pull/1841>`_.
* Added a ``no_wait`` option to the ``fdbcli`` exclude command to avoid blocking. `(PR #1852) <https://github.com/apple/foundationdb/pull/1852>`_.
* Idle clusters will fsync much less frequently. `(PR #1697) <https://github.com/apple/foundationdb/pull/1697>`_.
* CMake is now the official build system. The Makefile based build system is deprecated.
* The incompatible client list in status (``cluster.incompatible_connections``) may now spuriously include clients that use the multi-version API to try connecting to the cluster at multiple versions.

Fixes only impacting 6.2.0+
---------------------------

* Clients could crash when closing connections with incompatible servers. [6.2.1] `(PR #1976) <https://github.com/apple/foundationdb/pull/1976>`_.
* Do not close idle network connections with incompatible servers. [6.2.1] `(PR #1976) <https://github.com/apple/foundationdb/pull/1976>`_.
* In status, ``max_protocol_clients`` were incorrectly added to the ``connected_clients`` list. [6.2.2] `(PR #1990) <https://github.com/apple/foundationdb/pull/1990>`_.
* Ratekeeper ignores the (default 5 second) MVCC window when controlling on durability lag. [6.2.3] `(PR #2012) <https://github.com/apple/foundationdb/pull/2012>`_.
* The macOS client was not compatible with a Linux server. [6.2.3] `(PR #2045) <https://github.com/apple/foundationdb/pull/2045>`_.
* Incompatible clients would continually reconnect with coordinators. [6.2.3] `(PR #2048) <https://github.com/apple/foundationdb/pull/2048>`_.
* Connections were being closed as idle when there were still unreliable requests waiting for a response. [6.2.3] `(PR #2048) <https://github.com/apple/foundationdb/pull/2048>`_.
* The cluster controller would saturate its CPU for a few seconds when sending configuration information to all of the worker processes. [6.2.4] `(PR #2086) <https://github.com/apple/foundationdb/pull/2086>`_.
* The data distributor would build all possible team combinations if it was tracking an unhealthy server with less than 10 teams. [6.2.4] `(PR #2099) <https://github.com/apple/foundationdb/pull/2099>`_.
* The cluster controller could crash if a coordinator was unreachable when compiling cluster status. [6.2.4] `(PR #2065) <https://github.com/apple/foundationdb/pull/2065>`_.
* A storage server could crash if it took longer than 10 minutes to fetch a key range from another server. [6.2.5] `(PR #2170) <https://github.com/apple/foundationdb/pull/2170>`_.
* Excluding or including servers would restart the data distributor. [6.2.5] `(PR #2170) <https://github.com/apple/foundationdb/pull/2170>`_.
* The data distributor could read invalid memory when estimating database size. [6.2.6] `(PR #2225) <https://github.com/apple/foundationdb/pull/2225>`_.
* Status could incorrectly report that backup and DR were not sharing a mutation stream. [6.2.7] `(PR #2274) <https://github.com/apple/foundationdb/pull/2274>`_.

Earlier release notes
---------------------
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
