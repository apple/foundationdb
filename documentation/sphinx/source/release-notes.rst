#############
Release Notes
#############

6.2.16
======

Fixes
-----

* Storage servers could fail to advance their version correctly in response to empty commits. `(PR #2617) <https://github.com/apple/foundationdb/pull/2617>`_.

6.2.15
======

Fixes
-----

* TLS throttling could block legitimate connections. `(PR #2575) <https://github.com/apple/foundationdb/pull/2575>`_.

6.2.14
======

Fixes
-----

* Data distribution was prioritizing shard merges too highly. `(PR #2562) <https://github.com/apple/foundationdb/pull/2562>`_.
* Status would incorrectly mark clusters as having no fault tolerance. `(PR #2562) <https://github.com/apple/foundationdb/pull/2562>`_.
* A proxy could run out of memory if disconnected from the cluster for too long. `(PR #2562) <https://github.com/apple/foundationdb/pull/2562>`_.

6.2.13
======

Performance
-----------

* Optimized the commit path the proxies to significantly reduce commit latencies in large clusters. `(PR #2536) <https://github.com/apple/foundationdb/pull/2536>`_.
* Data distribution could create temporarily untrackable shards which could not be split if they became hot. `(PR #2546) <https://github.com/apple/foundationdb/pull/2546>`_.

6.2.12
======

Performance
-----------

* Throttle TLS connect attempts from misconfigured clients. `(PR #2529) <https://github.com/apple/foundationdb/pull/2529>`_.
* Reduced master recovery times in large clusters. `(PR #2430) <https://github.com/apple/foundationdb/pull/2430>`_.
* Improved performance while a remote region is catching up. `(PR #2527) <https://github.com/apple/foundationdb/pull/2527>`_.
* The data distribution algorithm does a better job preventing hot shards while recovering from machine failures. `(PR #2526) <https://github.com/apple/foundationdb/pull/2526>`_.

Fixes
-----

* Improve the reliability of a ``kill`` command from ``fdbcli``. `(PR #2512) <https://github.com/apple/foundationdb/pull/2512>`_.
* The ``--traceclock`` parameter to fdbserver incorrectly had no effect. `(PR #2420) <https://github.com/apple/foundationdb/pull/2420>`_.
* Clients could throw an internal error during ``commit`` if client buggification was enabled. `(PR #2427) <https://github.com/apple/foundationdb/pull/2427>`_.
* Backup and DR agent transactions which update and clean up status had an unnecessarily high conflict rate. `(PR #2483) <https://github.com/apple/foundationdb/pull/2483>`_.
* The slow task profiler used an unsafe call to get a timestamp in its signal handler that could lead to rare crashes. `(PR #2515) <https://github.com/apple/foundationdb/pull/2515>`_.

6.2.11
======

Fixes
-----

* Clients could hang indefinitely on reads if all storage servers holding a keyrange were removed from a cluster since the last time the client read a key in the range. `(PR #2377) <https://github.com/apple/foundationdb/pull/2377>`_.
* In rare scenarios, status could falsely report no replicas remain of some data. `(PR #2380) <https://github.com/apple/foundationdb/pull/2380>`_.
* Latency band tracking could fail to configure correctly after a recovery or upon process startup. `(PR #2371) <https://github.com/apple/foundationdb/pull/2371>`_.

6.2.10
======

Fixes
-----

* ``backup_agent`` crashed on startup. `(PR #2356) <https://github.com/apple/foundationdb/pull/2356>`_.

6.2.9
=====

Fixes
-----

* Small clusters using specific sets of process classes could cause the data distributor to be continuously killed and re-recruited. `(PR #2344) <https://github.com/apple/foundationdb/pull/2344>`_.
* The data distributor and ratekeeper could be recruited on non-optimal processes. `(PR #2344) <https://github.com/apple/foundationdb/pull/2344>`_.
* A ``kill`` command from ``fdbcli`` could take a long time before being executed by a busy process. `(PR #2339) <https://github.com/apple/foundationdb/pull/2339>`_.
* Committing transactions larger than 1 MB could cause the proxy to stall for up to a second. `(PR #2350) <https://github.com/apple/foundationdb/pull/2350>`_.
* Transaction timeouts would use memory for the entire duration of the timeout, regardless of whether the transaction had been destroyed. `(PR #2353) <https://github.com/apple/foundationdb/pull/2353>`_.

6.2.8
=====

Fixes
-----

* Significantly improved the rate at which the transaction logs in a remote region can pull data from the primary region. `(PR #2307) <https://github.com/apple/foundationdb/pull/2307>`_ `(PR #2323) <https://github.com/apple/foundationdb/pull/2323>`_.
* The ``system_kv_size_bytes`` status field could report a size much larger than the actual size of the system keyspace. `(PR #2305) <https://github.com/apple/foundationdb/pull/2305>`_.

6.2.7
=====

Performance
-----------

Fixes
-----

Status
------
* Replaced ``cluster.database_locked`` status field with ``cluster.database_lock_state``, which contains two subfields: ``locked`` (boolean) and ``lock_uid`` (which contains the database lock uid if the database is locked). `(PR #2058) <https://github.com/apple/foundationdb/pull/2058>`_

Bindings
--------
* API version updated to 700. See the :ref:`API version upgrade guide <api-version-upgrade-guide-700>` for upgrade details.
* Java: Introduced ``keyAfter`` utility function that can be used to create the immediate next key for a given byte array. `(PR #2458) <https://github.com/apple/foundationdb/pull/2458>`_
* C: The ``FDBKeyValue`` struct's ``key`` and ``value`` members have changed type from ``void*`` to ``uint8_t*``. `(PR #2622) <https://github.com/apple/foundationdb/pull/2622>`_

Other Changes
-------------
* Double the number of shard locations that the client will cache locally. `(PR #2198) <https://github.com/apple/foundationdb/pull/2198>`_

Earlier release notes
---------------------
* :doc:`6.2 (API Version 620) </old-release-notes/release-notes-620>`
* :doc:`6.1 (API Version 610) </old-release-notes/release-notes-610>`
* :doc:`6.0 (API Version 600) </old-release-notes/release-notes-600>`
* :doc:`5.2 (API Version 520) </old-release-notes/release-notes-520>`
* :doc:`5.1 (API Version 510) </old-release-notes/release-notes-510>`
* :doc:`5.0 (API Version 500) </old-release-notes/release-notes-500>`
* :doc:`4.6 (API Version 460) </old-release-notes/release-notes-460>`
* :doc:`4.5 (API Version 450) </old-release-notes/release-notes-450>`
* :doc:`4.4 (API Version 440) </old-release-notes/release-notes-440>`
* :doc:`4.3 (API Version 430) </old-release-notes/release-notes-430>`
* :doc:`4.2 (API Version 420) </old-release-notes/release-notes-420>`
* :doc:`4.1 (API Version 410) </old-release-notes/release-notes-410>`
* :doc:`4.0 (API Version 400) </old-release-notes/release-notes-400>`
* :doc:`3.0 (API Version 300) </old-release-notes/release-notes-300>`
* :doc:`2.0 (API Version 200) </old-release-notes/release-notes-200>`
* :doc:`1.0 (API Version 100) </old-release-notes/release-notes-100>`
* :doc:`Beta 3 (API Version 23) </old-release-notes/release-notes-023>`
* :doc:`Beta 2 (API Version 22) </old-release-notes/release-notes-022>`
* :doc:`Beta 1 (API Version 21) </old-release-notes/release-notes-021>`
* :doc:`Alpha 6 (API Version 16) </old-release-notes/release-notes-016>`
* :doc:`Alpha 5 (API Version 14) </old-release-notes/release-notes-014>`
