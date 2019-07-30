#############
Release Notes
#############

6.2.0
=====

Features
--------
* Improved team collection for data distribution that builds a balanced number of teams per server and gurantees that each server has at least one team. `(PR #1785) <https://github.com/apple/foundationdb/pull/1785>`_.
* Added the option to have data distribution FetchKeys to run at a lower priority by setting the knob ``FETCH_KEYS_LOWER_PRIORITY`` `(PR #1791) <https://github.com/apple/foundationdb/pull/1791>`_.

* CMake is now our official build system. The Makefile based build system is deprecated.

* Added local ratekeeper, to throttle reads at a per-storage-process level. `(PR #1447) <https://github.com/apple/foundationdb/pull/1477>`_.

* FDB backups based on disk snapshots, provides an ability to take cluster level backup based on disk level snapshots of storage, tlogs and coordinators. `(PR #1733) <https://github.com/apple/foundationdb/pull/1733>`_.

Performance
-----------

* Use CRC32 checksum for SQLite pages. `(PR #1582) <https://github.com/apple/foundationdb/pull/1582>`_.
* Added a 96-byte fast allocator, so storage queue nodes use less memory. `(PR #1336) <https://github.com/apple/foundationdb/pull/1336>`_.
* Handle large packets better. `(PR #1684) <https://github.com/apple/foundationdb/pull/1684>`_.

Fixes
-----

* Set the priority of redundant teams to remove as PRIORITY_TEAM_REDUNDANT, instead of PRIORITY_TEAM_UNHEALTHY. `(PR #1802) <https://github.com/apple/foundationdb/pull/1802>`_.
* During an upgrade, the multi-version client now persists database default options and transaction options that aren't reset on retry (e.g. transaction timeout). In order for these options to function correctly during an upgrade, a 6.2 or later client should be used as the primary client. `(PR #1767) <https://github.com/apple/foundationdb/pull/1767>`_.
* If a cluster is upgraded during an ``onError`` call, the cluster could return a ``cluster_version_changed`` error. `(PR #1734) <https://github.com/apple/foundationdb/pull/1734>`_.
* Do not set doBuildTeams in StorageServerTracker unless a storage server's interface changes, in order to avoid unnecessary work. `(PR #1779) <https://github.com/apple/foundationdb/pull/1779>`_.
* Data distribution will now pick a random destination when merging shards in the ``\xff`` keyspace. This avoids an issue with backup where the write-heavy mutation log shards could concentrate on a single process that has less data than everybody else. `(PR #1916) <https://github.com/apple/foundationdb/pull/1916>`_.

Status
------

* Added ``run_loop_busy`` to the ``processes`` section to record the fraction of time the run loop is busy. `(PR #1760) <https://github.com/apple/foundationdb/pull/1760>`_.
* Added ``cluster.page_cache`` section to status. In this section, added two new statistics ``storage_hit_rate`` and ``log_hit_rate`` that indicate the fraction of recent page reads that were served by cache. `(PR #1823) <https://github.com/apple/foundationdb/pull/1823>`_.
* Added transaction start counts by priority to ``cluster.workload.transactions``. The new counters are named ``started_immediate_priority``, ``started_default_priority``, and ``started_batch_priority``. `(PR #1836) <https://github.com/apple/foundationdb/pull/1836>`_.
* Remove ``cluster.datacenter_version_difference`` and replace it with ``cluster.datacenter_lag`` that has subfields ``versions`` and ``seconds``. `(PR #1800) <https://github.com/apple/foundationdb/pull/1800>`_.
* Added ``local_rate`` to the ``roles`` section to record the throttling rate of the local ratekeeper `(PR #1712) <http://github.com/apple/foundationdb/pull/1712>`_.
* ``fdbcli`` status now reports the configured zone count. The fault tolerance is now reported in terms of the number of zones unless machine IDs are being used as zone IDs. `(PR #1924) <https://github.com/apple/foundationdb/pull/1924>`_.

Bindings
--------

* Add a transaction size limit for both database option and transaction option. `(PR #1725) <https://github.com/apple/foundationdb/pull/1725>`_.
* Added a new API to get the approximated transaction size before commit, e.g., ``fdb_transaction_get_approximate_size`` in the C binding. `(PR #1756) <https://github.com/apple/foundationdb/pull/1756>`_.
* C: ``fdb_future_get_version`` has been renamed to ``fdb_future_get_int64``. `(PR #1756) <https://github.com/apple/foundationdb/pull/1756>`_.
* C: Applications linking to libfdb_c can now use ``pkg-config foundationdb-client`` or ``find_package(FoundationDB-Client ...)`` (for cmake) to get the proper flags for compiling and linking. `(PR #1636) <https://github.com/apple/foundationdb/pull/1636>`_.
* Go: The Go bindings now require Go version 1.11 or later.
* Go: Fix issue with finalizers running too early that could lead to undefined behavior. `(PR #1451) <https://github.com/apple/foundationdb/pull/1451>`_.
* Added transaction option to control the field length of keys and values in debug transaction logging in order to avoid truncation. `(PR #1844) <https://github.com/apple/foundationdb/pull/1844>`_.

Other Changes
-------------

* Clients will throw ``transaction_too_old`` when attempting to read if ``setVersion`` was called with a version smaller than the smallest read version obtained from the cluster. This is a protection against reading from the wrong cluster in multi-cluster scenarios. `(PR #1413) <https://github.com/apple/foundationdb/pull/1413>`_.
* Trace files are now ordered lexicographically. This means that the filename format for trace files did change. `(PR #1828) <https://github.com/apple/foundationdb/pull/1828>`_.
* Improved ``TransactionMetrics`` log events by adding a random UID to distinguish multiple open connections, a flag to identify internal vs. client connections, and logging of rates and roughness in addition to total count for several metrics. `(PR #1808) <https://github.com/apple/foundationdb/pull/1808>`_.
* FoundationDB can now be built with clang and libc++ on Linux `(PR #1666) <https://github.com/apple/foundationdb/pull/1666>`_.
* Added experimental framework to run C and Java clients in simulator `(PR #1678) <https://github.com/apple/foundationdb/pull/1678>`_.
* Added new network option for client buggify which will randomly throw expected exceptions in the client. Intended for client testing `(PR #1417) <https://github.com/apple/foundationdb/pull/1417>`_.
* Added ``--cache_memory`` parameter for ``fdbserver`` processes to control the amount of memory dedicated to caching pages read from disk. `(PR #1889) <https://github.com/apple/foundationdb/pull/1889>`_.
* Added ``MakoWorkload``, used as a benchmark to do performance testing of FDB. `(PR #1586) <https://github.com/apple/foundationdb/pull/1586>`_.
* Added two knobs ``LOAD_BALANCE_ZONE_ID_LOCALITY_ENABLED`` and ``LOAD_BALANCE_DC_ID_LOCALITY_ENABLED`` allowing locality-based decision-making to be toggled on/off during load balancing. `(PR #1820) <https://github.com/apple/foundationdb/pull/1820>`_.
* Ratekeeper will aggressively throttle when unable to fetch the list of storage servers for a considerable period of time. `(PR #1858) <https://github.com/apple/foundationdb/pull/1858>`_.
* ``fdbserver`` now accepts a comma separated list of public and listen addresses. `(PR #1721) <https://github.com/apple/foundationdb/pull/1721>`_.

Earlier release notes
---------------------
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
