#############
Release Notes
#############

6.1.13
======

* Loading a 6.1 or newer ``fdb_c`` library as a secondary client using the multi-version client could lead to an infinite recursion when run with API versions older than 610. `(PR #2169) <https://github.com/apple/foundationdb/pull/2169>`_
* Using C API functions that were removed in 6.1 when using API version 610 or above now results in a compilation error. `(PR #2169) <https://github.com/apple/foundationdb/pull/2169>`_
* ``fdbrestore`` commands other than ``start`` required a default cluster file to be found but did not actually use it. `(PR #1912) <https://github.com/apple/foundationdb/pull/1912>`_.

6.1.12
======

Fixes
-----

* Fixed a thread safety issue while writing large keys or values. `(Issue #1846) <https://github.com/apple/foundationdb/issues/1846>`_
* An untracked data distributor could prevent a newly recruited data distributor from being started. `(PR #1849) <https://github.com/apple/foundationdb/pull/1849>`_

6.1.11
======

Fixes
-----

* Machines which were added to a cluster immediately after the cluster was upgraded to 6.1 would not be given data. `(PR #1764) <https://github.com/apple/foundationdb/pull/1764>`_

6.1.10
======

Performance
-----------

* Improved the recovery speed of storage servers with large amount of data. `(PR #1700) <https://github.com/apple/foundationdb/pull/1700>`_

Fixes
-----

* The ``fdbrestore`` commands ``abort``, ``wait``, and ``status`` would use a default cluster file instead of the destination cluster file argument.  `(PR #1701) <https://github.com/apple/foundationdb/pull/1701>`_

6.1.9
=====

Fixes
-----

* Sometimes a minority of coordinators would not converge to the leader. `(PR #1649) <https://github.com/apple/foundationdb/pull/1649>`_
* HTTP responses indicating a server-side error are no longer expected to contain a ResponseID header. `(PR #1651) <https://github.com/apple/foundationdb/pull/1651>`_

6.1.8
=====

Features
--------

* Improved replication mechanism using a new hierarchical technique that significantly reduces the frequency of data loss events even when multiple fault-tolerance zones permanently fail at the same time. After upgrading to 6.1 clusters will experience a low level of background data movement to store data in accordance with the new policy. `(PR #964) <https://github.com/apple/foundationdb/pull/964>`_.
* Added a background actor to remove redundant teams from team collection so that the healthy team number is guaranteed to not exceed the desired number. `(PR #1139) <https://github.com/apple/foundationdb/pull/1139>`_
* Get read version, read, and commit requests are counted and aggregated by server-side latency in configurable latency bands and output in JSON status. `(PR #1084) <https://github.com/apple/foundationdb/pull/1084>`_
* Added configuration option to choose log spilling implementation `(PR #1160) <https://github.com/apple/foundationdb/pull/1160>`_
* Added configuration option to choose log system implementation `(PR #1160) <https://github.com/apple/foundationdb/pull/1160>`_
* Batch priority transactions are now limited separately by ratekeeper and will be throttled at lower levels of cluster saturation. This makes it possible to run a more intense background load at saturation without significantly affecting normal priority transactions. It is still recommended not to run excessive loads at batch priority. `(PR #1198) <https://github.com/apple/foundationdb/pull/1198>`_
* Restore now requires the destination cluster to be specified explicitly to avoid confusion. `(PR #1240) <https://github.com/apple/foundationdb/pull/1240>`_
* Restore now accepts a timestamp that can be used to determine the restore version if the original cluster is available. `(PR #1240) <https://github.com/apple/foundationdb/pull/1240>`_
* Backup ``status`` and ``describe`` commands now have a ``--json`` output option. `(PR #1248) <https://github.com/apple/foundationdb/pull/1248>`_
* Separated data distribution from the master into its own role. `(PR #1062) <https://github.com/apple/foundationdb/pull/1062>`_
* Separated ratekeeper from the master into its own role. `(PR #1176) <https://github.com/apple/foundationdb/pull/1176>`_
* Added a ``CompareAndClear`` atomic op that clears a key if its value matches the supplied value. `(PR #1105) <https://github.com/apple/foundationdb/pull/1105>`_
* Added support for IPv6. `(PR #1178) <https://github.com/apple/foundationdb/pull/1178>`_
* FDB can now simultaneously listen to TLS and unencrypted ports to facilitate smoother migration to and from TLS. `(PR #1157) <https://github.com/apple/foundationdb/pull/1157>`_
* Added ``DISABLE_POSIX_KERNEL_AIO`` knob to fallback to libeio instead of kernel async I/O (KAIO) for systems that do not support KAIO or O_DIRECT flag. `(PR #1283) <https://github.com/apple/foundationdb/pull/1283>`_
* Added support for configuring the cluster to use the primary and remote DC's as satellites. `(PR #1320) <https://github.com/apple/foundationdb/pull/1320>`_
* Added support for restoring multiple key ranges in a single restore job. `(PR #1190) <https://github.com/apple/foundationdb/pull/1190>`_
* Deprecated transaction option ``TRANSACTION_LOGGING_ENABLE``. Added two new transaction options ``DEBUG_TRANSACTION_IDENTIFIER`` and ``LOG_TRANSACTION`` that sets an identifier for the transaction and logs the transaction to the trace file respectively. `(PR #1200) <https://github.com/apple/foundationdb/pull/1200>`_
* Clients can now specify default transaction timeouts and retry limits for all transactions through a database option. `(Issue #775) <https://github.com/apple/foundationdb/issues/775>`_
* The "timeout", "max retry delay", and "retry limit" transaction options are no longer reset when the transaction is reset after a call to ``onError`` (as of API version 610). `(Issue #775) <https://github.com/apple/foundationdb/issues/775>`_
* Added the ``force_recovery_with_data_loss`` command to ``fdbcli``. When a cluster is configured with usable_regions=2, this command will force the database to recover in the remote region. `(PR #1168) <https://github.com/apple/foundationdb/pull/1168>`_
* Added a limit to the number of status requests the cluster controller will handle. `(PR #1093) <https://github.com/apple/foundationdb/pull/1093>`_ (submitted by tclinken)
* Added a ``coordinator`` process class. Processes with this class can only be used as a coordinator, and ``coordinators auto`` will prefer to choose processes of this class. `(PR #1069) <https://github.com/apple/foundationdb/pull/1069>`_ (submitted by tclinken)
* The ``consistencycheck`` fdbserver role will check the entire database at most once every week. `(PR #1126) <https://github.com/apple/foundationdb/pull/1126>`_
* Added the metadata version key (``\xff/metadataVersion``). The value of this key is sent with every read version. It is intended to help clients cache rarely changing metadata. `(PR #1213) <https://github.com/apple/foundationdb/pull/1213>`_
* The ``fdbdr switch`` command verifies a ``dr_agent`` exists in both directions. `(Issue #1220) <https://github.com/apple/foundationdb/issues/1220>`_
* Transaction logs that cannot commit to disk for more than 5 seconds are marked as degraded. The cluster controller will prefer to recruit transaction logs on other processes before using degraded processes. `(Issue #690) <https://github.com/apple/foundationdb/issues/690>`_
* The ``memory`` storage engine configuration now uses the ssd engine for transaction log spilling. Transaction log spilling only happens when the transaction logs are using too much memory, so using the memory storage engine for this purpose can cause the process to run out of memory. Existing clusters will NOT automatically change their configuration. `(PR #1314) <https://github.com/apple/foundationdb/pull/1314>`_
* Trace logs can be output as JSON instead of XML using the ``--trace_format`` command line option. `(PR #976) <https://github.com/apple/foundationdb/pull/976>`_ (by atn34)
* Added ``modify`` command to fdbbackup for modifying parameters of a running backup. `(PR #1237) <https://github.com/apple/foundationdb/pull/1237>`_
* Added ``header`` parameter to blobstore backup URLs for setting custom HTTP headers. `(PR #1237) <https://github.com/apple/foundationdb/pull/1237>`_
* Added the ``maintenance`` command to ``fdbcli``. This command will stop data distribution from moving data away from processes with a specified zoneID. `(PR #1397) <https://github.com/apple/foundationdb/pull/1397>`_
* Added the ``three_data_hall_fallback`` configuration, which can be used to drop storage replicas in a dead data hall. [6.1.1] `(PR #1422) <https://github.com/apple/foundationdb/pull/1422>`_

Performance
-----------

* Increased the get read version batch size in the client. This change reduces the load on the proxies when doing many transactions with only a few operations per transaction. `(PR #1311) <https://github.com/apple/foundationdb/pull/1311>`_
* Clients no longer attempt to connect to the master during recovery. `(PR #1317) <https://github.com/apple/foundationdb/pull/1317>`_
* Increase the rate that deleted pages are made available for reuse in the SQLite storage engine. Rename and add knobs to provide more control over this process. [6.1.3] `(PR #1485) <https://github.com/apple/foundationdb/pull/1485>`_
* SQLite page files now grow and shrink in chunks based on a knob which defaults to an effective chunk size of 100MB. [6.1.4] `(PR #1482) <https://github.com/apple/foundationdb/pull/1482>`_ `(PR #1499) <https://github.com/apple/foundationdb/pull/1499>`_
* Reduced the rate at which data is moved between servers, to reduce the impact a failure has on cluster performance. [6.1.4] `(PR #1499) <https://github.com/apple/foundationdb/pull/1499>`_
* Avoid closing saturated network connections which have not received ping packets. [6.1.7] `(PR #1601) <https://github.com/apple/foundationdb/pull/1601>`_

Fixes
-----

* Python: Creating a ``SingleFloat`` for the tuple layer didn't work with integers. `(PR #1216) <https://github.com/apple/foundationdb/pull/1216>`_
* In some cases, calling ``OnError`` with a non-retryable error would partially reset a transaction. As of API version 610, the transaction will no longer be reset in these cases and will instead put the transaction into an error state. `(PR #1298) <https://github.com/apple/foundationdb/pull/1298>`_
* Standardized datetime string format across all backup and restore command options and outputs. `(PR #1248) <https://github.com/apple/foundationdb/pull/1248>`_
* Read workload status metrics would disappear when a storage server was missing. `(PR #1348) <https://github.com/apple/foundationdb/pull/1348>`_
* The ``coordinators auto`` command could recruit multiple coordinators with the same zone ID. `(Issue #988) <https://github.com/apple/foundationdb/issues/988>`_
* The data version of a cluster after a restore could have been lower than the restore version, making versionstamp operations get smaller. `(PR #1213) <https://github.com/apple/foundationdb/pull/1213>`_
* Fixed a few thread safety issues with slow task profiling. `(PR #1085) <https://github.com/apple/foundationdb/pull/1085>`_
* Changing the class of a process would not change its preference for becoming the cluster controller. `(PR #1350) <https://github.com/apple/foundationdb/pull/1350>`_
* The Go bindings reported an incorrect required version when trying to load an incompatible fdb_c library. `(PR #1053) <https://github.com/apple/foundationdb/pull/1053>`_
* The ``include`` command in fdbcli would falsely include all machines with IP addresses that
  have the included IP address as a prefix (for example ``include 1.0.0.1`` would also include
  ``1.0.0.10``). `(PR #1121) <https://github.com/apple/foundationdb/pull/1121>`_
* Restore could crash when reading a file that ends on a block boundary (1MB default). `(PR #1205) <https://github.com/apple/foundationdb/pull/1205>`_
* Java: Successful commits and range reads no longer create ``FDBException`` objects, which avoids wasting resources and reduces memory pressure. `(Issue #1235) <https://github.com/apple/foundationdb/issues/1235>`_
* Windows: Fixed a crash when deleting files. `(Issue #1380) <https://github.com/apple/foundationdb/issues/1380>`_ (by KrzysFR)
* Starting a restore on a tag already in-use would hang and the process would eventually run out of memory. `(PR #1394) <https://github.com/apple/foundationdb/pull/1394>`_
* The ``proxy_memory_limit_exceeded`` error was treated as retryable, but ``fdb_error_predicate`` returned that it is not retryable. `(PR #1438) <https://github.com/apple/foundationdb/pull/1438>`_.
* Consistency check could report inaccurate shard size estimates if there were enough keys with large values and a small number of keys with small values. [6.1.3] `(PR #1468) <https://github.com/apple/foundationdb/pull/1468>`_.
* Storage servers could not rejoin the cluster when the proxies were saturated. [6.1.4] `(PR #1486) <https://github.com/apple/foundationdb/pull/1486>`_ `(PR #1499) <https://github.com/apple/foundationdb/pull/1499>`_
* The ``configure`` command in ``fdbcli`` returned successfully even when the configuration was not changed for some error types. [6.1.4] `(PR #1509) <https://github.com/apple/foundationdb/pull/1509>`_
* Safety protections in the ``configure`` command in ``fdbcli`` would trigger spuriously when changing between ``three_datacenter`` replication and a region configuration. [6.1.4] `(PR #1509) <https://github.com/apple/foundationdb/pull/1509>`_
* Status could report an incorrect reason for ongoing data movement. [6.1.5] `(PR #1544) <https://github.com/apple/foundationdb/pull/1544>`_
* Storage servers were considered failed as soon as they were rebooted, instead of waiting to see if they rejoin the cluster. [6.1.8] `(PR #1618) <https://github.com/apple/foundationdb/pull/1618>`_

Status
------

* Report the number of connected coordinators for each client. This aids in monitoring client TLS support when enabling TLS on a live cluster. `(PR #1222) <https://github.com/apple/foundationdb/pull/1222>`_
* Degraded processes are reported in ``status json``. `(Issue #690) <https://github.com/apple/foundationdb/issues/690>`_

Bindings
--------

* API version updated to 610. See the :ref:`API version upgrade guide <api-version-upgrade-guide-610>` for upgrade details.
* The API to create a database has been simplified across the bindings. All changes are backward compatible with previous API versions, with one exception in Java noted below. `(PR #942) <https://github.com/apple/foundationdb/pull/942>`_
* C: ``FDBCluster`` objects and related methods (``fdb_create_cluster``, ``fdb_cluster_create_database``, ``fdb_cluster_set_option``, ``fdb_cluster_destroy``, ``fdb_future_get_cluster``) have been removed. `(PR #942) <https://github.com/apple/foundationdb/pull/942>`_
* C: Added ``fdb_create_database`` that creates a new ``FDBDatabase`` object synchronously and removed ``fdb_future_get_database``. `(PR #942) <https://github.com/apple/foundationdb/pull/942>`_
* Python: Removed ``fdb.init``, ``fdb.create_cluster``, and ``fdb.Cluster``. ``fdb.open`` no longer accepts a ``database_name`` parameter. `(PR #942) <https://github.com/apple/foundationdb/pull/942>`_
* Java: Deprecated ``FDB.createCluster`` and ``Cluster``. The preferred way to get a ``Database`` is by using ``FDB.open``, which should work in both new and old API versions. `(PR #942) <https://github.com/apple/foundationdb/pull/942>`_
* Java: Removed ``Cluster(long cPtr, Executor executor)`` constructor. This is API breaking for any code that has subclassed the ``Cluster`` class and is not protected by API versioning. `(PR #942) <https://github.com/apple/foundationdb/pull/942>`_
* Java: Several methods relevant to read-only transactions have been moved into the ``ReadTransaction`` interface.
* Java: Tuples now cache previous hash codes and equality checking no longer requires packing the underlying Tuples. `(PR #1166) <https://github.com/apple/foundationdb/pull/1166>`_
* Java: Tuple performance has been improved to use fewer allocations when packing and unpacking. `(Issue #1206) <https://github.com/apple/foundationdb/issues/1206>`_
* Java: Unpacking a Tuple with a byte array or string that is missing the end-of-string character now throws an error. `(Issue #671) <https://github.com/apple/foundationdb/issues/671>`_
* Java: Unpacking a Tuple constrained to a subset of the underlying array now throws an error when it encounters a truncated integer. `(Issue #672) <https://github.com/apple/foundationdb/issues/672>`_
* Ruby: Removed ``FDB.init``, ``FDB.create_cluster``, and ``FDB.Cluster``. ``FDB.open`` no longer accepts a ``database_name`` parameter. `(PR #942) <https://github.com/apple/foundationdb/pull/942>`_
* Golang: Deprecated ``fdb.StartNetwork``, ``fdb.Open``, ``fdb.MustOpen``, and ``fdb.CreateCluster`` and added ``fdb.OpenDatabase`` and ``fdb.MustOpenDatabase``. The preferred way to start the network and get a ``Database`` is by using ``FDB.OpenDatabase`` or ``FDB.OpenDefault``. `(PR #942) <https://github.com/apple/foundationdb/pull/942>`_
* Flow: Removed ``API::createCluster`` and ``Cluster`` and added ``API::createDatabase``. The new way to get a ``Database`` is by using ``API::createDatabase``. `(PR #942) <https://github.com/apple/foundationdb/pull/942>`_ `(PR #1215) <https://github.com/apple/foundationdb/pull/1215>`_
* Flow: Changed ``DatabaseContext`` to ``Database``, and ``API::createDatabase`` returns ``Reference<Database>`` instead of ``Reference<<DatabaseContext>``.  `(PR #1215) <https://github.com/apple/foundationdb/pull/1215>`_
* Flow: Converted ``Transaction`` into an interface and moved its implementation into an internal class. Transactions should now be created using ``Database::createTransaction(db)``. `(PR #1215) <https://github.com/apple/foundationdb/pull/1215>`_
* Flow: Added ``ReadTransaction`` interface that allows only read operations on a transaction. The ``Transaction`` interface inherits from ``ReadTransaction`` and can be used when a ``ReadTransaction`` is required. `(PR #1215) <https://github.com/apple/foundationdb/pull/1215>`_
* Flow: Changed ``Transaction::setVersion`` to ``Transaction::setReadVersion``. `(PR #1215) <https://github.com/apple/foundationdb/pull/1215>`_
* Flow: On update to this version of the Flow bindings, client code will fail to build due to the changes in the API, irrespective of the API version used. Client code must be updated to use the new bindings API. These changes affect the bindings only and won't impact compatibility with different versions of the cluster. `(PR #1215) <https://github.com/apple/foundationdb/pull/1215>`_
* Golang: Added ``fdb.Printable`` to print a human-readable string for a given byte array. Add ``Key.String()``, which converts the ``Key`` to a ``string`` using the ``Printable`` function. `(PR #1010) <https://github.com/apple/foundationdb/pull/1010>`_ (submitted by pjvds)
* Golang: Tuples now support ``Versionstamp`` operations. `(PR #1187) <https://github.com/apple/foundationdb/pull/1187>`_ (submitted by ryanworl)
* Python: Python signal handling didn't work when waiting on a future. In particular, pressing Ctrl-C would not successfully interrupt the program. `(PR #1138) <https://github.com/apple/foundationdb/pull/1138>`_

Other Changes
-------------

* Migrated to Boost 1.67. `(PR #1242) <https://github.com/apple/foundationdb/pull/1242>`_
* IPv4 address in trace log filename is no longer zero-padded. `(PR #1157) <https://github.com/apple/foundationdb/pull/1157>`_
* The ``process_behind`` error can now be thrown by clients and is treated as retryable. [6.1.1] `(PR #1438) <https://github.com/apple/foundationdb/pull/1438>`_.

Fixes only impacting 6.1.0+
---------------------------

* The ``consistencycheck`` fdbserver role would repeatedly exit. [6.1.1] `(PR #1437) <https://github.com/apple/foundationdb/pull/1437>`_
* The ``consistencycheck`` fdbserver role could proceed at a very slow rate after inserting data into an empty database. [6.1.2] `(PR #1452) <https://github.com/apple/foundationdb/pull/1452>`_
* The background actor which removes redundant teams could leave data unbalanced. [6.1.3] `(PR #1479) <https://github.com/apple/foundationdb/pull/1479>`_
* The transaction log spill-by-reference policy could read too much data from disk. [6.1.5] `(PR #1527) <https://github.com/apple/foundationdb/pull/1527>`_
* Memory tracking trace events could cause the program to crash when called from inside a trace event. [6.1.5] `(PR #1541) <https://github.com/apple/foundationdb/pull/1541>`_
* TLogs will replace a large file with an empty file rather than doing a large truncate operation. [6.1.5] `(PR #1545) <https://github.com/apple/foundationdb/pull/1545>`_
* Fix PR #1545 to work on Windows and Linux. [6.1.6] `(PR #1556) <https://github.com/apple/foundationdb/pull/1556>`_
* Adding a read conflict range for the metadata version key no longer requires read access to the system keys. [6.1.6] `(PR #1556) <https://github.com/apple/foundationdb/pull/1556>`_
* The TLog's disk queue files would grow indefinitely after a storage server was removed from the cluster. [6.1.8] `(PR #1617) <https://github.com/apple/foundationdb/pull/1617>`_

Earlier release notes
---------------------
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
