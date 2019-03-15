#############
Release Notes
#############

6.1.0
=====

Features
--------

* Improved replication mechanism, a new hierarchical replication technique that further significantly reduces the frequency of data loss events even when multiple machines (e.g., fault-tolerant zones in the current code) permanently fail at the same time.  `(PR #964) <https://github.com/apple/foundationdb/pull/964>`_.
* Added background actor to remove redundant teams from team collection so that the healthy team number is guaranteed not exceeding the desired number. `(PR #1139) <https://github.com/apple/foundationdb/pull/1139>`_
* Show the number of connected coordinators per client in JSON status `(PR #1222) <https://github.com/apple/foundationdb/pull/1222>`_
* Get read version, read, and commit requests are counted and aggregated by server-side latency in configurable latency bands and output in JSON status. `(PR #1084) <https://github.com/apple/foundationdb/pull/1084>`_
* Added configuration option to choose log spilling implementation `(PR #1160) <https://github.com/apple/foundationdb/pull/1160>`_
* Added configuration option to choose log system implementation `(PR #1160) <https://github.com/apple/foundationdb/pull/1160>`_
* Batch priority transactions are now limited separately by ratekeeper and will be throttled at lower levels of cluster saturation. This makes it possible to run a more intense background load at saturation without significantly affecting normal priority transactions. It is still recommended not to run excessive loads at batch priority. `(PR #1198) <https://github.com/apple/foundationdb/pull/1198>`_
* Restore now requires the destnation cluster to be specified explicitly to avoid confusion. `(PR #1240) <https://github.com/apple/foundationdb/pull/1240>`_
* Restore target version can now be specified by timestamp if the original cluster is available. `(PR #1240) <https://github.com/apple/foundationdb/pull/1240>`_
* Backup status and describe commands now have a --json output option. `(PR #1248) <https://github.com/apple/foundationdb/pull/1248>`_
* Separate data distribution out from master as a new role. `(PR #1062) <https://github.com/apple/foundationdb/pull/1062>`_
* Separate rate keeper out from data distribution as a new role. `(PR ##1176) <https://github.com/apple/foundationdb/pull/1176>`_
* Added a new atomic op `CompareAndClear`. `(PR #1105) <https://github.com/apple/foundationdb/pull/1105>`_
* Added support for IPv6. `(PR #1176) https://github.com/apple/foundationdb/pull/1178`_
* FDB can now simultaneously listen to TLS and unencrypted ports to facilitate smoother migration to TLS. `(PR #1157) https://github.com/apple/foundationdb/pull/1157`_
* Added `DISABLE_POSIX_KERNEL_AIO` knob to fallback to libeio instead of kernel async I/O (KAIO) for systems that do not support KAIO or O_DIRECT flag. `(PR #1283) https://github.com/apple/foundationdb/pull/1283`_

Performance
-----------

* Java: Succesful commits and range reads no longer create ``FDBException`` objects to reduce memory pressure. `(Issue #1235) <https://github.com/apple/foundationdb/issues/1235>`_

Fixes
-----

* Python: Creating a ``SingleFloat`` for the tuple layer didn't work with integers. `(PR #1216) <https://github.com/apple/foundationdb/pull/1216>`_
* In some cases, calling ``OnError`` with a non-retryable error would partially reset a transaction. As of API version 610, the transaction will no longer be reset in these cases and will instead put the transaction into an error state. `(PR #1298) <https://github.com/apple/foundationdb/pull/1298>`_
* Standardized datetime string format across all backup and restore command options and outputs. `(PR #1248) <https://github.com/apple/foundationdb/pull/1248>`_

Status
------

Bindings
--------

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
* Golang: Added ``fdb.Printable`` to print a human-readable string for a given byte array. Add ``Key.String()``, which converts the ``Key`` to a ``string`` using the ``Printable`` function. `(PR #1010) <https://github.com/apple/foundationdb/pull/1010>`_
* Golang: Tuples now support ``Versionstamp`` operations. `(PR #1187) <https://github.com/apple/foundationdb/pull/1187>`_
* Python: Python signal handling didn't work when waiting on a future. In particular, pressing Ctrl-C would not successfully interrupt the program. `(PR #1138) <https://github.com/apple/foundationdb/pull/1138>`_

Other Changes
-------------

* Migrated  to Boost 1.67. `(PR #1242) https://github.com/apple/foundationdb/pull/1242`_

Earlier release notes
---------------------
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
