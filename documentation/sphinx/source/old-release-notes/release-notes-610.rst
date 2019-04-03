#############
Release Notes
#############

6.1.0
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
* Added support for IPv6. `(PR #1176) <https://github.com/apple/foundationdb/pull/1178>`_
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

Performance
-----------

* Increased the get read version batch size in the client. This change reduces the load on the proxies when doing many transactions with only a few operations per transaction. `(PR #1311) <https://github.com/apple/foundationdb/pull/1311>`_
* Clients no longer attempt to connect to the master during recovery. `(PR #1317) <https://github.com/apple/foundationdb/pull/1317>`_

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

Status
------

* Report the number of connected coordinators for each client. This aids in monitoring client TLS support when enabling TLS on a live cluster. `(PR #1222) <https://github.com/apple/foundationdb/pull/1222>`_
* Degraded processes are reported in ``status json``. `(Issue #690) <https://github.com/apple/foundationdb/issues/690>`_

Bindings
--------

* API version updated to 610.
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