#############
Release Notes
#############

5.0.8
=====

Features
--------

* Support upgrades from 3.0 versions.

Fixes
-----

* Blob backups didn't retry 429 errors. 429 errors do not count against the retry limit.
* Status was not correctly populating all configuration fields.

5.0.7
=====

Fixes
-----

* Blob backups became corrupt when handling non-retryable errors. <rdar://problem/35289547>
* Blob backup did not retry all http errors correctly. <rdar://problem/34937616> 

5.0.6
=====

5.0.5
=====

Fixes
-----

* Set a default memory limit of 8GB on all backup and DR executables. This limit is configurable on the command line. <rdar://problem/34744417>
* The backup agent would keep attempting to write a file to blob for up to an hour after the task was cancelled. <rdar://problem/34745079>
* Incorrect blob backup destination URLs could be parsed as correct but missing IP addresses. <rdar://problem/34751574>
* Blob load balancing and per address connection limits have been improved. <rdar://problem/34744419>
* Fdbmonitor now supports 0-parameter flags. <rdar://problem/34738924>
* The read latencies reported in status were higher than what clients observed. <rdar://problem/33877094>

5.0.4
=====

Fixes
-----

* Logs continued to make their data persistent to disk after being removed. <rdar://problem/33852607>
* Removed logs did not delete their data before shutting down. <rdar://problem/33852342>
* In rare scenarios, a disk error which occurred during log recruitment could cause the recruitment to hang indefinately.

5.0.3
=====

Fixes
-----

* In rare scenarios, recovery could get stuck for 10 minutes. <rdar://problem/33782338> <rdar://problem/33780273>
* The consistency check did not work on locked databases. <rdar://problem/33241411>
* In rare scenarios, backup, DR, or fdbcli could hang indefinitely. <rdar://problem/33763769>
* Some transaction log metrics were not being reported. <rdar://problem/30313222>
* Some network metrics were reported incorrectly as extremely large numbers. <rdar://problem/32364301> <rdar://problem/32363905>

5.0.2
=====

Fixes
-----

* Functionality to slowly delete large files with incremental truncation was not enabled. <rdar://problem/33550683>
* Fixed a source of crashes from fdbcli. <rdar://problem/32933471>
* Data distribution was prematurely reporting that it had started.

Bindings
--------

* Go: Use fully-qualified import paths for fdb dependencies. <rdar://problem/32932617>

Other
-----

* Publish header files and static libraries for flow and flow bindings on Linux and macOS. <rdar://problem/33191326>

5.0.1
=====

Fixes
-----

* Bytes input and bytes durable on the log would drift apart due to rounding errors.

5.0.0
=====

Features
--------

* All recoveries no longer copy log data before completion. As a result, the fast_recovery_double and fast_recovery_triple configurations have been removed. <rdar://problem/30235865>
* Added a new configuration ``three_data_hall`` where a single data hall failure cannot bring down a cluster. <rdar://problem/30822968>
* Multiple log processes can be within the same zone. <rdar://problem/29407578>
* Clients have access to sampled latency statistics for their operations. <rdar://problem/29757812>
* Added network checksums. <rdar://problem/30703358>
* Fault tolerance is restored much quicker after a storage server failure. <rdar://problem/30125038>

Performance
-----------

* Improved recovery speed after rebooting the cluster. <rdar://problem/32956590>
* Improved saturation performance of write-heavy workloads. <rdar://problem/30381001>
* We no longer require extra log durability for fast recoveries. <rdar://problem/30235865>
* Backup/DR now use far less cluster resources while idle. <rdar://problem/28374226> <rdar://problem/28640412>
* Improved load balancing performance. <rdar://problem/29289012>
* Reduced conflict range sizes when performing get range queries with key selectors such that the resolved begin key is greater than or equal to the resolved end key. <rdar://problem/30561532>
* Added functionality to slowly delete large files with incremental truncation. <rdar://problem/30193500>

Fixes
-----

* Fixed a pathology where multiple successive failures could lead to a long 30+ minute availability outage. <rdar://problem/30235865>
* Updated status to have failures of old tlogs included in the failure tolerance calculation. <rdar://problem/30615411>
* The fdbserver and fdbbackup processes could return a successful error code after a fatal error. <rdar://problem/31350017>
* Fault tolerance did not reflect coordinators sharing the same machine ID. <rdar://problem/31195167>
* Prevent the DR seconds behind measurement from potentially returning a negative amount. <rdar://problem/32235105>
* Increased the priority of all cluster controller work to prevent the cluster controller from being starved by other work on the same process. <rdar://problem/32958023>
* Fixed a rare crash in the DR agent. <rdar://problem/30766452>
* fdbcli and fdb_c clients logs had 0 values for most ProcessMetrics log event fields. <rdar://problem/31017524>
* DR could get stuck if the time required to copy range data was longer than the task timeout. <rdar://problem/32958570>

Status
------

* Improved latency probe accuracy when the cluster is loaded. <rdar://problem/30465855>
* Report GRV latencies at all priorities in the latency probe. <rdar://problem/30465855>
* For the SSD storage engine, available disk space now includes space within data files that is not currently in use and can be reused. <rdar://problem/29998454>
* Storage servers report how far they are lagging behind the logs. ``fdbcli`` now reports servers that are lagging sufficiently far behind. <rdar://problem/30166503>
* Status json "incompatible_connections" did not work with multiversion clients. <rdar://problem/28396098>
* Added connection counts and establish/close metrics to status json. <rdar://problem/28393970>

Bindings
--------

* API version updated to 500. See the :ref:`API version upgrade guide <api-version-upgrade-guide-500>` for upgrade details.  
* Tuples now support single- and double-precision floating point numbers, UUIDs, booleans, and nested tuples. <rdar://problem/30053926>
* Add ``TRANSACTION_LOGGING_ENABLE`` transaction option that causes the details of a transaction's operations to be logged to the client trace logs. <rdar://problem/32074484>
* Add ``USED_DURING_COMMIT_PROTECTION_DISABLE`` transaction option that prevents operations performed during that transaction's commit from causing the commit to fail. <rdar://problem/30378251>
* Add ``ENABLE_SLOW_TASK_PROFILING`` network option that logs backtraces for long running flow tasks. <rdar://problem/30975759>
* ``getBoundaryKeys`` can be used on locked databases. <rdar://problem/28760070>
* Flow: API versions prior to 500 are no longer supported. <rdar://problem/32433458>
* Flow: ``Cluster::createDatabase`` no longer takes a DB name parameter. <rdar://problem/32433458>
* Node: API versions prior to 500 are no longer supported. <rdar://problem/32433437>
* Node: ``fdb.open`` and ``Cluster.openDatabase`` no longer take a DB name parameter. <rdar://problem/32433437>
* Java: API versions prior to 500 are no longer supported. <rdar://problem/30378251>
* Java: ``FDB.open`` and ``Cluster.openDatabase`` no longer take a DB name parameter. <rdar://problem/32078379>
* Java: Removed ``Transaction.reset`` from the API. <rdar://problem/32409970>
* Java: ``Transaction.onError`` invalidates its ``Transaction`` and asynchronously returns a new ``Transaction`` to replace it. <rdar://problem/30378251>
* Java: Transactions always enable the ``USED_DURING_COMMIT_PROTECTION_DISABLE`` transaction option, preventing operations that occur during a commit from causing the commit to fail. <rdar://problem/30378251>
* Java: There are now options to set the executor for async call backs at the database and transaction level. <rdar://problem/31636701>
* Java: Static functions that perform async operations now have overloads that allow the user to specify an executor. <rdar://problem/26143365>
* Java: Range class now implements equals, toString, and hashCode methods. <rdar://problem/31790542>
* Java: Tuples now expose a "stream" method to get a stream of their objects and "fromStream" to convert streams back into tuples. <rdar://problem/31767147>
* Java: Addressed a pathology that made AsyncUtil.whileTrue susceptible to long chains of futures. <rdar://problem/30054445>

Other Changes
-------------

* Added the ``-v``/``--version`` flag to report version information for the ``fdbcli`` binary <rdar://problem/31091644>
* Introduced the ``data_filesystem`` command line argument for the ``fdbserver`` binary to prevent data from being written to the root drive. <rdar://problem/30716138>
* Added a ``ClientStart`` trace event to client trace files with details about the client library being used.
* fdbserver now rejects all unrecognized command-line arguments. <rdar://problem/31853278>
* All fdbserver command-line options now have both short- and long-form equivalents. <rdar://problem/31853278>

Earlier release notes
---------------------
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
