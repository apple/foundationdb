#############
Release Notes
#############

4.6.5
=====

Bindings
--------

* Java bindings now perform marshaling off of the network thread. <rdar://problem/32413365>


4.6.4
=====

Features
--------

* Added ability to use --io_trust_seconds in a warn-only mode, which logs a trace event rather than failing the process when a disk operation takes a long time. This is enabled with --io_trust_warn_only. <rdar://problem/32344389>

Fixes
-----

* Disk operation timeouts now cause the process to restart rather than hang indefinitely. <rdar://problem/31888796>
* ``fdbdr switch`` did not start the DR in the opposite direction correctly, resulting in mutations being lost. <rdar://problem/32598128>
* Lowered backup and DR batch sizes to avoid large packet warnings. <rdar://problem/30933203>
* Remove partial pipelining of tlog commits.

4.6.3
=====

Features
--------

* Added the ability to run a consistency check of a database using a new server role. <rdar://problem/30903086>

Fixes
-----

* Added the ability to automatically shutdown processes if a disk operation takes a long time to complete. This is enabled with --io_trust_seconds. <rdar://problem/31229332>
* Too many outstanding storage recruitment requests causes the cluster controller to hang. <rdar://problem/30271581>
* Corrected issue with Ubuntu installer package on Ubuntu 16.04 not starting daemon. <rdar://problem/27752324>
* Package non-Linux builds of JNI component into Java jars. <rdar://problem/30786246>
* Published backup-related binaries on macOS were incorrectly pointing to symbolic link specification files. <rdar://problem/31403408>

Performance
-----------

* We no longer fsync trace files. <rdar://problem/30400189>
* Lowered the default bandwidth shard splitting knobs for better performance with hot key ranges. <rdar://problem/30234328>

4.6.2
=====

Fixes
-----

* The tlog could commit more than 100MB at a time <rdar://problem/29312187>
* Metrics with filename component not present in trace events <rdar://problem/29933550>
* Setting new locality information causes missing process metrics in status details <rdar://problem/29992530>
* FDB processes killed via CLI could hang while killing themselves <rdar://problem/29518674>
* Enabled recovery of on-disk data files in the event of a very specific rare corruption situation <rdar://problem/29679886>
* Process messages get reported as errors by status, but don't get attributed to a process in the status details list <rdar://problem/29866630>
* DR prematurely reported progress for work that needed to be retried <rdar://problem/29741198>

Performance
-----------

* Storage engine performance improvements to reduce the overhead that ssd-2 requires for its benefits over ssd-1 <rdar://problem/29332661>
* Lowered the default fetch keys parallelism to slow down data distribution <rdar://problem/29934862>

4.6.1
=====

Fixes
-----

* Starting a new DR on a large database can cause the secondary cluster to lose availability <rdar://problem/29422130>
* Secondary clusters that have been upgraded were reporting "primary" metrics <rdar://problem/29407318>
* Backup and DR could get stuck if too many tasks timed out simultaneously <rdar://problem/29422234>

4.6.0
=====

Features
--------

* Added a new storage engine type ``ssd-2`` that includes page checksums and more efficient storage of large values. The previous storage engine has been renamed ``ssd-1``, and the storage engine ``ssd`` is an alias for ``ssd-2``. <rdar://problem/28565614> <rdar://problem/28723720>
* DR and Restore won't overwrite a non empty database <rdar://problem/27082102> <rdar://problem/27065780>

Performance
-----------

* Improve performance of the ssd storage engine in databases with large keys or values <rdar://problem/28701207>
* Improved cluster recovery speed <rdar://problem/28877814>
* Restore is faster due to better load leveling across the keyspace <rdar://problem/27554051>
* Reduced the conflict ranges applied for get range calls in rare cases <rdar://problem/28034705>

Fixes
-----

* Backup to Blobstore sends and verifies MD5 sums for uploads <rdar://problem/23077230>
* Backup restoration could be unrestorable in certain cases <rdar://problem/27933144>
* Clients using the multi-version client functionality would incorrectly report incompatible connections in status <rdar://problem/28396098>
* Backup and DR network metrics were incorrectly reported as 0 <rdar://problem/28589577>
* Java: fix race condition when removing an empty directory which could lead to a NoSuchElementException <rdar://problem/28858833>
* Fixed a source of potential crashes in fdbcli <rdar://problem/27063940>

Bindings
--------

* API version updated to 460. There are no behavior changes in this API version. See the :ref:`API version upgrade guide <api-version-upgrade-guide-460>` for upgrade details.

Status
------

* The following fields were added: cluster.data.moving_data.total_written_bytes, cluster.qos.limiting_queue_bytes_storage_server, cluster.qos.worst_version_lag_storage_server, cluster.qos.limiting_version_lag_storage_server, cluster.qos.transaction_per_second_limit, cluster.qos.released_transactions_per_second, cluster.qos.performance_limited_by.reason_id, and cluster.database_available

Earlier release notes
---------------------
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
