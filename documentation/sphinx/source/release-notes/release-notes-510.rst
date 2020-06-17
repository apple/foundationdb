#############
Release Notes
#############

5.1.7
=====

Fixes
-----

* fdbdr switch could take a long time to complete if the two clusters were not created at the same time. <rdar://problem/37551521>

5.1.6
=====

Fixes
-----

* Expiring a backup could cause the fdbbackup process to hang indefinitely. <rdar://problem/39382121>

5.1.5
=====

Fixes
-----

* The consistency check calculated the size of the database inefficiently. <rdar://problem/38385230>
* Could not create new directories with the Python and Ruby implementations of the directory layer. <rdar://problem/38911902> <rdar://problem/38477474>
* fdbcli could erroneously report that it was incompatible with some processes in the cluster. <rdar://problem/39353867>
* The commit commmand in fdbcli did not wait for the result of the commit before continuing to the next command.


Other Changes
-------------

* renamed the ``multi_dc`` replication mode to ``three_datacenter``.

5.1.4
=====

Fixes
-----

* The master would recover twice when a new cluster controller was elected. <rdar://problem/38305649>
* The cluster controller could be elected on a storage process after restarting all processes in a cluster. <rdar://problem/37946424>
* Allow backup expiration to succeed if the backup is too new to be restorable. <rdar://problem/38237313>
* Process metric collection in status could sometimes fail. <rdar://problem/38311829>

5.1.3
=====

Fixes
-----

* The backup agents ran out of memory when heavily loaded. <rdar://problem/37509745>
* Storage servers were not marked as failed until after their files were deleted. <rdar://problem/38266562>
* The consistency check requested too many shards in the same request from the proxy. <rdar://problem/37326268>
* Client knobs for blob send/receive were reversed in meaning. <rdar://problem/37945529>
* fdbbackup status provides more information on reported errors. <rdar://problem/36200858> <rdar://problem/37461836>

5.1.2
=====

Fixes
-----

* Backup did not incrementally delete mutations from the mutation log. <rdar://problem/37609229>
* fdbcli status misreported completed backup/DR as running. <rdar://problem/37608661>
* Stopped producing the "fdbblob" alias for fdbbackup. <rdar://problem/37244632>

5.1.1
=====

Fixes
-----

* Bindings: Disposing a transaction during a commit resulted in a broken promise from ``get_versionstamp``. <rdar://problem/35835272>
* Bindings: Calling ``create_cluster`` before initializing the network would result in a crash. <rdar://problem/35563243>
* Latest restorable version of a running backup was not being updated in backup layer status. <rdar://problem/37288715>
* Backup layer status would sometimes show an error or an incorrect value for the recent blob bandwidth metric. <rdar://problem/37288943>
* Backup deletions were not deleting all of the files related to the backup. <rdar://problem/37756550>
* The cluster controller was sharing a process with the master even when better locations existed. <rdar://problem/37318611>
* Blob credentials files were being opened in read-write mode.
* Sometimes fdbbackup did not write log files even when ``--log`` was passed on the command line. <rdar://problem/36259384>

Performance
-----------

* Backup file uploads will respond to server-side throttling in the middle of a chunk upload rather than only between chunks. <rdar://problem/37245992>

5.1.0
=====

Features
--------

* Backups continually write snapshots at a configured interval, reducing restore times for long running backups. <rdar://problem/25512772>
* Old backup snapshots and associated logs can be deleted from a backup. <rdar://problem/25512772>
* Backup files are stored in a deep folder structure. <rdar://problem/27723412>
* Restore allows you to specify an approximate time instead of a version. <rdar://problem/34557380>
* Backup and DR agents can be paused from ``fdbbackup`` and ``fdbdr`` respectively. <rdar://problem/34776039>
* Added byte min and byte max atomic operations. <rdar://problem/29255441>
* The behavior of atomic "and" and "min" operations has changed when the key doesn't exist in the database. If the key is not present, then an "and" or "min" is now equivalent to a set. <rdar://problem/29255441>
* Exception messages are more descriptive. <rdar://problem/33665340> 
* Clients can view a sample of committed mutations. <rdar://problem/33324935>
* When switching to a DR cluster, the commit versions on that cluster will be higher than the versions on the primary cluster. <rdar://problem/33572665>
* Added a read-only lock aware transaction option. <rdar://problem/34579176>
* Automatically suppress trace log events which occur too frequently. <rdar://problem/33764208>
* Added a new ``multi_dc`` replication mode designed for cross data center deployments. <rdar://problem/36489132>

Performance
-----------

* The data distribution algorithm can split the system keyspace. <rdar://problem/29932360>
* Improved load balancing when servers are located across multiple data centers. <rdar://problem/34213649>
* Improved read latencies after recoveries by only making servers responsible for keys if they have finished copying the data from other servers. <rdar://problem/34697182>
* Improved recovery times by waiting until a process has finished recovering its data from disk before letting it be recruited for new roles. <rdar://problem/32000146> <rdar://problem/34212951> 
* Improved 95% read version latencies by reducing the number of logs required to confirm that a proxy has not been replaced. <rdar://problem/33196298>
* Stopped the transaction logs from copying unneeded data after multiple successive recoveries. <rdar://problem/36488946>
* Significantly improved the performance of range reads. <rdar://problem/33926224>
* The cluster controller prefers to be recruited on stateless class processes and will not put other stateless roles on the same process. <rdar://problem/35155324>
* Excluded servers no longer take on stateless roles. <rdar://problem/27110802>
* Stateless roles will be proactively moved off of excluded processes. <rdar://problem/27110802> <rdar://problem/35155044>
* Dramatically improved restore speeds of large disk queue files. <rdar://problem/35567320>
* Clients get key location information directly from the proxies, significantly reducing the latency of worst case read patterns. <rdar://problem/35953920>
* Reduced the amount of work incompatible clients generate for coordinators and the cluster controller. In particular, this reduces the load on the cluster caused by using the multi-version client. <rdar://problem/30897631> 
* Pop partially recovered mutations from the transaction log to save disk space after multiple successive recoveries. <rdar://problem/33755270>
* Stopped using network checksums when also using TLS. <rdar://problem/32157852>
* Improved cluster performance after recoveries by prioritizing processing new mutations on the logs over copying data from the previous logs. <rdar://problem/36489337>
* Backup agents prefer reading from servers in the same data center. <rdar://problem/34213617>

Fixes
-----

* New databases immediately configured into ``three_data_hall`` would not respect the ``three_data_hall`` constraint. <rdar://problem/34415440>
* Exclude considered the free space of non-storage processes when determining if an exclude was safe.
* ``fdbmonitor`` failed to start processes after fork failure. <rdar://problem/34743257> 
* ``fdbmonitor`` will only stop processes when the configuration file is deleted if ``kill_on_configuration_change`` is set. <rdar://problem/35497412>
* The data distribution algorithm would hang indefinitely when asked to build storage teams with more than three servers.
* Mutations from a restore could continue to be applied for a very short amount of time after a restore was successfully aborted.

Extremely Rare Bug Fixes
------------------------

* Storage servers did not properly handle rollbacks to versions before their restored version.
* A newly recruited transaction log configured with the memory storage engine could crash on startup.
* The data distribution algorithm could split a key range so that one part did not have any data.
* Storage servers could update to an incorrect version after a master failure.
* The disk queue could report a commit as successful before the sync of the disk queue files completed.
* A disk queue which was shutdown before completing its first commit could become unrecoverable.

Status
------

* If a cluster cannot recover because too many transaction logs are missing, status lists the missing logs. <rdar://problem/34965531>
* The list of connected clients includes their trace log groups. <rdar://problem/33779874>
* Status reports if a cluster is being used as a DR destination. <rdar://problem/34971187>

Bindings
--------

* API version updated to 510. See the :ref:`API version upgrade guide <api-version-upgrade-guide-510>` for upgrade details.  
* Add versionstamp support to the Tuple layer in Java and Python. <rdar://problem/25560444>

Java
----

* API versions prior to 510 are no longer supported.
* The bindings have been moved to the package ``com.apple.foundationdb`` from ``com.apple.cie.foundationdb``. <rdar://problem/33271641>
* We no longer offer a version of the Java bindings with our custom futures library or support Java versions less than 8. The bindings that use completable futures have been renamed to ``fdb-java``. <rdar://problem/35029630>
* Finalizers now log a warning to stderr if an object with native resources is not closed. This can be disabled by calling ``FDB.setUnclosedWarning()``. <rdar://problem/35421530>
* Implementers of the ``Disposable`` interface now implement ``AutoCloseable`` instead, with ``close()`` replacing ``dispose()``.
* ``AutoCloseable`` objects will continue to be closed in object finalizers, but this behavior is being deprecated. All ``AutoCloseable`` objects should be explicitly closed. <rdar://problem/35421530>
* ``AsyncIterator`` is no longer closeable. <rdar://problem/35595971>
* ``getBoundaryKeys()`` now returns a ``CloseableAsyncIterable`` rather than an ``AsyncIterator``. <rdar://problem/35421530>
* ``Transaction.getRange()`` no longer initiates a range read immediately. Instead, the read is issued by a call to ``AsyncIterable.asList()`` or ``AsyncIterable.iterator()``. <rdar://problem/35595971>
* Added ``hashCode()`` method to ``Subspace``. <rdar://problem/35125601>
* Added thread names to threads created by our default executor. <rdar://problem/36077166>
* The network thread by default will be named ``fdb-network-thread``. <rdar://problem/36077166>
* Added an overload of ``whileTrue()`` which takes a ``Supplier``. <rdar://problem/35096338>
* Added experimental support for enabling native callbacks from external threads. <rdar://problem/33300740>
* Fix: Converting the result of ``Transaction.getRange()`` to a list would issue an unneeded range read. <rdar://problem/35325444>
* Fix: range iterators failed to close underlying native resources. <rdar://problem/35595971>
* Fix: various objects internal to the bindings were not properly closed. <rdar://problem/35541447>

Other Changes
-------------

* Backups made prior to 5.1 can no longer be restored. <rdar://problem/25512772>
* Backup now uses a hostname in the connection string instead of a list of IPs when backing up to blob storage. This hostname is resolved using DNS. <rdar://problem/34093405> 
* ``fdbblob`` functionality has been moved to ``fdbbackup``. <rdar://problem/25512772>
* ``fdbcli`` will warn the user if it is used to connect to an incompatible cluster. <rdar://problem/33363571>
* Cluster files that do not match the current connection string are no longer corrected automatically. <rdar://problem/35129575>
* Improved computation of available memory on pre-3.14 kernels. <rdar://problem/35336487>
* Stopped reporting blob storage connection credentials in ``fdbbackup`` status output. <rdar://problem/31483629>

Earlier release notes
---------------------
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
