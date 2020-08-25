#############
Release Notes
#############

Beta 1
======

Platform support
----------------

* Added AWS CloudFormation support for FoundationDB.

Features
--------

* Servers can be safely :ref:`removed <removing-machines-from-a-cluster>` from the cluster.

* :ref:`Improved status <administration-monitoring-cluster-status>` with information about database configuration, health, workload, and performance.

* Improved resiliency against low disk space conditions.

* The CLI can automatically choose :ref:`coordination servers <configuration-changing-coordination-servers>`.

* The CLI allows multiple semicolon separated commands per line; a new --exec flag was added to the CLI to pass commands to the CLI and quit when done.

* Old :ref:`log files <administration-managing-trace-files>` are automatically deleted.

* More specific :ref:`error codes <developer-guide-error-codes>`.

Performance
-----------

* Reduced latency of getRange when iterating through large amounts of data.

* Reduced idle CPU usage.

* Java API: Join in ArrayUtil is efficient for all container types.

* Java API: Optimized tuple creation.

Changes to all APIs
-------------------

* The API version has been updated from 16 to 21. (Thanks to our API versioning technology, programs requesting earlier API versions will work unmodified.) There are no changes required to migrate from version 16 to 21.

Fixes
-----

* Commit could return the error commit_conflict (renamed to not_committed) after the transaction successfully committed. (This was previously documented as a known limitation.)

* If a call to commit returned an error, but onError was not called, the transaction would not be reset.

* The memory storage engine was too aggressive in reserving disk space.

* If a key selector in a getRange resolved to the beginning or the end of the database, then its transaction may not have correctly conflicted with other transactions.

* Ranges passed to clearRange and getRange with the begin key larger than the end could incorrectly cause client API errors.

* Databases with small amounts of data in them (~20000 bytes) would sometimes slowly move data back and forth between the servers.

* Large network latencies (> ~250 ms) could impede data balancing between servers.

* Setting callbacks or calling ``blockUntilReady`` on a future from multiple threads resulted in an error.

* If a machine running the memory storage engine was killed multiple times in close succession, data loss might occur.

* C: The headers were not standards compliant and would not compile in some environments.

* Ruby: API versions were not checked for validity.

* Windows: The server could crash on non-English versions of Windows.
  
* Windows: Manually running fdbserver.exe could fail because of overly restrictive permissions set on shared resources.

* OS X: Java client had an extraneous linker dependency.

* Java: In multithreaded conditions, getRange and AsyncUtil.whileTrue() could sometimes never return.
 
* Python/Ruby: In multithreaded conditions, the client worker thread could crash.

Earlier release notes
---------------------

* :doc:`Alpha 6 (API Version 16) <release-notes-016>`
* :doc:`Alpha 5 (API Version 14) <release-notes-014>`
