###################
Release Notes - 1.0
###################

1.0.1
=====

* Fix segmentation fault in client when there are a very large number of dependent operations in a transaction and certain errors occur.

1.0.0
=====

After a year and a half of Alpha and Beta testing, FoundationDB is now commercially available. Thanks to the help of the thousands of Alpha and Beta testers in our community, we believe that this release is highly robust and capable.

You can now find pricing and order enterprise licenses online.

The new Community License now permits free-of-charge use for production systems with up to 6 server processes and for non-production systems with an unlimited number of processes.

There are only minor technical differences between this release and the 0.3.0 release of August 7, 2013:

Java
----

* ``clear(Range)`` replaces the now deprecated ``clearRangeStartsWith()``.

Python
------

* Windows installer supports Python 3.

Node and Ruby
-------------

* String option parameters are converted to UTF-8.
 
All
---

* API version updated to 100. See the :ref:`API version upgrade guide <api-version-upgrade-guide-100>` for upgrade details.
* Runs on Mac OS X 10.7.
* Improvements to installation packages, including package paths and directory modes.
* Eliminated cases of excessive resource usage in the locality API.
* Watches are disabled when read-your-writes functionality is disabled.
* Fatal error paths now call ``_exit()`` instead instead of ``exit()``.

Fixes
-----

* A few Python API entry points failed to respect the ``as_foundationdb_key()`` convenience interface.
* ``fdbcli`` could print commit version numbers incorrectly in Windows.
* Multiple watches set on the same key were not correctly triggered by a subsequent write in the same transaction.


Beta 3
======

The Beta 3 release focuses on major improvements across our language APIs, including new capabilities for locality, watches, transaction cancellation and timeouts, explicit conflict ranges, and atomic operations. It also improves performance and removes known limitations.

Features
--------

* Discover where keys are physically stored using the new :ref:`locality <api-python-locality>` API.
* Create :ref:`watches <api-python-watches>` that asynchronously report changes to the values of specified keys.
* :ref:`Cancel <api-python-cancel>` transactions or set them to automatically :ref:`timeout <api-python-timeout>` and cancel.
* Explicitly add read or write :ref:`conflict ranges <api-python-conflict-ranges>`.
* Perform :ref:`atomic operations <api-python-transaction-atomic-operations>` that transform a value (e.g. incrementing it) without client reads to avoid transaction conflicts.
* API version updated to 23.

Java
----

Based on customer feedback and internal testing, the API has been significantly revised for increased performance and ease of use. This is a **breaking** API change. We will continue to make the previous JAR
available for the time being.

* The asynchronous programming library has been moved to its own package (``com.foundationdb.async``). The library has a host of new members for greater flexibility and more predictable error handling.
* ``Database.run(...)`` can now return an arbitrary object from user code, simplifying use of this recommended retry loop.
* The new interface ``Function`` replaces several interfaces: ``Mapper``, ``Block``, ``Retryable``, and ``AsyncRetryable``.
* Added the ability to cancel any ``Future`` instance, even one not backed with native resources.
* Removed ``onSuccess()`` and ``onFailure()`` in favor of ``map()`` and ``flatMap()``. If code needs simple triggering, ``onReady()`` is still available.
* Range iteration via ``Transaction.getRange(...)`` starts fetching data immediately upon invocation. This simplifies development of code that reads ranges in parallel.
* Many other changes that facilitate writing fast, efficient, and correct Java applications!

Python
------

* Python API methods that :ref:`accept a key <api-python-keys>` will also accept a Python object with an ``as_foundationdb_key()`` method that returns a key. Likewise, methods that accept a value will also accept a Python object with an ``as_foundationdb_value()`` method that returns a value.

Performance
-----------

* Clients can preferentially communicate with servers on the same machine or in the same datacenter for :ref:`location-aware load balancing <api-python-database-options>`.
* Removed from the client library debugging code included in versions up through Beta 2, leading to higher, more predictable performance.
* Improved data distribution algorithms to optimize data movement during failure scenarios.
* Improved range-read iterators in Node.js using lazy evaluation.
* Improved client-side range-read prefetching in Node.js, Ruby, and Python.
* Incrementally improved performance across all language bindings.

Fixes
-----

* A storage node could be prevented from rejoining the cluster until the process was restarted.
* A reverse ``GetRange`` request using a row limit and an end key selector that enters the system keyspace could return too few results.
* A machine power loss immediately following a process restart could result in an invalid transaction log.
* ``GetRange`` could improperly cache too large a range of data when the end key selector resolved past the end of user keyspace, temporarily resulting in incorrect answers to read requests.
* In Node.js, reusing a range iterator for a second request could result in an incomplete result set.


Beta 2
======

Features
--------

* ``fdbcli`` history is stored between sessions; consecutive duplicate commands are stored as a single history entry
* The ``fdbcli`` tool prints a minimal cluster status message if an operation does not complete in 5 seconds.

Performance
-----------

* Support for databases up to 100TB (aggregate key-value size). We recommend you contact us for configuration suggestions for databases exceeding 10TB.
* Reduced client CPU usage when returning locally cached values.
* Clients do not write to the database if a value is set to its known current value.
* Improved transaction queuing behavior when a significant portion of transactions are "System Immediate" priority.
* Reduced downtime in certain server-rejoin situations.

Language APIs
-------------
	
* All

	* The API version has been updated from 21 to 22. (Thanks to our API versioning technology, programs requesting earlier API versions will work unmodified.) There are no changes required to migrate from version 21 to 22.
	* The ``open()`` call blocks until the client can communicate with the cluster.

* Node.js

	* Support for Node.js v0.10.x.
	* Functions throw errors of type ``FDBError``.
	* Removed some variables from the global scope.

* Java

	* Compiles class files with 1.6 source and target flags.
	* Single-jar packaging for all platforms. (In rare cases, setting the ``FDB_LIBRARY_PATH_FDB_JAVA`` environment variable will be required if you previously relied on loading the library from a system path.)

* Ruby
   
	* Support for Ruby on Windows. Requires Ruby version at least 2.0.0 (x64).
	* Added implementation of ``on_ready()``.
	
Fixes
-----

* Coordinators could fail to respond if they were busy with other work.
* Fixed a rare segmentation fault on cluster shutdown.
* Fixed an issue where CLI status could sometimes fail.
* Status showed the wrong explanation when performance was limited by system write-to-read latency limit.
* Fixed a rare issue where a "stuck" process trying to participate in the database could run out of RAM.
* Increased robustness of FoundationDB server when loaded with large data sets.
* Eliminated certain cases where the data distribution algorithm could do unnecessary splitting and merging work.
* Several fixes for rare issues encountered by our fault simulation framework.
* Certain uncommon usage of on_ready() in Python could cause segmentation faults.


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


Alpha 6
=======

Platform support
----------------

* FoundationDB now supports both clients and development servers on :doc:`Mac OS X </getting-started-mac>`.

* FoundationDB now supports both clients and development servers on (64-bit) Windows.

* All language APIs are supported on Linux, Mac, and Windows (except for Ruby on Windows, because there is not a 64-bit Ruby for Windows.)

Features
--------

* The set of coordination servers can be safely :ref:`changed <configuration-changing-coordination-servers>` on-the-fly via the CLI.

* Unintentional deletion of the coordination state files is now ACID-safe and self-correcting when a majority of the state files still exist. 

* The :ref:`foundationdb.conf <foundationdb-conf>` file format has changed.

* A new more flexible and automatic system for :ref:`network configuration <foundationdb-conf-fdbserver>`. Common server setups will auto-configure using the cluster file. More advanced setups are supported via separate configurable listen and public addresses.

* The CLI now support tab-completion.

* The CLI now supports setting transaction options

* The CLI has a new command "getrangekeys" that returns the keys in a range and omits the values.

* The database size estimate shown in the CLI status is much more accurate.

Performance
-----------

* Improved latency performance for intense workloads with range-read operations.

* Improved performance and decreased memory usage for certain intense write workloads targeting a small set of keys (such as sequential insert).

Fixes
-----

* An incorrect result could be returned by a range read when: (1) The range start was specified using a non-default "less than" type key selector; and (2) the range read started at the beginning of the database; and (3) the transaction also included a prior write to a key less than the key of the begin key selector.

* In certain cases a FoundationDB cluster would not correctly re-configure itself to achieve a more optimal usage of servers of specific machine classes.

Changes to all APIs
-------------------

* The API version has been updated from 14 to 16. (Thanks to our API versioning technology, programs requesting API version 14 will work unmodified.)

* Calling the :py:meth:`reset <fdb.Transaction.reset>` method of a transaction now also resets transaction options.

* :ref:`System keys <system-keys>` (those beginning with the byte ``0xFF``) are now inaccessible by default.

* Simpler network setup: The network connection options are no longer necessary and have been deprecated.

* Three new transaction options (:py:meth:`READ_AHEAD_DISABLE <fdb.Transaction.options.set_read_ahead_disable>`, :py:meth:`READ_YOUR_WRITES_DISABLE <fdb.Transaction.options.set_read_your_writes_disable>`, and :py:meth:`ACCESS_SYSTEM_KEYS <fdb.Transaction.options.set_access_system_keys>`) enable more control for advanced applications.

Changes to the Java API
-----------------------

* A new construct `AsyncUtil.whileTrue() <../javadoc/com/apple/cie/foundationdb/async/AsyncUtil.html#whileTrue-com.apple.foundationdb.async.Function->`_ simplifies writing loops using the asynchronous version of the Java FDB client.


Alpha 5
=======

Language support
----------------

* FoundationDB now supports :doc:`Ruby </api-ruby>`

* FoundationDB now supports Node.js

* FoundationDB now supports `Java </javadoc/index.html>`_ and other JVM languages.

.. _alpha-5-rel-notes-features:

Features
--------

* A new :doc:`backup </backups>` system allows scheduled backups of a snapshot of the FoundationDB database to an external filesystem.
	
* :doc:`Integrated HTML documentation </index>`

* :ref:`Snapshot reads <snapshot isolation>` allow API clients to selectively relax FoundationDB's strong isolation guarantee. Appropriate use can of them can reduce :ref:`conflict-ranges` but makes reasoning about concurrency harder.

* :ref:`Streaming modes <streaming-mode-python>` allow API clients to adjust how FoundationDB transfers data for range reads for improved performance.

* Client APIs automatically detect the appropriate network interface (local address) when connecting to a cluster, and will look for a :ref:`default-cluster-file`.

Compatibility
-------------

* Tuples encoded with prior alpha versions are incompatible with the tuple layer in Alpha 5.

* Databases created with Alpha 4 will be compatible. (See :ref:`Upgrading from older versions <upgrading-from-older-versions>` for upgrade instructions)

* Databases created before Alpha 4 will be incompatible. (See :ref:`Upgrading from older versions <upgrading-from-older-versions>` for details)

Changes to all APIs
-------------------

* The API version has been updated to 14.

* :ref:`Snapshot reads <snapshot isolation>` (see :ref:`Features <alpha-5-rel-notes-features>`, above).

* :ref:`Streaming modes <streaming-mode-python>` (see :ref:`Features <alpha-5-rel-notes-features>`, above).

* Automatic network interface detection (see :ref:`Features <alpha-5-rel-notes-features>`, above).

* The tuple layer supports unicode strings (encoded as UTF-8), has a more compact encoding, and is not compatible with data from prior versions.

* Reversed range reads are now exposed through a separate parameter rather than via a negative ``limit``.

* Extensible options are now exposed at the network, cluster, database and transaction levels. The parameters to :c:func:`fdb_setup_network` and :py:func:`fdb.init` have been replaced by network options.

* Option enumerations are available in a machine-readable format for the benefit of third-party language binding developers.

Python API changes
------------------

* :py:func:`fdb.open` can be called with no parameters to use the :ref:`default-cluster-file`.

* Waiting on a Future object has changed from ``.get()`` to :py:meth:`.wait() <fdb.Future.wait>`

* Reversed range reads can by specified by passing a slice object with a -1 step.

* The convenience read methods on :py:class:`fdb.Database` are now transactional.

C API changes
-------------

* Byte limits exposed in :c:func:`fdb_transaction_get_range`.  These are not currently exposed by any of the higher level clients (and usually streaming modes should be preferred).

* :c:func:`fdb_future_get_keyvalue_array` returns an explicit flag indicating whether there is more data in the range beyond the limits passed to :c:func:`fdb_transaction_get_range`.
 
* ``fdb_transaction_get_range_selector`` has been eliminated - :c:func:`fdb_transaction_get_range` always takes key selectors.
