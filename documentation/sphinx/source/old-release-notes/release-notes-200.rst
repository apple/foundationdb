#############
Release Notes
#############

2.0.10
======

Release 2.0.10 is protocol-compatible with all prior 2.0.x releases. Users should continue to employ the bindings released with 2.0.0, with the exception of the following bindings:

* Java - updated to 2.0.8
* PHP - updated to 2.0.8
* Python - updated to 2.0.6

Fixes
-----

* Clients running long enough to execute 2\ :sup:`32` internal tasks could experience a reordering of client operations. The outcome of this reordering is undefined and could include crashes or incorrect behavior.
* The ``fdbcli`` command-line interface would incorrectly report an internal error when running ``coordinators auto`` if there weren't enough machines in the cluster.

2.0.9
=====

Release 2.0.9 is protocol-compatible with all prior 2.0.x releases. Users should continue to employ the bindings released with 2.0.0, with the exception of the following bindings:

* Java - updated to 2.0.8
* PHP - updated to 2.0.8
* Python - updated to 2.0.6

Fixes
-----

* Long-running clusters using the ``ssd`` storage engine could eventually deprioritize failure monitoring, causing busy machines to be considered down.

2.0.8
=====

Release 2.0.8 is protocol-compatible with all prior 2.0.x releases. Users should continue to employ the bindings released with 2.0.0, with the exception of the following bindings:

* Java - updated to 2.0.8
* PHP - updated to 2.0.8
* Python - updated to 2.0.6

Fixes
-----

* Resetting a transaction did not release its memory.
* Windows: FoundationDB client applications could crash when starting up due to a race condition.
* Ubuntu: Ubuntu software center reported that the FoundationDB package was of bad quality.

PHP
---

* Package updated to support PHP 5.4+ (instead of 5.3+).
* Fix: ``get_boundary_keys()`` could fail to complete successfully if certain retryable errors were encountered.
* Fix: Bindings set error reporting level, which could interfere with clients that used alternate settings.

Java
----

* Fix: Calling ``getRange`` on a ``Transaction`` could leak memory.
  
2.0.7
=====

Release 2.0.7 is protocol-compatible with all prior 2.0.x releases. Users should continue to employ the bindings released with 2.0.0, with the exception of the following bindings:

* Java - updated to 2.0.4
* PHP - updated to 2.0.6
* Python - updated to 2.0.6

Fixes
-----

* Updated FDBGnuTLS plugin with GnuTLS 3.2.15, incorporating the fix for `GNUTLS-SA-2014-3 <http://gnutls.org/security.html#GNUTLS-SA-2014-3>`_.
* Linux and Mac OS X: Processes configured with a 5-digit port number would listen on the wrong port.
  
2.0.6
=====

Release 2.0.6 is protocol-compatible with all prior 2.0.x releases. Users should continue to employ the bindings released with 2.0.0, with the exception of the following bindings:

* Java - updated to 2.0.4
* PHP - updated to 2.0.6
* Python - updated to 2.0.6

Fixes
-----

* Memory storage engine files could grow very large in an idle database.

Performance
-----------

* When disk bound, the storage server would use up to 90% of the disk IOPS on data balancing rather than processing new writes. As a result, the system could perform at about 10% of its maximum speed while restoring durability after a machine failure. The storage server can now use at most 50% of its disk IOPS on data balancing.

PHP
---

* Fix: ``get_boundary_keys()`` would throw an error if passed a transaction.
* Fix: Options which take an integer parameter would not use the value supplied.

Python
------

* Fix: Python 3 compatibility was broken.
* Fix: Choosing a custom Python path in the Windows installer would install to the wrong location.

2.0.5
=====

Release 2.0.5 is protocol-compatible with 2.0.0, 2.0.1, 2.0.2, 2.0.3, and 2.0.4. Users should continue to employ the bindings released with 2.0.0, with the exception of the Java bindings, which have been updated to 2.0.4, and the PHP bindings, which have been updated to 2.0.5.

Fixes
-----

* Clients and servers that specified a cluster file as a filename only (without path) could crash when the coordinators were changed.

PHP
---

* Directory layer partitions created with the PHP bindings were incompatible with other language bindings. Contact us if you have data stored in a directory partition created by PHP that can't easily be restored and needs to be migrated.

2.0.4
=====

Release 2.0.4 is protocol-compatible with 2.0.0, 2.0.1, 2.0.2, and 2.0.3. Users should continue to employ the bindings released with 2.0.0, with the exception of the Java bindings, which have been updated to 2.0.4.

Fixes
-----

* Clearing a key larger than the legal limit of 10 kB caused the database to crash and become unreadable.
* Explicitly added write conflict ranges were ignored when read-your-writes was disabled.

Java
----

* ``ByteArrayUtil.compareUnsigned()`` failed to return in some circumstances.

2.0.3
=====

Release 2.0.3 is protocol-compatible with 2.0.0, 2.0.1, and 2.0.2. There are no updates to the language bindings, so users should continue to employ the bindings released with 2.0.0.

Fixes
-----

* Updated FDBGnuTLS plugin with GnuTLS 3.2.12, incorporating fixes for `GNUTLS-SA-2014-1 <http://gnutls.org/security.html#GNUTLS-SA-2014-1>`_ and `GNUTLS-SA-2014-2 <http://gnutls.org/security.html#GNUTLS-SA-2014-2>`_.
* When inserting a large number of keys close to the key size limit, server logs were unexpectedly verbose.

2.0.2
=====

Release 2.0.2 is protocol-compatible with 2.0.0 and 2.0.1. There are no updates to the language bindings, so users should continue to employ the bindings released with 2.0.0.

Fixes
-----

* Windows: Possible database corruption when the FoundationDB service is stopped but unable to kill its child processes.

2.0.1
=====

Release 2.0.1 is protocol-compatible with 2.0.0. There are no updates to the language bindings, so users should continue to employ the bindings released with 2.0.0.

Fixes
-----

* In some cases, a server reincluded after previous exclusion would not participate in data distribution.
* Clients could not reliably connect to multiple clusters.
* The calculation of usable disk space on Linux and Mac OS X improperly included space reserved for superuser.

2.0.0
=====

New language support
--------------------

* `Go <../godoc/fdb.html>`_
* PHP

New layers available in all languages
-------------------------------------

* The :ref:`Subspace <developer-guide-sub-keyspaces>` layer provides a recommended way to define subspaces of keys by managing key prefixes.
* The :ref:`Directory <developer-guide-directories>` layer provides a tool to manage related subspaces as virtual directories. Recommended as a convenient and high-performance way to organize and layout different kinds of data within a single FoundationDB database.

Security
--------

* Added certificate-based :doc:`Transport Layer Security </tls>` to encrypt network traffic.
 
Monitoring
----------

* The ``fdbcli`` command-line interface reports information and warnings about available memory.

Performance
-----------

* Improved client CPU performance overall.
* Greatly improved client CPU performance for range-read operations.
* Greatly improved concurrency when issuing writes between reads.
* Snapshot reads are now fully cached.
* Trade off: ``get_key`` is cached, but ``get_key`` now also retrieves the value of the key, using network bandwidth. (Using ``OPTION_RYW_DISABLE`` will avoid both the cache and the network bandwidth.)
* Windows: Improved latencies.

Fixes
-----

* In rare cases when many keys very close to the maximum key size are inserted, the database could become unavailable. 
* ``GetReadVersion`` did not properly throw ``transaction_cancelled`` when called on a transaction that had been cancelled.
* When using the ``access_system_keys`` option, a ``get_range_startswith(\xff)`` would incorrectly return no results.
* ``get_range_startswith``, when invoked using a key ending in the byte ``\xff``, could return results outside the desired range.
* Linux: A process could become unresponsive if unable to find a TCP network device in ``/proc/net/snmp``.
* Destroying client threads leaked memory.
* Database availability could be unnecessarily compromised in certain rare, low-disk conditions on a "transaction" class machine.
* Writing a zero-byte value to the key ``''`` caused the database to crash.
* Mac OS X: Power loss could cause data corruption.

Other changes
-------------

* To avoid confusing situations, any use of a transaction that is currently committing will cause both the commit and the use to throw a ``used_during_commit`` error.
* The ``FDB_CLUSTER_FILE`` environment variable can point to a cluster file that takes precedence over both the current working directory and (e.g., in Linux) ``/etc/foundationdb/fdb.cluster``.
* Disabled unloading the ``fdb_c`` library to prevent consequent unavoidable race conditions.
* Discontinued testing and support for Ubuntu 11.04. We continue to support Ubuntu 11.10 and later.

Bindings
--------

* API version updated to 200. See the :ref:`API version upgrade guide <api-version-upgrade-guide-200>` for upgrade details.

Java
----

* New APIs for allocating and managing keyspace (:ref:`Directory <developer-guide-directories>`).
* In most cases, exceptions thrown in synchronous-style Java programs will have the original calling line of code in the backtrace.
* Native resources are handled in a safer and more efficient manner.
* Fix: ``AsyncUtil.whenReady`` crashed when the future being waited on was an error.
* Fix: Calling ``strinc`` on an empty string or a string containing only ``\xff`` bytes threw an exception.
* Fix: Trailing null bytes on the result of ``strinc`` are removed.
 
Node 
----

* New APIs for allocating and managing keyspace (:ref:`Directory <developer-guide-directories>`).
* Support for the Promise/A+ specification with supporting utilities.
* Futures can take multiple callbacks. Callbacks can be added if the original function was called with a callback. The Future type is exposed in our binding.
* Added ``as_foundationdb_key`` and ``as_foundationdb_value`` support.
* Node prints a stack trace if an error occurs in a callback from V8.
* Snapshot transactions can be used in retry loops.
* The methods ``db.setAndWatch`` and ``db.clearAndWatch`` now return an object with a watch member instead of a future.
* Fix: Could not use the ``'this'`` pointer with the retry decorator.
* Fix: Node transactional decorator didn't return a result to the caller if the function was called with a transaction.
* Fix: The program could sometimes crash when watches were manually cancelled.

Ruby
----

* New APIs for allocating and managing keyspace (:ref:`Directory <developer-guide-directories>`).
* Tuple and subspace range assume the empty tuple if none is passed.
* Added ``as_foundationdb_key`` and ``as_foundationdb_value`` support.
* Snapshot transactions can be used in retry loops.
* Allow specifying the API version multiple times, so long as the same version is used each time.
* Fix: ``FDB.options.set_trace_enable`` threw an exception when passed a ``nil`` value.

Python
------

* New APIs for allocating and managing keyspace (:ref:`Directory <developer-guide-directories>`).
* Snapshot transactions can be used in retry loops.
* Support for gevent 1.0.
* Renamed the bitwise atomic operations (``and``, ``or``, ``xor``) to ``bit_and``, ``bit_or``, ``bit_xor``. Added aliases for backwards compatibility.
* Fix: ``get_range_startswith`` didn't work with ``as_foundationdb_key``
* Fix: ``fdb.locality.get_boundary_keys`` and ``fdb.locality.get_addresses_for_key`` did not support ``as_foundationdb_key``.

C
-

* Support for API version 200 and backwards compatibility with previous API versions.

.NET
----

* New APIs for allocating and managing keyspace (:ref:`Directory <developer-guide-directories>`).

Earlier release notes
---------------------
* :doc:`1.0 (API Version 100) <release-notes-100>`
* :doc:`Beta 3 (API Version 23) <release-notes-023>`
* :doc:`Beta 2 (API Version 22) <release-notes-022>`
* :doc:`Beta 1 (API Version 21) <release-notes-021>`
* :doc:`Alpha 6 (API Version 16) <release-notes-016>`
* :doc:`Alpha 5 (API Version 14) <release-notes-014>`
