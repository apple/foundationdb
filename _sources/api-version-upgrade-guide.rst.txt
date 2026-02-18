#########################
API Version Upgrade Guide
#########################

Overview
========

This document provides an overview of changes that an application developer may need to make or effects that they should consider when upgrading the API version in their code. For each version, a list is provided that details the relevant changes when upgrading to that version from a prior version. To upgrade across multiple versions, make sure you apply changes from each version starting after your start version up to and including your target version.

For more details about API versions, see :ref:`api-versions`.

.. _api-version-upgrade-guide-730:

API version 730
===============

General
-------

.. _api-version-upgrade-guide-720:

API version 720
===============

General
-------

* Special keys ``\xff\xff/management/profiling/<client_txn_sample_rate|client_txn_size_limit>`` are removed in 7.2 and the functionalities they provide are now covered by the global configuration module.

.. _api-version-upgrade-guide-710:

API version 710
===============

General
-------

* ``fdb_transaction_get_range_and_flat_map`` API is replaced by ``fdb_transaction_get_mapped_range`` in version 710. The function ``fdb_transaction_get_range_and_flat_map`` is not supported in any API version.

.. _api-version-upgrade-guide-700:

API version 700
===============

General
-------

* Committing a transaction will no longer partially reset it. In particular, getting the read version from a transaction that has committed or failed to commit with an error will return the original read version.

Python bindings
---------------

* The function ``get_estimated_range_size_bytes`` will now throw an error if the ``begin_key`` or ``end_key`` is ``None``.

.. _api-version-upgrade-guide-630:

API version 630
===============

General
-------

* In previous api versions, a get range starting at ``\xff\xff/worker_interfaces`` ignored its arguments and returned keys outside the range requested. In api version 630, you can get similar behavior by reading from ``\xff\xff/worker_interfaces/`` to ``\xff\xff/worker_interfaces0`` and stripping the prefix ``\xff\xff/worker_interfaces/`` from each key in the result.
* The ``get_addresses_for_key`` function now returns strings that include the port in the address. Prior to API version 630, this required using the ``INCLUDE_PORT_IN_ADDRESS`` option, which has now been deprecated.
* The ``ENABLE_SLOW_TASK_PROFILING`` network option has been replaced by ``ENABLE_RUN_LOOP_PROFILING`` and is now deprecated.

C bindings
----------

* The ``FDBKeyValue`` struct's ``key`` and ``value`` members have changed type from ``void*`` to ``uint8_t*``.

Python bindings
---------------

* ``@fdb.transactional`` decorated functions will now throw an error if they return a generator. Previously, it was very easy to write these generators in a way that resulted them committing or retrying earlier than intended.

.. _api-version-upgrade-guide-620:

API version 620
===============

C bindings
----------

* ``fdb_future_get_version`` has been renamed to ``fdb_future_get_int64``.

.. _api-version-upgrade-guide-610:

API version 610
===============

General
-------

* The concept of opening a cluster has been removed from the API. Instead, databases are opened directly. See binding specific notes for the details as they apply to your language binding.
* The ``TIMEOUT``, ``MAX_RETRY_DELAY``, and ``RETRY_LIMIT`` transaction options are no longer reset by calls to ``onError``. 
* Calling ``onError`` with a non-retryable error will now put a transaction into an error state. Previously, this would partially reset the transaction.
* The ``TRANSACTION_LOGGING_ENABLE`` option has been deprecated. Its behavior can be replicated by setting the ``DEBUG_TRANSACTION_IDENTIFIER`` and ``LOG_TRANSACTION`` options.

C bindings
----------

* Creating a database is now done by calling ``fdb_create_database``, which is a synchronous operation. 
* The ``FDBCluster`` type has been eliminated and the following functions have been removed: ``fdb_create_cluster``, ``fdb_cluster_create_database``, ``fdb_cluster_set_option``, ``fdb_cluster_destroy``, ``fdb_future_get_cluster``, and ``fdb_future_get_database``.

Python bindings
---------------

* ``fdb.open`` no longer accepts a ``database_name`` parameter.
* Removed ``fdb.init``, ``fdb.create_cluster``, and ``fdb.Cluster``. ``fdb.open`` should be used instead.

Java bindings
-------------

* ``FDB.createCluster`` and  the ``Cluster`` class have been deprecated. ``FDB.open`` should be used instead.

Ruby bindings
-------------

* ``FDB.open`` no longer accepts a ``database_name`` parameter.
*  Removed ``FDB.init``, ``FDB.create_cluster``, and ``FDB.Cluster``. ``FDB.open`` should be used instead.

Go bindings
-----------

* Added ``fdb.OpenDatabase`` and ``fdb.MustOpenDatabase`` to open a connection to the database by specifying a cluster file.
* Deprecated ``fdb.StartNetwork``, ``fdb.Open``, ``fdb.MustOpen``, and ``fdb.CreateCluster``. ``fdb.OpenDatabase`` or ``fdb.OpenDefault`` should be used instead.

.. _api-version-upgrade-guide-600:

API version 600
===============

General
-------

* The ``TLS_PLUGIN`` option is now a no-op and has been deprecated. TLS support is now included in the published binaries.

.. _api-version-upgrade-guide-520:

API version 520
===============

General
-------

* The ``SET_VERSIONSTAMPED_KEY`` atomic operation now uses four bytes instead of two to specify the versionstamp offset.
* The ``SET_VERSIONSTAMPED_VALUE`` atomic operation now requires a four byte versionstamp offset to be specified at the end of the value, similar to the behavior with ``SET_VERSIONSTAMPED_KEY``.
* The ``READ_AHEAD_DISABLE`` option has been deprecated.

Java and Python bindings
------------------------

* Tuples packed with versionstamps will be encoded with four byte offsets instead of two.

.. _api-version-upgrade-guide-510:

API version 510
===============

General
-------

* The atomic operations ``AND`` and ``MIN`` have changed behavior when used on a key that isn't present in the database. Previously, these operations would set an unset key to a value of equal length with the specified value but containing all null bytes (0x00). Now, an unset key will be set with the value passed to the operation (equivalent to a set). 

Java bindings
-------------

* Note: the Java bindings as of 5.1 no longer support API versions older that 510.
* The Java bindings have moved packages from ``com.apple.cie.foundationdb`` to ``com.apple.foundationdb``.
* The version of the Java bindings using our custom futures library has been deprecated and is no longer being maintained. The Java bindings using ``CompletableFuture`` are the only ones that remain.
* Finalizers now log a warning to ``stderr`` if an object with native resources is not closed. This can be disabled by calling ``FDB.setUnclosedWarning()``.
* Implementers of the ``Disposable`` interface now implement ``AutoCloseable`` instead, with ``close()`` replacing ``dispose()``.
* ``AutoCloseable`` objects will continue to be closed in object finalizers, but this behavior is being deprecated. All ``AutoCloseable`` objects should be explicitly closed.
* ``AsyncIterator`` is no longer closeable.
* ``getBoundaryKeys()`` now returns a ``CloseableAsyncIterable`` rather than an ``AsyncIterator``.

.. _api-version-upgrade-guide-500:

API version 500
===============

Java bindings
-------------

* Note: the Java bindings as of 5.0 no longer support API versions older than 500.
* ``FDB.open`` and ``Cluster.openDatabase`` no longer take a DB name parameter.
* ``Transaction.onError`` invalidates its transaction and asynchronously return a new replacement ``Transaction``.
* ``Transaction.reset`` has been removed.

.. _api-version-upgrade-guide-460:

API version 460
===============

There are no behavior changes in this API version.

.. _api-version-upgrade-guide-450:

API version 450
===============

There are no behavior changes in this API version.

.. _api-version-upgrade-guide-440:

API version 440
===============

There are no behavior changes in this API version.

.. _api-version-upgrade-guide-430:

API version 430
===============

There are no behavior changes in this API version.

.. _api-version-upgrade-guide-420:

API version 420
===============

There are no behavior changes in this API version.

.. _api-version-upgrade-guide-410:

API version 410
===============

General
-------

* Transactions no longer reset after a successful commit.

.. _api-version-upgrade-guide-400:

API version 400
===============

Java bindings
-------------

* The Java bindings have moved packages from ``com.foundationdb`` to ``com.apple.cie.foundationdb``.

.. _api-version-upgrade-guide-300:

API version 300
===============

General
-------

* Snapshot reads now see the effects of prior writes within the same transaction. The previous behavior can be achieved using the ``SNAPSHOT_RYW_DISABLE`` transaction option.
* The transaction size limit now includes the size of conflict ranges in its calculation. The size of a conflict range is the sum of the lengths of its begin and end keys.
* Adding conflict ranges or watches in the system keyspace (beginning with ``\xFF``) now requires setting the ``READ_SYSTEM_KEYS`` or ``ACCESS_SYSTEM_KEYS`` option.

.. _api-version-upgrade-guide-200:

API version 200
===============

General
-------

* Read version requests will now fail when the transaction is reset or has experienced another error.

.. _api-version-upgrade-guide-100:

API version 100
===============

Java bindings
-------------

* ``Transaction.clearRangeStartsWith`` has been deprecated. ``Transaction.clear(Range)`` should be used instead.

Older API versions
==================

API versions from the beta and alpha releases of Foundationdb (pre-100) are not documented here. See :doc:`release-notes/release-notes-023` for details about changes in those releases.
