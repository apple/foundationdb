#############
Release Notes
#############

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

Earlier release notes
---------------------

* :doc:`Beta 2 (API Version 22) <release-notes-022>`
* :doc:`Beta 1 (API Version 21) <release-notes-023>`
* :doc:`Alpha 6 (API Version 16) <release-notes-016>`
* :doc:`Alpha 5 (API Version 14) <release-notes-014>`
